from __future__ import annotations

import ast
from collections import defaultdict
from datetime import date, datetime, timedelta
from functools import lru_cache
import inspect
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
import threading
import time
from types import SimpleNamespace
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

import uuid as _uuid_mod

from flask import Flask, abort, after_this_request, flash, g, get_flashed_messages, has_request_context, jsonify, make_response, redirect, render_template, request, session, url_for
from flask_limiter import Limiter

from sqlalchemy import and_, case, distinct, func, or_, text
from sqlalchemy.orm import sessionmaker
from social_media.hupu.forums import normalize_hupu_forum
from social_media.images import store_prepared_image

from db.llm_models import (
    available_llm_models,
    get_default_llm_model_for_ui,
    get_llm_model_for_purpose,
    resolve_llm_model,
    set_default_llm_model,
    set_llm_model_for_purpose,
)
from db.feature_access import (
    access_level_label,
    feature_access_descriptors,
    get_feature_access_config,
    get_feature_access_level,
    set_feature_access_level,
)
from db.paperclip_settings import (
    build_paperclip_issue_url,
    get_paperclip_issue_base_url,
    set_paperclip_issue_base_url,
)
from db.ai_usage import get_ai_usage_dashboard, log_ai_usage_event
from db.models import Award, Feedback, Game, GameContentAnalysisIssuePost, GameLineScore, GamePlayByPlay, MagicToken, MetricComputeRun, MetricDefinition as MetricDefinitionModel, MetricPerfLog, MetricResult as MetricResultModel, MetricRunLog, PageView, Player, PlayerGameStats, PlayerSalary, ShotRecord, SocialPost, SocialPostDelivery, SocialPostImage, SocialPostVariant, Team, TeamGameStats, User, engine
from db.backfill_nba_player_shot_detail import back_fill_game_shot_record_from_api
from content_pipeline.game_analysis_issues import (
    ensure_game_content_analysis_issue_for_game,
    ensure_game_content_analysis_issues,
    game_analysis_readiness_detail,
    game_analysis_issue_history,
    link_post_to_game_analysis_issue,
    resolve_game_analysis_issue_record,
)
from metrics.framework.family import (
    FAMILY_VARIANT_CAREER,
    FAMILY_VARIANT_SEASON,
    WINDOW_SUFFIXES,
    build_career_code_variant,
    build_career_rule_definition,
    derive_career_description,
    derive_career_min_sample,
    derive_career_name,
    derive_window_description,
    derive_window_min_sample,
    derive_window_name,
    family_base_key,
    family_career_key,
    family_window_key,
    is_reserved_career_key,
    rule_is_career_variant,
    rule_supports_career,
    window_type_from_key,
)
from web.paperclip_bridge import (
    PaperclipBridgeError,
    PaperclipClient,
    actor_label_for_issue,
    append_admin_comment,
    build_post_issue_description,
    build_post_issue_title,
    build_status_handoff_comment,
    desired_issue_state_for_post,
    load_paperclip_bridge_config,
    merge_paperclip_comments,
    normalize_admin_comments,
)
from web.auth_routes import _safe_redirect_url, create_oauth, register_auth_routes
from web.billing_routes import register_billing_routes
from web.admin_content_routes import register_admin_content_routes
from web.admin_misc_routes import register_admin_misc_routes
from web.detail_routes import register_detail_routes
from web.feedback_routes import register_feedback_routes
from web.metric_detail_routes import register_metric_detail_routes
from web.metrics_read_routes import register_metrics_read_routes
from web.metrics_write_routes import register_metrics_write_routes
from web.public_routes import register_public_routes
from runtime_flags import load_runtime_flags, set_runtime_flag

_DRAFT_KEY_PREFIX = "_d_"
_PBP_EVENT_TYPE_LABELS = {
    1: "Made FG",
    2: "Missed FG",
    3: "Free Throw",
    4: "Rebound",
    5: "Turnover",
    6: "Foul",
    7: "Violation",
    8: "Sub",
    9: "Timeout",
    10: "Jump Ball",
    11: "Ejection",
    12: "Period Start",
    13: "Period End",
    18: "Replay",
}
_SOCIAL_POST_EVENT_METRIC_DEEP_DIVE_BRIEF = "metric_deep_dive_brief"


def _make_draft_key(user_id: str, key: str) -> str:
    """Prefix a metric key for draft storage: _d_{user_id[:8]}_{key}"""
    return f"{_DRAFT_KEY_PREFIX}{user_id[:8]}_{key}"


def _is_draft_key(key: str) -> bool:
    return key.startswith(_DRAFT_KEY_PREFIX)


def _strip_draft_prefix(key: str) -> str:
    """_d_abc12345_foo → foo"""
    if not _is_draft_key(key):
        return key
    # Skip "_d_" + 8 chars + "_"
    parts = key.split("_", 3)  # ['', 'd', 'abc12345', 'foo_bar']
    return parts[3] if len(parts) >= 4 else key


def _replace_key_in_code(code: str, old_key: str, new_key: str) -> str:
    """Replace the key attribute value in generated Python code."""
    import re as _re
    return _re.sub(
        r'''(key\s*=\s*)(["'])''' + _re.escape(old_key) + r'''(\2)''',
        lambda m: f'{m.group(1)}{m.group(2)}{new_key}{m.group(3)}',
        code,
    )


def _pbp_event_type_label(event_type):
    if event_type is None:
        return "-"
    return _PBP_EVENT_TYPE_LABELS.get(event_type, str(event_type))


def _box_score_source_label(value: str | None) -> str:
    source = (value or "").strip()
    if not source:
        return "Unknown"
    mapping = {
        "nba_api_box_scores": "NBA API box scores",
        "kaggle_box_scores": "Kaggle box scores",
    }
    if source in mapping:
        return mapping[source]
    return source.replace("_", " ").strip().title()


_ADMIN_TOP_PAGES_WINDOWS = {
    "1d": timedelta(days=1),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
}


def _admin_top_pages_window(raw_value: str | None) -> str:
    value = (raw_value or "1d").strip().lower()
    return value if value in _ADMIN_TOP_PAGES_WINDOWS else "1d"


def _extract_referrer_source(referrer: str | None) -> str:
    value = (referrer or "").strip()
    if not value:
        return "Direct"

    parsed = urlparse(value if "://" in value else f"//{value}")
    host = (parsed.hostname or parsed.netloc or value).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host or "Direct"


def _human_page_view_filter(page_view_model):
    return or_(page_view_model.is_crawler.is_(False), page_view_model.is_crawler.is_(None))


def _load_admin_top_pages_panel(session, raw_window: str | None):
    selected_window = _admin_top_pages_window(raw_window)
    cutoff = datetime.utcnow() - _ADMIN_TOP_PAGES_WINDOWS[selected_window]

    top_page_rows = (
        session.query(
            PageView.path.label("path"),
            func.count(PageView.id).label("views"),
            func.count(func.distinct(PageView.visitor_id)).label("unique_visitors"),
        )
        .filter(PageView.created_at >= cutoff, _human_page_view_filter(PageView))
        .group_by(PageView.path)
        .order_by(func.count(PageView.id).desc(), PageView.path.asc())
        .limit(20)
        .all()
    )

    top_pages = [
        {
            "rank": index,
            "path": row.path or "/",
            "views": int(row.views or 0),
            "unique_visitors": int(row.unique_visitors or 0),
        }
        for index, row in enumerate(top_page_rows, start=1)
    ]

    raw_referrer_rows = (
        session.query(
            PageView.referrer.label("referrer"),
            func.count(PageView.id).label("views"),
        )
        .filter(PageView.created_at >= cutoff, _human_page_view_filter(PageView))
        .group_by(PageView.referrer)
        .all()
    )

    referrer_totals: dict[str, int] = defaultdict(int)
    for row in raw_referrer_rows:
        referrer_totals[_extract_referrer_source(row.referrer)] += int(row.views or 0)

    top_referrers = [
        {"rank": index, "source": source, "views": views}
        for index, (source, views) in enumerate(
            sorted(referrer_totals.items(), key=lambda item: (-item[1], item[0]))[:10],
            start=1,
        )
    ]

    return {
        "selected_window": selected_window,
        "top_pages": top_pages,
        "top_referrers": top_referrers,
    }


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32))
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=30)
SessionLocal = sessionmaker(bind=engine)

_LOCALIZED_PUBLIC_ENDPOINTS = {
    "home": "home_zh",
    "games_list": "games_list_zh",
    "awards_page": "awards_page_zh",
    "players_browse": "players_browse_zh",
    "players_compare": "players_compare_zh",
    "draft_page": "draft_page_zh",
    "player_page": "player_page_zh",
    "team_page": "team_page_zh",
    "game_page": "game_page_zh",
    "game_fragment_metrics": "game_fragment_metrics_zh",
    "metrics_browse": "metrics_browse_zh",
    "metric_detail": "metric_detail_zh",
    "my_metrics": "my_metrics_zh",
    "metric_new": "metric_new_zh",
    "metric_edit": "metric_edit_zh",
    "pricing": "pricing_zh",
    "account_page": "account_page_zh",
    "news_detail": "news_detail_zh",
    "teams_list_page": "teams_list_page_zh",
}
_ZH_TO_BASE_ENDPOINT = {zh_endpoint: endpoint for endpoint, zh_endpoint in _LOCALIZED_PUBLIC_ENDPOINTS.items()}
_LEGACY_ENTITY_PATH_PATTERNS = {
    "player": re.compile(r"^(/cn)?/players/([^/]+)$"),
    "team": re.compile(r"^(/cn)?/teams/([^/]+)$"),
    "game": re.compile(r"^(/cn)?/games/([^/]+)$"),
}


def _current_lang() -> str:
    if not has_request_context():
        return "en"
    return getattr(g, "lang", "en")


def _is_zh(lang: str | None = None) -> bool:
    return (lang or _current_lang()) == "zh"


def _t(en_text: str, zh_text: str | None = None) -> str:
    if _is_zh() and zh_text is not None:
        return zh_text
    return en_text


def _base_public_endpoint(endpoint: str | None) -> str | None:
    if endpoint in _ZH_TO_BASE_ENDPOINT:
        return _ZH_TO_BASE_ENDPOINT[endpoint]
    return endpoint


def _localized_url_for(endpoint: str, **values) -> str:
    lang = values.pop("_lang", None) or _current_lang()
    base_endpoint = _base_public_endpoint(endpoint)
    if base_endpoint in _LOCALIZED_PUBLIC_ENDPOINTS:
        endpoint = _LOCALIZED_PUBLIC_ENDPOINTS[base_endpoint] if lang == "zh" else base_endpoint
    return url_for(endpoint, **values)


def _language_toggle_url() -> str | None:
    if not has_request_context() or not request.endpoint:
        return None
    base_endpoint = _base_public_endpoint(request.endpoint)
    if base_endpoint not in _LOCALIZED_PUBLIC_ENDPOINTS:
        return None
    values = dict(request.view_args or {})
    values.update(request.args.to_dict(flat=True))
    return _localized_url_for(base_endpoint, _lang="en" if _is_zh() else "zh", **values)


def _canonical_url() -> str | None:
    """Return the canonical (English) absolute URL for the current page."""
    if not has_request_context() or not request.endpoint:
        return None
    base_endpoint = _base_public_endpoint(request.endpoint)
    if base_endpoint not in _LOCALIZED_PUBLIC_ENDPOINTS:
        return request.url
    values = dict(request.view_args or {})
    return url_for(base_endpoint, _external=True, **values)


def _hreflang_links() -> list[dict[str, str]]:
    """Return hreflang alternate link dicts for the current page."""
    if not has_request_context() or not request.endpoint:
        return []
    base_endpoint = _base_public_endpoint(request.endpoint)
    if base_endpoint not in _LOCALIZED_PUBLIC_ENDPOINTS:
        return []
    values = dict(request.view_args or {})
    en_url = url_for(base_endpoint, _external=True, **values)
    zh_url = url_for(_LOCALIZED_PUBLIC_ENDPOINTS[base_endpoint], _external=True, **values)
    return [
        {"lang": "en", "url": en_url},
        {"lang": "zh-CN", "url": zh_url},
        {"lang": "x-default", "url": en_url},
    ]


def _localized_metric_name(name: str | None, name_zh: str | None = None) -> str:
    localized = (name_zh or "").strip()
    if _is_zh() and localized:
        return localized
    return (name or "").strip()


def _localized_metric_description(description: str | None, description_zh: str | None = None) -> str:
    localized = (description_zh or "").strip()
    if _is_zh() and localized:
        return localized
    return (description or "").strip()


def _display_team_name(team: Team | None) -> str:
    if team is None:
        return "-"
    if _is_zh() and getattr(team, "full_name_zh", None):
        return team.full_name_zh
    return team.full_name or team.abbr or team.team_id or "-"


def _display_player_name(player: Player | None) -> str:
    if player is None:
        return "-"
    if _is_zh() and getattr(player, "full_name_zh", None):
        return player.full_name_zh
    return player.full_name or player.player_id or "-"

def _real_ip() -> str:
    """Return the real client IP, preferring Cloudflare header."""
    return (
        request.headers.get("CF-Connecting-IP")
        or request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        or request.remote_addr
    )


limiter = Limiter(
    _real_ip,
    app=app,
    default_limits=["200 per minute"],
    storage_uri="memory://",
)


@app.before_request
def set_request_language():
    path = request.path or "/"
    g.lang = "zh" if path == "/cn" or path.startswith("/cn/") else "en"


@app.before_request
def _redirect_legacy_entity_id_paths():
    path = (request.path or "").strip()
    if not path or request.method != "GET":
        return

    player_match = _LEGACY_ENTITY_PATH_PATTERNS["player"].match(path)
    if player_match:
        raw_value = player_match.group(2)
        legacy_player_id = raw_value.removeprefix("player-") if raw_value.startswith("player-") else raw_value
        slug = _ensure_player_slug_cache().get(str(legacy_player_id))
        if slug and slug != raw_value:
            return redirect(_localized_url_for("player_page", slug=slug, **request.args.to_dict(flat=True)), code=302)
        return

    team_match = _LEGACY_ENTITY_PATH_PATTERNS["team"].match(path)
    if team_match:
        raw_value = team_match.group(2)
        legacy_team_id = raw_value.removeprefix("team-") if raw_value.startswith("team-") else raw_value
        slug = _ensure_team_slug_cache().get(str(legacy_team_id))
        if slug and slug != raw_value:
            return redirect(_localized_url_for("team_page", slug=slug, **request.args.to_dict(flat=True)), code=302)
        return

    game_match = _LEGACY_ENTITY_PATH_PATTERNS["game"].match(path)
    if game_match:
        raw_value = game_match.group(2)
        legacy_game_id = raw_value.removeprefix("game-") if raw_value.startswith("game-") else raw_value
        slug = _ensure_game_slug_cache().get(str(legacy_game_id))
        if slug and slug != raw_value:
            return redirect(_localized_url_for("game_page", slug=slug, **request.args.to_dict(flat=True)), code=302)


@app.template_filter("pct_fmt")
def pct_fmt(value) -> str:
    if value is None:
        return "—"

    if isinstance(value, str):
        text = value.strip()
        if not text or text in {"-", "—"}:
            return "—"
        has_pct_suffix = text.endswith("%")
        if has_pct_suffix:
            text = text[:-1].strip()
        try:
            number = float(text)
        except ValueError:
            return value
    else:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return "—"
        has_pct_suffix = False

    if not has_pct_suffix and -1.0 <= number <= 1.0:
        number *= 100.0
    return f"{number:.1f}%"


# ── Error handlers ───────────────────────────────────────────────────────────

@app.errorhandler(404)
def page_not_found(e):
    return render_template("404.html"), 404


@app.errorhandler(500)
def internal_server_error(e):
    return render_template("500.html"), 500


# ── SEO routes ───────────────────────────────────────────────────────────────

@app.route("/robots.txt")
def robots_txt():
    lines = [
        "User-agent: *",
        "Allow: /",
        "Disallow: /admin",
        "Disallow: /auth/",
        "Disallow: /api/",
        "",
        "Sitemap: https://funba.app/sitemap.xml",
    ]
    return make_response("\n".join(lines)), 200, {"Content-Type": "text/plain"}


_SITEMAP_BASE = "https://funba.app"
_SITEMAP_NS = "http://www.sitemaps.org/schemas/sitemap/0.9"


def _xml_response(xml_lines: list[str]):
    return make_response("\n".join(xml_lines)), 200, {"Content-Type": "application/xml"}


def _urlset(urls: list[str]):
    xml = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<urlset xmlns="{_SITEMAP_NS}">',
    ]
    for url in urls:
        xml.append(f"  <url><loc>{url}</loc></url>")
    xml.append("</urlset>")
    return xml


@app.route("/sitemap.xml")
def sitemap_xml():
    """Sitemap index pointing to sub-sitemaps."""
    subs = [
        f"{_SITEMAP_BASE}/sitemap-static.xml",
        f"{_SITEMAP_BASE}/sitemap-teams.xml",
        f"{_SITEMAP_BASE}/sitemap-players.xml",
        f"{_SITEMAP_BASE}/sitemap-metrics.xml",
    ]
    with SessionLocal() as db:
        seasons = [s for (s,) in db.query(distinct(Game.season)).order_by(Game.season).all()]
    for season in seasons:
        subs.append(f"{_SITEMAP_BASE}/sitemap-games-{season}.xml")
    xml = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<sitemapindex xmlns="{_SITEMAP_NS}">',
    ]
    for loc in subs:
        xml.append(f"  <sitemap><loc>{loc}</loc></sitemap>")
    xml.append("</sitemapindex>")
    return _xml_response(xml)


@app.route("/sitemap-static.xml")
def sitemap_static_xml():
    return _xml_response(_urlset([
        f"{_SITEMAP_BASE}/",
        f"{_SITEMAP_BASE}/games",
        f"{_SITEMAP_BASE}/awards",
        f"{_SITEMAP_BASE}/metrics",
    ]))


@app.route("/sitemap-teams.xml")
def sitemap_teams_xml():
    with SessionLocal() as db:
        teams = db.query(Team.slug).filter(Team.slug.isnot(None)).all()
    return _xml_response(_urlset([f"{_SITEMAP_BASE}/teams/{slug}" for (slug,) in teams]))


@app.route("/sitemap-players.xml")
def sitemap_players_xml():
    with SessionLocal() as db:
        players = db.query(Player.slug).filter(Player.slug.isnot(None)).all()
    return _xml_response(_urlset([f"{_SITEMAP_BASE}/players/{slug}" for (slug,) in players]))


@app.route("/sitemap-metrics.xml")
def sitemap_metrics_xml():
    with SessionLocal() as db:
        metrics = db.query(MetricDefinitionModel.key).all()
    return _xml_response(_urlset([f"{_SITEMAP_BASE}/metrics/{k}" for (k,) in metrics]))


@app.route("/sitemap-games-<int:season>.xml")
def sitemap_games_xml(season: int):
    with SessionLocal() as db:
        games = db.query(Game.slug).filter(Game.season == season, Game.slug.isnot(None)).all()
    if not games:
        abort(404)
    return _xml_response(_urlset([f"{_SITEMAP_BASE}/games/{slug}" for (slug,) in games]))


# ── Google OAuth ─────────────────────────────────────────────────────────────
oauth = create_oauth(app)

# Real lat/lon for each NBA team (slight offsets for same-city pairs)
_TEAM_MAP_POSITIONS: dict[str, tuple[float, float]] = {
    "ATL": (33.757,  -84.396),
    "BOS": (42.366,  -71.062),
    "BKN": (40.683,  -74.004),
    "CHA": (35.225,  -80.839),
    "CHI": (41.881,  -87.674),
    "CLE": (41.497,  -81.688),
    "DAL": (32.790,  -96.810),
    "DEN": (39.749, -105.007),
    "DET": (42.341,  -83.055),
    "GSW": (37.768, -122.388),
    "HOU": (29.751,  -95.362),
    "IND": (39.764,  -86.156),
    "LAC": (34.130, -118.100),  # offset from LAL
    "LAL": (33.950, -118.450),
    "MEM": (35.138,  -90.051),
    "MIA": (25.781,  -80.188),
    "MIL": (43.045,  -87.918),
    "MIN": (44.980,  -93.276),
    "NOP": (29.949,  -90.082),
    "NYK": (40.800,  -73.940),  # offset from BKN
    "OKC": (35.463,  -97.515),
    "ORL": (28.539,  -81.384),
    "PHI": (39.901,  -75.172),
    "PHX": (33.446, -112.071),
    "POR": (45.532, -122.667),
    "SAC": (38.580, -121.499),
    "SAS": (29.427,  -98.438),
    "TOR": (43.643,  -79.379),
    "UTA": (40.768, -111.901),
    "WAS": (38.898,  -77.021),
}


# Historical franchise name overrides.
# Each entry: team_id -> list of (season_start_year_exclusive, abbr, name)
# If the season's start year is < cutoff, that name applies.
# Listed with the oldest cutoff first.
_FRANCHISE_NAME_HISTORY: dict[str, list[tuple[int, str, str]]] = {
    "1610612740": [(2013, "NOH", "New Orleans Hornets")],       # → Pelicans 2013-14
    "1610612751": [(2012, "NJN", "New Jersey Nets")],           # → Nets 2012-13
    "1610612760": [(2008, "SEA", "Seattle SuperSonics")],       # → Thunder 2008-09
    "1610612763": [(2001, "VAN", "Vancouver Grizzlies")],       # → Grizzlies 2001-02
    "1610612766": [(2014, "CHA", "Charlotte Bobcats")],         # → Hornets 2014-15
}


def _franchise_display(team_id: str, season: str | None, team: Team | None) -> tuple[str, str]:
    """Return (abbr, full_name) for a team in a given season, applying historical overrides."""
    default_abbr = team.abbr if team else team_id
    default_name = _display_team_name(team) if team else team_id
    if season and team_id in _FRANCHISE_NAME_HISTORY:
        try:
            season_year = int(season[1:])  # e.g. "22012" -> 2012
        except (ValueError, IndexError):
            return default_abbr, default_name
        for cutoff, abbr, name in _FRANCHISE_NAME_HISTORY[team_id]:
            if season_year < cutoff:
                return abbr, name
    return default_abbr, default_name


def _team_map(session) -> dict[str, Team]:
    teams = session.query(Team).all()
    return {team.team_id: team for team in teams}


def _team_name(teams: dict[str, Team], team_id: str | None) -> str:
    if not team_id:
        return "-"
    team = teams.get(team_id)
    if team is None:
        return team_id
    return _display_team_name(team)


def _team_abbr(teams: dict[str, Team], team_id: str | None) -> str:
    if not team_id:
        return "-"
    team = teams.get(team_id)
    if team is None:
        return team_id
    return team.abbr or team.full_name or team_id


def _team_logo_url(team_id: str | None) -> str | None:
    if not team_id:
        return None
    return f"https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"


_AWARD_TYPE_META: dict[str, dict[str, str]] = {
    "champion": {
        "label": "Champions",
        "short_label": "Champion",
        "badge_label": "Champion",
        "entity": "team",
    },
    "finals_mvp": {
        "label": "Finals MVP",
        "short_label": "Finals MVP",
        "badge_label": "Finals MVP",
        "entity": "player",
    },
    "mvp": {
        "label": "MVP",
        "short_label": "MVP",
        "badge_label": "MVP",
        "entity": "player",
    },
    "scoring_champion": {
        "label": "Scoring Champ",
        "short_label": "Scoring Champ",
        "badge_label": "Scoring Champ",
        "entity": "player",
    },
    "all_nba_first": {
        "label": "All-NBA 1st",
        "short_label": "All-NBA 1st",
        "badge_label": "1st Team",
        "entity": "player",
    },
    "all_nba_second": {
        "label": "All-NBA 2nd",
        "short_label": "All-NBA 2nd",
        "badge_label": "2nd Team",
        "entity": "player",
    },
    "all_nba_third": {
        "label": "All-NBA 3rd",
        "short_label": "All-NBA 3rd",
        "badge_label": "3rd Team",
        "entity": "player",
    },
    "dpoy": {
        "label": "DPOY",
        "short_label": "DPOY",
        "badge_label": "DPOY",
        "entity": "player",
    },
    "roy": {
        "label": "ROY",
        "short_label": "ROY",
        "badge_label": "ROY",
        "entity": "player",
    },
    "mip": {
        "label": "MIP",
        "short_label": "MIP",
        "badge_label": "MIP",
        "entity": "player",
    },
    "sixth_man": {
        "label": "Sixth Man",
        "short_label": "6th Man",
        "badge_label": "6th Man",
        "entity": "player",
    },
    "all_defensive_first": {
        "label": "All-Def 1st",
        "short_label": "All-Def 1st",
        "badge_label": "1st Team",
        "entity": "player",
    },
    "all_defensive_second": {
        "label": "All-Def 2nd",
        "short_label": "All-Def 2nd",
        "badge_label": "2nd Team",
        "entity": "player",
    },
    "all_rookie_first": {
        "label": "All-Rookie 1st",
        "short_label": "All-Rk 1st",
        "badge_label": "1st Team",
        "entity": "player",
    },
    "all_rookie_second": {
        "label": "All-Rookie 2nd",
        "short_label": "All-Rk 2nd",
        "badge_label": "2nd Team",
        "entity": "player",
    },
}
_AWARD_TYPE_ORDER = list(_AWARD_TYPE_META.keys())
_AWARD_SORT_INDEX = {award_type: idx for idx, award_type in enumerate(_AWARD_TYPE_ORDER)}
_AWARD_TAB_GROUPS = [
    {"label": None, "types": ["champion"]},
    {"label": "Individual", "types": ["mvp", "finals_mvp", "dpoy", "roy", "mip", "sixth_man", "scoring_champion"]},
    {"label": "All-NBA", "types": ["all_nba_first", "all_nba_second", "all_nba_third"]},
    {"label": "All-Defensive", "types": ["all_defensive_first", "all_defensive_second"]},
    {"label": "All-Rookie", "types": ["all_rookie_first", "all_rookie_second"]},
]
_GRID_AWARD_PREFIXES = ("all_nba_", "all_defensive_", "all_rookie_")


def _award_type_label(award_type: str, *, short: bool = False) -> str:
    meta = _AWARD_TYPE_META.get(award_type, {})
    key = "short_label" if short else "label"
    label = meta.get(key, award_type.replace("_", " ").title())
    zh_labels = {
        "Champions": "总冠军",
        "Champion": "总冠军",
        "MVP": "最有价值球员",
        "Finals MVP": "总决赛 MVP",
        "Scoring Champion": "得分王",
        "DPOY": "最佳防守球员",
        "ROY": "最佳新秀",
        "MIP": "最快进步球员",
        "Sixth Man": "最佳第六人",
        "6th Man": "第六人",
        "All-NBA 1st": "最佳阵容一阵",
        "All-NBA 2nd": "最佳阵容二阵",
        "All-NBA 3rd": "最佳阵容三阵",
        "All-Def 1st": "最佳防守一阵",
        "All-Def 2nd": "最佳防守二阵",
        "All-Rookie 1st": "最佳新秀一阵",
        "All-Rookie 2nd": "最佳新秀二阵",
        "All-Rk 1st": "新秀一阵",
        "All-Rk 2nd": "新秀二阵",
    }
    return zh_labels.get(label, label) if _is_zh() else label


def _award_badge_label(award_type: str) -> str:
    meta = _AWARD_TYPE_META.get(award_type, {})
    label = meta.get("badge_label", _award_type_label(award_type, short=True))
    zh_labels = {
        "Champion": "总冠军",
        "All-NBA 1st": "一阵",
        "All-NBA 2nd": "二阵",
        "All-NBA 3rd": "三阵",
        "All-Def 1st": "防守一阵",
        "All-Def 2nd": "防守二阵",
        "All-Rk 1st": "新秀一阵",
        "All-Rk 2nd": "新秀二阵",
        "6th Man": "第六人",
    }
    return zh_labels.get(label, label) if _is_zh() else label


def _award_order_case(column):
    return case(_AWARD_SORT_INDEX, value=column, else_=len(_AWARD_TYPE_ORDER) + 1)


def _is_grid_award_type(award_type: str) -> bool:
    return award_type.startswith(_GRID_AWARD_PREFIXES)


def _coerce_award_season(value: str | int | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 5 and text.isdigit():
        return int(text)
    return None


def _player_headshot_url(player_id: str | None) -> str | None:
    if not player_id:
        return None
    return f"https://cdn.nba.com/headshots/nba/latest/260x190/{player_id}.png"


# ── Player slug cache ────────────────────────────────────────────────────────

_player_slug_cache: dict[str, str] = {}
_player_slug_cache_ts: float = 0


def _ensure_player_slug_cache() -> dict[str, str]:
    import time
    global _player_slug_cache, _player_slug_cache_ts
    now = time.monotonic()
    if _player_slug_cache and (now - _player_slug_cache_ts) < 300:
        return _player_slug_cache
    with SessionLocal() as session:
        rows = session.query(Player.player_id, Player.slug).filter(Player.slug.isnot(None)).all()
        _player_slug_cache = {r.player_id: r.slug for r in rows}
    _player_slug_cache_ts = now
    return _player_slug_cache


def _player_url(player_id: str) -> str:
    slug_map = _ensure_player_slug_cache()
    slug = slug_map.get(str(player_id))
    if slug:
        return _localized_url_for("player_page", slug=slug)
    return _localized_url_for("player_page", slug=f"player-{player_id}")


# ── Game slug cache ──────────────────────────────────────────────────────────

_game_slug_cache: dict[str, str] = {}
_game_slug_cache_ts: float = 0


def _ensure_game_slug_cache() -> dict[str, str]:
    import time
    global _game_slug_cache, _game_slug_cache_ts
    now = time.monotonic()
    if _game_slug_cache and (now - _game_slug_cache_ts) < 300:
        return _game_slug_cache
    with SessionLocal() as session:
        rows = session.query(Game.game_id, Game.slug).filter(Game.slug.isnot(None)).all()
        _game_slug_cache = {r.game_id: r.slug for r in rows}
    _game_slug_cache_ts = now
    return _game_slug_cache


def _game_url(game_id: str) -> str:
    slug_map = _ensure_game_slug_cache()
    slug = slug_map.get(str(game_id))
    if slug:
        return _localized_url_for("game_page", slug=slug)
    return _localized_url_for("game_page", slug=f"game-{game_id}")


# ── Team slug cache ──────────────────────────────────────────────────────────

_team_slug_cache: dict[str, str] = {}
_team_slug_cache_ts: float = 0


def _ensure_team_slug_cache() -> dict[str, str]:
    import time
    global _team_slug_cache, _team_slug_cache_ts
    now = time.monotonic()
    if _team_slug_cache and (now - _team_slug_cache_ts) < 300:
        return _team_slug_cache
    with SessionLocal() as session:
        rows = session.query(Team.team_id, Team.slug).filter(Team.slug.isnot(None)).all()
        _team_slug_cache = {r.team_id: r.slug for r in rows}
    _team_slug_cache_ts = now
    return _team_slug_cache


def _team_url(team_id: str | None) -> str | None:
    if not team_id:
        return None
    slug_map = _ensure_team_slug_cache()
    slug = slug_map.get(str(team_id))
    if slug:
        return _localized_url_for("team_page", slug=slug)
    return None


def _award_entry_from_row(row, teams: dict[str, Team]) -> dict[str, object]:
    season_value = _coerce_award_season(row.season)
    season_token = str(season_value) if season_value is not None else str(row.season)
    team = teams.get(row.team_id) if row.team_id else None
    team_abbr, team_name = (None, None)
    if row.team_id:
        team_abbr, team_name = _franchise_display(str(row.team_id), season_token, team)
    elif row.notes:
        team_name = str(row.notes)

    return {
        "award_type": row.award_type,
        "season": season_value or 0,
        "season_label": _season_year_label(season_token),
        "player_id": row.player_id,
        "player_name": (row.player_name_zh if _is_zh() and getattr(row, "player_name_zh", None) else row.player_name) or row.player_id,
        "player_headshot_url": _player_headshot_url(row.player_id),
        "team_id": row.team_id,
        "team_abbr": team_abbr,
        "team_name": team_name,
        "notes": row.notes,
        "winner_key": row.player_id or row.team_id or row.notes or f"{row.award_type}:{season_token}:{row.id}",
        "streak": None,
    }


def _group_award_entries(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    sections: list[dict[str, object]] = []
    for award_type in _AWARD_TYPE_ORDER:
        award_entries = [entry.copy() for entry in entries if entry["award_type"] == award_type]
        if not award_entries:
            continue

        by_season: dict[int, list[dict[str, object]]] = defaultdict(list)
        for entry in award_entries:
            by_season[int(entry["season"])].append(entry)

        ordered_seasons = sorted(by_season.keys(), reverse=True)
        season_winners = {
            season: {str(entry["winner_key"]) for entry in season_entries if entry.get("winner_key")}
            for season, season_entries in by_season.items()
        }

        groups: list[dict[str, object]] = []
        for season in ordered_seasons:
            season_entries = by_season[season]
            season_entries.sort(
                key=lambda entry: (
                    str(entry.get("player_name") or entry.get("team_name") or ""),
                    str(entry.get("player_id") or ""),
                    str(entry.get("team_id") or ""),
                )
            )
            group = {
                "award_type": award_type,
                "season": season,
                "season_label": season_entries[0]["season_label"],
                "entries": season_entries,
                "streak": None,
                "is_dynasty": False,
            }

            if _is_grid_award_type(award_type):
                for entry in season_entries:
                    winner_key = str(entry.get("winner_key") or "")
                    streak = 1
                    next_season = season - 1
                    while winner_key and winner_key in season_winners.get(next_season, set()):
                        streak += 1
                        next_season -= 1
                    entry["streak"] = streak if streak > 1 else None
            else:
                winner_key = str(season_entries[0].get("winner_key") or "")
                streak = 1
                next_season = season - 1
                while winner_key and winner_key in season_winners.get(next_season, set()):
                    streak += 1
                    next_season -= 1
                group["streak"] = streak if streak > 1 else None
                group["is_dynasty"] = award_type == "champion" and streak > 1

            groups.append(group)

        sections.append(
            {
                "award_type": award_type,
                "label": _award_type_label(award_type),
                "short_label": _award_type_label(award_type, short=True),
                "is_team_award": _AWARD_TYPE_META[award_type]["entity"] == "team",
                "is_grid_award": _is_grid_award_type(award_type),
                "groups": groups,
            }
        )
    return sections


_METRIC_SAMPLE_NOTE_RE = re.compile(r"[（(]([^)）]+)[）)]")


def _metric_def_view(metric_def, *, status: str | None = None, source_type: str | None = None):
    """Normalize runtime and DB metric objects for template rendering."""
    name_zh = getattr(metric_def, "name_zh", "") or ""
    description_zh = getattr(metric_def, "description_zh", "") or ""
    description = _localized_metric_description(getattr(metric_def, "description", "") or "", description_zh)
    min_sample = int(getattr(metric_def, "min_sample", 1) or 1)
    sample_note = ""
    if min_sample > 1:
        m = _METRIC_SAMPLE_NOTE_RE.search(description or "")
        if m:
            sample_note = m.group(1).strip()
    return SimpleNamespace(
        key=metric_def.key,
        name=_localized_metric_name(metric_def.name, name_zh),
        name_en=metric_def.name,
        name_zh=name_zh,
        description=description,
        sample_note=sample_note,
        description_en=getattr(metric_def, "description", "") or "",
        description_zh=description_zh,
        scope=metric_def.scope,
        category=getattr(metric_def, "category", "") or "",
        status=status or getattr(metric_def, "status", "published"),
        source_type=source_type or getattr(metric_def, "source_type", "code"),
        min_sample=int(getattr(metric_def, "min_sample", 1) or 1),
        group_key=getattr(metric_def, "group_key", None),
        career=bool(getattr(metric_def, "career", False)),
        supports_career=bool(getattr(metric_def, "supports_career", False)),
        trigger=getattr(metric_def, "trigger", "game"),
        sub_key_type=getattr(metric_def, "sub_key_type", None),
        sub_key_label=getattr(metric_def, "sub_key_label", None),
        sub_key_label_zh=getattr(metric_def, "sub_key_label_zh", None),
        sub_key_rank_scope=getattr(metric_def, "sub_key_rank_scope", None),
        fill_missing_sub_keys_with_zero=bool(getattr(metric_def, "fill_missing_sub_keys_with_zero", False)),
    )


def _batch_metric_names(session, metric_keys: set[str]) -> dict[str, str]:
    """Batch-load localized metric names for a set of keys (1 query)."""
    from metrics.framework.runtime import get_metric as _get_metric

    if not metric_keys:
        return {}

    # Collect all possible DB keys (both original and base keys)
    all_lookup_keys = set()
    for mk in metric_keys:
        all_lookup_keys.add(mk)
        all_lookup_keys.add(family_base_key(mk))

    db_rows = (
        session.query(MetricDefinitionModel.key, MetricDefinitionModel.name, MetricDefinitionModel.name_zh)
        .filter(MetricDefinitionModel.key.in_(all_lookup_keys))
        .all()
    )
    db_by_key = {r.key: r for r in db_rows}

    result = {}
    for mk in metric_keys:
        base_key = family_base_key(mk)
        db_metric = db_by_key.get(mk) or db_by_key.get(base_key)
        runtime_metric = _get_metric(mk, session=session)
        if runtime_metric is None and base_key != mk:
            runtime_metric = _get_metric(base_key, session=session)

        name = getattr(db_metric, "name", None) or getattr(runtime_metric, "name", None) or base_key.replace("_", " ").title()
        name_zh = getattr(db_metric, "name_zh", None) or getattr(runtime_metric, "name_zh", None)
        localized = _localized_metric_name(name, name_zh)
        result[mk] = localized
    return result


def _metric_name_for_key(session, metric_key: str) -> str:
    from metrics.framework.runtime import get_metric as _get_metric

    base_key = family_base_key(metric_key)
    db_metric = (
        session.query(MetricDefinitionModel)
        .filter(MetricDefinitionModel.key.in_([metric_key, base_key]))
        .order_by(MetricDefinitionModel.key.desc())
        .first()
    )
    runtime_metric = _get_metric(metric_key, session=session)
    if runtime_metric is None and base_key != metric_key:
        runtime_metric = _get_metric(base_key, session=session)

    name = getattr(db_metric, "name", None) or getattr(runtime_metric, "name", None) or base_key.replace("_", " ").title()
    name_zh = getattr(db_metric, "name_zh", None) or getattr(runtime_metric, "name_zh", None)
    return _localized_metric_name(name, name_zh)


def _truncate_search_text(text: str | None, limit: int = 2400) -> str:
    value = (text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 15].rstrip() + " ...[truncated]"


def _metric_source_owner(metric_def) -> type:
    return type(metric_def)


@lru_cache(maxsize=256)
def _metric_module_doc(metric_cls: type) -> str:
    module = inspect.getmodule(metric_cls)
    if module is None:
        return ""
    return _truncate_search_text(inspect.getdoc(module) or "", limit=600)


@lru_cache(maxsize=256)
def _metric_source_excerpt(metric_cls: type) -> str:
    try:
        return _truncate_search_text(inspect.getsource(metric_cls), limit=2400)
    except (OSError, TypeError):
        return ""


def _is_metric_definition_base(base: ast.expr) -> bool:
    return (
        isinstance(base, ast.Name) and base.id == "MetricDefinition"
    ) or (
        isinstance(base, ast.Attribute) and base.attr == "MetricDefinition"
    )


def _normalize_code_metric_key(code_python: str, expected_key: str | None = None) -> str:
    if not expected_key:
        return code_python

    try:
        tree = ast.parse(code_python)
    except SyntaxError:
        return code_python

    lines = code_python.splitlines(keepends=True)
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not any(_is_metric_definition_base(base) for base in node.bases):
            continue
        for stmt in node.body:
            target_name = None
            value_node = None
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target_name = stmt.targets[0].id
                value_node = stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target_name = stmt.target.id
                value_node = stmt.value

            if target_name != "key" or not isinstance(value_node, ast.Constant) or not isinstance(value_node.value, str):
                continue
            if stmt.end_lineno is None or stmt.end_col_offset is None or stmt.lineno != stmt.end_lineno:
                return code_python

            lineno = stmt.lineno - 1
            line = lines[lineno]
            prefix = line[:stmt.col_offset]
            suffix = line[stmt.end_col_offset:]
            lines[lineno] = prefix + f"key = {expected_key!r}" + suffix
            return "".join(lines)

    return code_python


def _normalize_code_metric_rank_order(code_python: str, rank_order: str | None = None) -> str:
    if rank_order not in {"asc", "desc"}:
        return code_python

    try:
        tree = ast.parse(code_python)
    except SyntaxError:
        return code_python

    lines = code_python.splitlines(keepends=True)
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not any(_is_metric_definition_base(base) for base in node.bases):
            continue

        indent = " " * ((node.body[0].col_offset if node.body else node.col_offset + 4))
        insert_at = node.lineno

        for stmt in node.body:
            target_name = None
            value_node = None
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target_name = stmt.targets[0].id
                value_node = stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target_name = stmt.target.id
                value_node = stmt.value

            if target_name == "rank_order" and isinstance(value_node, ast.Constant) and isinstance(value_node.value, str):
                if stmt.end_lineno is None or stmt.end_col_offset is None or stmt.lineno != stmt.end_lineno:
                    return code_python
                lineno = stmt.lineno - 1
                line = lines[lineno]
                prefix = line[:stmt.col_offset]
                suffix = line[stmt.end_col_offset:]
                lines[lineno] = prefix + f'rank_order = "{rank_order}"' + suffix
                return "".join(lines)

            if target_name in {"category", "min_sample", "incremental", "supports_career"}:
                insert_at = max(insert_at, getattr(stmt, "end_lineno", stmt.lineno))
                indent = " " * stmt.col_offset
            elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                insert_at = min(insert_at, stmt.lineno - 1 if insert_at == node.lineno else insert_at)
                break

        lines.insert(insert_at, f'{indent}rank_order = "{rank_order}"\n')
        return "".join(lines)

    return code_python


def _normalize_code_metric_season_types(code_python: str, season_types=None) -> str:
    from metrics.framework.base import normalize_metric_season_types

    if season_types is None:
        return code_python
    normalized_types = normalize_metric_season_types(season_types)

    try:
        tree = ast.parse(code_python)
    except SyntaxError:
        return code_python

    lines = code_python.splitlines(keepends=True)
    literal = repr(tuple(normalized_types))
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        if not any(_is_metric_definition_base(base) for base in node.bases):
            continue

        indent = " " * ((node.body[0].col_offset if node.body else node.col_offset + 4))
        insert_at = node.lineno

        for stmt in node.body:
            target_name = None
            value_node = None
            if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                target_name = stmt.targets[0].id
                value_node = stmt.value
            elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                target_name = stmt.target.id
                value_node = stmt.value

            if target_name == "season_types" and isinstance(value_node, (ast.Constant, ast.Tuple, ast.List, ast.Set)):
                if stmt.end_lineno is None or stmt.end_col_offset is None or stmt.lineno != stmt.end_lineno:
                    return code_python
                lineno = stmt.lineno - 1
                line = lines[lineno]
                prefix = line[:stmt.col_offset]
                suffix = line[stmt.end_col_offset:]
                lines[lineno] = prefix + f"season_types = {literal}" + suffix
                return "".join(lines)

            if target_name in {"category", "min_sample", "incremental", "supports_career", "rank_order"}:
                insert_at = max(insert_at, getattr(stmt, "end_lineno", stmt.lineno))
                indent = " " * stmt.col_offset
            elif isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                insert_at = min(insert_at, stmt.lineno - 1 if insert_at == node.lineno else insert_at)
                break

        lines.insert(insert_at, f"{indent}season_types = {literal}\n")
        return "".join(lines)

    return code_python


def _code_metric_metadata_from_code(
    code_python: str,
    *,
    expected_key: str | None = None,
    rank_order_override: str | None = None,
    season_types_override=None,
) -> dict:
    from metrics.framework.code_optimizer import optimize_metric_code
    from metrics.framework.runtime import load_code_metric

    normalized_code = _normalize_code_metric_key(code_python, expected_key)
    normalized_code = _normalize_code_metric_rank_order(normalized_code, rank_order_override)
    normalized_code = _normalize_code_metric_season_types(normalized_code, season_types_override)
    normalized_code = optimize_metric_code(normalized_code)
    metric = load_code_metric(normalized_code)
    metadata = {
        "key": metric.key,
        "name": metric.name,
        "name_zh": getattr(metric, "name_zh", "") or "",
        "description": metric.description,
        "description_zh": getattr(metric, "description_zh", "") or "",
        "scope": metric.scope,
        "category": getattr(metric, "category", "") or "",
        "min_sample": int(getattr(metric, "min_sample", 1) or 1),
        "career_min_sample": getattr(metric, "career_min_sample", None),
        "career_name_suffix": getattr(metric, "career_name_suffix", " (Career)"),
        "supports_career": bool(getattr(metric, "supports_career", False)),
        "career": bool(getattr(metric, "career", False)),
        "trigger": getattr(metric, "trigger", "game"),
        "incremental": bool(getattr(metric, "incremental", True)),
        "rank_order": getattr(metric, "rank_order", "desc"),
        "season_types": list(getattr(metric, "season_types", ("regular", "playoffs", "playin")) or ()),
        "context_label_template": getattr(metric, "context_label_template", None),
        "max_results_per_season": getattr(metric, "max_results_per_season", None),
        "code_python": normalized_code,
    }
    if expected_key and metadata["key"] != expected_key:
        raise ValueError(
            f"Code metric key {metadata['key']!r} must match metric key {expected_key!r}."
        )
    return metadata


@lru_cache(maxsize=1024)
def _cached_code_metric_metadata_for_catalog(code_python: str, expected_key: str | None) -> tuple[tuple[str, object], ...]:
    metadata = _code_metric_metadata_from_code(code_python, expected_key=expected_key)
    return tuple(metadata.items())


def _safe_code_metric_metadata(row: MetricDefinitionModel) -> dict:
    if row.source_type != "code" or not row.code_python:
        return {}
    try:
        return dict(_cached_code_metric_metadata_for_catalog(row.code_python, row.key))
    except Exception as exc:
        logger.warning("Failed to inspect code metric %s for catalog metadata: %s", row.key, exc)
        return {}


def _db_metric_search_fields(row: MetricDefinitionModel, *, code_metadata: dict | None = None) -> dict:
    details = {
        "group_key": row.group_key,
        "min_sample": int(row.min_sample or 1),
        "expression": row.expression or "",
        "definition_json": "",
        "code_python": _truncate_search_text(row.code_python or "", limit=2400),
    }

    if row.source_type == "rule":
        try:
            definition = json.loads(row.definition_json or "{}")
        except json.JSONDecodeError:
            definition = {}
        time_scope = str(definition.get("time_scope") or "season").strip().lower() if definition else ""
        details.update(
            definition_json=json.dumps(definition, ensure_ascii=True, sort_keys=True) if definition else "",
            time_scope=time_scope or None,
            supports_career=bool(
                definition.get("supports_career")
                or time_scope == "season_and_career"
            ) if definition else False,
            career=time_scope == "career",
            trigger=str(definition.get("trigger") or "game").strip().lower() if definition else "game",
            incremental=False,
            rank_order=str(definition.get("rank_order") or "desc").strip().lower() if definition else "desc",
            career_min_sample=definition.get("career_min_sample") if definition else None,
            career_name_suffix=str(definition.get("career_name_suffix") or " (Career)") if definition else " (Career)",
        )
        return details

    if code_metadata:
        row_name = getattr(row, "name", "") or ""
        row_name_zh = getattr(row, "name_zh", "") or ""
        row_description = getattr(row, "description", "") or ""
        row_description_zh = getattr(row, "description_zh", "") or ""
        details.update(
            min_sample=code_metadata["min_sample"],
            career_min_sample=code_metadata["career_min_sample"],
            supports_career=code_metadata["supports_career"],
            career=code_metadata["career"],
            trigger=code_metadata.get("trigger", "game"),
            incremental=code_metadata["incremental"],
            rank_order=code_metadata["rank_order"],
            season_types=code_metadata.get("season_types", ["regular", "playoffs", "playin"]),
            # Code metadata reflects the actual runtime metric behavior and is
            # the best source when DB display fields go stale. Preserve DB
            # localization only when the code metadata does not provide it.
            name=code_metadata.get("name") or row_name,
            name_zh=code_metadata.get("name_zh", "") or row_name_zh,
            description=code_metadata.get("description") or row_description,
            description_zh=code_metadata.get("description_zh", "") or row_description_zh,
            scope=code_metadata["scope"],
            category=code_metadata["category"],
            career_name_suffix=code_metadata.get("career_name_suffix", " (Career)"),
        )
    return details


def _metric_family_rows(session, row: MetricDefinitionModel) -> list[MetricDefinitionModel]:
    family_key = getattr(row, "family_key", None) or getattr(row, "key", None)
    if not family_key:
        return [row]
    if not getattr(row, "managed_family", False):
        return [row]
    return (
        session.query(MetricDefinitionModel)
        .filter(MetricDefinitionModel.family_key == family_key)
        .order_by(MetricDefinitionModel.variant.asc(), MetricDefinitionModel.created_at.asc(), MetricDefinitionModel.id.asc())
        .all()
    )


def _metric_family_base_row(session, row: MetricDefinitionModel) -> MetricDefinitionModel:
    if getattr(row, "managed_family", False) and getattr(row, "variant", FAMILY_VARIANT_SEASON) == FAMILY_VARIANT_CAREER:
        row_key = getattr(row, "key", "")
        family_key = getattr(row, "family_key", None) or family_base_key(row_key)
        base_row = (
            session.query(MetricDefinitionModel)
            .filter(
                MetricDefinitionModel.family_key == family_key,
                MetricDefinitionModel.variant == FAMILY_VARIANT_SEASON,
            )
            .first()
        )
        if base_row is not None:
            return base_row
    return row


def _metric_supports_career(
    source_type: str,
    *,
    scope: str,
    code_metadata: dict | None = None,
    definition: dict | None = None,
) -> tuple[bool, bool, int | None]:
    if source_type == "code":
        supports = bool(code_metadata and code_metadata["supports_career"]) and scope != "game" and not code_metadata["career"]
        career_only = bool(code_metadata and code_metadata["career"])
        career_min_sample = int(code_metadata["career_min_sample"]) if code_metadata and code_metadata["career_min_sample"] is not None else None
        return supports, career_only, career_min_sample

    supports = rule_supports_career(definition, scope)
    career_only = rule_is_career_variant(definition)
    career_min_sample = None
    if definition and definition.get("career_min_sample") is not None:
        career_min_sample = int(definition["career_min_sample"])
    return supports, career_only, career_min_sample


def _sync_metric_family(
    session,
    base_row: MetricDefinitionModel,
    *,
    source_type: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    scope: str,
    category: str,
    group_key: str | None,
    expression: str | None,
    min_sample: int,
    code_python: str | None,
    definition: dict | None,
    code_metadata: dict | None,
    now,
) -> None:
    supports_career, career_only, career_min_sample = _metric_supports_career(
        source_type,
        scope=scope,
        code_metadata=code_metadata,
        definition=definition,
    )

    base_row.family_key = base_row.key
    base_row.variant = FAMILY_VARIANT_CAREER if career_only else FAMILY_VARIANT_SEASON
    base_row.base_metric_key = None
    base_row.managed_family = bool(supports_career and not career_only)
    base_row.name = name
    base_row.name_zh = name_zh or None
    base_row.description = description
    base_row.description_zh = description_zh or None
    base_row.scope = scope
    base_row.category = category
    base_row.group_key = group_key
    base_row.source_type = source_type
    base_row.expression = expression or ""
    base_row.min_sample = int(min_sample or 1)
    base_row.max_results_per_season = (code_metadata or {}).get("max_results_per_season")
    base_row.updated_at = now

    if source_type == "code":
        base_row.code_python = code_python or ""
        base_row.definition_json = None
        base_row.context_label_template = (code_metadata or {}).get("context_label_template")
    else:
        base_row.definition_json = json.dumps(definition or {})
        base_row.code_python = None
        base_row.context_label_template = None

    existing_sibling = (
        session.query(MetricDefinitionModel)
        .filter(MetricDefinitionModel.key == family_career_key(base_row.key))
        .first()
    )

    if supports_career and not career_only:
        career_suffix = (
            (code_metadata or {}).get("career_name_suffix")
            if source_type == "code"
            else str((definition or {}).get("career_name_suffix") or " (Career)")
        ) or " (Career)"
        career_name = derive_career_name(name, career_suffix)
        career_description = derive_career_description(description)
        career_name_zh = f"{name_zh}（生涯）" if name_zh else ""
        career_description_zh = f"生涯{description_zh}" if description_zh else ""
        career_min_sample_value = derive_career_min_sample(min_sample, career_min_sample)

        if existing_sibling is None:
            existing_sibling = MetricDefinitionModel(
                key=family_career_key(base_row.key),
                created_at=base_row.created_at or now,
            )
            session.add(existing_sibling)
        elif existing_sibling.id != base_row.id and getattr(existing_sibling, "family_key", None) not in {None, base_row.key}:
            raise ValueError(f"Key '{existing_sibling.key}' already exists")

        existing_sibling.family_key = base_row.key
        existing_sibling.variant = FAMILY_VARIANT_CAREER
        existing_sibling.base_metric_key = base_row.key
        existing_sibling.managed_family = True
        existing_sibling.name = career_name
        existing_sibling.name_zh = career_name_zh or None
        existing_sibling.description = career_description
        existing_sibling.description_zh = career_description_zh or None
        existing_sibling.scope = scope
        existing_sibling.category = category
        existing_sibling.group_key = group_key
        existing_sibling.source_type = source_type
        existing_sibling.status = base_row.status
        existing_sibling.expression = expression or ""
        existing_sibling.min_sample = career_min_sample_value
        existing_sibling.updated_at = now
        if source_type == "code":
            existing_sibling.code_python = build_career_code_variant(
                code_python or "",
                base_key=base_row.key,
                name=career_name,
                description=career_description,
                min_sample=career_min_sample_value,
            )
            existing_sibling.definition_json = None
            existing_sibling.context_label_template = base_row.context_label_template
        else:
            existing_sibling.definition_json = json.dumps(build_career_rule_definition(definition or {}))
            existing_sibling.code_python = None
            existing_sibling.context_label_template = None
    elif existing_sibling is not None and getattr(existing_sibling, "managed_family", False):
        existing_sibling.status = "archived"
        existing_sibling.updated_at = now

    # Refresh search embeddings for any row whose searchable text just changed.
    # Errors are swallowed so a missing OPENAI_API_KEY (e.g. in tests) does not
    # block metric writes — search will fall back to its full-LLM path.
    try:
        from metrics.framework.search import update_metric_embedding

        update_metric_embedding(session, base_row)
        if existing_sibling is not None and existing_sibling.status != "archived":
            update_metric_embedding(session, existing_sibling)
    except Exception:
        logger.exception("Failed to update metric embedding for %s", base_row.key)


def _related_metric_links(session, metric_key: str, runtime_metric, db_metric) -> list[dict]:
    """Return related metric links for the current metric family.

    Families are resolved from either:
    - `group_key` for DB-defined metric variants (regular/combined/etc.)
    - season/career siblings for metrics that support a `_career` variant
    """
    from metrics.framework.runtime import get_metric as _get_metric

    current_key = metric_key
    base_key = family_base_key(metric_key)
    family_key = (
        getattr(runtime_metric, "group_key", None)
        or getattr(db_metric, "group_key", None)
    )

    candidate_keys: list[str] = []
    seen: set[str] = set()

    def _add(key: str) -> None:
        if key and key not in seen:
            seen.add(key)
            candidate_keys.append(key)

    if family_key:
        rows = (
            session.query(MetricDefinitionModel.key)
            .filter(
                MetricDefinitionModel.group_key == family_key,
                MetricDefinitionModel.status != "archived",
            )
            .order_by(MetricDefinitionModel.created_at.asc(), MetricDefinitionModel.key.asc())
            .all()
        )
        for row in rows:
            _add(row.key)

    if _get_metric(base_key, session=session) is not None:
        _add(base_key)

    links = []
    for key in candidate_keys:
        related = _get_metric(key, session=session)
        if related is None:
            continue
        links.append(
            {
                "metric_key": key,
                "label": related.name,
                "active": key == current_key,
            }
        )

    return links if len(links) > 1 else []


_METRIC_RESULT_COUNTS_TTL_SECONDS = 300
_metric_result_counts_cache: tuple[float, dict[str, int]] | None = None
_metric_result_counts_lock = threading.Lock()


def _metric_result_counts(session) -> dict[str, int]:
    """Return {metric_key: row_count} grouped over the full MetricResult table.

    Uses a process-wide TTL cache because the underlying GROUP BY scans
    ~15M rows and runs in ~4 seconds even on a covering index. Counts
    drive UI badges only, so a few minutes of staleness is fine. The
    full-table GROUP BY is intentional — filtering with IN(<all keys>)
    runs ~4x slower because the optimizer falls back to per-key range
    scans.
    """
    global _metric_result_counts_cache
    now = time.monotonic()
    cached = _metric_result_counts_cache
    if cached is not None and now - cached[0] < _METRIC_RESULT_COUNTS_TTL_SECONDS:
        return cached[1]
    with _metric_result_counts_lock:
        cached = _metric_result_counts_cache
        if cached is not None and time.monotonic() - cached[0] < _METRIC_RESULT_COUNTS_TTL_SECONDS:
            return cached[1]
        counts = {
            row.metric_key: row.count
            for row in session.query(
                MetricResultModel.metric_key,
                func.count(MetricResultModel.id).label("count"),
            )
            .group_by(MetricResultModel.metric_key)
            .all()
        }
        _metric_result_counts_cache = (time.monotonic(), counts)
    return counts


def _catalog_metrics(
    session,
    scope_filter: str = "",
    status_filter: str = "",
    current_user_id: str | None = None,
    *,
    include_result_counts: bool = True,
) -> list[dict]:
    db_q = _catalog_metric_base_query(
        session,
        scope_filter=scope_filter,
        status_filter=status_filter,
    )
    all_defs = db_q.order_by(MetricDefinitionModel.created_at.desc()).all()
    existing_keys = {m.key for m in all_defs}
    count_keys = set(existing_keys)
    for m in all_defs:
        if m.status == "published":
            count_keys.add(f"{m.key}_career")

    counts = {}
    if include_result_counts and count_keys:
        counts = _metric_result_counts(session)

    db_metrics = []
    for m in all_defs:
        db_metrics.extend(
            _catalog_metric_entries_for_row(
                m,
                existing_keys=existing_keys,
                counts=counts,
                current_user_id=current_user_id,
            )
        )
    # Own metrics first, then by original order
    db_metrics.sort(key=lambda d: (not d["is_mine"],))
    return db_metrics


def _catalog_metric_base_query(
    session,
    scope_filter: str = "",
    status_filter: str = "",
):
    db_q = session.query(MetricDefinitionModel).filter(
        MetricDefinitionModel.status != "archived"
    )
    if not status_filter:
        db_q = db_q.filter(MetricDefinitionModel.status != "draft")
        if not is_admin():
            db_q = db_q.filter(MetricDefinitionModel.status != "disabled")
    if scope_filter:
        if scope_filter == "player":
            db_q = db_q.filter(MetricDefinitionModel.scope.in_(["player", "player_franchise"]))
        else:
            db_q = db_q.filter(MetricDefinitionModel.scope == scope_filter)
    if status_filter:
        db_q = db_q.filter(MetricDefinitionModel.status == status_filter)
    return db_q


def _catalog_metric_ordered_query(db_q, current_user_id: str | None = None):
    if current_user_id:
        mine_first = case((MetricDefinitionModel.created_by_user_id == current_user_id, 0), else_=1)
        return db_q.order_by(mine_first.asc(), MetricDefinitionModel.created_at.desc())
    return db_q.order_by(MetricDefinitionModel.created_at.desc())


def _catalog_metric_entries_for_row(
    row,
    *,
    existing_keys: set[str],
    counts: dict[str, int],
    current_user_id: str | None = None,
) -> list[dict]:
    code_metadata = _safe_code_metric_metadata(row)
    search_fields = _db_metric_search_fields(row, code_metadata=code_metadata)
    is_mine = bool(current_user_id and row.created_by_user_id == current_user_id)
    entries = [
        {
            "key": row.key,
            "name": _localized_metric_name(search_fields.get("name", row.name), search_fields.get("name_zh", getattr(row, "name_zh", ""))),
            "description": _localized_metric_description(search_fields.get("description", row.description), search_fields.get("description_zh", getattr(row, "description_zh", ""))),
            "scope": search_fields.get("scope", row.scope),
            "category": search_fields.get("category", row.category or ""),
            "status": row.status,
            "source_type": row.source_type,
            "result_count": counts.get(row.key, 0),
            "is_mine": is_mine,
            **{k: v for k, v in search_fields.items() if k not in ("name", "description")},
        }
    ]
    entries.extend(
        _virtual_window_catalog_metrics(
            row,
            search_fields=search_fields,
            existing_keys=existing_keys,
            counts=counts,
            is_mine=is_mine,
        )
    )
    return entries


def _catalog_eligible_window_types(row, *, search_fields: dict) -> list[str]:
    if (
        row.status != "published"
        or search_fields.get("career")
        or not search_fields.get("supports_career")
    ):
        return []
    scope = search_fields.get("scope", row.scope)
    if scope in ("game", "season"):
        return []
    window_types = ["career"]
    trigger = str(search_fields.get("trigger") or "game").strip().lower()
    if trigger == "season":
        window_types.extend(["last5", "last3"])
    return window_types


def _catalog_has_virtual_career_metric(
    row,
    *,
    search_fields: dict,
    existing_keys: set[str],
) -> bool:
    if "career" not in _catalog_eligible_window_types(row, search_fields=search_fields):
        return False
    return family_window_key(row.key, "career") not in existing_keys


def _virtual_window_catalog_metrics(
    row,
    *,
    search_fields: dict,
    existing_keys: set[str],
    counts: dict[str, int],
    is_mine: bool,
) -> list[dict]:
    eligible = _catalog_eligible_window_types(row, search_fields=search_fields)
    if not eligible:
        return []

    base_name = search_fields.get("name", row.name)
    base_description = search_fields.get("description", row.description)
    base_name_zh = search_fields.get("name_zh", getattr(row, "name_zh", "")) or ""
    base_description_zh = search_fields.get("description_zh", getattr(row, "description_zh", "")) or ""
    career_suffix = str(search_fields.get("career_name_suffix") or " (Career)")
    min_sample = int(search_fields.get("min_sample", row.min_sample or 1) or 1)
    career_min_sample = search_fields.get("career_min_sample")

    _WINDOW_ZH_SUFFIX = {
        "career": "（生涯）",
        "last3": "（近 3 季）",
        "last5": "（近 5 季）",
    }
    _WINDOW_ZH_DESC_PREFIX = {
        "career": "生涯",
        "last3": "近 3 季",
        "last5": "近 5 季",
    }

    entries: list[dict] = []
    for window_type in eligible:
        window_key = family_window_key(row.key, window_type)
        if window_key in existing_keys:
            continue
        name_suffix = career_suffix if window_type == "career" else None
        zh_suffix = _WINDOW_ZH_SUFFIX[window_type]
        zh_desc_prefix = _WINDOW_ZH_DESC_PREFIX[window_type]
        entries.append(
            {
                "key": window_key,
                "name": _localized_metric_name(
                    derive_window_name(base_name, window_type, suffix=name_suffix),
                    f"{base_name_zh}{zh_suffix}" if base_name_zh else "",
                ),
                "name_zh": f"{base_name_zh}{zh_suffix}" if base_name_zh else "",
                "description": _localized_metric_description(
                    derive_window_description(base_description, window_type),
                    f"{zh_desc_prefix}{base_description_zh}" if base_description_zh else "",
                ),
                "description_zh": f"{zh_desc_prefix}{base_description_zh}" if base_description_zh else "",
                "scope": search_fields.get("scope", row.scope),
                "category": search_fields.get("category", row.category or ""),
                "status": "published",
                "source_type": row.source_type,
                "result_count": counts.get(window_key, 0),
                "is_mine": is_mine,
                "group_key": search_fields.get("group_key"),
                "min_sample": derive_window_min_sample(
                    min_sample,
                    window_type,
                    career_min_sample=career_min_sample,
                ),
                "expression": row.expression or "",
                "definition_json": search_fields.get("definition_json", ""),
                "code_python": search_fields.get("code_python", ""),
                "supports_career": bool(search_fields.get("supports_career")),
                "career": True,
                "window_type": window_type,
                "incremental": bool(search_fields.get("incremental", False)),
                "rank_order": search_fields.get("rank_order", "desc"),
                "career_min_sample": career_min_sample,
                "time_scope": "career",
                "base_metric_key": row.key,
            }
        )
    return entries


def _virtual_career_catalog_metric(
    row,
    *,
    search_fields: dict,
    existing_keys: set[str],
    counts: dict[str, int],
    is_mine: bool,
) -> dict | None:
    for entry in _virtual_window_catalog_metrics(
        row,
        search_fields=search_fields,
        existing_keys=existing_keys,
        counts=counts,
        is_mine=is_mine,
    ):
        if entry.get("window_type") == "career":
            return entry
    return None


def _catalog_metrics_total(
    session,
    scope_filter: str = "",
    status_filter: str = "",
) -> int:
    rows = _catalog_metric_base_query(
        session,
        scope_filter=scope_filter,
        status_filter=status_filter,
    ).all()
    existing_keys = {row.key for row in rows}
    total = 0
    for row in rows:
        code_metadata = _safe_code_metric_metadata(row)
        search_fields = _db_metric_search_fields(row, code_metadata=code_metadata)
        total += 1
        for window_type in _catalog_eligible_window_types(row, search_fields=search_fields):
            if family_window_key(row.key, window_type) not in existing_keys:
                total += 1
    return total


def _catalog_metrics_page(
    session,
    scope_filter: str = "",
    status_filter: str = "",
    current_user_id: str | None = None,
    *,
    offset: int = 0,
    limit: int | None = None,
) -> tuple[list[dict], bool]:
    limit = limit or _METRICS_CATALOG_PAGE_SIZE
    base_query = _catalog_metric_base_query(
        session,
        scope_filter=scope_filter,
        status_filter=status_filter,
    )
    ordered_query = _catalog_metric_ordered_query(base_query, current_user_id=current_user_id)
    existing_keys = {
        key for (key,) in base_query.with_entities(MetricDefinitionModel.key).all()
    }
    batch_size = max(limit * 2, 64)
    rows_offset = 0
    catalog_index = 0
    metrics_page: list[dict] = []

    while True:
        rows = ordered_query.offset(rows_offset).limit(batch_size).all()
        if not rows:
            return metrics_page, False
        rows_offset += len(rows)

        for row in rows:
            entries = _catalog_metric_entries_for_row(
                row,
                existing_keys=existing_keys,
                counts={},
                current_user_id=current_user_id,
            )
            for entry in entries:
                if catalog_index >= offset:
                    if len(metrics_page) < limit:
                        metrics_page.append(entry)
                    else:
                        return metrics_page, True
                catalog_index += 1


def _catalog_top3(session, metrics_list: list[dict]) -> dict[str, list[dict]]:
    """Bulk-load top 3 results per metric for the current season.

    Returns {metric_key: [{entity_id, label, value_str, headshot_url|logo_url}, ...]}.
    """
    from types import SimpleNamespace

    # Determine current regular season
    all_seasons = sorted(
        [r[0] for r in session.query(Game.season).distinct().all() if r[0] and str(r[0]).startswith("2")]
    )
    current_season = max(all_seasons) if all_seasons else None
    if current_season is None:
        return {}

    # Build map of metric_key → (scope, rank_order, window_type)
    # window_type is "career"/"last3"/"last5" for pseudo-season metrics, None for concrete-season metrics.
    metric_info: dict[str, tuple[str, str, str | None]] = {}
    for m in metrics_list:
        if m.get("status") != "published":
            continue
        window_type = m.get("window_type") or (window_type_from_key(m["key"]) if m.get("career") else None)
        if m.get("career") and window_type is None:
            window_type = "career"
        metric_info[m["key"]] = (m["scope"], m.get("rank_order", "desc"), window_type)

    if not metric_info:
        return {}

    season_keys = [k for k, v in metric_info.items() if v[2] is None and v[0] != "season"]
    season_scope_keys = [k for k, v in metric_info.items() if v[2] is None and v[0] == "season"]
    window_keys_by_type: dict[str, list[str]] = {"career": [], "last3": [], "last5": []}
    for k, v in metric_info.items():
        if v[2] in window_keys_by_type:
            window_keys_by_type[v[2]].append(k)

    def _fetch_top_rows(metric_keys: list[str], *, season_value: str | None = None, season_prefix: str | None = None, rank_order: str) -> list[SimpleNamespace]:
        if not metric_keys:
            return []
        order_columns = [
            MetricResultModel.value_num.desc() if rank_order == "desc" else MetricResultModel.value_num.asc(),
            MetricResultModel.entity_id.asc(),
        ]
        base_q = (
            session.query(
                MetricResultModel.metric_key.label("metric_key"),
                MetricResultModel.entity_id.label("entity_id"),
                MetricResultModel.value_num.label("value_num"),
                MetricResultModel.value_str.label("value_str"),
                func.row_number()
                .over(
                    partition_by=MetricResultModel.metric_key,
                    order_by=order_columns,
                )
                .label("row_num"),
            )
            .filter(
                MetricResultModel.metric_key.in_(metric_keys),
                MetricResultModel.value_num.isnot(None),
            )
        )
        if season_value is not None:
            base_q = base_q.filter(MetricResultModel.season == season_value)
        elif season_prefix is not None:
            base_q = base_q.filter(MetricResultModel.season.like(f"{season_prefix}%"))
        ranked = base_q.subquery()
        return [
            SimpleNamespace(
                metric_key=row.metric_key,
                entity_id=row.entity_id,
                value_num=row.value_num,
                value_str=row.value_str,
            )
            for row in session.query(
                ranked.c.metric_key,
                ranked.c.entity_id,
                ranked.c.value_num,
                ranked.c.value_str,
            )
            .filter(ranked.c.row_num <= 3)
            .all()
        ]

    # Bulk query only the top 3 rows per metric instead of loading every result row.
    rows: list[SimpleNamespace] = []

    def _split_by_order(keys: list[str]) -> tuple[list[str], list[str]]:
        desc = [k for k in keys if metric_info.get(k, ("", "desc", None))[1] == "desc"]
        asc = [k for k in keys if metric_info.get(k, ("", "desc", None))[1] != "desc"]
        return desc, asc

    season_desc, season_asc = _split_by_order(season_keys)
    season_scope_desc, season_scope_asc = _split_by_order(season_scope_keys)
    rows.extend(_fetch_top_rows(season_desc, season_value=current_season, rank_order="desc"))
    rows.extend(_fetch_top_rows(season_asc, season_value=current_season, rank_order="asc"))
    rows.extend(_fetch_top_rows(season_scope_desc, season_prefix="2", rank_order="desc"))
    rows.extend(_fetch_top_rows(season_scope_asc, season_prefix="2", rank_order="asc"))
    _WINDOW_PSEUDO_SEASON = {
        "career": "all_regular",
        "last3": "last3_regular",
        "last5": "last5_regular",
    }
    for window_type, window_keys in window_keys_by_type.items():
        pseudo_season = _WINDOW_PSEUDO_SEASON[window_type]
        window_desc, window_asc = _split_by_order(window_keys)
        rows.extend(_fetch_top_rows(window_desc, season_value=pseudo_season, rank_order="desc"))
        rows.extend(_fetch_top_rows(window_asc, season_value=pseudo_season, rank_order="asc"))

    # Group by metric_key, sort, take top 3
    from collections import defaultdict
    by_metric: dict[str, list] = defaultdict(list)
    for r in rows:
        by_metric[r.metric_key].append(r)

    # Collect all entity IDs we need to resolve
    player_ids: set[str] = set()
    team_ids: set[str] = set()
    game_entity_ids: set[str] = set()
    game_base_ids: set[str] = set()

    top3_raw: dict[str, list] = {}
    for key, results in by_metric.items():
        scope, rank_order, _ = metric_info.get(key, ("player", "desc", None))
        reverse = rank_order == "desc"
        results.sort(key=lambda r: r.value_num if r.value_num is not None else 0, reverse=reverse)
        top = results[:3]
        top3_raw[key] = top
        for r in top:
            if scope in ("player", "player_franchise"):
                eid = r.entity_id.split(":")[0] if ":" in (r.entity_id or "") else r.entity_id
                if eid:
                    player_ids.add(eid)
            elif scope == "team":
                if r.entity_id:
                    team_ids.add(r.entity_id)
            elif scope == "season":
                pass  # season entities use _season_label(), no DB lookup needed
            elif scope == "game" and r.entity_id:
                game_entity_ids.add(r.entity_id)
                base_game_id = r.entity_id.split(":")[0] if ":" in r.entity_id else r.entity_id
                if base_game_id:
                    game_base_ids.add(base_game_id)

    # Bulk resolve names
    player_names = {}
    if player_ids:
        for p in session.query(Player.player_id, Player.full_name, Player.full_name_zh).filter(Player.player_id.in_(player_ids)).all():
            player_names[p.player_id] = p.full_name_zh if _is_zh() and getattr(p, "full_name_zh", None) else p.full_name

    team_labels = {}
    if team_ids:
        for t in session.query(Team.team_id, Team.abbr, Team.full_name, Team.full_name_zh).filter(Team.team_id.in_(team_ids)).all():
            team_labels[t.team_id] = t.full_name_zh if _is_zh() and getattr(t, "full_name_zh", None) else (t.full_name or t.abbr)

    game_labels = {}
    game_dates = {}
    if game_entity_ids:
        game_labels, game_dates = _resolve_game_entity_names(session, sorted(game_entity_ids))
    game_meta = {}
    if game_base_ids:
        for game in session.query(Game.game_id, Game.home_team_id, Game.road_team_id).filter(Game.game_id.in_(game_base_ids)).all():
            game_meta[str(game.game_id)] = {
                "home_team_id": str(game.home_team_id) if game.home_team_id else None,
                "road_team_id": str(game.road_team_id) if game.road_team_id else None,
            }

    # Build final output
    result: dict[str, list[dict]] = {}
    for key, top in top3_raw.items():
        scope = metric_info[key][0]
        entries = []
        for r in top:
            eid = r.entity_id or ""
            if scope in ("player", "player_franchise"):
                pid = eid.split(":")[0] if ":" in eid else eid
                label = player_names.get(pid, eid)
                entry = {
                    "entity_id": pid,
                    "label": label,
                    "value_str": r.value_str or (f"{r.value_num:.1f}" if r.value_num is not None else ""),
                    "headshot_url": f"https://cdn.nba.com/headshots/nba/latest/260x190/{pid}.png",
                }
            elif scope == "team":
                label = team_labels.get(eid, eid)
                entry = {
                    "entity_id": eid,
                    "label": label,
                    "value_str": r.value_str or (f"{r.value_num:.1f}" if r.value_num is not None else ""),
                    "logo_url": f"https://cdn.nba.com/logos/nba/{eid}/global/L/logo.svg",
                }
            elif scope == "season":
                label = _season_label(eid)
                entry = {
                    "entity_id": eid,
                    "label": label,
                    "value_str": r.value_str or (f"{r.value_num:.1f}" if r.value_num is not None else ""),
                }
            else:
                label = game_labels.get(eid, eid)
                game_date = game_dates.get(eid)
                if game_date:
                    label = f"{label} · {game_date}"
                base_game_id = eid.split(":")[0] if ":" in eid else eid
                meta = game_meta.get(base_game_id, {})
                entry = {
                    "entity_id": eid,
                    "label": label,
                    "value_str": r.value_str or (f"{r.value_num:.1f}" if r.value_num is not None else ""),
                    "road_logo_url": _team_logo_url(meta.get("road_team_id")),
                    "home_logo_url": _team_logo_url(meta.get("home_team_id")),
                }
            entries.append(entry)
        if entries:
            result[key] = entries

    return result


_METRICS_CATALOG_PAGE_SIZE = 24


def _fmt_minutes(minute: int | None, sec: int | None) -> str:
    if minute is None and sec is None:
        return "-"
    return f"{minute or 0:02d}:{sec or 0:02d}"


def _player_status(stat: PlayerGameStats) -> str:
    comment = (stat.comment or "").strip()
    if comment:
        return comment
    if stat.min is None and stat.sec is None:
        return _t("Did not play", "未出场")
    if stat.starter:
        return _t("Starter", "首发")
    return _t("Played", "出场")


def _fmt_date(d: date | None) -> str:
    if d is None:
        return "-"
    return d.isoformat()


def _fmt_int(v) -> str:
    return str(int(v)) if v is not None else "0"


def _metric_rank_order(session, metric_key: str) -> str:
    from metrics.framework.runtime import get_metric as _get_metric

    metric = _get_metric(metric_key, session=session)
    return getattr(metric, "rank_order", "desc") if metric is not None else "desc"


def _asc_metric_keys(session) -> set[str]:
    from metrics.framework.runtime import get_all_metrics as _get_all_metrics

    return {
        metric.key
        for metric in _get_all_metrics(session=session)
        if getattr(metric, "rank_order", "desc") == "asc"
    }


def _disabled_metric_keys(session) -> set[str]:
    """Return metric keys (including career variants) with status='disabled'."""
    rows = session.query(MetricDefinitionModel.key).filter(
        MetricDefinitionModel.status == "disabled"
    ).all()
    keys = set()
    for (key,) in rows:
        keys.add(key)
        keys.add(key + "_career")
    return keys


def _game_entity_filter(entity_col, game_id: str):
    return or_(entity_col == game_id, entity_col.like(f"{game_id}:%"))


def _resolve_context_label(base_key: str, ctx: dict, db_templates: dict[str, str]) -> str | None:
    """Resolve context label from DB template."""
    template = db_templates.get(base_key)
    if not template:
        return None
    try:
        fmt_ctx = {k: _fmt_int(v) if isinstance(v, (int, float)) else v for k, v in ctx.items()}
        return template.format_map(fmt_ctx)
    except Exception:
        return None


def _load_context_label_templates(session, metric_keys: set[str]) -> dict[str, str]:
    """Load context_label_template from DB for the given metric keys."""
    if not metric_keys:
        return {}
    rows = (
        session.query(
            MetricDefinitionModel.key,
            MetricDefinitionModel.context_label_template,
        )
        .filter(
            MetricDefinitionModel.key.in_(metric_keys),
            MetricDefinitionModel.context_label_template.isnot(None),
        )
        .all()
    )
    return {r.key: r.context_label_template for r in rows}


def _apply_game_metric_tiers(season_metrics: list) -> None:
    """Mark is_hero flag and sort game metric entries by exceptionality in-place.

    Entries must already have all_games_rank/all_games_total (or None).
    Tiers: hero (top 1%) → notable (top 25%) → normal.  Within each tier,
    rarest (lowest ratio) comes first.
    """
    for entry in season_metrics:
        ag_rank = entry["all_games_rank"]
        ag_total = entry["all_games_total"]
        if ag_rank is not None and ag_total:
            entry["is_hero"] = ag_rank / ag_total <= 0.01
        else:
            entry["is_hero"] = entry["total"] > 0 and entry["rank"] / entry["total"] <= 0.01

    def _sort_key(e):
        ag_rank = e["all_games_rank"]
        ag_total = e["all_games_total"]
        if ag_rank is not None and ag_total:
            ratio = ag_rank / ag_total
        elif e["total"]:
            ratio = e["rank"] / e["total"]
        else:
            ratio = 1.0
        tier = 0 if ratio <= 0.01 else (1 if ratio <= 0.25 else 2)
        return (tier, ratio)

    season_metrics.sort(key=_sort_key)


def _game_metric_badge_text(rank: int | None, total: int | None, label: str) -> str | None:
    if rank is None or not total:
        return None
    ratio = rank / total
    if ratio <= 0.25:
        return f"#{rank} {label}"
    return None


def _game_metric_best_ratio(entry: dict) -> float:
    ratios = []
    if entry.get("total"):
        ratios.append(entry["rank"] / entry["total"])
    if entry.get("all_games_rank") is not None and entry.get("all_games_total"):
        ratios.append(entry["all_games_rank"] / entry["all_games_total"])
    return min(ratios) if ratios else 1.0


def _game_metric_best_rank(entry: dict) -> int:
    ranks: list[int] = []
    if entry.get("rank") is not None and entry.get("total"):
        ranks.append(entry["rank"])
    if entry.get("all_games_rank") is not None and entry.get("all_games_total"):
        ranks.append(entry["all_games_rank"])
    return min(ranks) if ranks else 10**9


def _prepare_game_metric_cards(
    season_metrics: list[dict],
    visible_limit: int = 4,
    extra_limit: int = 8,
) -> tuple[list[dict], list[dict]]:
    if not season_metrics:
        return [], []

    cards: list[dict] = []
    for entry in season_metrics:
        card = dict(entry)
        card["best_ratio"] = _game_metric_best_ratio(card)
        card["best_rank"] = _game_metric_best_rank(card)
        card["is_featured"] = card["best_rank"] <= 10 or card["best_ratio"] <= 0.01
        card["is_notable"] = card["is_featured"] and not card["is_hero"]
        card["season_badge_text"] = _game_metric_badge_text(card["rank"], card["total"], "Season")
        card["all_badge_text"] = _game_metric_badge_text(
            card.get("all_games_rank"),
            card.get("all_games_total"),
            "All",
        )
        cards.append(card)

    cards.sort(key=lambda card: (card["best_ratio"], card["best_rank"], card["metric_key"], card["entity_id"]))

    featured = [card for card in cards if card["is_featured"]]
    if len(featured) > visible_limit:
        return featured, []

    visible_count = max(visible_limit, len(featured))
    visible_cards = cards[:visible_count]
    extra_cards = cards[visible_count:visible_count + extra_limit]
    return visible_cards, extra_cards


def _get_metric_results(session, entity_type: str, entity_id: str, season: str | None = None) -> dict:
    """Fetch metric results for an entity, split into season and alltime lists.

    Returns {"season": [...], "alltime": [...]} each sorted by rank asc (best first).
    Rank and total are derived at query time via SQL window functions.
    """
    import json
    from sqlalchemy import func
    from metrics.framework.base import CAREER_SEASON_PREFIX, WINDOW_SEASONS, is_career_season, season_type_for, window_type_from_season

    _CAREER_TYPE_LABEL = {
        "all_regular": _t("Regular Season", "常规赛"),
        "all_playoffs": _t("Playoffs", "季后赛"),
        "all_playin": _t("Play-In", "附加赛"),
        "last3_regular": _t("Last 3 Regular Seasons", "近 3 个常规赛季"),
        "last3_playoffs": _t("Last 3 Playoff Seasons", "近 3 个季后赛季"),
        "last3_playin": _t("Last 3 Play-In Seasons", "近 3 个附加赛季"),
        "last5_regular": _t("Last 5 Regular Seasons", "近 5 个常规赛季"),
        "last5_playoffs": _t("Last 5 Playoff Seasons", "近 5 个季后赛季"),
        "last5_playin": _t("Last 5 Play-In Seasons", "近 5 个附加赛季"),
    }
    _WINDOW_LABELS = {
        "career": _t("Career", "生涯"),
        "last3": _t("Last 3", "近 3 季"),
        "last5": _t("Last 5", "近 5 季"),
    }

    _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
    _asc_keys = _asc_metric_keys(session)
    _disabled_keys = _disabled_metric_keys(session) if not is_admin() else set()
    scope_label = {"player": "players", "team": "teams", "game": "games"}.get(entity_type, "entities")

    if season:
        pseudo_season_filters = [
            MetricResultModel.season.like(CAREER_SEASON_PREFIX + "%"),
            MetricResultModel.season.like("last3_%"),
            MetricResultModel.season.like("last5_%"),
        ]
        season_filter = or_(MetricResultModel.season == season, *pseudo_season_filters)
    else:
        season_filter = None

    # Step 1: Fetch this entity's own rows first (fast, indexed lookup).
    entity_filters = [
        MetricResultModel.entity_type == entity_type,
        MetricResultModel.value_num.isnot(None),
    ]
    if entity_type == "game":
        entity_filters.append(_game_entity_filter(MetricResultModel.entity_id, entity_id))
    else:
        entity_filters.append(MetricResultModel.entity_id == entity_id)
    if _disabled_keys:
        entity_filters.append(MetricResultModel.metric_key.notin_(_disabled_keys))
    if season_filter is not None:
        entity_filters.append(season_filter)

    entity_rows = (
        session.query(
            MetricResultModel.id,
            MetricResultModel.metric_key,
            MetricResultModel.entity_id,
            MetricResultModel.season,
            MetricResultModel.sub_key,
            MetricResultModel.rank_group,
            MetricResultModel.value_num,
            MetricResultModel.value_str,
            MetricResultModel.context_json,
            MetricResultModel.computed_at,
        )
        .filter(*entity_filters)
        .all()
    )

    if not entity_rows:
        return {"season": [], "alltime": []}

    # Identify split metrics (with sub_key_type) — they need special handling
    # and must NOT go through the per-row peer-rank self-join, which would
    # Cartesian-explode when the split metric has O(players × opponents) rows.
    all_entity_keys = {r.metric_key for r in entity_rows}
    split_base_keys: set[str] = set()
    if all_entity_keys:
        base_lookup = {family_base_key(k) for k in all_entity_keys}
        md_sub_rows = (
            session.query(MetricDefinitionModel.key)
            .filter(
                MetricDefinitionModel.key.in_(base_lookup),
                MetricDefinitionModel.sub_key_type.isnot(None),
            )
            .all()
        )
        split_base_keys = {row.key for row in md_sub_rows}
    split_metric_keys = {
        k for k in all_entity_keys
        if family_base_key(k) in split_base_keys
    }

    # Step 2: Compute rank and total via a self-join — only for non-split rows.
    # For desc-ranked metrics: rank = COUNT(peers with higher value) + 1
    # For asc-ranked metrics: rank = COUNT(peers with lower value) + 1
    non_split_ids = {r.id for r in entity_rows if r.metric_key not in split_metric_keys}
    rank_map: dict[int, tuple[int, int]] = {}
    if non_split_ids:
        MR = MetricResultModel
        e_alias = MR.__table__.alias("e")
        p_alias = MR.__table__.alias("p")

        if _asc_keys:
            better_expr = case(
                (e_alias.c.metric_key.in_(_asc_keys), p_alias.c.value_num < e_alias.c.value_num),
                else_=(p_alias.c.value_num > e_alias.c.value_num),
            )
        else:
            better_expr = p_alias.c.value_num > e_alias.c.value_num

        join_cond = and_(
            p_alias.c.entity_type == e_alias.c.entity_type,
            p_alias.c.metric_key == e_alias.c.metric_key,
            p_alias.c.season == e_alias.c.season,
            func.coalesce(p_alias.c.rank_group, "__none__") == func.coalesce(e_alias.c.rank_group, "__none__"),
            p_alias.c.value_num.isnot(None),
        )

        rank_q = (
            session.query(
                e_alias.c.id,
                func.count(p_alias.c.id).label("total"),
                (func.sum(case((better_expr, 1), else_=0)) + 1).label("rank"),
            )
            .select_from(e_alias)
            .join(p_alias, join_cond)
            .filter(e_alias.c.id.in_(non_split_ids))
            .group_by(e_alias.c.id)
        )
        rank_map = {r.id: (r.rank, r.total) for r in rank_q.all()}

    # Build combined rows with rank/total attached
    rows = []
    for r in entity_rows:
        rank, total = rank_map.get(r.id, (1, 1))
        rows.append(SimpleNamespace(
            id=r.id, metric_key=r.metric_key, entity_id=r.entity_id,
            season=r.season, sub_key=r.sub_key or "", rank_group=r.rank_group,
            value_num=r.value_num, value_str=r.value_str,
            context_json=r.context_json, computed_at=r.computed_at,
            rank=rank, total=total,
        ))
    rows.sort(key=lambda r: r.rank)

    team_map = _team_map(session)

    all_base_keys = {family_base_key(r.metric_key) for r in rows}
    db_templates = _load_context_label_templates(session, all_base_keys)

    # Batch-load all metric names to avoid N+1 queries
    all_metric_keys = {r.metric_key for r in rows}
    metric_name_cache = _batch_metric_names(session, all_metric_keys)

    # Batch-load categories (keyed by base key — career variants share the parent's category).
    metric_category_cache: dict[str, str] = {}
    if all_base_keys:
        for k, cat in (
            session.query(MetricDefinitionModel.key, MetricDefinitionModel.category)
            .filter(MetricDefinitionModel.key.in_(all_base_keys))
            .all()
        ):
            if cat:
                metric_category_cache[k] = cat

    # Batch-load sub_key_type + fill flag + min_sample for split metrics.
    # min_sample > 1 is used as the signal that this is a rate-style metric
    # (needs a meaningful attempt threshold), so placeholder cells show "—"
    # instead of "0" to avoid implying a degenerate 0/0 rate.
    sub_key_type_map: dict[str, str] = {}
    fill_zero_keys: set[str] = set()
    metric_min_sample: dict[str, int] = {}
    metric_sample_note: dict[str, str] = {}
    if split_base_keys:
        md_rows = (
            session.query(
                MetricDefinitionModel.key,
                MetricDefinitionModel.sub_key_type,
                MetricDefinitionModel.fill_missing_sub_keys_with_zero,
                MetricDefinitionModel.min_sample,
                MetricDefinitionModel.description,
                MetricDefinitionModel.description_zh,
            )
            .filter(MetricDefinitionModel.key.in_(split_base_keys))
            .all()
        )
        base_sub_key_types = {k: st for k, st, _, _, _, _ in md_rows if st}
        base_fill_zero = {k for k, _, fz, _, _, _ in md_rows if fz}
        base_min_sample = {k: int(ms or 1) for k, _, _, ms, _, _ in md_rows}
        # Extract the "(min X attempts)" / "(至少 X 次出手)" parenthetical from the
        # localized description so the UI can surface it as a subtitle.
        _paren_re = re.compile(r"[（(]([^)）]+)[）)]")
        base_sample_note: dict[str, str] = {}
        for k, _st, _fz, ms, desc, desc_zh in md_rows:
            if not ms or int(ms) <= 1:
                continue
            localized = _localized_metric_description(desc, desc_zh)
            m = _paren_re.search(localized or "")
            if m:
                base_sample_note[k] = m.group(1).strip()
        for mk in split_metric_keys:
            base = family_base_key(mk)
            if base in base_sub_key_types:
                sub_key_type_map[mk] = base_sub_key_types[base]
            if base in base_fill_zero:
                fill_zero_keys.add(mk)
            metric_min_sample[mk] = base_min_sample.get(base, 1)
            if base in base_sample_note:
                metric_sample_note[mk] = base_sample_note[base]
    sub_key_label_cache: dict[tuple[str, str], dict] = {}
    if sub_key_type_map:
        team_sub_keys = {r.sub_key for r in rows if r.sub_key and sub_key_type_map.get(r.metric_key) == "team"}
        for tid in team_sub_keys:
            team = team_map.get(tid)
            if team:
                sub_key_label_cache[("team", tid)] = {
                    "label": _display_team_name(team) or team.abbr or tid,
                    "abbr": team.abbr,
                    "team_id": tid,
                }
            else:
                sub_key_label_cache[("team", tid)] = {"label": tid, "abbr": None, "team_id": tid}
        player_sub_keys = {r.sub_key for r in rows if r.sub_key and sub_key_type_map.get(r.metric_key) == "player"}
        if player_sub_keys:
            for pid, fn, fn_zh in (
                session.query(Player.player_id, Player.full_name, Player.full_name_zh)
                .filter(Player.player_id.in_(list(player_sub_keys)))
                .all()
            ):
                sub_key_label_cache[("player", str(pid))] = {
                    "label": (fn_zh if _is_zh() and fn_zh else fn) or str(pid),
                    "player_id": str(pid),
                }

    season_metrics = []
    alltime_metrics = []
    # For split metrics (sub_key_type set), collect all splits per (metric_key, season)
    # and later collapse into a single card with a `splits` list.
    split_accum: dict[tuple, dict] = {}
    for r in rows:
        ctx = json.loads(r.context_json) if r.context_json else {}
        rank_group_label = _team_name(team_map, r.rank_group) if r.rank_group else None
        base_key = family_base_key(r.metric_key)
        context_label = _resolve_context_label(base_key, ctx, db_templates)
        rank, total = r.rank, r.total
        is_notable = total > 0 and rank / total <= 0.25
        sub_key_type = sub_key_type_map.get(r.metric_key)

        if sub_key_type and r.sub_key:
            info = sub_key_label_cache.get((sub_key_type, r.sub_key), {"label": r.sub_key})
            split_entry = {
                "sub_key": r.sub_key,
                "sub_key_info": info,
                "value_num": r.value_num,
                "value_str": r.value_str,
                "rank": rank,
                "total": total,
                "is_notable": is_notable,
                "context": ctx,
                "context_label": context_label,
            }
            group_key = (r.metric_key, r.season)
            bucket = split_accum.get(group_key)
            if bucket is None:
                bucket = {
                    "metric_key": r.metric_key,
                    "metric_name": metric_name_cache.get(r.metric_key, r.metric_key.replace("_", " ").title()),
                    "entity_id": r.entity_id,
                    "season": r.season,
                    "rank_group": r.rank_group,
                    "rank_group_label": rank_group_label,
                    "computed_at": r.computed_at,
                    "sub_key_type": sub_key_type,
                    "splits": [],
                    "career_rank": None,
                    "career_total": None,
                    "career_is_notable": False,
                }
                split_accum[group_key] = bucket
            bucket["splits"].append(split_entry)
            continue

        entry = {
            "metric_key": r.metric_key,
            "metric_name": metric_name_cache.get(r.metric_key, r.metric_key.replace("_", " ").title()),
            "category": metric_category_cache.get(family_base_key(r.metric_key), ""),
            "entity_id": r.entity_id,
            "value_num": r.value_num,
            "value_str": r.value_str,
            "rank": rank,
            "total": total,
            "is_notable": is_notable,
            "is_hero": False,
            "context": ctx,
            "context_label": context_label,
            "rank_group": r.rank_group,
            "rank_group_label": rank_group_label,
            "computed_at": r.computed_at,
            "window_type": None,
            # career cross-reference filled in below
            "career_rank": None,
            "career_total": None,
            "career_is_notable": False,
            "career": None,
            "last3": None,
            "last5": None,
        }
        if is_career_season(r.season):
            entry["window_type"] = window_type_from_season(r.season)
            entry["career_type"] = r.season
            entry["career_type_label"] = _CAREER_TYPE_LABEL.get(r.season, "Career")
            entry["window_label"] = _WINDOW_LABELS.get(entry["window_type"], _t("Career", "生涯"))
            alltime_metrics.append(entry)
        else:
            season_metrics.append(entry)

    # For split metrics that opted into fill_missing_sub_keys_with_zero, fill in
    # opponents the player faced but didn't produce any of the measured thing
    # against (e.g. Jokic 0 3PM vs some teams). Rate metrics should leave this
    # flag off because 0 on a rate is ambiguous.
    opponent_universe: dict[tuple, set[str]] = {}
    if split_accum and entity_type == "player" and fill_zero_keys:
        wanted_buckets = {
            (bucket["entity_id"], season_val)
            for (mk, season_val), bucket in split_accum.items()
            if mk in fill_zero_keys
        }
        for eid, season_val in wanted_buckets:
            from metrics.framework.base import career_season_type_code as _career_type, is_career_season as _is_career
            if _is_career(season_val):
                code = _career_type(season_val)
                season_filter = Game.season.like(f"{code}%") if code else None
            else:
                season_filter = Game.season == season_val
            if season_filter is None:
                opponent_universe[(eid, season_val)] = set()
                continue
            q = (
                session.query(
                    case(
                        (PlayerGameStats.team_id == Game.home_team_id, Game.road_team_id),
                        else_=Game.home_team_id,
                    ).label("opp_id")
                )
                .select_from(PlayerGameStats)
                .join(Game, Game.game_id == PlayerGameStats.game_id)
                .filter(
                    PlayerGameStats.player_id == eid,
                    PlayerGameStats.team_id.isnot(None),
                    season_filter,
                )
                .distinct()
            )
            opponent_universe[(eid, season_val)] = {str(row.opp_id) for row in q.all() if row.opp_id}

    # Resolve labels for any opponent ids we haven't seen yet in split rows.
    extra_team_ids = {
        opp_id
        for universe in opponent_universe.values()
        for opp_id in universe
        if ("team", opp_id) not in sub_key_label_cache
    }
    for tid in extra_team_ids:
        team = team_map.get(tid)
        if team:
            sub_key_label_cache[("team", tid)] = {
                "label": _display_team_name(team) or team.abbr or tid,
                "abbr": team.abbr,
                "team_id": tid,
            }
        else:
            sub_key_label_cache[("team", tid)] = {"label": tid, "abbr": None, "team_id": tid}

    # Collapse split-metric buckets: sort splits best-first, promote the top as primary,
    # then append to season_metrics or alltime_metrics.
    for (metric_key, season_val), bucket in split_accum.items():
        splits = bucket["splits"]
        is_asc = metric_key in _asc_keys or family_base_key(metric_key) in _asc_keys
        splits.sort(key=lambda s: (s["value_num"] or 0), reverse=not is_asc)

        # Fill missing opponents with is_placeholder=True zero entries so the UI
        # can display every opponent the player faced, including ones that
        # didn't produce the measured thing. Rate-style metrics (min_sample > 1)
        # use "—" to avoid implying a 0/0 rate; count/max metrics use "0".
        if metric_key in fill_zero_keys:
            universe = opponent_universe.get((bucket["entity_id"], season_val), set())
            existing = {s["sub_key"] for s in splits}
            is_rate = metric_min_sample.get(metric_key, 1) > 1
            ph_value_num = None if is_rate else 0
            ph_value_str = "—" if is_rate else "0"
            for opp_id in sorted(universe - existing):
                info = sub_key_label_cache.get(("team", opp_id), {"label": opp_id, "abbr": None, "team_id": opp_id})
                splits.append({
                    "sub_key": opp_id,
                    "sub_key_info": info,
                    "value_num": ph_value_num,
                    "value_str": ph_value_str,
                    "rank": None,
                    "total": None,
                    "is_notable": False,
                    "is_placeholder": True,
                    "context": {},
                    "context_label": "",
                    "games_meta": [],
                })

        top = splits[0]
        entry = {
            "metric_key": metric_key,
            "metric_name": bucket["metric_name"],
            "category": metric_category_cache.get(family_base_key(metric_key), ""),
            "entity_id": bucket["entity_id"],
            "value_num": top["value_num"],
            "value_str": top["value_str"],
            "rank": top["rank"],
            "total": top["total"],
            "is_notable": top["is_notable"],
            "is_hero": False,
            "context": top["context"],
            "context_label": top["context_label"],
            "rank_group": bucket["rank_group"],
            "rank_group_label": bucket["rank_group_label"],
            "computed_at": bucket["computed_at"],
            "window_type": None,
            "career_rank": None,
            "career_total": None,
            "career_is_notable": False,
            "career": None,
            "last3": None,
            "last5": None,
            "sub_key_type": bucket["sub_key_type"],
            "splits": splits,
            "primary_sub_key_info": top["sub_key_info"],
            "sample_note": metric_sample_note.get(metric_key, ""),
        }
        if is_career_season(season_val):
            entry["window_type"] = window_type_from_season(season_val)
            entry["career_type"] = season_val
            entry["career_type_label"] = _CAREER_TYPE_LABEL.get(season_val, "Career")
            entry["window_label"] = _WINDOW_LABELS.get(entry["window_type"], _t("Career", "生涯"))
            alltime_metrics.append(entry)
        else:
            season_metrics.append(entry)

    # Attach matching window variants to each current-season entry so cards can
    # show season + career/last5/last3 together.
    current_season_type = season_type_for(season)
    matching_window_seasons = {
        window_type: next(
            (
                pseudo_season
                for pseudo_season in pseudo_seasons
                if season_type_for(pseudo_season) == current_season_type
            ),
            None,
        )
        for window_type, pseudo_seasons in WINDOW_SEASONS.items()
    }
    windows_by_base: dict[str, dict[str, dict]] = {}
    for e in alltime_metrics:
        window_type = e.get("window_type")
        if not window_type:
            continue
        if current_season_type and e.get("career_type") != matching_window_seasons.get(window_type):
            continue
        windows_by_base.setdefault(family_base_key(e["metric_key"]), {})[window_type] = e
    for entry in season_metrics:
        entry["all_games_rank"] = None
        entry["all_games_total"] = None
        entry["all_games_is_notable"] = False
        entry["career"] = None
        entry["last3"] = None
        entry["last5"] = None
        window_entries = windows_by_base.get(family_base_key(entry["metric_key"]), {})
        for window_type in ("career", "last5", "last3"):
            entry[window_type] = window_entries.get(window_type)
        if entry["career"]:
            entry["career_rank"] = entry["career"]["rank"]
            entry["career_total"] = entry["career"]["total"]
            entry["career_is_notable"] = entry["career"]["is_notable"]

    # For game-scope metrics, compute a cross-season "All Games" rank
    # (RANK partitioned by metric_key only, no season filter).
    if entity_type == "game" and season_metrics:
        metric_keys = [e["metric_key"] for e in season_metrics]
        season_type_prefix = _season_type_prefix(season)
        _ag_rank_value = case(
            (MetricResultModel.metric_key.in_(_asc_keys), -MetricResultModel.value_num),
            else_=MetricResultModel.value_num,
        )
        ag_query = (
            session.query(
                MetricResultModel.metric_key,
                MetricResultModel.entity_id,
                func.rank().over(
                    partition_by=[MetricResultModel.metric_key, func.coalesce(MetricResultModel.rank_group, "__all__")],
                    order_by=_ag_rank_value.desc(),
                ).label("rank"),
                func.count(MetricResultModel.id).over(
                    partition_by=[MetricResultModel.metric_key, func.coalesce(MetricResultModel.rank_group, "__all__")],
                ).label("total"),
            )
            .filter(
                MetricResultModel.entity_type == "game",
                MetricResultModel.value_num.isnot(None),
                MetricResultModel.metric_key.in_(metric_keys),
            )
        )
        if season_type_prefix:
            ag_query = ag_query.filter(MetricResultModel.season.like(f"{season_type_prefix}%"))
        ag_inner = ag_query.subquery()
        ag_rows = (
            session.query(ag_inner)
            .filter(_game_entity_filter(ag_inner.c.entity_id, entity_id))
            .all()
        )
        ag_by_key = {(r.metric_key, r.entity_id): (r.rank, r.total) for r in ag_rows}
        for entry in season_metrics:
            ag_rank, ag_total = ag_by_key.get((entry["metric_key"], entry["entity_id"]), (None, None))
            if ag_rank is not None:
                entry["all_games_rank"] = ag_rank
                entry["all_games_total"] = ag_total
                entry["all_games_is_notable"] = ag_total > 0 and ag_rank / ag_total <= 0.25

        # Mark hero metrics (top 1% of all games) and sort by exceptionality
        _apply_game_metric_tiers(season_metrics)
        season_metrics, season_extra = _prepare_game_metric_cards(season_metrics)
    else:
        season_extra = []

    # Split off matchup-split entries so the player page can render them in
    # their own section (full-width cards) instead of mixing them into the
    # regular metric grid where they don't fit.
    def _partition_splits(items):
        main = [e for e in items if not e.get("splits")]
        splits = [e for e in items if e.get("splits")]
        return main, splits

    season_metrics, season_splits = _partition_splits(season_metrics)
    alltime_metrics, alltime_splits = _partition_splits(alltime_metrics)

    # Batch-load game metadata for every game_id referenced by split cells,
    # so the template can build a dropdown list (date + opponent + link)
    # without N+1 queries. Attach as split_entry["games_meta"].
    all_split_entries = season_splits + alltime_splits
    if all_split_entries:
        gid_set: set[str] = set()
        for entry in all_split_entries:
            for s in entry.get("splits", []):
                for gid in (s.get("context") or {}).get("qualifying_game_ids") or []:
                    if gid:
                        gid_set.add(str(gid))
        if gid_set:
            game_rows = (
                session.query(
                    Game.game_id,
                    Game.game_date,
                    Game.season,
                    Game.home_team_id,
                    Game.road_team_id,
                    Game.home_team_score,
                    Game.road_team_score,
                    Game.slug,
                )
                .filter(Game.game_id.in_(list(gid_set)))
                .all()
            )
            game_meta_map: dict[str, dict] = {}
            for g in game_rows:
                game_meta_map[str(g.game_id)] = {
                    "game_id": str(g.game_id),
                    "date": g.game_date.isoformat() if g.game_date else "",
                    "season": str(g.season) if g.season else "",
                    "home_team_id": str(g.home_team_id) if g.home_team_id else "",
                    "road_team_id": str(g.road_team_id) if g.road_team_id else "",
                    "home_score": g.home_team_score,
                    "road_score": g.road_team_score,
                    "slug": g.slug,
                }
            for entry in all_split_entries:
                for s in entry.get("splits", []):
                    gids = (s.get("context") or {}).get("qualifying_game_ids") or []
                    metas = []
                    for gid in gids:
                        meta = game_meta_map.get(str(gid))
                        if not meta:
                            continue
                        metas.append(meta)
                    metas.sort(key=lambda m: m.get("date") or "", reverse=True)
                    s["games_meta"] = metas

    return {
        "season": season_metrics,
        "season_extra": season_extra,
        "season_splits": season_splits,
        "alltime": alltime_metrics,
        "alltime_splits": alltime_splits,
    }


def _metric_backfill_component(session, metric_key: str, total_games: int) -> dict:
    from sqlalchemy import desc, func

    # Check MetricComputeRun first — it's cheap and tells us the status.
    latest_compute_run = (
        session.query(MetricComputeRun)
        .filter(MetricComputeRun.metric_key == metric_key)
        .order_by(desc(MetricComputeRun.created_at))
        .first()
    )

    # When a compute run exists, use its target as the authoritative total.
    # For season metrics the passed-in total_games is MetricResult row count (e.g. 418K)
    # but the compute run target is the number of tasks (e.g. 86 seasons).
    if latest_compute_run and latest_compute_run.target_game_count:
        total_games = int(latest_compute_run.target_game_count)

    if latest_compute_run and latest_compute_run.status in ("complete", "reducing"):
        done_games = int(latest_compute_run.target_game_count)
    elif latest_compute_run and latest_compute_run.status == "mapping":
        done_games = int(latest_compute_run.done_game_count or 0)
    elif latest_compute_run and latest_compute_run.status == "failed":
        done_games = int(latest_compute_run.done_game_count or 0)
    else:
        # No compute run — use MetricResult count as a proxy
        done_games = (
            session.query(func.count(MetricResultModel.id))
            .filter(MetricResultModel.metric_key == metric_key)
            .scalar() or 0
        )

    active_games = 0
    latest_run_at = (
        session.query(MetricRunLog.computed_at)
        .filter(MetricRunLog.metric_key == metric_key)
        .order_by(desc(MetricRunLog.computed_at))
        .limit(1)
        .scalar()
    )
    if latest_run_at is None:
        latest_run_at = (
            session.query(MetricResultModel.computed_at)
            .filter(MetricResultModel.metric_key == metric_key)
            .order_by(desc(MetricResultModel.computed_at))
            .limit(1)
            .scalar()
        )

    reduce_done_seasons = None
    reduce_total_seasons = None

    if latest_compute_run and latest_compute_run.status == "failed":
        status = "failed"
    elif total_games and done_games >= total_games:
        # All claims done — but reduce may still be pending/running.
        if latest_compute_run and latest_compute_run.status in ("mapping", "reducing"):
            status = "finalizing"
            # Count reduce progress: seasons with fresh results vs total seasons with deltas.
            reduce_total_seasons = (
                session.query(func.count(func.distinct(MetricRunLog.season)))
                .filter(MetricRunLog.metric_key == metric_key)
                .scalar() or 0
            )
            if latest_compute_run.reduce_enqueued_at:
                reduce_done_seasons = (
                    session.query(func.count(func.distinct(MetricResultModel.season)))
                    .filter(
                        MetricResultModel.metric_key == metric_key,
                        MetricResultModel.computed_at >= latest_compute_run.reduce_enqueued_at,
                    )
                    .scalar() or 0
                )
            else:
                reduce_done_seasons = 0
        else:
            status = "complete"
    elif active_games > 0 or done_games > 0:
        status = "running"
    else:
        status = "not_started"

    # Extract compute run timeline for the UI.
    run_info = None
    if latest_compute_run:
        run_info = {
            "started_at": _format_backfill_timestamp(latest_compute_run.started_at),
            "reduce_enqueued_at": _format_backfill_timestamp(latest_compute_run.reduce_enqueued_at),
            "completed_at": _format_backfill_timestamp(latest_compute_run.completed_at),
            "failed_at": _format_backfill_timestamp(latest_compute_run.failed_at),
            "run_status": latest_compute_run.status,
        }

    return {
        "metric_key": metric_key,
        "status": status,
        "done_games": int(done_games),
        "active_games": int(active_games),
        "pending_games": max(int(total_games) - int(done_games) - int(active_games), 0),
        "total_games": int(total_games),
        "progress_pct": round((int(done_games) / int(total_games) * 100.0), 1) if total_games else 0.0,
        "reduce_done_seasons": reduce_done_seasons,
        "reduce_total_seasons": reduce_total_seasons,
        "run_info": run_info,
        "latest_run_at": latest_run_at,
    }


def _combine_backfill_components(
    metric_def,
    components: list[dict],
) -> dict:
    latest_run_at = max((c["latest_run_at"] for c in components if c["latest_run_at"] is not None), default=None)

    if getattr(metric_def, "status", None) == "draft":
        status = "draft"
    elif components and all(c["status"] == "complete" for c in components):
        status = "complete"
    elif any(c["status"] == "finalizing" for c in components):
        status = "finalizing"
    elif any(c["status"] == "running" for c in components):
        status = "running"
    elif any(c["status"] == "failed" for c in components):
        status = "failed"
    elif getattr(metric_def, "source_type", None) == "rule" and getattr(metric_def, "status", None) == "published":
        if any(c["status"] in {"not_started"} for c in components):
            status = "queued"
        else:
            status = "queued"
    else:
        status = "not_started"

    total_jobs = sum(c["total_games"] for c in components)
    done_jobs = sum(c["done_games"] for c in components)
    active_jobs = sum(c["active_games"] for c in components)

    reduce_done = sum(c["reduce_done_seasons"] or 0 for c in components)
    reduce_total = sum(c["reduce_total_seasons"] or 0 for c in components)

    # Combined progress: map phase = 0-90%, reduce phase = 90-100%
    map_pct = round((done_jobs / total_jobs * 100.0), 1) if total_jobs else 0.0
    if status == "complete":
        overall_pct = 100.0
    elif status == "finalizing":
        reduce_pct = (reduce_done / reduce_total * 100.0) if reduce_total else 0.0
        overall_pct = round(90.0 + reduce_pct * 0.1, 1)
    else:
        overall_pct = round(map_pct * 0.9, 1)

    return {
        "status": status,
        "total_games": total_jobs,
        "done_games": done_jobs,
        "active_games": active_jobs,
        "pending_games": max(total_jobs - done_jobs - active_jobs, 0),
        "progress_pct": map_pct,
        "overall_pct": overall_pct,
        "reduce_done_seasons": reduce_done,
        "reduce_total_seasons": reduce_total,
        "latest_run_at": latest_run_at,
        "components": components,
        "is_multi_component": len(components) > 1,
    }


def _format_backfill_timestamp(value) -> str | None:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _build_metric_backfill_status(session, metric_key: str):
    from metrics.framework.runtime import get_metric as _get_metric

    base_metric_key = family_base_key(metric_key)
    db_metric = (
        session.query(MetricDefinitionModel)
        .filter(
            MetricDefinitionModel.key == base_metric_key,
            MetricDefinitionModel.status != "archived",
        )
        .first()
    )
    runtime_metric = _get_metric(metric_key, session=session)
    if db_metric is None and runtime_metric is None:
        return None, None

    metric_def = _metric_def_view(
        runtime_metric or db_metric,
        source_type=getattr(db_metric, "source_type", None),
    )
    is_career_metric = bool(getattr(runtime_metric, "career", False))
    is_season_trigger = getattr(runtime_metric, "trigger", "game") == "season"
    if is_season_trigger:
        total_games = (
            session.query(func.count(MetricResultModel.id))
            .filter(MetricResultModel.metric_key == metric_key)
            .scalar() or 0
        )
    else:
        total_games = (
            session.query(func.count(Game.game_id))
            .filter(Game.game_date.isnot(None))
            .scalar() or 0
        )

    backfill_keys = [metric_key]
    if runtime_metric is not None and not is_career_metric and getattr(runtime_metric, "supports_career", False):
        career_key = metric_key + "_career"
        if _get_metric(career_key, session=session) is not None:
            backfill_keys.append(career_key)

    components = []
    for key in backfill_keys:
        if is_season_trigger and key != metric_key:
            # Career sibling uses its own result count as total
            key_total = (
                session.query(func.count(MetricResultModel.id))
                .filter(MetricResultModel.metric_key == key)
                .scalar() or 0
            )
        else:
            key_total = total_games
        component = _metric_backfill_component(session, key, int(key_total))
        component["label"] = "Career" if key.endswith("_career") else "Season"
        component["latest_run_at"] = _format_backfill_timestamp(component["latest_run_at"])
        components.append(component)

    backfill = _combine_backfill_components(metric_def, components)
    backfill["latest_run_at"] = _format_backfill_timestamp(backfill["latest_run_at"])
    return metric_def, backfill


def _dispatch_metric_backfill(metric_key: str) -> None:
    import subprocess
    import sys

    from metrics.framework.runtime import get_metric

    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    m = get_metric(metric_key)
    if m and getattr(m, "trigger", "game") == "season":
        # Dispatch concrete seasons first. Career variants aggregate from season
        # results and should be enqueued only after the base pass finishes.
        subprocess.Popen(
            [sys.executable, "-m", "tasks.dispatch", "season-metrics", "--metric", metric_key],
            cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    else:
        subprocess.Popen(
            [sys.executable, "-m", "tasks.dispatch", "metric-backfill", "--metric", metric_key],
            cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


def _pbp_text(play: GamePlayByPlay) -> str:
    return (play.home_description or play.visitor_description or play.neutral_description or "").strip()



def _season_label(season_id: str | None) -> str:
    if not season_id:
        return "-"

    s = str(season_id).strip()
    if not s:
        return "-"

    if len(s) == 5 and s.isdigit():
        season_type_map = {
            "1": _t("Pre Season", "季前赛"),
            "2": _t("Regular Season", "常规赛"),
            "3": _t("All Star", "全明星"),
            "4": _t("Playoffs", "季后赛"),
            "5": _t("PlayIn", "附加赛"),
        }
        season_type = season_type_map.get(s[0], f"Type {s[0]}")
        year = s[1:]
        try:
            next_year_suffix = str(int(year) + 1)[-2:]
            return f"{year}-{next_year_suffix} {season_type}"
        except ValueError:
            return s

    return s


_SEASON_TYPE_NAMES = {
    "2": "Regular Season",
    "4": "Playoffs",
    "5": "PlayIn",
    "1": "Pre Season",
    "3": "All Star",
}

_SEASON_TYPE_PLURAL = {
    "2": "Regular Seasons",
    "4": "Playoffs",
    "5": "PlayIn",
    "1": "Pre Seasons",
    "3": "All Star",
}


def _season_year_label(season_id: str | None) -> str:
    """Year-only label, e.g. '2025-26', for use inside grouped dropdowns."""
    if not season_id:
        return "-"
    s = str(season_id).strip()
    if len(s) == 5 and s.isdigit():
        year = s[1:]
        try:
            next_year_suffix = str(int(year) + 1)[-2:]
            return f"{year}-{next_year_suffix}"
        except ValueError:
            return s
    return s


def _season_start_year_label(season: int | None) -> str:
    if season is None:
        return "-"
    return f"{season}-{(season + 1) % 100:02d}"


def _season_sort_key(season_id: str | None) -> tuple[int, int]:
    """
    Sort seasons by actual year first, then type priority.
    Higher tuple means newer/preferred.
    """
    if not season_id:
        return (-1, -1)
    s = str(season_id).strip()
    if len(s) == 5 and s.isdigit():
        year = int(s[1:])
        # Prefer regular season view for "current" when same year exists.
        type_priority = {
            "2": 5,  # Regular Season
            "5": 4,  # PlayIn
            "4": 3,  # Playoffs
            "1": 2,  # Pre Season
            "3": 1,  # All Star
        }.get(s[0], 0)
        return (year, type_priority)
    return (-1, -1)


def _season_type_prefix(season_id: str | None) -> str | None:
    s = str(season_id or "").strip()
    return s[0] if len(s) == 5 and s.isdigit() else None


def _pick_current_season(season_ids: list[str]) -> str | None:
    if not season_ids:
        return None
    regular = [s for s in season_ids if str(s).startswith("2")]
    if regular:
        return max(regular, key=_season_sort_key)
    return max(season_ids, key=_season_sort_key)


def _non_pro_metric_detail_season_options(season_ids: list[str]) -> list[str]:
    regular = sorted(
        [s for s in season_ids if _season_type_prefix(s) == "2"],
        key=_season_sort_key,
        reverse=True,
    )
    if regular:
        return regular[:2]
    current = _pick_current_season(season_ids)
    return [current] if current else []


def _pct_text(made: int, attempted: int) -> str:
    if attempted <= 0:
        return "-"
    return f"{(made / attempted):.3f}"


SHOT_ZONE_LAYOUT: list[dict[str, str | float]] = [
    {
        "key": "left_corner_3",
        "label": "Left Corner 3",
        "x": -235.0,
        "y": 24.0,
        "path_d": "M -250 -47.5 L -220 -47.5 L -220 92.5 L -250 92.5 Z",
    },
    {
        "key": "right_corner_3",
        "label": "Right Corner 3",
        "x": 235.0,
        "y": 24.0,
        "path_d": "M 220 -47.5 L 250 -47.5 L 250 92.5 L 220 92.5 Z",
    },
    {
        "key": "left_above_break_3",
        "label": "Left Above Break 3",
        "x": -170.0,
        "y": 285.0,
        "path_d": "M -250 92.5 L -250 375 L -90 375 L -90 222.8 A 237.5 237.5 0 0 1 -220 92.5 L -250 92.5 Z",
    },
    {
        "key": "center_above_break_3",
        "label": "Center Above Break 3",
        "x": 0.0,
        "y": 305.0,
        "path_d": "M -90 222.8 L -90 375 L 90 375 L 90 222.8 A 237.5 237.5 0 0 1 -90 222.8 Z",
    },
    {
        "key": "right_above_break_3",
        "label": "Right Above Break 3",
        "x": 170.0,
        "y": 285.0,
        "path_d": "M 250 92.5 L 250 375 L 90 375 L 90 222.8 A 237.5 237.5 0 0 0 220 92.5 L 250 92.5 Z",
    },
    {
        "key": "left_mid_range",
        "label": "Left Mid-Range",
        "x": -145.0,
        "y": 145.0,
        "path_d": "M -220 -47.5 L -80 -47.5 L -80 142.5 L -154 183.8 A 237.5 237.5 0 0 1 -220 92.5 Z",
    },
    {
        "key": "center_mid_range",
        "label": "Center Mid-Range",
        "x": 0.0,
        "y": 192.0,
        "path_d": "M -154 183.8 L -80 142.5 L 80 142.5 L 154 183.8 A 237.5 237.5 0 0 1 -154 183.8 Z",
    },
    {
        "key": "right_mid_range",
        "label": "Right Mid-Range",
        "x": 145.0,
        "y": 145.0,
        "path_d": "M 220 -47.5 L 80 -47.5 L 80 142.5 L 154 183.8 A 237.5 237.5 0 0 0 220 92.5 Z",
    },
    {
        "key": "paint_non_ra",
        "label": "Paint (Non-RA)",
        "x": 0.0,
        "y": 108.0,
        "path_d": "M -80 -47.5 L -80 142.5 L 80 142.5 L 80 -47.5 Z",
    },
    {
        "key": "restricted_area",
        "label": "Restricted Area",
        "x": 0.0,
        "y": 14.0,
        "path_d": "M -40 -47.5 L -40 0 A 40 40 0 0 0 40 0 L 40 -47.5 Z",
    },
    {
        "key": "backcourt",
        "label": "Backcourt",
        "x": 0.0,
        "y": 399.0,
        "path_d": "M -250 375 L 250 375 L 250 422.5 L -250 422.5 Z",
    },
]
SHOT_ZONE_META = {str(row["key"]): row for row in SHOT_ZONE_LAYOUT}


def _shot_zone_key(zone_basic: str | None, zone_area: str | None) -> str | None:
    basic = (zone_basic or "").strip().lower()
    area = (zone_area or "").strip().lower()

    if not basic:
        return None
    if basic == "restricted area":
        return "restricted_area"
    if basic == "in the paint (non-ra)":
        return "paint_non_ra"
    if basic == "left corner 3":
        return "left_corner_3"
    if basic == "right corner 3":
        return "right_corner_3"
    if basic == "backcourt":
        return "backcourt"

    if "left" in area:
        side = "left"
    elif "right" in area:
        side = "right"
    else:
        side = "center"

    if basic == "above the break 3":
        return f"{side}_above_break_3"
    if basic == "mid-range":
        return f"{side}_mid_range"
    return None


def _build_shot_zone_heatmap(
    shot_rows: list[tuple[str | None, str | None, bool | int | None]],
) -> tuple[list[dict[str, float | int | str]], int, int]:
    buckets: dict[str, dict[str, float | int | str]] = {}
    attempts_total = 0
    made_total = 0

    for zone_basic, zone_area, raw_made in shot_rows:
        key = _shot_zone_key(zone_basic, zone_area)
        if key is None or key not in SHOT_ZONE_META:
            continue
        zone_meta = SHOT_ZONE_META[key]
        if key not in buckets:
            buckets[key] = {
                "zone_key": key,
                "zone_label": str(zone_meta["label"]),
                "x": float(zone_meta["x"]),
                "y": float(zone_meta["y"]),
                "path_d": str(zone_meta["path_d"]),
                "attempts": 0,
                "made": 0,
            }
        buckets[key]["attempts"] = int(buckets[key]["attempts"]) + 1
        if bool(raw_made):
            buckets[key]["made"] = int(buckets[key]["made"]) + 1
            made_total += 1
        attempts_total += 1

    max_attempts = max((int(b["attempts"]) for b in buckets.values()), default=0)
    zones: list[dict[str, float | int | str]] = []
    for zone in SHOT_ZONE_LAYOUT:
        key = str(zone["key"])
        b = buckets.get(key, {})
        attempts = int(b.get("attempts", 0))
        made = int(b.get("made", 0))
        fg_rate = (made / attempts) if attempts > 0 else 0.0
        if attempts == 0:
            color = "rgba(255,255,255,0.04)"
            alpha = 0.04
        else:
            # Smooth gradient: light red (0%) → light green (60%+)
            t = min(fg_rate / 0.6, 1.0)
            hue = int(t * 130)  # 0=red → 65=yellow → 130=green
            color = f"hsl({hue}, 60%, 58%)"
            alpha = 0.78
        zones.append(
            {
                "zone_key": key,
                "zone_label": str(zone["label"]),
                "x": float(zone["x"]),
                "y": float(zone["y"]),
                "path_d": str(zone["path_d"]),
                "attempts": attempts,
                "made": made,
                "fg_pct": _pct_text(made, attempts),
                "color": color,
                "alpha": alpha,
            }
        )

    return zones, attempts_total, made_total


def is_admin() -> bool:
    """True for logged-in admin users, or localhost fallback requests.

    Primary path: authenticated user record has `is_admin == True`.
    Fallback path: request originates directly from localhost (not via
    Cloudflare tunnel) for emergency/operator access.
    """
    user = _current_user()
    if user is not None and getattr(user, "is_admin", False):
        return True

    if request.remote_addr not in ("127.0.0.1", "::1"):
        return False
    host = (request.host or "").strip().lower()
    if "://" not in host:
        host = f"//{host}"
    host_name = (urlparse(host).hostname or "").strip().lower()
    if host_name not in ("127.0.0.1", "::1", "localhost"):
        return False
    # If Cloudflare (or any upstream proxy) added a forwarding header the
    # request came through the tunnel, not directly from the local browser.
    if request.headers.get("CF-Connecting-IP"):
        return False
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for and forwarded_for.strip() not in ("127.0.0.1", "::1", ""):
        return False
    return True


def _require_admin_json():
    """Return a 403 JSON response if the caller is not admin, else None."""
    if not is_admin():
        return jsonify({"error": "admin_only"}), 403
    return None


def _require_admin_page():
    """Render a 403 page if the caller is not admin, else None."""
    if not is_admin():
        return render_template("403.html"), 403
    return None


def is_pro() -> bool:
    """True if the current user has Pro subscription access.

    Pro access is granted if the user is admin, or has an active/grace-period
    Pro subscription.
    """
    if is_admin():
        return True
    user = _current_user()
    if user is None or user.subscription_tier != "pro":
        return False
    if user.subscription_expires_at is not None:
        if datetime.utcnow() > user.subscription_expires_at:
            return False
    return True


def _require_pro_json():
    if not is_pro():
        return jsonify({"error": "pro_required", "upgrade_url": _localized_url_for("pricing")}), 403
    return None


def _require_pro_page():
    if not is_pro():
        return render_template("upgrade_required.html"), 403
    return None


def _request_next_url() -> str:
    next_url = request.full_path or request.path or url_for("home")
    return next_url[:-1] if next_url.endswith("?") else next_url


_SHOT_LINE_RE = re.compile(
    r"(?P<a>\d+)\s*投\s*(?P<m>\d+)\s*中(?:，|,)?\s*命中率\s*(?P<p>\d+(?:\.\d+)?)%",
    re.IGNORECASE,
)


def _content_review_validation_errors(text: str) -> list[str]:
    raw = str(text or "")
    errors: list[str] = []

    for match in _SHOT_LINE_RE.finditer(raw):
        attempts = int(match.group("a"))
        made = int(match.group("m"))
        pct = float(match.group("p"))
        if made > attempts:
            errors.append(
                f"Shot line looks inverted or impossible: '{match.group(0)}' (made {made} > attempts {attempts})."
            )
            continue
        actual_pct = round((made / attempts) * 100, 1) if attempts else 0.0
        if abs(actual_pct - pct) > 0.2:
            errors.append(
                f"Shot line pct mismatch: '{match.group(0)}' (computed {actual_pct:.1f}%)."
            )

    text_no_space = re.sub(r"\s+", "", raw)
    box_match = re.search(
        r"(\d+)分(\d+)篮板(\d+)助攻|(\d+)分(\d+)助攻(\d+)篮板|(\d+)篮板(\d+)助攻(\d+)分|(\d+)篮板(\d+)分(\d+)助攻|(\d+)助攻(\d+)篮板(\d+)分|(\d+)助攻(\d+)分(\d+)篮板",
        text_no_space,
    )
    if box_match:
        nums = [int(val) for val in box_match.groups() if val is not None]
        has_triple_double = sum(1 for val in nums if val >= 10) >= 3
        mentions_quasi = "准三双" in raw
        mentions_triple = "三双" in raw and "准三双" not in raw
        if has_triple_double and mentions_quasi:
            errors.append("Copy says '准三双' but the stat line already qualifies as a triple-double.")
        if not has_triple_double and mentions_triple:
            errors.append("Copy says '三双' but the visible stat line does not show three categories at 10+.")

    return errors


def _post_ai_review_validation_errors(db_sess, post_id: int) -> list[str]:
    variants = (
        db_sess.query(SocialPostVariant.content_raw)
        .filter(SocialPostVariant.post_id == post_id)
        .all()
    )
    errors: list[str] = []
    for idx, (content_raw,) in enumerate(variants, start=1):
        for err in _content_review_validation_errors(content_raw):
            errors.append(f"Variant {idx}: {err}")
    return errors


def _require_login_json():
    if _current_user():
        return None
    return jsonify({"error": "login_required"}), 401


def _require_login_page():
    if _current_user():
        return None
    return redirect(url_for("auth_login", next=_request_next_url()))


_ACCESS_LEVEL_RANK = {
    "anonymous": 0,
    "logged_in": 1,
    "pro": 2,
    "admin": 3,
}


def current_access_level() -> str:
    if is_admin():
        return "admin"
    if is_pro():
        return "pro"
    if _current_user():
        return "logged_in"
    return "anonymous"


def _has_required_access(required_level: str) -> bool:
    return _ACCESS_LEVEL_RANK[current_access_level()] >= _ACCESS_LEVEL_RANK[required_level]


def _feature_access_level(feature: str) -> str:
    with SessionLocal() as session:
        return get_feature_access_level(session, feature)


def _build_metric_feature_context(access_config: dict | None = None) -> dict:
    config = dict(access_config or _feature_access_config())
    current_level = current_access_level()
    can_search_metrics = _ACCESS_LEVEL_RANK[current_level] >= _ACCESS_LEVEL_RANK[config["metric_search"]]
    can_create_metrics = _ACCESS_LEVEL_RANK[current_level] >= _ACCESS_LEVEL_RANK[config["metric_create"]]

    metric_create_cta_href = _localized_url_for("metric_new")
    metric_create_cta_badge = None
    if not can_create_metrics:
        if current_level == "anonymous":
            metric_create_cta_href = url_for("auth_login", next=_request_next_url())
        elif config["metric_create"] == "admin":
            metric_create_cta_badge = "ADMIN"
        else:
            metric_create_cta_href = _localized_url_for("pricing")
            metric_create_cta_badge = "PRO"

    return {
        "current_access_level": current_level,
        "metric_search_required_level": config["metric_search"],
        "metric_create_required_level": config["metric_create"],
        "metric_search_required_label": access_level_label(config["metric_search"]),
        "metric_create_required_label": access_level_label(config["metric_create"]),
        "can_search_metrics": can_search_metrics,
        "can_create_metrics": can_create_metrics,
        "metric_create_cta_href": metric_create_cta_href,
        "metric_create_cta_badge": metric_create_cta_badge,
    }


def _feature_access_config() -> dict[str, str]:
    with SessionLocal() as session:
        return get_feature_access_config(session)


def _serialize_feature_access(session) -> list[dict]:
    config = get_feature_access_config(session)
    serialized = []
    for descriptor in feature_access_descriptors():
        serialized.append(
            {
                "key": descriptor["key"],
                "label": descriptor["label"],
                "description": descriptor["description"],
                "default_level": descriptor["default_level"],
                "current_level": config[descriptor["key"]],
                "allowed_levels": [
                    {"value": level, "label": access_level_label(level)}
                    for level in descriptor["allowed_levels"]
                ],
            }
        )
    return serialized


def _feature_access_denied_json(required_level: str):
    current_level = current_access_level()
    if current_level == "anonymous":
        return jsonify({"error": "login_required", "required_level": required_level}), 401
    if required_level == "admin":
        return jsonify({"error": "admin_only", "required_level": required_level}), 403
    return jsonify(
        {
            "error": "pro_required",
            "required_level": required_level,
            "upgrade_url": _localized_url_for("pricing"),
        }
    ), 403


def _feature_access_denied_page(required_level: str):
    current_level = current_access_level()
    if current_level == "anonymous":
        return redirect(url_for("auth_login", next=_request_next_url()))
    if required_level == "admin":
        return render_template("403.html"), 403
    return render_template("upgrade_required.html"), 403


def _require_feature_json(feature: str):
    required_level = _feature_access_level(feature)
    if _has_required_access(required_level):
        return None
    return _feature_access_denied_json(required_level)


def _require_feature_page(feature: str):
    required_level = _feature_access_level(feature)
    if _has_required_access(required_level):
        return None
    return _feature_access_denied_page(required_level)


def _require_metric_creator_json():
    """Users meeting the configured metric-create level can create/edit metrics."""
    return _require_feature_json("metric_create")


def _require_metric_creator_page():
    return _require_feature_page("metric_create")


_VISITOR_COOKIE = "funba_visitor"
_VISITOR_COOKIE_MAX_AGE = 365 * 24 * 3600  # 1 year in seconds


def _request_visitor_id(*, ensure_cookie: bool = False) -> str | None:
    visitor_id = request.cookies.get(_VISITOR_COOKIE)
    if visitor_id:
        return visitor_id
    if not ensure_cookie:
        return None

    visitor_id = str(_uuid_mod.uuid4())

    @after_this_request
    def _set_cookie(response):
        response.set_cookie(
            _VISITOR_COOKIE,
            visitor_id,
            max_age=_VISITOR_COOKIE_MAX_AGE,
            httponly=True,
            samesite="Lax",
        )
        return response

    return visitor_id


def _ai_usage_preview(text: str | None, limit: int = 240) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    if len(raw) <= limit:
        return raw
    return raw[: limit - 15].rstrip() + " ...[truncated]"


def _record_ai_usage_event(
    *,
    feature: str,
    operation: str,
    model: str,
    usage: dict | None,
    started_at: float,
    success: bool,
    http_status: int,
    error_code: str | None = None,
    conversation_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    current_user = _current_user()
    visitor_id = _request_visitor_id(ensure_cookie=current_user is None)
    usage_payload = dict(usage or {})
    provider = str(usage_payload.get("provider") or "").strip()
    selected_model = str(usage_payload.get("model") or model or "").strip()
    prompt_tokens = usage_payload.get("prompt_tokens")
    completion_tokens = usage_payload.get("completion_tokens")
    total_tokens = usage_payload.get("total_tokens")
    latency_ms = int(round((time.perf_counter() - started_at) * 1000))

    try:
        with SessionLocal() as session:
            log_ai_usage_event(
                session,
                user_id=getattr(current_user, "id", None),
                visitor_id=visitor_id,
                feature=feature,
                operation=operation,
                endpoint=request.path,
                provider=provider or "unknown",
                model=selected_model or "unknown",
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                latency_ms=latency_ms,
                success=success,
                error_code=error_code,
                http_status=http_status,
                conversation_id=conversation_id,
                metadata=metadata,
            )
            session.commit()
    except Exception:
        logger.exception("AI usage logging failed for %s/%s", feature, operation)


# Known crawlers. Only Google/Bing/Baidu are allowlisted; other crawlers are
# still recorded as crawler traffic but remain blocked by the bot filter.
_KNOWN_CRAWLER_SIGNATURES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("googlebot", ("googlebot",)),
    ("google-inspectiontool", ("google-inspectiontool",)),
    ("apis-google", ("apis-google",)),
    ("bingbot", ("bingbot",)),
    ("bingpreview", ("bingpreview",)),
    ("baiduspider", ("baiduspider",)),
    ("facebookexternalhit", ("facebookexternalhit",)),
    ("meta-webindexer", ("meta-webindexer",)),
    ("twitterbot", ("twitterbot",)),
    ("linkedinbot", ("linkedinbot",)),
    ("duckduckbot", ("duckduckbot",)),
    ("yandexbot", ("yandexbot",)),
    ("applebot", ("applebot",)),
    ("sogou", ("sogou",)),
    ("360spider", ("360spider",)),
    ("slurp", ("slurp",)),
    ("mediapartners-google", ("mediapartners-google",)),
    ("bytespider", ("bytespider",)),
    ("gptbot", ("gptbot",)),
    ("claudebot", ("claudebot",)),
)

_ALLOWLISTED_CRAWLER_NAMES = frozenset({
    "googlebot",
    "google-inspectiontool",
    "apis-google",
    "bingbot",
    "bingpreview",
    "baiduspider",
})

_BOT_SIGNATURES = (
    "bot", "crawl", "spider", "slurp", "mediapartners",
    "bytespider",
    "gptbot", "claudebot",
    "curl/", "wget/", "httpie/", "python-requests", "python-urllib",
    "go-http-client", "java/", "okhttp", "axios/", "node-fetch",
    "scrapy", "headlesschrome", "phantomjs", "selenium",
)

# Bare or too-short UAs are almost never real browsers
_MIN_REAL_UA_LENGTH = 40
_SUSPICIOUS_PROBE_PATH_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"^/\.git/",
        r"^/\.env",
        r"^/wp-",
        r"^/wordpress",
        r"^/xmlrpc\.php$",
        r"^/phpmyadmin",
        r"^/boaform",
        r"^/cgi-bin",
        r"^/actuator",
        r"^/vendor/",
        r"^/server-status",
        r"^/login\.php$",
        r"\.php$",
        r"\.asp$",
        r"\.aspx$",
    )
)
_REPEAT_CRAWLER_IP_LOOKBACK = timedelta(days=30)
_REPEAT_CRAWLER_IP_CACHE_SECONDS = 300


def _crawler_name_from_user_agent(user_agent: str | None) -> str | None:
    ua = (user_agent or "").lower().strip()
    if not ua:
        return None
    for crawler_name, signatures in _KNOWN_CRAWLER_SIGNATURES:
        if any(sig in ua for sig in signatures):
            return crawler_name
    return None


def _is_allowlisted_crawler_name(crawler_name: str | None) -> bool:
    return bool(crawler_name and crawler_name in _ALLOWLISTED_CRAWLER_NAMES)


def _probe_crawler_name_for_path(path: str | None) -> str | None:
    value = (path or "").strip()
    if not value:
        return None
    if any(pattern.search(value) for pattern in _SUSPICIOUS_PROBE_PATH_PATTERNS):
        return "probe-bot"
    return None


@lru_cache(maxsize=8192)
def _recent_repeat_crawler_ip_cached(ip_address: str, cache_bucket: int) -> bool:
    del cache_bucket
    value = (ip_address or "").strip()
    if not value or value in ("127.0.0.1", "::1"):
        return False
    cutoff = datetime.utcnow() - _REPEAT_CRAWLER_IP_LOOKBACK
    try:
        with SessionLocal() as db_sess:
            auth_spray_seen = (
                db_sess.query(PageView.id)
                .filter(
                    PageView.ip_address == value,
                    PageView.crawler_name == "auth-spray-bot",
                    PageView.created_at >= cutoff,
                )
                .first()
            )
            return auth_spray_seen is not None
    except Exception:
        logger.exception("repeat crawler ip lookup failed")
        return False


def _recent_repeat_crawler_ip(ip_address: str | None) -> bool:
    value = (ip_address or "").strip()
    cache_bucket = int(time.time() // _REPEAT_CRAWLER_IP_CACHE_SECONDS)
    return _recent_repeat_crawler_ip_cached(value, cache_bucket)


def _request_crawler_decision() -> dict[str, object]:
    cached = getattr(g, "_crawler_decision", None)
    if cached is not None:
        return cached

    ua = (request.user_agent.string or "").lower().strip()
    crawler_name = _crawler_name_from_user_agent(ua)
    decision: dict[str, object]
    if crawler_name is not None:
        decision = {
            "is_crawler": True,
            "crawler_name": crawler_name,
            "should_block": not _is_allowlisted_crawler_name(crawler_name),
        }
    else:
        probe_crawler_name = _probe_crawler_name_for_path(request.path)
        if probe_crawler_name is not None:
            decision = {
                "is_crawler": True,
                "crawler_name": probe_crawler_name,
                "should_block": True,
            }
        elif not app.config.get("TESTING") and _recent_repeat_crawler_ip(_real_ip()):
            decision = {
                "is_crawler": True,
                "crawler_name": "auth-spray-bot",
                "should_block": True,
            }
        elif not app.config.get("TESTING") and _is_bot():
            decision = {
                "is_crawler": True,
                "crawler_name": "other-bot",
                "should_block": True,
            }
        else:
            decision = {
                "is_crawler": False,
                "crawler_name": None,
                "should_block": False,
            }
    g._crawler_decision = decision
    return decision


def _should_track_page_view_request() -> bool:
    return request.method == "GET" and not request.path.startswith("/api/") and not request.path.startswith("/static/")


def _page_view_visitor_id(*, is_crawler: bool, crawler_name: str | None) -> str:
    if is_crawler:
        fingerprint = f"{crawler_name or 'bot'}|{_real_ip() or '-'}|{(request.user_agent.string or '').strip().lower()}"
        return str(_uuid_mod.uuid5(_uuid_mod.NAMESPACE_URL, fingerprint))
    return _request_visitor_id(ensure_cookie=True) or str(_uuid_mod.uuid4())


def _record_page_view(*, is_crawler: bool, crawler_name: str | None) -> None:
    if getattr(g, "_page_view_recorded", False):
        return
    g._page_view_recorded = True

    # Exclude admin traffic so ranking signals (e.g. NewsCluster.unique_view_count)
    # reflect real readers, not our own browsing.
    if not is_crawler:
        try:
            if is_admin():
                return
        except Exception:
            pass

    pv = PageView(
        visitor_id=_page_view_visitor_id(is_crawler=is_crawler, crawler_name=crawler_name),
        path=request.path,
        referrer=(request.referrer or "")[:1000],
        user_agent=(request.user_agent.string or "")[:500],
        is_crawler=is_crawler,
        crawler_name=(crawler_name or "")[:64] or None,
        ip_address=_real_ip(),
        created_at=datetime.utcnow(),
    )
    try:
        with SessionLocal() as db_sess:
            db_sess.add(pv)
            db_sess.commit()
    except Exception:
        logger.exception("page view tracking failed")


def _is_bot() -> bool:
    ua = (request.user_agent.string or "").lower().strip()
    crawler_name = _crawler_name_from_user_agent(ua)
    if crawler_name is not None:
        return not _is_allowlisted_crawler_name(crawler_name)
    if not ua or len(ua) < _MIN_REAL_UA_LENGTH:
        return True
    return any(sig in ua for sig in _BOT_SIGNATURES)


@app.before_request
def _block_bots():
    """Return 403 for known bots / non-browser clients."""
    if app.config.get("TESTING"):
        return
    if request.path.startswith("/static/") or request.path == "/robots.txt":
        return
    # Exempt only direct localhost requests, not Cloudflare-tunneled traffic.
    if request.remote_addr in ("127.0.0.1", "::1") and _real_ip() in ("127.0.0.1", "::1"):
        return
    # Exempt AI coding tools (Claude Code, Codex) by User-Agent
    ua = (request.user_agent.string or "").lower().strip()
    if any(sig in ua for sig in ("claude-code", "codex", "anthropic", "openai")):
        return
    crawler_decision = _request_crawler_decision()
    if crawler_decision["is_crawler"] and crawler_decision["should_block"]:
        if _should_track_page_view_request():
            _record_page_view(
                is_crawler=True,
                crawler_name=str(crawler_decision["crawler_name"] or "other-bot"),
            )
        return "Forbidden", 403


@app.before_request
def _track_page_view():
    """Log each page load and ensure the visitor cookie is set."""
    if not _should_track_page_view_request():
        return
    # Skip tracking for localhost requests
    if _real_ip() in ("127.0.0.1", "::1"):
        return
    crawler_decision = _request_crawler_decision()
    if crawler_decision["is_crawler"]:
        _record_page_view(
            is_crawler=True,
            crawler_name=str(crawler_decision["crawler_name"] or "other-bot"),
        )
        return
    # Skip tracking for admin / owner sessions
    _uid = session.get("user_id")
    if _uid:
        try:
            with SessionLocal() as _db:
                _u = _db.get(User, _uid)
                if _u and (_u.is_admin or _u.email in ("yuewang.sj@gmail.com", "yuewang9269@gmail.com")):
                    return
        except Exception:
            pass
    _record_page_view(is_crawler=False, crawler_name=None)


def _current_user() -> User | None:
    """Return the logged-in User object, or None if not authenticated."""
    user_id = session.get("user_id")
    if not user_id:
        return None
    try:
        with SessionLocal() as db:
            return db.get(User, user_id)
    except Exception:
        return SimpleNamespace(
            id=user_id,
            is_admin=False,
            subscription_tier="free",
            subscription_expires_at=None,
            display_name="",
            avatar_url=None,
        )


def _paperclip_bridge_config():
    return load_paperclip_bridge_config()


def _paperclip_bridge_enabled() -> bool:
    return _paperclip_bridge_config() is not None


def _paperclip_actor_name() -> str:
    user = _current_user()
    if user is not None and getattr(user, "display_name", None):
        return str(user.display_name)
    return "Funba admin"


def _social_post_comments(post: SocialPost) -> list[dict]:
    if not post.admin_comments:
        return []
    try:
        raw_comments = json.loads(post.admin_comments)
    except Exception:
        logger.warning("Failed to decode admin_comments for SocialPost %s", post.id, exc_info=True)
        return []
    return normalize_admin_comments(raw_comments)


def _write_social_post_comments(post: SocialPost, comments: list[dict]) -> None:
    post.admin_comments = json.dumps(comments, ensure_ascii=False)
    post.updated_at = datetime.utcnow()


def _find_comment_index(
    comments: list[dict],
    *,
    timestamp: str,
    text: str,
    origin: str,
) -> int | None:
    for idx in range(len(comments) - 1, -1, -1):
        comment = comments[idx]
        if comment.get("paperclip_comment_id"):
            continue
        if comment.get("timestamp") != timestamp:
            continue
        if comment.get("origin") != origin:
            continue
        if comment.get("text") != text:
            continue
        return idx
    return None


def _paperclip_workflow_view(post: SocialPost) -> dict[str, object]:
    cfg = _paperclip_bridge_config()
    owner_label = actor_label_for_issue(
        assignee_agent_id=post.paperclip_assignee_agent_id,
        assignee_user_id=post.paperclip_assignee_user_id,
        cfg=cfg,
    )
    issue_url = _paperclip_issue_url(post.paperclip_issue_identifier)
    if (
        str(getattr(post, "status", "") or "").strip() == "ai_review"
        and owner_label.startswith("agent:")
    ):
        owner_label = (cfg.content_reviewer_name if cfg is not None else None) or "Content Reviewer"
    return {
        "enabled": _paperclip_bridge_enabled(),
        "issue_id": post.paperclip_issue_id,
        "issue_identifier": post.paperclip_issue_identifier,
        "issue_url": issue_url,
        "issue_status": post.paperclip_issue_status,
        "owner_label": owner_label,
        "assignee_agent_id": post.paperclip_assignee_agent_id,
        "assignee_user_id": post.paperclip_assignee_user_id,
        "last_comment_id": post.paperclip_last_comment_id,
        "last_synced_at": post.paperclip_last_synced_at.isoformat() if post.paperclip_last_synced_at else None,
        "sync_error": post.paperclip_sync_error,
    }


def _paperclip_issue_base_url() -> str | None:
    with SessionLocal() as session:
        return get_paperclip_issue_base_url(session)


def _paperclip_issue_url(identifier: str | None) -> str | None:
    return build_paperclip_issue_url(identifier, _paperclip_issue_base_url())


def _social_post_source_metrics(post: SocialPost) -> list[str]:
    try:
        value = json.loads(post.source_metrics) if post.source_metrics else []
    except Exception:
        return []
    return [str(item) for item in value if item]


def _is_metric_deep_dive_post(post: SocialPost, metric_key: str) -> bool:
    comments = _social_post_comments(post)
    if not any(str(comment.get("event_type") or "") == _SOCIAL_POST_EVENT_METRIC_DEEP_DIVE_BRIEF for comment in comments):
        return False
    return metric_key in _social_post_source_metrics(post)


def _is_active_metric_deep_dive_post(post: SocialPost) -> bool:
    if str(getattr(post, "status", "") or "").strip() != "draft":
        return False
    issue_status = str(getattr(post, "paperclip_issue_status", "") or "").strip()
    return issue_status not in {"done", "cancelled", "blocked"}


def _metric_deep_dive_post_view(post: SocialPost) -> dict[str, object]:
    workflow = _paperclip_workflow_view(post)
    return {
        "id": post.id,
        "topic": post.topic,
        "status": post.status,
        "created_at": post.created_at.isoformat() if post.created_at else None,
        "created_at_label": _format_backfill_timestamp(post.created_at),
        "workflow": workflow,
        "admin_url": url_for("admin_content_post", post_id=post.id),
    }


def _metric_deep_dive_state(session, metric_key: str) -> dict[str, object]:
    posts = (
        session.query(SocialPost)
        .order_by(SocialPost.created_at.desc(), SocialPost.id.desc())
        .all()
    )
    metric_posts = [post for post in posts if _is_metric_deep_dive_post(post, metric_key)]
    active_post = next((post for post in metric_posts if _is_active_metric_deep_dive_post(post)), None)
    latest_post = metric_posts[0] if metric_posts else None
    return {
        "can_trigger": active_post is None,
        "active_post": _metric_deep_dive_post_view(active_post) if active_post else None,
        "latest_post": _metric_deep_dive_post_view(latest_post) if latest_post else None,
    }


def _build_metric_deep_dive_brief(
    *,
    session,
    metric_name: str,
    metric_name_zh: str,
    metric_key: str,
    metric_description: str,
    metric_scope: str,
    metric_page_url: str,
) -> str:
    from content_pipeline.metric_analysis_issues import build_metric_analysis_issue_description
    from runtime_flags import get_enabled_platforms

    enabled = get_enabled_platforms()
    return build_metric_analysis_issue_description(
        session=session,
        metric_key=metric_key,
        metric_name=metric_name,
        metric_name_zh=metric_name_zh,
        metric_description=metric_description,
        metric_scope=metric_scope,
        metric_page_url=metric_page_url,
        enabled_platforms=enabled,
    )


def _create_metric_deep_dive_placeholder_post(metric_key: str, metric_name: str, brief_text: str) -> tuple[int, str]:
    now = datetime.utcnow()
    comments: list[dict[str, object]] = []
    brief_timestamp = append_admin_comment(
        comments,
        text=brief_text,
        author=_paperclip_actor_name(),
        origin="system",
        event_type=_SOCIAL_POST_EVENT_METRIC_DEEP_DIVE_BRIEF,
    )
    with SessionLocal() as s:
        post = SocialPost(
            topic=f"{metric_name} 数据分析",
            source_date=date.today(),
            source_metrics=json.dumps([metric_key], ensure_ascii=False),
            source_game_ids=json.dumps([], ensure_ascii=False),
            status="draft",
            priority=35,
            llm_model=None,
            admin_comments=json.dumps(comments, ensure_ascii=False),
            created_at=now,
            updated_at=now,
        )
        s.add(post)
        s.commit()
        return post.id, brief_timestamp


def _social_post_image_spec(raw_spec: str | None) -> tuple[object | None, str | None]:
    if not raw_spec:
        return None, None
    try:
        parsed = json.loads(raw_spec)
    except Exception:
        return None, str(raw_spec)
    return parsed, json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True)


def _social_post_image_url(post_id: int, img) -> str | None:
    file_path = getattr(img, "file_path", None)
    if not file_path:
        return None
    filename = os.path.basename(str(file_path).strip()) or f"{getattr(img, 'slot', 'image')}.png"
    return f"/media/social_posts/{post_id}/{filename}"


def _validate_prepared_image_specs(raw_images: list[dict]) -> list[dict[str, object]]:
    prepared: list[dict[str, object]] = []
    seen_slots: set[str] = set()
    for idx, img_spec in enumerate(raw_images, start=1):
        slot = (img_spec.get("slot") or "").strip()
        image_type = (img_spec.get("type") or "").strip()
        if not slot:
            raise ValueError(f"images[{idx}] slot required")
        if slot in seen_slots:
            raise ValueError(f"duplicate image slot: {slot}")
        seen_slots.add(slot)
        if not image_type:
            raise ValueError(f"images[{idx}] type required")
        source_path = (img_spec.get("file_path") or "").strip()
        if not source_path:
            raise ValueError(f"images[{idx}] file_path required")
        source = Path(source_path).expanduser()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Prepared image file not found: {source}")
        note = (img_spec.get("note") or "").strip() or None
        spec_json = json.dumps(
            {k: v for k, v in img_spec.items() if k not in ("slot", "note", "file_path", "is_enabled")},
            ensure_ascii=False,
        )
        prepared.append(
            {
                "slot": slot,
                "image_type": image_type,
                "note": note,
                "source_path": str(source),
                "is_enabled": bool(img_spec.get("is_enabled", True)),
                "spec_json": spec_json,
            }
        )
    return prepared


def _remove_managed_post_image_file(file_path: str | None, *, post_id: int) -> None:
    if not file_path:
        return
    candidate = Path(file_path).expanduser()
    try:
        candidate.resolve().relative_to((Path(__file__).resolve().parent.parent / "media" / "social_posts" / str(post_id)).resolve())
    except Exception:
        return
    candidate.unlink(missing_ok=True)


def _extract_image_slots_from_content(content_raw: str | None) -> list[str]:
    if not content_raw:
        return []
    return re.findall(r"\[\[IMAGE:slot=([A-Za-z0-9_]+)\]\]", str(content_raw))


def _truncate_image_error_text(text: str | None, limit: int = 120) -> str | None:
    value = str(text or "").strip()
    if not value:
        return None
    compact = re.sub(r"\s+", " ", value)
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _classify_auto_review_reason(reason: str | None) -> tuple[str, str]:
    cleaned = str(reason or "").strip()
    lowered = cleaned.lower()
    if any(token in lowered for token in ("watermark", "branding", "stock-photo overlay", "agency branding", "usa today sports", "visible agency", "brand")):
        return "Auto-review: Watermark / branding", "Contains visible watermark, branding, or stock-photo overlay."
    if any(token in lowered for token in ("wrong player", "wrong team", "wrong context", "unrelated", "does not clearly match", "not clearly match", "not tied to", "reliable match")):
        return "Auto-review: Wrong player / team", "Does not clearly match the requested player, team, or game context."
    if any(token in lowered for token in ("ai-generated", "synthetic", "illustration", "illustrated", "uncanny", "promo art", "posed promotional", "posed promo", "editorial photo")):
        return "Auto-review: Synthetic / promo", "Looks synthetic, illustrated, or promo-like instead of a clean editorial photo."
    if any(token in lowered for token in ("broadcast screenshot", "scoreboard", "tv graphics", "collage", "graphic", "story promo", "ui screenshot", "team-logo", "logo")):
        return "Auto-review: Graphic / screenshot", "Looks like a graphic, collage, screenshot, or other non-photo asset."
    if any(token in lowered for token in ("not clearly basketball", "not basketball", "arena map", "seating chart")):
        return "Auto-review: Not a usable basketball photo", "Does not look like a clean basketball image for publishing."
    return "Auto-review rejected", _truncate_image_error_text(cleaned) or "Failed the image quality review."


def _social_post_image_error_view(error_message: str | None, *, is_enabled: bool) -> dict[str, str | None]:
    message = str(error_message or "").strip()
    if not message:
        if is_enabled:
            return {"error_title": None, "error_summary": None}
        return {"error_title": "Disabled", "error_summary": "No explicit rejection reason was saved."}

    if message.startswith("Auto-review rejected"):
        reason = message.split(":", 1)[1].strip() if ":" in message else ""
        title, summary = _classify_auto_review_reason(reason)
        return {"error_title": title, "error_summary": summary}

    if "input_fidelity" in message:
        return {
            "error_title": "AI generation failed",
            "error_summary": "OpenAI rejected the input_fidelity parameter in the image-edit request.",
        }

    if message.startswith("Page.goto: Timeout"):
        return {
            "error_title": "Screenshot timeout",
            "error_summary": "Page capture timed out before the target page finished loading.",
        }

    if "Official player headshot unavailable" in message:
        return {
            "error_title": "Headshot unavailable",
            "error_summary": "Could not download the official NBA headshot for this player.",
        }

    if message.startswith("Error code:"):
        inner_message = None
        inner_match = re.search(r"'message': \"([^\"]+)\"", message)
        if inner_match:
            inner_message = inner_match.group(1)
        return {
            "error_title": "Image generation failed",
            "error_summary": _truncate_image_error_text(inner_message or message),
        }

    return {
        "error_title": "Image error",
        "error_summary": _truncate_image_error_text(message),
    }


def _social_post_image_view(post_id: int, img) -> dict[str, object]:
    spec, spec_text = _social_post_image_spec(getattr(img, "spec", None))
    file_path = getattr(img, "file_path", None)
    file_name = os.path.basename(str(file_path).strip()) if file_path else None
    error_view = _social_post_image_error_view(getattr(img, "error_message", None), is_enabled=bool(img.is_enabled))
    return {
        "id": img.id,
        "slot": img.slot,
        "image_type": img.image_type,
        "note": img.note,
        "is_enabled": bool(img.is_enabled),
        "has_file": bool(file_path),
        "error_message": img.error_message,
        "error_title": error_view["error_title"],
        "error_summary": error_view["error_summary"],
        "review_decision": getattr(img, "review_decision", None),
        "review_reason": getattr(img, "review_reason", None),
        "review_source": getattr(img, "review_source", None),
        "reviewed_at": img.reviewed_at.isoformat() if getattr(img, "reviewed_at", None) else None,
        "url": _social_post_image_url(post_id, img),
        "file_path": str(file_path).strip() if file_path else None,
        "file_name": file_name,
        "spec": spec,
        "spec_text": spec_text,
    }


def _normalize_image_review_source(value: str | None) -> str | None:
    source = str(value or "").strip()
    return source or None


def _apply_image_review_metadata(
    img,
    *,
    decision: str | None,
    reason: str | None,
    source: str | None,
    reviewed_at: datetime | None,
) -> None:
    if decision is not None:
        img.review_decision = decision
    if reason is not None:
        img.review_reason = reason
    if source is not None:
        img.review_source = source
    if reviewed_at is not None:
        img.reviewed_at = reviewed_at


def _is_valid_hupu_thread_url(url: str | None) -> bool:
    if not url:
        return False
    candidate = str(url).strip()
    return bool(re.fullmatch(r"https://bbs\.hupu\.com/\d{6,12}\.html(?:[?#].*)?", candidate))


def _normalize_reddit_forum(forum: str | None) -> str | None:
    from social_media.reddit.forums import normalize_reddit_subreddit
    return normalize_reddit_subreddit(forum)


def _reddit_english_audience_hint(audience_hint: str | None, *, forum: str | None) -> str:
    normalized_forum = _normalize_reddit_forum(forum)
    forum_label = f"r/{normalized_forum}" if normalized_forum else "Reddit"
    existing = str(audience_hint or "").strip()
    lowered = existing.lower()
    if "reddit" in lowered and "english" in lowered:
        return existing
    suffix = f"write in English for {forum_label} readers"
    if existing:
        if suffix.lower() in lowered:
            return existing
        return f"{existing}; {suffix}"
    return f"English-language {forum_label} readers"


def _social_post_delivery_view(delivery) -> dict[str, object]:
    status = delivery.status
    published_url = delivery.published_url
    error_message = delivery.error_message
    if delivery.platform == "hupu" and status == "published" and not _is_valid_hupu_thread_url(published_url):
        status = "failed"
        published_url = None
        if not error_message:
            bad_url = delivery.published_url or "<missing>"
            error_message = f"Invalid Hupu published_url recorded: {bad_url}"
    return {
        "id": delivery.id,
        "platform": delivery.platform,
        "forum": delivery.forum,
        "is_enabled": bool(getattr(delivery, "is_enabled", True)),
        "status": status,
        "content_final": getattr(delivery, "content_final", None),
        "published_url": published_url,
        "published_at": delivery.published_at.isoformat() if getattr(delivery, "published_at", None) else None,
        "error_message": error_message,
    }


def _load_social_post_bundle(db_sess, post_id: int):
    post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
    if not post:
        return None
    variants = (
        db_sess.query(SocialPostVariant)
        .filter(SocialPostVariant.post_id == post_id)
        .order_by(SocialPostVariant.id)
        .all()
    )
    variant_ids = [v.id for v in variants]
    deliveries = (
        db_sess.query(SocialPostDelivery)
        .filter(SocialPostDelivery.variant_id.in_(variant_ids))
        .order_by(SocialPostDelivery.id)
        .all()
        if variant_ids
        else []
    )
    deliveries_by_variant: dict[int, list] = {}
    for delivery in deliveries:
        deliveries_by_variant.setdefault(delivery.variant_id, []).append(delivery)
    try:
        source_metrics = json.loads(post.source_metrics) if post.source_metrics else []
    except Exception:
        source_metrics = []
    try:
        source_game_ids = json.loads(post.source_game_ids) if post.source_game_ids else []
    except Exception:
        source_game_ids = []
    images = (
        db_sess.query(SocialPostImage)
        .filter(SocialPostImage.post_id == post_id)
        .order_by(SocialPostImage.id)
        .all()
    )
    snapshot = {
        "id": post.id,
        "topic": post.topic,
        "source_date": post.source_date.isoformat() if post.source_date else None,
        "status": post.status,
        "priority": post.priority,
        "source_metrics": source_metrics,
        "source_game_ids": source_game_ids,
        "variants": [
            {
                "id": variant.id,
                "title": variant.title,
                "audience_hint": variant.audience_hint,
                "content_raw": variant.content_raw,
                "destinations": [
                    {
                        "id": delivery.id,
                        "platform": delivery.platform,
                        "forum": delivery.forum,
                        "status": delivery.status,
                    }
                    for delivery in deliveries_by_variant.get(variant.id, [])
                ],
            }
            for variant in variants
        ],
        "images": [
            {
                "id": img.id,
                "slot": img.slot,
                "image_type": img.image_type,
                "note": img.note,
                "is_enabled": bool(img.is_enabled),
                "has_file": bool(img.file_path),
                "spec": _social_post_image_spec(getattr(img, "spec", None))[0],
            }
            for img in images
        ],
    }
    return post, snapshot


def _build_social_post_rows(db_sess, posts: list[SocialPost]) -> list[dict[str, object]]:
    post_ids = [p.id for p in posts]
    variants = db_sess.query(SocialPostVariant).filter(
        SocialPostVariant.post_id.in_(post_ids)
    ).all() if post_ids else []
    variant_ids = [v.id for v in variants]
    deliveries = db_sess.query(SocialPostDelivery).filter(
        SocialPostDelivery.variant_id.in_(variant_ids)
    ).all() if variant_ids else []
    images = db_sess.query(SocialPostImage).filter(
        SocialPostImage.post_id.in_(post_ids)
    ).order_by(SocialPostImage.id).all() if post_ids else []

    v_by_post: dict[int, list] = {}
    for v in variants:
        v_by_post.setdefault(v.post_id, []).append(v)
    d_by_variant: dict[int, list] = {}
    for d in deliveries:
        d_by_variant.setdefault(d.variant_id, []).append(d)
    img_by_post: dict[int, list] = {}
    for img in images:
        img_by_post.setdefault(img.post_id, []).append(img)

    rows = []
    for p in posts:
        pvariants = v_by_post.get(p.id, [])
        rows.append({
            "id": p.id,
            "topic": p.topic,
            "source_date": p.source_date.isoformat() if p.source_date else "",
            "source_metrics": json.loads(p.source_metrics) if p.source_metrics else [],
            "source_game_ids": json.loads(p.source_game_ids) if p.source_game_ids else [],
            "status": p.status,
            "priority": p.priority,
            "admin_comments": _social_post_comments(p),
            "llm_model": p.llm_model,
            "created_at": p.created_at,
            "workflow": _paperclip_workflow_view(p),
            "variant_count": len(pvariants),
            "variants": [
                {
                    "id": v.id,
                    "title": v.title,
                    "content_raw": v.content_raw,
                    "audience_hint": v.audience_hint,
                    "deliveries": [_social_post_delivery_view(d) for d in d_by_variant.get(v.id, [])],
                }
                for v in pvariants
            ],
            "images": [_social_post_image_view(p.id, img) for img in img_by_post.get(p.id, [])],
        })
    return rows


def _apply_paperclip_issue_fields(post: SocialPost, issue: dict | None, *, sync_error: str | None = None) -> None:
    if issue:
        post.paperclip_issue_id = issue.get("id") or post.paperclip_issue_id
        post.paperclip_issue_identifier = issue.get("identifier") or post.paperclip_issue_identifier
        post.paperclip_issue_status = issue.get("status") or post.paperclip_issue_status
        post.paperclip_assignee_agent_id = issue.get("assigneeAgentId")
        post.paperclip_assignee_user_id = issue.get("assigneeUserId")
    post.paperclip_last_synced_at = datetime.utcnow()
    post.paperclip_sync_error = sync_error
    post.updated_at = datetime.utcnow()


def _paperclip_client_or_raise() -> tuple[PaperclipClient, object]:
    cfg = _paperclip_bridge_config()
    if cfg is None:
        raise PaperclipBridgeError("Paperclip bridge is not configured.")
    client = PaperclipClient(cfg)
    resolved_cfg = client.discover_defaults()
    if not resolved_cfg.company_id:
        raise PaperclipBridgeError("Paperclip bridge could not resolve company_id.")
    if not resolved_cfg.project_id:
        raise PaperclipBridgeError("Paperclip bridge could not resolve the Funba project in Paperclip.")
    return client, resolved_cfg


def _ensure_paperclip_issue_for_post(post_id: int) -> None:
    try:
        client, cfg = _paperclip_client_or_raise()
        with SessionLocal() as db_sess:
            bundle = _load_social_post_bundle(db_sess, post_id)
            if bundle is None:
                return
            post, snapshot = bundle
            desired_state = desired_issue_state_for_post(snapshot, cfg)
            payload = {
                "projectId": cfg.project_id,
                "title": build_post_issue_title(snapshot),
                "description": build_post_issue_description(snapshot),
                "status": desired_state.status,
                "priority": "medium",
                "assigneeAgentId": desired_state.assignee_agent_id,
                "assigneeUserId": desired_state.assignee_user_id,
            }
            if post.paperclip_issue_id:
                issue = client.update_issue(post.paperclip_issue_id, payload)
            else:
                issue = client.create_issue(payload)
            warning_text = "\n".join(desired_state.warnings) if desired_state.warnings else None
            _apply_paperclip_issue_fields(post, issue, sync_error=warning_text)
            db_sess.commit()
    except PaperclipBridgeError as exc:
        logger.warning("Failed to ensure Paperclip issue for SocialPost %s: %s", post_id, exc)
        with SessionLocal() as db_sess:
            post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
            if post:
                _apply_paperclip_issue_fields(post, None, sync_error=str(exc))
                db_sess.commit()


def _mirror_paperclip_comment(post_id: int, *, text: str, local_comment_timestamp: str) -> None:
    try:
        client, _cfg = _paperclip_client_or_raise()
        _ensure_paperclip_issue_for_post(post_id)
        with SessionLocal() as db_sess:
            post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post or not post.paperclip_issue_id:
                return
            remote_comment = client.add_comment(post.paperclip_issue_id, text)
            comments = _social_post_comments(post)
            idx = _find_comment_index(
                comments,
                timestamp=local_comment_timestamp,
                text=text,
                origin="funba_user",
            )
            if idx is not None:
                comments[idx]["paperclip_comment_id"] = remote_comment.get("id")
            if remote_comment.get("id"):
                post.paperclip_last_comment_id = remote_comment.get("id")
            _apply_paperclip_issue_fields(post, None, sync_error=None)
            _write_social_post_comments(post, comments)
            db_sess.commit()
    except PaperclipBridgeError as exc:
        logger.warning("Failed to mirror Paperclip comment for SocialPost %s: %s", post_id, exc)
        with SessionLocal() as db_sess:
            post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
            if post:
                _apply_paperclip_issue_fields(post, None, sync_error=str(exc))
                db_sess.commit()


def _handoff_social_post(post_id: int, *, action: str, local_comment_timestamp: str, local_comment_text: str) -> None:
    try:
        client, cfg = _paperclip_client_or_raise()
        with SessionLocal() as db_sess:
            bundle = _load_social_post_bundle(db_sess, post_id)
            if bundle is None:
                return
            post, snapshot = bundle
            desired_state = desired_issue_state_for_post(snapshot, cfg)
        _ensure_paperclip_issue_for_post(post_id)
        with SessionLocal() as db_sess:
            bundle = _load_social_post_bundle(db_sess, post_id)
            if bundle is None:
                return
            post, snapshot = bundle
            desired_state = desired_issue_state_for_post(snapshot, cfg)
            if not post.paperclip_issue_id:
                raise PaperclipBridgeError("Paperclip issue was not created for this post.")
            payload = {
                "title": build_post_issue_title(snapshot),
                "description": build_post_issue_description(snapshot),
                "status": desired_state.status,
                "assigneeAgentId": desired_state.assignee_agent_id,
                "assigneeUserId": desired_state.assignee_user_id,
                "comment": build_status_handoff_comment(
                    post=snapshot,
                    action=action,
                    actor_name=_paperclip_actor_name(),
                    desired_state=desired_state,
                ),
            }
            issue = client.update_issue(post.paperclip_issue_id, payload)
            if desired_state.assignee_agent_id:
                try:
                    client.wake_agent(
                        desired_state.assignee_agent_id,
                        reason=action,
                        payload={"issueId": post.paperclip_issue_id},
                        force_fresh_session=True,
                    )
                except Exception as wake_exc:
                    logger.warning("wake_agent failed for SocialPost %s: %s", post_id, wake_exc)
            comments = _social_post_comments(post)
            idx = _find_comment_index(
                comments,
                timestamp=local_comment_timestamp,
                text=local_comment_text,
                origin="system",
            )
            remote_comment = issue.get("comment") if isinstance(issue.get("comment"), dict) else None
            if idx is not None and remote_comment and remote_comment.get("id"):
                comments[idx]["paperclip_comment_id"] = remote_comment.get("id")
            if remote_comment and remote_comment.get("id"):
                post.paperclip_last_comment_id = remote_comment.get("id")
            warning_text = "\n".join(desired_state.warnings) if desired_state.warnings else None
            _apply_paperclip_issue_fields(post, issue, sync_error=warning_text)
            _write_social_post_comments(post, comments)
            db_sess.commit()
    except PaperclipBridgeError as exc:
        logger.warning("Failed to hand off SocialPost %s to Paperclip: %s", post_id, exc)
        with SessionLocal() as db_sess:
            post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
            if post:
                _apply_paperclip_issue_fields(post, None, sync_error=str(exc))
                db_sess.commit()


def _sync_social_post_from_paperclip(post_id: int, *, ensure_issue: bool = True) -> dict[str, object] | None:
    if ensure_issue:
        _ensure_paperclip_issue_for_post(post_id)
    try:
        client, cfg = _paperclip_client_or_raise()
        with SessionLocal() as db_sess:
            post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
            if not post:
                return None
            if not post.paperclip_issue_id:
                _apply_paperclip_issue_fields(post, None, sync_error="Paperclip issue not linked for this post.")
                db_sess.commit()
                return {"workflow": _paperclip_workflow_view(post), "comments": _social_post_comments(post)}
            issue = client.get_issue(post.paperclip_issue_id)
            remote_comments = client.list_comments(post.paperclip_issue_id, after_comment_id=post.paperclip_last_comment_id)
            comments = _social_post_comments(post)
            changed = merge_paperclip_comments(comments, remote_comments, cfg=cfg)
            if remote_comments:
                post.paperclip_last_comment_id = remote_comments[-1].get("id") or post.paperclip_last_comment_id
            _apply_paperclip_issue_fields(post, issue, sync_error=None)
            if changed:
                _write_social_post_comments(post, comments)
            db_sess.commit()
            return {"workflow": _paperclip_workflow_view(post), "comments": _social_post_comments(post)}
    except PaperclipBridgeError as exc:
        logger.warning("Failed to sync SocialPost %s from Paperclip: %s", post_id, exc)
        with SessionLocal() as db_sess:
            post = db_sess.query(SocialPost).filter(SocialPost.id == post_id).first()
            if post:
                _apply_paperclip_issue_fields(post, None, sync_error=str(exc))
                db_sess.commit()
                return {"workflow": _paperclip_workflow_view(post), "comments": _social_post_comments(post)}
    return None


def _season_year(season) -> int | None:
    """Extract the NBA season start year from a season code like '22023' -> 2023."""
    s = str(season or "").strip()
    if len(s) == 5 and s.isdigit():
        return int(s[1:])
    return None


def _team_name_for_year(team_id, year=None):
    """Jinja helper: era-appropriate full team name for a given season year.
    Falls back to the current DB team name when no era covers that year.
    """
    if not team_id:
        return "-"
    from web.historical_team_locations import get_era_name_for_year
    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None
    era_name = get_era_name_for_year(str(team_id), y)
    if era_name:
        return era_name
    with SessionLocal() as session:
        tm = _team_map(session)
    return _team_name(tm, str(team_id))


def _team_abbr_for_year(team_id, year=None):
    """Jinja helper: era-appropriate abbreviation for a given season year.
    Falls back to the current DB team abbr when no era covers that year.
    """
    if not team_id:
        return "-"
    from web.historical_team_locations import get_era_abbr_for_year
    try:
        y = int(year) if year is not None else None
    except (TypeError, ValueError):
        y = None
    era_abbr = get_era_abbr_for_year(str(team_id), y)
    if era_abbr:
        return era_abbr
    with SessionLocal() as session:
        tm = _team_map(session)
    return _team_abbr(tm, str(team_id))


def _team_logo(team_id: str | None, year: int | None = None) -> str:
    """Jinja helper: URL for a team's logo at a given season year.

    Falls through the FRANCHISE_LOGOS registry (historical era match →
    current-era local file) with a final defensive fallback to the NBA CDN.
    When `year` is None, returns the current-era logo.
    """
    if not team_id:
        return ""
    from web.historical_team_locations import get_logo_url_for_year, get_current_logo
    from datetime import date as _date
    if year is None:
        today = _date.today()
        year = today.year if today.month >= 7 else today.year - 1
    try:
        year_int = int(year)
    except (TypeError, ValueError):
        current = get_current_logo(str(team_id))
        if current is None:
            return f"https://cdn.nba.com/logos/nba/{team_id}/global/L/logo.svg"
        return "/" + current["path"]
    return get_logo_url_for_year(str(team_id), year_int, static_prefix="/")


@app.context_processor
def inject_template_helpers():
    from datetime import date
    with SessionLocal() as session:
        paperclip_issue_base_url = get_paperclip_issue_base_url(session)
    return {
        "season_label": _season_label,
        "lang": _current_lang(),
        "t": _t,
        "url_for": _localized_url_for,
        "display_player_name": _display_player_name,
        "display_team_name": _display_team_name,
        "toggle_language_url": _language_toggle_url(),
        "toggle_language_label": _t("中文", "EN"),
        "active_public_endpoint": _base_public_endpoint(request.endpoint) if has_request_context() else None,
        "is_admin": is_admin(),
        "is_pro": is_pro(),
        "current_user": _current_user(),
        "site_name": _t("FUNBA", "智趣NBA"),
        "current_year": date.today().year,
        "clean_key": _strip_draft_prefix,
        "paperclip_issue_base_url": paperclip_issue_base_url,
        "paperclip_issue_url": lambda identifier: build_paperclip_issue_url(identifier, paperclip_issue_base_url),
        "canonical_url": _canonical_url(),
        "hreflang_links": _hreflang_links(),
        "player_url": _player_url,
        "game_url": _game_url,
        "team_url": _team_url,
        "game_slug_map": _ensure_game_slug_cache(),
        "team_logo": _team_logo,
        "season_year": _season_year,
        "team_name_for_year": _team_name_for_year,
        "team_abbr_for_year": _team_abbr_for_year,
    }


# ── Auth routes ──────────────────────────────────────────────────────────────
_auth_views = register_auth_routes(
    app,
    get_session_local=lambda: SessionLocal,
    get_oauth=lambda: oauth,
    get_logger=lambda: logger,
    get_user_model=lambda: User,
    get_magic_token_model=lambda: MagicToken,
    create_user_id=lambda: str(_uuid_mod.uuid4()),
    limiter=limiter,
)
auth_login = _auth_views.auth_login
auth_google = _auth_views.auth_google
auth_callback = _auth_views.auth_callback
auth_magic_send = _auth_views.auth_magic_send
auth_magic_verify = _auth_views.auth_magic_verify
auth_logout = _auth_views.auth_logout


# ── Subscription routes ──────────────────────────────────────────────────────

_billing_views = register_billing_routes(
    app,
    get_session_local=lambda: SessionLocal,
    get_current_user=lambda: _current_user(),
    get_localized_url_for=_localized_url_for,
    get_user_model=lambda: User,
    get_logger=lambda: logger,
)
pricing = _billing_views.pricing
account_page = _billing_views.account_page
subscribe_checkout = _billing_views.subscribe_checkout
subscribe_portal = _billing_views.subscribe_portal
stripe_webhook = _billing_views.stripe_webhook
_get_stripe_price = _billing_views.get_stripe_price
_handle_stripe_event = _billing_views.handle_stripe_event
_on_checkout_completed = _billing_views.on_checkout_completed
_on_subscription_changed = _billing_views.on_subscription_changed
_on_payment_failed = _billing_views.on_payment_failed


# ── Feedback routes ───────────────────────────────────────────────────────────

_feedback_views = register_feedback_routes(
    app,
    get_session_local=lambda: SessionLocal,
    get_current_user=lambda: _current_user(),
    get_feedback_model=lambda: Feedback,
    get_user_model=lambda: User,
    require_admin_page=_require_admin_page,
    limiter=limiter,
)
submit_feedback = _feedback_views.submit_feedback
admin_feedback = _feedback_views.admin_feedback


# ── Page routes ───────────────────────────────────────────────────────────────

_public_views = register_public_routes(
    app,
    get_session_local=lambda: SessionLocal,
    get_render_template=lambda: render_template,
    get_team_model=lambda: Team,
    get_game_model=lambda: Game,
    get_team_game_stats_model=lambda: TeamGameStats,
    get_player_game_stats_model=lambda: PlayerGameStats,
    get_metric_result_model=lambda: MetricResultModel,
    get_game_pbp_model=lambda: GamePlayByPlay,
    get_player_model=lambda: Player,
    get_award_model=lambda: Award,
    get_team_map=_team_map,
    get_season_sort_key=lambda: _season_sort_key,
    get_franchise_display=lambda: _franchise_display,
    get_display_team_name=lambda: _display_team_name,
    get_season_label=lambda: _season_label,
    get_is_zh=lambda: _is_zh(),
    get_team_map_positions=lambda: _TEAM_MAP_POSITIONS,
    get_fmt_date=lambda: _fmt_date,
    get_coerce_award_season=lambda: _coerce_award_season,
    get_award_type_meta=lambda: _AWARD_TYPE_META,
    get_award_order_case=lambda: _award_order_case,
    get_award_entry_from_row=lambda: _award_entry_from_row,
    get_group_award_entries=lambda: _group_award_entries,
    get_award_tab_groups=lambda: _AWARD_TAB_GROUPS,
    get_award_type_label=lambda: _award_type_label,
    limiter=limiter,
    get_pct_text=lambda: _pct_text,
    get_pick_current_season=lambda: _pick_current_season,
    get_team_abbr=lambda: _team_abbr,
    get_metric_name_for_key=lambda: _metric_name_for_key,
    get_asc_metric_keys=lambda: _asc_metric_keys,
    get_metric_results=lambda: _get_metric_results,
    get_player_headshot_url=lambda: _player_headshot_url,
    get_localized_url_for=lambda: _localized_url_for,
    get_t=lambda: _t,
    get_pct_fmt=lambda: pct_fmt,
)
home = _public_views.home
games_list = _public_views.games_list
awards_page = _public_views.awards_page
player_hints_api = _public_views.player_hints_api
players_browse = _public_views.players_browse
players_compare = _public_views.players_compare
draft_page = _public_views.draft_page
_player_stat_summary = _public_views.player_stat_summary
_player_career_summary = _public_views.player_career_summary
_latest_regular_season = _public_views.latest_regular_season
_build_compare_stat_rows = _public_views.build_compare_stat_rows
_build_compare_current_rows = _public_views.build_compare_current_rows
_build_compare_metric_sections = _public_views.build_compare_metric_sections
_player_compare_team_abbrs = _public_views.player_compare_team_abbrs
_derive_player_top_rankings = _public_views.derive_player_top_rankings


_detail_views = register_detail_routes(
    app,
    get_session_local=lambda: SessionLocal,
    get_render_template=lambda: render_template,
    get_player_model=lambda: Player,
    get_award_model=lambda: Award,
    get_game_model=lambda: Game,
    get_shot_record_model=lambda: ShotRecord,
    get_player_game_stats_model=lambda: PlayerGameStats,
    get_player_salary_model=lambda: PlayerSalary,
    get_team_model=lambda: Team,
    get_team_game_stats_model=lambda: TeamGameStats,
    get_game_pbp_model=lambda: GamePlayByPlay,
    get_team_map=lambda: _team_map,
    get_award_order_case=lambda: _award_order_case,
    get_award_badge_label=lambda: _award_badge_label,
    get_player_career_summary=lambda: _player_career_summary,
    get_build_shot_zone_heatmap=lambda: _build_shot_zone_heatmap,
    get_season_sort_key=lambda: _season_sort_key,
    get_season_label=lambda: _season_label,
    get_is_pro=lambda: is_pro,
    get_pick_current_season=lambda: _pick_current_season,
    get_team_abbr=lambda: _team_abbr,
    get_fmt_date=lambda: _fmt_date,
    get_player_status=lambda: _player_status,
    get_fmt_minutes=lambda: _fmt_minutes,
    get_localized_url_for=lambda: _localized_url_for,
    get_metric_results=lambda: _get_metric_results,
    get_season_start_year_label=lambda: _season_start_year_label,
    get_season_year_label=lambda: _season_year_label,
    get_coerce_award_season=lambda: _coerce_award_season,
    get_team_name=lambda: _team_name,
    get_display_player_name=lambda: _display_player_name,
    get_pbp_event_type_label=lambda: _pbp_event_type_label,
    get_pbp_text=lambda: _pbp_text,
    get_paperclip_issue_url=lambda: _paperclip_issue_url,
    get_game_analysis_issue_history=lambda: game_analysis_issue_history,
    get_player_headshot_url=lambda: _player_headshot_url,
    get_logger=lambda: logger,
)
player_page = _detail_views.player_page
team_page = _detail_views.team_page
game_page = _detail_views.game_page
game_fragment_metrics = _detail_views.game_fragment_metrics


_metrics_read_views = register_metrics_read_routes(
    app,
    get_session_local=lambda: SessionLocal,
    get_render_template=lambda: render_template,
    get_current_user=lambda: _current_user(),
    get_catalog_metrics_page=lambda: _catalog_metrics_page,
    get_catalog_top3=lambda: _catalog_top3,
    get_catalog_metrics_total=lambda: _catalog_metrics_total,
    get_feature_access_config=lambda: get_feature_access_config,
    get_build_metric_feature_context=lambda: _build_metric_feature_context,
    get_llm_model_for_purpose=lambda: get_llm_model_for_purpose,
    get_available_llm_models=lambda: available_llm_models,
    get_metric_definition_model=lambda: MetricDefinitionModel,
    get_game_model=lambda: Game,
    get_require_login_page=lambda: _require_login_page,
    get_require_metric_creator_page=lambda: _require_metric_creator_page,
    get_pick_current_season=_pick_current_season,
    get_family_variant_season=lambda: FAMILY_VARIANT_SEASON,
    get_family_variant_career=lambda: FAMILY_VARIANT_CAREER,
    get_family_base_key=family_base_key,
    get_metrics_catalog_page_size=lambda: _METRICS_CATALOG_PAGE_SIZE,
)
metrics_browse = _metrics_read_views.metrics_browse
api_metrics_catalog = _metrics_read_views.api_metrics_catalog
api_metrics_catalog_count = _metrics_read_views.api_metrics_catalog_count
my_metrics = _metrics_read_views.my_metrics
metric_new = _metrics_read_views.metric_new
metric_edit = _metrics_read_views.metric_edit


_metrics_write_views = register_metrics_write_routes(
    app,
    SimpleNamespace(
        limiter=lambda: limiter,
        require_feature_json=lambda: _require_feature_json,
        require_metric_creator_json=lambda: _require_metric_creator_json,
        require_admin_json=lambda: _require_admin_json,
        session_local=lambda: SessionLocal,
        catalog_metrics=lambda: _catalog_metrics,
        resolve_llm_model=lambda: resolve_llm_model,
        record_ai_usage_event=lambda: _record_ai_usage_event,
        ai_usage_preview=lambda: _ai_usage_preview,
        logger=lambda: logger,
        is_admin=lambda: is_admin,
        current_user=lambda: _current_user,
        preview_code_metric=lambda: _preview_code_metric,
        resolve_game_entity_names=lambda: _resolve_game_entity_names,
        team_map=lambda: _team_map,
        team_name=lambda: _team_name,
        team_abbr=lambda: _team_abbr,
        player_model=lambda: Player,
        team_model=lambda: Team,
        game_model=lambda: Game,
        metric_definition_model=lambda: MetricDefinitionModel,
        metric_result_model=lambda: MetricResultModel,
        metric_compute_run_model=lambda: MetricComputeRun,
        metric_run_log_model=lambda: MetricRunLog,
        player_game_stats_model=lambda: PlayerGameStats,
        team_game_stats_model=lambda: TeamGameStats,
        code_metric_metadata_from_code=lambda: _code_metric_metadata_from_code,
        make_draft_key=lambda: _make_draft_key,
        replace_key_in_code=lambda: _replace_key_in_code,
        is_reserved_career_key=lambda: is_reserved_career_key,
        metric_supports_career=lambda: _metric_supports_career,
        family_career_key=lambda: family_career_key,
        family_variant_career=lambda: FAMILY_VARIANT_CAREER,
        family_variant_season=lambda: FAMILY_VARIANT_SEASON,
        sync_metric_family=lambda: _sync_metric_family,
        metric_family_rows=lambda: _metric_family_rows,
        metric_family_base_row=lambda: _metric_family_base_row,
        strip_draft_prefix=lambda: _strip_draft_prefix,
        is_draft_key=lambda: _is_draft_key,
        dispatch_metric_backfill=lambda: _dispatch_metric_backfill,
        build_metric_backfill_status=lambda: _build_metric_backfill_status,
        engine=lambda: engine,
    ),
)
api_metric_search = _metrics_write_views.api_metric_search
api_metric_check_similar = _metrics_write_views.api_metric_check_similar
api_metric_generate = _metrics_write_views.api_metric_generate
api_metric_preview = _metrics_write_views.api_metric_preview
api_metric_create = _metrics_write_views.api_metric_create
api_metric_publish = _metrics_write_views.api_metric_publish
api_admin_toggle_metric_enabled = _metrics_write_views.api_admin_toggle_metric_enabled
api_qualifying_games = _metrics_write_views.api_qualifying_games
api_metric_backfill_status = _metrics_write_views.api_metric_backfill_status
api_metric_update = _metrics_write_views.api_metric_update


def _resolve_game_entity_names(session, entity_ids: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve game entity IDs (simple or composite) to readable names.

    Handles:
      "0022500826"                    → "PHX 77 - POR 92 (Feb 22)"
      "0022500826:1610612756:Q1"      → "PHX Q1 — PHX vs POR Feb 22"
    """
    # Collect unique game IDs
    game_ids = set()
    for eid in entity_ids:
        game_ids.add(eid.split(":")[0])

    # Bulk fetch games and teams
    games = {
        g.game_id: g for g in session.query(Game)
        .filter(Game.game_id.in_(game_ids)).all()
    }
    tm = _team_map(session)

    names = {}
    dates = {}
    for eid in entity_ids:
        parts = eid.split(":")
        gid = parts[0]
        game = games.get(gid)
        if not game:
            names[eid] = eid
            dates[eid] = ""
            continue

        home_team = tm.get(game.home_team_id)
        road_team = tm.get(game.road_team_id)
        home_abbr = (home_team.abbr if home_team and home_team.abbr else "?")
        road_abbr = (road_team.abbr if road_team and road_team.abbr else "?")
        date_str = game.game_date.strftime("%Y-%m-%d") if game.game_date else ""

        if len(parts) == 1:
            # Simple game ID
            h_score = game.home_team_score or 0
            r_score = game.road_team_score or 0
            names[eid] = f"{home_abbr} {h_score} - {road_abbr} {r_score}"
        else:
            # Composite: game_id:team_id:qualifier — just show matchup
            names[eid] = f"{home_abbr} vs {road_abbr}"

        # Always include date as separate field
        dates[eid] = date_str

    return names, dates


def _preview_code_metric(
    session,
    code_python: str,
    scope: str,
    season: str,
    limit: int = 25,
    rank_order_override: str | None = None,
    season_types_override=None,
):
    """Run a code-based metric against sample games and return ranked results."""
    from metrics.framework.runtime import ReadOnlySession, load_code_metric
    from metrics.framework.base import season_matches_metric_types

    metadata = _code_metric_metadata_from_code(
        code_python,
        rank_order_override=rank_order_override,
        season_types_override=season_types_override,
    )
    metric = load_code_metric(metadata["code_python"])
    if season and season != "all" and not season_matches_metric_types(season, metadata.get("season_types")):
        return []
    ro_session = ReadOnlySession(session)
    rank_order = metadata["rank_order"]
    ctx_template = metadata.get("context_label_template")
    _reverse = rank_order == "desc"

    def _row(r):
        row = {
            "entity_id": r.entity_id,
            "value_num": round(r.value_num, 4),
            "value_str": r.value_str,
            "baseline": None,
        }
        if ctx_template and r.context:
            try:
                fmt_ctx = {k: _fmt_int(v) if isinstance(v, (int, float)) else v for k, v in r.context.items()}
                row["context_label"] = ctx_template.format_map(fmt_ctx)
            except Exception:
                pass
        return row

    # trigger="season" — one call handles everything regardless of scope
    if getattr(metric, "trigger", "game") == "season":
        results = metric.compute_season(ro_session, season)
        rows = [_row(r) for r in (results or []) if r and r.value_num is not None]
        rows.sort(key=lambda r: r["value_num"], reverse=_reverse)
        return rows[:limit]

    if scope == "game":
        # For game-scope, run against recent games
        game_q = session.query(Game.game_id, Game.season).filter(Game.home_team_score.isnot(None))
        if season and season != "all":
            game_q = game_q.filter(Game.season == season)
        game_limit = 2000 if season == "all" else 500
        game_rows = game_q.order_by(Game.game_date.desc()).limit(game_limit).all()

        rows = []
        for gr in game_rows:
            try:
                result = metric.compute(ro_session, gr.game_id, gr.season, gr.game_id)
            except Exception:
                continue
            if not result:
                continue
            result_list = result if isinstance(result, list) else [result]
            for r in result_list:
                if r.value_num is not None:
                    rows.append(_row(r))
        rows.sort(key=lambda r: r["value_num"], reverse=_reverse)
        return rows[:limit]

    elif scope == "team":
        game_q = session.query(Game.game_id).filter(Game.home_team_score.isnot(None))
        if season and season != "all":
            game_q = game_q.filter(Game.season == season)
        team_ids = [r.team_id for r in session.query(TeamGameStats.team_id).filter(
            TeamGameStats.game_id.in_(game_q)
        ).distinct().all()]
        game_ids = [r.game_id for r in game_q.order_by(Game.game_date.asc()).all()]
        # Run incrementally if the metric supports it
        if metric.incremental:
            from metrics.framework.base import merge_totals
            accum: dict[str, dict] = {}
            for gid in game_ids:
                for tid in team_ids:
                    try:
                        delta = metric.compute_delta(ro_session, tid, gid)
                    except Exception:
                        continue
                    if delta is None:
                        continue
                    accum[tid] = merge_totals(accum.get(tid, {}), delta)
            rows = []
            for tid, totals in accum.items():
                try:
                    result = metric.compute_value(totals, season, tid)
                except Exception:
                    continue
                if result and result.value_num is not None:
                    rows.append(_row(result))
        else:
            rows = []
            for tid in team_ids:
                try:
                    result = metric.compute(ro_session, tid, season)
                except Exception:
                    continue
                if result and result.value_num is not None:
                    rows.append(_row(result))
        rows.sort(key=lambda r: r["value_num"], reverse=_reverse)
        return rows[:limit]

    elif scope == "player":
        # For player scope, sample recent games and run incrementally.
        # Cap at 200 games for preview speed (full backfill runs all games).
        game_q = session.query(Game.game_id).filter(Game.home_team_score.isnot(None))
        if season and season != "all":
            game_q = game_q.filter(Game.season == season)
        game_ids = [r.game_id for r in game_q.order_by(Game.game_date.desc()).limit(200).all()]
        game_ids.reverse()  # restore chronological order for incremental accumulation
        if metric.incremental:
            from metrics.framework.base import merge_totals
            # Pre-warm the session identity map with all Games for this season
            # so compute_delta's session.query(Game).filter(game_id==X).first()
            # hits the identity map instead of issuing a new query each time.
            session.query(Game).filter(Game.game_id.in_(game_ids)).all()
            accum: dict[str, dict] = {}
            for gid in game_ids:
                player_ids = [r.player_id for r in session.query(PlayerGameStats.player_id)
                    .filter(PlayerGameStats.game_id == gid).distinct().all()]
                for pid in player_ids:
                    try:
                        delta = metric.compute_delta(ro_session, pid, gid)
                    except Exception:
                        continue
                    if delta is None:
                        continue
                    accum[pid] = merge_totals(accum.get(pid, {}), delta)
            rows = []
            for pid, totals in accum.items():
                try:
                    result = metric.compute_value(totals, season, pid)
                except Exception:
                    continue
                if result and result.value_num is not None:
                    rows.append(_row(result))
        else:
            rows = []
        rows.sort(key=lambda r: r["value_num"], reverse=_reverse)
        return rows[:limit]

    return []


_metric_detail_views = register_metric_detail_routes(
    app,
    SimpleNamespace(
        session_local=lambda: SessionLocal,
        metric_definition_model=lambda: MetricDefinitionModel,
        metric_result_model=lambda: MetricResultModel,
        metric_run_log_model=lambda: MetricRunLog,
        metric_perf_log_model=lambda: MetricPerfLog,
        player_model=lambda: Player,
        team_model=lambda: Team,
        game_model=lambda: Game,
        current_user=lambda: _current_user,
        is_admin=lambda: is_admin,
        metric_def_view=lambda: _metric_def_view,
        related_metric_links=lambda: _related_metric_links,
        season_sort_key=lambda: _season_sort_key,
        pick_current_season=lambda: _pick_current_season,
        season_type_names=lambda: _SEASON_TYPE_NAMES,
        season_type_plural=lambda: _SEASON_TYPE_PLURAL,
        t=lambda: _t,
        is_zh=lambda: _is_zh(),
        metric_rank_order=lambda: _metric_rank_order,
        team_map=lambda: _team_map,
        team_name=lambda: _team_name,
        team_abbr=lambda: _team_abbr,
        display_team_name=lambda: _display_team_name,
        fmt_date=lambda: _fmt_date,
        resolve_context_label=lambda: _resolve_context_label,
        load_context_label_templates=lambda: _load_context_label_templates,
        build_metric_backfill_status=lambda: _build_metric_backfill_status,
        metric_deep_dive_state=lambda: _metric_deep_dive_state,
        get_feature_access_config=lambda: get_feature_access_config,
        build_metric_feature_context=lambda: _build_metric_feature_context,
        season_label=lambda: _season_label,
        season_year_label=lambda: _season_year_label,
        render_template=lambda: render_template,
    ),
)
metric_detail = _metric_detail_views.metric_detail


_admin_cache: dict = {}
_ADMIN_CACHE_TTL = 30  # seconds
_ADMIN_STALE_REDUCE_GRACE_SECONDS = 300


def _admin_page_arg(name: str, default: int = 1) -> int:
    try:
        return max(1, int(request.args.get(name, default) or default))
    except (TypeError, ValueError):
        return default


def _admin_page_url(param_name: str, page: int) -> str:
    args = dict(request.args)
    if page <= 1:
        args.pop(param_name, None)
    else:
        args[param_name] = str(page)
    return url_for("admin_pipeline", **args)


def _admin_fragment_url(section: str, param_name: str, page: int) -> str:
    args = dict(request.args)
    if page <= 1:
        args.pop(param_name, None)
    else:
        args[param_name] = str(page)
    return url_for("admin_fragment", section=section, **args)



def _admin_compute_run_activity(session, run) -> dict:
    games_q = session.query(Game.game_id).filter(Game.game_date.isnot(None))
    if run.target_season:
        games_q = games_q.filter(Game.season.like(f"{run.target_season}%"))
    if run.target_date_from:
        games_q = games_q.filter(Game.game_date >= run.target_date_from)
    if run.target_date_to:
        games_q = games_q.filter(Game.game_date <= run.target_date_to)
    game_ids = games_q.subquery()

    # Use target_game_count for completed/reducing runs to avoid expensive COUNT DISTINCT.
    if run.status in ("complete", "reducing"):
        scope_done_games = int(run.target_game_count)
    else:
        scope_done_games = (
            session.query(func.count(func.distinct(MetricRunLog.game_id)))
            .filter(
                MetricRunLog.metric_key == run.metric_key,
                MetricRunLog.game_id.in_(session.query(game_ids.c.game_id)),
            )
            .scalar()
            or 0
        )
    scope_active_games = 0

    metric_seasons = 0
    fresh_result_seasons = 0
    if getattr(run, "reduce_enqueued_at", None) is not None:
        season_subquery = (
            session.query(MetricRunLog.season.label("season"))
            .filter(MetricRunLog.metric_key == run.metric_key)
            .distinct()
            .subquery()
        )
        metric_seasons = (
            session.query(func.count())
            .select_from(season_subquery)
            .scalar()
            or 0
        )
        if metric_seasons:
            fresh_result_seasons = (
                session.query(func.count(func.distinct(MetricResultModel.season)))
                .filter(
                    MetricResultModel.metric_key == run.metric_key,
                    MetricResultModel.season.in_(session.query(season_subquery.c.season)),
                    MetricResultModel.computed_at >= run.reduce_enqueued_at,
                )
                .scalar()
                or 0
            )

    return {
        "scope_done_games": int(scope_done_games),
        "scope_active_games": int(scope_active_games),
        "metric_seasons": int(metric_seasons),
        "fresh_result_seasons": int(fresh_result_seasons),
    }


def _admin_compute_run_display_status(
    run,
    *,
    scope_done_games: int,
    scope_active_games: int,
    metric_seasons: int,
    fresh_result_seasons: int,
    now=None,
) -> tuple[str, str | None]:
    from datetime import datetime

    raw_status = getattr(run, "status", "")
    if raw_status != "reducing":
        return raw_status, None

    if getattr(run, "reduce_enqueued_at", None) is None:
        return raw_status, None

    now = now or datetime.utcnow()
    age_s = max(int((now - run.reduce_enqueued_at).total_seconds()), 0)
    mapping_complete = int(scope_done_games) >= int(getattr(run, "target_game_count", 0) or 0)
    no_active_claims = int(scope_active_games) == 0

    if mapping_complete and no_active_claims and metric_seasons > 0 and fresh_result_seasons >= metric_seasons:
        return "needs_finalize", "Reduce output is current, but this run never recorded completion."

    if mapping_complete and no_active_claims and age_s >= _ADMIN_STALE_REDUCE_GRACE_SECONDS and fresh_result_seasons == 0:
        return "stalled", "Mapping finished, but no reduce output landed after this run was enqueued."

    return raw_status, None


def _load_admin_compute_runs_panel(session, *, runs_page: int, runs_page_size: int) -> dict:
    from datetime import datetime

    compute_run_counts = dict(
        session.query(MetricComputeRun.status, func.count())
        .group_by(MetricComputeRun.status)
        .all()
    )
    derived_run_state: dict[str, dict] = {}
    reducing_runs = (
        session.query(MetricComputeRun)
        .filter(MetricComputeRun.status == "reducing")
        .all()
    )
    for run in reducing_runs:
        activity = _admin_compute_run_activity(session, run)
        display_status, status_detail = _admin_compute_run_display_status(run, **activity)
        derived_run_state[run.id] = {
            **activity,
            "status": display_status,
            "status_detail": status_detail,
        }
        if display_status != "reducing":
            compute_run_counts["reducing"] = max(compute_run_counts.get("reducing", 0) - 1, 0)
            compute_run_counts[display_status] = compute_run_counts.get(display_status, 0) + 1

    active_compute_runs_q = (
        session.query(MetricComputeRun)
        .filter(MetricComputeRun.status.in_(("mapping", "reducing", "failed")))
        .order_by(MetricComputeRun.created_at.desc())
    )
    compute_run_total = active_compute_runs_q.count()
    compute_run_total_pages = max(1, (compute_run_total + runs_page_size - 1) // runs_page_size)
    runs_page = min(runs_page, compute_run_total_pages)
    active_compute_runs = (
        active_compute_runs_q
        .offset((runs_page - 1) * runs_page_size)
        .limit(runs_page_size)
        .all()
    )
    compute_runs = [
        {
            "id": r.id,
            "metric_key": r.metric_key,
            "status": derived_run_state.get(r.id, {}).get("status", r.status),
            "raw_status": r.status,
            "status_label": derived_run_state.get(r.id, {}).get("status", r.status).replace("_", " "),
            "status_detail": derived_run_state.get(r.id, {}).get("status_detail"),
            "target_game_count": r.target_game_count,
            "created_at": r.created_at,
            "completed_at": r.completed_at,
            "failed_at": r.failed_at,
            "age_s": int((datetime.utcnow() - r.created_at).total_seconds()) if r.created_at else None,
        }
        for r in active_compute_runs
    ]

    return {
        "compute_run_counts": compute_run_counts,
        "compute_runs": compute_runs,
        "runs_page": runs_page,
        "runs_total_pages": compute_run_total_pages,
    }


def _load_admin_recent_runs_panel(session, *, recent_page: int, recent_page_size: int) -> dict:
    recent_runs_q = (
        session.query(MetricRunLog.game_id, MetricRunLog.metric_key, MetricRunLog.computed_at)
        .filter(MetricRunLog.computed_at >= func.date_sub(func.now(), text("INTERVAL 3 DAY")))
        .order_by(MetricRunLog.computed_at.desc())
    )
    recent_runs = (
        recent_runs_q
        .offset((recent_page - 1) * recent_page_size)
        .limit(recent_page_size + 1)
        .all()
    )
    has_next = len(recent_runs) > recent_page_size
    recent_runs = recent_runs[:recent_page_size]
    recent = [{"game_id": r.game_id, "metric_key": r.metric_key, "computed_at": r.computed_at} for r in recent_runs]

    return {
        "recent": recent,
        "recent_page": recent_page,
        "recent_has_prev": recent_page > 1,
        "recent_has_next": has_next,
    }


def _load_admin_metric_perf_panel(session, *, perf_page: int, perf_page_size: int) -> dict:
    metric_perf_q = (
        session.query(
            MetricPerfLog.metric_key.label("metric_key"),
            func.avg(MetricPerfLog.duration_ms).label("avg_ms"),
            func.min(MetricPerfLog.duration_ms).label("min_ms"),
            func.max(MetricPerfLog.duration_ms).label("max_ms"),
            func.count(MetricPerfLog.id).label("sample_count"),
        )
        .group_by(MetricPerfLog.metric_key)
        .order_by(func.avg(MetricPerfLog.duration_ms).desc(), MetricPerfLog.metric_key.asc())
    )
    perf_total = metric_perf_q.count()
    perf_total_pages = max(1, (perf_total + perf_page_size - 1) // perf_page_size)
    perf_page = min(perf_page, perf_total_pages)
    metric_stats = (
        metric_perf_q
        .offset((perf_page - 1) * perf_page_size)
        .limit(perf_page_size)
        .all()
    )

    metric_keys = [row.metric_key for row in metric_stats]
    perf_rows = []
    if metric_keys:
        perf_rows = (
            session.query(MetricPerfLog)
            .filter(MetricPerfLog.metric_key.in_(metric_keys))
            .order_by(MetricPerfLog.metric_key.asc(), MetricPerfLog.recorded_at.desc(), MetricPerfLog.id.desc())
            .all()
        )

    rows_by_metric: dict[str, list[MetricPerfLog]] = {metric_key: [] for metric_key in metric_keys}
    for row in perf_rows:
        rows_by_metric.setdefault(row.metric_key, []).append(row)

    perf_data = []
    for stat in metric_stats:
        metric_rows = rows_by_metric.get(stat.metric_key, [])
        latest = metric_rows[0] if metric_rows else None
        perf_data.append(
            {
                "metric_key": stat.metric_key,
                "avg_ms": int(round(stat.avg_ms or 0)),
                "min_ms": stat.min_ms or 0,
                "max_ms": stat.max_ms or 0,
                "latest_ms": latest.duration_ms if latest else None,
                "last_run_at": latest.recorded_at if latest else None,
                "db_reads": latest.db_reads if latest else None,
                "db_writes": latest.db_writes if latest else None,
                "sample_count": stat.sample_count,
                "samples_ms": [row.duration_ms for row in metric_rows[:5]],
            }
        )

    return {
        "perf_data": perf_data,
        "perf_page": perf_page,
        "perf_total_pages": perf_total_pages,
        "perf_has_prev": perf_page > 1,
        "perf_has_next": perf_page < perf_total_pages,
    }




_admin_content_deps = SimpleNamespace(
    session_local=lambda: SessionLocal,
    require_admin_page=lambda: _require_admin_page,
    require_admin_json=lambda: _require_admin_json,
    render_template=lambda: render_template,
    admin_page_url=lambda: _admin_page_url,
    admin_fragment_url=lambda: _admin_fragment_url,
    available_llm_models=lambda: available_llm_models,
    social_post_model=lambda: SocialPost,
    social_post_variant_model=lambda: SocialPostVariant,
    social_post_delivery_model=lambda: SocialPostDelivery,
    social_post_image_model=lambda: SocialPostImage,
    metric_definition_model=lambda: MetricDefinitionModel,
    game_content_analysis_issue_post_model=lambda: GameContentAnalysisIssuePost,
    current_user=lambda: _current_user,
    social_post_comments=lambda: _social_post_comments,
    write_social_post_comments=lambda: _write_social_post_comments,
    build_social_post_rows=lambda: _build_social_post_rows,
    social_post_delivery_view=lambda: _social_post_delivery_view,
    social_post_image_view=lambda: _social_post_image_view,
    paperclip_workflow_view=lambda: _paperclip_workflow_view,
    post_ai_review_validation_errors=lambda: _post_ai_review_validation_errors,
    append_admin_comment=lambda: append_admin_comment,
    paperclip_actor_name=lambda: _paperclip_actor_name,
    handoff_social_post=lambda: _handoff_social_post,
    ensure_paperclip_issue_for_post=lambda: _ensure_paperclip_issue_for_post,
    sync_social_post_from_paperclip=lambda: _sync_social_post_from_paperclip,
    mirror_paperclip_comment=lambda: _mirror_paperclip_comment,
    paperclip_client_or_raise=lambda: _paperclip_client_or_raise,
    paperclip_bridge_enabled=lambda: _paperclip_bridge_enabled,
    paperclip_issue_url=lambda: _paperclip_issue_url,
    metric_deep_dive_state=lambda: _metric_deep_dive_state,
    build_metric_deep_dive_brief=lambda: _build_metric_deep_dive_brief,
    create_metric_deep_dive_placeholder_post=lambda: _create_metric_deep_dive_placeholder_post,
    metric_def_view=lambda: _metric_def_view,
    resolve_game_analysis_issue_record=lambda: resolve_game_analysis_issue_record,
    ensure_game_content_analysis_issues=lambda: ensure_game_content_analysis_issues,
    ensure_game_content_analysis_issue_for_game=lambda: ensure_game_content_analysis_issue_for_game,
    game_analysis_readiness_detail=lambda: game_analysis_readiness_detail,
    game_analysis_issue_history=lambda: game_analysis_issue_history,
    link_post_to_game_analysis_issue=lambda: link_post_to_game_analysis_issue,
    validate_prepared_image_specs=lambda: _validate_prepared_image_specs,
    apply_image_review_metadata=lambda: _apply_image_review_metadata,
    normalize_image_review_source=lambda: _normalize_image_review_source,
    remove_managed_post_image_file=lambda: _remove_managed_post_image_file,
    extract_image_slots_from_content=lambda: _extract_image_slots_from_content,
    store_prepared_image=lambda: store_prepared_image,
    normalize_hupu_forum=lambda: normalize_hupu_forum,
    normalize_reddit_forum=lambda: _normalize_reddit_forum,
    reddit_english_audience_hint=lambda: _reddit_english_audience_hint,
    logger=lambda: logger,
    paperclip_bridge_error_cls=lambda: PaperclipBridgeError,
    social_post_event_metric_deep_dive_brief=lambda: _SOCIAL_POST_EVENT_METRIC_DEEP_DIVE_BRIEF,
    is_valid_hupu_thread_url=lambda: _is_valid_hupu_thread_url,
)
_admin_content_views = register_admin_content_routes(app, _admin_content_deps)
admin_pipeline = _admin_content_views.admin_pipeline
admin_settings = _admin_content_views.admin_settings
admin_content = _admin_content_views.admin_content
admin_content_post = _admin_content_views.admin_content_post
admin_content_card = _admin_content_views.admin_content_card
admin_content_detail = _admin_content_views.admin_content_detail
admin_content_update = _admin_content_views.admin_content_update
admin_content_comment = _admin_content_views.admin_content_comment
admin_content_delete = _admin_content_views.admin_content_delete
admin_content_sync_paperclip = _admin_content_views.admin_content_sync_paperclip
admin_metric_trigger_deep_dive_post = _admin_content_views.admin_metric_trigger_deep_dive_post
admin_content_trigger_daily_analysis = _admin_content_views.admin_content_trigger_daily_analysis
admin_game_trigger_content_analysis = _admin_content_views.admin_game_trigger_content_analysis
admin_content_variant_update = _admin_content_views.admin_content_variant_update
admin_content_add_destination = _admin_content_views.admin_content_add_destination
admin_content_toggle_delivery = _admin_content_views.admin_content_toggle_delivery
admin_content_toggle_image = _admin_content_views.admin_content_toggle_image
admin_content_add_image = _admin_content_views.admin_content_add_image
admin_content_replace_image = _admin_content_views.admin_content_replace_image
admin_content_image_review_payload = _admin_content_views.admin_content_image_review_payload
admin_content_apply_image_review = _admin_content_views.admin_content_apply_image_review
serve_social_post_image = _admin_content_views.serve_social_post_image
api_content_create_post = _admin_content_views.api_content_create_post
api_content_list_posts = _admin_content_views.api_content_list_posts
api_content_delivery_status = _admin_content_views.api_content_delivery_status


# ---------------------------------------------------------------------------
# Data API for Paperclip (localhost, read-only NBA data)
# ---------------------------------------------------------------------------

_admin_misc_views = register_admin_misc_routes(
    app,
    SimpleNamespace(
        require_admin_json=lambda: _require_admin_json,
        require_admin_page=lambda: _require_admin_page,
        session_local=lambda: SessionLocal,
        render_template=lambda: render_template,
        admin_page_arg=lambda: _admin_page_arg,
        admin_page_url=lambda: _admin_page_url,
        admin_fragment_url=lambda: _admin_fragment_url,
        load_admin_top_pages_panel=lambda: _load_admin_top_pages_panel,
        load_admin_compute_runs_panel=lambda: _load_admin_compute_runs_panel,
        load_admin_recent_runs_panel=lambda: _load_admin_recent_runs_panel,
        load_admin_metric_perf_panel=lambda: _load_admin_metric_perf_panel,
        human_page_view_filter=lambda: _human_page_view_filter,
        admin_cache=lambda: _admin_cache,
        admin_cache_ttl=lambda: _ADMIN_CACHE_TTL,
        time_module=lambda: time,
        box_score_source_label=lambda: _box_score_source_label,
        season_label=lambda: _season_label,
        logger=lambda: logger,
        page_view_model=lambda: PageView,
        user_model=lambda: User,
        game_model=lambda: Game,
        player_game_stats_model=lambda: PlayerGameStats,
        shot_record_model=lambda: ShotRecord,
        metric_run_log_model=lambda: MetricRunLog,
        social_post_delivery_model=lambda: SocialPostDelivery,
        social_post_variant_model=lambda: SocialPostVariant,
        feature_access_descriptors=lambda: feature_access_descriptors,
        set_feature_access_level=lambda: set_feature_access_level,
        serialize_feature_access=lambda: _serialize_feature_access,
        get_default_llm_model_for_ui=lambda: get_default_llm_model_for_ui,
        get_llm_model_for_purpose=lambda: get_llm_model_for_purpose,
        set_default_llm_model=lambda: set_default_llm_model,
        set_llm_model_for_purpose=lambda: set_llm_model_for_purpose,
        available_llm_models=lambda: available_llm_models,
        get_paperclip_issue_base_url=lambda: get_paperclip_issue_base_url,
        set_paperclip_issue_base_url=lambda: set_paperclip_issue_base_url,
        load_runtime_flags=lambda: load_runtime_flags,
        default_runtime_flags=lambda: lambda: __import__("runtime_flags", fromlist=["DEFAULT_RUNTIME_FLAGS"]).DEFAULT_RUNTIME_FLAGS,
        set_runtime_flag=lambda: set_runtime_flag,
        get_ai_usage_dashboard=lambda: get_ai_usage_dashboard,
        app=lambda: app,
        back_fill_game_shot_record_from_api=lambda: back_fill_game_shot_record_from_api,
    ),
)
admin_users = _admin_misc_views.admin_users
api_data_games = _admin_misc_views.api_data_games
api_data_boxscore = _admin_misc_views.api_data_boxscore
api_data_pbp = _admin_misc_views.api_data_pbp
api_data_game_metrics = _admin_misc_views.api_data_game_metrics
api_data_metric_top = _admin_misc_views.api_data_metric_top
api_data_triggered_metrics = _admin_misc_views.api_data_triggered_metrics
admin_fragment = _admin_misc_views.admin_fragment
api_admin_infra_status = _admin_misc_views.api_admin_infra_status
api_admin_feature_access = _admin_misc_views.api_admin_feature_access
api_admin_update_feature_access = _admin_misc_views.api_admin_update_feature_access
api_admin_model_config = _admin_misc_views.api_admin_model_config
api_admin_paperclip_config = _admin_misc_views.api_admin_paperclip_config
api_admin_update_paperclip_config = _admin_misc_views.api_admin_update_paperclip_config
api_admin_update_model_config = _admin_misc_views.api_admin_update_model_config
api_admin_runtime_flags = _admin_misc_views.api_admin_runtime_flags
api_admin_ai_usage = _admin_misc_views.api_admin_ai_usage
api_admin_visitor_timeseries = _admin_misc_views.api_admin_visitor_timeseries
api_admin_update_runtime_flags = _admin_misc_views.api_admin_update_runtime_flags
admin_backfill = _admin_misc_views.admin_backfill
game_shotchart_backfill = _admin_misc_views.game_shotchart_backfill
game_shotchart_backfill_api = _admin_misc_views.game_shotchart_backfill_api


if __name__ == "__main__":
    port = int(os.getenv("FUNBA_WEB_PORT", "5000"))
    host = os.getenv("FUNBA_WEB_HOST", "127.0.0.1")
    debug = os.getenv("FUNBA_WEB_DEBUG", "1") != "0"
    app.run(host=host, port=port, debug=debug)
