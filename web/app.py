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
import time
from types import SimpleNamespace
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

import uuid as _uuid_mod

from flask import Flask, abort, after_this_request, flash, g, get_flashed_messages, has_request_context, jsonify, make_response, redirect, render_template, request, session, url_for
from flask_limiter import Limiter

from authlib.integrations.flask_client import OAuth
from sqlalchemy import and_, case, func, or_, text
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
    build_career_code_variant,
    build_career_rule_definition,
    derive_career_description,
    derive_career_min_sample,
    derive_career_name,
    family_base_key,
    family_career_key,
    is_reserved_career_key,
    rule_is_career_variant,
    rule_supports_career,
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


def _load_admin_top_pages_panel(session, raw_window: str | None):
    selected_window = _admin_top_pages_window(raw_window)
    cutoff = datetime.utcnow() - _ADMIN_TOP_PAGES_WINDOWS[selected_window]

    top_page_rows = (
        session.query(
            PageView.path.label("path"),
            func.count(PageView.id).label("views"),
            func.count(func.distinct(PageView.visitor_id)).label("unique_visitors"),
        )
        .filter(PageView.created_at >= cutoff)
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
        .filter(PageView.created_at >= cutoff)
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
}
_ZH_TO_BASE_ENDPOINT = {zh_endpoint: endpoint for endpoint, zh_endpoint in _LOCALIZED_PUBLIC_ENDPOINTS.items()}


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


@app.route("/sitemap.xml")
def sitemap_xml():
    urls = [
        "https://funba.app/",
        "https://funba.app/games",
        "https://funba.app/awards",
        "https://funba.app/metrics",
    ]
    with SessionLocal() as db:
        teams = db.query(Team.team_id).all()
        for (tid,) in teams:
            urls.append(f"https://funba.app/teams/{tid}")
    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for url in urls:
        xml.append(f"  <url><loc>{url}</loc></url>")
    xml.append("</urlset>")
    return make_response("\n".join(xml)), 200, {"Content-Type": "application/xml"}


# ── Google OAuth ─────────────────────────────────────────────────────────────
oauth = OAuth(app)
oauth.register(
    name="google",
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

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
        "1st Team": "一阵",
        "2nd Team": "二阵",
        "3rd Team": "三阵",
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


def _metric_def_view(metric_def, *, status: str | None = None, source_type: str | None = None):
    """Normalize runtime and DB metric objects for template rendering."""
    name_zh = getattr(metric_def, "name_zh", "") or ""
    description_zh = getattr(metric_def, "description_zh", "") or ""
    return SimpleNamespace(
        key=metric_def.key,
        name=_localized_metric_name(metric_def.name, name_zh),
        name_en=metric_def.name,
        name_zh=name_zh,
        description=_localized_metric_description(getattr(metric_def, "description", "") or "", description_zh),
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
    )


def _metric_name_for_key(session, metric_key: str) -> str:
    from metrics.framework.runtime import get_metric as _get_metric

    base_key = metric_key.removesuffix("_career")
    db_metric = (
        session.query(MetricDefinitionModel)
        .filter(MetricDefinitionModel.key.in_([metric_key, base_key]))
        .order_by(MetricDefinitionModel.key.desc())
        .first()
    )
    runtime_metric = _get_metric(metric_key, session=session)
    if runtime_metric is None and metric_key.endswith("_career"):
        runtime_metric = _get_metric(base_key, session=session)

    name = getattr(db_metric, "name", None) or getattr(runtime_metric, "name", None) or base_key.replace("_", " ").title()
    name_zh = getattr(db_metric, "name_zh", None) or getattr(runtime_metric, "name_zh", None)
    localized = _localized_metric_name(name, name_zh)
    if metric_key.endswith("_career") and localized == _localized_metric_name(base_key.replace("_", " ").title(), name_zh):
        return f"{localized}{_t(' (Career)', '（生涯）')}"
    return localized


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


def _safe_code_metric_metadata(row: MetricDefinitionModel) -> dict:
    if row.source_type != "code" or not row.code_python:
        return {}
    try:
        return _code_metric_metadata_from_code(row.code_python, expected_key=row.key)
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
            incremental=False,
            rank_order=str(definition.get("rank_order") or "desc").strip().lower() if definition else "desc",
            career_min_sample=definition.get("career_min_sample") if definition else None,
            career_name_suffix=str(definition.get("career_name_suffix") or " (Career)") if definition else " (Career)",
        )
        return details

    if code_metadata:
        row_name_zh = getattr(row, "name_zh", "") or ""
        row_description_zh = getattr(row, "description_zh", "") or ""
        details.update(
            min_sample=code_metadata["min_sample"],
            career_min_sample=code_metadata["career_min_sample"],
            supports_career=code_metadata["supports_career"],
            career=code_metadata["career"],
            incremental=code_metadata["incremental"],
            rank_order=code_metadata["rank_order"],
            season_types=code_metadata.get("season_types", ["regular", "playoffs", "playin"]),
            name=code_metadata["name"],
            name_zh=code_metadata.get("name_zh", "") or row_name_zh,
            description=code_metadata["description"],
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


def _related_metric_links(session, metric_key: str, runtime_metric, db_metric) -> list[dict]:
    """Return related metric links for the current metric family.

    Families are resolved from either:
    - `group_key` for DB-defined metric variants (regular/combined/etc.)
    - season/career siblings for metrics that support a `_career` variant
    """
    from metrics.framework.runtime import get_metric as _get_metric

    current_key = metric_key
    base_key = metric_key.removesuffix("_career")
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
            related = _get_metric(row.key, session=session)
            if related is not None and getattr(related, "supports_career", False):
                career_key = row.key + "_career"
                if _get_metric(career_key, session=session) is not None:
                    _add(career_key)

    season_metric = _get_metric(base_key, session=session)
    if season_metric is not None:
        _add(base_key)
        if getattr(season_metric, "supports_career", False):
            career_key = base_key + "_career"
            if _get_metric(career_key, session=session) is not None:
                _add(career_key)

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
        counts = {
            row.metric_key: row.count
            for row in session.query(
                MetricResultModel.metric_key,
                func.count(MetricResultModel.id).label("count"),
            )
            .filter(MetricResultModel.metric_key.in_(count_keys))
            .group_by(MetricResultModel.metric_key)
            .all()
        }

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
    career_entry = _virtual_career_catalog_metric(
        row,
        search_fields=search_fields,
        existing_keys=existing_keys,
        counts=counts,
        is_mine=is_mine,
    )
    if career_entry is not None:
        entries.append(career_entry)
    return entries


def _catalog_has_virtual_career_metric(
    row,
    *,
    search_fields: dict,
    existing_keys: set[str],
) -> bool:
    if (
        row.status != "published"
        or search_fields.get("career")
        or not search_fields.get("supports_career")
    ):
        return False
    return f"{row.key}_career" not in existing_keys


def _virtual_career_catalog_metric(
    row,
    *,
    search_fields: dict,
    existing_keys: set[str],
    counts: dict[str, int],
    is_mine: bool,
) -> dict | None:
    if not _catalog_has_virtual_career_metric(
        row,
        search_fields=search_fields,
        existing_keys=existing_keys,
    ):
        return None

    career_key = f"{row.key}_career"
    base_name = search_fields.get("name", row.name)
    base_description = search_fields.get("description", row.description)
    base_name_zh = search_fields.get("name_zh", getattr(row, "name_zh", "")) or ""
    base_description_zh = search_fields.get("description_zh", getattr(row, "description_zh", "")) or ""
    career_suffix = str(search_fields.get("career_name_suffix") or " (Career)")
    min_sample = int(search_fields.get("min_sample", row.min_sample or 1) or 1)
    career_min_sample = search_fields.get("career_min_sample")

    return {
        "key": career_key,
        "name": _localized_metric_name(
            derive_career_name(base_name, career_suffix),
            f"{base_name_zh}（生涯）" if base_name_zh else "",
        ),
        "name_zh": f"{base_name_zh}（生涯）" if base_name_zh else "",
        "description": _localized_metric_description(
            derive_career_description(base_description),
            f"生涯{base_description_zh}" if base_description_zh else "",
        ),
        "description_zh": f"生涯{base_description_zh}" if base_description_zh else "",
        "scope": search_fields.get("scope", row.scope),
        "category": search_fields.get("category", row.category or ""),
        "status": "published",
        "source_type": row.source_type,
        "result_count": counts.get(career_key, 0),
        "is_mine": is_mine,
        "group_key": search_fields.get("group_key"),
        "min_sample": derive_career_min_sample(min_sample, career_min_sample),
        "expression": row.expression or "",
        "definition_json": search_fields.get("definition_json", ""),
        "code_python": search_fields.get("code_python", ""),
        "supports_career": bool(search_fields.get("supports_career")),
        "career": True,
        "incremental": bool(search_fields.get("incremental", False)),
        "rank_order": search_fields.get("rank_order", "desc"),
        "career_min_sample": career_min_sample,
        "time_scope": "career",
        "base_metric_key": row.key,
    }


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
        if _catalog_has_virtual_career_metric(
            row,
            search_fields=search_fields,
            existing_keys=existing_keys,
        ):
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

    # Build map of metric_key → (scope, rank_order, is_career)
    metric_info: dict[str, tuple[str, str, bool]] = {}
    for m in metrics_list:
        if m.get("status") != "published":
            continue
        metric_info[m["key"]] = (m["scope"], m.get("rank_order", "desc"), bool(m.get("career")))

    if not metric_info:
        return {}

    season_keys = [k for k, v in metric_info.items() if not v[2] and v[0] != "season"]
    season_scope_keys = [k for k, v in metric_info.items() if not v[2] and v[0] == "season"]
    career_keys = [k for k, v in metric_info.items() if v[2]]

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
    season_desc = [k for k in season_keys if metric_info.get(k, ("", "desc", False))[1] == "desc"]
    season_asc = [k for k in season_keys if metric_info.get(k, ("", "desc", False))[1] != "desc"]
    career_desc = [k for k in career_keys if metric_info.get(k, ("", "desc", False))[1] == "desc"]
    career_asc = [k for k in career_keys if metric_info.get(k, ("", "desc", False))[1] != "desc"]
    season_scope_desc = [k for k in season_scope_keys if metric_info.get(k, ("", "desc", False))[1] == "desc"]
    season_scope_asc = [k for k in season_scope_keys if metric_info.get(k, ("", "desc", False))[1] != "desc"]
    rows.extend(_fetch_top_rows(season_desc, season_value=current_season, rank_order="desc"))
    rows.extend(_fetch_top_rows(season_asc, season_value=current_season, rank_order="asc"))
    rows.extend(_fetch_top_rows(career_desc, season_value="all_regular", rank_order="desc"))
    rows.extend(_fetch_top_rows(career_asc, season_value="all_regular", rank_order="asc"))
    rows.extend(_fetch_top_rows(season_scope_desc, season_prefix="2", rank_order="desc"))
    rows.extend(_fetch_top_rows(season_scope_asc, season_prefix="2", rank_order="asc"))

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
        scope, rank_order, _ = metric_info.get(key, ("player", "desc", False))
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
    from metrics.framework.base import CAREER_SEASON, CAREER_SEASON_PREFIX, is_career_season, SEASON_TYPE_TO_CAREER

    _CAREER_TYPE_LABEL = {
        "all_regular": _t("Regular Season", "常规赛"),
        "all_playoffs": _t("Playoffs", "季后赛"),
        "all_playin": _t("Play-In", "附加赛"),
    }

    _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
    _asc_keys = _asc_metric_keys(session)
    _disabled_keys = _disabled_metric_keys(session) if not is_admin() else set()
    scope_label = {"player": "players", "team": "teams", "game": "games"}.get(entity_type, "entities")

    # Inner subquery: compute rank and total over the full population for
    # each (metric_key, season) group, filtered to this entity_type.
    season_filter = (
        (MetricResultModel.season == season) | (MetricResultModel.season.like(CAREER_SEASON_PREFIX + "%"))
        if season
        else None
    )
    inner_filters = [
        MetricResultModel.entity_type == entity_type,
        MetricResultModel.value_num.isnot(None),
    ]
    if _disabled_keys:
        inner_filters.append(MetricResultModel.metric_key.notin_(_disabled_keys))
    if season_filter is not None:
        inner_filters.append(season_filter)

    rank_partition = func.coalesce(MetricResultModel.rank_group, "__all__")
    # Flip sign for "asc" metrics so DESC ordering ranks lowest value first
    _rank_value = case(
        (MetricResultModel.metric_key.in_(_asc_keys), -MetricResultModel.value_num),
        else_=MetricResultModel.value_num,
    )

    inner_q = (
        session.query(
            MetricResultModel.id,
            MetricResultModel.metric_key,
            MetricResultModel.entity_id,
            MetricResultModel.season,
            MetricResultModel.rank_group,
            MetricResultModel.value_num,
            MetricResultModel.value_str,
            MetricResultModel.context_json,
            MetricResultModel.computed_at,
            func.rank().over(
                partition_by=[MetricResultModel.metric_key, MetricResultModel.season, rank_partition],
                order_by=_rank_value.desc(),
            ).label("rank"),
            func.count(MetricResultModel.id).over(
                partition_by=[MetricResultModel.metric_key, MetricResultModel.season, rank_partition],
            ).label("total"),
        )
        .filter(*inner_filters)
        .subquery()
    )

    rows_q = session.query(inner_q)
    if entity_type == "game":
        rows_q = rows_q.filter(_game_entity_filter(inner_q.c.entity_id, entity_id))
    else:
        rows_q = rows_q.filter(inner_q.c.entity_id == entity_id)
    rows = rows_q.order_by(inner_q.c.rank.asc()).all()

    team_map = _team_map(session)

    all_base_keys = {r.metric_key.removesuffix("_career") for r in rows}
    db_templates = _load_context_label_templates(session, all_base_keys)

    season_metrics = []
    alltime_metrics = []
    for r in rows:
        ctx = json.loads(r.context_json) if r.context_json else {}
        rank_group_label = _team_name(team_map, r.rank_group) if r.rank_group else None
        base_key = r.metric_key.removesuffix("_career")
        context_label = _resolve_context_label(base_key, ctx, db_templates)
        rank, total = r.rank, r.total
        is_notable = total > 0 and rank / total <= 0.25
        entry = {
            "metric_key": r.metric_key,
            "metric_name": _metric_name_for_key(session, r.metric_key),
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
            # career cross-reference filled in below
            "career_rank": None,
            "career_total": None,
            "career_is_notable": False,
        }
        if r.metric_key.endswith("_career") or is_career_season(r.season):
            entry["career_type"] = r.season
            entry["career_type_label"] = _CAREER_TYPE_LABEL.get(r.season, "Career")
            alltime_metrics.append(entry)
        else:
            season_metrics.append(entry)

    # Attach career rank to each season entry so cards can show both at once.
    # Match career bucket to the current season's type.
    current_type = season[0] if season and len(season) == 5 and season.isdigit() else None
    matching_career_season = SEASON_TYPE_TO_CAREER.get(current_type) if current_type else None
    career_by_base = {}
    for e in alltime_metrics:
        if matching_career_season and e.get("career_type") != matching_career_season:
            continue
        career_by_base[e["metric_key"].removesuffix("_career")] = e
    for entry in season_metrics:
        entry["all_games_rank"] = None
        entry["all_games_total"] = None
        entry["all_games_is_notable"] = False
        career = career_by_base.get(entry["metric_key"])
        if career:
            entry["career_rank"] = career["rank"]
            entry["career_total"] = career["total"]
            entry["career_is_notable"] = career["is_notable"]

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

    return {"season": season_metrics, "season_extra": season_extra, "alltime": alltime_metrics}


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

    base_metric_key = metric_key.removesuffix("_career")
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


_BOT_SIGNATURES = (
    "bot", "crawl", "spider", "slurp", "mediapartners",
    "meta-webindexer", "facebookexternalhit", "bytespider",
    "gptbot", "claudebot", "bingpreview", "yandex",
    "curl/", "wget/", "httpie/", "python-requests", "python-urllib",
    "go-http-client", "java/", "okhttp", "axios/", "node-fetch",
    "scrapy", "headlesschrome", "phantomjs", "selenium",
)

# Bare or too-short UAs are almost never real browsers
_MIN_REAL_UA_LENGTH = 40


def _is_bot() -> bool:
    ua = (request.user_agent.string or "").lower().strip()
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
    # Exempt localhost API calls (Paperclip agents and admin tools)
    if request.path.startswith(("/api/content/", "/api/data/", "/api/admin/")) and request.remote_addr in ("127.0.0.1", "::1"):
        return
    if _is_bot():
        return "Forbidden", 403


@app.before_request
def _track_page_view():
    """Log each page load and ensure the visitor cookie is set."""
    # Only track GET requests for HTML pages (skip API, static, etc.)
    if request.method != "GET":
        return
    if request.path.startswith("/api/") or request.path.startswith("/static/"):
        return
    if not app.config.get("TESTING") and _is_bot():
        return
    # Skip tracking for localhost requests
    if _real_ip() in ("127.0.0.1", "::1"):
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

    visitor_id = request.cookies.get(_VISITOR_COOKIE)
    new_visitor = visitor_id is None
    if new_visitor:
        visitor_id = str(_uuid_mod.uuid4())

    from datetime import datetime
    pv = PageView(
        visitor_id=visitor_id,
        path=request.path,
        referrer=(request.referrer or "")[:1000],
        user_agent=(request.user_agent.string or "")[:500],
        ip_address=_real_ip(),
        created_at=datetime.utcnow(),
    )
    try:
        with SessionLocal() as db_sess:
            db_sess.add(pv)
            db_sess.commit()
    except Exception:
        logger.exception("page view tracking failed")

    if new_visitor:
        # Attach cookie to the response after this request completes
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
    }


# ── Auth routes ──────────────────────────────────────────────────────────────

def _safe_redirect_url(url: str | None) -> str:
    """Return a safe local redirect target; fall back to home.

    Accepts:
    - Local paths: /foo, /foo?q=1
    - Same-origin absolute URLs: http://localhost:5001/foo — normalized to /foo

    Rejects protocol-relative (//evil.com) and cross-origin URLs.
    """
    if not url:
        return url_for("home")
    from urllib.parse import urlparse
    parsed = urlparse(url)
    # Path-only (no scheme, no netloc): must start with / and not be //
    if not parsed.scheme and not parsed.netloc:
        if parsed.path.startswith("/") and not url.startswith("//"):
            return url
        return url_for("home")
    # Absolute URL: accept only same-origin (current request host)
    if parsed.scheme in ("http", "https") and parsed.netloc == request.host:
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        if parsed.fragment:
            path += "#" + parsed.fragment
        return path
    return url_for("home")


@app.route("/auth/login")
def auth_login():
    """Show login page with Google and email options."""
    next_url = _safe_redirect_url(request.args.get("next") or request.referrer)
    session["oauth_next"] = next_url
    return render_template("login.html", next_url=next_url)


@app.route("/auth/google")
def auth_google():
    """Redirect to Google OAuth consent screen."""
    if not os.environ.get("GOOGLE_CLIENT_ID") or os.environ.get("GOOGLE_CLIENT_ID", "").startswith("REPLACE_"):
        flash("Google sign-in is not configured on this server.", "error")
        return redirect(url_for("home"))
    next_url = _safe_redirect_url(request.args.get("next") or request.referrer)
    session["oauth_next"] = next_url
    callback = url_for("auth_callback", _external=True)
    return oauth.google.authorize_redirect(callback)


@app.route("/auth/callback")
def auth_callback():
    """Handle OAuth callback: create/update User, set session."""
    from datetime import datetime
    try:
        token = oauth.google.authorize_access_token()
        userinfo = token.get("userinfo") or oauth.google.userinfo()
    except Exception:
        flash("Sign-in failed. Please try again.", "error")
        return redirect(url_for("home"))

    google_id = userinfo.get("sub")
    email = userinfo.get("email", "")
    display_name = userinfo.get("name", email)
    avatar_url = userinfo.get("picture")

    if not google_id:
        flash("Sign-in failed. Please try again.", "error")
        return redirect(url_for("home"))

    now = datetime.utcnow()
    try:
        with SessionLocal() as db:
            user = db.query(User).filter(User.google_id == google_id).first()
            if user is None:
                # Check for email conflict (different google_id, same email) — update
                user = db.query(User).filter(User.email == email).first()
            if user is None:
                user = User(
                    id=str(_uuid_mod.uuid4()),
                    google_id=google_id,
                    email=email,
                    display_name=display_name,
                    avatar_url=avatar_url,
                    created_at=now,
                    last_login_at=now,
                )
                db.add(user)
            else:
                user.google_id = google_id
                user.email = email
                user.display_name = display_name
                user.avatar_url = avatar_url
                user.last_login_at = now
            db.commit()
            db.refresh(user)
            session.permanent = True
            session["user_id"] = user.id
    except Exception:
        logger.exception("auth_callback: DB error")
        flash("Sign-in failed. Please try again.", "error")
        return redirect(url_for("home"))

    next_url = _safe_redirect_url(session.pop("oauth_next", None))
    return redirect(next_url)


@app.route("/auth/magic", methods=["POST"])
@limiter.limit("5 per minute")
def auth_magic_send():
    """Send a magic login link to the provided email."""
    import secrets
    import resend
    from datetime import datetime, timedelta

    email = (request.form.get("email") or "").strip().lower()
    next_url = _safe_redirect_url(request.form.get("next"))

    if not email or "@" not in email:
        flash("Please enter a valid email address.", "error")
        return redirect(url_for("auth_login", next=next_url))

    resend_key = os.environ.get("RESEND_API_KEY")
    if not resend_key:
        flash("Email sign-in is not configured.", "error")
        return redirect(url_for("auth_login", next=next_url))

    now = datetime.utcnow()
    token_str = secrets.token_urlsafe(32)

    try:
        with SessionLocal() as db:
            mt = MagicToken(
                token=token_str,
                email=email,
                expires_at=now + timedelta(minutes=15),
                used=False,
                next_url=next_url if next_url != url_for("home") else None,
                created_at=now,
            )
            db.add(mt)
            db.commit()
    except Exception:
        logger.exception("auth_magic_send: DB error")
        flash("Something went wrong. Please try again.", "error")
        return redirect(url_for("auth_login", next=next_url))

    magic_url = url_for("auth_magic_verify", token=token_str, _external=True)
    try:
        resend.api_key = resend_key
        resend.Emails.send({
            "from": "Funba <noreply@funba.app>",
            "to": [email],
            "subject": "Your Funba login link",
            "html": (
                f'<div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:40px 20px;">'
                f'<h2 style="color:#f97316;margin-bottom:24px;">Funba</h2>'
                f'<p>Click the button below to sign in. This link expires in 15 minutes.</p>'
                f'<a href="{magic_url}" style="display:inline-block;margin:24px 0;padding:12px 32px;'
                f'background:#f97316;color:#fff;text-decoration:none;border-radius:8px;font-weight:600;">'
                f'Sign in to Funba</a>'
                f'<p style="color:#888;font-size:13px;">If you didn\'t request this, you can safely ignore this email.</p>'
                f'</div>'
            ),
        })
    except Exception:
        logger.exception("auth_magic_send: Resend error")
        flash("Failed to send login email. Please try again.", "error")
        return redirect(url_for("auth_login", next=next_url))

    return render_template("magic_sent.html", email=email)


@app.route("/auth/magic/verify")
def auth_magic_verify():
    """Verify magic link token, create/update user, log in."""
    from datetime import datetime

    token_str = request.args.get("token", "")
    if not token_str:
        flash("Invalid login link.", "error")
        return redirect(url_for("auth_login"))

    now = datetime.utcnow()
    try:
        with SessionLocal() as db:
            mt = db.query(MagicToken).filter(MagicToken.token == token_str).first()
            if mt is None:
                flash("Invalid login link.", "error")
                return redirect(url_for("auth_login"))
            if mt.used:
                flash("This login link has already been used.", "error")
                return redirect(url_for("auth_login"))
            if now > mt.expires_at:
                flash("This login link has expired. Please request a new one.", "error")
                return redirect(url_for("auth_login"))

            mt.used = True
            email = mt.email
            next_url = mt.next_url

            user = db.query(User).filter(User.email == email).first()
            if user is None:
                user = User(
                    id=str(_uuid_mod.uuid4()),
                    google_id=None,
                    email=email,
                    display_name=email.split("@")[0],
                    created_at=now,
                    last_login_at=now,
                )
                db.add(user)
            else:
                user.last_login_at = now
            db.commit()
            db.refresh(user)
            session.permanent = True
            session["user_id"] = user.id
    except Exception:
        logger.exception("auth_magic_verify: DB error")
        flash("Sign-in failed. Please try again.", "error")
        return redirect(url_for("auth_login"))

    return redirect(_safe_redirect_url(next_url))


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    """Clear session and redirect to home."""
    session.pop("user_id", None)
    return redirect(url_for("home"))


# ── Subscription routes ──────────────────────────────────────────────────────

_stripe_price_cache: dict = {}  # {"amount": 900, "currency": "usd", "interval": "month", "fetched_at": ...}


def _get_stripe_price() -> dict | None:
    """Fetch Pro price from Stripe, cached for 1 hour."""
    import time
    cached = _stripe_price_cache.get("data")
    if cached and time.time() - _stripe_price_cache.get("fetched_at", 0) < 3600:
        return cached
    price_id = os.environ.get("STRIPE_PRO_PRICE_ID", "")
    secret_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if not price_id or not secret_key:
        return None
    try:
        import stripe
        stripe.api_key = secret_key
        price = stripe.Price.retrieve(price_id)
        data = {
            "amount": price.unit_amount,  # cents
            "currency": (price.currency or "usd").upper(),
            "interval": price.recurring.interval if price.recurring else "month",
        }
        _stripe_price_cache["data"] = data
        _stripe_price_cache["fetched_at"] = time.time()
        return data
    except Exception:
        logger.exception("Failed to fetch Stripe price")
        return cached  # return stale cache if available


@app.route("/cn/pricing", endpoint="pricing_zh")
@app.route("/pricing")
def pricing():
    price_info = _get_stripe_price()
    return render_template("pricing.html", price_info=price_info)


@app.route("/cn/account", endpoint="account_page_zh")
@app.route("/account")
def account_page():
    user = _current_user()
    if not user:
        return redirect(url_for("auth_login", next=_localized_url_for("account_page")))
    return render_template("account.html", user=user, checkout=request.args.get("checkout"))


@app.post("/subscribe/checkout")
def subscribe_checkout():
    import stripe
    user = _current_user()
    if not user:
        return redirect(url_for("auth_login", next=_localized_url_for("pricing")))

    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if not stripe.api_key:
        flash("Payment is not configured yet.", "error")
        return redirect(_localized_url_for("pricing"))

    # Create or retrieve Stripe Customer
    if not user.stripe_customer_id:
        customer = stripe.Customer.create(
            email=user.email,
            name=user.display_name,
            metadata={"funba_user_id": user.id},
        )
        with SessionLocal() as db:
            db_user = db.get(User, user.id)
            db_user.stripe_customer_id = customer.id
            db.commit()
        customer_id = customer.id
    else:
        customer_id = user.stripe_customer_id

    price_id = os.environ.get("STRIPE_PRO_PRICE_ID", "")
    if not price_id:
        flash("Payment is not configured yet.", "error")
        return redirect(_localized_url_for("pricing"))

    checkout_session = stripe.checkout.Session.create(
        customer=customer_id,
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=_localized_url_for("account_page", checkout="success", _external=True),
        cancel_url=_localized_url_for("pricing", _external=True),
    )
    return redirect(checkout_session.url, code=303)


@app.post("/subscribe/portal")
def subscribe_portal():
    import stripe
    user = _current_user()
    if not user or not user.stripe_customer_id:
        return redirect(_localized_url_for("pricing"))

    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    portal_session = stripe.billing_portal.Session.create(
        customer=user.stripe_customer_id,
        return_url=_localized_url_for("account_page", _external=True),
    )
    return redirect(portal_session.url, code=303)


@app.post("/stripe/webhook")
def stripe_webhook():
    import stripe
    payload = request.get_data()
    sig_header = request.headers.get("Stripe-Signature")
    webhook_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
    except (ValueError, stripe.error.SignatureVerificationError):
        return "Invalid signature", 400

    _handle_stripe_event(event)
    return "", 200


def _handle_stripe_event(event):
    event_type = event["type"]
    data = event["data"]["object"]

    if event_type == "checkout.session.completed":
        _on_checkout_completed(data)
    elif event_type in ("customer.subscription.updated", "customer.subscription.deleted"):
        _on_subscription_changed(data)
    elif event_type == "invoice.payment_failed":
        _on_payment_failed(data)


def _on_checkout_completed(session_data):
    customer_id = session_data.get("customer")
    if not customer_id:
        return
    with SessionLocal() as db:
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            user.subscription_tier = "pro"
            user.subscription_status = "active"
            user.subscription_expires_at = None
            db.commit()


def _on_subscription_changed(subscription):
    from datetime import datetime as _dt
    customer_id = subscription.get("customer")
    if not customer_id:
        return
    status = subscription.get("status", "")
    with SessionLocal() as db:
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if not user:
            return
        user.subscription_status = status
        if status == "active":
            user.subscription_tier = "pro"
            user.subscription_expires_at = None
        elif status == "canceled":
            period_end = subscription.get("current_period_end")
            if period_end:
                user.subscription_expires_at = _dt.utcfromtimestamp(period_end)
            else:
                user.subscription_tier = "free"
        elif status in ("unpaid", "incomplete_expired"):
            user.subscription_tier = "free"
            user.subscription_expires_at = None
        db.commit()


def _on_payment_failed(invoice):
    customer_id = invoice.get("customer")
    if not customer_id:
        return
    with SessionLocal() as db:
        user = db.query(User).filter(User.stripe_customer_id == customer_id).first()
        if user:
            user.subscription_status = "past_due"
            db.commit()


# ── Feedback routes ───────────────────────────────────────────────────────────

@app.post("/feedback")
@limiter.limit("10 per minute")
def submit_feedback():
    user = _current_user()
    if not user:
        return {"error": "login_required"}, 401
    content = (request.json or {}).get("content", "").strip()
    if not content:
        return {"error": "empty"}, 400
    if len(content) > 2000:
        return {"error": "too_long"}, 400
    page_url = (request.json or {}).get("page_url", "")[:500] or None
    from datetime import datetime
    with SessionLocal() as db:
        fb = Feedback(
            user_id=user.id,
            content=content,
            page_url=page_url,
            created_at=datetime.utcnow(),
        )
        db.add(fb)
        db.commit()
    return {"ok": True}, 201


@app.get("/admin/feedback")
def admin_feedback():
    denied = _require_admin_page()
    if denied:
        return denied
    with SessionLocal() as db:
        rows = (
            db.query(Feedback, User)
            .join(User, Feedback.user_id == User.id)
            .order_by(Feedback.created_at.desc())
            .limit(200)
            .all()
        )
    items = [
        {
            "id": fb.id,
            "content": fb.content,
            "page_url": fb.page_url,
            "created_at": fb.created_at,
            "user_display_name": u.display_name,
            "user_email": u.email,
            "user_avatar": u.avatar_url,
        }
        for fb, u in rows
    ]
    return render_template("admin_feedback.html", items=items)


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/cn/", endpoint="home_zh")
@app.route("/")
def home():
    with SessionLocal() as session:
        teams = (
            session.query(Team)
            .filter(Team.is_legacy.is_(False))
            .order_by(Team.full_name.asc())
            .limit(30)
            .all()
        )
        team_lookup = _team_map(session)

        # Available regular seasons for standings
        standing_season_ids = [
            r.season for r in session.query(Game.season)
            .filter(Game.season.like("2%"))
            .distinct().all()
        ]
        standing_season_ids = sorted(standing_season_ids, key=_season_sort_key, reverse=True)
        selected_standing_season = request.args.get("season") or (standing_season_ids[0] if standing_season_ids else None)

        # Conference membership (static)
        _EAST = {
            "1610612737", "1610612751", "1610612738", "1610612766", "1610612741",
            "1610612739", "1610612765", "1610612754", "1610612748", "1610612749",
            "1610612752", "1610612753", "1610612755", "1610612761", "1610612764",
        }

        # Compute standings: wins/losses per team for selected season
        east_standings, west_standings = [], []
        if selected_standing_season:
            rows = (
                session.query(
                    TeamGameStats.team_id,
                    func.sum(case((TeamGameStats.win.is_(True), 1), else_=0)).label("wins"),
                    func.sum(case((TeamGameStats.win.is_(False), 1), else_=0)).label("losses"),
                )
                .join(Game, TeamGameStats.game_id == Game.game_id)
                .filter(
                    Game.season == selected_standing_season,
                    TeamGameStats.win.isnot(None),
                )
                .group_by(TeamGameStats.team_id)
                .all()
            )
            for r in rows:
                team = team_lookup.get(r.team_id)
                abbr, full_name = _franchise_display(r.team_id, selected_standing_season, team)
                w, l = int(r.wins or 0), int(r.losses or 0)
                total = w + l
                entry = {
                    "team_id": r.team_id,
                    "abbr": abbr,
                    "full_name": full_name,
                    "wins": w,
                    "losses": l,
                    "win_pct": w / total if total > 0 else 0.0,
                }
                if r.team_id in _EAST:
                    east_standings.append(entry)
                else:
                    west_standings.append(entry)
            east_standings.sort(key=lambda x: x["win_pct"], reverse=True)
            west_standings.sort(key=lambda x: x["win_pct"], reverse=True)

    # Build team map data for D3 map
    team_map_data = []
    for team in teams:
        pos = _TEAM_MAP_POSITIONS.get(team.abbr)
        if pos:
            team_map_data.append({
                "abbr": team.abbr,
                "full_name": _display_team_name(team),
                "team_id": team.team_id,
                "lat": pos[0],
                "lon": pos[1],
            })

    return render_template(
        "home.html",
        teams=teams,
        team_map_data=team_map_data,
        east_standings=east_standings,
        west_standings=west_standings,
        standing_season_ids=standing_season_ids,
        selected_standing_season=selected_standing_season,
        fmt_season=_season_label,
    )


@app.route("/cn/games", endpoint="games_list_zh")
@app.route("/games")
def games_list():
    PAGE_SIZE = 30
    with SessionLocal() as session:
        all_season_ids = sorted(
            {r.season for r in session.query(Game.season).filter(Game.season.isnot(None)).all()},
            key=_season_sort_key, reverse=True,
        )
        selected_season = request.args.get("season") or (all_season_ids[0] if all_season_ids else None)
        selected_team = (request.args.get("team") or "").strip() or None
        try:
            page = max(1, int(request.args.get("page", 1)))
        except ValueError:
            page = 1

        all_teams = (
            session.query(Team)
            .filter(Team.is_legacy.is_(False))
            .order_by(Team.full_name.asc(), Team.abbr.asc())
            .all()
        )
        games_q = session.query(Game).filter(Game.game_date.isnot(None))
        if selected_season:
            games_q = games_q.filter(Game.season == selected_season)
        if selected_team:
            games_q = games_q.filter(
                or_(Game.home_team_id == selected_team, Game.road_team_id == selected_team)
            )
        games_q = games_q.order_by(Game.game_date.desc(), Game.game_id.desc())

        total = games_q.count()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = min(page, total_pages)
        games = games_q.offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()

        team_lookup = _team_map(session)
        selected_team_obj = next((team for team in all_teams if team.team_id == selected_team), None)
        if selected_team_obj is None and selected_team:
            selected_team_obj = team_lookup.get(selected_team)

    return render_template(
        "games_list.html",
        games=games,
        team_lookup=team_lookup,
        all_teams=all_teams,
        all_season_ids=all_season_ids,
        selected_season=selected_season,
        selected_team=selected_team,
        selected_team_obj=selected_team_obj,
        fmt_date=_fmt_date,
        fmt_season=_season_label,
        page=page,
        total_pages=total_pages,
        total=total,
    )


@app.route("/cn/awards", endpoint="awards_page_zh")
@app.route("/awards")
def awards_page():
    selected_award_type = request.args.get("type", "champion")
    if selected_award_type not in _AWARD_TYPE_META:
        selected_award_type = "champion"

    with SessionLocal() as session:
        season_rows = session.query(Award.season).distinct().order_by(Award.season.desc()).all()
        season_options = [int(row[0]) for row in season_rows if _coerce_award_season(row[0]) is not None]
        selected_season = _coerce_award_season(request.args.get("season"))
        if selected_season not in season_options:
            selected_season = None

        award_query = (
            session.query(
                Award.id,
                Award.award_type,
                Award.season,
                Award.player_id,
                Award.team_id,
                Award.notes,
                Player.full_name.label("player_name"),
                Player.full_name_zh.label("player_name_zh"),
                Team.full_name.label("team_name"),
                Team.full_name_zh.label("team_name_zh"),
                Team.abbr.label("team_abbr"),
            )
            .outerjoin(Player, Award.player_id == Player.player_id)
            .outerjoin(Team, Award.team_id == Team.team_id)
        )
        if selected_season is not None:
            award_query = award_query.filter(Award.season == selected_season)
        else:
            award_query = award_query.filter(Award.award_type == selected_award_type)

        award_rows = award_query.order_by(_award_order_case(Award.award_type), Award.season.desc(), Award.id.asc()).all()
        teams = _team_map(session)
        award_entries = [_award_entry_from_row(row, teams) for row in award_rows]
        award_sections = _group_award_entries(award_entries)

    return render_template(
        "awards.html",
        title="Awards • FUNBA",
        award_tab_groups=[
            {
                "label": group["label"],
                "tabs": [{"award_type": award_type, "label": _award_type_label(award_type)} for award_type in group["types"] if award_type in _AWARD_TYPE_META],
            }
            for group in _AWARD_TAB_GROUPS
        ],
        award_sections=award_sections,
        selected_award_type=selected_award_type,
        season_options=season_options,
        selected_season=selected_season,
    )


@app.route("/api/players/hints")
@limiter.limit("60 per minute")
def player_hints_api():
    query = (request.args.get("q") or "").strip()
    try:
        limit = int(request.args.get("limit", 12))
    except ValueError:
        limit = 12
    limit = max(1, min(limit, 30))

    with SessionLocal() as session:
        q = session.query(Player).filter(Player.full_name.isnot(None))
        if query:
            q = q.filter(
                or_(
                    Player.full_name.ilike(f"%{query}%"),
                    Player.full_name_zh.ilike(f"%{query}%"),
                )
            )
        players = q.order_by(Player.is_active.desc(), Player.full_name.asc()).limit(limit).all()

    items = [
        {
            "player_id": p.player_id,
            "full_name": p.full_name_zh if _is_zh() and getattr(p, "full_name_zh", None) else p.full_name,
        }
        for p in players
        if p.player_id and p.full_name
    ]
    return jsonify({"items": items})


_COMPARE_EMPTY_MARK = "—"
_COMPARE_STATS_ROWS = [
    ("ppg", "PPG"),
    ("rpg", "RPG"),
    ("apg", "APG"),
    ("mpg", "MPG"),
    ("fg_pct", "FG%"),
    ("fg3_pct", "3P%"),
    ("ft_pct", "FT%"),
]


def _player_summary_fields(played_condition):
    return [
        func.count(PlayerGameStats.game_id).label("games_tracked"),
        func.sum(case((played_condition, 1), else_=0)).label("games_played"),
        func.sum(func.coalesce(PlayerGameStats.min, 0)).label("total_min"),
        func.sum(func.coalesce(PlayerGameStats.sec, 0)).label("total_sec"),
        func.sum(func.coalesce(PlayerGameStats.pts, 0)).label("pts"),
        func.sum(func.coalesce(PlayerGameStats.reb, 0)).label("reb"),
        func.sum(func.coalesce(PlayerGameStats.ast, 0)).label("ast"),
        func.sum(func.coalesce(PlayerGameStats.stl, 0)).label("stl"),
        func.sum(func.coalesce(PlayerGameStats.blk, 0)).label("blk"),
        func.sum(func.coalesce(PlayerGameStats.tov, 0)).label("tov"),
        func.sum(func.coalesce(PlayerGameStats.fgm, 0)).label("fgm"),
        func.sum(func.coalesce(PlayerGameStats.fga, 0)).label("fga"),
        func.sum(func.coalesce(PlayerGameStats.fg3m, 0)).label("fg3m"),
        func.sum(func.coalesce(PlayerGameStats.fg3a, 0)).label("fg3a"),
        func.sum(func.coalesce(PlayerGameStats.ftm, 0)).label("ftm"),
        func.sum(func.coalesce(PlayerGameStats.fta, 0)).label("fta"),
    ]


def _player_summary_from_row(raw_row) -> dict[str, str | int]:
    games_tracked = int(raw_row.games_tracked or 0)
    games_played = int(raw_row.games_played or 0)
    total_sec = int(raw_row.total_sec or 0)
    total_min = int(raw_row.total_min or 0) + (total_sec // 60)

    summary = {
        "games_tracked": games_tracked,
        "games_played": games_played,
        "minutes": total_min,
        "pts": int(raw_row.pts or 0),
        "reb": int(raw_row.reb or 0),
        "ast": int(raw_row.ast or 0),
        "stl": int(raw_row.stl or 0),
        "blk": int(raw_row.blk or 0),
        "tov": int(raw_row.tov or 0),
        "fgm": int(raw_row.fgm or 0),
        "fga": int(raw_row.fga or 0),
        "fg3m": int(raw_row.fg3m or 0),
        "fg3a": int(raw_row.fg3a or 0),
        "ftm": int(raw_row.ftm or 0),
        "fta": int(raw_row.fta or 0),
    }
    summary["fg_pct"] = _pct_text(summary["fgm"], summary["fga"])
    summary["fg3_pct"] = _pct_text(summary["fg3m"], summary["fg3a"])
    summary["ft_pct"] = _pct_text(summary["ftm"], summary["fta"])

    if games_played > 0:
        summary["mpg"] = f"{summary['minutes'] / games_played:.1f}"
        summary["ppg"] = f"{summary['pts'] / games_played:.1f}"
        summary["rpg"] = f"{summary['reb'] / games_played:.1f}"
        summary["apg"] = f"{summary['ast'] / games_played:.1f}"
        summary["spg"] = f"{summary['stl'] / games_played:.1f}"
        summary["bpg"] = f"{summary['blk'] / games_played:.1f}"
        summary["tpg"] = f"{summary['tov'] / games_played:.1f}"
    else:
        summary["mpg"] = "-"
        summary["ppg"] = "-"
        summary["rpg"] = "-"
        summary["apg"] = "-"
        summary["spg"] = "-"
        summary["bpg"] = "-"
        summary["tpg"] = "-"
    return summary


def _player_stat_summary(
    session,
    player_id: str,
    *,
    season: str | None = None,
    season_prefix: str | None = None,
) -> dict[str, str | int]:
    played_condition = (func.coalesce(PlayerGameStats.min, 0) > 0) | (func.coalesce(PlayerGameStats.sec, 0) > 0)
    query = (
        session.query(*_player_summary_fields(played_condition))
        .join(Game, PlayerGameStats.game_id == Game.game_id)
        .filter(PlayerGameStats.player_id == player_id)
    )
    if season:
        query = query.filter(Game.season == season)
    elif season_prefix:
        query = query.filter(Game.season.like(f"{season_prefix}%"))
    return _player_summary_from_row(query.one())


def _player_career_summary(
    session,
    player_id: str,
    *,
    season_prefix: str,
    teams: dict[str, Team],
) -> tuple[dict[str, str | int], list[dict[str, object]]]:
    played_condition = (func.coalesce(PlayerGameStats.min, 0) > 0) | (func.coalesce(PlayerGameStats.sec, 0) > 0)
    season_rows_raw = (
        session.query(
            Game.season.label("season"),
            *_player_summary_fields(played_condition),
        )
        .join(Game, PlayerGameStats.game_id == Game.game_id)
        .filter(
            PlayerGameStats.player_id == player_id,
            Game.season.like(f"{season_prefix}%"),
        )
        .group_by(Game.season)
        .all()
    )

    career_season_rows = []
    for row in season_rows_raw:
        career_season_rows.append(
            {
                "season": row.season,
                "stats": _player_summary_from_row(row),
            }
        )
    career_season_rows.sort(key=lambda row: _season_sort_key(row["season"]), reverse=True)

    season_team_rows = (
        session.query(Game.season, PlayerGameStats.team_id)
        .join(Game, PlayerGameStats.game_id == Game.game_id)
        .filter(
            PlayerGameStats.player_id == player_id,
            Game.season.like(f"{season_prefix}%"),
            PlayerGameStats.team_id.isnot(None),
        )
        .distinct()
        .all()
    )
    season_team_abbrs: dict[str, list[str]] = defaultdict(list)
    for st_row in season_team_rows:
        abbr = _team_abbr(teams, st_row.team_id)
        if abbr not in season_team_abbrs[st_row.season]:
            season_team_abbrs[st_row.season].append(abbr)
    for row in career_season_rows:
        row["team_abbrs"] = season_team_abbrs.get(row["season"], [])

    return _player_stat_summary(session, player_id, season_prefix=season_prefix), career_season_rows


def _latest_regular_season(session) -> str | None:
    seasons = [
        row.season
        for row in session.query(Game.season.label("season"))
        .filter(Game.season.like("2%"))
        .distinct()
        .all()
        if row.season
    ]
    return _pick_current_season(seasons) or "22025"


def _compare_metric_label(session, metric_key: str) -> str:
    return _metric_name_for_key(session, metric_key)


def _compare_metric_value_text(entry: dict | None, missing: str = "N/A") -> str:
    if not entry:
        return missing
    if entry.get("value_str"):
        return str(entry["value_str"])
    if entry.get("value_num") is None:
        return missing
    return f"{float(entry['value_num']):.1f}"


def _compare_summary_value(summary: dict[str, str | int] | None, key: str, missing: str = _COMPARE_EMPTY_MARK) -> str:
    if not summary or int(summary.get("games_played") or 0) <= 0:
        return missing
    value = summary.get(key)
    if value in (None, "-", ""):
        return missing
    if key.endswith("_pct") and isinstance(value, str):
        return pct_fmt(value)
    return str(value)


def _compare_numeric_value(value) -> float | None:
    if value in (None, "", "-", _COMPARE_EMPTY_MARK, "N/A"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _compare_best_index(values: list[float | None], *, ascending: bool = False) -> int | None:
    scored = [(idx, value) for idx, value in enumerate(values) if value is not None]
    if not scored:
        return None
    if ascending:
        best_idx, _best_value = min(scored, key=lambda item: item[1])
    else:
        best_idx, _best_value = max(scored, key=lambda item: item[1])
    return best_idx


def _compare_metric_scope_label(entry: dict) -> str:
    season = str(entry.get("season") or "").strip()
    if len(season) == 5 and season.isdigit():
        return _season_label(season)
    career_labels = {
        "all_regular": _t("Regular Season Career", "常规赛生涯"),
        "all_playoffs": _t("Playoffs Career", "季后赛生涯"),
        "all_playin": _t("Play-In Career", "附加赛生涯"),
    }
    if season in career_labels:
        return career_labels[season]
    return entry.get("career_type_label") or _t("Career", "生涯")


def _build_compare_stat_rows(player_cards: list[dict]) -> list[dict]:
    rows = []
    for stat_key, label in _COMPARE_STATS_ROWS:
        values = [_compare_summary_value(card.get("career_summary"), stat_key) for card in player_cards]
        best_index = _compare_best_index([_compare_numeric_value(value) for value in values])
        rows.append(
            {
                "label": label,
                "values": values,
                "best_index": best_index,
            }
        )
    return rows


def _build_compare_current_rows(player_cards: list[dict]) -> list[dict]:
    rows = []
    for stat_key, label in _COMPARE_STATS_ROWS:
        values = [_compare_summary_value(card.get("current_summary"), stat_key) for card in player_cards]
        best_index = _compare_best_index([_compare_numeric_value(value) for value in values])
        rows.append(
            {
                "label": label,
                "values": values,
                "best_index": best_index,
            }
        )
    return rows


def _build_compare_metric_sections(session, player_cards: list[dict]) -> list[dict]:
    sections: list[dict] = []
    asc_keys = _asc_metric_keys(session)

    def build_rows(entries_by_card: list[list[dict]], *, group_title: str | None = None) -> dict | None:
        row_map: dict[str, dict] = {}
        for player_idx, entries in enumerate(entries_by_card):
            for entry in entries:
                row_key = entry["metric_key"]
                row = row_map.setdefault(
                    row_key,
                    {
                        "metric_key": entry["metric_key"],
                        "label": _compare_metric_label(session, entry["metric_key"]),
                        "href": _localized_url_for("metric_detail", metric_key=entry["metric_key"]),
                        "values": [None] * len(player_cards),
                        "ascending": entry["metric_key"].removesuffix("_career") in asc_keys,
                    },
                )
                row["values"][player_idx] = entry
        if not row_map:
            return None

        rows = []
        for row in sorted(row_map.values(), key=lambda item: item["label"].lower()):
            numeric_values = [
                float(value["value_num"]) if value and value.get("value_num") is not None else None
                for value in row["values"]
            ]
            best_index = _compare_best_index(numeric_values, ascending=row["ascending"])
            display_values = [
                {
                    "text": _compare_metric_value_text(value),
                    "aria_label": (
                        f"Best: {_compare_metric_value_text(value)}"
                        if best_index == idx and value is not None
                        else None
                    ),
                    "context_label": value.get("context_label") if value else None,
                }
                for idx, value in enumerate(row["values"])
            ]
            rows.append(
                {
                    "label": row["label"],
                    "href": row["href"],
                    "values": display_values,
                    "best_index": best_index,
                }
            )
        return {"title": group_title, "rows": rows}

    season_section = build_rows([card["metrics"]["season"] for card in player_cards], group_title=None)
    if season_section is not None:
        sections.append(season_section)

    grouped_alltime: dict[str, list[list[dict]]] = {}
    for card in player_cards:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for entry in card["metrics"]["alltime"]:
            grouped[entry.get("career_type_label") or _t("Career", "生涯")].append(entry)
        for title in grouped:
            grouped_alltime.setdefault(title, [[] for _ in player_cards])
    for idx, card in enumerate(player_cards):
        grouped: dict[str, list[dict]] = defaultdict(list)
        for entry in card["metrics"]["alltime"]:
            grouped[entry.get("career_type_label") or _t("Career", "生涯")].append(entry)
        for title, lists in grouped_alltime.items():
            lists[idx] = grouped.get(title, [])

    for title in sorted(grouped_alltime.keys()):
        section = build_rows(grouped_alltime[title], group_title=f"{title}{_t(' Career', '生涯')}")
        if section is not None:
            sections.append(section)

    return sections


def _player_compare_team_abbrs(
    session,
    player_id: str,
    teams: dict[str, Team],
    *,
    preferred_season: str | None = None,
) -> list[str]:
    def load_for_season(season_value: str | None) -> list[str]:
        if not season_value:
            return []
        rows = (
            session.query(PlayerGameStats.team_id)
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(
                PlayerGameStats.player_id == player_id,
                PlayerGameStats.team_id.isnot(None),
                Game.season == season_value,
            )
            .distinct()
            .all()
        )
        abbrs: list[str] = []
        for row in rows:
            abbr = _team_abbr(teams, row.team_id)
            if abbr not in abbrs:
                abbrs.append(abbr)
        return abbrs

    abbrs = load_for_season(preferred_season)
    if abbrs:
        return abbrs

    latest_row = (
        session.query(Game.season)
        .join(PlayerGameStats, PlayerGameStats.game_id == Game.game_id)
        .filter(
            PlayerGameStats.player_id == player_id,
            PlayerGameStats.team_id.isnot(None),
            Game.season.isnot(None),
        )
        .order_by(Game.season.desc(), Game.game_date.desc(), Game.game_id.desc())
        .first()
    )
    return load_for_season(latest_row.season if latest_row else None)


def _get_player_top_rankings(
    session,
    player_id: str,
    *,
    current_season: str | None,
    limit: int = 3,
) -> list[dict]:
    from metrics.framework.base import CAREER_SEASON_PREFIX, SEASON_TYPE_TO_CAREER

    asc_keys = _asc_metric_keys(session)
    filters = [
        MetricResultModel.entity_type == "player",
        MetricResultModel.value_num.isnot(None),
    ]
    season_filters = []
    if current_season:
        season_filters.append(MetricResultModel.season == current_season)
        current_type = current_season[0] if len(current_season) == 5 and current_season.isdigit() else None
        matching_career = SEASON_TYPE_TO_CAREER.get(current_type) if current_type else None
        if matching_career:
            season_filters.append(MetricResultModel.season == matching_career)
        else:
            season_filters.append(MetricResultModel.season.like(CAREER_SEASON_PREFIX + "%"))
    if season_filters:
        filters.append(or_(*season_filters))

    rank_partition = func.coalesce(MetricResultModel.rank_group, "__all__")
    rank_value = case(
        (MetricResultModel.metric_key.in_(asc_keys), -MetricResultModel.value_num),
        else_=MetricResultModel.value_num,
    )
    inner = (
        session.query(
            MetricResultModel.metric_key.label("metric_key"),
            MetricResultModel.entity_id.label("entity_id"),
            MetricResultModel.season.label("season"),
            MetricResultModel.value_num.label("value_num"),
            MetricResultModel.value_str.label("value_str"),
            MetricResultModel.noteworthiness.label("noteworthiness"),
            func.rank().over(
                partition_by=[MetricResultModel.metric_key, MetricResultModel.season, rank_partition],
                order_by=rank_value.desc(),
            ).label("rank"),
            func.count(MetricResultModel.id).over(
                partition_by=[MetricResultModel.metric_key, MetricResultModel.season, rank_partition],
            ).label("total"),
        )
        .filter(*filters)
        .subquery()
    )
    rows = (
        session.query(inner)
        .filter(inner.c.entity_id == player_id)
        .order_by(
            func.coalesce(inner.c.noteworthiness, -1).desc(),
            inner.c.rank.asc(),
            inner.c.metric_key.asc(),
        )
        .limit(limit)
        .all()
    )

    rankings = []
    for row in rows:
        metric_key = row.metric_key
        label = _compare_metric_label(session, metric_key)
        scope_label = _compare_metric_scope_label({"season": row.season})
        badge = f"#{int(row.rank)} of {int(row.total)} · {label}" if row.rank and row.total else label
        rankings.append(
            {
                "metric_key": metric_key,
                "label": label,
                "badge": badge,
                "scope_label": scope_label,
                "href": _localized_url_for("metric_detail", metric_key=metric_key, season=row.season)
                if row.season
                else _localized_url_for("metric_detail", metric_key=metric_key),
            }
        )
    return rankings


@app.route("/cn/players/compare", endpoint="players_compare_zh")
@app.route("/players/compare")
def players_compare():
    raw_ids = [part.strip() for part in (request.args.get("ids") or "").split(",") if part.strip()]
    requested_ids: list[str] = []
    for player_id in raw_ids:
        if player_id not in requested_ids:
            requested_ids.append(player_id)
        if len(requested_ids) == 4:
            break

    with SessionLocal() as session:
        players_by_id = {
            player.player_id: player
            for player in session.query(Player)
            .filter(Player.player_id.in_(requested_ids))
            .all()
        } if requested_ids else {}
        players = [players_by_id[player_id] for player_id in requested_ids if player_id in players_by_id]
        teams = _team_map(session)
        current_season = _latest_regular_season(session)

        season_rows: list[dict] = []
        current_rows: list[dict] = []
        metric_sections: list[dict] = []

        player_cards = []
        for player in players:
            team_abbrs = _player_compare_team_abbrs(session, player.player_id, teams, preferred_season=current_season)
            player_cards.append(
                {
                    "player": player,
                    "headshot_url": _player_headshot_url(player.player_id),
                    "team_abbrs": team_abbrs,
                    "team_label": " / ".join(team_abbrs) if team_abbrs else "NBA",
                    "career_summary": _player_stat_summary(session, player.player_id, season_prefix="2"),
                    "current_summary": _player_stat_summary(session, player.player_id, season=current_season),
                    "metrics": _get_metric_results(session, "player", player.player_id, current_season),
                    "top_rankings": _get_player_top_rankings(session, player.player_id, current_season=current_season),
                }
            )

        if len(player_cards) >= 2:
            season_rows = _build_compare_stat_rows(player_cards)
            current_rows = _build_compare_current_rows(player_cards)
            metric_sections = _build_compare_metric_sections(session, player_cards)

    return render_template(
        "compare.html",
        requested_ids=requested_ids,
        players=player_cards,
        active_player_ids=[card["player"].player_id for card in player_cards],
        comparison_count=len(player_cards),
        can_compare=len(player_cards) >= 2,
        current_season=current_season,
        season_rows=season_rows,
        current_rows=current_rows,
        metric_sections=metric_sections,
    )


@app.route("/cn/draft/<int:year>", endpoint="draft_page_zh")
@app.route("/draft/<int:year>")
def draft_page(year: int):
    current_year = date.today().year
    if year < 1947 or year > current_year:
        abort(404)

    with SessionLocal() as session:
        min_year, max_year = (
            session.query(
                func.min(Player.draft_year),
                func.max(Player.draft_year),
            )
            .filter(Player.draft_year.isnot(None))
            .one()
        )

        draft_players = (
            session.query(Player)
            .filter(Player.draft_year == year)
            .order_by(
                func.coalesce(Player.draft_round, 99).asc(),
                func.coalesce(Player.draft_number, 99).asc(),
                Player.full_name.asc(),
            )
            .all()
        )

    min_year = min_year or year
    max_year = max_year or year

    return render_template(
        "draft.html",
        year=year,
        draft_players=draft_players,
        draft_count=len(draft_players),
        min_year=min_year,
        max_year=max_year,
    )


@app.route("/cn/players/<player_id>", endpoint="player_page_zh")
@app.route("/players/<player_id>")
def player_page(player_id: str):
    with SessionLocal() as session:
        player = session.query(Player).filter(Player.player_id == player_id).first()
        if player is None:
            abort(404, description=f"Player {player_id} not found")

        player_award_rows = (
            session.query(
                Award.award_type,
                func.count(Award.id).label("award_count"),
            )
            .filter(Award.player_id == player_id)
            .group_by(Award.award_type)
            .order_by(_award_order_case(Award.award_type))
            .all()
        )
        player_awards = [
            {
                "award_type": row.award_type,
                "label": _award_badge_label(row.award_type),
                "count": int(row.award_count or 0),
            }
            for row in player_award_rows
        ]

        selected_career_kind = request.args.get("career_kind", "regular")
        if selected_career_kind not in {"regular", "playoffs"}:
            selected_career_kind = "regular"
        season_prefix = "2" if selected_career_kind == "regular" else "4"
        career_kind_label = "Regular Season" if selected_career_kind == "regular" else "Playoffs"

        teams = _team_map(session)
        career_overall, career_season_rows = _player_career_summary(
            session,
            player_id,
            season_prefix=season_prefix,
            teams=teams,
        )

        heatmap_season_rows = (
            session.query(Game.season)
            .join(ShotRecord, ShotRecord.game_id == Game.game_id)
            .filter(
                ShotRecord.player_id == player_id,
                Game.season.like(f"{season_prefix}%"),
                ShotRecord.shot_zone_basic.isnot(None),
            )
            .distinct()
            .all()
        )
        heatmap_season_options = sorted([row[0] for row in heatmap_season_rows], key=_season_sort_key, reverse=True)
        selected_heatmap_season = request.args.get("heatmap_season", "overall")
        if selected_heatmap_season != "overall" and selected_heatmap_season not in heatmap_season_options:
            selected_heatmap_season = "overall"

        shot_base_filter = [
            ShotRecord.player_id == player_id,
            Game.season.like(f"{season_prefix}%"),
        ]

        shot_query = (
            session.query(ShotRecord.loc_x, ShotRecord.loc_y, ShotRecord.shot_made)
            .join(Game, ShotRecord.game_id == Game.game_id)
            .filter(*shot_base_filter, ShotRecord.loc_x.isnot(None), ShotRecord.loc_y.isnot(None))
        )
        zone_query = (
            session.query(ShotRecord.shot_zone_basic, ShotRecord.shot_zone_area, ShotRecord.shot_made)
            .join(Game, ShotRecord.game_id == Game.game_id)
            .filter(*shot_base_filter, ShotRecord.shot_zone_basic.isnot(None))
        )
        if selected_heatmap_season != "overall":
            shot_query = shot_query.filter(Game.season == selected_heatmap_season)
            zone_query = zone_query.filter(Game.season == selected_heatmap_season)

        shot_rows = shot_query.all()
        shot_dots = [{"x": r.loc_x, "y": r.loc_y, "made": bool(r.shot_made)} for r in shot_rows]
        shot_attempts = len(shot_dots)
        shot_made_count = sum(1 for d in shot_dots if d["made"])
        heatmap_zones, _, _ = _build_shot_zone_heatmap(zone_query.all())
        heatmap_scope_label = (
            f"Overall {career_kind_label}" if selected_heatmap_season == "overall" else _season_label(selected_heatmap_season)
        )

        seasons = (
            session.query(Game.season)
            .join(PlayerGameStats, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id == player_id, Game.season.isnot(None))
            .distinct()
            .all()
        )
        season_options = sorted([row[0] for row in seasons], key=_season_sort_key, reverse=True)
        if not is_pro():
            _cur = _pick_current_season(season_options)
            if _cur:
                season_options = [_cur]
        selected_season = request.args.get("season")
        if selected_season not in season_options:
            selected_season = _pick_current_season(season_options)

        game_rows = []

        if selected_season is not None:
            rows = (
                session.query(PlayerGameStats, Game)
                .join(Game, PlayerGameStats.game_id == Game.game_id)
                .filter(PlayerGameStats.player_id == player_id, Game.season == selected_season)
                .order_by(Game.game_date.desc(), Game.game_id.desc())
                .all()
            )
            for stat, game in rows:
                if stat.team_id == game.home_team_id:
                    opponent_id = game.road_team_id
                    matchup = f"{_team_abbr(teams, stat.team_id)} vs {_team_abbr(teams, opponent_id)}"
                else:
                    opponent_id = game.home_team_id
                    matchup = f"{_team_abbr(teams, stat.team_id)} @ {_team_abbr(teams, opponent_id)}"

                if game.wining_team_id:
                    result = "W" if game.wining_team_id == stat.team_id else "L"
                else:
                    result = "-"

                is_home = stat.team_id == game.home_team_id
                player_team_score = game.home_team_score if is_home else game.road_team_score
                opponent_score = game.road_team_score if is_home else game.home_team_score

                game_rows.append(
                    {
                        "game_id": game.game_id,
                        "game_date": _fmt_date(game.game_date),
                        "matchup": matchup,
                        "result": result,
                        "status": _player_status(stat),
                        "minutes": _fmt_minutes(stat.min, stat.sec),
                        "pts": stat.pts if stat.pts is not None else "-",
                        "reb": stat.reb if stat.reb is not None else "-",
                        "ast": stat.ast if stat.ast is not None else "-",
                        "player_team_id": stat.team_id,
                        "player_team_abbr": _team_abbr(teams, stat.team_id),
                        "player_team_href": _localized_url_for("team_page", team_id=stat.team_id) if stat.team_id else None,
                        "opponent_id": opponent_id,
                        "opponent_abbr": _team_abbr(teams, opponent_id),
                        "opponent_href": _localized_url_for("team_page", team_id=opponent_id) if opponent_id else None,
                        "is_home": is_home,
                        "player_team_score": player_team_score,
                        "opponent_score": opponent_score,
                    }
                )

        # Current team from most recent game
        player_current_team_id = game_rows[0]["player_team_id"] if game_rows else None
        if not player_current_team_id:
            latest_team = (
                session.query(PlayerGameStats.team_id)
                .join(Game, PlayerGameStats.game_id == Game.game_id)
                .filter(PlayerGameStats.player_id == player_id)
                .order_by(Game.game_date.desc())
                .first()
            )
            if latest_team:
                player_current_team_id = latest_team[0]

        player_metrics = _get_metric_results(session, "player", player_id, selected_season)
        salary_records = (
            session.query(PlayerSalary)
            .filter(PlayerSalary.player_id == player_id)
            .order_by(PlayerSalary.season.desc())
            .all()
        )
        salary_rows = [
            SimpleNamespace(
                season=row.season,
                season_label=_season_start_year_label(row.season),
                salary_usd=row.salary_usd,
            )
            for row in salary_records
        ]

    return render_template(
        "player.html",
        player=player,
        selected_career_kind=selected_career_kind,
        career_kind_label=career_kind_label,
        career_overall=career_overall,
        career_season_rows=career_season_rows,
        selected_heatmap_season=selected_heatmap_season,
        heatmap_season_options=heatmap_season_options,
        heatmap_scope_label=heatmap_scope_label,
        shot_dots=shot_dots,
        shot_attempts=shot_attempts,
        shot_made_count=shot_made_count,
        heatmap_zones=heatmap_zones,
        player_current_team_id=player_current_team_id,
        season_options=season_options,
        selected_season=selected_season,
        game_rows=game_rows,
        player_metrics=player_metrics,
        player_awards=player_awards,
        salary_rows=salary_rows,
    )


@app.route("/cn/teams/<team_id>", endpoint="team_page_zh")
@app.route("/teams/<team_id>")
def team_page(team_id: str):
    with SessionLocal() as session:
        team = session.query(Team).filter(Team.team_id == team_id).first()
        if team is None:
            abort(404, description=f"Team {team_id} not found")

        championship_rows = (
            session.query(Award.season)
            .filter(
                Award.award_type == "champion",
                Award.team_id == team_id,
            )
            .order_by(Award.season.desc())
            .all()
        )
        team_championships = [
            {
                "season": int(row.season),
                "season_label": _season_year_label(str(row.season)),
            }
            for row in championship_rows
            if _coerce_award_season(row.season) is not None
        ]

        season_summary_rows = (
            session.query(
                Game.season.label("season"),
                func.sum(case((TeamGameStats.win.is_(True), 1), else_=0)).label("wins"),
                func.sum(case((TeamGameStats.win.is_(False), 1), else_=0)).label("losses"),
                func.count(TeamGameStats.game_id).label("games"),
                func.sum(func.coalesce(TeamGameStats.fgm, 0)).label("fgm"),
                func.sum(func.coalesce(TeamGameStats.fga, 0)).label("fga"),
                func.sum(func.coalesce(TeamGameStats.fg3m, 0)).label("fg3m"),
                func.sum(func.coalesce(TeamGameStats.fg3a, 0)).label("fg3a"),
                func.sum(func.coalesce(TeamGameStats.ftm, 0)).label("ftm"),
                func.sum(func.coalesce(TeamGameStats.fta, 0)).label("fta"),
            )
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(TeamGameStats.team_id == team_id, Game.season.isnot(None))
            .group_by(Game.season)
            .order_by(Game.season.desc())
            .all()
        )
        season_summary = [
            {
                "season": row.season,
                "wins": int(row.wins or 0),
                "losses": int(row.losses or 0),
                "games": int(row.games or 0),
                "fg_pct": _pct_text(int(row.fgm or 0), int(row.fga or 0)),
                "fg3_pct": _pct_text(int(row.fg3m or 0), int(row.fg3a or 0)),
                "ft_pct": _pct_text(int(row.ftm or 0), int(row.fta or 0)),
            }
            for row in season_summary_rows
        ]
        season_summary.sort(key=lambda row: _season_sort_key(row["season"]), reverse=True)

        season_kind = request.args.get("season_kind", "regular")
        if season_kind not in {"regular", "playoffs"}:
            season_kind = "regular"

        if season_kind == "regular":
            season_summary_view = [row for row in season_summary if str(row["season"]).startswith("2")]
        else:
            season_summary_view = [row for row in season_summary if str(row["season"]).startswith("4")]

        current_season = _pick_current_season([row["season"] for row in season_summary])
        season_options = [row["season"] for row in season_summary]
        selected_games_season = request.args.get("games_season")
        if selected_games_season not in season_options:
            selected_games_season = current_season

        teams = _team_map(session)
        current_games = []

        if selected_games_season is not None:
            rows = (
                session.query(TeamGameStats, Game)
                .join(Game, TeamGameStats.game_id == Game.game_id)
                .filter(TeamGameStats.team_id == team_id, Game.season == selected_games_season)
                .order_by(Game.game_date.desc(), Game.game_id.desc())
                .all()
            )

            for stat, game in rows:
                if stat.team_id == game.home_team_id:
                    opponent_id = game.road_team_id
                    where = "Home"
                    team_score = game.home_team_score
                    opp_score = game.road_team_score
                else:
                    opponent_id = game.home_team_id
                    where = "Away"
                    team_score = game.road_team_score
                    opp_score = game.home_team_score

                if team_score is not None and opp_score is not None:
                    score = f"{team_score}-{opp_score}"
                else:
                    score = "-"

                if stat.win is None:
                    result = "-"
                    status = "Not finished"
                elif stat.win:
                    result = "W"
                    status = "Win"
                else:
                    result = "L"
                    status = "Loss"

                current_games.append(
                    {
                        "game_id": game.game_id,
                        "game_date": _fmt_date(game.game_date),
                        "opponent_id": opponent_id,
                        "opponent_name": _team_name(teams, opponent_id),
                        "where": where,
                        "result": result,
                        "score": score,
                        "status": status,
                    }
                )

        team_metrics = _get_metric_results(session, "team", team_id, current_season)

    return render_template(
        "team.html",
        team=team,
        season_summary=season_summary_view,
        season_kind=season_kind,
        current_season=current_season,
        season_options=season_options,
        selected_games_season=selected_games_season,
        current_games=current_games,
        team_metrics=team_metrics,
        team_championships=team_championships,
    )


@app.route("/cn/games/<game_id>", endpoint="game_page_zh")
@app.route("/games/<game_id>")
def game_page(game_id: str):
    with SessionLocal() as session:
        from db.backfill_nba_game_line_score import has_game_line_score

        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            abort(404, description=f"Game {game_id} not found")

        if not has_game_line_score(session, game_id):
            try:
                from db.backfill_nba_game_line_score import back_fill_game_line_score
                back_fill_game_line_score(session, game_id, commit=True)
            except Exception:
                logger.exception("inline line-score fetch failed for game_id=%s", game_id)

        teams = _team_map(session)
        team_stats_rows = (
            session.query(TeamGameStats)
            .filter(TeamGameStats.game_id == game_id)
            .order_by(TeamGameStats.team_id.asc())
            .all()
        )
        team_stats = sorted(team_stats_rows, key=lambda row: 0 if row.team_id == game.home_team_id else 1)

        player_rows = (
            session.query(PlayerGameStats, Player)
            .outerjoin(Player, Player.player_id == PlayerGameStats.player_id)
            .filter(PlayerGameStats.game_id == game_id)
            .order_by(PlayerGameStats.team_id.asc(), PlayerGameStats.starter.desc(), PlayerGameStats.player_id.asc())
            .all()
        )
        players_by_team: dict[str, list[dict[str, str | int]]] = defaultdict(list)
        for stat, player in player_rows:
            player_name = _display_player_name(player) if player is not None else stat.player_id
            players_by_team[stat.team_id].append(
                {
                    "player_id": stat.player_id,
                    "player_name": player_name,
                    "status": _player_status(stat),
                    "minutes": _fmt_minutes(stat.min, stat.sec),
                    "pts": stat.pts if stat.pts is not None else "-",
                    "reb": stat.reb if stat.reb is not None else "-",
                    "ast": stat.ast if stat.ast is not None else "-",
                    "plus_minus": stat.plus if stat.plus is not None else "-",
                }
            )

        ordered_team_ids = [tid for tid in [game.road_team_id, game.home_team_id] if tid]
        for team_id in players_by_team:
            if team_id not in ordered_team_ids:
                ordered_team_ids.append(team_id)

        pbp_rows_raw = (
            session.query(GamePlayByPlay)
            .filter(GamePlayByPlay.game_id == game_id)
            .order_by(GamePlayByPlay.period.asc(), GamePlayByPlay.event_num.asc(), GamePlayByPlay.id.asc())
            .all()
        )
        pbp_rows = [
            {
                "event_num": row.event_num if row.event_num is not None else "-",
                "period": row.period if row.period is not None else "-",
                "clock": row.pc_time or row.wc_time or "-",
                "event_type": _pbp_event_type_label(row.event_msg_type),
                "event_type_code": row.event_msg_type,
                "description": _pbp_text(row) or "-",
                "score": row.score or "-",
                "margin": row.score_margin or "-",
                "team_id": (
                    game.home_team_id if row.home_description
                    else game.road_team_id if row.visitor_description
                    else None
                ),
            }
            for row in pbp_rows_raw
        ]

        # Player name map from already-fetched player_rows (no extra query)
        _player_name_map = {
            str(stat.player_id): (_display_player_name(player) if player else str(stat.player_id))
            for stat, player in player_rows
        }

        # Build score progression for the line chart.
        # PBP score stored as "HOME - AWAY" (see backfill_nba_game_pbp._build_score_and_margin)
        score_progression = [{"t": 0.0, "road": 0, "home": 0, "scorer": None, "desc": None}]
        _prev_road = _prev_home = 0
        for _row in pbp_rows_raw:
            if not _row.score or _row.period is None:
                continue
            try:
                _parts = _row.score.split("-")
                if len(_parts) != 2:
                    continue
                _home_s, _road_s = int(_parts[0].strip()), int(_parts[1].strip())
            except (ValueError, AttributeError):
                continue
            if _road_s == _prev_road and _home_s == _prev_home:
                continue
            _period = int(_row.period)
            _clock = _row.pc_time or "0:00"
            try:
                _m, _s = _clock.split(":")
                _remaining = int(_m) * 60 + int(_s)
            except Exception:
                _remaining = 0
            if _period <= 4:
                _offset = (_period - 1) * 12 * 60
                _dur = 12 * 60
            else:
                _offset = 48 * 60 + (_period - 5) * 5 * 60
                _dur = 5 * 60
            _elapsed = round((_offset + _dur - _remaining) / 60, 3)
            # Pick description from the side that scored
            _raw_desc = (
                _row.home_description if _home_s > _prev_home
                else _row.visitor_description if _road_s > _prev_road
                else None
            ) or ""
            # Try player1_id lookup first, fall back to first word of description
            if _row.player1_id and str(_row.player1_id) in _player_name_map:
                _scorer = _player_name_map[str(_row.player1_id)]
            else:
                _scorer = _raw_desc.split()[0] if _raw_desc.strip() else None
            # Strip parenthetical clauses "(N PTS) (Assist)" to keep desc compact
            _desc = _raw_desc.split("(")[0].strip() or None
            score_progression.append({
                "t": _elapsed, "road": _road_s, "home": _home_s,
                "scorer": _scorer, "desc": _desc,
            })
            _prev_road, _prev_home = _road_s, _home_s

        # Derive per-period point totals from PBP using shared helper
        from metrics.helpers import get_quarter_scores as _get_quarter_scores
        _qs = _get_quarter_scores(session, game_id)
        quarter_scores = [{"period": q["period"], "home": q["home_pts"], "road": q["road_pts"]} for q in _qs]

        shot_rows_raw = (
            session.query(ShotRecord)
            .filter(ShotRecord.game_id == game_id, ShotRecord.shot_attempted.is_(True))
            .order_by(ShotRecord.id.asc())
            .all()
        )
        shot_player_ids = sorted({str(row.player_id) for row in shot_rows_raw if row.player_id})
        shot_player_map = {}
        if shot_player_ids:
            shot_players = session.query(Player).filter(Player.player_id.in_(shot_player_ids)).all()
            shot_player_map = {
                str(player.player_id): (_display_player_name(player) or str(player.player_id))
                for player in shot_players
            }
        shot_rows = []
        shot_rows_by_team: dict[str, list[dict[str, str | int | bool]]] = defaultdict(list)
        shot_made_count_by_team: dict[str, int] = defaultdict(int)
        shot_miss_count_by_team: dict[str, int] = defaultdict(int)
        made_count = 0
        miss_count = 0
        for row in shot_rows_raw:
            if row.loc_x is None or row.loc_y is None:
                continue
            is_made = bool(row.shot_made)
            period = int(row.period or 0)
            if period <= 4:
                period_label = f"Q{period}" if period > 0 else "-"
            else:
                period_label = f"OT{period - 4}"
            if is_made:
                made_count += 1
            else:
                miss_count += 1
            shot = {
                "x": row.loc_x,
                "y": row.loc_y,
                "made": is_made,
                "period": period,
                "period_label": period_label,
                "clock": _fmt_minutes(row.min, row.sec),
                "team_id": row.team_id,
                "team_name": _team_name(teams, row.team_id),
                "player_id": row.player_id,
                "player_name": shot_player_map.get(str(row.player_id), str(row.player_id or "-")),
                "shot_type": row.shot_type or "-",
                "shot_distance": row.shot_distance if row.shot_distance is not None else "-",
            }
            shot_rows.append(shot)
            if row.team_id:
                shot_rows_by_team[row.team_id].append(shot)
                if is_made:
                    shot_made_count_by_team[row.team_id] += 1
                else:
                    shot_miss_count_by_team[row.team_id] += 1

        shot_chart_team_ids = [tid for tid in [game.road_team_id, game.home_team_id] if tid]
        for team_id in shot_rows_by_team:
            if team_id not in shot_chart_team_ids:
                shot_chart_team_ids.append(team_id)

        import json as _json
        score_progression_json = _json.dumps(score_progression)
        road_abbr = _team_abbr(teams, game.road_team_id)
        home_abbr = _team_abbr(teams, game.home_team_id)
        home_team_id = game.home_team_id
        game_analysis_issues = [
            {
                "id": item.id,
                "issue_id": item.issue_id,
                "issue_identifier": item.issue_identifier,
                "issue_url": _paperclip_issue_url(item.issue_identifier),
                "issue_status": item.issue_status,
                "title": item.title,
                "trigger_source": item.trigger_source,
                "source_date": item.source_date,
                "created_at": item.created_at,
                "updated_at": item.updated_at,
                "created_at_label": item.created_at.replace("T", " ")[:19] if item.created_at else None,
                "updated_at_label": item.updated_at.replace("T", " ")[:19] if item.updated_at else None,
                "posts": [
                    {
                        "post_id": int(post["post_id"]),
                        "topic": str(post.get("topic") or ""),
                        "status": str(post.get("status") or ""),
                        "source_date": str(post.get("source_date") or ""),
                        "discovered_via": str(post.get("discovered_via") or ""),
                    }
                    for post in item.posts
                ],
            }
            for item in game_analysis_issue_history(game_id)
        ]

    return render_template(
        "game.html",
        game=game,
        team_name=lambda team_id: _team_name(teams, team_id),
        team_abbr=lambda team_id: _team_abbr(teams, team_id),
        fmt_date=_fmt_date,
        team_stats=team_stats,
        players_by_team=players_by_team,
        ordered_team_ids=ordered_team_ids,
        pbp_rows=pbp_rows,
        shot_rows=shot_rows,
        shot_rows_by_team=shot_rows_by_team,
        shot_chart_team_ids=shot_chart_team_ids,
        shot_made_count=made_count,
        shot_miss_count=miss_count,
        shot_made_count_by_team=shot_made_count_by_team,
        shot_miss_count_by_team=shot_miss_count_by_team,
        shot_backfill_status=request.args.get("shot_backfill"),
        shot_backfill_count=request.args.get("shot_count"),
        score_progression_json=score_progression_json,
        road_abbr=road_abbr,
        home_abbr=home_abbr,
        quarter_scores=quarter_scores,
        home_team_id=home_team_id,
        game_analysis_issues=game_analysis_issues,
    )


@app.get("/cn/games/<game_id>/fragment/metrics", endpoint="game_fragment_metrics_zh")
@app.get("/games/<game_id>/fragment/metrics")
def game_fragment_metrics(game_id: str):
    """Async fragment: game metrics section."""
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            abort(404)
        game_metrics = _get_metric_results(session, "game", game_id, game.season)
    return render_template("_game_metrics.html", game_metrics=game_metrics)


@app.route("/cn/metrics", endpoint="metrics_browse_zh")
@app.route("/metrics")
def metrics_browse():
    scope_filter = request.args.get("scope", "")
    status_filter = request.args.get("status", "")  # draft | published | ""
    search_query = request.args.get("q", "").strip()

    cur_user = _current_user()
    with SessionLocal() as session:
        metrics_list, metrics_has_more = _catalog_metrics_page(
            session,
            scope_filter=scope_filter,
            status_filter=status_filter,
            current_user_id=cur_user.id if cur_user else None,
        )
        llm_default_model = get_llm_model_for_purpose(session, "search")
        metrics_total = len(metrics_list) if not metrics_has_more else None
        top3_by_metric = _catalog_top3(session, metrics_list)
        feature_access = get_feature_access_config(session)

    return render_template(
        "metrics.html",
        metrics_list=metrics_list,
        metrics_total=metrics_total,
        metrics_has_more=metrics_has_more,
        metrics_page_size=_METRICS_CATALOG_PAGE_SIZE,
        scope_filter=scope_filter,
        status_filter=status_filter,
        search_query=search_query,
        top3_by_metric=top3_by_metric,
        llm_default_model=llm_default_model,
        llm_available_models=available_llm_models(),
        **_build_metric_feature_context(feature_access),
    )


@app.get("/api/metrics/catalog")
def api_metrics_catalog():
    scope_filter = request.args.get("scope", "")
    status_filter = request.args.get("status", "")
    offset = max(0, request.args.get("offset", 0, type=int))
    limit = request.args.get("limit", _METRICS_CATALOG_PAGE_SIZE, type=int)
    limit = max(1, min(limit, 48))

    cur_user = _current_user()
    with SessionLocal() as session:
        metrics_slice, has_more = _catalog_metrics_page(
            session,
            scope_filter=scope_filter,
            status_filter=status_filter,
            current_user_id=cur_user.id if cur_user else None,
            offset=offset,
            limit=limit,
        )
        top3_by_metric = _catalog_top3(session, metrics_slice)

    html = render_template(
        "_metrics_catalog_cards.html",
        metrics_list=metrics_slice,
        top3_by_metric=top3_by_metric,
    )
    next_offset = offset + len(metrics_slice)
    return jsonify(
        {
            "ok": True,
            "html": html,
            "count": len(metrics_slice),
            "offset": offset,
            "next_offset": next_offset,
            "has_more": has_more,
            "total": next_offset if not has_more else None,
        }
    )


@app.get("/api/metrics/catalog-count")
def api_metrics_catalog_count():
    scope_filter = request.args.get("scope", "")
    status_filter = request.args.get("status", "")
    with SessionLocal() as session:
        total = _catalog_metrics_total(
            session,
            scope_filter=scope_filter,
            status_filter=status_filter,
        )
    return jsonify({"ok": True, "total": total})


@app.route("/cn/metrics/mine", endpoint="my_metrics_zh")
@app.route("/metrics/mine")
def my_metrics():
    denied = _require_login_page()
    if denied:
        return denied

    cur_user = _current_user()
    if cur_user is None:
        return redirect(url_for("auth_login", next=request.url))

    with SessionLocal() as session:
        feature_access = get_feature_access_config(session)
        drafts = (
            session.query(MetricDefinitionModel)
            .filter(
                MetricDefinitionModel.created_by_user_id == cur_user.id,
                MetricDefinitionModel.base_metric_key.is_(None),
                MetricDefinitionModel.status == "draft",
            )
            .order_by(MetricDefinitionModel.updated_at.desc())
            .all()
        )
        published = (
            session.query(MetricDefinitionModel)
            .filter(
                MetricDefinitionModel.created_by_user_id == cur_user.id,
                MetricDefinitionModel.base_metric_key.is_(None),
                MetricDefinitionModel.status.in_(["published", "disabled"]),
            )
            .order_by(MetricDefinitionModel.created_at.desc())
            .all()
        )

    return render_template(
        "my_metrics.html",
        drafts=drafts,
        published=published,
        total_metrics=len(drafts) + len(published),
        scope_labels={
            "player": "Player",
            "player_franchise": "Player Franchise",
            "team": "Team",
            "game": "Game",
            "season": "Season",
        },
        **_build_metric_feature_context(feature_access),
    )


@app.route("/cn/metrics/new", endpoint="metric_new_zh")
@app.route("/metrics/new")
def metric_new():
    denied = _require_metric_creator_page()
    if denied:
        return denied
    initial_expression = request.args.get("expression", "").strip()
    with SessionLocal() as session:
        all_seasons = sorted(
            [r[0] for r in session.query(Game.season).distinct().all()],
            reverse=True,
        )
        current_season = _pick_current_season(all_seasons)
        llm_default_model = get_llm_model_for_purpose(session, "generate")
        feature_access = get_feature_access_config(session)
    return render_template(
        "metric_new.html",
        current_season=current_season,
        all_seasons=all_seasons,
        initial_expression=initial_expression,
        edit_metric=None,
        llm_default_model=llm_default_model,
        llm_available_models=available_llm_models(),
        **_build_metric_feature_context(feature_access),
    )


@app.route("/cn/metrics/<metric_key>/edit", endpoint="metric_edit_zh")
@app.route("/metrics/<metric_key>/edit")
def metric_edit(metric_key: str):
    denied = _require_metric_creator_page()
    if denied:
        return denied
    import json as _json

    with SessionLocal() as session:
        from metrics.framework.runtime import get_metric as _get_metric

        m = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
        if m is None:
            abort(404)
        if getattr(m, "managed_family", False) and getattr(m, "variant", FAMILY_VARIANT_SEASON) == FAMILY_VARIANT_CAREER:
            return redirect(url_for("metric_edit", metric_key=getattr(m, "base_metric_key", None) or getattr(m, "family_key", None) or family_base_key(m.key)))

        all_seasons = sorted(
            [r[0] for r in session.query(Game.season).distinct().all()],
            reverse=True,
        )
        current_season = _pick_current_season(all_seasons)
        runtime_metric = _get_metric(metric_key, session=session)

        edit_data = {
            "key": m.key,
            "name": m.name,
            "name_zh": m.name_zh or "",
            "description": m.description or "",
            "description_zh": m.description_zh or "",
            "scope": m.scope,
            "category": m.category or "",
            "code": m.code_python or "",
            "expression": m.expression or "",
            "min_sample": m.min_sample,
            "rank_order": getattr(runtime_metric, "rank_order", "desc"),
            "season_types": list(getattr(runtime_metric, "season_types", ("regular", "playoffs", "playin")) or ()),
            "max_results_per_season": getattr(runtime_metric, "max_results_per_season", None) or m.max_results_per_season,
            "group_key": m.group_key,
            "status": m.status,
        }
        llm_default_model = get_llm_model_for_purpose(session, "generate")
        feature_access = get_feature_access_config(session)

    return render_template(
        "metric_new.html",
        current_season=current_season,
        all_seasons=all_seasons,
        initial_expression="",
        edit_metric=edit_data,
        llm_default_model=llm_default_model,
        llm_available_models=available_llm_models(),
        **_build_metric_feature_context(feature_access),
    )


@app.post("/api/metrics/search")
@limiter.limit("30 per minute")
def api_metric_search():
    denied = _require_feature_json("metric_search")
    if denied:
        return denied
    from metrics.framework.search import rank_metrics

    body = request.get_json(force=True) or {}
    query = (body.get("query") or "").strip()
    scope_filter = (body.get("scope") or "").strip()
    status_filter = (body.get("status") or "").strip()
    if status_filter == "draft":
        status_filter = ""
    requested_model = (body.get("model") or "").strip() if is_admin() else None
    if not query:
        return jsonify({"ok": False, "error": "query is required"}), 400
    if len(query) > 200:
        return jsonify({"ok": False, "error": "query too long (max 200 characters)"}), 400

    usage_payload: dict = {}
    started_at = time.perf_counter()
    candidate_count = 0

    with SessionLocal() as session:
        catalog = _catalog_metrics(session, scope_filter=scope_filter, status_filter=status_filter)
        candidate_count = len(catalog)
        try:
            llm_model = resolve_llm_model(session, requested_model=requested_model, purpose="search")
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    try:
        ranked = rank_metrics(
            query,
            catalog,
            limit=8,
            model=llm_model,
            usage_recorder=usage_payload.update,
        )
    except ValueError as exc:
        _record_ai_usage_event(
            feature="metric_search",
            operation="rank",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=False,
            http_status=400,
            error_code=type(exc).__name__,
            metadata={
                "query_chars": len(query),
                "query_text": _ai_usage_preview(query),
                "candidate_count": candidate_count,
                "scope_filter": scope_filter,
                "status_filter": status_filter,
            },
        )
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("metric search failed")
        _record_ai_usage_event(
            feature="metric_search",
            operation="rank",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=False,
            http_status=500,
            error_code=type(exc).__name__,
            metadata={
                "query_chars": len(query),
                "query_text": _ai_usage_preview(query),
                "candidate_count": candidate_count,
                "scope_filter": scope_filter,
                "status_filter": status_filter,
            },
        )
        return jsonify({"ok": False, "error": str(exc)}), 500

    by_key = {metric["key"]: metric for metric in catalog}
    matches = []
    for ranked_item in ranked:
        metric = by_key.get(ranked_item["key"])
        if metric is None:
            continue
        matches.append({**metric, "reason": ranked_item["reason"]})

    _record_ai_usage_event(
        feature="metric_search",
        operation="rank",
        model=llm_model,
        usage=usage_payload,
        started_at=started_at,
        success=True,
        http_status=200,
        metadata={
            "query_chars": len(query),
            "query_text": _ai_usage_preview(query),
            "candidate_count": candidate_count,
            "match_count": len(matches),
            "scope_filter": scope_filter,
            "status_filter": status_filter,
        },
    )
    return jsonify({"ok": True, "matches": matches})


@app.post("/api/metrics/check-similar")
@limiter.limit("15 per minute")
def api_metric_check_similar():
    denied = _require_metric_creator_json()
    if denied:
        return denied
    from metrics.framework.generator import check_similar
    body = request.get_json(force=True) or {}
    expression = (body.get("expression") or "").strip()
    conversation_id = (body.get("conversationId") or "").strip() or None
    requested_model = (body.get("model") or "").strip() if is_admin() else None
    if not expression:
        return jsonify({"ok": False, "error": "expression is required"}), 400
    usage_payload: dict = {}
    started_at = time.perf_counter()
    candidate_count = 0
    with SessionLocal() as session:
        try:
            llm_model = resolve_llm_model(session, requested_model=requested_model, purpose="search")
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        catalog = _catalog_metrics(session, status_filter="published")
        candidate_count = len(catalog)
    try:
        similar = check_similar(
            expression,
            catalog,
            model=llm_model,
            usage_recorder=usage_payload.update,
        )
        success = True
        error_code = None
    except Exception as exc:
        logger.exception("check-similar failed")
        similar = []
        success = False
        error_code = type(exc).__name__
    _record_ai_usage_event(
        feature="metric_create",
        operation="check_similar",
        model=llm_model,
        usage=usage_payload,
        started_at=started_at,
        success=success,
        http_status=200,
        error_code=error_code,
        conversation_id=conversation_id,
        metadata={
            "input_chars": len(expression),
            "input_text": _ai_usage_preview(expression),
            "candidate_count": candidate_count,
            "similar_count": len(similar),
        },
    )
    return jsonify({"ok": True, "similar": similar})


@app.post("/api/metrics/generate")
@limiter.limit("10 per minute")
def api_metric_generate():
    denied = _require_metric_creator_json()
    if denied:
        return denied
    from metrics.framework.generator import generate
    body = request.get_json(force=True) or {}
    expression = body.get("expression", "").strip()
    history = body.get("history")  # list of {"role", "content"} or None
    existing = body.get("existing")  # dict with current metric info for edit mode
    conversation_id = (body.get("conversationId") or "").strip() or None
    requested_model = (body.get("model") or "").strip() if is_admin() else None
    if not expression:
        return jsonify({"ok": False, "error": "expression is required"}), 400
    usage_payload: dict = {}
    started_at = time.perf_counter()
    with SessionLocal() as session:
        try:
            llm_model = resolve_llm_model(session, requested_model=requested_model, purpose="generate")
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
    try:
        spec = generate(
            expression,
            history=history,
            existing=existing,
            model=llm_model,
            usage_recorder=usage_payload.update,
        )
        response_type = (spec.get("responseType") or "code") if isinstance(spec, dict) else "code"
        if response_type == "clarification":
            _record_ai_usage_event(
                feature="metric_create",
                operation="generate",
                model=llm_model,
                usage=usage_payload,
                started_at=started_at,
                success=True,
                http_status=200,
                conversation_id=conversation_id,
                metadata={
                    "input_chars": len(expression),
                    "input_text": _ai_usage_preview(expression),
                    "history_turn_count": len(history or []),
                    "is_edit": bool(existing),
                    "response_type": "clarification",
                    "metric_key": (existing or {}).get("key"),
                },
            )
            return jsonify({
                "ok": True,
                "responseType": "clarification",
                "message": spec.get("message", ""),
            })
        _record_ai_usage_event(
            feature="metric_create",
            operation="generate",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=True,
            http_status=200,
            conversation_id=conversation_id,
            metadata={
                "input_chars": len(expression),
                "input_text": _ai_usage_preview(expression),
                "history_turn_count": len(history or []),
                "is_edit": bool(existing),
                "response_type": "code",
                "metric_key": (existing or {}).get("key") or (spec or {}).get("key"),
            },
        )
        return jsonify({
            "ok": True,
            "responseType": "code",
            "spec": spec,
        })
    except ValueError as exc:
        _record_ai_usage_event(
            feature="metric_create",
            operation="generate",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=False,
            http_status=400,
            error_code=type(exc).__name__,
            conversation_id=conversation_id,
            metadata={
                "input_chars": len(expression),
                "input_text": _ai_usage_preview(expression),
                "history_turn_count": len(history or []),
                "is_edit": bool(existing),
                "metric_key": (existing or {}).get("key"),
            },
        )
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("metric generate failed")
        _record_ai_usage_event(
            feature="metric_create",
            operation="generate",
            model=llm_model,
            usage=usage_payload,
            started_at=started_at,
            success=False,
            http_status=500,
            error_code=type(exc).__name__,
            conversation_id=conversation_id,
            metadata={
                "input_chars": len(expression),
                "input_text": _ai_usage_preview(expression),
                "history_turn_count": len(history or []),
                "is_edit": bool(existing),
                "metric_key": (existing or {}).get("key"),
            },
        )
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/metrics/preview")
@limiter.limit("20 per minute")
def api_metric_preview():
    body = request.get_json(force=True) or {}
    definition = body.get("definition")
    code_python = (body.get("code") or "").strip()
    scope = body.get("scope", "player")
    season = body.get("season", "")
    rank_order = str(body.get("rank_order") or "").strip().lower() or None
    season_types = body.get("season_types")

    if not definition and not code_python:
        return jsonify({"ok": False, "error": "definition or code is required"}), 400

    with SessionLocal() as session:
        try:
            if code_python:
                rows = _preview_code_metric(
                    session,
                    code_python,
                    scope,
                    season,
                    limit=25,
                    rank_order_override=rank_order,
                    season_types_override=season_types,
                )
            else:
                from metrics.framework.rule_engine import preview as re_preview
                rows = re_preview(session, definition, scope, season, limit=25)
        except Exception as exc:
            logger.exception("metric preview failed")
            return jsonify({"ok": False, "error": str(exc)}), 400

        # Bulk resolve names
        entity_ids = [r["entity_id"] for r in rows]
        if scope == "player":
            names = {
                p.player_id: p.full_name
                for p in session.query(Player.player_id, Player.full_name)
                .filter(Player.player_id.in_(entity_ids)).all()
            }
        elif scope == "team":
            tm = _team_map(session)
            names = {tid: _team_name(tm, tid) for tid in entity_ids}
        elif scope == "game":
            names, game_dates = _resolve_game_entity_names(session, entity_ids)
        else:
            names = {}

        for r in rows:
            r["entity_name"] = names.get(r["entity_id"], r.get("value_str") or r["entity_id"])
            if scope == "game":
                r["date"] = game_dates.get(r["entity_id"], "")

    return jsonify({"ok": True, "rows": rows})


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


@app.post("/api/metrics")
def api_metric_create():
    denied = _require_metric_creator_json()
    if denied:
        return denied
    import json as _json
    from datetime import datetime
    from metrics.framework.runtime import get_metric as _get_runtime_metric
    body = request.get_json(force=True) or {}

    key = (body.get("key") or "").strip().lower().replace(" ", "_")
    name = (body.get("name") or "").strip()
    name_zh = (body.get("name_zh") or "").strip()
    scope = (body.get("scope") or "").strip()
    code_python = (body.get("code") or "").strip()
    definition = body.get("definition")
    rank_order_override = str(body.get("rank_order") or "").strip().lower() or None
    season_types_override = body.get("season_types")

    if not code_python and not definition:
        return jsonify({"ok": False, "error": "code or definition is required"}), 400

    # Determine source type
    source_type = "code" if code_python else "rule"
    code_metadata = None

    # Validate code by loading it
    if code_python:
        try:
            code_metadata = _code_metric_metadata_from_code(
                code_python,
                rank_order_override=rank_order_override,
                season_types_override=season_types_override,
            )
        except Exception as exc:
            return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400
        code_python = code_metadata["code_python"]
        key = code_metadata["key"]
        name = code_metadata["name"]
        name_zh = code_metadata.get("name_zh", "")
        scope = code_metadata["scope"]
        description = code_metadata["description"]
        description_zh = code_metadata.get("description_zh", "")
        category = code_metadata["category"]
        min_sample = code_metadata["min_sample"]
    else:
        if not key:
            return jsonify({"ok": False, "error": "key is required"}), 400
        if not name or not scope:
            return jsonify({"ok": False, "error": "name and scope are required"}), 400
        description = body.get("description", "")
        description_zh = body.get("description_zh", "")
        category = body.get("category", "")
        min_sample = int(body.get("min_sample", 1))
        definition = definition or {}

    if is_reserved_career_key(key):
        return jsonify({"ok": False, "error": "Keys ending with '_career' are reserved for managed sibling metrics"}), 409

    supports_career, career_only, _ = _metric_supports_career(
        source_type,
        scope=scope,
        code_metadata=code_metadata,
        definition=definition if source_type == "rule" else None,
    )

    with SessionLocal() as session:
        # Only check published metrics for key conflict (drafts use prefixed keys)
        reserved_keys = [key]
        if supports_career and not career_only:
            reserved_keys.append(family_career_key(key))
        for reserved_key in reserved_keys:
            if _get_runtime_metric(reserved_key, session=session) is not None:
                return jsonify({"ok": False, "error": f"Key '{reserved_key}' is already published"}), 409
            existing = session.query(MetricDefinitionModel).filter(
                MetricDefinitionModel.key == reserved_key,
                MetricDefinitionModel.status == "published",
            ).first()
            if existing:
                return jsonify({"ok": False, "error": f"Key '{reserved_key}' is already published"}), 409

        now = datetime.utcnow()
        cur_user = _current_user()

        # Prefix key for draft storage so multiple users can draft the same key
        draft_key = _make_draft_key(cur_user.id, key) if cur_user else key
        if code_python:
            code_python = _replace_key_in_code(code_python, key, draft_key)

        m = MetricDefinitionModel(
            key=draft_key,
            family_key=draft_key,
            variant=FAMILY_VARIANT_CAREER if career_only else FAMILY_VARIANT_SEASON,
            base_metric_key=None,
            managed_family=False,
            name=name,
            name_zh=name_zh or None,
            description=description,
            description_zh=description_zh or None,
            scope=scope,
            category=category,
            group_key=body.get("group_key"),
            source_type=source_type,
            status="draft",
            definition_json=_json.dumps(definition) if definition else None,
            code_python=code_python or None,
            expression=body.get("expression", ""),
            min_sample=min_sample,
            created_by_user_id=cur_user.id if cur_user else None,
            created_at=now,
            updated_at=now,
        )
        session.add(m)
        session.flush()
        _sync_metric_family(
            session,
            m,
            source_type=source_type,
            name=name,
            name_zh=name_zh,
            description=description,
            description_zh=description_zh,
            scope=scope,
            category=category,
            group_key=body.get("group_key"),
            expression=body.get("expression", ""),
            min_sample=min_sample,
            code_python=code_python,
            definition=definition if source_type == "rule" else None,
            code_metadata=code_metadata,
            now=now,
        )
        session.commit()
        return jsonify({"ok": True, "key": draft_key}), 201


@app.post("/api/metrics/<metric_key>/publish")
def api_metric_publish(metric_key: str):
    denied = _require_metric_creator_json()
    if denied:
        return denied
    from datetime import datetime
    from metrics.framework.runtime import get_metric as _get_runtime_metric

    with SessionLocal() as session:
        m = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
        if m is None:
            return jsonify({"ok": False, "error": "Not found"}), 404
        base_row = _metric_family_base_row(session, m)
        if not getattr(base_row, "key", None):
            base_row.key = metric_key

        # If draft key has prefix, strip it and rename to the clean key
        old_key = base_row.key
        clean_key = _strip_draft_prefix(old_key)
        needs_rename = _is_draft_key(old_key)

        if needs_rename:
            # Check that the clean key isn't already published
            if _get_runtime_metric(clean_key, session=session) is not None:
                return jsonify({"ok": False, "error": f"Key '{clean_key}' is already published by another user"}), 409
            existing_published = session.query(MetricDefinitionModel).filter(
                MetricDefinitionModel.key == clean_key,
                MetricDefinitionModel.status == "published",
            ).first()
            if existing_published:
                return jsonify({"ok": False, "error": f"Key '{clean_key}' is already published by another user"}), 409

        family_rows = _metric_family_rows(session, base_row)
        now = datetime.utcnow()
        for row in family_rows:
            if row.status == "archived":
                continue
            if needs_rename:
                old_row_key = row.key
                new_row_key = old_row_key.replace(old_key, clean_key, 1)
                row.key = new_row_key
                row.family_key = clean_key
                if row.base_metric_key and _is_draft_key(row.base_metric_key):
                    row.base_metric_key = _strip_draft_prefix(row.base_metric_key)
                if row.code_python:
                    row.code_python = _replace_key_in_code(row.code_python, old_row_key, new_row_key)
            row.status = "published"
            row.updated_at = now
        session.commit()
        dispatch_key = clean_key if needs_rename else getattr(base_row, "key", metric_key)
    try:
        _dispatch_metric_backfill(dispatch_key)
    except Exception:
        logger.exception("Failed to enqueue backfill for %s", dispatch_key)
        return jsonify({
            "ok": True,
            "key": dispatch_key,
            "status": "published",
            "warning": "Metric published but backfill enqueue failed. Run manually.",
        })
    return jsonify({"ok": True, "key": dispatch_key, "status": "published"})


@app.post("/api/admin/metrics/<metric_key>/toggle-enabled")
def api_admin_toggle_metric_enabled(metric_key: str):
    """Enable or disable a published metric. Disabled metrics are hidden from
    non-admin users and excluded from daily computation."""
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime

    with SessionLocal() as session:
        m = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
        if m is None:
            return jsonify({"ok": False, "error": "Not found"}), 404

        # Always operate on the base (season) row, even if triggered from a career page
        base_row = _metric_family_base_row(session, m)
        if base_row.status not in ("published", "disabled"):
            return jsonify({"ok": False, "error": f"Cannot toggle metric with status '{base_row.status}'"}), 400

        new_status = "disabled" if base_row.status == "published" else "published"
        family_rows = _metric_family_rows(session, base_row)
        now = datetime.utcnow()
        toggled_keys = []
        for row in family_rows:
            if row.status in ("published", "disabled"):
                row.status = new_status
                row.updated_at = now
                toggled_keys.append(row.key)
        session.commit()

    return jsonify({"ok": True, "status": new_status, "toggled_keys": toggled_keys})


@app.get("/api/metrics/<metric_key>/qualifying-games")
@limiter.limit("30 per minute")
def api_qualifying_games(metric_key: str):
    """List games where an entity met the qualifying criteria for a metric."""
    entity_id = request.args.get("entity_id")
    season = request.args.get("season")
    if not entity_id:
        return jsonify({"ok": False, "error": "entity_id is required"}), 400
    page = max(1, int(request.args.get("page", 1) or 1))
    page_size = 10
    with SessionLocal() as session:
        from metrics.framework.runtime import (
            _aggregated_career_qualification_game_ids,
            get_metric as _rt_get_metric,
        )

        runtime_metric = _rt_get_metric(metric_key, session=session)
        aggregated_game_ids = _aggregated_career_qualification_game_ids(
            runtime_metric,
            session,
            season,
            entity_id,
        )

        if aggregated_game_ids is not None:
            games_q = session.query(Game).filter(Game.game_id.in_(aggregated_game_ids))
            total = games_q.count()
            rows = [
                (None, game)
                for game in games_q.order_by(Game.game_date.desc(), Game.game_id.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
                .all()
            ]
        else:
            base_q = session.query(MetricRunLog, Game).join(
                Game, MetricRunLog.game_id == Game.game_id,
            ).filter(
                MetricRunLog.metric_key == metric_key,
                MetricRunLog.entity_id == entity_id,
                MetricRunLog.qualified == True,
            )
            if season:
                base_q = base_q.filter(MetricRunLog.season == season)
            total = base_q.count()
            rows = base_q.order_by(Game.game_date.desc(), Game.game_id.desc()).offset((page - 1) * page_size).limit(page_size).all()
        team_map = _team_map(session)

        # Batch-load player/team stats for these games
        game_ids = [game.game_id for _, game in rows]

        player_stats_map = {}
        team_stats_map = {}
        if game_ids:
            for ps in session.query(PlayerGameStats).filter(
                PlayerGameStats.game_id.in_(game_ids),
                PlayerGameStats.player_id == entity_id,
            ).all():
                player_stats_map[ps.game_id] = ps
            for ts in session.query(TeamGameStats).filter(
                TeamGameStats.game_id.in_(game_ids),
                TeamGameStats.team_id == entity_id,
            ).all():
                team_stats_map[ts.game_id] = ts

        # Build home/road score lookup from TeamGameStats
        game_scores = {}
        if game_ids:
            for ts in session.query(TeamGameStats).filter(
                TeamGameStats.game_id.in_(game_ids),
            ).all():
                game_scores.setdefault(ts.game_id, {})[str(ts.team_id)] = int(ts.pts or 0)

        games = []
        for log, game in rows:
            gid = game.game_id
            home_id = str(game.home_team_id)
            road_id = str(game.road_team_id)
            scores = game_scores.get(gid, {})
            home_score = scores.get(home_id)
            road_score = scores.get(road_id)

            entry = {
                "game_id": gid,
                "game_date": game.game_date.isoformat() if game.game_date else None,
                "season": game.season,
                "home_team": _team_abbr(team_map, game.home_team_id),
                "road_team": _team_abbr(team_map, game.road_team_id),
                "home_team_id": str(game.home_team_id),
                "road_team_id": str(game.road_team_id),
                "home_score": home_score,
                "road_score": road_score,
                "delta": json.loads(log.delta_json) if log and log.delta_json else None,
            }

            # Add player stat line and W/L if available
            ps = player_stats_map.get(gid)
            if ps:
                entry["player_line"] = f"{int(ps.pts or 0)} PTS, {int(ps.reb or 0)} REB, {int(ps.ast or 0)} AST"
                entity_team_id = str(ps.team_id) if ps.team_id else None
                if entity_team_id and game.wining_team_id is not None:
                    entry["win"] = str(game.wining_team_id) == entity_team_id

            # Add team stat line if available
            ts = team_stats_map.get(gid)
            if ts:
                entry["team_line"] = f"{int(ts.pts or 0)} PTS"
                entry["win"] = bool(ts.win)

            games.append(entry)

        total_pages = max(1, (total + page_size - 1) // page_size)
        return jsonify({
            "ok": True,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "games": games,
        })


@app.get("/api/metrics/<metric_key>/backfill-status")
def api_metric_backfill_status(metric_key: str):
    with SessionLocal() as session:
        metric_def, backfill = _build_metric_backfill_status(session, metric_key)
        if metric_def is None:
            return jsonify({"ok": False, "error": "Not found"}), 404
        return jsonify({"ok": True, "metric_key": metric_key, "backfill": backfill})


@app.post("/api/metrics/<metric_key>/update")
def api_metric_update(metric_key: str):
    """Update an existing metric's code/settings and optionally re-backfill."""
    denied = _require_metric_creator_json()
    if denied:
        return denied
    import json as _json
    from datetime import datetime

    body = request.get_json(force=True) or {}
    code_python = (body.get("code") or "").strip()
    code_metadata = None
    rank_order_override = str(body.get("rank_order") or "").strip().lower() or None
    season_types_override = body.get("season_types")

    with SessionLocal() as session:
        m = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
        if m is None:
            return jsonify({"ok": False, "error": "Not found"}), 404
        m = _metric_family_base_row(session, m)
        if not getattr(m, "key", None):
            m.key = metric_key
        result_key = m.key  # capture before session closes

        if code_python:
            try:
                code_metadata = _code_metric_metadata_from_code(
                    code_python,
                    expected_key=m.key,
                    rank_order_override=rank_order_override,
                    season_types_override=season_types_override,
                )
            except Exception as exc:
                return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400
            code_python = code_metadata["code_python"]

        metadata_fields = {"code", "definition", "name", "name_zh", "description", "description_zh", "scope", "category", "min_sample", "group_key", "expression", "rank_order", "season_types"}
        if not any(field in body for field in metadata_fields):
            m.updated_at = datetime.utcnow()
            session.commit()
        else:
            source_type = "code" if code_python else ("rule" if body.get("definition") is not None else getattr(m, "source_type", "rule"))
            if source_type == "code":
                source_code = code_python or getattr(m, "code_python", "") or ""
                if code_metadata is None and source_code:
                    code_metadata = _code_metric_metadata_from_code(
                        source_code,
                        expected_key=m.key,
                        rank_order_override=rank_order_override,
                        season_types_override=body.get("season_types"),
                    )
                    source_code = code_metadata["code_python"]
                source_definition = None
                name = body.get("name") or code_metadata["name"]
                name_zh = body.get("name_zh") if body.get("name_zh") is not None else code_metadata.get("name_zh", "")
                description = body.get("description") if body.get("description") is not None else code_metadata["description"]
                description_zh = body.get("description_zh") if body.get("description_zh") is not None else code_metadata.get("description_zh", "")
                scope = body.get("scope") or code_metadata["scope"]
                category = body.get("category") or code_metadata["category"]
                min_sample = int(body.get("min_sample") or code_metadata["min_sample"])
                if "max_results_per_season" in body:
                    code_metadata["max_results_per_season"] = body["max_results_per_season"]
            else:
                source_code = None
                source_definition = body.get("definition")
                if source_definition is None:
                    try:
                        source_definition = _json.loads(getattr(m, "definition_json", None) or "{}")
                    except Exception:
                        source_definition = {}
                name = body.get("name", getattr(m, "name", metric_key))
                name_zh = body.get("name_zh", getattr(m, "name_zh", "") or "")
                description = body["description"] if body.get("description") is not None else (getattr(m, "description", "") or "")
                description_zh = body.get("description_zh", getattr(m, "description_zh", "") or "")
                scope = body.get("scope", getattr(m, "scope", "player"))
                category = body.get("category", getattr(m, "category", "") or "")
                min_sample = int(body.get("min_sample", getattr(m, "min_sample", 1) or 1))

            now = datetime.utcnow()
            _sync_metric_family(
                session,
                m,
                source_type=source_type,
                name=name,
                name_zh=name_zh or "",
                description=description,
                description_zh=description_zh or "",
                scope=scope,
                category=category,
                group_key=body.get("group_key", getattr(m, "group_key", None)),
                expression=body.get("expression", getattr(m, "expression", "") or ""),
                min_sample=min_sample,
                code_python=source_code,
                definition=source_definition,
                code_metadata=code_metadata,
                now=now,
            )
            session.commit()

        # Clear old results if re-backfill requested
        if body.get("rebackfill") and m.status == "published":
            family_keys = [row.key for row in _metric_family_rows(session, m)]
            session.query(MetricResultModel).filter(MetricResultModel.metric_key.in_(family_keys)).delete(synchronize_session=False)
            session.query(MetricComputeRun).filter(MetricComputeRun.metric_key.in_(family_keys)).delete(synchronize_session=False)
            session.commit()
            # RunLog deletion can be very slow for metrics migrated from trigger="game"
            # (millions of rows). Run it in a background thread to avoid blocking the UI.
            _bg_keys = list(family_keys)
            def _delete_run_logs_bg():
                from sqlalchemy.orm import sessionmaker as _sm
                _sess = _sm(bind=engine)()
                try:
                    _sess.query(MetricRunLog).filter(MetricRunLog.metric_key.in_(_bg_keys)).delete(synchronize_session=False)
                    _sess.commit()
                except Exception:
                    logger.exception("Background RunLog cleanup failed for %s", _bg_keys)
                    _sess.rollback()
                finally:
                    _sess.close()
            import threading
            threading.Thread(target=_delete_run_logs_bg, daemon=True).start()
            try:
                _dispatch_metric_backfill(m.key)
            except Exception:
                logger.exception("Failed to enqueue backfill for %s", m.key)
                return jsonify({"ok": True, "key": result_key, "warning": "Metric updated but backfill enqueue failed. Run manually."})

    return jsonify({"ok": True, "key": result_key})


def _resolve_entity_labels(session, rows):
    """Bulk-resolve entity IDs to human-readable labels.

    Returns (labels_dict, player_active_dict) where labels_dict is keyed by
    (entity_type, entity_id) and player_active_dict maps player_id → bool.
    """
    player_ids = {r.entity_id for r in rows if r.entity_type == "player" and r.entity_id}
    player_franchise_pairs = {
        tuple(r.entity_id.split(":", 1))
        for r in rows
        if r.entity_type == "player_franchise" and r.entity_id and ":" in r.entity_id
    }
    player_ids.update({player_id for player_id, _ in player_franchise_pairs})
    team_ids   = {r.entity_id for r in rows if r.entity_type == "team"   and r.entity_id}
    team_ids.update({franchise_id for _, franchise_id in player_franchise_pairs})
    game_ids   = {r.entity_id.split(":")[0] for r in rows if r.entity_type == "game" and r.entity_id}

    player_info = {
        p.player_id: (
            p.full_name_zh if _is_zh() and getattr(p, "full_name_zh", None) else p.full_name,
            bool(p.is_active),
        )
        for p in session.query(Player.player_id, Player.full_name, Player.full_name_zh, Player.is_active).filter(Player.player_id.in_(player_ids)).all()
    } if player_ids else {}
    player_names = {pid: info[0] for pid, info in player_info.items()}
    player_active = {pid: info[1] for pid, info in player_info.items()}
    team_map = _team_map(session)
    game_info = {
        g.game_id: (g.game_date, g.home_team_id, g.road_team_id)
        for g in session.query(Game.game_id, Game.game_date, Game.home_team_id, Game.road_team_id)
        .filter(Game.game_id.in_(game_ids)).all()
    } if game_ids else {}

    def _label(entity_type, entity_id):
        if entity_type == "season":
            return _season_label(entity_id)
        if entity_type == "player":
            return player_names.get(entity_id) or entity_id
        if entity_type == "player_franchise" and entity_id and ":" in entity_id:
            player_id, franchise_id = entity_id.split(":", 1)
            player_name = player_names.get(player_id) or player_id
            franchise_name = _team_name(team_map, franchise_id)
            return f"{player_name} — {franchise_name}"
        if entity_type == "team":
            t = team_map.get(entity_id)
            return (_display_team_name(t) or t.abbr) if t else entity_id
        if entity_type == "game":
            parts = entity_id.split(":")
            gid = parts[0]
            if gid in game_info:
                gdate, home_id, road_id = game_info[gid]
                matchup = f"{_team_abbr(team_map, road_id)} @ {_team_abbr(team_map, home_id)}"
                date_str = _fmt_date(gdate)
                if len(parts) > 1:
                    # Composite: game_id:team_id:qualifier
                    team_id = parts[1] if len(parts) > 1 else None
                    qualifier = parts[2] if len(parts) > 2 else ""
                    team_label = _team_abbr(team_map, team_id) if team_id else ""
                    return f"{team_label} {qualifier} — {matchup} ({date_str})"
                return f"{matchup} ({date_str})"
        return entity_id

    labels = {(r.entity_type, r.entity_id): _label(r.entity_type, r.entity_id) for r in rows}
    return labels, player_active, game_info


@app.route("/cn/metrics/<metric_key>", endpoint="metric_detail_zh")
@app.route("/metrics/<metric_key>")
def metric_detail(metric_key: str):
    import json
    from metrics.framework.base import CAREER_SEASON, is_career_season

    _CAREER_SEASON_LABELS = {"all_regular": "Regular Season", "all_playoffs": "Playoffs", "all_playin": "Play-In"}

    # Season filter — "all_X" for type-specific cross-season, "all" legacy → regular season
    selected_season = request.args.get("season", "")
    all_season_type = None  # e.g. "2" for regular, "4" for playoffs
    if selected_season.startswith("all_") and len(selected_season) == 5:
        show_all_seasons = True
        all_season_type = selected_season[4]
    elif selected_season == "all":
        show_all_seasons = True
        all_season_type = "2"  # legacy URL defaults to regular season
    else:
        show_all_seasons = False
    page = max(1, int(request.args.get("page", 1) or 1))
    search_q = request.args.get("q", "").strip()
    active_only = request.args.get("active") == "1"
    expand = request.args.get("expand") == "1"
    page_size = 50

    with SessionLocal() as session:
        from metrics.framework.runtime import get_metric as _get_metric

        base_metric_key = metric_key.removesuffix("_career")
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
            abort(404, description=f"Metric '{metric_key}' not found.")
        if db_metric and db_metric.status == "disabled" and not is_admin():
            _cur = _current_user()
            if not (_cur and db_metric.created_by_user_id == _cur.id):
                abort(404, description=f"Metric '{metric_key}' not found.")

        metric_def = _metric_def_view(
            runtime_metric or db_metric,
            source_type=getattr(db_metric, "source_type", None),
        )
        is_career_metric = bool(getattr(runtime_metric, "career", False))
        is_season_scope = metric_def.scope == "season"
        related_metrics = _related_metric_links(session, metric_key, runtime_metric, db_metric)
        current_metric_season = None

        # Available seasons for this metric
        season_rows = (
            session.query(MetricResultModel.season)
            .filter(MetricResultModel.metric_key == metric_key, MetricResultModel.season.isnot(None))
            .distinct()
            .all()
        )
        season_values = [r.season for r in season_rows]
        if is_career_metric:
            show_all_seasons = False
            career_season_options = sorted([s for s in season_values if is_career_season(s)])
            if not selected_season or selected_season not in career_season_options:
                selected_season = "all_regular" if "all_regular" in career_season_options else (career_season_options[0] if career_season_options else "all_regular")
            season_options = career_season_options
            season_groups = [{
                "type_code": "career",
                "type_name": "Career",
                "type_name_plural": "Career",
                "all_value": None,
                "seasons": [{"value": s, "label": _CAREER_SEASON_LABELS.get(s, s)} for s in career_season_options],
            }]
        elif is_season_scope:
            # Season-scope: cross-season leaderboard, dropdown only shows type groups (no individual seasons)
            show_all_seasons = True
            if not all_season_type:
                all_season_type = "2"  # default to regular season
            season_options = sorted(
                [s for s in season_values if not is_career_season(s) and s != CAREER_SEASON],
                key=_season_sort_key,
                reverse=True,
            )
            current_metric_season = _pick_current_season(season_options)
            from collections import defaultdict
            _type_buckets = defaultdict(list)
            for s in season_options:
                if len(s) == 5 and s.isdigit():
                    _type_buckets[s[0]].append(s)
            season_groups = []
            for type_code in ["2", "4", "5", "1", "3"]:
                if type_code in _type_buckets:
                    season_groups.append({
                        "type_code": type_code,
                        "type_name": _t(_SEASON_TYPE_NAMES.get(type_code, type_code), {
                            "Regular Season": "常规赛", "Playoffs": "季后赛", "PlayIn": "附加赛",
                            "Pre Season": "季前赛", "All Star": "全明星",
                        }.get(_SEASON_TYPE_NAMES.get(type_code, type_code), _SEASON_TYPE_NAMES.get(type_code, type_code))),
                        "type_name_plural": _t(_SEASON_TYPE_PLURAL.get(type_code, type_code), {
                            "Regular Seasons": "常规赛", "Playoffs": "季后赛", "PlayIn": "附加赛",
                            "Pre Seasons": "季前赛", "All Star": "全明星",
                        }.get(_SEASON_TYPE_PLURAL.get(type_code, type_code), _SEASON_TYPE_PLURAL.get(type_code, type_code))),
                        "all_value": f"all_{type_code}",
                        "seasons": [],
                    })
        else:
            season_options = sorted(
                [s for s in season_values if not is_career_season(s) and s != CAREER_SEASON],
                key=_season_sort_key,
                reverse=True,
            )
            current_metric_season = _pick_current_season(season_options)
            # Group seasons by type for the dropdown
            from collections import defaultdict
            _type_buckets = defaultdict(list)
            for s in season_options:
                if len(s) == 5 and s.isdigit():
                    _type_buckets[s[0]].append(s)
            season_groups = []
            for type_code in ["2", "4", "5", "1", "3"]:
                if type_code in _type_buckets:
                    season_groups.append({
                        "type_code": type_code,
                        "type_name": _t(_SEASON_TYPE_NAMES.get(type_code, type_code), {
                            "Regular Season": "常规赛",
                            "Playoffs": "季后赛",
                            "PlayIn": "附加赛",
                            "Pre Season": "季前赛",
                            "All Star": "全明星",
                        }.get(_SEASON_TYPE_NAMES.get(type_code, type_code), _SEASON_TYPE_NAMES.get(type_code, type_code))),
                        "type_name_plural": _t(_SEASON_TYPE_PLURAL.get(type_code, type_code), {
                            "Regular Seasons": "常规赛",
                            "Playoffs": "季后赛",
                            "PlayIn": "附加赛",
                            "Pre Seasons": "季前赛",
                            "All Star": "全明星",
                        }.get(_SEASON_TYPE_PLURAL.get(type_code, type_code), _SEASON_TYPE_PLURAL.get(type_code, type_code))),
                        "all_value": f"all_{type_code}",
                        "seasons": _type_buckets[type_code],
                    })
            if not show_all_seasons and not selected_season and season_options:
                selected_season = season_options[0]

        filtered_q = (
            session.query(MetricResultModel)
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.value_num.isnot(None),
            )
        )
        if show_all_seasons and all_season_type:
            filtered_q = filtered_q.filter(MetricResultModel.season.like(f"{all_season_type}%"))
        elif not show_all_seasons and selected_season:
            filtered_q = filtered_q.filter(MetricResultModel.season == selected_season)

        # Check if this metric uses sub_key (multiple rows per entity per season)
        has_sub_keys = (
            session.query(MetricResultModel.id)
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.sub_key != "",
            )
            .limit(1)
            .first()
        ) is not None

        # When sub_keys exist and not expanded, deduplicate: keep best row per entity per season
        if has_sub_keys and not expand:
            _is_asc_dedup = _metric_rank_order(session, metric_key) == "asc"
            _dedup_order = MetricResultModel.value_num.asc() if _is_asc_dedup else MetricResultModel.value_num.desc()
            dedup_rn = func.row_number().over(
                partition_by=[MetricResultModel.entity_type, MetricResultModel.entity_id, MetricResultModel.season],
                order_by=_dedup_order,
            ).label("_dedup_rn")
            dedup_sub = filtered_q.with_entities(MetricResultModel.id, dedup_rn).subquery()
            filtered_q = (
                session.query(MetricResultModel)
                .join(dedup_sub, MetricResultModel.id == dedup_sub.c.id)
                .filter(dedup_sub.c._dedup_rn == 1)
            )

        rank_partition = func.coalesce(MetricResultModel.rank_group, "__all__")
        _is_asc = _metric_rank_order(session, metric_key) == "asc"
        _detail_rank_val = -MetricResultModel.value_num if _is_asc else MetricResultModel.value_num
        rank_group_fields = [MetricResultModel.metric_key, rank_partition]
        if not show_all_seasons:
            rank_group_fields.insert(1, MetricResultModel.season)
        ranked_q = (
            filtered_q.with_entities(
                MetricResultModel.id.label("id"),
                MetricResultModel.entity_type.label("entity_type"),
                MetricResultModel.entity_id.label("entity_id"),
                MetricResultModel.season.label("season"),
                MetricResultModel.sub_key.label("sub_key"),
                MetricResultModel.rank_group.label("rank_group"),
                MetricResultModel.value_num.label("value_num"),
                MetricResultModel.value_str.label("value_str"),
                MetricResultModel.context_json.label("context_json"),
                MetricResultModel.computed_at.label("computed_at"),
                func.rank().over(
                    partition_by=rank_group_fields,
                    order_by=_detail_rank_val.desc(),
                ).label("rank"),
                func.count(MetricResultModel.id).over(
                    partition_by=rank_group_fields,
                ).label("standing_total"),
            )
            .subquery()
        )

        _detail_sort_col = ranked_q.c.value_num.asc() if _is_asc else ranked_q.c.value_num.desc()
        base_rows_q = (
            session.query(ranked_q)
            .order_by(_detail_sort_col, ranked_q.c.entity_id.asc())
        )

        if active_only and metric_def.scope in ("player", "player_franchise"):
            active_player_ids = [
                r[0] for r in session.query(Player.player_id)
                .filter(Player.is_active == True).all()
            ]
            if metric_def.scope == "player":
                base_rows_q = base_rows_q.filter(ranked_q.c.entity_id.in_(active_player_ids))
            else:
                # player_franchise: entity_id is "player_id:franchise_id"
                active_like_filters = [ranked_q.c.entity_id.like(f"{pid}:%") for pid in active_player_ids]
                base_rows_q = base_rows_q.filter(or_(*active_like_filters)) if active_like_filters else base_rows_q.filter(False)

        if search_q:
            matching_player_ids = [
                r[0] for r in session.query(Player.player_id)
                .filter(
                    or_(
                        Player.full_name.ilike(f"%{search_q}%"),
                        Player.full_name_zh.ilike(f"%{search_q}%"),
                    )
                ).all()
            ]
            matching_team_ids = [
                r[0] for r in session.query(Team.team_id)
                .filter(
                    or_(
                        Team.full_name.ilike(f"%{search_q}%"),
                        Team.full_name_zh.ilike(f"%{search_q}%"),
                    )
                ).all()
            ]
            name_filters = []
            if matching_player_ids:
                name_filters.append(and_(ranked_q.c.entity_type == "player", ranked_q.c.entity_id.in_(matching_player_ids)))
            if matching_team_ids:
                name_filters.append(and_(ranked_q.c.entity_type == "team", ranked_q.c.entity_id.in_(matching_team_ids)))
            if name_filters:
                base_rows_q = base_rows_q.filter(or_(*name_filters))
            else:
                base_rows_q = base_rows_q.filter(False)
            rows = base_rows_q.limit(200).all()
            total = len(rows)
            total_pages = 1
            page = 1
        else:
            import math
            total = base_rows_q.count() or 0
            total_pages = max(1, math.ceil(total / page_size))
            page = min(page, total_pages)
            offset = (page - 1) * page_size
            rows = base_rows_q.offset(offset).limit(page_size).all()

        labels, player_active, game_info = _resolve_entity_labels(session, rows)
        team_map = _team_map(session)

        _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
        scope_label = {"player": "players", "player_franchise": "franchise stints", "team": "teams", "game": "results", "season": "seasons"}.get(
            metric_def.scope, "entities"
        )
        if is_career_metric:
            period = "across all seasons"
        elif show_all_seasons:
            _type_name = _t(
                _SEASON_TYPE_NAMES.get(all_season_type, "").lower(),
                {
                    "2": "常规赛",
                    "4": "季后赛",
                    "5": "附加赛",
                    "1": "季前赛",
                    "3": "全明星",
                }.get(all_season_type, _SEASON_TYPE_NAMES.get(all_season_type, "").lower()),
            )
            period = f"跨全部{_type_name}" if _is_zh() and _type_name else ("across all seasons" if not _type_name else f"across all {_type_name} seasons")
        else:
            period = "this season"
        base_key = metric_key.removesuffix("_career")
        detail_db_templates = _load_context_label_templates(session, {base_key})
        result_rows = []
        for r in rows:
            ctx = json.loads(r.context_json) if r.context_json else {}
            games_counted = (
                ctx.get("games")
                or ctx.get("total_games")
                or ctx.get("games_played")
                or ctx.get("games_leading_at_half")
                or ctx.get("games_trailing_at_half")
                or ctx.get("road_games")
                or ctx.get("home_games")
            )
            rank_group_label = _team_name(team_map, r.rank_group) if r.rank_group else None
            base_key = metric_key.removesuffix("_career")
            context_label = _resolve_context_label(base_key, ctx, detail_db_templates)
            rank = int(r.rank or 0)
            standing_total = int(r.standing_total or 0)
            is_notable = standing_total > 0 and rank / standing_total <= 0.25
            label = _RANK_LABELS.get(rank, f"#{rank}")
            group_phrase = f" in {rank_group_label}" if rank_group_label else ""
            notable_reason = f"{label} of {standing_total} {scope_label}{group_phrase} {period}."
            player_id_for_active = r.entity_id.split(":")[0] if r.entity_type in ("player", "player_franchise") else None
            game_home_team_id = None
            game_road_team_id = None
            game_road_abbr = None
            game_home_abbr = None
            game_date_str = None
            if r.entity_type == "game" and r.entity_id:
                gid = r.entity_id.split(":")[0]
                gi = game_info.get(gid)
                if gi:
                    game_home_team_id = str(gi[1]) if gi[1] else None
                    game_road_team_id = str(gi[2]) if gi[2] else None
                    game_road_abbr = _team_abbr(team_map, gi[2])
                    game_home_abbr = _team_abbr(team_map, gi[1])
                    game_date_str = _fmt_date(gi[0])
            result_rows.append({
                "rank": rank,
                "total": standing_total,
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "entity_label": labels.get((r.entity_type, r.entity_id), r.entity_id),
                "is_active": player_active.get(player_id_for_active) if player_id_for_active else None,
                "home_team_id": game_home_team_id,
                "road_team_id": game_road_team_id,
                "road_abbr": game_road_abbr,
                "home_abbr": game_home_abbr,
                "game_date_str": game_date_str,
                "season": _season_label(r.season),
                "season_raw": r.season,
                "sub_key": r.sub_key or "",
                "value_num": r.value_num,
                "value_str": r.value_str,
                "is_notable": is_notable,
                "notable_reason": notable_reason if is_notable else None,
                "context": ctx,
                "context_label": context_label,
                "rank_group": r.rank_group,
                "rank_group_label": rank_group_label,
                "games_counted": int(games_counted) if games_counted is not None else None,
            })
        show_rank_group = any(r["rank_group_label"] for r in result_rows)

        _, backfill = _build_metric_backfill_status(session, metric_key)
        # Career metrics with a reducer read qualifications from the base
        # metric's season logs, so check the base key for drill-down support.
        _dd_key = metric_key
        if is_career_metric and metric_key.endswith("_career"):
            from metrics.framework.family import family_base_key as _fbk
            from metrics.framework.runtime import _metric_declares_career_reducer as _mcr
            if runtime_metric and _mcr(runtime_metric):
                _dd_key = _fbk(metric_key)
        has_drilldown = (
            session.query(MetricRunLog.game_id)
            .filter(MetricRunLog.metric_key == _dd_key, MetricRunLog.qualified == True)
            .limit(1)
            .first()
        ) is not None
        metric_deep_dive = _metric_deep_dive_state(session, metric_key)
        feature_access = get_feature_access_config(session)

        # Admin-only: last 5 execution latencies for this metric
        metric_perf_samples = []
        if is_admin():
            _perf_key = metric_key.removesuffix("_career")
            perf_rows = (
                session.query(MetricPerfLog.duration_ms, MetricPerfLog.recorded_at)
                .filter(MetricPerfLog.metric_key == _perf_key)
                .order_by(MetricPerfLog.recorded_at.desc())
                .limit(5)
                .all()
            )
            metric_perf_samples = [{"ms": r.duration_ms, "at": r.recorded_at} for r in perf_rows]

    if is_career_metric:
        display_season_label = "Career"
    elif show_all_seasons:
        _type_name = _t(
            _SEASON_TYPE_PLURAL.get(all_season_type, "Seasons"),
            {
                "2": "常规赛",
                "4": "季后赛",
                "5": "附加赛",
                "1": "季前赛",
                "3": "全明星",
            }.get(all_season_type, "赛季"),
        )
        display_season_label = f"全部{_type_name}" if _is_zh() else f"All {_type_name}"
    else:
        display_season_label = _season_label(selected_season)
    current_metric_season_label = _season_label(current_metric_season) if current_metric_season else None
    is_player_scope = metric_def.scope in ("player", "player_franchise")
    return render_template(
        "metric_detail.html",
        metric_def=metric_def,
            result_rows=result_rows,
            show_rank_group=show_rank_group,
            is_player_scope=is_player_scope,
            is_season_scope=is_season_scope,
            active_only=active_only,
        season_options=season_options,
        season_groups=season_groups,
        selected_season=selected_season,
        show_all_seasons=show_all_seasons,
        all_season_type=all_season_type,
        is_career_metric=is_career_metric,
        related_metrics=related_metrics,
        season_label=display_season_label,
        current_metric_season=current_metric_season,
        current_metric_season_label=current_metric_season_label,
        fmt_season=_season_label,
        fmt_season_short=_season_year_label,
        page=page,
        total_pages=total_pages,
        total=total,
        page_size=page_size,
        backfill=backfill,
        has_drilldown=has_drilldown,
        search_q=search_q,
        metric_deep_dive=metric_deep_dive,
        has_sub_keys=has_sub_keys,
        expand=expand,
        metric_perf_samples=metric_perf_samples,
        **_build_metric_feature_context(feature_access),
    )


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




@app.get("/admin")
def admin_pipeline():
    denied = _require_admin_page()
    if denied:
        return denied
    return render_template(
        "admin.html",
        admin_page_url=_admin_page_url,
        admin_fragment_url=_admin_fragment_url,
    )


@app.get("/admin/settings")
def admin_settings():
    denied = _require_admin_page()
    if denied:
        return denied
    return render_template(
        "admin_settings.html",
        llm_available_models=available_llm_models(),
    )


# ---------------------------------------------------------------------------
# Content pipeline: SocialPost kanban + API
# ---------------------------------------------------------------------------

@app.get("/admin/content")
def admin_content():
    """Kanban board for content pipeline."""
    denied = _require_admin_page()
    if denied:
        return denied
    status_filter = request.args.get("status")
    page = max(1, request.args.get("page", 1, type=int))
    page_size = 30

    with SessionLocal() as s:
        q = s.query(SocialPost).order_by(SocialPost.source_date.desc(), SocialPost.priority.asc())
        if status_filter and status_filter in ("draft", "ai_review", "in_review", "approved", "archived"):
            q = q.filter(SocialPost.status == status_filter)
        total = q.count()
        import math
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        posts = q.offset((page - 1) * page_size).limit(page_size).all()
        post_rows = _build_social_post_rows(s, posts)

    from datetime import date as _date, timedelta as _td
    yesterday = (_date.today() - _td(days=1)).isoformat()
    return render_template(
        "admin_content.html",
        posts=post_rows,
        page=page,
        total_pages=total_pages,
        total=total,
        status_filter=status_filter or "all",
        today=yesterday,
        single_post_view=False,
    )


@app.get("/admin/content/<int:post_id>")
def admin_content_post(post_id: int):
    """Single-post management view for a SocialPost."""
    denied = _require_admin_page()
    if denied:
        return denied
    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not post:
            abort(404)
        post_rows = _build_social_post_rows(s, [post])

    from datetime import date as _date, timedelta as _td
    yesterday = (_date.today() - _td(days=1)).isoformat()
    return render_template(
        "admin_content.html",
        posts=post_rows,
        page=1,
        total_pages=1,
        total=1,
        status_filter="all",
        today=yesterday,
        single_post_view=True,
        focused_post_id=post_id,
    )


@app.get("/api/admin/content/<int:post_id>/card")
def admin_content_card(post_id: int):
    """Render one post card as HTML for partial page refreshes."""
    denied = _require_admin_json()
    if denied:
        return denied
    expanded = request.args.get("expanded") in {"1", "true", "True"}
    active_variant_id = request.args.get("active_variant_id", type=int)
    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not post:
            return jsonify({"error": "not_found"}), 404
        row = _build_social_post_rows(s, [post])[0]
    html = render_template(
        "_admin_content_post_card.html",
        p=row,
        expanded=expanded,
        active_variant_id=active_variant_id,
    )
    return jsonify({"ok": True, "html": html, "post_status": row["status"]})


@app.get("/api/admin/content/<int:post_id>")
def admin_content_detail(post_id: int):
    """Get full post detail with variants and deliveries."""
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as s:
        p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not p:
            return jsonify({"error": "not_found"}), 404
        variants = s.query(SocialPostVariant).filter(
            SocialPostVariant.post_id == post_id
        ).order_by(SocialPostVariant.id).all()
        variant_ids = [v.id for v in variants]
        deliveries = s.query(SocialPostDelivery).filter(
            SocialPostDelivery.variant_id.in_(variant_ids)
        ).all() if variant_ids else []
        d_by_variant: dict[int, list] = {}
        for d in deliveries:
            d_by_variant.setdefault(d.variant_id, []).append(d)
        images = s.query(SocialPostImage).filter(
            SocialPostImage.post_id == post_id
        ).order_by(SocialPostImage.id).all()

        return jsonify({
            "id": p.id,
            "topic": p.topic,
            "source_date": p.source_date.isoformat() if p.source_date else None,
            "source_metrics": json.loads(p.source_metrics) if p.source_metrics else [],
            "source_game_ids": json.loads(p.source_game_ids) if p.source_game_ids else [],
            "status": p.status,
            "priority": p.priority,
            "admin_comments": _social_post_comments(p),
            "llm_model": p.llm_model,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "workflow": _paperclip_workflow_view(p),
            "variants": [
                {
                    "id": v.id,
                    "title": v.title,
                    "content_raw": v.content_raw,
                    "audience_hint": v.audience_hint,
                    "deliveries": [_social_post_delivery_view(d) for d in d_by_variant.get(v.id, [])],
                }
                for v in variants
            ],
            "images": [_social_post_image_view(post_id, img) for img in images],
        })


@app.post("/api/admin/content/<int:post_id>/update")
def admin_content_update(post_id: int):
    """Update a social post's topic, status, or priority."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    handoff_action = None
    handoff_comment_text = None
    handoff_comment_timestamp = None
    topic_changed = False
    priority_changed = False
    with SessionLocal() as s:
        p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not p:
            return jsonify({"error": "not_found"}), 404
        previous_status = p.status
        comments = _social_post_comments(p)
        if "topic" in data:
            new_topic = (data["topic"] or "").strip()
            if new_topic and new_topic != p.topic:
                p.topic = new_topic
                topic_changed = True
        if "status" in data and data["status"] in ("draft", "ai_review", "in_review", "approved", "archived"):
            p.status = data["status"]
        if "priority" in data:
            new_priority = int(data["priority"])
            if new_priority != p.priority:
                p.priority = new_priority
                priority_changed = True
        if previous_status == "ai_review" and p.status == "in_review":
            validation_errors = _post_ai_review_validation_errors(s, post_id)
            if validation_errors:
                return jsonify({"error": "ai_review_validation_failed", "details": validation_errors}), 400
        if p.status != previous_status:
            if p.status == "ai_review":
                handoff_action = "send_to_ai_review"
                handoff_comment_text = "Sent this post to AI review from Funba."
            elif p.status == "in_review":
                handoff_action = "send_to_review"
                handoff_comment_text = "Sent this post to review from Funba."
            elif p.status == "draft":
                handoff_action = "request_revision"
                handoff_comment_text = "Requested revision on this post from Funba."
            elif p.status == "approved":
                handoff_action = "approve_and_queue_publish"
                handoff_comment_text = "Approved this post and queued publishing from Funba."
            elif p.status == "archived":
                handoff_action = "archive_post"
                handoff_comment_text = "Archived this post from Funba."
            if handoff_comment_text:
                handoff_comment_timestamp = append_admin_comment(
                    comments,
                    text=handoff_comment_text,
                    author=_paperclip_actor_name(),
                    origin="system",
                    event_type="handoff",
                )
                _write_social_post_comments(p, comments)
            else:
                p.updated_at = datetime.utcnow()
        else:
            p.updated_at = datetime.utcnow()
        s.commit()
    if handoff_action and handoff_comment_timestamp and handoff_comment_text:
        _handoff_social_post(
            post_id,
            action=handoff_action,
            local_comment_timestamp=handoff_comment_timestamp,
            local_comment_text=handoff_comment_text,
        )
    elif topic_changed or priority_changed:
        _ensure_paperclip_issue_for_post(post_id)
    sync_result = _sync_social_post_from_paperclip(post_id, ensure_issue=False)
    return jsonify({"ok": True, **(sync_result or {})})


@app.post("/api/admin/content/<int:post_id>/comment")
def admin_content_comment(post_id: int):
    """Add an admin comment to a post."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    text_val = (data.get("text") or "").strip()
    if not text_val:
        return jsonify({"error": "text required"}), 400
    comment_timestamp = None
    with SessionLocal() as s:
        p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not p:
            return jsonify({"error": "not_found"}), 404
        comments = _social_post_comments(p)
        user = _current_user()
        comment_timestamp = append_admin_comment(
            comments,
            text=text_val,
            author=user.display_name if user and getattr(user, "display_name", None) else "admin",
            origin="funba_user",
            event_type="comment",
        )
        _write_social_post_comments(p, comments)
        s.commit()
    if comment_timestamp:
        _mirror_paperclip_comment(post_id, text=text_val, local_comment_timestamp=comment_timestamp)
    sync_result = _sync_social_post_from_paperclip(post_id, ensure_issue=False)
    comments = None
    workflow = None
    if sync_result:
        comments = sync_result.get("comments")
        workflow = sync_result.get("workflow")
    if comments is None or workflow is None:
        with SessionLocal() as s:
            p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
            comments = _social_post_comments(p) if p else []
            workflow = _paperclip_workflow_view(p) if p else {}
    return jsonify({"ok": True, "comments": comments, "workflow": workflow})


@app.post("/api/admin/content/<int:post_id>/delete")
def admin_content_delete(post_id: int):
    """Delete a social post and all its variants/deliveries (cascade)."""
    denied = _require_admin_json()
    if denied:
        return denied
    issue_id = None
    with SessionLocal() as s:
        p = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not p:
            return jsonify({"error": "not_found"}), 404
        issue_id = p.paperclip_issue_id
        # Delete deliveries → variants → post (manual cascade for safety)
        variant_ids = [v.id for v in s.query(SocialPostVariant.id).filter(SocialPostVariant.post_id == post_id).all()]
        if variant_ids:
            s.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id.in_(variant_ids)).delete(synchronize_session=False)
        s.query(SocialPostVariant).filter(SocialPostVariant.post_id == post_id).delete(synchronize_session=False)
        s.query(SocialPost).filter(SocialPost.id == post_id).delete(synchronize_session=False)
        s.commit()
    if issue_id and _paperclip_bridge_enabled():
        try:
            client, _cfg = _paperclip_client_or_raise()
            client.update_issue(
                issue_id,
                {
                    "status": "cancelled",
                    "comment": "## Funba Workflow Update\n\nAction: delete_post\nTriggered from: Funba admin content\n\nThe linked SocialPost was deleted in Funba, so this workflow thread is now cancelled.",
                },
            )
        except PaperclipBridgeError as exc:
            logger.warning("Failed to cancel Paperclip issue %s for deleted post %s: %s", issue_id, post_id, exc)
    return jsonify({"ok": True})


@app.post("/api/admin/content/<int:post_id>/paperclip/sync")
def admin_content_sync_paperclip(post_id: int):
    """Sync the linked Paperclip issue and comments back into Funba."""
    denied = _require_admin_json()
    if denied:
        return denied
    result = _sync_social_post_from_paperclip(post_id, ensure_issue=True)
    if result is None:
        return jsonify({"error": "not_found"}), 404
    return jsonify({"ok": True, **result})


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


@app.post("/api/admin/metrics/<metric_key>/deep-dive-post")
def admin_metric_trigger_deep_dive_post(metric_key: str):
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    metric_page_url = (data.get("metric_page_url") or "").strip()
    if metric_page_url.startswith("/"):
        metric_page_url = request.url_root.rstrip("/") + metric_page_url
    if not metric_page_url:
        metric_page_url = url_for("metric_detail", metric_key=metric_key, _external=True)

    try:
        _paperclip_client_or_raise()
    except PaperclipBridgeError as exc:
        return jsonify({"error": str(exc)}), 400

    with SessionLocal() as s:
        from metrics.framework.runtime import get_metric as _get_metric

        runtime_metric = _get_metric(metric_key, session=s)
        db_metric = (
            s.query(MetricDefinitionModel)
            .filter(MetricDefinitionModel.key == metric_key, MetricDefinitionModel.status != "archived")
            .first()
        )
        if runtime_metric is None and db_metric is None:
            return jsonify({"error": "metric_not_found"}), 404

        metric_def = _metric_def_view(
            runtime_metric or db_metric,
            source_type=getattr(db_metric, "source_type", None),
        )
        deep_dive_state = _metric_deep_dive_state(s, metric_key)
        if not deep_dive_state["can_trigger"]:
            return jsonify({"error": "already_running", "metric_deep_dive": deep_dive_state}), 409

        brief_text = _build_metric_deep_dive_brief(
            session=s,
            metric_name=metric_def.name_en,
            metric_name_zh=metric_def.name_zh,
            metric_key=metric_key,
            metric_description=metric_def.description_en,
            metric_scope=metric_def.scope,
            metric_page_url=metric_page_url,
        )

    post_id, brief_timestamp = _create_metric_deep_dive_placeholder_post(metric_key, metric_def.name, brief_text)
    _ensure_paperclip_issue_for_post(post_id)

    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if post is None or not post.paperclip_issue_id:
            error_message = (post.paperclip_sync_error if post is not None else None) or "Failed to create Paperclip issue for this deep-dive post."
            variant_ids = [v.id for v in s.query(SocialPostVariant.id).filter(SocialPostVariant.post_id == post_id).all()]
            if variant_ids:
                s.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id.in_(variant_ids)).delete(synchronize_session=False)
            s.query(SocialPostVariant).filter(SocialPostVariant.post_id == post_id).delete(synchronize_session=False)
            s.query(SocialPost).filter(SocialPost.id == post_id).delete(synchronize_session=False)
            s.commit()
            return jsonify({"error": error_message}), 500

    _mirror_paperclip_comment(post_id, text=brief_text, local_comment_timestamp=brief_timestamp)
    sync_result = _sync_social_post_from_paperclip(post_id, ensure_issue=False)

    with SessionLocal() as s:
        deep_dive_state = _metric_deep_dive_state(s, metric_key)

    response = {
        "ok": True,
        "post_id": post_id,
        "metric_deep_dive": deep_dive_state,
    }
    if sync_result:
        response.update(sync_result)
    return jsonify(response)


@app.post("/api/admin/content/daily-analysis/trigger")
def admin_content_trigger_daily_analysis():
    """Create or refresh per-game content analysis issues for a source date."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    source_date = (data.get("source_date") or "").strip()
    force = bool(data.get("force"))
    try:
        target_date = date.fromisoformat(source_date) if source_date else (date.today() - timedelta(days=1))
    except ValueError:
        return jsonify({"error": "invalid source_date"}), 400
    try:
        result = ensure_game_content_analysis_issues(target_date, force=force)
    except PaperclipBridgeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("Failed to trigger game content analysis for %s", target_date.isoformat())
        return jsonify({"error": str(exc)}), 500
    if result.get("issue_identifier"):
        result["issue_url"] = _paperclip_issue_url(result.get("issue_identifier"))
    return jsonify(result)


@app.post("/api/admin/games/<game_id>/content-analysis/trigger")
def admin_game_trigger_content_analysis(game_id: str):
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    force = bool(data.get("force"))
    try:
        result = ensure_game_content_analysis_issue_for_game(game_id, force=force, trigger_source="manual")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404
    except PaperclipBridgeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("Failed to trigger game content analysis for game_id=%s", game_id)
        return jsonify({"error": str(exc)}), 500
    if result.get("status") == "waiting_for_pipeline":
        result["readiness_detail"] = game_analysis_readiness_detail(game_id)
    result["issues"] = [
        {
            "id": item.id,
            "issue_id": item.issue_id,
            "issue_identifier": item.issue_identifier,
            "issue_url": _paperclip_issue_url(item.issue_identifier),
            "issue_status": item.issue_status,
            "title": item.title,
            "trigger_source": item.trigger_source,
            "source_date": item.source_date,
            "created_at": item.created_at,
            "updated_at": item.updated_at,
            "created_at_label": item.created_at.replace("T", " ")[:19] if item.created_at else None,
            "updated_at_label": item.updated_at.replace("T", " ")[:19] if item.updated_at else None,
            "posts": [
                {
                    "post_id": int(post["post_id"]),
                    "topic": str(post.get("topic") or ""),
                    "status": str(post.get("status") or ""),
                    "source_date": str(post.get("source_date") or ""),
                    "discovered_via": str(post.get("discovered_via") or ""),
                }
                for post in item.posts
            ],
        }
        for item in game_analysis_issue_history(game_id)
    ]
    return jsonify(result)


@app.post("/api/admin/content/<int:post_id>/variants/<int:variant_id>/update")
def admin_content_variant_update(post_id: int, variant_id: int):
    """Update a variant's title, content, or audience hint."""
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime
    data = request.get_json(force=True) or {}
    with SessionLocal() as s:
        v = s.query(SocialPostVariant).filter(
            SocialPostVariant.id == variant_id,
            SocialPostVariant.post_id == post_id,
        ).first()
        if not v:
            return jsonify({"error": "not_found"}), 404
        if "title" in data:
            v.title = data["title"]
        if "content_raw" in data:
            v.content_raw = data["content_raw"]
        if "audience_hint" in data:
            v.audience_hint = data["audience_hint"]
        v.updated_at = datetime.utcnow()
        s.commit()
    _ensure_paperclip_issue_for_post(post_id)
    return jsonify({"ok": True})


@app.post("/api/admin/content/<int:post_id>/variants/<int:variant_id>/destinations")
def admin_content_add_destination(post_id: int, variant_id: int):
    """Add a delivery destination to a variant."""
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime
    data = request.get_json(force=True) or {}
    platform = (data.get("platform") or "").strip()
    forum = (data.get("forum") or "").strip() or None
    if platform.lower() == "hupu":
        forum = normalize_hupu_forum(forum)
    if platform.lower() == "reddit":
        forum = _normalize_reddit_forum(forum)
    if not platform:
        return jsonify({"error": "platform required"}), 400
    now = datetime.utcnow()
    with SessionLocal() as s:
        v = s.query(SocialPostVariant).filter(
            SocialPostVariant.id == variant_id,
            SocialPostVariant.post_id == post_id,
        ).first()
        if not v:
            return jsonify({"error": "variant not_found"}), 404
        d = SocialPostDelivery(
            variant_id=variant_id,
            platform=platform,
            forum=forum,
            is_enabled=True,
            status="pending",
            created_at=now,
            updated_at=now,
        )
        s.add(d)
        if platform.lower() == "reddit":
            v.audience_hint = _reddit_english_audience_hint(v.audience_hint, forum=forum)
            v.updated_at = now
        s.commit()
        delivery_id = d.id
    _ensure_paperclip_issue_for_post(post_id)
    return jsonify({"ok": True, "delivery_id": delivery_id})


@app.post("/api/admin/content/<int:post_id>/deliveries/<int:delivery_id>/toggle")
def admin_content_toggle_delivery(post_id: int, delivery_id: int):
    """Enable or disable a single delivery destination."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    if "is_enabled" not in data:
        return jsonify({"error": "is_enabled required"}), 400
    enabled = bool(data.get("is_enabled"))
    handoff_action = None
    handoff_comment_text = None
    handoff_comment_timestamp = None
    retry_issue_id = None
    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        d = (
            s.query(SocialPostDelivery)
            .join(SocialPostVariant, SocialPostVariant.id == SocialPostDelivery.variant_id)
            .filter(
                SocialPostDelivery.id == delivery_id,
                SocialPostVariant.post_id == post_id,
            )
            .first()
        )
        if not d:
            return jsonify({"error": "not_found"}), 404
        d.is_enabled = enabled
        d.updated_at = datetime.utcnow()
        if not enabled and d.status == "publishing":
            d.status = "failed"
            d.error_message = "Delivery disabled while publishing"
        elif enabled and d.status == "failed":
            d.status = "pending"
            d.error_message = None
            d.published_url = None
            d.published_at = None
        if enabled and post and post.status == "approved":
            comments = _social_post_comments(post)
            handoff_action = "retry_enabled_delivery"
            handoff_comment_text = f"Re-enabled delivery {delivery_id} for retry from Funba."
            handoff_comment_timestamp = append_admin_comment(
                comments,
                text=handoff_comment_text,
                author=_paperclip_actor_name(),
                origin="system",
                event_type="handoff",
            )
            _write_social_post_comments(post, comments)
            retry_issue_id = post.paperclip_issue_id
        s.commit()
    if handoff_action and handoff_comment_timestamp and handoff_comment_text:
        _handoff_social_post(
            post_id,
            action=handoff_action,
            local_comment_timestamp=handoff_comment_timestamp,
            local_comment_text=handoff_comment_text,
        )
        try:
            client, cfg = _paperclip_client_or_raise()
            if cfg.delivery_publisher_agent_id and retry_issue_id:
                client.wake_agent(
                    cfg.delivery_publisher_agent_id,
                    reason="retry_enabled_delivery",
                    payload={"issueId": retry_issue_id},
                    force_fresh_session=True,
                )
        except Exception as exc:
            logger.warning("Failed to explicitly wake Delivery Publisher for post %s retry: %s", post_id, exc)
    else:
        _ensure_paperclip_issue_for_post(post_id)
    return jsonify({"ok": True, "delivery_id": delivery_id, "is_enabled": enabled})


@app.post("/api/admin/content/<int:post_id>/images/<int:image_id>/toggle")
def admin_content_toggle_image(post_id: int, image_id: int):
    """Enable or disable an image in the post's image pool."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    if "is_enabled" not in data:
        return jsonify({"error": "is_enabled required"}), 400
    enabled = bool(data["is_enabled"])
    review_reason = (data.get("reason") or "").strip() or None
    review_source = _normalize_image_review_source(data.get("review_source"))
    now = datetime.utcnow()
    with SessionLocal() as s:
        img = (
            s.query(SocialPostImage)
            .filter(SocialPostImage.id == image_id, SocialPostImage.post_id == post_id)
            .first()
        )
        if not img:
            return jsonify({"error": "not_found"}), 404
        img.is_enabled = enabled
        if review_reason or review_source:
            _apply_image_review_metadata(
                img,
                decision="enable" if enabled else "disable",
                reason=review_reason,
                source=review_source or "manual_toggle",
                reviewed_at=now,
            )
        s.commit()
    return jsonify({"ok": True, "image_id": image_id, "is_enabled": enabled})


@app.post("/api/admin/content/<int:post_id>/images")
def admin_content_add_image(post_id: int):
    """Add one prepared image asset to an existing post image pool."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    try:
        prepared = _validate_prepared_image_specs([data])[0]
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 400

    now = datetime.utcnow()
    stored_path = None
    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not post:
            return jsonify({"error": "not_found"}), 404

        existing = (
            s.query(SocialPostImage)
            .filter(SocialPostImage.post_id == post_id, SocialPostImage.slot == prepared["slot"])
            .first()
        )
        if existing:
            return jsonify({"error": "slot_exists", "slot": prepared["slot"]}), 400

        try:
            stored_path = store_prepared_image(prepared["source_path"], post_id=post_id, slot=prepared["slot"])
            img = SocialPostImage(
                post_id=post_id,
                slot=prepared["slot"],
                image_type=prepared["image_type"],
                spec=prepared["spec_json"],
                note=prepared["note"],
                file_path=stored_path,
                is_enabled=bool(prepared["is_enabled"]),
                error_message=None,
                created_at=now,
            )
            s.add(img)
            s.commit()
            image_id = img.id
        except Exception as exc:
            s.rollback()
            _remove_managed_post_image_file(stored_path, post_id=post_id)
            return jsonify({"error": str(exc)}), 400

    _ensure_paperclip_issue_for_post(post_id)
    return jsonify({"ok": True, "image_id": image_id})


@app.post("/api/admin/content/<int:post_id>/images/<int:image_id>/replace")
def admin_content_replace_image(post_id: int, image_id: int):
    """Replace one existing image asset with a newly prepared local file."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    try:
        prepared = _validate_prepared_image_specs([data])[0]
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 400

    stored_path = None
    old_path = None
    with SessionLocal() as s:
        img = (
            s.query(SocialPostImage)
            .filter(SocialPostImage.id == image_id, SocialPostImage.post_id == post_id)
            .first()
        )
        if not img:
            return jsonify({"error": "not_found"}), 404

        if prepared["slot"] != img.slot:
            existing = (
                s.query(SocialPostImage)
                .filter(
                    SocialPostImage.post_id == post_id,
                    SocialPostImage.slot == prepared["slot"],
                    SocialPostImage.id != image_id,
                )
                .first()
            )
            if existing:
                return jsonify({"error": "slot_exists", "slot": prepared["slot"]}), 400

        old_path = img.file_path
        try:
            stored_path = store_prepared_image(prepared["source_path"], post_id=post_id, slot=prepared["slot"])
            img.slot = prepared["slot"]
            img.image_type = prepared["image_type"]
            img.spec = prepared["spec_json"]
            img.note = prepared["note"]
            img.file_path = stored_path
            img.is_enabled = bool(prepared["is_enabled"])
            img.error_message = None
            img.review_decision = None
            img.review_reason = None
            img.review_source = None
            img.reviewed_at = None
            s.commit()
        except Exception as exc:
            s.rollback()
            _remove_managed_post_image_file(stored_path, post_id=post_id)
            return jsonify({"error": str(exc)}), 400

    if old_path and old_path != stored_path:
        _remove_managed_post_image_file(old_path, post_id=post_id)

    _ensure_paperclip_issue_for_post(post_id)
    return jsonify({"ok": True, "image_id": image_id})


@app.get("/api/admin/content/<int:post_id>/image-review-payload")
def admin_content_image_review_payload(post_id: int):
    """Get one content-review payload with variants plus currently enabled images."""
    denied = _require_admin_json()
    if denied:
        return denied
    include_disabled = request.args.get("include_disabled") in {"1", "true", "True"}
    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not post:
            return jsonify({"error": "not_found"}), 404
        variants = (
            s.query(SocialPostVariant)
            .filter(SocialPostVariant.post_id == post_id)
            .order_by(SocialPostVariant.id)
            .all()
        )
        image_query = (
            s.query(SocialPostImage)
            .filter(SocialPostImage.post_id == post_id)
            .order_by(SocialPostImage.id)
        )
        if not include_disabled:
            image_query = image_query.filter(SocialPostImage.is_enabled == True)
        images = image_query.all()

        return jsonify({
            "ok": True,
            "post_id": post.id,
            "topic": post.topic,
            "status": post.status,
            "source_date": post.source_date.isoformat() if post.source_date else None,
            "variants": [
                {
                    "id": v.id,
                    "title": v.title,
                    "audience_hint": v.audience_hint,
                    "content_raw": v.content_raw,
                    "referenced_slots": _extract_image_slots_from_content(v.content_raw),
                }
                for v in variants
            ],
            "images": [_social_post_image_view(post_id, img) for img in images],
        })


@app.post("/api/admin/content/<int:post_id>/image-review/apply")
def admin_content_apply_image_review(post_id: int):
    """Apply structured image-review decisions from an external reviewer/agent."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    decisions = data.get("image_decisions") or []
    if not isinstance(decisions, list) or not decisions:
        return jsonify({"error": "image_decisions required"}), 400

    review_source = _normalize_image_review_source(data.get("review_source")) or "content_reviewer_agent"
    review_summary = (data.get("summary") or "").strip() or None
    now = datetime.utcnow()
    updated_images: list[dict[str, object]] = []

    with SessionLocal() as s:
        post = s.query(SocialPost).filter(SocialPost.id == post_id).first()
        if not post:
            return jsonify({"error": "not_found"}), 404

        comments = _social_post_comments(post)
        for decision in decisions:
            image_id = int(decision.get("image_id") or 0)
            action = str(decision.get("action") or "").strip().lower()
            reason = str(decision.get("reason") or "").strip() or None
            if action not in {"keep", "disable", "enable"}:
                return jsonify({"error": "invalid_action", "image_id": image_id}), 400
            img = (
                s.query(SocialPostImage)
                .filter(SocialPostImage.id == image_id, SocialPostImage.post_id == post_id)
                .first()
            )
            if not img:
                return jsonify({"error": "image_not_found", "image_id": image_id}), 404
            if action == "disable":
                img.is_enabled = False
            elif action == "enable":
                img.is_enabled = True
            _apply_image_review_metadata(
                img,
                decision=action,
                reason=reason,
                source=review_source,
                reviewed_at=now,
            )
            updated_images.append(
                {
                    "image_id": image_id,
                    "action": action,
                    "is_enabled": bool(img.is_enabled),
                    "reason": reason,
                }
            )

        if review_summary:
            append_admin_comment(
                comments,
                text=f"Image review ({review_source}): {review_summary}",
                author=review_source,
                origin="system",
                event_type="image_review",
                timestamp=now.isoformat() + "Z",
            )
            _write_social_post_comments(post, comments)

        s.commit()

    return jsonify(
        {
            "ok": True,
            "post_id": post_id,
            "review_source": review_source,
            "updated_images": updated_images,
        }
    )


@app.get("/media/social_posts/<int:post_id>/<filename>")
def serve_social_post_image(post_id: int, filename: str):
    """Serve an image file from the post media directory."""
    from pathlib import Path
    media_dir = Path(__file__).resolve().parent.parent / "media" / "social_posts" / str(post_id)
    file_path = media_dir / filename
    if not file_path.exists() or not file_path.is_file():
        abort(404)
    # Security: ensure path doesn't escape media dir
    try:
        file_path.resolve().relative_to(media_dir.resolve())
    except ValueError:
        abort(403)
    mime_type = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
    return app.send_static_file(None) if False else make_response(
        open(file_path, "rb").read(),
        200,
        {"Content-Type": mime_type, "Cache-Control": "public, max-age=3600"},
    )


# ---------------------------------------------------------------------------
# Content API for Paperclip (localhost, no browser session needed)
# ---------------------------------------------------------------------------

@app.post("/api/content/posts")
def api_content_create_post():
    """Create a SocialPost with variants and suggested deliveries.

    Expects JSON:
    {
      "topic": "...",
      "source_date": "2026-03-28",
      "source_metrics": ["metric_key_1"],
      "source_game_ids": ["0022501058"],
      "priority": 30,
      "llm_model": "claude-sonnet-4-6",
      "images": [
        {"slot": "img1", "type": "screenshot", "file_path": "/tmp/flagg_player_page.png", "target": "https://funba.app/players/1642843", "note": "弗拉格球员页截图"},
        {"slot": "img2", "type": "web_search", "file_path": "/tmp/flagg_game_photo.jpg", "query": "Cooper Flagg Mavericks 51 points", "note": "弗拉格比赛图"}
      ],
      "variants": [
        {
          "title": "...",
          "content_raw": "...",
          "audience_hint": "thunder fans",
          "destinations": [
            {"platform": "hupu", "forum": "雷霆专区"}
          ]
        }
      ]
    }
    """
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime
    data = request.get_json(force=True) or {}
    topic = (data.get("topic") or "").strip()
    if not topic:
        return jsonify({"error": "topic required"}), 400
    source_date_str = data.get("source_date")
    if not source_date_str:
        return jsonify({"error": "source_date required"}), 400
    analysis_issue_id = (data.get("analysis_issue_id") or "").strip() or None
    analysis_issue_identifier = (data.get("analysis_issue_identifier") or "").strip() or None
    if analysis_issue_id or analysis_issue_identifier:
        try:
            resolved_issue = resolve_game_analysis_issue_record(
                analysis_issue_id=analysis_issue_id,
                analysis_issue_identifier=analysis_issue_identifier,
            )
        except PaperclipBridgeError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception as exc:
            return jsonify({"error": str(exc)}), 400
        if resolved_issue is None:
            return jsonify({"error": "analysis issue not found"}), 400
        # Enforce one-post-per-game-analysis-issue rule
        with SessionLocal() as s:
            existing_link = (
                s.query(GameContentAnalysisIssuePost)
                .filter(GameContentAnalysisIssuePost.issue_record_id == resolved_issue.id)
                .first()
            )
            if existing_link:
                return jsonify({
                    "error": "this game-analysis issue already has a linked post",
                    "existing_post_id": existing_link.post_id,
                }), 409
    try:
        prepared_images = _validate_prepared_image_specs(data.get("images", []))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 400

    # Dedup: skip if same source_date + topic already exists (non-archived)
    with SessionLocal() as s:
        existing = (
            s.query(SocialPost)
            .filter(
                SocialPost.source_date == date.fromisoformat(source_date_str),
                SocialPost.topic == topic,
                SocialPost.status != "archived",
            )
            .first()
        )
        if existing:
            return jsonify({"ok": True, "post_id": existing.id, "status": "duplicate_skipped"}), 200

    now = datetime.utcnow()
    staged_files: list[str] = []
    variant_ids: list[int] = []
    issue_link_requested = bool(analysis_issue_id or analysis_issue_identifier)
    try:
        with SessionLocal() as s:
            sp = SocialPost(
                topic=topic,
                source_date=date.fromisoformat(source_date_str),
                source_metrics=json.dumps(data.get("source_metrics", []), ensure_ascii=False),
                source_game_ids=json.dumps(data.get("source_game_ids", []), ensure_ascii=False),
                status=data.get("status", "draft"),
                priority=int(data.get("priority", 50)),
                llm_model=data.get("llm_model"),
                admin_comments=None,
                created_at=now,
                updated_at=now,
            )
            s.add(sp)
            s.flush()

            for vd in data.get("variants", []):
                vtitle = (vd.get("title") or "").strip()
                vcontent = (vd.get("content_raw") or "").strip()
                if not vtitle or not vcontent:
                    continue
                destinations = vd.get("destinations", [])
                audience_hint = (vd.get("audience_hint") or "").strip() or None
                for dest in destinations:
                    if str(dest.get("platform") or "").strip().lower() == "reddit":
                        audience_hint = _reddit_english_audience_hint(
                            audience_hint,
                            forum=dest.get("forum"),
                        )
                sv = SocialPostVariant(
                    post_id=sp.id,
                    title=vtitle,
                    content_raw=vcontent,
                    audience_hint=audience_hint,
                    created_at=now,
                    updated_at=now,
                )
                s.add(sv)
                s.flush()
                variant_ids.append(sv.id)

                for dest in destinations:
                    platform = (dest.get("platform") or "").strip()
                    forum = (dest.get("forum") or "").strip() or None
                    if platform.lower() == "hupu":
                        forum = normalize_hupu_forum(forum)
                    if platform.lower() == "reddit":
                        forum = _normalize_reddit_forum(forum)
                    if platform:
                        s.add(SocialPostDelivery(
                            variant_id=sv.id,
                            platform=platform,
                            forum=forum,
                            is_enabled=True,
                            status="pending",
                            created_at=now,
                            updated_at=now,
                        ))

            post_id = sp.id
            image_results = []
            for img in prepared_images:
                stored_path = store_prepared_image(img["source_path"], post_id=post_id, slot=img["slot"])
                staged_files.append(stored_path)
                image_results.append((img["slot"], img["image_type"], img["spec_json"], img["note"], stored_path, None, img["is_enabled"]))

            for slot, image_type, spec_json, note, file_path, error_msg, is_enabled in image_results:
                s.add(SocialPostImage(
                    post_id=post_id,
                    slot=slot,
                    image_type=image_type,
                    spec=spec_json,
                    note=note,
                    file_path=file_path,
                    is_enabled=bool(is_enabled and file_path is not None),
                    error_message=error_msg,
                    created_at=now,
                ))

            s.commit()
            if issue_link_requested:
                link_post_to_game_analysis_issue(
                    post_id,
                    analysis_issue_id=analysis_issue_id,
                    analysis_issue_identifier=analysis_issue_identifier,
                    discovered_via="api_create",
                )
    except Exception as exc:
        for staged_path in staged_files:
            try:
                Path(staged_path).unlink(missing_ok=True)
            except Exception:
                pass
        logger.warning("Prepared image ingest failed for post topic %s: %s", topic, exc)
        return jsonify({"error": str(exc)}), 400

    _ensure_paperclip_issue_for_post(post_id)
    sync_result = _sync_social_post_from_paperclip(post_id, ensure_issue=False)
    response = {"ok": True, "post_id": post_id, "variant_ids": variant_ids}
    if prepared_images:
        response["images"] = [
            {"slot": slot, "ok": True, "error": None}
            for slot, _, _, _, _, _, _ in image_results
        ]
    if sync_result:
        response.update(sync_result)
    return jsonify(response)


@app.get("/api/content/posts")
def api_content_list_posts():
    """List social posts, optionally filtered by status or date."""
    denied = _require_admin_json()
    if denied:
        return denied
    status_filter = request.args.get("status")
    date_filter = request.args.get("date")
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))

    with SessionLocal() as s:
        q = s.query(SocialPost).order_by(SocialPost.source_date.desc(), SocialPost.priority.asc())
        if status_filter:
            q = q.filter(SocialPost.status == status_filter)
        if date_filter:
            q = q.filter(SocialPost.source_date == date_filter)
        total = q.count()
        posts = q.offset(offset).limit(limit).all()

        return jsonify({
            "total": total,
            "posts": [
                {
                    "id": p.id,
                    "topic": p.topic,
                    "source_date": p.source_date.isoformat() if p.source_date else None,
                    "status": p.status,
                    "priority": p.priority,
                    "created_at": p.created_at.isoformat() if p.created_at else None,
                    "source_metrics": json.loads(p.source_metrics) if p.source_metrics else [],
                    "source_game_ids": json.loads(p.source_game_ids) if p.source_game_ids else [],
                }
                for p in posts
            ],
        })


@app.post("/api/content/deliveries/<int:delivery_id>/status")
def api_content_delivery_status(delivery_id: int):
    """Update a delivery's status (called by Paperclip after publishing).

    Expects JSON:
    {
      "status": "published",
      "published_url": "https://bbs.hupu.com/...",
      "error_message": null
    }
    """
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime
    data = request.get_json(force=True) or {}
    new_status = (data.get("status") or "").strip()
    if new_status not in ("pending", "publishing", "published", "failed"):
        return jsonify({"error": "invalid status"}), 400

    with SessionLocal() as s:
        d = s.query(SocialPostDelivery).filter(SocialPostDelivery.id == delivery_id).first()
        if not d:
            return jsonify({"error": "not_found"}), 404
        if "published_url" in data:
            d.published_url = data["published_url"]
        if "content_final" in data:
            d.content_final = data["content_final"]
        if "error_message" in data:
            d.error_message = data["error_message"]
        if new_status == "published" and d.platform == "hupu" and not _is_valid_hupu_thread_url(d.published_url):
            bad_url = d.published_url or "<missing>"
            d.status = "failed"
            d.published_url = None
            d.error_message = f"Invalid Hupu published_url reported: {bad_url}"
            d.published_at = None
        else:
            d.status = new_status
            if new_status == "published":
                d.published_at = datetime.utcnow()
        response_status = d.status
        d.updated_at = datetime.utcnow()
        s.commit()
    return jsonify({"ok": True, "status": response_status})


# ---------------------------------------------------------------------------
# Data API for Paperclip (localhost, read-only NBA data)
# ---------------------------------------------------------------------------

@app.get("/api/data/games")
def api_data_games():
    """Get games for a date. Query: ?date=2026-03-28"""
    denied = _require_admin_json()
    if denied:
        return denied
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "date required"}), 400
    from tasks.topics import get_games_by_date
    result = get_games_by_date(date.fromisoformat(date_str))
    return jsonify({"date": date_str, "games": result})


@app.get("/api/data/games/<game_id>/boxscore")
def api_data_boxscore(game_id: str):
    """Get box score for a game."""
    denied = _require_admin_json()
    if denied:
        return denied
    from tasks.topics import get_game_box_score
    return jsonify(get_game_box_score(game_id))


@app.get("/api/data/games/<game_id>/pbp")
def api_data_pbp(game_id: str):
    """Get play-by-play for a game period. Query: ?period=4"""
    denied = _require_admin_json()
    if denied:
        return denied
    period = int(request.args.get("period", 4))
    from tasks.topics import get_game_play_by_play
    return jsonify({"game_id": game_id, "period": period, "plays": get_game_play_by_play(game_id, period)})


@app.get("/api/data/games/<game_id>/metrics")
def api_data_game_metrics(game_id: str):
    """Get all metrics triggered by a single game."""
    denied = _require_admin_json()
    if denied:
        return denied
    from tasks.topics import get_game_metrics
    return jsonify({"game_id": game_id, "metrics": get_game_metrics(game_id)})


@app.get("/api/data/metrics/<metric_key>/top")
def api_data_metric_top(metric_key: str):
    """Get top N results for a metric. Query: ?season=22025&limit=10"""
    denied = _require_admin_json()
    if denied:
        return denied
    season = request.args.get("season")
    limit = min(int(request.args.get("limit", 10)), 100)
    from tasks.topics import get_metric_top_results
    return jsonify({"metric_key": metric_key, "results": get_metric_top_results(metric_key, season, limit)})


@app.get("/api/data/metrics/triggered")
def api_data_triggered_metrics():
    """Get triggered metrics for a date. Query: ?date=2026-03-28"""
    denied = _require_admin_json()
    if denied:
        return denied
    date_str = request.args.get("date")
    if not date_str:
        return jsonify({"error": "date required"}), 400
    from tasks.topics import get_triggered_metrics
    result = get_triggered_metrics(date.fromisoformat(date_str))
    return jsonify({"date": date_str, "metrics": result})


@app.get("/admin/fragment/<section>")
def admin_fragment(section: str):
    denied = _require_admin_page()
    if denied:
        return denied

    section = (section or "").strip().lower()
    runs_page_size = 25
    recent_page_size = 25
    perf_page_size = 20

    with SessionLocal() as session:
        if section == "visitor-stats":
            now_dt = datetime.utcnow()
            cutoff_24h = now_dt - timedelta(hours=24)
            cutoff_7d  = now_dt - timedelta(days=7)
            cutoff_30d = now_dt - timedelta(days=30)
            visitor_stats = {
                "user_count":   session.query(func.count(User.id)).scalar() or 0,
                "views_24h":    session.query(func.count(PageView.id)).filter(PageView.created_at >= cutoff_24h).scalar() or 0,
                "views_7d":     session.query(func.count(PageView.id)).filter(PageView.created_at >= cutoff_7d).scalar() or 0,
                "views_30d":    session.query(func.count(PageView.id)).filter(PageView.created_at >= cutoff_30d).scalar() or 0,
                "unique_24h":   session.query(func.count(func.distinct(PageView.visitor_id))).filter(PageView.created_at >= cutoff_24h).scalar() or 0,
                "unique_7d":    session.query(func.count(func.distinct(PageView.visitor_id))).filter(PageView.created_at >= cutoff_7d).scalar() or 0,
                "unique_30d":   session.query(func.count(func.distinct(PageView.visitor_id))).filter(PageView.created_at >= cutoff_30d).scalar() or 0,
            }
            return render_template("_admin_visitor_stats.html", visitor_stats=visitor_stats)

        if section == "top-pages":
            panel = _load_admin_top_pages_panel(session, request.args.get("window"))
            return render_template(
                "_admin_top_pages.html",
                selected_window=panel["selected_window"],
                top_pages=panel["top_pages"],
                top_referrers=panel["top_referrers"],
            )

        if section == "coverage":
            now = time.time()
            if "coverage" not in _admin_cache or now - _admin_cache.get("ts", 0) > _ADMIN_CACHE_TTL:
                from sqlalchemy import text as sa_text
                coverage_rows = session.execute(sa_text("""
                    SELECT
                        g.season,
                        COUNT(DISTINCT g.game_id)   AS total,
                        COUNT(DISTINCT box.game_id) AS has_detail,
                        COUNT(DISTINCT pbp.game_id) AS has_pbp,
                        COUNT(DISTINCT gls.game_id) AS has_line,
                        COUNT(DISTINCT sr.game_id)  AS has_shot,
                        COALESCE(SUM(mrl_agg.metric_cnt > 0), 0) AS has_metrics,
                        0                                        AS active_claims
                    FROM Game g
                    LEFT JOIN (
                        SELECT DISTINCT game_id FROM TeamGameStats
                        UNION
                        SELECT DISTINCT game_id FROM PlayerGameStats
                    ) box ON box.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM GamePlayByPlay)  pbp ON pbp.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM GameLineScore)   gls ON gls.game_id = g.game_id
                    LEFT JOIN (SELECT DISTINCT game_id FROM ShotRecord)       sr  ON sr.game_id  = g.game_id
                    LEFT JOIN (
                        SELECT game_id,
                               COUNT(DISTINCT metric_key) AS metric_cnt
                        FROM MetricRunLog
                        GROUP BY game_id
                    ) mrl_agg ON mrl_agg.game_id = g.game_id
                    WHERE g.game_date IS NOT NULL
                    GROUP BY g.season
                    ORDER BY g.season DESC
                """)).fetchall()
                coverage_source_rows = session.execute(sa_text("""
                    SELECT
                        g.season,
                        COALESCE(g.data_source, 'unknown') AS data_source,
                        COUNT(DISTINCT g.game_id) AS detail_games
                    FROM Game g
                    JOIN (
                        SELECT DISTINCT game_id FROM TeamGameStats
                        UNION
                        SELECT DISTINCT game_id FROM PlayerGameStats
                    ) box ON box.game_id = g.game_id
                    WHERE g.game_date IS NOT NULL
                    GROUP BY g.season, COALESCE(g.data_source, 'unknown')
                    ORDER BY g.season DESC, data_source ASC
                """)).fetchall()
                _admin_cache["coverage"] = {
                    "rows": coverage_rows,
                    "sources": coverage_source_rows,
                }
                _admin_cache["ts"] = now
            else:
                cached_coverage = _admin_cache["coverage"]
                if isinstance(cached_coverage, list):
                    coverage_rows = cached_coverage
                    coverage_source_rows = []
                else:
                    coverage_rows = cached_coverage["rows"]
                    coverage_source_rows = cached_coverage["sources"]
            source_counts_by_season: dict[str, list[dict[str, object]]] = defaultdict(list)
            for row in coverage_source_rows:
                season_key = str(row.season)
                source_counts_by_season[season_key].append(
                    {
                        "source": row.data_source,
                        "label": _box_score_source_label(row.data_source),
                        "count": int(row.detail_games or 0),
                    }
                )
            coverage = [
                {
                    "season": _season_label(row.season),
                    "season_raw": row.season,
                    "total": row.total,
                    "detail": row.has_detail,
                    "detail_sources": source_counts_by_season.get(str(row.season), []),
                    "detail_remaining": max(int(row.total or 0) - int(row.has_detail or 0), 0),
                    "pbp": row.has_pbp,
                    "line": row.has_line,
                    "shot": row.has_shot,
                    "metrics": row.has_metrics,
                    "active_claims": row.active_claims,
                    "complete": row.total == row.has_detail == row.has_pbp == row.has_shot == row.has_metrics,
                }
                for row in coverage_rows
            ]
            return render_template("_admin_coverage.html", coverage=coverage)

        if section == "compute-runs":
            panel = _load_admin_compute_runs_panel(session, runs_page=_admin_page_arg("runs_page"), runs_page_size=runs_page_size)
            return render_template(
                "_admin_compute_runs_card.html",
                compute_run_counts=panel["compute_run_counts"],
                compute_runs=panel["compute_runs"],
                runs_page=panel["runs_page"],
                runs_total_pages=panel["runs_total_pages"],
                admin_page_url=_admin_page_url,
                admin_fragment_url=_admin_fragment_url,
            )

        if section == "recent-runs":
            panel = _load_admin_recent_runs_panel(session, recent_page=_admin_page_arg("recent_page"), recent_page_size=recent_page_size)
            return render_template(
                "_admin_recent_runs_card.html",
                recent=panel["recent"],
                recent_page=panel["recent_page"],
                recent_has_prev=panel["recent_has_prev"],
                recent_has_next=panel["recent_has_next"],
                admin_page_url=_admin_page_url,
                admin_fragment_url=_admin_fragment_url,
            )

        if section == "metric-perf":
            panel = _load_admin_metric_perf_panel(
                session,
                perf_page=_admin_page_arg("perf_page"),
                perf_page_size=perf_page_size,
            )
            return render_template(
                "_admin_metric_perf.html",
                perf_data=panel["perf_data"],
                perf_page=panel["perf_page"],
                perf_total_pages=panel["perf_total_pages"],
                perf_has_prev=panel["perf_has_prev"],
                perf_has_next=panel["perf_has_next"],
                admin_page_url=_admin_page_url,
                admin_fragment_url=_admin_fragment_url,
            )

        if section == "missing":
            season_filter = Game.season.like("22024%") | Game.season.like("22025%")
            def _missing(joined_model, joined_col, limit=20):
                rows = (
                    session.query(Game.game_id, Game.game_date, Game.season)
                    .outerjoin(joined_model, joined_col == Game.game_id)
                    .filter(season_filter, Game.game_date.isnot(None), joined_col.is_(None))
                    .order_by(Game.game_date)
                    .limit(limit + 1)
                    .all()
                )
                overflow = len(rows) > limit
                rows = rows[:limit]
                total = len(rows) + (1 if overflow else 0)
                return {
                    "total": total,
                    "overflow": overflow,
                    "rows": [{"game_id": r.game_id, "game_date": r.game_date, "season": _season_label(r.season)} for r in rows],
                }
            return render_template(
                "_admin_missing.html",
                missing_detail=_missing(PlayerGameStats, PlayerGameStats.game_id),
                missing_shot=_missing(ShotRecord, ShotRecord.game_id),
                missing_metrics=_missing(MetricRunLog, MetricRunLog.game_id),
            )

    abort(404)


@app.get("/api/admin/infra-status")
def api_admin_infra_status():
    """Return Redis broker status, queue lengths, and Celery worker status."""
    denied = _require_admin_json()
    if denied:
        return denied

    # Redis broker status and queue lengths
    queues = []
    broker_ok = False
    try:
        import redis as _redis
        r = _redis.Redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"), socket_timeout=2)
        r.ping()
        broker_ok = True
    except Exception:
        pass

    # Celery inspect — get worker queues, concurrency, and active task counts
    workers = []
    try:
        from tasks.celery_app import app as celery_app
        inspector = celery_app.control.inspect(timeout=1.5)
        ping_result = celery_app.control.ping(timeout=1.5)
        active_queues = inspector.active_queues() or {}
        stats = inspector.stats() or {}
        active_tasks = inspector.active() or {}

        pinged = set()
        for entry in ping_result:
            for worker_name in entry:
                pinged.add(worker_name)

        for worker_name in pinged:
            wq = active_queues.get(worker_name, [])
            queue_names = sorted(set(q["name"] for q in wq if not q["name"].endswith(".pidbox")))
            ws = stats.get(worker_name, {})
            pool = ws.get("pool", {})
            concurrency = pool.get("max-concurrency", None)
            active_count = len(active_tasks.get(worker_name, []))
            role = ", ".join(queue_names) if queue_names else "unknown"
            workers.append({
                "name": worker_name,
                "role": role,
                "concurrency": concurrency,
                "active": active_count,
                "ok": True,
            })
        workers.sort(key=lambda w: w["role"])

        # Build queue info from Redis lengths + worker consumer counts
        if broker_ok:
            import redis as _redis
            r = _redis.Redis.from_url(os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"), socket_timeout=2)
            consumer_count: dict[str, int] = {}
            for wq_list in active_queues.values():
                for q in wq_list:
                    qn = q.get("name", "")
                    if not qn.endswith(".pidbox"):
                        consumer_count[qn] = consumer_count.get(qn, 0) + 1
            for qname in ("ingest", "metrics", "reduce"):
                length = r.llen(qname) or 0
                queues.append({"name": qname, "ready": length, "unacked": 0, "consumers": consumer_count.get(qname, 0)})
    except Exception:
        pass

    # Scheduled tasks from Celery Beat config
    scheduled = []
    try:
        from tasks.celery_app import app as _celery_app
        for name, entry in (_celery_app.conf.beat_schedule or {}).items():
            scheduled.append({
                "name": name,
                "task": entry.get("task", ""),
                "every": str(entry.get("schedule", "")),
            })
    except Exception:
        pass

    return jsonify({"ok": True, "broker_ok": broker_ok, "queues": queues, "workers": workers, "scheduled": scheduled})


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


@app.get("/api/admin/feature-access")
def api_admin_feature_access():
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as session:
        return jsonify({"ok": True, "features": _serialize_feature_access(session)})


@app.post("/api/admin/feature-access")
def api_admin_update_feature_access():
    denied = _require_admin_json()
    if denied:
        return denied
    body = request.get_json(force=True) or {}
    try:
        with SessionLocal() as session:
            updated = {}
            for descriptor in feature_access_descriptors():
                feature_key = descriptor["key"]
                if feature_key in body:
                    updated[feature_key] = set_feature_access_level(
                        session, feature_key, body[feature_key]
                    )
            session.commit()
            features = _serialize_feature_access(session)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("failed to save feature access config")
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True, "updated": updated, "features": features})


@app.get("/api/admin/model-config")
def api_admin_model_config():
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as session:
        return jsonify(
            {
                "default_model": get_default_llm_model_for_ui(session),
                "search_model": get_llm_model_for_purpose(session, "search"),
                "generate_model": get_llm_model_for_purpose(session, "generate"),
                "available_models": available_llm_models(),
            }
        )


@app.get("/api/admin/paperclip-config")
def api_admin_paperclip_config():
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as session:
        return jsonify(
            {
                "ok": True,
                "issue_base_url": get_paperclip_issue_base_url(session),
            }
        )


@app.post("/api/admin/paperclip-config")
def api_admin_update_paperclip_config():
    denied = _require_admin_json()
    if denied:
        return denied
    body = request.get_json(force=True) or {}
    try:
        with SessionLocal() as session:
            issue_base_url = set_paperclip_issue_base_url(session, body.get("issue_base_url"))
            session.commit()
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("failed to save paperclip config")
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify({"ok": True, "issue_base_url": issue_base_url})


@app.post("/api/admin/model-config")
def api_admin_update_model_config():
    denied = _require_admin_json()
    if denied:
        return denied
    body = request.get_json(force=True) or {}
    try:
        with SessionLocal() as session:
            result = {}
            if "search_model" in body:
                result["search_model"] = set_llm_model_for_purpose(session, "search", body["search_model"])
            if "generate_model" in body:
                result["generate_model"] = set_llm_model_for_purpose(session, "generate", body["generate_model"])
            if "default_model" in body:
                result["default_model"] = set_default_llm_model(session, body["default_model"])
            session.commit()
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("failed to save admin model config")
        return jsonify({"ok": False, "error": str(exc)}), 500
    return jsonify(
        {
            "ok": True,
            **result,
            "available_models": available_llm_models(),
        }
    )


@app.get("/api/admin/runtime-flags")
def api_admin_runtime_flags():
    denied = _require_admin_json()
    if denied:
        return denied
    return jsonify({"ok": True, "flags": load_runtime_flags()})


@app.get("/api/admin/ai-usage")
def api_admin_ai_usage():
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as session:
        return jsonify({"ok": True, "dashboard": get_ai_usage_dashboard(session)})


@app.get("/api/admin/visitor-timeseries")
def api_admin_visitor_timeseries():
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime, timedelta
    from sqlalchemy import extract

    days = min(int(request.args.get("days", 90)), 365)
    cutoff = datetime.utcnow() - timedelta(days=days)

    year_col = extract("year", PageView.created_at)
    month_col = extract("month", PageView.created_at)
    day_col = extract("day", PageView.created_at)
    hour_col = extract("hour", PageView.created_at)

    with SessionLocal() as session:
        rows = (
            session.query(
                year_col.label("y"),
                month_col.label("m"),
                day_col.label("d"),
                hour_col.label("h"),
                func.count(PageView.id).label("views"),
                func.count(func.distinct(PageView.visitor_id)).label("unique"),
            )
            .filter(PageView.created_at >= cutoff)
            .group_by("y", "m", "d", "h")
            .order_by("y", "m", "d", "h")
            .all()
        )
        data = [
            {
                "date": f"{int(r.y):04d}-{int(r.m):02d}-{int(r.d):02d}T{int(r.h):02d}:00:00Z",
                "views": r.views,
                "unique": r.unique,
            }
            for r in rows
        ]

        # Published posts in the same window — group by hour bucket
        posts = (
            session.query(
                SocialPostDelivery.published_at,
                SocialPostDelivery.platform,
                SocialPostVariant.title,
            )
            .join(SocialPostVariant, SocialPostDelivery.variant_id == SocialPostVariant.id)
            .filter(
                SocialPostDelivery.status == "published",
                SocialPostDelivery.published_at >= cutoff,
                SocialPostDelivery.published_at.isnot(None),
            )
            .order_by(SocialPostDelivery.published_at)
            .all()
        )
        from collections import OrderedDict
        post_buckets: dict[str, dict] = OrderedDict()
        for p in posts:
            key = p.published_at.strftime("%Y-%m-%dT%H:00:00Z")
            bucket = post_buckets.setdefault(key, {"date": key, "count": 0, "titles": []})
            bucket["count"] += 1
            label = f"[{p.platform}] {(p.title or '')[:60]}"
            bucket["titles"].append(label)
        post_data = list(post_buckets.values())
    return jsonify({"ok": True, "series": data, "posts": post_data})


@app.post("/api/admin/runtime-flags")
def api_admin_update_runtime_flags():
    denied = _require_admin_json()
    if denied:
        return denied
    body = request.get_json(force=True) or {}
    from runtime_flags import DEFAULT_RUNTIME_FLAGS
    flags = load_runtime_flags()
    updated = False
    for key in DEFAULT_RUNTIME_FLAGS:
        if key in body:
            try:
                flags = set_runtime_flag(key, body[key])
                updated = True
            except KeyError:
                return jsonify({"ok": False, "error": f"unknown runtime flag: {key}"}), 400
    if not updated:
        return jsonify({"ok": False, "error": "no recognized flags in request body"}), 400
    return jsonify({"ok": True, "flags": flags})


@app.post("/admin/backfill/<season>")
def admin_backfill(season: str):
    """Discover + insert any missing games from NBA API, then enqueue ingest for the season."""
    denied = _require_admin_json()
    if denied:
        return denied
    from tasks.ingest import ingest_game
    from tasks.celery_app import app as celery_app
    from tasks.dispatch import discover_and_insert_games
    from metrics.framework.runtime import get_all_metrics as _get_runtime_metrics

    # Convert DB season code (e.g. "22025") → NBA API season string + type
    _type_map = {"2": "Regular Season", "4": "Playoffs", "5": "PlayIn", "1": "Pre Season"}
    prefix = season[0] if season else "2"
    year = int(season[1:]) if len(season) > 1 else 0
    nba_season = f"{year}-{(year + 1) % 100:02d}"
    season_type = _type_map.get(prefix, "Regular Season")

    game_ids = discover_and_insert_games(
        season=nba_season,
        season_types=[season_type],
    )

    if not game_ids:
        return jsonify({"error": f"No games found for season {season}"}), 404

    # Use the pre-configured Queue object (with DLX args) so RabbitMQ doesn't
    # reject the declaration as inequivalent to the existing queue.
    ingest_q = next(q for q in celery_app.conf.task_queues if q.name == "ingest")
    metric_keys = [m.key for m in _get_runtime_metrics()]
    for gid in game_ids:
        ingest_game.apply_async(args=[gid], kwargs={"metric_keys": metric_keys}, declare=[ingest_q])

    return jsonify({"season": season, "enqueued": len(game_ids)})


@app.post("/games/<game_id>/shotchart/backfill")
def game_shotchart_backfill(game_id: str):
    denied = _require_admin_page()
    if denied:
        return denied
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            abort(404, description=f"Game {game_id} not found")

        try:
            count = back_fill_game_shot_record_from_api(session, game_id, commit=True, replace_existing=False)
            return redirect(url_for("game_page", game_id=game_id, shot_backfill="ok", shot_count=count))
        except Exception:
            session.rollback()
            app.logger.exception("manual shotchart backfill failed for game_id=%s", game_id)
            return redirect(url_for("game_page", game_id=game_id, shot_backfill="error"))


@app.post("/api/games/<game_id>/shotchart/backfill")
def game_shotchart_backfill_api(game_id: str):
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            return jsonify({"ok": False, "error": f"Game {game_id} not found"}), 404

        try:
            count = back_fill_game_shot_record_from_api(session, game_id, commit=True, replace_existing=False)
            return jsonify({"ok": True, "game_id": game_id, "shot_count": int(count)})
        except Exception as exc:
            session.rollback()
            app.logger.exception("manual shotchart backfill failed for game_id=%s", game_id)
            return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.getenv("FUNBA_WEB_PORT", "5000"))
    host = os.getenv("FUNBA_WEB_HOST", "127.0.0.1")
    debug = os.getenv("FUNBA_WEB_DEBUG", "1") != "0"
    app.run(host=host, port=port, debug=debug)
