from __future__ import annotations

import ast
from collections import defaultdict
from datetime import date
from functools import lru_cache
import inspect
import json
import logging
import os
import time
from types import SimpleNamespace

logger = logging.getLogger(__name__)

import uuid as _uuid_mod

from flask import Flask, abort, after_this_request, flash, get_flashed_messages, jsonify, make_response, redirect, render_template, request, session, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from authlib.integrations.flask_client import OAuth
from sqlalchemy import and_, case, func, or_, text
from sqlalchemy.orm import sessionmaker

from db.llm_models import (
    available_llm_models,
    get_default_llm_model_for_ui,
    get_llm_model_for_purpose,
    resolve_llm_model,
    set_default_llm_model,
    set_llm_model_for_purpose,
)
from db.models import Award, Feedback, Game, GameLineScore, GamePlayByPlay, MagicToken, MetricComputeRun, MetricDefinition as MetricDefinitionModel, MetricResult as MetricResultModel, MetricRunLog, PageView, Player, PlayerGameStats, PlayerSalary, ShotRecord, Team, TeamGameStats, TopicPost, User, engine
from db.backfill_nba_player_shot_detail import back_fill_game_shot_record_from_api
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


app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32))
SessionLocal = sessionmaker(bind=engine)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
    storage_uri="memory://",
)


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
    default_name = team.full_name if team else team_id
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
    return team.full_name or team_id


def _team_abbr(teams: dict[str, Team], team_id: str | None) -> str:
    if not team_id:
        return "-"
    team = teams.get(team_id)
    if team is None:
        return team_id
    return team.abbr or team.full_name or team_id


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
    return meta.get(key, award_type.replace("_", " ").title())


def _award_badge_label(award_type: str) -> str:
    meta = _AWARD_TYPE_META.get(award_type, {})
    return meta.get("badge_label", _award_type_label(award_type, short=True))


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
        "player_name": row.player_name or row.player_id,
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
    return SimpleNamespace(
        key=metric_def.key,
        name=metric_def.name,
        description=getattr(metric_def, "description", "") or "",
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


def _code_metric_metadata_from_code(
    code_python: str,
    *,
    expected_key: str | None = None,
    rank_order_override: str | None = None,
) -> dict:
    from metrics.framework.code_optimizer import optimize_metric_code
    from metrics.framework.runtime import load_code_metric

    normalized_code = _normalize_code_metric_key(code_python, expected_key)
    normalized_code = _normalize_code_metric_rank_order(normalized_code, rank_order_override)
    normalized_code = optimize_metric_code(normalized_code)
    metric = load_code_metric(normalized_code)
    metadata = {
        "key": metric.key,
        "name": metric.name,
        "description": metric.description,
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
        )
        return details

    if code_metadata:
        details.update(
            min_sample=code_metadata["min_sample"],
            career_min_sample=code_metadata["career_min_sample"],
            supports_career=code_metadata["supports_career"],
            career=code_metadata["career"],
            incremental=code_metadata["incremental"],
            rank_order=code_metadata["rank_order"],
            name=code_metadata["name"],
            description=code_metadata["description"],
            scope=code_metadata["scope"],
            category=code_metadata["category"],
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
    description: str,
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
    base_row.description = description
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
        existing_sibling.description = career_description
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


def _catalog_metrics(session, scope_filter: str = "", status_filter: str = "", current_user_id: str | None = None) -> list[dict]:
    from metrics.framework.runtime import get_metric as _get_metric

    counts = {
        row.metric_key: row.count
        for row in session.query(
            MetricResultModel.metric_key,
            func.count(MetricResultModel.id).label("count"),
        ).group_by(MetricResultModel.metric_key).all()
    }

    db_q = session.query(MetricDefinitionModel).filter(
        MetricDefinitionModel.status != "archived"
    )
    if not status_filter:
        db_q = db_q.filter(MetricDefinitionModel.status != "draft")
    if scope_filter:
        if scope_filter == "player":
            db_q = db_q.filter(MetricDefinitionModel.scope.in_(["player", "player_franchise"]))
        else:
            db_q = db_q.filter(MetricDefinitionModel.scope == scope_filter)
    if status_filter:
        db_q = db_q.filter(MetricDefinitionModel.status == status_filter)

    all_defs = db_q.order_by(MetricDefinitionModel.created_at.desc()).all()
    existing_keys = {m.key for m in all_defs}

    db_metrics = []
    for m in all_defs:
        code_metadata = _safe_code_metric_metadata(m)
        search_fields = _db_metric_search_fields(m, code_metadata=code_metadata)
        is_mine = bool(current_user_id and m.created_by_user_id == current_user_id)
        db_metrics.append(
            {
                "key": m.key,
                "name": search_fields.get("name", m.name),
                "description": search_fields.get("description", m.description),
                "scope": search_fields.get("scope", m.scope),
                "category": search_fields.get("category", m.category or ""),
                "status": m.status,
                "source_type": m.source_type,
                "result_count": counts.get(m.key, 0),
                "is_mine": is_mine,
                **search_fields,
            }
        )
        if (
            m.status == "published"
            and not search_fields.get("career")
            and search_fields.get("supports_career")
        ):
            career_key = f"{m.key}_career"
            if career_key not in existing_keys:
                career_metric = _get_metric(career_key, session=session)
                if career_metric is not None:
                    db_metrics.append(
                        {
                            "key": career_metric.key,
                            "name": career_metric.name,
                            "description": getattr(career_metric, "description", "") or "",
                            "scope": career_metric.scope,
                            "category": getattr(career_metric, "category", "") or "",
                            "status": "published",
                            "source_type": getattr(career_metric, "source_type", m.source_type),
                            "result_count": counts.get(career_metric.key, 0),
                            "is_mine": is_mine,
                            "group_key": search_fields.get("group_key"),
                            "min_sample": int(getattr(career_metric, "min_sample", m.min_sample or 1) or 1),
                            "expression": m.expression or "",
                            "definition_json": search_fields.get("definition_json", ""),
                            "code_python": search_fields.get("code_python", ""),
                            "supports_career": bool(getattr(career_metric, "supports_career", False)),
                            "career": True,
                            "incremental": bool(getattr(career_metric, "incremental", False)),
                            "rank_order": getattr(career_metric, "rank_order", search_fields.get("rank_order", "desc")),
                            "career_min_sample": search_fields.get("career_min_sample"),
                            "time_scope": "career",
                            "base_metric_key": m.key,
                        }
                    )
    # Own metrics first, then by original order
    db_metrics.sort(key=lambda d: (not d["is_mine"],))
    return db_metrics


def _catalog_top3(session, metrics_list: list[dict]) -> dict[str, list[dict]]:
    """Bulk-load top 3 results per metric for the current season.

    Returns {metric_key: [{entity_id, label, value_str, headshot_url|logo_url}, ...]}.
    """
    from sqlalchemy import case

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

    season_keys = [k for k, v in metric_info.items() if not v[2]]
    career_keys = [k for k, v in metric_info.items() if v[2]]

    # Bulk query: season metrics use current_season, career metrics use "all_regular"
    rows = []
    if season_keys:
        rows.extend(
            session.query(MetricResultModel)
            .filter(
                MetricResultModel.metric_key.in_(season_keys),
                MetricResultModel.season == current_season,
                MetricResultModel.value_num.isnot(None),
            )
            .all()
        )
    if career_keys:
        rows.extend(
            session.query(MetricResultModel)
            .filter(
                MetricResultModel.metric_key.in_(career_keys),
                MetricResultModel.season == "all_regular",
                MetricResultModel.value_num.isnot(None),
            )
            .all()
        )

    # Group by metric_key, sort, take top 3
    from collections import defaultdict
    by_metric: dict[str, list] = defaultdict(list)
    for r in rows:
        by_metric[r.metric_key].append(r)

    # Collect all entity IDs we need to resolve
    player_ids: set[str] = set()
    team_ids: set[str] = set()

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

    # Bulk resolve names
    player_names = {}
    if player_ids:
        for p in session.query(Player.player_id, Player.full_name).filter(Player.player_id.in_(player_ids)).all():
            player_names[p.player_id] = p.full_name

    team_abbrs = {}
    if team_ids:
        for t in session.query(Team.team_id, Team.abbr).filter(Team.team_id.in_(team_ids)).all():
            team_abbrs[t.team_id] = t.abbr

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
                label = team_abbrs.get(eid, eid)
                entry = {
                    "entity_id": eid,
                    "label": label,
                    "value_str": r.value_str or (f"{r.value_num:.1f}" if r.value_num is not None else ""),
                    "logo_url": f"https://cdn.nba.com/logos/nba/{eid}/global/L/logo.svg",
                }
            else:
                # game scope — just show value_str
                entry = {
                    "entity_id": eid,
                    "label": r.value_str or "",
                    "value_str": r.value_str or (f"{r.value_num:.1f}" if r.value_num is not None else ""),
                }
            entries.append(entry)
        if entries:
            result[key] = entries

    return result


def _fmt_minutes(minute: int | None, sec: int | None) -> str:
    if minute is None and sec is None:
        return "-"
    return f"{minute or 0:02d}:{sec or 0:02d}"


def _player_status(stat: PlayerGameStats) -> str:
    comment = (stat.comment or "").strip()
    if comment:
        return comment
    if stat.min is None and stat.sec is None:
        return "Did not play"
    if stat.starter:
        return "Starter"
    return "Played"


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

    _CAREER_TYPE_LABEL = {"all_regular": "Regular Season", "all_playoffs": "Playoffs", "all_playin": "Play-In"}

    _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
    _asc_keys = _asc_metric_keys(session)
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
            "1": "Pre Season",
            "2": "Regular Season",
            "3": "All Star",
            "4": "Playoffs",
            "5": "PlayIn",
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
        "path_d": "M -220 92.5 L -220 375 L -90 375 L -90 219.8 A 237.5 237.5 0 0 1 -220 92.5 Z",
    },
    {
        "key": "center_above_break_3",
        "label": "Center Above Break 3",
        "x": 0.0,
        "y": 305.0,
        "path_d": "M -90 219.8 L -90 375 L 90 375 L 90 219.8 A 237.5 237.5 0 0 0 -90 219.8 Z",
    },
    {
        "key": "right_above_break_3",
        "label": "Right Above Break 3",
        "x": 170.0,
        "y": 285.0,
        "path_d": "M 220 92.5 L 220 375 L 90 375 L 90 219.8 A 237.5 237.5 0 0 0 220 92.5 Z",
    },
    {
        "key": "left_mid_range",
        "label": "Left Mid-Range",
        "x": -145.0,
        "y": 145.0,
        "path_d": "M -220 -47.5 L -80 -47.5 L -80 142.5 L -154 180.8 A 237.5 237.5 0 0 1 -220 92.5 Z",
    },
    {
        "key": "center_mid_range",
        "label": "Center Mid-Range",
        "x": 0.0,
        "y": 192.0,
        "path_d": "M -154 180.8 L -80 142.5 L 80 142.5 L 154 180.8 A 237.5 237.5 0 0 1 -154 180.8 Z",
    },
    {
        "key": "right_mid_range",
        "label": "Right Mid-Range",
        "x": 145.0,
        "y": 145.0,
        "path_d": "M 220 -47.5 L 80 -47.5 L 80 142.5 L 154 180.8 A 237.5 237.5 0 0 0 220 92.5 Z",
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
        "y": 372.0,
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
        density = (attempts / max_attempts) if max_attempts > 0 else 0.0
        hue = int(210 - (210 * density))  # blue -> red based on attempt density
        lightness = int(88 - (38 * density))
        alpha = round(0.08 + (0.72 * density), 3) if attempts > 0 else 0.035
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
                "color": f"hsl({hue} 86% {lightness}%)",
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
        from datetime import datetime
        if datetime.utcnow() > user.subscription_expires_at:
            return False
    return True


def _require_pro_json():
    if not is_pro():
        return jsonify({"error": "pro_required", "upgrade_url": url_for("pricing")}), 403
    return None


def _require_pro_page():
    if not is_pro():
        return render_template("upgrade_required.html"), 403
    return None


def _request_next_url() -> str:
    next_url = request.full_path or request.path or url_for("home")
    return next_url[:-1] if next_url.endswith("?") else next_url


def _require_login_json():
    if _current_user():
        return None
    return jsonify({"error": "login_required"}), 401


def _require_login_page():
    if _current_user():
        return None
    return redirect(url_for("auth_login", next=_request_next_url()))


def _require_metric_creator_json():
    """Admin or Pro can create/edit metrics."""
    if is_admin() or is_pro():
        return None
    if _current_user():
        return jsonify({"error": "pro_required", "upgrade_url": url_for("pricing")}), 403
    return jsonify({"error": "login_required"}), 401


def _require_metric_creator_page():
    if is_admin() or is_pro():
        return None
    if _current_user():
        return render_template("upgrade_required.html"), 403
    return redirect(url_for("auth_login", next=request.url))


_VISITOR_COOKIE = "funba_visitor"
_VISITOR_COOKIE_MAX_AGE = 365 * 24 * 3600  # 1 year in seconds


@app.before_request
def _track_page_view():
    """Log each page load and ensure the visitor cookie is set."""
    # Only track GET requests for HTML pages (skip API, static, etc.)
    if request.method != "GET":
        return
    if request.path.startswith("/api/") or request.path.startswith("/static/"):
        return

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
        ip_address=request.remote_addr,
        created_at=datetime.utcnow(),
    )
    try:
        with SessionLocal() as session:
            session.add(pv)
            session.commit()
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


@app.context_processor
def inject_template_helpers():
    from datetime import date
    return {
        "season_label": _season_label,
        "is_admin": is_admin(),
        "is_pro": is_pro(),
        "current_user": _current_user(),
        "current_year": date.today().year,
        "clean_key": _strip_draft_prefix,
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


@app.route("/pricing")
def pricing():
    price_info = _get_stripe_price()
    return render_template("pricing.html", price_info=price_info)


@app.route("/account")
def account_page():
    user = _current_user()
    if not user:
        return redirect(url_for("auth_login", next=url_for("account_page")))
    return render_template("account.html", user=user, checkout=request.args.get("checkout"))


@app.post("/subscribe/checkout")
def subscribe_checkout():
    import stripe
    user = _current_user()
    if not user:
        return redirect(url_for("auth_login", next=url_for("pricing")))

    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    if not stripe.api_key:
        flash("Payment is not configured yet.", "error")
        return redirect(url_for("pricing"))

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
        return redirect(url_for("pricing"))

    checkout_session = stripe.checkout.Session.create(
        customer=customer_id,
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=url_for("account_page", checkout="success", _external=True),
        cancel_url=url_for("pricing", _external=True),
    )
    return redirect(checkout_session.url, code=303)


@app.post("/subscribe/portal")
def subscribe_portal():
    import stripe
    user = _current_user()
    if not user or not user.stripe_customer_id:
        return redirect(url_for("pricing"))

    stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
    portal_session = stripe.billing_portal.Session.create(
        customer=user.stripe_customer_id,
        return_url=url_for("account_page", _external=True),
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
                "full_name": team.full_name,
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
                Team.full_name.label("team_name"),
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
            q = q.filter(Player.full_name.ilike(f"%{query}%"))
        players = q.order_by(Player.is_active.desc(), Player.full_name.asc()).limit(limit).all()

    items = [{"player_id": p.player_id, "full_name": p.full_name} for p in players if p.player_id and p.full_name]
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
    from metrics.framework.runtime import get_metric as _get_metric

    runtime_metric = _get_metric(metric_key, session=session)
    if runtime_metric is None and metric_key.endswith("_career"):
        runtime_metric = _get_metric(metric_key.removesuffix("_career"), session=session)
    if runtime_metric is not None and getattr(runtime_metric, "name", None):
        return runtime_metric.name

    base_key = metric_key.removesuffix("_career")
    db_metric = (
        session.query(MetricDefinitionModel)
        .filter(MetricDefinitionModel.key == base_key)
        .first()
    )
    if db_metric is not None and db_metric.name:
        return db_metric.name

    return base_key.replace("_", " ").title()


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
        "all_regular": "Regular Season Career",
        "all_playoffs": "Playoffs Career",
        "all_playin": "Play-In Career",
    }
    if season in career_labels:
        return career_labels[season]
    return entry.get("career_type_label") or "Career"


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
                        "href": url_for("metric_detail", metric_key=entry["metric_key"]),
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
            grouped[entry.get("career_type_label") or "Career"].append(entry)
        for title in grouped:
            grouped_alltime.setdefault(title, [[] for _ in player_cards])
    for idx, card in enumerate(player_cards):
        grouped: dict[str, list[dict]] = defaultdict(list)
        for entry in card["metrics"]["alltime"]:
            grouped[entry.get("career_type_label") or "Career"].append(entry)
        for title, lists in grouped_alltime.items():
            lists[idx] = grouped.get(title, [])

    for title in sorted(grouped_alltime.keys()):
        section = build_rows(grouped_alltime[title], group_title=f"{title} Career")
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
                "href": url_for("metric_detail", metric_key=metric_key, season=row.season)
                if row.season
                else url_for("metric_detail", metric_key=metric_key),
            }
        )
    return rankings


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

        heatmap_query = (
            session.query(ShotRecord.shot_zone_basic, ShotRecord.shot_zone_area, ShotRecord.shot_made)
            .join(Game, ShotRecord.game_id == Game.game_id)
            .filter(
                ShotRecord.player_id == player_id,
                Game.season.like(f"{season_prefix}%"),
                ShotRecord.shot_zone_basic.isnot(None),
            )
        )
        if selected_heatmap_season != "overall":
            heatmap_query = heatmap_query.filter(Game.season == selected_heatmap_season)

        heatmap_zones, heatmap_attempts, heatmap_made = _build_shot_zone_heatmap(heatmap_query.all())
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
                    }
                )

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
        heatmap_zones=heatmap_zones,
        heatmap_attempts=heatmap_attempts,
        heatmap_made=heatmap_made,
        season_options=season_options,
        selected_season=selected_season,
        game_rows=game_rows,
        player_metrics=player_metrics,
        player_awards=player_awards,
        salary_rows=salary_rows,
    )


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
        if not is_pro() and current_season:
            season_options = [s for s in season_options if s == current_season]
            season_summary_view = [r for r in season_summary_view if r["season"] == current_season]
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
            player_name = player.full_name if player is not None and player.full_name else stat.player_id
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
            str(stat.player_id): (player.full_name if player and player.full_name else str(stat.player_id))
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
                str(player.player_id): (player.full_name or str(player.player_id))
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
    )


@app.get("/games/<game_id>/fragment/metrics")
def game_fragment_metrics(game_id: str):
    """Async fragment: game metrics section."""
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            abort(404)
        game_metrics = _get_metric_results(session, "game", game_id, game.season)
    return render_template("_game_metrics.html", game_metrics=game_metrics)


@app.route("/metrics")
def metrics_browse():
    scope_filter = request.args.get("scope", "")
    status_filter = request.args.get("status", "")  # draft | published | ""
    search_query = request.args.get("q", "").strip()

    cur_user = _current_user()
    with SessionLocal() as session:
        metrics_list = _catalog_metrics(session, scope_filter=scope_filter, status_filter=status_filter, current_user_id=cur_user.id if cur_user else None)
        llm_default_model = get_llm_model_for_purpose(session, "search")
        top3_by_metric = _catalog_top3(session, metrics_list)

    return render_template(
        "metrics.html",
        metrics_list=metrics_list,
        scope_filter=scope_filter,
        status_filter=status_filter,
        search_query=search_query,
        top3_by_metric=top3_by_metric,
        llm_default_model=llm_default_model,
        llm_available_models=available_llm_models(),
    )


@app.route("/metrics/mine")
def my_metrics():
    denied = _require_login_page()
    if denied:
        return denied

    cur_user = _current_user()
    if cur_user is None:
        return redirect(url_for("auth_login", next=request.url))

    with SessionLocal() as session:
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
                MetricDefinitionModel.status == "published",
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
        },
    )


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
    return render_template(
        "metric_new.html",
        current_season=current_season,
        all_seasons=all_seasons,
        initial_expression=initial_expression,
        edit_metric=None,
        llm_default_model=llm_default_model,
        llm_available_models=available_llm_models(),
    )


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
            "description": m.description or "",
            "scope": m.scope,
            "category": m.category or "",
            "code": m.code_python or "",
            "expression": m.expression or "",
            "min_sample": m.min_sample,
            "rank_order": getattr(runtime_metric, "rank_order", "desc"),
            "max_results_per_season": getattr(runtime_metric, "max_results_per_season", None) or m.max_results_per_season,
            "group_key": m.group_key,
            "status": m.status,
        }
        llm_default_model = get_llm_model_for_purpose(session, "generate")

    return render_template(
        "metric_new.html",
        current_season=current_season,
        all_seasons=all_seasons,
        initial_expression="",
        edit_metric=edit_data,
        llm_default_model=llm_default_model,
        llm_available_models=available_llm_models(),
    )


@app.post("/api/metrics/search")
@limiter.limit("30 per minute")
def api_metric_search():
    denied = _require_login_json()
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

    with SessionLocal() as session:
        catalog = _catalog_metrics(session, scope_filter=scope_filter, status_filter=status_filter)
        try:
            llm_model = resolve_llm_model(session, requested_model=requested_model, purpose="search")
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

    try:
        ranked = rank_metrics(query, catalog, limit=8, model=llm_model)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("metric search failed")
        return jsonify({"ok": False, "error": str(exc)}), 500

    by_key = {metric["key"]: metric for metric in catalog}
    matches = []
    for ranked_item in ranked:
        metric = by_key.get(ranked_item["key"])
        if metric is None:
            continue
        matches.append({**metric, "reason": ranked_item["reason"]})

    return jsonify({"ok": True, "matches": matches})


@app.post("/api/metrics/check-similar")
@limiter.limit("15 per minute")
def api_metric_check_similar():
    denied = _require_login_json()
    if denied:
        return denied
    from metrics.framework.generator import check_similar
    body = request.get_json(force=True) or {}
    expression = (body.get("expression") or "").strip()
    requested_model = (body.get("model") or "").strip() if is_admin() else None
    if not expression:
        return jsonify({"ok": False, "error": "expression is required"}), 400
    with SessionLocal() as session:
        try:
            llm_model = resolve_llm_model(session, requested_model=requested_model, purpose="search")
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
        catalog = _catalog_metrics(session, status_filter="published")
    try:
        similar = check_similar(expression, catalog, model=llm_model)
    except Exception as exc:
        logger.exception("check-similar failed")
        similar = []
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
    requested_model = (body.get("model") or "").strip() if is_admin() else None
    if not expression:
        return jsonify({"ok": False, "error": "expression is required"}), 400
    with SessionLocal() as session:
        try:
            llm_model = resolve_llm_model(session, requested_model=requested_model, purpose="generate")
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400
    try:
        spec = generate(expression, history=history, existing=existing, model=llm_model)
        response_type = (spec.get("responseType") or "code") if isinstance(spec, dict) else "code"
        if response_type == "clarification":
            return jsonify({
                "ok": True,
                "responseType": "clarification",
                "message": spec.get("message", ""),
            })
        return jsonify({
            "ok": True,
            "responseType": "code",
            "spec": spec,
        })
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        logger.exception("metric generate failed")
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
):
    """Run a code-based metric against sample games and return ranked results."""
    from metrics.framework.runtime import ReadOnlySession, load_code_metric

    metadata = _code_metric_metadata_from_code(code_python, rank_order_override=rank_order_override)
    metric = load_code_metric(metadata["code_python"])
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
        try:
            results = metric.compute_season(ro_session, season)
        except Exception:
            logger.exception("preview compute_season failed for season=%s", season)
            results = []
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
    scope = (body.get("scope") or "").strip()
    code_python = (body.get("code") or "").strip()
    definition = body.get("definition")
    rank_order_override = str(body.get("rank_order") or "").strip().lower() or None

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
            )
        except Exception as exc:
            return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400
        code_python = code_metadata["code_python"]
        key = code_metadata["key"]
        name = code_metadata["name"]
        scope = code_metadata["scope"]
        description = code_metadata["description"]
        category = code_metadata["category"]
        min_sample = code_metadata["min_sample"]
    else:
        if not key:
            return jsonify({"ok": False, "error": "key is required"}), 400
        if not name or not scope:
            return jsonify({"ok": False, "error": "name and scope are required"}), 400
        description = body.get("description", "")
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
            description=description,
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
            description=description,
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
    denied = _require_pro_json()
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
        rows = base_q.order_by(Game.game_date.desc()).offset((page - 1) * page_size).limit(page_size).all()
        team_map = _team_map(session)

        # Batch-load player/team stats for these games
        game_ids = [game.game_id for _, game in rows]
        entity_type = MetricRunLog.entity_type

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
                "delta": json.loads(log.delta_json) if log.delta_json else None,
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
                )
            except Exception as exc:
                return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400
            code_python = code_metadata["code_python"]

        metadata_fields = {"code", "definition", "name", "description", "scope", "category", "min_sample", "group_key", "expression", "rank_order"}
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
                    )
                    source_code = code_metadata["code_python"]
                source_definition = None
                name = body.get("name") or code_metadata["name"]
                description = body.get("description") if body.get("description") is not None else code_metadata["description"]
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
                description = body["description"] if body.get("description") is not None else (getattr(m, "description", "") or "")
                scope = body.get("scope", getattr(m, "scope", "player"))
                category = body.get("category", getattr(m, "category", "") or "")
                min_sample = int(body.get("min_sample", getattr(m, "min_sample", 1) or 1))

            now = datetime.utcnow()
            _sync_metric_family(
                session,
                m,
                source_type=source_type,
                name=name,
                description=description,
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
        p.player_id: (p.full_name, bool(p.is_active))
        for p in session.query(Player.player_id, Player.full_name, Player.is_active).filter(Player.player_id.in_(player_ids)).all()
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
        if entity_type == "player":
            return player_names.get(entity_id) or entity_id
        if entity_type == "player_franchise" and entity_id and ":" in entity_id:
            player_id, franchise_id = entity_id.split(":", 1)
            player_name = player_names.get(player_id) or player_id
            franchise_name = _team_name(team_map, franchise_id)
            return f"{player_name} — {franchise_name}"
        if entity_type == "team":
            t = team_map.get(entity_id)
            return (t.full_name or t.abbr) if t else entity_id
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

        metric_def = _metric_def_view(
            runtime_metric or db_metric,
            source_type=getattr(db_metric, "source_type", None),
        )
        is_career_metric = bool(getattr(runtime_metric, "career", False))
        related_metrics = _related_metric_links(session, metric_key, runtime_metric, db_metric)

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
        else:
            season_options = sorted(
                [s for s in season_values if not is_career_season(s) and s != CAREER_SEASON],
                key=_season_sort_key,
                reverse=True,
            )
            if not is_pro():
                _cur = _pick_current_season(season_options)
                if _cur:
                    season_options = [_cur]
                    show_all_seasons = False
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
                        "type_name": _SEASON_TYPE_NAMES.get(type_code, type_code),
                        "type_name_plural": _SEASON_TYPE_PLURAL.get(type_code, type_code),
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
                .filter(Player.full_name.ilike(f"%{search_q}%")).all()
            ]
            matching_team_ids = [
                r[0] for r in session.query(Team.team_id)
                .filter(Team.full_name.ilike(f"%{search_q}%")).all()
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
        scope_label = {"player": "players", "player_franchise": "franchise stints", "team": "teams", "game": "results"}.get(
            metric_def.scope, "entities"
        )
        if is_career_metric:
            period = "across all seasons"
        elif show_all_seasons:
            _type_name = _SEASON_TYPE_NAMES.get(all_season_type, "").lower()
            period = f"across all {_type_name} seasons" if _type_name else "across all seasons"
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
        has_drilldown = (
            session.query(MetricRunLog.game_id)
            .filter(MetricRunLog.metric_key == metric_key, MetricRunLog.qualified == True)
            .limit(1)
            .first()
        ) is not None

    if is_career_metric:
        display_season_label = "Career"
    elif show_all_seasons:
        _type_name = _SEASON_TYPE_PLURAL.get(all_season_type, "Seasons")
        display_season_label = f"All {_type_name}"
    else:
        display_season_label = _season_label(selected_season)
    is_player_scope = metric_def.scope in ("player", "player_franchise")
    return render_template(
        "metric_detail.html",
        metric_def=metric_def,
            result_rows=result_rows,
            show_rank_group=show_rank_group,
            is_player_scope=is_player_scope,
            active_only=active_only,
        season_options=season_options,
        season_groups=season_groups,
        selected_season=selected_season,
        show_all_seasons=show_all_seasons,
        all_season_type=all_season_type,
        is_career_metric=is_career_metric,
        related_metrics=related_metrics,
        season_label=display_season_label,
        fmt_season=_season_label,
        fmt_season_short=_season_year_label,
        page=page,
        total_pages=total_pages,
        total=total,
        page_size=page_size,
        backfill=backfill,
        has_drilldown=has_drilldown,
        search_q=search_q,
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




@app.get("/admin")
def admin_pipeline():
    denied = _require_admin_page()
    if denied:
        return denied
    return render_template(
        "admin.html",
        admin_page_url=_admin_page_url,
        admin_fragment_url=_admin_fragment_url,
        llm_available_models=available_llm_models(),
    )


@app.get("/admin/topics")
def admin_topics():
    denied = _require_admin_page()
    if denied:
        return denied
    import json as _json
    status_filter = request.args.get("status")
    page = max(1, request.args.get("page", 1, type=int))
    page_size = 30

    with SessionLocal() as session:
        q = session.query(TopicPost).order_by(TopicPost.date.desc(), TopicPost.priority.asc())
        if status_filter and status_filter in ("draft", "approved", "sent", "archived"):
            q = q.filter(TopicPost.status == status_filter)
        total = q.count()
        import math
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        topics = q.offset((page - 1) * page_size).limit(page_size).all()

        topic_rows = []
        for t in topics:
            topic_rows.append({
                "id": t.id,
                "date": t.date.isoformat() if t.date else "",
                "title": t.title,
                "body": t.body,
                "priority": t.priority,
                "status": t.status,
                "source_metric_keys": _json.loads(t.source_metric_keys) if t.source_metric_keys else [],
                "source_game_ids": _json.loads(t.source_game_ids) if t.source_game_ids else [],
                "llm_model": t.llm_model,
                "created_at": t.created_at,
            })

    from datetime import date as _date, timedelta as _td
    yesterday = (_date.today() - _td(days=1)).isoformat()
    return render_template(
        "admin_topics.html",
        topics=topic_rows,
        page=page,
        total_pages=total_pages,
        total=total,
        status_filter=status_filter or "all",
        today=yesterday,
    )


@app.post("/admin/topics/<int:topic_id>/update")
def admin_topic_update(topic_id: int):
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime
    data = request.get_json(force=True) or {}
    with SessionLocal() as session:
        topic = session.query(TopicPost).filter(TopicPost.id == topic_id).first()
        if not topic:
            return jsonify({"error": "not_found"}), 404
        if "title" in data:
            topic.title = data["title"]
        if "body" in data:
            topic.body = data["body"]
        if "status" in data and data["status"] in ("draft", "approved", "sent", "archived"):
            topic.status = data["status"]
        if "priority" in data:
            topic.priority = int(data["priority"])
        topic.updated_at = datetime.utcnow()
        session.commit()
    return jsonify({"ok": True})


@app.post("/admin/topics/<int:topic_id>/delete")
def admin_topic_delete(topic_id: int):
    denied = _require_admin_json()
    if denied:
        return denied
    with SessionLocal() as session:
        session.query(TopicPost).filter(TopicPost.id == topic_id).delete()
        session.commit()
    return jsonify({"ok": True})


@app.post("/admin/topics/generate")
def admin_topics_generate():
    """Trigger topic generation for a specific date (runs inline, not via Celery)."""
    denied = _require_admin_json()
    if denied:
        return denied
    data = request.get_json(force=True) or {}
    target_date = data.get("date")
    if not target_date:
        return jsonify({"ok": False, "error": "date is required"}), 400
    force = bool(data.get("force", False))
    try:
        from tasks.topics import generate_daily_topics
        result = generate_daily_topics(target_date, force=force)
        return jsonify({"ok": True, **result})
    except Exception as exc:
        logger.exception("Topic generation failed for %s", target_date)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.get("/admin/fragment/<section>")
def admin_fragment(section: str):
    denied = _require_admin_page()
    if denied:
        return denied

    section = (section or "").strip().lower()
    runs_page_size = 25
    recent_page_size = 25

    with SessionLocal() as session:
        if section == "visitor-stats":
            from datetime import datetime, timedelta
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

        if section == "coverage":
            now = time.time()
            if "coverage" not in _admin_cache or now - _admin_cache.get("ts", 0) > _ADMIN_CACHE_TTL:
                from sqlalchemy import text as sa_text
                coverage_rows = session.execute(sa_text("""
                    SELECT
                        g.season,
                        COUNT(DISTINCT g.game_id)   AS total,
                        COUNT(DISTINCT pgs.game_id) AS has_detail,
                        COUNT(DISTINCT pbp.game_id) AS has_pbp,
                        COUNT(DISTINCT gls.game_id) AS has_line,
                        COUNT(DISTINCT sr.game_id)  AS has_shot,
                        COALESCE(SUM(mrl_agg.metric_cnt > 0), 0) AS has_metrics,
                        0                                        AS active_claims
                    FROM Game g
                    LEFT JOIN (SELECT DISTINCT game_id FROM PlayerGameStats) pgs ON pgs.game_id = g.game_id
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
                _admin_cache["coverage"] = coverage_rows
                _admin_cache["ts"] = now
            else:
                coverage_rows = _admin_cache["coverage"]
            coverage = [
                {
                    "season": _season_label(row.season),
                    "season_raw": row.season,
                    "total": row.total,
                    "detail": row.has_detail,
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
