from __future__ import annotations

from collections import defaultdict
from datetime import date
import logging
import os
import time
from types import SimpleNamespace

logger = logging.getLogger(__name__)

import uuid as _uuid_mod

from flask import Flask, abort, after_this_request, flash, get_flashed_messages, jsonify, make_response, redirect, render_template, request, session, url_for
from authlib.integrations.flask_client import OAuth
from sqlalchemy import and_, case, func, or_, text
from sqlalchemy.orm import sessionmaker

from db.models import Feedback, Game, GamePlayByPlay, MetricJobClaim, MetricDefinition as MetricDefinitionModel, MetricResult as MetricResultModel, MetricRunLog, PageView, Player, PlayerGameStats, ShotRecord, Team, TeamGameStats, User, engine
from db.backfill_nba_player_shot_detail import back_fill_game_shot_record_from_api

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32))
SessionLocal = sessionmaker(bind=engine)

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


def _metric_def_view(metric_def, *, status: str | None = None, source_type: str | None = None):
    """Normalize built-in and DB metric objects for template rendering."""
    return SimpleNamespace(
        key=metric_def.key,
        name=metric_def.name,
        description=getattr(metric_def, "description", "") or "",
        scope=metric_def.scope,
        category=getattr(metric_def, "category", "") or "",
        status=status or getattr(metric_def, "status", "published"),
        source_type=source_type or getattr(metric_def, "source_type", "builtin"),
        min_sample=int(getattr(metric_def, "min_sample", 1) or 1),
        group_key=getattr(metric_def, "group_key", None),
        career=bool(getattr(metric_def, "career", False)),
        supports_career=bool(getattr(metric_def, "supports_career", False)),
    )


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


def _catalog_metrics(session, scope_filter: str = "", status_filter: str = "") -> list[dict]:
    from metrics.framework.registry import get_all as _get_all_metrics

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
    if scope_filter:
        if scope_filter == "player":
            db_q = db_q.filter(MetricDefinitionModel.scope.in_(["player", "player_franchise"]))
        else:
            db_q = db_q.filter(MetricDefinitionModel.scope == scope_filter)
    if status_filter:
        db_q = db_q.filter(MetricDefinitionModel.status == status_filter)
    db_metrics = [
        {
            "key": m.key,
            "name": m.name,
            "description": m.description,
            "scope": m.scope,
            "category": m.category or "",
            "status": m.status,
            "source_type": m.source_type,
            "result_count": counts.get(m.key, 0),
        }
        for m in db_q.order_by(MetricDefinitionModel.created_at.desc()).all()
    ]

    db_keys = {m["key"] for m in db_metrics}
    include_builtins = not status_filter or status_filter == "published"
    builtin_metrics = [
        {
            "key": m.key,
            "name": m.name,
            "description": m.description,
            "scope": m.scope,
            "category": m.category,
            "status": "published",
            "source_type": "builtin",
            "result_count": counts.get(m.key, 0),
        }
        for m in _get_all_metrics()
        if include_builtins
        and m.key not in db_keys
        and (
            not scope_filter
            or m.scope == scope_filter
            or (scope_filter == "player" and m.scope == "player_franchise")
        )
    ]

    return builtin_metrics + db_metrics


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


_METRIC_CONTEXT_LABEL: dict = {
    "clutch_fg_pct":          lambda c: f"{_fmt_int(c.get('clutch_made'))}/{_fmt_int(c.get('clutch_attempts'))} clutch",
    "hot_hand":               lambda c: f"{_fmt_int(c.get('hot_made'))}/{_fmt_int(c.get('hot_opps'))} after make",
    "cold_streak_recovery":   lambda c: f"{_fmt_int(c.get('cold_made'))}/{_fmt_int(c.get('cold_opps'))} after miss",
    "double_double_rate":     lambda c: f"{_fmt_int(c.get('dd_count'))}/{_fmt_int(c.get('games_played'))} games",
    "scoring_consistency":    lambda c: f"{_fmt_int(c.get('games_20_plus'))}/{_fmt_int(c.get('games_played'))} games",
    "true_shooting_pct":      lambda c: f"{_fmt_int(c.get('pts'))} pts / {_fmt_int(c.get('fga'))} FGA + {_fmt_int(c.get('fta'))} FTA",
    "assist_to_turnover_ratio": lambda c: f"{_fmt_int(c.get('ast'))} ast / {_fmt_int(c.get('tov'))} tov",
    "paint_scoring_share":    lambda c: f"{_fmt_int(c.get('paint_shots'))}/{_fmt_int(c.get('total_shots'))} shots",
    "bench_scoring_share":    lambda c: f"{_fmt_int(c.get('bench_pts'))}/{_fmt_int(c.get('total_pts'))} pts",
    "blowout_rate":           lambda c: f"{_fmt_int(c.get('blowout_wins'))}/{_fmt_int(c.get('total_games'))} games",
    "comeback_win_pct":       lambda c: f"{_fmt_int(c.get('trailing_wins'))}/{_fmt_int(c.get('trailing_total'))} trailing",
    "win_pct_leading_at_half": lambda c: f"{_fmt_int(c.get('leading_wins'))}/{_fmt_int(c.get('leading_total'))} at half",
    "road_win_pct":           lambda c: f"{_fmt_int(c.get('road_wins'))}/{_fmt_int(c.get('road_games'))} road",
    "home_court_advantage":   lambda c: f"home {_fmt_int(c.get('home_wins'))}/{_fmt_int(c.get('home_games'))} · road {_fmt_int(c.get('road_wins'))}/{_fmt_int(c.get('road_games'))}",
}


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


def _get_metric_results(session, entity_type: str, entity_id: str, season: str | None = None) -> dict:
    """Fetch metric results for an entity, split into season and alltime lists.

    Returns {"season": [...], "alltime": [...]} each sorted by rank asc (best first).
    Rank and total are derived at query time via SQL window functions.
    """
    import json
    from sqlalchemy import func
    from metrics.framework.base import CAREER_SEASON
    from metrics.framework.registry import get_asc_metric_keys

    _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
    _asc_keys = get_asc_metric_keys()
    scope_label = {"player": "players", "team": "teams", "game": "games"}.get(entity_type, "entities")

    # Inner subquery: compute rank and total over the full population for
    # each (metric_key, season) group, filtered to this entity_type.
    season_filter = (
        (MetricResultModel.season == season) | (MetricResultModel.season == CAREER_SEASON)
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

    rows = (
        session.query(inner_q)
        .filter(inner_q.c.entity_id == entity_id)
        .order_by(inner_q.c.rank.asc())
        .all()
    )

    team_map = _team_map(session)

    season_metrics = []
    alltime_metrics = []
    for r in rows:
        ctx = json.loads(r.context_json) if r.context_json else {}
        rank_group_label = _team_name(team_map, r.rank_group) if r.rank_group else None
        base_key = r.metric_key.removesuffix("_career")
        label_fn = _METRIC_CONTEXT_LABEL.get(base_key)
        context_label = None
        if label_fn:
            try:
                context_label = label_fn(ctx)
            except Exception:
                pass
        rank, total = r.rank, r.total
        is_notable = total > 0 and rank / total <= 0.25
        entry = {
            "metric_key": r.metric_key,
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
        if r.metric_key.endswith("_career") or r.season == CAREER_SEASON:
            alltime_metrics.append(entry)
        else:
            season_metrics.append(entry)

    # Attach career rank to each season entry so cards can show both at once
    career_by_base = {
        e["metric_key"].removesuffix("_career"): e for e in alltime_metrics
    }
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
        _ag_rank_value = case(
            (MetricResultModel.metric_key.in_(_asc_keys), -MetricResultModel.value_num),
            else_=MetricResultModel.value_num,
        )
        ag_inner = (
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
            .subquery()
        )
        ag_rows = (
            session.query(ag_inner)
            .filter(ag_inner.c.entity_id == entity_id)
            .all()
        )
        ag_by_key = {r.metric_key: (r.rank, r.total) for r in ag_rows}
        for entry in season_metrics:
            ag_rank, ag_total = ag_by_key.get(entry["metric_key"], (None, None))
            if ag_rank is not None:
                entry["all_games_rank"] = ag_rank
                entry["all_games_total"] = ag_total
                entry["all_games_is_notable"] = ag_total > 0 and ag_rank / ag_total <= 0.25

        # Mark hero metrics (top 1% of all games) and sort by exceptionality
        _apply_game_metric_tiers(season_metrics)

    return {"season": season_metrics, "alltime": alltime_metrics}


def _metric_backfill_component(session, metric_key: str, total_games: int) -> dict:
    from sqlalchemy import desc, func

    done_games = (
        session.query(func.count())
        .select_from(MetricJobClaim)
        .filter(
            MetricJobClaim.metric_key == metric_key,
            MetricJobClaim.status == "done",
        )
        .scalar() or 0
    )
    active_games = (
        session.query(func.count())
        .select_from(MetricJobClaim)
        .filter(
            MetricJobClaim.metric_key == metric_key,
            MetricJobClaim.status == "in_progress",
        )
        .scalar() or 0
    )
    latest_run_at = (
        session.query(MetricRunLog.computed_at)
        .filter(MetricRunLog.metric_key == metric_key)
        .order_by(desc(MetricRunLog.computed_at))
        .limit(1)
        .scalar()
    )

    if total_games and done_games >= total_games:
        status = "complete"
    elif active_games > 0 or done_games > 0:
        status = "running"
    else:
        status = "not_started"

    return {
        "metric_key": metric_key,
        "status": status,
        "done_games": int(done_games),
        "active_games": int(active_games),
        "pending_games": max(int(total_games) - int(done_games) - int(active_games), 0),
        "total_games": int(total_games),
        "progress_pct": round((int(done_games) / int(total_games) * 100.0), 1) if total_games else 0.0,
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
    elif any(c["status"] == "running" for c in components):
        status = "running"
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

    return {
        "status": status,
        "total_games": total_jobs,
        "done_games": done_jobs,
        "active_games": active_jobs,
        "pending_games": max(total_jobs - done_jobs - active_jobs, 0),
        "progress_pct": round((done_jobs / total_jobs * 100.0), 1) if total_jobs else 0.0,
        "latest_run_at": latest_run_at,
        "components": components,
        "is_multi_component": len(components) > 1,
    }


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
        return None


@app.context_processor
def inject_template_helpers():
    from datetime import date
    return {
        "season_label": _season_label,
        "is_admin": is_admin(),
        "current_user": _current_user(),
        "current_year": date.today().year,
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
    """Redirect to Google OAuth consent screen."""
    if not os.environ.get("GOOGLE_CLIENT_ID") or os.environ.get("GOOGLE_CLIENT_ID", "").startswith("REPLACE_"):
        flash("Sign-in is not configured on this server.", "error")
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


@app.route("/auth/logout", methods=["POST"])
def auth_logout():
    """Clear session and redirect to home."""
    session.pop("user_id", None)
    return redirect(url_for("home"))


# ── Feedback routes ───────────────────────────────────────────────────────────

@app.post("/feedback")
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
        try:
            page = max(1, int(request.args.get("page", 1)))
        except ValueError:
            page = 1

        games_q = session.query(Game).filter(Game.game_date.isnot(None))
        if selected_season:
            games_q = games_q.filter(Game.season == selected_season)
        games_q = games_q.order_by(Game.game_date.desc(), Game.game_id.desc())

        total = games_q.count()
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = min(page, total_pages)
        games = games_q.offset((page - 1) * PAGE_SIZE).limit(PAGE_SIZE).all()

        team_lookup = _team_map(session)

    return render_template(
        "games_list.html",
        games=games,
        team_lookup=team_lookup,
        all_season_ids=all_season_ids,
        selected_season=selected_season,
        fmt_date=_fmt_date,
        fmt_season=_season_label,
        page=page,
        total_pages=total_pages,
        total=total,
    )


@app.route("/api/players/hints")
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


@app.route("/players/<player_id>")
def player_page(player_id: str):
    with SessionLocal() as session:
        player = session.query(Player).filter(Player.player_id == player_id).first()
        if player is None:
            abort(404, description=f"Player {player_id} not found")

        played_condition = (func.coalesce(PlayerGameStats.min, 0) > 0) | (func.coalesce(PlayerGameStats.sec, 0) > 0)
        selected_career_kind = request.args.get("career_kind", "regular")
        if selected_career_kind not in {"regular", "playoffs"}:
            selected_career_kind = "regular"
        season_prefix = "2" if selected_career_kind == "regular" else "4"
        career_kind_label = "Regular Season" if selected_career_kind == "regular" else "Playoffs"

        def _summary_fields():
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

        def _to_summary(raw_row) -> dict[str, str | int]:
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

        season_rows_raw = (
            session.query(
                Game.season.label("season"),
                *_summary_fields(),
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
            season_stats = _to_summary(row)
            career_season_rows.append(
                {
                    "season": row.season,
                    "stats": season_stats,
                }
            )
        career_season_rows.sort(key=lambda row: _season_sort_key(row["season"]), reverse=True)

        teams = _team_map(session)

        # Attach distinct team abbreviations to each season row
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

        overall_row = (
            session.query(*_summary_fields())
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(
                PlayerGameStats.player_id == player_id,
                Game.season.like(f"{season_prefix}%"),
            )
            .one()
        )
        career_overall = _to_summary(overall_row)

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
    )


@app.route("/teams/<team_id>")
def team_page(team_id: str):
    with SessionLocal() as session:
        team = session.query(Team).filter(Team.team_id == team_id).first()
        if team is None:
            abort(404, description=f"Team {team_id} not found")

        season_summary_rows = (
            session.query(
                Game.season.label("season"),
                func.sum(case((TeamGameStats.win.is_(True), 1), else_=0)).label("wins"),
                func.sum(case((TeamGameStats.win.is_(False), 1), else_=0)).label("losses"),
                func.count(TeamGameStats.game_id).label("games"),
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
    )


@app.route("/games/<game_id>")
def game_page(game_id: str):
    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            abort(404, description=f"Game {game_id} not found")

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
                "event_type": row.event_msg_type if row.event_msg_type is not None else "-",
                "description": _pbp_text(row) or "-",
                "score": row.score or "-",
                "margin": row.score_margin or "-",
            }
            for row in pbp_rows_raw
        ]

        # Player name map from already-fetched player_rows (no extra query)
        _player_name_map = {
            str(stat.player_id): (player.full_name if player and player.full_name else str(stat.player_id))
            for stat, player in player_rows
        }

        # Build score progression for the line chart.
        # NBA PBP score format: "HOME - VISITOR" (home team is first number)
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

        # Derive per-period point totals from PBP (last score seen each period)
        # Reorder by (period, clock DESC) to handle misplaced events in legacy data
        def _clock_secs(pc_time):
            if not pc_time:
                return 0
            try:
                parts = pc_time.split(":")
                return int(parts[0]) * 60 + int(parts[1])
            except (ValueError, IndexError):
                return 0

        _score_rows = [(r.period, r.score, r.pc_time, r.event_num) for r in pbp_rows_raw if r.score and r.period is not None]
        _score_rows.sort(key=lambda r: (r[0] or 0, -_clock_secs(r[2]), r[3] or 0))

        _period_end: dict[int, tuple[int, int]] = {}
        for _period, _score, _, _ in _score_rows:
            try:
                _parts = _score.split("-")
                if len(_parts) != 2:
                    continue
                _h, _r = int(_parts[0].strip()), int(_parts[1].strip())
                _period_end[int(_period)] = (_h, _r)
            except (ValueError, AttributeError):
                continue
        quarter_scores: list[dict] = []
        _ph = _pr = 0
        for _p in sorted(_period_end.keys()):
            _eh, _er = _period_end[_p]
            quarter_scores.append({"period": _p, "home": _eh - _ph, "road": _er - _pr})
            _ph, _pr = _eh, _er

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

        game_metrics = _get_metric_results(session, "game", game_id, game.season)

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
        game_metrics=game_metrics,
        score_progression_json=score_progression_json,
        road_abbr=road_abbr,
        home_abbr=home_abbr,
        quarter_scores=quarter_scores,
        home_team_id=home_team_id,
    )


@app.route("/metrics")
def metrics_browse():
    scope_filter = request.args.get("scope", "")
    status_filter = request.args.get("status", "")  # draft | published | ""
    search_query = request.args.get("q", "").strip()

    with SessionLocal() as session:
        metrics_list = _catalog_metrics(session, scope_filter=scope_filter, status_filter=status_filter)

    return render_template(
        "metrics.html",
        metrics_list=metrics_list,
        scope_filter=scope_filter,
        status_filter=status_filter,
        search_query=search_query,
    )


@app.route("/metrics/new")
def metric_new():
    denied = _require_admin_page()
    if denied:
        return denied
    all_seasons = sorted(
        [r[0] for r in SessionLocal().query(Game.season).distinct().all()],
        reverse=True,
    )
    current_season = _pick_current_season(all_seasons)
    initial_expression = request.args.get("expression", "").strip()
    return render_template(
        "metric_new.html",
        current_season=current_season,
        all_seasons=all_seasons,
        initial_expression=initial_expression,
    )


@app.post("/api/metrics/search")
def api_metric_search():
    from metrics.framework.search import rank_metrics

    body = request.get_json(force=True) or {}
    query = (body.get("query") or "").strip()
    scope_filter = (body.get("scope") or "").strip()
    status_filter = (body.get("status") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "query is required"}), 400

    with SessionLocal() as session:
        catalog = _catalog_metrics(session, scope_filter=scope_filter, status_filter=status_filter)

    try:
        ranked = rank_metrics(
            query,
            [
                {
                    "key": metric["key"],
                    "name": metric["name"],
                    "description": metric["description"],
                    "scope": metric["scope"],
                    "category": metric["category"],
                    "status": metric["status"],
                    "source_type": metric["source_type"],
                }
                for metric in catalog
            ],
            limit=8,
        )
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


@app.post("/api/metrics/generate")
def api_metric_generate():
    denied = _require_admin_json()
    if denied:
        return denied
    from metrics.framework.generator import generate
    body = request.get_json(force=True) or {}
    expression = body.get("expression", "").strip()
    history = body.get("history")  # list of {"role", "content"} or None
    if not expression:
        return jsonify({"ok": False, "error": "expression is required"}), 400
    try:
        spec = generate(expression, history=history)
        return jsonify({"ok": True, "spec": spec})
    except Exception as exc:
        logger.exception("metric generate failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/metrics/preview")
def api_metric_preview():
    body = request.get_json(force=True) or {}
    definition = body.get("definition")
    code_python = (body.get("code") or "").strip()
    scope = body.get("scope", "player")
    season = body.get("season", "")

    if not definition and not code_python:
        return jsonify({"ok": False, "error": "definition or code is required"}), 400

    with SessionLocal() as session:
        try:
            if code_python:
                rows = _preview_code_metric(session, code_python, scope, season, limit=25)
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
            names = _resolve_game_entity_names(session, entity_ids)
        else:
            names = {}

        for r in rows:
            r["entity_name"] = names.get(r["entity_id"], r.get("value_str") or r["entity_id"])

    return jsonify({"ok": True, "rows": rows})


def _resolve_game_entity_names(session, entity_ids: list[str]) -> dict[str, str]:
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
    for eid in entity_ids:
        parts = eid.split(":")
        gid = parts[0]
        game = games.get(gid)
        if not game:
            names[eid] = eid
            continue

        home_abbr = _team_name(tm, game.home_team_id) if game.home_team_id else "?"
        road_abbr = _team_name(tm, game.road_team_id) if game.road_team_id else "?"
        date_str = game.game_date.strftime("%b %d") if game.game_date else ""

        if len(parts) == 1:
            # Simple game ID
            h_score = game.home_team_score or 0
            r_score = game.road_team_score or 0
            names[eid] = f"{home_abbr} {h_score} - {road_abbr} {r_score} ({date_str})"
        else:
            # Composite: game_id:team_id:qualifier
            team_id = parts[1] if len(parts) > 1 else None
            qualifier = parts[2] if len(parts) > 2 else ""
            team_abbr = _team_name(tm, team_id) if team_id else "?"
            names[eid] = f"{team_abbr} {qualifier} — {home_abbr} vs {road_abbr} {date_str}"

    return names


def _preview_code_metric(session, code_python: str, scope: str, season: str, limit: int = 25):
    """Run a code-based metric against sample games and return ranked results."""
    from metrics.framework.runtime import load_code_metric

    metric = load_code_metric(code_python)
    rank_order = getattr(metric, "rank_order", "desc")

    if scope == "game":
        # For game-scope, run against recent games
        game_q = session.query(Game.game_id, Game.season).filter(Game.home_team_score.isnot(None))
        if season and season != "all":
            game_q = game_q.filter(Game.season == season)
        game_rows = game_q.order_by(Game.game_date.desc()).limit(500).all()

        rows = []
        for gr in game_rows:
            try:
                result = metric.compute(session, gr.game_id, gr.season, gr.game_id)
            except Exception:
                continue
            if not result:
                continue
            result_list = result if isinstance(result, list) else [result]
            for r in result_list:
                if r.value_num is not None:
                    rows.append({
                        "entity_id": r.entity_id,
                        "value_num": round(r.value_num, 4),
                        "value_str": r.value_str,
                        "baseline": None,
                    })
        rows.sort(key=lambda r: r["value_num"], reverse=(rank_order == "desc"))
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
                        delta = metric.compute_delta(session, tid, gid)
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
                    rows.append({"entity_id": tid, "value_num": round(result.value_num, 4),
                                 "value_str": result.value_str, "baseline": None})
        else:
            rows = []
            for tid in team_ids:
                try:
                    result = metric.compute(session, tid, season)
                except Exception:
                    continue
                if result and result.value_num is not None:
                    rows.append({"entity_id": tid, "value_num": round(result.value_num, 4),
                                 "value_str": result.value_str, "baseline": None})
        rows.sort(key=lambda r: r["value_num"], reverse=(rank_order == "desc"))
        return rows[:limit]

    elif scope == "player":
        # For player scope, sample recent games and run incrementally
        game_q = session.query(Game.game_id).filter(Game.home_team_score.isnot(None))
        if season and season != "all":
            game_q = game_q.filter(Game.season == season)
        game_ids = [r.game_id for r in game_q.order_by(Game.game_date.asc()).all()]
        if metric.incremental:
            from metrics.framework.base import merge_totals
            accum: dict[str, dict] = {}
            for gid in game_ids:
                player_ids = [r.player_id for r in session.query(PlayerGameStats.player_id)
                    .filter(PlayerGameStats.game_id == gid).distinct().all()]
                for pid in player_ids:
                    try:
                        delta = metric.compute_delta(session, pid, gid)
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
                    rows.append({"entity_id": pid, "value_num": round(result.value_num, 4),
                                 "value_str": result.value_str, "baseline": None})
        else:
            rows = []
        rows.sort(key=lambda r: r["value_num"], reverse=(rank_order == "desc"))
        return rows[:limit]

    return []


@app.post("/api/metrics")
def api_metric_create():
    denied = _require_admin_json()
    if denied:
        return denied
    import json as _json
    from datetime import datetime
    from metrics.framework.registry import get as _get_builtin_metric
    body = request.get_json(force=True) or {}

    key = (body.get("key") or "").strip().lower().replace(" ", "_")
    name = (body.get("name") or "").strip()
    scope = (body.get("scope") or "").strip()
    code_python = (body.get("code") or "").strip()
    definition = body.get("definition")

    if not key or not name or not scope:
        return jsonify({"ok": False, "error": "key, name, and scope are required"}), 400
    if not code_python and not definition:
        return jsonify({"ok": False, "error": "code or definition is required"}), 400

    # Determine source type
    source_type = "code" if code_python else "rule"

    # Validate code by loading it
    if code_python:
        try:
            from metrics.framework.runtime import load_code_metric
            load_code_metric(code_python)
        except Exception as exc:
            return jsonify({"ok": False, "error": f"Code validation failed: {exc}"}), 400

    with SessionLocal() as session:
        if _get_builtin_metric(key) is not None:
            return jsonify({"ok": False, "error": f"Key '{key}' conflicts with a built-in metric"}), 409
        if session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == key).first():
            return jsonify({"ok": False, "error": f"Key '{key}' already exists"}), 409

        now = datetime.utcnow()
        m = MetricDefinitionModel(
            key=key,
            name=name,
            description=body.get("description", ""),
            scope=scope,
            category=body.get("category", ""),
            group_key=body.get("group_key"),
            source_type=source_type,
            status="draft",
            definition_json=_json.dumps(definition) if definition else None,
            code_python=code_python or None,
            expression=body.get("expression", ""),
            min_sample=int(body.get("min_sample", 1)),
            created_at=now,
            updated_at=now,
        )
        session.add(m)
        session.commit()
        return jsonify({"ok": True, "key": key}), 201


@app.post("/api/metrics/<metric_key>/publish")
def api_metric_publish(metric_key: str):
    denied = _require_admin_json()
    if denied:
        return denied
    from datetime import datetime
    from tasks.ingest import enqueue_metric_backfill

    with SessionLocal() as session:
        m = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
        if m is None:
            return jsonify({"ok": False, "error": "Not found"}), 404
        m.status = "published"
        m.updated_at = datetime.utcnow()
        session.commit()
    # Route the control-plane fanout task to the metrics queue so it doesn't get
    # buried behind long ingest backlogs. The task itself only enqueues ingest
    # jobs; it does not fetch NBA API data.
    enqueue_metric_backfill.apply_async(args=[metric_key], queue="metrics")
    return jsonify({"ok": True, "key": metric_key, "status": "published"})


def _resolve_entity_labels(session, rows):
    """Bulk-resolve entity IDs to human-readable labels. Returns dict keyed by (entity_type, entity_id)."""
    player_ids = {r.entity_id for r in rows if r.entity_type == "player" and r.entity_id}
    player_franchise_pairs = {
        tuple(r.entity_id.split(":", 1))
        for r in rows
        if r.entity_type == "player_franchise" and r.entity_id and ":" in r.entity_id
    }
    player_ids.update({player_id for player_id, _ in player_franchise_pairs})
    team_ids   = {r.entity_id for r in rows if r.entity_type == "team"   and r.entity_id}
    team_ids.update({franchise_id for _, franchise_id in player_franchise_pairs})
    game_ids   = {r.entity_id for r in rows if r.entity_type == "game"   and r.entity_id}

    player_names = {
        p.player_id: p.full_name
        for p in session.query(Player.player_id, Player.full_name).filter(Player.player_id.in_(player_ids)).all()
    } if player_ids else {}
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
        if entity_type == "game" and entity_id in game_info:
            gdate, home_id, road_id = game_info[entity_id]
            return f"{_team_abbr(team_map, road_id)} @ {_team_abbr(team_map, home_id)} ({_fmt_date(gdate)})"
        return entity_id

    return {(r.entity_type, r.entity_id): _label(r.entity_type, r.entity_id) for r in rows}


@app.route("/metrics/<metric_key>")
def metric_detail(metric_key: str):
    import json
    from metrics.framework.base import CAREER_SEASON
    from metrics.framework.registry import get as _get_metric_def

    # Season filter — "all" is the explicit sentinel for cross-season view
    selected_season = request.args.get("season", "")
    show_all_seasons = selected_season == "all"
    page = max(1, int(request.args.get("page", 1) or 1))
    search_q = request.args.get("q", "").strip()
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

        metric_def = _metric_def_view(runtime_metric or db_metric)
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
            selected_season = CAREER_SEASON
            season_options = []
        else:
            season_options = sorted(
                [s for s in season_values if s != CAREER_SEASON],
                key=_season_sort_key,
                reverse=True,
            )
            if not show_all_seasons and not selected_season and season_options:
                selected_season = season_options[0]

        filtered_q = (
            session.query(MetricResultModel)
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.value_num.isnot(None),
            )
        )
        if not show_all_seasons and selected_season:
            filtered_q = filtered_q.filter(MetricResultModel.season == selected_season)

        rank_partition = func.coalesce(MetricResultModel.rank_group, "__all__")
        _mdef = _get_metric_def(metric_key)
        _is_asc = _mdef is not None and _mdef.rank_order == "asc"
        _detail_rank_val = -MetricResultModel.value_num if _is_asc else MetricResultModel.value_num
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
                    partition_by=[MetricResultModel.metric_key, MetricResultModel.season, rank_partition],
                    order_by=_detail_rank_val.desc(),
                ).label("rank"),
                func.count(MetricResultModel.id).over(
                    partition_by=[MetricResultModel.metric_key, MetricResultModel.season, rank_partition],
                ).label("standing_total"),
            )
            .subquery()
        )

        _detail_sort_col = ranked_q.c.value_num.asc() if _is_asc else ranked_q.c.value_num.desc()
        base_rows_q = (
            session.query(ranked_q)
            .order_by(_detail_sort_col, ranked_q.c.entity_id.asc())
        )

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
            total = session.query(func.count()).select_from(ranked_q).scalar() or 0
            total_pages = max(1, math.ceil(total / page_size))
            page = min(page, total_pages)
            offset = (page - 1) * page_size
            rows = base_rows_q.offset(offset).limit(page_size).all()

        labels = _resolve_entity_labels(session, rows)
        team_map = _team_map(session)

        _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
        scope_label = {"player": "players", "player_franchise": "franchise stints", "team": "teams", "game": "games"}.get(
            metric_def.scope, "entities"
        )
        if is_career_metric:
            period = "across all seasons"
        else:
            period = "across all seasons" if show_all_seasons else "this season"
        result_rows = []
        for r in rows:
            ctx = json.loads(r.context_json) if r.context_json else {}
            games_counted = (
                ctx.get("games")
                or ctx.get("total_games")
                or ctx.get("games_leading_at_half")
                or ctx.get("games_trailing_at_half")
                or ctx.get("road_games")
                or ctx.get("home_games")
            )
            rank_group_label = _team_name(team_map, r.rank_group) if r.rank_group else None
            base_key = metric_key.removesuffix("_career")
            label_fn = _METRIC_CONTEXT_LABEL.get(base_key)
            context_label = None
            if label_fn:
                try:
                    context_label = label_fn(ctx)
                except Exception:
                    pass
            rank = int(r.rank or 0)
            standing_total = int(r.standing_total or 0)
            is_notable = standing_total > 0 and rank / standing_total <= 0.25
            label = _RANK_LABELS.get(rank, f"#{rank}")
            group_phrase = f" in {rank_group_label}" if rank_group_label else ""
            notable_reason = f"{label} of {standing_total} {scope_label}{group_phrase} {period}."
            result_rows.append({
                "rank": rank,
                "total": standing_total,
                "entity_type": r.entity_type,
                "entity_id": r.entity_id,
                "entity_label": labels.get((r.entity_type, r.entity_id), r.entity_id),
                "season": _season_label(r.season),
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
            component = _metric_backfill_component(session, key, int(total_games))
            component["label"] = "Career" if key.endswith("_career") else "Season"
            components.append(component)
        backfill = _combine_backfill_components(metric_def, components)

    if is_career_metric:
        display_season_label = "Career"
    else:
        display_season_label = "All Seasons" if show_all_seasons else _season_label(selected_season)
    return render_template(
        "metric_detail.html",
        metric_def=metric_def,
            result_rows=result_rows,
            show_rank_group=show_rank_group,
        season_options=season_options,
        selected_season=selected_season,
        show_all_seasons=show_all_seasons,
        is_career_metric=is_career_metric,
        related_metrics=related_metrics,
        season_label=display_season_label,
        fmt_season=_season_label,
        page=page,
        total_pages=total_pages,
        total=total,
        page_size=page_size,
        backfill=backfill,
        search_q=search_q,
    )


_admin_cache: dict = {}
_ADMIN_CACHE_TTL = 30  # seconds


@app.get("/admin")
def admin_pipeline():
    denied = _require_admin_page()
    if denied:
        return denied
    from datetime import datetime, timedelta

    with SessionLocal() as session:
        # --- Coverage per season — cached to avoid 6s query on every load ---
        now = time.time()
        if "coverage" not in _admin_cache or now - _admin_cache.get("ts", 0) > _ADMIN_CACHE_TTL:
            from sqlalchemy import text as sa_text
            coverage_rows = session.execute(sa_text("""
                SELECT
                    g.season,
                    COUNT(DISTINCT g.game_id)   AS total,
                    COUNT(DISTINCT pgs.game_id) AS has_detail,
                    COUNT(DISTINCT pbp.game_id) AS has_pbp,
                    COUNT(DISTINCT sr.game_id)  AS has_shot,
                    COALESCE(SUM(mjc_agg.done_cnt > 0), 0)   AS has_metrics,
                    COALESCE(SUM(mjc_agg.active_cnt), 0)     AS active_claims
                FROM Game g
                LEFT JOIN (SELECT DISTINCT game_id FROM PlayerGameStats) pgs ON pgs.game_id = g.game_id
                LEFT JOIN (SELECT DISTINCT game_id FROM GamePlayByPlay)  pbp ON pbp.game_id = g.game_id
                LEFT JOIN (SELECT DISTINCT game_id FROM ShotRecord)       sr  ON sr.game_id  = g.game_id
                LEFT JOIN (
                    SELECT game_id,
                           SUM(status = 'done')        AS done_cnt,
                           SUM(status = 'in_progress') AS active_cnt
                    FROM MetricJobClaim
                    GROUP BY game_id
                ) mjc_agg ON mjc_agg.game_id = g.game_id
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
                "shot": row.has_shot,
                "metrics": row.has_metrics,
                "active_claims": row.active_claims,
                "complete": row.total == row.has_detail == row.has_pbp == row.has_shot == row.has_metrics,
            }
            for row in coverage_rows
        ]

        # --- Claim status summary ---
        claim_counts = dict(
            session.query(MetricJobClaim.status, func.count())
            .group_by(MetricJobClaim.status)
            .all()
        )

        # --- Currently in-progress claims (active workers) ---
        active_claims = (
            session.query(MetricJobClaim)
            .filter(MetricJobClaim.status == "in_progress")
            .order_by(MetricJobClaim.claimed_at)
            .limit(50)
            .all()
        )
        active = [
            {
                "game_id": c.game_id,
                "metric_key": c.metric_key,
                "worker_id": c.worker_id,
                "claimed_at": c.claimed_at,
                "age_s": int((datetime.utcnow() - c.claimed_at).total_seconds()) if c.claimed_at else None,
            }
            for c in active_claims
        ]

        # --- Stuck claims (in_progress older than 10 min) ---
        stuck_cutoff = datetime.utcnow() - timedelta(seconds=600)
        stuck_count = (
            session.query(func.count())
            .filter(MetricJobClaim.status == "in_progress", MetricJobClaim.claimed_at < stuck_cutoff)
            .scalar() or 0
        )

        # --- Games missing any artifact (last 2 seasons) — all done in SQL ---
        season_filter = Game.season.like("22024%") | Game.season.like("22025%")

        def _missing(joined_model, joined_col, limit=20):
            """Games in the two target seasons with no row in joined_model.

            Uses limit+1 trick to detect overflow without a separate COUNT query.
            """
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
            # total is exact when small, otherwise ">limit" signal
            total = len(rows) + (1 if overflow else 0)  # caller checks overflow via total > len(rows)
            return {
                "total": total,
                "overflow": overflow,
                "rows": [{"game_id": r.game_id, "game_date": r.game_date, "season": _season_label(r.season)} for r in rows],
            }

        missing_detail = _missing(PlayerGameStats, PlayerGameStats.game_id)
        missing_shot = _missing(ShotRecord, ShotRecord.game_id)
        missing_metrics = _missing(MetricRunLog, MetricRunLog.game_id)

        # --- Recent metric runs (last 20) — filter to recent days to avoid full table scan ---
        recent_runs = (
            session.query(MetricRunLog.game_id, MetricRunLog.metric_key, MetricRunLog.computed_at)
            .filter(MetricRunLog.computed_at >= func.date_sub(func.now(), text("INTERVAL 3 DAY")))
            .order_by(MetricRunLog.computed_at.desc())
            .limit(20)
            .all()
        )
        recent = [{"game_id": r.game_id, "metric_key": r.metric_key, "computed_at": r.computed_at} for r in recent_runs]

        # --- Visitor stats ---
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

    return render_template(
        "admin.html",
        coverage=coverage,
        claim_counts=claim_counts,
        active=active,
        stuck_count=stuck_count,
        missing_detail=missing_detail,
        missing_shot=missing_shot,
        missing_metrics=missing_metrics,
        recent=recent,
        visitor_stats=visitor_stats,
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
