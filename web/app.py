from __future__ import annotations

from collections import defaultdict
from datetime import date
import logging
import os

logger = logging.getLogger(__name__)

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from sqlalchemy import case, func
from sqlalchemy.orm import sessionmaker

from db.models import Game, GamePlayByPlay, MetricDefinition as MetricDefinitionModel, MetricResult as MetricResultModel, Player, PlayerGameStats, ShotRecord, Team, TeamGameStats, engine
from db.backfill_nba_player_shot_detail import back_fill_game_shot_record_from_api

app = Flask(__name__)
SessionLocal = sessionmaker(bind=engine)

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


def _get_metric_results(session, entity_type: str, entity_id: str, season: str | None = None) -> dict:
    """Fetch metric results for an entity, split into season and alltime lists.

    Returns {"season": [...], "alltime": [...]} each sorted by rank asc (best first).
    Rank and total are derived at query time via SQL window functions.
    """
    import json
    from sqlalchemy import func
    from metrics.framework.base import CAREER_SEASON

    _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
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

    inner_q = (
        session.query(
            MetricResultModel.id,
            MetricResultModel.metric_key,
            MetricResultModel.entity_id,
            MetricResultModel.season,
            MetricResultModel.value_num,
            MetricResultModel.value_str,
            MetricResultModel.context_json,
            MetricResultModel.computed_at,
            func.rank().over(
                partition_by=[MetricResultModel.metric_key, MetricResultModel.season],
                order_by=MetricResultModel.value_num.desc(),
            ).label("rank"),
            func.count(MetricResultModel.id).over(
                partition_by=[MetricResultModel.metric_key, MetricResultModel.season],
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

    season_metrics = []
    alltime_metrics = []
    for r in rows:
        ctx = json.loads(r.context_json) if r.context_json else {}
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
            "context": ctx,
            "context_label": context_label,
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
        ag_inner = (
            session.query(
                MetricResultModel.metric_key,
                MetricResultModel.entity_id,
                func.rank().over(
                    partition_by=[MetricResultModel.metric_key],
                    order_by=MetricResultModel.value_num.desc(),
                ).label("rank"),
                func.count(MetricResultModel.id).over(
                    partition_by=[MetricResultModel.metric_key],
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

    return {"season": season_metrics, "alltime": alltime_metrics}


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


@app.context_processor
def inject_template_helpers():
    return {
        "season_label": _season_label,
    }


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
        _period_end: dict[int, tuple[int, int]] = {}  # period -> (home_cumulative, road_cumulative)
        for _row in pbp_rows_raw:
            if not _row.score or _row.period is None:
                continue
            try:
                _parts = _row.score.split("-")
                if len(_parts) != 2:
                    continue
                _h, _r = int(_parts[0].strip()), int(_parts[1].strip())
                _period_end[int(_row.period)] = (_h, _r)
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
    from metrics.framework.registry import get_all as _get_all_metrics
    from sqlalchemy import func as sqlfunc

    scope_filter = request.args.get("scope", "")
    status_filter = request.args.get("status", "")  # draft | published | ""

    with SessionLocal() as session:
        # Counts of computed results per metric_key
        counts = {
            row.metric_key: row.count
            for row in session.query(
                MetricResultModel.metric_key,
                sqlfunc.count(MetricResultModel.id).label("count"),
            ).group_by(MetricResultModel.metric_key).all()
        }

        # User-defined metrics from DB
        db_q = session.query(MetricDefinitionModel).filter(
            MetricDefinitionModel.status != "archived"
        )
        if scope_filter:
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

    # Builtin metrics from registry (not yet in DB)
    db_keys = {m["key"] for m in db_metrics}
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
        if m.key not in db_keys and (not scope_filter or m.scope == scope_filter)
    ]

    metrics_list = builtin_metrics + db_metrics

    return render_template(
        "metrics.html",
        metrics_list=metrics_list,
        scope_filter=scope_filter,
        status_filter=status_filter,
    )


@app.route("/metrics/new")
def metric_new():
    current_season = _pick_current_season(
        [r[0] for r in SessionLocal().query(Game.season).distinct().all()]
    )
    return render_template("metric_new.html", current_season=current_season)


@app.post("/api/metrics/generate")
def api_metric_generate():
    from metrics.framework.generator import generate
    body = request.get_json(force=True)
    expression = (body or {}).get("expression", "").strip()
    if not expression:
        return jsonify({"ok": False, "error": "expression is required"}), 400
    try:
        spec = generate(expression)
        return jsonify({"ok": True, "spec": spec})
    except Exception as exc:
        logger.exception("metric generate failed")
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.post("/api/metrics/preview")
def api_metric_preview():
    from metrics.framework.rule_engine import preview as re_preview
    body = request.get_json(force=True) or {}
    definition = body.get("definition")
    scope = body.get("scope", "player")
    season = body.get("season", "")
    if not definition:
        return jsonify({"ok": False, "error": "definition is required"}), 400

    with SessionLocal() as session:
        # Resolve entity names for results
        try:
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
        else:
            names = {}

        for r in rows:
            r["entity_name"] = names.get(r["entity_id"], r["entity_id"])

    return jsonify({"ok": True, "rows": rows})


@app.post("/api/metrics")
def api_metric_create():
    import json as _json
    from datetime import datetime
    body = request.get_json(force=True) or {}

    required = ("key", "name", "scope", "definition")
    missing = [k for k in required if not body.get(k)]
    if missing:
        return jsonify({"ok": False, "error": f"Missing: {missing}"}), 400

    key = body["key"].strip().lower().replace(" ", "_")

    with SessionLocal() as session:
        if session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == key).first():
            return jsonify({"ok": False, "error": f"Key '{key}' already exists"}), 409

        now = datetime.utcnow()
        m = MetricDefinitionModel(
            key=key,
            name=body["name"],
            description=body.get("description", ""),
            scope=body["scope"],
            category=body.get("category", ""),
            group_key=body.get("group_key"),
            source_type="rule",
            status="draft",
            definition_json=_json.dumps(body["definition"]),
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
    from datetime import datetime
    with SessionLocal() as session:
        m = session.query(MetricDefinitionModel).filter(MetricDefinitionModel.key == metric_key).first()
        if m is None:
            return jsonify({"ok": False, "error": "Not found"}), 404
        m.status = "published"
        m.updated_at = datetime.utcnow()
        session.commit()
    return jsonify({"ok": True, "key": metric_key, "status": "published"})


def _resolve_entity_labels(session, rows):
    """Bulk-resolve entity IDs to human-readable labels. Returns dict keyed by (entity_type, entity_id)."""
    player_ids = {r.entity_id for r in rows if r.entity_type == "player" and r.entity_id}
    team_ids   = {r.entity_id for r in rows if r.entity_type == "team"   and r.entity_id}
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
    from metrics.framework.registry import get as _get_metric

    metric_def = _get_metric(metric_key)
    if metric_def is None:
        abort(404, description=f"Metric '{metric_key}' not found.")

    # Season filter — "all" is the explicit sentinel for cross-season view
    selected_season = request.args.get("season", "")
    show_all_seasons = selected_season == "all"
    page = max(1, int(request.args.get("page", 1) or 1))
    page_size = 50

    with SessionLocal() as session:
        # Available seasons for this metric
        season_rows = (
            session.query(MetricResultModel.season)
            .filter(MetricResultModel.metric_key == metric_key, MetricResultModel.season.isnot(None))
            .distinct()
            .all()
        )
        season_options = sorted(
            [r.season for r in season_rows],
            key=_season_sort_key,
            reverse=True,
        )
        if not show_all_seasons and not selected_season and season_options:
            selected_season = season_options[0]

        q = (
            session.query(MetricResultModel)
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.value_num.isnot(None),
            )
        )
        if not show_all_seasons and selected_season:
            q = q.filter(MetricResultModel.season == selected_season)

        total = q.count()
        import math
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        offset = (page - 1) * page_size

        rows = q.order_by(MetricResultModel.value_num.desc()).offset(offset).limit(page_size).all()

        labels = _resolve_entity_labels(session, rows)

        _RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}
        scope_label = {"player": "players", "team": "teams", "game": "games"}.get(
            metric_def.scope, "entities"
        )
        period = "across all seasons" if show_all_seasons else "this season"
        result_rows = []
        for i, r in enumerate(rows):
            ctx = json.loads(r.context_json) if r.context_json else {}
            games_counted = (
                ctx.get("games")
                or ctx.get("total_games")
                or ctx.get("games_leading_at_half")
                or ctx.get("games_trailing_at_half")
                or ctx.get("road_games")
                or ctx.get("home_games")
            )
            base_key = metric_key.removesuffix("_career")
            label_fn = _METRIC_CONTEXT_LABEL.get(base_key)
            context_label = None
            if label_fn:
                try:
                    context_label = label_fn(ctx)
                except Exception:
                    pass
            rank = offset + i + 1
            is_notable = total > 0 and rank / total <= 0.25
            label = _RANK_LABELS.get(rank, f"#{rank}")
            notable_reason = f"{label} of {total} {scope_label} {period}."
            result_rows.append({
                "rank": rank,
                "total": total,
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
                "games_counted": int(games_counted) if games_counted is not None else None,
            })

    display_season_label = "All Seasons" if show_all_seasons else _season_label(selected_season)
    return render_template(
        "metric_detail.html",
        metric_def=metric_def,
        result_rows=result_rows,
        season_options=season_options,
        selected_season=selected_season,
        show_all_seasons=show_all_seasons,
        season_label=display_season_label,
        fmt_season=_season_label,
        page=page,
        total_pages=total_pages,
        total=total,
        page_size=page_size,
    )


@app.post("/games/<game_id>/shotchart/backfill")
def game_shotchart_backfill(game_id: str):
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
