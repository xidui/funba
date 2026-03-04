from __future__ import annotations

from collections import defaultdict
from datetime import date
import os

from flask import Flask, abort, jsonify, redirect, render_template, request, url_for
from sqlalchemy import case, func
from sqlalchemy.orm import sessionmaker

from db.models import Game, GamePlayByPlay, Player, PlayerGameStats, ShotRecord, Team, TeamGameStats, engine
from db.backfill_nba_player_shot_detail import back_fill_game_shot_record_from_api

app = Flask(__name__)
SessionLocal = sessionmaker(bind=engine)


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
        players = session.query(Player).filter(Player.is_active.is_(True)).order_by(Player.full_name.asc()).limit(20).all()
        recent_games = session.query(Game).order_by(Game.game_date.desc(), Game.game_id.desc()).limit(20).all()
        team_lookup = _team_map(session)

    return render_template(
        "home.html",
        teams=teams,
        players=players,
        recent_games=recent_games,
        team_lookup=team_lookup,
        fmt_date=_fmt_date,
        team_name=_team_name,
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

    return render_template(
        "team.html",
        team=team,
        season_summary=season_summary_view,
        season_kind=season_kind,
        current_season=current_season,
        season_options=season_options,
        selected_games_season=selected_games_season,
        current_games=current_games,
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
