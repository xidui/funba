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


@app.route("/players/<player_id>")
def player_page(player_id: str):
    with SessionLocal() as session:
        player = session.query(Player).filter(Player.player_id == player_id).first()
        if player is None:
            abort(404, description=f"Player {player_id} not found")

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
        teams = _team_map(session)

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
