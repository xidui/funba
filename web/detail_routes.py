from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Callable

from flask import abort, jsonify, request
from sqlalchemy import case, func

from db.game_status import GAME_STATUS_COMPLETED, GAME_STATUS_LIVE, GAME_STATUS_UPCOMING, get_game_status
from web.live_game_data import build_live_game_stub, fetch_live_game_detail, fetch_live_scoreboard_map


def _format_tipoff_et(iso_et: str | None) -> str | None:
    if not iso_et:
        return None
    try:
        text = iso_et.strip()
        if text.endswith("Z"):
            text = text[:-1]
        dt = datetime.fromisoformat(text)
        return dt.strftime("%-I:%M %p ET")
    except Exception:
        return None


def _build_upcoming_preview(session, game, teams, live_summary):
    """Pre-tipoff context for the game detail page.

    Returns a dict with both teams' season W-L, last 10, head-to-head
    series, rest days / B2B flag, and a formatted tipoff time. The
    template renders this in place of the empty box-score panels.
    Pure read; no caching needed (low traffic on upcoming pages).
    """
    from db.models import Game as GameModel

    if not game.home_team_id or not game.road_team_id or not game.game_date:
        return None

    # Pick the latest in-progress regular season as the record source.
    current_season_row = (
        session.query(GameModel.season)
        .filter(GameModel.wining_team_id.isnot(None))
        .filter(GameModel.season.like("2%"))
        .order_by(GameModel.game_date.desc())
        .limit(1)
        .first()
    )
    current_season = current_season_row[0] if current_season_row else None
    if not current_season:
        return None

    team_ids = {game.home_team_id, game.road_team_id}
    past_games = (
        session.query(GameModel)
        .filter(
            GameModel.season == current_season,
            GameModel.wining_team_id.isnot(None),
            (GameModel.home_team_id.in_(team_ids)) | (GameModel.road_team_id.in_(team_ids)),
        )
        .order_by(GameModel.game_date.desc(), GameModel.game_id.desc())
        .all()
    )

    team_games: dict[str, list] = defaultdict(list)
    for pg in past_games:
        if pg.home_team_id in team_ids:
            team_games[pg.home_team_id].append(pg)
        if pg.road_team_id in team_ids:
            team_games[pg.road_team_id].append(pg)

    def _team_block(team_id):
        rows = team_games.get(team_id, [])
        wins = sum(1 for pg in rows if pg.wining_team_id == team_id)
        losses = len(rows) - wins
        last10 = ["W" if pg.wining_team_id == team_id else "L" for pg in reversed(rows[:10])]
        rest = None
        if rows:
            days = (game.game_date - rows[0].game_date).days
            if days > 0:
                rest = {"days": days, "is_b2b": days == 1}
        return {
            "team_id": team_id,
            "wins": wins,
            "losses": losses,
            "last10": last10,
            "rest": rest,
        }

    home_block = _team_block(game.home_team_id)
    road_block = _team_block(game.road_team_id)

    # Head-to-head series this season.
    home_h2h = 0
    road_h2h = 0
    for pg in past_games:
        if {pg.home_team_id, pg.road_team_id} != team_ids:
            continue
        if pg.wining_team_id == game.home_team_id:
            home_h2h += 1
        elif pg.wining_team_id == game.road_team_id:
            road_h2h += 1
    h2h = None
    if home_h2h + road_h2h > 0:
        h2h = {"home_wins": home_h2h, "road_wins": road_h2h}

    tipoff = None
    if live_summary:
        tipoff = _format_tipoff_et(live_summary.get("game_time_et"))

    return {
        "home": home_block,
        "road": road_block,
        "h2h": h2h,
        "tipoff_et": tipoff,
    }


def register_detail_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_render_template: Callable[..., Any],
    get_player_model: Callable[[], Any],
    get_award_model: Callable[[], Any],
    get_game_model: Callable[[], Any],
    get_shot_record_model: Callable[[], Any],
    get_player_game_stats_model: Callable[[], Any],
    get_player_salary_model: Callable[[], Any],
    get_team_model: Callable[[], Any],
    get_team_game_stats_model: Callable[[], Any],
    get_game_pbp_model: Callable[[], Any],
    get_team_map: Callable[[], Callable[[Any], dict[str, Any]]],
    get_award_order_case: Callable[[], Callable[[Any], Any]],
    get_award_badge_label: Callable[[], Callable[[str], str]],
    get_player_career_summary: Callable[[], Callable[..., tuple[dict[str, str | int], list[dict[str, object]]]]],
    get_build_shot_zone_heatmap: Callable[[], Callable[[list[Any]], tuple[list[dict[str, object]], object | None, object | None]]],
    get_season_sort_key: Callable[[], Callable[[str | None], tuple[int, int]]],
    get_season_label: Callable[[], Callable[[str | None], str]],
    get_is_pro: Callable[[], Callable[[], bool]],
    get_pick_current_season: Callable[[], Callable[[list[str]], str | None]],
    get_team_abbr: Callable[[], Callable[[dict[str, Any], str | None], str]],
    get_fmt_date: Callable[[], Callable[[Any], str]],
    get_player_status: Callable[[], Callable[[Any], str]],
    get_fmt_minutes: Callable[[], Callable[[int | None, int | None], str]],
    get_localized_url_for: Callable[[], Callable[..., str]],
    get_metric_results: Callable[[], Callable[[Any, str, str, str | None], dict]],
    get_season_start_year_label: Callable[[], Callable[[int | None], str]],
    get_season_year_label: Callable[[], Callable[[str | None], str]],
    get_coerce_award_season: Callable[[], Callable[[str | int | None], int | None]],
    get_team_name: Callable[[], Callable[[dict[str, Any], str | None], str]],
    get_display_player_name: Callable[[], Callable[[Any], str]],
    get_pbp_event_type_label: Callable[[], Callable[[Any], str]],
    get_pbp_text: Callable[[], Callable[[Any], str]],
    get_paperclip_issue_url: Callable[[], Callable[[str | None], str | None]],
    get_game_analysis_issue_history: Callable[[], Callable[[str], list[Any]]],
    get_player_headshot_url: Callable[[], Callable[[str | None], str | None]],
    get_logger: Callable[[], Any],
):
    def player_page(slug: str):
        SessionLocal = get_session_local()
        Player = get_player_model()
        Award = get_award_model()
        Game = get_game_model()
        ShotRecord = get_shot_record_model()
        PlayerGameStats = get_player_game_stats_model()
        PlayerSalary = get_player_salary_model()

        with SessionLocal() as session:
            player = session.query(Player).filter(Player.slug == slug).first()
            if player is None:
                abort(404, description=f"Player not found")
            player_id = player.player_id

            player_award_rows = (
                session.query(
                    Award.award_type,
                    func.count(Award.id).label("award_count"),
                )
                .filter(Award.player_id == player_id)
                .group_by(Award.award_type)
                .order_by(get_award_order_case()(Award.award_type))
                .all()
            )
            player_awards = [
                {
                    "award_type": row.award_type,
                    "label": get_award_badge_label()(row.award_type),
                    "count": int(row.award_count or 0),
                }
                for row in player_award_rows
            ]

            selected_career_kind = request.args.get("career_kind", "regular")
            if selected_career_kind not in {"regular", "playoffs"}:
                selected_career_kind = "regular"
            season_prefix = "2" if selected_career_kind == "regular" else "4"
            career_kind_label = "Regular Season" if selected_career_kind == "regular" else "Playoffs"

            teams = get_team_map()(session)
            career_overall, career_season_rows = get_player_career_summary()(
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
            heatmap_season_options = sorted([row[0] for row in heatmap_season_rows], key=get_season_sort_key(), reverse=True)
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
            shot_dots = [{"x": row.loc_x, "y": row.loc_y, "made": bool(row.shot_made)} for row in shot_rows]
            shot_attempts = len(shot_dots)
            shot_made_count = sum(1 for dot in shot_dots if dot["made"])
            heatmap_zones, _, _ = get_build_shot_zone_heatmap()(zone_query.all())
            heatmap_scope_label = (
                f"Overall {career_kind_label}"
                if selected_heatmap_season == "overall"
                else get_season_label()(selected_heatmap_season)
            )

            seasons = (
                session.query(Game.season)
                .join(PlayerGameStats, Game.game_id == PlayerGameStats.game_id)
                .filter(PlayerGameStats.player_id == player_id, Game.season.isnot(None))
                .distinct()
                .all()
            )
            season_options = sorted([row[0] for row in seasons], key=get_season_sort_key(), reverse=True)
            if not get_is_pro()():
                current = get_pick_current_season()(season_options)
                if current:
                    season_options = [current]
            selected_season = request.args.get("season")
            if selected_season not in season_options:
                selected_season = get_pick_current_season()(season_options)

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
                        matchup = f"{get_team_abbr()(teams, stat.team_id)} vs {get_team_abbr()(teams, opponent_id)}"
                    else:
                        opponent_id = game.home_team_id
                        matchup = f"{get_team_abbr()(teams, stat.team_id)} @ {get_team_abbr()(teams, opponent_id)}"

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
                            "game_date": get_fmt_date()(game.game_date),
                            "matchup": matchup,
                            "result": result,
                            "status": get_player_status()(stat),
                            "minutes": get_fmt_minutes()(stat.min, stat.sec),
                            "pts": stat.pts if stat.pts is not None else "-",
                            "reb": stat.reb if stat.reb is not None else "-",
                            "ast": stat.ast if stat.ast is not None else "-",
                            "player_team_id": stat.team_id,
                            "player_team_abbr": get_team_abbr()(teams, stat.team_id),
                            "player_team_href": get_localized_url_for()("team_page", slug=teams[stat.team_id].slug) if stat.team_id and stat.team_id in teams and teams[stat.team_id].slug else None,
                            "opponent_id": opponent_id,
                            "opponent_abbr": get_team_abbr()(teams, opponent_id),
                            "opponent_href": get_localized_url_for()("team_page", slug=teams[opponent_id].slug) if opponent_id and opponent_id in teams and teams[opponent_id].slug else None,
                            "is_home": is_home,
                            "player_team_score": player_team_score,
                            "opponent_score": opponent_score,
                        }
                    )

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

            player_current_team = teams.get(player_current_team_id) if player_current_team_id else None

            player_metrics = get_metric_results()(session, "player", player_id, selected_season)
            salary_records = (
                session.query(PlayerSalary)
                .filter(PlayerSalary.player_id == player_id)
                .order_by(PlayerSalary.season.desc())
                .all()
            )
            salary_rows = [
                SimpleNamespace(
                    season=row.season,
                    season_label=get_season_start_year_label()(row.season),
                    salary_usd=row.salary_usd,
                )
                for row in salary_records
            ]

        return get_render_template()(
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
            player_current_team=player_current_team,
            season_options=season_options,
            selected_season=selected_season,
            game_rows=game_rows,
            player_metrics=player_metrics,
            player_awards=player_awards,
            salary_rows=salary_rows,
        )

    def team_page(slug: str):
        SessionLocal = get_session_local()
        Team = get_team_model()
        Award = get_award_model()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()

        with SessionLocal() as session:
            team = session.query(Team).filter(Team.slug == slug).first()
            if team is None:
                abort(404, description=f"Team not found")
            team_id = team.team_id

            canonical_team = None
            if team.canonical_team_id and team.canonical_team_id != team.team_id:
                canonical_team = session.query(Team).filter(Team.team_id == team.canonical_team_id).first()

            championship_rows = (
                session.query(Award.season)
                .filter(Award.award_type == "champion", Award.team_id == team_id)
                .order_by(Award.season.desc())
                .all()
            )
            team_championships = [
                {
                    "season": int(row.season),
                    "season_label": get_season_year_label()(str(row.season)),
                }
                for row in championship_rows
                if get_coerce_award_season()(row.season) is not None
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
                    "fg_pct": f"{int(row.fgm or 0) / int(row.fga or 1):.3f}" if int(row.fga or 0) > 0 else "0.000",
                    "fg3_pct": f"{int(row.fg3m or 0) / int(row.fg3a or 1):.3f}" if int(row.fg3a or 0) > 0 else "0.000",
                    "ft_pct": f"{int(row.ftm or 0) / int(row.fta or 1):.3f}" if int(row.fta or 0) > 0 else "0.000",
                }
                for row in season_summary_rows
            ]
            season_summary.sort(key=lambda row: get_season_sort_key()(row["season"]), reverse=True)

            season_kind = request.args.get("season_kind", "regular")
            if season_kind not in {"regular", "playoffs"}:
                season_kind = "regular"

            if season_kind == "regular":
                season_summary_view = [row for row in season_summary if str(row["season"]).startswith("2")]
            else:
                season_summary_view = [row for row in season_summary if str(row["season"]).startswith("4")]

            current_season = get_pick_current_season()([row["season"] for row in season_summary])
            season_options = [row["season"] for row in season_summary]
            selected_games_season = request.args.get("games_season")
            if selected_games_season not in season_options:
                selected_games_season = current_season

            teams = get_team_map()(session)
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

                    score = f"{team_score}-{opp_score}" if team_score is not None and opp_score is not None else "-"
                    if stat.win is None:
                        result = "-"
                        status = "Not finished"
                    elif stat.win:
                        result = "W"
                        status = "Win"
                    else:
                        result = "L"
                        status = "Loss"

                    opponent_team = teams.get(opponent_id) if opponent_id else None
                    current_games.append(
                        {
                            "game_id": game.game_id,
                            "game_date": get_fmt_date()(game.game_date),
                            "opponent_id": opponent_id,
                            "opponent_slug": opponent_team.slug if opponent_team else None,
                            "opponent_name": get_team_name()(teams, opponent_id),
                            "where": where,
                            "result": result,
                            "score": score,
                            "status": status,
                        }
                    )

            team_metrics = get_metric_results()(session, "team", team_id, current_season)

        return get_render_template()(
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
            canonical_team=canonical_team,
        )

    def game_page(slug: str):
        SessionLocal = get_session_local()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()
        PlayerGameStats = get_player_game_stats_model()
        Player = get_player_model()
        GamePlayByPlay = get_game_pbp_model()
        ShotRecord = get_shot_record_model()

        with SessionLocal() as session:
            from db.backfill_nba_game_line_score import has_game_line_score

            persisted_game = session.query(Game).filter(Game.slug == slug).first()
            game = persisted_game
            game_id = game.game_id if game else slug

            def _game_analysis_issues():
                return [
                    {
                        "id": item.id,
                        "issue_id": item.issue_id,
                        "issue_identifier": item.issue_identifier,
                        "issue_url": get_paperclip_issue_url()(item.issue_identifier),
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
                    for item in get_game_analysis_issue_history()(game_id)
                ]

            if game is None:
                live_payload = fetch_live_game_detail(game_id)
                live_summary = (live_payload or {}).get("summary") or fetch_live_scoreboard_map().get(game_id)
                if live_summary is None or live_summary.get("status") not in {GAME_STATUS_LIVE, GAME_STATUS_UPCOMING}:
                    abort(404, description=f"Game {game_id} not found")
                game = build_live_game_stub(live_summary)
                if game is None:
                    abort(404, description=f"Game {game_id} not found")
            else:
                live_payload = None

            if persisted_game is not None and not has_game_line_score(session, game_id):
                try:
                    from db.backfill_nba_game_line_score import back_fill_game_line_score
                    back_fill_game_line_score(session, game_id, commit=True)
                except Exception:
                    get_logger().exception("inline line-score fetch failed for game_id=%s", game_id)

            teams = get_team_map()(session)
            game_status = get_game_status(game)
            live_summary = (live_payload or {}).get("summary") or fetch_live_scoreboard_map().get(game_id)

            def _render_scoreboard_only_game(*, refresh_interval_ms, game_analysis_issues):
                return get_render_template()(
                    "game.html",
                    game=game,
                    game_status=game_status,
                    live_summary=live_summary,
                    live_refresh_interval_ms=refresh_interval_ms,
                    team_name=lambda team_id: get_team_name()(teams, team_id),
                    team_abbr=lambda team_id: get_team_abbr()(teams, team_id),
                    fmt_date=get_fmt_date(),
                    team_stats=[],
                    players_by_team={},
                    ordered_team_ids=[team_id for team_id in [game.road_team_id, game.home_team_id] if team_id],
                    pbp_rows=[],
                    shot_rows=[],
                    shot_rows_by_team={},
                    shot_chart_team_ids=[],
                    shot_made_count=0,
                    shot_miss_count=0,
                    shot_made_count_by_team={},
                    shot_miss_count_by_team={},
                    shot_backfill_status=request.args.get("shot_backfill"),
                    shot_backfill_count=request.args.get("shot_count"),
                    score_progression_json="[]",
                    road_abbr=get_team_abbr()(teams, game.road_team_id),
                    home_abbr=get_team_abbr()(teams, game.home_team_id),
                    quarter_scores=[],
                    home_team_id=game.home_team_id,
                    game_analysis_issues=game_analysis_issues,
                    upcoming_preview=None,
                )

            def _render_with_live_payload(payload, *, refresh_interval_ms):
                """Render game.html using nba_api live data instead of DB rows.

                Used for in-progress games AND for completed games that haven't
                been ingested yet (the 10-minute backfill window).
                """
                summary = payload["summary"]
                return get_render_template()(
                    "game.html",
                    game=game,
                    game_status=game_status,
                    live_summary=summary,
                    live_refresh_interval_ms=refresh_interval_ms,
                    team_name=lambda team_id: get_team_name()(teams, team_id),
                    team_abbr=lambda team_id: get_team_abbr()(teams, team_id),
                    fmt_date=get_fmt_date(),
                    team_stats=payload["team_stats"],
                    players_by_team=payload["players_by_team"],
                    ordered_team_ids=[team_id for team_id in payload["ordered_team_ids"] if team_id],
                    pbp_rows=payload["pbp_rows"],
                    shot_rows=[],
                    shot_rows_by_team={},
                    shot_chart_team_ids=[],
                    shot_made_count=0,
                    shot_miss_count=0,
                    shot_made_count_by_team={},
                    shot_miss_count_by_team={},
                    shot_backfill_status=request.args.get("shot_backfill"),
                    shot_backfill_count=request.args.get("shot_count"),
                    score_progression_json="[]",
                    road_abbr=get_team_abbr()(teams, game.road_team_id),
                    home_abbr=get_team_abbr()(teams, game.home_team_id),
                    quarter_scores=payload["quarter_scores"],
                    home_team_id=game.home_team_id,
                    game_analysis_issues=_game_analysis_issues(),
                    upcoming_preview=None,
                )

            if game_status == GAME_STATUS_LIVE:
                if live_payload is None:
                    live_payload = fetch_live_game_detail(game_id)
                if live_payload is not None:
                    return _render_with_live_payload(live_payload, refresh_interval_ms=15000)
                return _render_scoreboard_only_game(
                    refresh_interval_ms=15000,
                    game_analysis_issues=_game_analysis_issues(),
                )

            if game_status == GAME_STATUS_UPCOMING:
                upcoming_preview = _build_upcoming_preview(
                    session,
                    game,
                    teams,
                    live_summary,
                )
                return get_render_template()(
                    "game.html",
                    game=game,
                    game_status=game_status,
                    live_summary=live_summary,
                    live_refresh_interval_ms=60000,
                    team_name=lambda team_id: get_team_name()(teams, team_id),
                    team_abbr=lambda team_id: get_team_abbr()(teams, team_id),
                    fmt_date=get_fmt_date(),
                    team_stats=[],
                    players_by_team={},
                    ordered_team_ids=[team_id for team_id in [game.road_team_id, game.home_team_id] if team_id],
                    pbp_rows=[],
                    shot_rows=[],
                    shot_rows_by_team={},
                    shot_chart_team_ids=[],
                    shot_made_count=0,
                    shot_miss_count=0,
                    shot_made_count_by_team={},
                    shot_miss_count_by_team={},
                    shot_backfill_status=request.args.get("shot_backfill"),
                    shot_backfill_count=request.args.get("shot_count"),
                    score_progression_json="[]",
                    road_abbr=get_team_abbr()(teams, game.road_team_id),
                    home_abbr=get_team_abbr()(teams, game.home_team_id),
                    quarter_scores=[],
                    home_team_id=game.home_team_id,
                    game_analysis_issues=[],
                    upcoming_preview=upcoming_preview,
                )

            team_stats_rows = (
                session.query(TeamGameStats)
                .filter(TeamGameStats.game_id == game_id)
                .order_by(TeamGameStats.team_id.asc())
                .all()
            )
            team_stats = sorted(team_stats_rows, key=lambda row: 0 if row.team_id == game.home_team_id else 1)

            # If the game is marked completed but ingestion hasn't run yet
            # (the 10-min `ingest_recent_games` window), DB stats are empty
            # but nba_api still serves the final box score. Fall back to the
            # live endpoint and render with that payload — the page reloads
            # itself in 60s so the eventual DB-backed view replaces it.
            if (
                not team_stats_rows
                and game_status == GAME_STATUS_COMPLETED
            ):
                if live_payload is None:
                    live_payload = fetch_live_game_detail(game_id)
                if live_payload is not None and live_payload.get("team_stats"):
                    return _render_with_live_payload(live_payload, refresh_interval_ms=60000)

            player_rows = (
                session.query(PlayerGameStats, Player)
                .outerjoin(Player, Player.player_id == PlayerGameStats.player_id)
                .filter(PlayerGameStats.game_id == game_id)
                .order_by(PlayerGameStats.team_id.asc(), PlayerGameStats.player_id.asc())
                .all()
            )

            def _is_dnp(stat):
                return (stat.min is None and stat.sec is None) or (stat.comment or "").strip() != ""

            def _sort_key(stat):
                if stat.starter:
                    return (0, -(stat.pts or 0))
                if not _is_dnp(stat):
                    return (1, -(stat.pts or 0))
                return (2, 0)

            players_by_team: dict[str, list[dict[str, str | int]]] = defaultdict(list)
            team_rows: dict[str, list] = defaultdict(list)
            for stat, player in player_rows:
                team_rows[stat.team_id].append((stat, player))
            for team_id, rows in team_rows.items():
                rows.sort(key=lambda row: _sort_key(row[0]))
                for stat, player in rows:
                    player_name = get_display_player_name()(player) if player is not None else stat.player_id
                    players_by_team[team_id].append(
                        {
                            "player_id": stat.player_id,
                            "player_name": player_name,
                            "status": get_player_status()(stat),
                            "minutes": get_fmt_minutes()(stat.min, stat.sec),
                            "is_starter": bool(stat.starter),
                            "is_dnp": _is_dnp(stat),
                            "pts": stat.pts if stat.pts is not None else "-",
                            "reb": stat.reb if stat.reb is not None else "-",
                            "ast": stat.ast if stat.ast is not None else "-",
                            "stl": stat.stl if stat.stl is not None else "-",
                            "blk": stat.blk if stat.blk is not None else "-",
                            "tov": stat.tov if stat.tov is not None else "-",
                            "fgm": stat.fgm if stat.fgm is not None else "-",
                            "fga": stat.fga if stat.fga is not None else "-",
                            "fg3m": stat.fg3m if stat.fg3m is not None else "-",
                            "fg3a": stat.fg3a if stat.fg3a is not None else "-",
                            "ftm": stat.ftm if stat.ftm is not None else "-",
                            "fta": stat.fta if stat.fta is not None else "-",
                            "plus_minus": stat.plus if stat.plus is not None else "-",
                        }
                    )

            ordered_team_ids = [team_id for team_id in [game.road_team_id, game.home_team_id] if team_id]
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
                    "event_type": get_pbp_event_type_label()(row.event_msg_type),
                    "event_type_code": row.event_msg_type,
                    "description": get_pbp_text()(row) or "-",
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

            player_name_map = {
                str(stat.player_id): (get_display_player_name()(player) if player else str(stat.player_id))
                for stat, player in player_rows
            }

            score_progression = [{"t": 0.0, "road": 0, "home": 0, "scorer": None, "desc": None}]
            prev_road = prev_home = 0
            for row in pbp_rows_raw:
                if not row.score or row.period is None:
                    continue
                try:
                    parts = row.score.split("-")
                    if len(parts) != 2:
                        continue
                    home_score, road_score = int(parts[0].strip()), int(parts[1].strip())
                except (AttributeError, ValueError):
                    continue
                if road_score == prev_road and home_score == prev_home:
                    continue
                period = int(row.period)
                clock = row.pc_time or "0:00"
                try:
                    mins, secs = clock.split(":")
                    remaining = int(mins) * 60 + int(secs)
                except Exception:
                    remaining = 0
                if period <= 4:
                    offset = (period - 1) * 12 * 60
                    dur = 12 * 60
                else:
                    offset = 48 * 60 + (period - 5) * 5 * 60
                    dur = 5 * 60
                elapsed = round((offset + dur - remaining) / 60, 3)
                raw_desc = (
                    row.home_description if home_score > prev_home
                    else row.visitor_description if road_score > prev_road
                    else None
                ) or ""
                if row.player1_id and str(row.player1_id) in player_name_map:
                    scorer = player_name_map[str(row.player1_id)]
                else:
                    scorer = raw_desc.split()[0] if raw_desc.strip() else None
                desc = raw_desc.split("(")[0].strip() or None
                score_progression.append({"t": elapsed, "road": road_score, "home": home_score, "scorer": scorer, "desc": desc})
                prev_road, prev_home = road_score, home_score

            from metrics.helpers import get_quarter_scores as _get_quarter_scores

            quarter_scores = [
                {"period": row["period"], "home": row["home_pts"], "road": row["road_pts"]}
                for row in _get_quarter_scores(session, game_id)
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
                    str(player.player_id): (get_display_player_name()(player) or str(player.player_id))
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
                    "clock": get_fmt_minutes()(row.min, row.sec),
                    "team_id": row.team_id,
                    "team_name": get_team_name()(teams, row.team_id),
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

            shot_chart_team_ids = [team_id for team_id in [game.road_team_id, game.home_team_id] if team_id]
            for team_id in shot_rows_by_team:
                if team_id not in shot_chart_team_ids:
                    shot_chart_team_ids.append(team_id)

            import json as _json

            score_progression_json = _json.dumps(score_progression)
            road_abbr = get_team_abbr()(teams, game.road_team_id)
            home_abbr = get_team_abbr()(teams, game.home_team_id)
            home_team_id = game.home_team_id
            game_analysis_issues = _game_analysis_issues()

        return get_render_template()(
            "game.html",
            game=game,
            game_status=game_status,
            live_summary=live_summary,
            live_refresh_interval_ms=None,
            team_name=lambda team_id: get_team_name()(teams, team_id),
            team_abbr=lambda team_id: get_team_abbr()(teams, team_id),
            fmt_date=get_fmt_date(),
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
            upcoming_preview=None,
        )

    def game_fragment_metrics(slug: str):
        SessionLocal = get_session_local()
        Game = get_game_model()

        with SessionLocal() as session:
            game = session.query(Game).filter(Game.slug == slug).first()
            if game is None:
                abort(404)
            game_metrics = get_metric_results()(session, "game", game.game_id, game.season)
        return get_render_template()("_game_metrics.html", game_metrics=game_metrics)

    def api_game_period_stats(game_id: str):
        """Return per-period player stats for a game. Fetch from NBA API if not in DB."""
        from db.models import PlayerGamePeriodStats

        SessionLocal = get_session_local()
        Game = get_game_model()

        with SessionLocal() as session:
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is None:
                return jsonify({"ok": False, "error": "game not found"}), 404

            rows = (
                session.query(PlayerGamePeriodStats)
                .filter(PlayerGamePeriodStats.game_id == game_id)
                .order_by(PlayerGamePeriodStats.period, PlayerGamePeriodStats.player_id)
                .all()
            )

            if not rows:
                # Try fetching from NBA API
                try:
                    from db.backfill_nba_game_detail import (
                        create_player_period_stats,
                        fetch_all_period_stats,
                    )

                    periods = fetch_all_period_stats(game_id)
                    if periods:
                        for period, period_rows in periods.items():
                            for ps in period_rows:
                                create_player_period_stats(session, game_id, period, ps)
                        session.commit()
                        rows = (
                            session.query(PlayerGamePeriodStats)
                            .filter(PlayerGamePeriodStats.game_id == game_id)
                            .order_by(PlayerGamePeriodStats.period, PlayerGamePeriodStats.player_id)
                            .all()
                        )
                except Exception:
                    get_logger().exception("Failed to fetch period stats for %s", game_id)

            result: dict[str, list[dict]] = {}
            for r in rows:
                result.setdefault(r.player_id, []).append({
                    "period": r.period,
                    "min": r.min or 0,
                    "sec": r.sec or 0,
                    "pts": r.pts or 0,
                    "reb": r.reb or 0,
                    "ast": r.ast or 0,
                    "stl": r.stl or 0,
                    "blk": r.blk or 0,
                    "tov": r.tov or 0,
                    "fgm": r.fgm or 0,
                    "fga": r.fga or 0,
                    "fg3m": r.fg3m or 0,
                    "fg3a": r.fg3a or 0,
                    "ftm": r.ftm or 0,
                    "fta": r.fta or 0,
                    "pf": r.pf or 0,
                    "plus_minus": r.plus_minus or 0,
                })

        return jsonify({"ok": True, "players": result})

    app.add_url_rule("/api/games/<game_id>/period-stats", endpoint="api_game_period_stats", view_func=api_game_period_stats)

    app.add_url_rule("/cn/players/<slug>", endpoint="player_page_zh", view_func=player_page)
    app.add_url_rule("/players/<slug>", endpoint="player_page", view_func=player_page)
    app.add_url_rule("/cn/teams/<slug>", endpoint="team_page_zh", view_func=team_page)
    app.add_url_rule("/teams/<slug>", endpoint="team_page", view_func=team_page)
    app.add_url_rule("/cn/games/<slug>", endpoint="game_page_zh", view_func=game_page)
    app.add_url_rule("/games/<slug>", endpoint="game_page", view_func=game_page)
    app.add_url_rule("/cn/games/<slug>/fragment/metrics", endpoint="game_fragment_metrics_zh", view_func=game_fragment_metrics)
    app.add_url_rule("/games/<slug>/fragment/metrics", endpoint="game_fragment_metrics", view_func=game_fragment_metrics)

    return SimpleNamespace(
        player_page=player_page,
        team_page=team_page,
        game_page=game_page,
        game_fragment_metrics=game_fragment_metrics,
    )
