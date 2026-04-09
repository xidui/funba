from __future__ import annotations

from collections import defaultdict
from datetime import date
from types import SimpleNamespace
from typing import Any, Callable

from flask import abort, jsonify, request
from sqlalchemy import case, func, or_


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


def register_public_routes(
    app,
    *,
    get_session_local: Callable[[], Any],
    get_render_template: Callable[..., Any],
    get_team_model: Callable[[], Any],
    get_game_model: Callable[[], Any],
    get_team_game_stats_model: Callable[[], Any],
    get_player_game_stats_model: Callable[[], Any],
    get_metric_result_model: Callable[[], Any],
    get_game_pbp_model: Callable[[], Any],
    get_player_model: Callable[[], Any],
    get_award_model: Callable[[], Any],
    get_team_map: Callable[[Any], dict[str, Any]],
    get_season_sort_key: Callable[[str | None], tuple[int, int]],
    get_franchise_display: Callable[[str, str | None, Any], tuple[str, str]],
    get_display_team_name: Callable[[Any], str],
    get_season_label: Callable[[str | None], str],
    get_is_zh: Callable[[], bool],
    get_team_map_positions: Callable[[], dict[str, tuple[float, float]]],
    get_fmt_date: Callable[[Any], str],
    get_coerce_award_season: Callable[[str | int | None], int | None],
    get_award_type_meta: Callable[[], dict[str, dict[str, str]]],
    get_award_order_case: Callable[[Any], Any],
    get_award_entry_from_row: Callable[[Any, dict[str, Any]], dict[str, object]],
    get_group_award_entries: Callable[[list[dict[str, object]]], list[dict[str, object]]],
    get_award_tab_groups: Callable[[], list[dict[str, object]]],
    get_award_type_label: Callable[[str], str],
    limiter,
    get_pct_text: Callable[[int, int], str],
    get_pick_current_season: Callable[[list[str]], str | None],
    get_team_abbr: Callable[[dict[str, Any], str | None], str],
    get_metric_name_for_key: Callable[[Any, str], str],
    get_asc_metric_keys: Callable[[Any], set[str]],
    get_metric_results: Callable[[Any, str, str, str | None], dict],
    get_player_headshot_url: Callable[[str | None], str | None],
    get_localized_url_for: Callable[..., str],
    get_t: Callable[[str, str | None], str],
    get_pct_fmt: Callable[[Any], str],
):
    def _build_today_games(team_lookup: dict) -> list[dict]:
        """Build today's games with fixed stats for the home page."""
        SessionLocal = get_session_local()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()
        PlayerGameStats = get_player_game_stats_model()
        MetricResultModel = get_metric_result_model()
        GamePlayByPlay = get_game_pbp_model()
        Player = get_player_model()

        from datetime import date as _date, timedelta

        with SessionLocal() as session:
            today = _date.today()
            games = (
                session.query(Game)
                .filter(Game.game_date == today, Game.home_team_score.isnot(None))
                .order_by(Game.game_id.asc())
                .all()
            )
            if not games:
                games = (
                    session.query(Game)
                    .filter(Game.game_date == today - timedelta(days=1), Game.home_team_score.isnot(None))
                    .order_by(Game.game_id.asc())
                    .all()
                )
                if not games:
                    return []

            game_date = games[0].game_date
            game_ids = [g.game_id for g in games]

            lc_rows = (
                session.query(MetricResultModel.game_id, MetricResultModel.value_num)
                .filter(
                    MetricResultModel.game_id.in_(game_ids),
                    MetricResultModel.metric_key == "lead_changes",
                )
                .all()
            )
            lead_changes_map = {r.game_id: int(r.value_num) for r in lc_rows}

            all_ts = (
                session.query(TeamGameStats)
                .filter(TeamGameStats.game_id.in_(game_ids))
                .all()
            )
            ts_map: dict[tuple[str, str], Any] = {}
            for ts in all_ts:
                ts_map[(ts.game_id, ts.team_id)] = ts

            all_ps = (
                session.query(PlayerGameStats)
                .filter(PlayerGameStats.game_id.in_(game_ids))
                .all()
            )
            ps_by_gt: dict[tuple[str, str], list] = defaultdict(list)
            for ps in all_ps:
                ps_by_gt[(ps.game_id, ps.team_id)].append(ps)

            def _top_by(rows, field):
                return max(rows, key=lambda r: int(getattr(r, field, 0) or 0), default=None)

            top_scorer_map: dict[tuple[str, str], Any] = {}
            top_rebounder_map: dict[tuple[str, str], Any] = {}
            top_assister_map: dict[tuple[str, str], Any] = {}
            for key, rows in ps_by_gt.items():
                top_scorer_map[key] = _top_by(rows, "pts")
                top_rebounder_map[key] = _top_by(rows, "reb")
                top_assister_map[key] = _top_by(rows, "ast")

            all_leader_ids = set()
            for mapping in (top_scorer_map, top_rebounder_map, top_assister_map):
                for ps in mapping.values():
                    if ps:
                        all_leader_ids.add(ps.player_id)

            player_names = {}
            if all_leader_ids:
                prows = (
                    session.query(Player.player_id, Player.full_name, Player.full_name_zh)
                    .filter(Player.player_id.in_(all_leader_ids))
                    .all()
                )
                for p in prows:
                    player_names[p.player_id] = p.full_name_zh if get_is_zh() and p.full_name_zh else p.full_name

            all_pbp_margins = (
                session.query(GamePlayByPlay.game_id, GamePlayByPlay.score_margin)
                .filter(
                    GamePlayByPlay.game_id.in_(game_ids),
                    GamePlayByPlay.score_margin.isnot(None),
                    GamePlayByPlay.score_margin != "TIE",
                )
                .all()
            )
            margins_by_game: dict[str, list[int]] = defaultdict(list)
            for row in all_pbp_margins:
                try:
                    margins_by_game[row.game_id].append(int(row.score_margin))
                except (TypeError, ValueError):
                    pass

            result = []
            for game in games:
                home_team = team_lookup.get(game.home_team_id)
                road_team = team_lookup.get(game.road_team_id)
                home_won = game.wining_team_id == game.home_team_id if game.wining_team_id else None

                margins = margins_by_game.get(game.game_id, [])
                home_lead = max(margins, default=0) if margins else 0
                road_lead = max((-margin for margin in margins), default=0) if margins else 0

                home_ts = ts_map.get((game.game_id, game.home_team_id))
                road_ts = ts_map.get((game.game_id, game.road_team_id))

                def _leader(player_stats, stat):
                    if not player_stats:
                        return None
                    return {
                        "player_id": player_stats.player_id,
                        "name": player_names.get(player_stats.player_id, ""),
                        "value": int(getattr(player_stats, stat, 0) or 0),
                    }

                result.append(
                    {
                        "game_id": game.game_id,
                        "game_date": game_date,
                        "home_team_id": game.home_team_id,
                        "road_team_id": game.road_team_id,
                        "home_abbr": home_team.abbr if home_team else "???",
                        "road_abbr": road_team.abbr if road_team else "???",
                        "home_score": game.home_team_score,
                        "road_score": game.road_team_score,
                        "home_won": home_won,
                        "lead_changes": lead_changes_map.get(game.game_id),
                        "home_largest_lead": max(home_lead, 0),
                        "road_largest_lead": max(road_lead, 0),
                        "home_fg_pct": round(home_ts.fg_pct * 100, 1) if home_ts and home_ts.fg_pct else None,
                        "road_fg_pct": round(road_ts.fg_pct * 100, 1) if road_ts and road_ts.fg_pct else None,
                        "home_fg3_pct": round(home_ts.fg3_pct * 100, 1) if home_ts and home_ts.fg3_pct else None,
                        "road_fg3_pct": round(road_ts.fg3_pct * 100, 1) if road_ts and road_ts.fg3_pct else None,
                        "home_scorer": _leader(top_scorer_map.get((game.game_id, game.home_team_id)), "pts"),
                        "road_scorer": _leader(top_scorer_map.get((game.game_id, game.road_team_id)), "pts"),
                        "home_rebounder": _leader(top_rebounder_map.get((game.game_id, game.home_team_id)), "reb"),
                        "road_rebounder": _leader(top_rebounder_map.get((game.game_id, game.road_team_id)), "reb"),
                        "home_assister": _leader(top_assister_map.get((game.game_id, game.home_team_id)), "ast"),
                        "road_assister": _leader(top_assister_map.get((game.game_id, game.road_team_id)), "ast"),
                    }
                )
            return result

    def home():
        SessionLocal = get_session_local()
        Team = get_team_model()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()

        with SessionLocal() as session:
            teams = (
                session.query(Team)
                .filter(Team.is_legacy.is_(False))
                .order_by(Team.full_name.asc())
                .limit(30)
                .all()
            )
            team_lookup = get_team_map(session)

            standing_season_ids = [
                row.season
                for row in session.query(Game.season).filter(Game.season.like("2%")).distinct().all()
            ]
            standing_season_ids = sorted(standing_season_ids, key=get_season_sort_key(), reverse=True)
            selected_standing_season = request.args.get("season") or (standing_season_ids[0] if standing_season_ids else None)

            east_ids = {
                "1610612737", "1610612751", "1610612738", "1610612766", "1610612741",
                "1610612739", "1610612765", "1610612754", "1610612748", "1610612749",
                "1610612752", "1610612753", "1610612755", "1610612761", "1610612764",
            }

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
                for row in rows:
                    team = team_lookup.get(row.team_id)
                    abbr, full_name = get_franchise_display()(row.team_id, selected_standing_season, team)
                    wins, losses = int(row.wins or 0), int(row.losses or 0)
                    total = wins + losses
                    entry = {
                        "team_id": row.team_id,
                        "abbr": abbr,
                        "full_name": full_name,
                        "wins": wins,
                        "losses": losses,
                        "win_pct": wins / total if total > 0 else 0.0,
                    }
                    if row.team_id in east_ids:
                        east_standings.append(entry)
                    else:
                        west_standings.append(entry)
                east_standings.sort(key=lambda item: item["win_pct"], reverse=True)
                west_standings.sort(key=lambda item: item["win_pct"], reverse=True)

        team_map_data = []
        team_map_positions = get_team_map_positions()
        for team in teams:
            pos = team_map_positions.get(team.abbr)
            if pos:
                team_map_data.append(
                    {
                        "abbr": team.abbr,
                        "full_name": get_display_team_name()(team),
                        "team_id": team.team_id,
                        "lat": pos[0],
                        "lon": pos[1],
                    }
                )

        today_games_data = _build_today_games(team_lookup)
        return get_render_template()(
            "home.html",
            teams=teams,
            team_map_data=team_map_data,
            east_standings=east_standings,
            west_standings=west_standings,
            standing_season_ids=standing_season_ids,
            selected_standing_season=selected_standing_season,
            fmt_season=get_season_label(),
            today_games=today_games_data,
        )

    def games_list():
        SessionLocal = get_session_local()
        Game = get_game_model()
        Team = get_team_model()

        page_size = 30
        with SessionLocal() as session:
            all_season_ids = sorted(
                {row.season for row in session.query(Game.season).filter(Game.season.isnot(None)).all()},
                key=get_season_sort_key(),
                reverse=True,
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
                games_q = games_q.filter(or_(Game.home_team_id == selected_team, Game.road_team_id == selected_team))
            games_q = games_q.order_by(Game.game_date.desc(), Game.game_id.desc())

            total = games_q.count()
            total_pages = max(1, (total + page_size - 1) // page_size)
            page = min(page, total_pages)
            games = games_q.offset((page - 1) * page_size).limit(page_size).all()

            team_lookup = get_team_map(session)
            selected_team_obj = next((team for team in all_teams if team.team_id == selected_team), None)
            if selected_team_obj is None and selected_team:
                selected_team_obj = team_lookup.get(selected_team)

        return get_render_template()(
            "games_list.html",
            games=games,
            team_lookup=team_lookup,
            all_teams=all_teams,
            all_season_ids=all_season_ids,
            selected_season=selected_season,
            selected_team=selected_team,
            selected_team_obj=selected_team_obj,
            fmt_date=get_fmt_date(),
            fmt_season=get_season_label(),
            page=page,
            total_pages=total_pages,
            total=total,
        )

    def awards_page():
        SessionLocal = get_session_local()
        Award = get_award_model()
        Player = get_player_model()
        Team = get_team_model()

        award_type_meta = get_award_type_meta()
        award_tab_groups = get_award_tab_groups()

        selected_award_type = request.args.get("type", "champion")
        if selected_award_type not in award_type_meta:
            selected_award_type = "champion"

        with SessionLocal() as session:
            season_rows = session.query(Award.season).distinct().order_by(Award.season.desc()).all()
            season_options = [int(row[0]) for row in season_rows if get_coerce_award_season()(row[0]) is not None]
            selected_season = get_coerce_award_season()(request.args.get("season"))
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

            award_rows = award_query.order_by(get_award_order_case()(Award.award_type), Award.season.desc(), Award.id.asc()).all()
            teams = get_team_map(session)
            award_entries = [get_award_entry_from_row()(row, teams) for row in award_rows]
            award_sections = get_group_award_entries()(award_entries)

        return get_render_template()(
            "awards.html",
            title="Awards • FUNBA",
            award_tab_groups=[
                {
                    "label": group["label"],
                    "tabs": [
                        {"award_type": award_type, "label": get_award_type_label()(award_type)}
                        for award_type in group["types"]
                        if award_type in award_type_meta
                    ],
                }
                for group in award_tab_groups
            ],
            award_sections=award_sections,
            selected_award_type=selected_award_type,
            season_options=season_options,
            selected_season=selected_season,
        )

    def player_hints_api():
        SessionLocal = get_session_local()
        Player = get_player_model()

        query = (request.args.get("q") or "").strip()
        try:
            limit = int(request.args.get("limit", 12))
        except ValueError:
            limit = 12
        limit = max(1, min(limit, 30))

        with SessionLocal() as session:
            player_query = session.query(Player).filter(Player.full_name.isnot(None))
            if query:
                player_query = player_query.filter(
                    or_(
                        Player.full_name.ilike(f"%{query}%"),
                        Player.full_name_zh.ilike(f"%{query}%"),
                    )
                )
            players = player_query.order_by(Player.is_active.desc(), Player.full_name.asc()).limit(limit).all()

        items = [
            {
                "player_id": player.player_id,
                "full_name": player.full_name_zh if get_is_zh() and getattr(player, "full_name_zh", None) else player.full_name,
            }
            for player in players
            if player.player_id and player.full_name
        ]
        return jsonify({"items": items})

    def _player_summary_fields(played_condition):
        PlayerGameStats = get_player_game_stats_model()
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
        summary["fg_pct"] = get_pct_text()(summary["fgm"], summary["fga"])
        summary["fg3_pct"] = get_pct_text()(summary["fg3m"], summary["fg3a"])
        summary["ft_pct"] = get_pct_text()(summary["ftm"], summary["fta"])

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

    def _player_stat_summary(session, player_id: str, *, season: str | None = None, season_prefix: str | None = None) -> dict[str, str | int]:
        PlayerGameStats = get_player_game_stats_model()
        Game = get_game_model()

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

    def _player_career_summary(session, player_id: str, *, season_prefix: str, teams: dict[str, Any]) -> tuple[dict[str, str | int], list[dict[str, object]]]:
        PlayerGameStats = get_player_game_stats_model()
        Game = get_game_model()

        played_condition = (func.coalesce(PlayerGameStats.min, 0) > 0) | (func.coalesce(PlayerGameStats.sec, 0) > 0)
        season_rows_raw = (
            session.query(Game.season.label("season"), *_player_summary_fields(played_condition))
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(
                PlayerGameStats.player_id == player_id,
                Game.season.like(f"{season_prefix}%"),
            )
            .group_by(Game.season)
            .all()
        )

        career_season_rows = [{"season": row.season, "stats": _player_summary_from_row(row)} for row in season_rows_raw]
        career_season_rows.sort(key=lambda row: get_season_sort_key()(row["season"]), reverse=True)

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
        for row in season_team_rows:
            abbr = get_team_abbr()(teams, row.team_id)
            if abbr not in season_team_abbrs[row.season]:
                season_team_abbrs[row.season].append(abbr)
        for row in career_season_rows:
            row["team_abbrs"] = season_team_abbrs.get(row["season"], [])

        return _player_stat_summary(session, player_id, season_prefix=season_prefix), career_season_rows

    def _latest_regular_season(session) -> str | None:
        Game = get_game_model()
        seasons = [
            row.season
            for row in session.query(Game.season.label("season"))
            .filter(Game.season.like("2%"))
            .distinct()
            .all()
            if row.season
        ]
        return get_pick_current_season()(seasons) or "22025"

    def _compare_metric_label(session, metric_key: str) -> str:
        return get_metric_name_for_key()(session, metric_key)

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
            return get_pct_fmt()(value)
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
            best_idx, _ = min(scored, key=lambda item: item[1])
        else:
            best_idx, _ = max(scored, key=lambda item: item[1])
        return best_idx

    def _compare_metric_scope_label(entry: dict) -> str:
        season = str(entry.get("season") or "").strip()
        if len(season) == 5 and season.isdigit():
            return get_season_label()(season)
        career_labels = {
            "all_regular": get_t()("Regular Season Career", "常规赛生涯"),
            "all_playoffs": get_t()("Playoffs Career", "季后赛生涯"),
            "all_playin": get_t()("Play-In Career", "附加赛生涯"),
        }
        if season in career_labels:
            return career_labels[season]
        return entry.get("career_type_label") or get_t()("Career", "生涯")

    def _build_compare_stat_rows(player_cards: list[dict]) -> list[dict]:
        rows = []
        for stat_key, label in _COMPARE_STATS_ROWS:
            values = [_compare_summary_value(card.get("career_summary"), stat_key) for card in player_cards]
            best_index = _compare_best_index([_compare_numeric_value(value) for value in values])
            rows.append({"label": label, "values": values, "best_index": best_index})
        return rows

    def _build_compare_current_rows(player_cards: list[dict]) -> list[dict]:
        rows = []
        for stat_key, label in _COMPARE_STATS_ROWS:
            values = [_compare_summary_value(card.get("current_summary"), stat_key) for card in player_cards]
            best_index = _compare_best_index([_compare_numeric_value(value) for value in values])
            rows.append({"label": label, "values": values, "best_index": best_index})
        return rows

    def _build_compare_metric_sections(session, player_cards: list[dict]) -> list[dict]:
        sections: list[dict] = []
        asc_keys = get_asc_metric_keys()(session)

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
                            "href": get_localized_url_for()("metric_detail", metric_key=entry["metric_key"]),
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
                grouped[entry.get("career_type_label") or get_t()("Career", "生涯")].append(entry)
            for title in grouped:
                grouped_alltime.setdefault(title, [[] for _ in player_cards])

        for idx, card in enumerate(player_cards):
            grouped: dict[str, list[dict]] = defaultdict(list)
            for entry in card["metrics"]["alltime"]:
                grouped[entry.get("career_type_label") or get_t()("Career", "生涯")].append(entry)
            for title, lists in grouped_alltime.items():
                lists[idx] = grouped.get(title, [])

        for title in sorted(grouped_alltime.keys()):
            section = build_rows(grouped_alltime[title], group_title=f"{title}{get_t()(' Career', '生涯')}")
            if section is not None:
                sections.append(section)

        return sections

    def _player_compare_team_abbrs(session, player_id: str, teams: dict[str, Any], *, preferred_season: str | None = None) -> list[str]:
        PlayerGameStats = get_player_game_stats_model()
        Game = get_game_model()

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
                abbr = get_team_abbr()(teams, row.team_id)
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

    def _get_player_top_rankings(session, player_id: str, *, current_season: str | None, limit: int = 3) -> list[dict]:
        from metrics.framework.base import CAREER_SEASON_PREFIX, SEASON_TYPE_TO_CAREER

        MetricResultModel = get_metric_result_model()
        asc_keys = get_asc_metric_keys()(session)
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
                    "href": get_localized_url_for()("metric_detail", metric_key=metric_key, season=row.season)
                    if row.season
                    else get_localized_url_for()("metric_detail", metric_key=metric_key),
                }
            )
        return rankings

    def players_compare():
        SessionLocal = get_session_local()
        Player = get_player_model()

        raw_ids = [part.strip() for part in (request.args.get("ids") or "").split(",") if part.strip()]
        requested_ids: list[str] = []
        for player_id in raw_ids:
            if player_id not in requested_ids:
                requested_ids.append(player_id)
            if len(requested_ids) == 4:
                break

        with SessionLocal() as session:
            players_by_id = (
                {
                    player.player_id: player
                    for player in session.query(Player).filter(Player.player_id.in_(requested_ids)).all()
                }
                if requested_ids
                else {}
            )
            players = [players_by_id[player_id] for player_id in requested_ids if player_id in players_by_id]
            teams = get_team_map(session)
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
                        "headshot_url": get_player_headshot_url()(player.player_id),
                        "team_abbrs": team_abbrs,
                        "team_label": " / ".join(team_abbrs) if team_abbrs else "NBA",
                        "career_summary": _player_stat_summary(session, player.player_id, season_prefix="2"),
                        "current_summary": _player_stat_summary(session, player.player_id, season=current_season),
                        "metrics": get_metric_results()(session, "player", player.player_id, current_season),
                        "top_rankings": _get_player_top_rankings(session, player.player_id, current_season=current_season),
                    }
                )

            if len(player_cards) >= 2:
                season_rows = _build_compare_stat_rows(player_cards)
                current_rows = _build_compare_current_rows(player_cards)
                metric_sections = _build_compare_metric_sections(session, player_cards)

        return get_render_template()(
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

    def draft_page(year: int):
        SessionLocal = get_session_local()
        Player = get_player_model()

        current_year = date.today().year
        if year < 1947 or year > current_year:
            abort(404)

        with SessionLocal() as session:
            min_year, max_year = (
                session.query(func.min(Player.draft_year), func.max(Player.draft_year))
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

        return get_render_template()(
            "draft.html",
            year=year,
            draft_players=draft_players,
            draft_count=len(draft_players),
            min_year=min_year,
            max_year=max_year,
        )

    app.add_url_rule("/cn/", endpoint="home_zh", view_func=home)
    app.add_url_rule("/", endpoint="home", view_func=home)
    app.add_url_rule("/cn/games", endpoint="games_list_zh", view_func=games_list)
    app.add_url_rule("/games", endpoint="games_list", view_func=games_list)
    app.add_url_rule("/cn/awards", endpoint="awards_page_zh", view_func=awards_page)
    app.add_url_rule("/awards", endpoint="awards_page", view_func=awards_page)
    app.add_url_rule("/api/players/hints", endpoint="player_hints_api", view_func=limiter.limit("60 per minute")(player_hints_api))
    app.add_url_rule("/cn/players/compare", endpoint="players_compare_zh", view_func=players_compare)
    app.add_url_rule("/players/compare", endpoint="players_compare", view_func=players_compare)
    app.add_url_rule("/cn/draft/<int:year>", endpoint="draft_page_zh", view_func=draft_page)
    app.add_url_rule("/draft/<int:year>", endpoint="draft_page", view_func=draft_page)

    return SimpleNamespace(
        home=home,
        games_list=games_list,
        awards_page=awards_page,
        player_hints_api=player_hints_api,
        players_compare=players_compare,
        draft_page=draft_page,
        player_stat_summary=_player_stat_summary,
        player_career_summary=_player_career_summary,
        latest_regular_season=_latest_regular_season,
        build_compare_stat_rows=_build_compare_stat_rows,
        build_compare_current_rows=_build_compare_current_rows,
        build_compare_metric_sections=_build_compare_metric_sections,
        player_compare_team_abbrs=_player_compare_team_abbrs,
        get_player_top_rankings=_get_player_top_rankings,
    )
