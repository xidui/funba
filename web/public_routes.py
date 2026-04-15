from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from itertools import groupby as _groupby
from types import SimpleNamespace
from typing import Any, Callable

from flask import abort, jsonify, request
from sqlalchemy import case, func, or_

from db.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_LIVE,
    GAME_STATUS_UPCOMING,
    get_game_status,
)
from web.live_game_data import (
    build_live_game_stub,
    fetch_live_card,
    fetch_live_game_detail,
    fetch_live_scoreboard_map,
)


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


def _game_status_rank(status: str | None) -> int:
    if status == GAME_STATUS_LIVE:
        return 0
    if status == GAME_STATUS_COMPLETED:
        return 1
    if status == GAME_STATUS_UPCOMING:
        return 2
    return 3


def _build_game_list_entry(game, live_snapshot: dict | None = None):
    status = live_snapshot.get("status") if live_snapshot else get_game_status(game)
    road_score = live_snapshot.get("road_score") if live_snapshot else game.road_team_score
    home_score = live_snapshot.get("home_score") if live_snapshot else game.home_team_score
    # Live scoreboard CDN reflects play-in / playoff matchups before
    # ScheduleLeagueV2 propagates them, so prefer live IDs when DB is NULL.
    home_team_id = game.home_team_id
    road_team_id = game.road_team_id
    if live_snapshot:
        if not home_team_id and live_snapshot.get("home_team_id"):
            home_team_id = live_snapshot["home_team_id"]
        if not road_team_id and live_snapshot.get("road_team_id"):
            road_team_id = live_snapshot["road_team_id"]
    return SimpleNamespace(
        game=game,
        game_id=game.game_id,
        game_date=game.game_date,
        season=game.season,
        road_team_id=road_team_id,
        home_team_id=home_team_id,
        road_score=road_score,
        home_score=home_score,
        road_won=status == GAME_STATUS_COMPLETED and game.wining_team_id == game.road_team_id,
        home_won=status == GAME_STATUS_COMPLETED and game.wining_team_id == game.home_team_id,
        status=status,
        status_summary=(live_snapshot or {}).get("summary") or "",
        # Upcoming games land on the detail page's preview section (H2H,
        # last 10, tipoff, previous top scorer) — make them linkable unless
        # we don't even know the teams yet.
        link_enabled=bool(home_team_id and road_team_id),
    )


def _supplement_missing_live_games(
    games: list,
    live_map: dict[str, dict],
    *,
    selected_season: str | None = None,
    selected_team: str | None = None,
) -> list:
    existing_game_ids = {game.game_id for game in games}
    supplemented = list(games)

    for snapshot in live_map.values():
        if snapshot.get("status") not in {GAME_STATUS_LIVE, GAME_STATUS_UPCOMING}:
            continue
        game_id = str(snapshot.get("game_id") or "")
        if not game_id or game_id in existing_game_ids:
            continue
        if selected_season and snapshot.get("season") != selected_season:
            continue
        if selected_team and selected_team not in {snapshot.get("home_team_id"), snapshot.get("road_team_id")}:
            continue

        stub = build_live_game_stub(snapshot)
        if stub is None:
            continue
        supplemented.append(stub)
        existing_game_ids.add(game_id)

    return supplemented


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
            live_map = fetch_live_scoreboard_map()
            games = (
                session.query(Game)
                .filter(Game.game_date == today)
                .order_by(Game.game_id.asc())
                .all()
            )
            persisted_game_ids = {game.game_id for game in games}
            games = _supplement_missing_live_games(games, live_map)
            if not games:
                games = (
                    session.query(Game)
                    .filter(Game.game_date == today - timedelta(days=1), Game.home_team_score.isnot(None))
                    .order_by(Game.game_id.asc())
                    .all()
                )
                if not games:
                    return []
                persisted_game_ids = {game.game_id for game in games}

            game_date = games[0].game_date
            game_ids = [g.game_id for g in games if g.game_id in persisted_game_ids]

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

            # ── Upcoming game extras: team records, last 10, head-to-head ──
            upcoming_team_ids: set[str] = set()
            upcoming_pairs: set[tuple[str, str]] = set()
            for game in games:
                game_status = (live_map.get(game.game_id, {}) or {}).get("status") or get_game_status(game)
                if game_status == GAME_STATUS_UPCOMING:
                    if game.home_team_id:
                        upcoming_team_ids.add(game.home_team_id)
                    if game.road_team_id:
                        upcoming_team_ids.add(game.road_team_id)
                    if game.home_team_id and game.road_team_id:
                        upcoming_pairs.add(tuple(sorted([game.home_team_id, game.road_team_id])))

            team_record: dict[str, tuple[int, int]] = {}
            team_last10: dict[str, list[str]] = {}
            team_last_date: dict[str, Any] = {}
            h2h_series: dict[tuple[str, str], tuple[int, int]] = {}
            current_season_for_records: str | None = None

            if upcoming_team_ids:
                # Determine the "current" regular season (prefer latest season
                # that has completed games). Playoffs/playin are not mixed in.
                current_season_row = (
                    session.query(Game.season)
                    .filter(Game.wining_team_id.isnot(None))
                    .filter(Game.season.like("2%"))
                    .order_by(Game.game_date.desc())
                    .limit(1)
                    .first()
                )
                if current_season_row:
                    current_season_for_records = current_season_row[0]

                if current_season_for_records:
                    # Pull every completed regular-season game for teams we care about.
                    past_games = (
                        session.query(Game)
                        .filter(
                            Game.season == current_season_for_records,
                            Game.wining_team_id.isnot(None),
                        )
                        .filter(
                            (Game.home_team_id.in_(upcoming_team_ids))
                            | (Game.road_team_id.in_(upcoming_team_ids))
                        )
                        .order_by(Game.game_date.desc(), Game.game_id.desc())
                        .all()
                    )
                    team_games: dict[str, list] = defaultdict(list)
                    for pg in past_games:
                        if pg.home_team_id in upcoming_team_ids:
                            team_games[pg.home_team_id].append(pg)
                        if pg.road_team_id in upcoming_team_ids:
                            team_games[pg.road_team_id].append(pg)
                    for tid, games_for_team in team_games.items():
                        wins = 0
                        losses = 0
                        for pg in games_for_team:
                            if pg.wining_team_id == tid:
                                wins += 1
                            else:
                                losses += 1
                        team_record[tid] = (wins, losses)
                        # Last 10 already sorted desc by date, take first 10, reverse to chrono.
                        last10_chrono = list(reversed(games_for_team[:10]))
                        team_last10[tid] = [
                            "W" if pg.wining_team_id == tid else "L"
                            for pg in last10_chrono
                        ]
                        # Most recent played date for rest-day calculation.
                        if games_for_team:
                            team_last_date[tid] = games_for_team[0].game_date

                    # Head-to-head series this season between each unique pair.
                    if upcoming_pairs:
                        for pg in past_games:
                            if not pg.home_team_id or not pg.road_team_id:
                                continue
                            pair = tuple(sorted([pg.home_team_id, pg.road_team_id]))
                            if pair not in upcoming_pairs:
                                continue
                            a_wins, b_wins = h2h_series.get(pair, (0, 0))
                            if pg.wining_team_id == pair[0]:
                                a_wins += 1
                            elif pg.wining_team_id == pair[1]:
                                b_wins += 1
                            h2h_series[pair] = (a_wins, b_wins)

            def _format_tipoff_local(iso_utc: str | None) -> str | None:
                if not iso_utc:
                    return None
                try:
                    from datetime import datetime, timezone
                    text = iso_utc.strip()
                    if text.endswith("Z"):
                        text = text[:-1] + "+00:00"
                    dt = datetime.fromisoformat(text)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    # Convert to ET for display (UTC-5 is EST; the API's gameEt
                    # is already ET so prefer that — this is a fallback).
                    return dt.strftime("%H:%M UTC")
                except Exception:
                    return None

            def _format_tipoff_et(iso_et: str | None) -> str | None:
                if not iso_et:
                    return None
                try:
                    from datetime import datetime
                    text = iso_et.strip()
                    if text.endswith("Z"):
                        text = text[:-1]
                    dt = datetime.fromisoformat(text)
                    return dt.strftime("%-I:%M %p ET")
                except Exception:
                    return None

            result = []
            for game in games:
                live_snapshot = live_map.get(game.game_id)
                # Stale DB rows for play-in games may have NULL team_ids before the
                # bracket is set; prefer the live snapshot when DB is missing.
                if live_snapshot:
                    if not game.home_team_id and live_snapshot.get("home_team_id"):
                        game.home_team_id = live_snapshot["home_team_id"]
                    if not game.road_team_id and live_snapshot.get("road_team_id"):
                        game.road_team_id = live_snapshot["road_team_id"]
                home_team = team_lookup.get(game.home_team_id)
                road_team = team_lookup.get(game.road_team_id)
                status = live_snapshot.get("status") if live_snapshot else get_game_status(game)
                live_card = fetch_live_card(game.game_id) if status == GAME_STATUS_LIVE else None
                winner_id = getattr(game, "wining_team_id", None)
                display_home_score = live_snapshot.get("home_score") if live_snapshot else game.home_team_score
                display_road_score = live_snapshot.get("road_score") if live_snapshot else game.road_team_score
                if (
                    winner_id is None
                    and status == GAME_STATUS_COMPLETED
                    and display_home_score is not None
                    and display_road_score is not None
                    and display_home_score != display_road_score
                ):
                    winner_id = game.home_team_id if display_home_score > display_road_score else game.road_team_id
                home_won = (
                    winner_id == game.home_team_id
                    if status == GAME_STATUS_COMPLETED and winner_id
                    else None
                )

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

                # Upcoming-only extras: records, last 10, H2H, tipoff time.
                home_rec = team_record.get(game.home_team_id) if status == GAME_STATUS_UPCOMING else None
                road_rec = team_record.get(game.road_team_id) if status == GAME_STATUS_UPCOMING else None
                home_l10 = team_last10.get(game.home_team_id) if status == GAME_STATUS_UPCOMING else None
                road_l10 = team_last10.get(game.road_team_id) if status == GAME_STATUS_UPCOMING else None

                def _rest_info(team_id: str | None):
                    if not team_id or not game.game_date:
                        return None
                    last = team_last_date.get(team_id)
                    if not last:
                        return None
                    days = (game.game_date - last).days
                    if days <= 0:
                        return None
                    return {
                        "days": days,
                        "is_b2b": days == 1,
                    }

                home_rest = _rest_info(game.home_team_id) if status == GAME_STATUS_UPCOMING else None
                road_rest = _rest_info(game.road_team_id) if status == GAME_STATUS_UPCOMING else None
                h2h_display = None
                if status == GAME_STATUS_UPCOMING and game.home_team_id and game.road_team_id:
                    pair = tuple(sorted([game.home_team_id, game.road_team_id]))
                    series = h2h_series.get(pair)
                    if series:
                        a_wins, b_wins = series
                        # Map back to home/road.
                        if pair[0] == game.home_team_id:
                            home_wins_h2h, road_wins_h2h = a_wins, b_wins
                        else:
                            home_wins_h2h, road_wins_h2h = b_wins, a_wins
                        if home_wins_h2h + road_wins_h2h > 0:
                            h2h_display = {
                                "home_wins": home_wins_h2h,
                                "road_wins": road_wins_h2h,
                            }
                tipoff_et_display = None
                if status == GAME_STATUS_UPCOMING and live_snapshot:
                    tipoff_et_display = _format_tipoff_et(live_snapshot.get("game_time_et"))
                    if not tipoff_et_display:
                        tipoff_et_display = _format_tipoff_local(live_snapshot.get("game_time_utc"))

                entry = {
                    "game_id": game.game_id,
                    "game_date": game_date,
                    "home_team_id": game.home_team_id,
                    "road_team_id": game.road_team_id,
                    "home_abbr": home_team.abbr if home_team else "TBD",
                    "road_abbr": road_team.abbr if road_team else "TBD",
                    "home_score": display_home_score,
                    "road_score": display_road_score,
                    "home_won": home_won,
                    "status": status,
                    "status_summary": (live_snapshot or {}).get("summary"),
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
                    "home_win_probability": None,
                    "road_win_probability": None,
                    "hot_player_ids": [],
                    "home_record": home_rec,
                    "road_record": road_rec,
                    "home_last10": home_l10,
                    "road_last10": road_l10,
                    "home_rest": home_rest,
                    "road_rest": road_rest,
                    "h2h": h2h_display,
                    "tipoff_et": tipoff_et_display,
                }
                # For LIVE games, override DB-sourced fields with live box-score
                # data (DB has no rows yet during play). Leaders, shooting pct,
                # win probability, and hot-player highlights all come from the
                # nba_api live BoxScore endpoint via the cached `fetch_live_card`.
                if live_card:
                    entry["home_fg_pct"] = live_card.get("home_fg_pct") or entry["home_fg_pct"]
                    entry["road_fg_pct"] = live_card.get("road_fg_pct") or entry["road_fg_pct"]
                    entry["home_fg3_pct"] = live_card.get("home_fg3_pct") or entry["home_fg3_pct"]
                    entry["road_fg3_pct"] = live_card.get("road_fg3_pct") or entry["road_fg3_pct"]
                    entry["home_scorer"] = live_card.get("home_scorer") or entry["home_scorer"]
                    entry["road_scorer"] = live_card.get("road_scorer") or entry["road_scorer"]
                    entry["home_rebounder"] = live_card.get("home_rebounder") or entry["home_rebounder"]
                    entry["road_rebounder"] = live_card.get("road_rebounder") or entry["road_rebounder"]
                    entry["home_assister"] = live_card.get("home_assister") or entry["home_assister"]
                    entry["road_assister"] = live_card.get("road_assister") or entry["road_assister"]
                    entry["home_win_probability"] = live_card.get("home_win_probability")
                    entry["road_win_probability"] = live_card.get("road_win_probability")
                    entry["hot_player_ids"] = live_card.get("hot_player_ids", [])
                result.append(entry)
            result.sort(
                key=lambda item: (
                    _game_status_rank(item["status"]),
                    item["game_date"],
                    item["game_id"],
                )
            )
            return result

    def teams_list_page():
        """/teams — SVG US map of all teams + chip grids + historical teams."""
        SessionLocal = get_session_local()
        Team = get_team_model()
        Game = get_game_model()
        TeamGameStats = get_team_game_stats_model()

        east_ids = {
            "1610612737", "1610612751", "1610612738", "1610612766", "1610612741",
            "1610612739", "1610612765", "1610612754", "1610612748", "1610612749",
            "1610612752", "1610612753", "1610612755", "1610612761", "1610612764",
        }

        with SessionLocal() as session:
            teams = (
                session.query(Team)
                .filter(Team.is_legacy.is_(False))
                .order_by(Team.full_name.asc())
                .all()
            )
            legacy_teams = (
                session.query(Team)
                .filter(Team.is_legacy.is_(True))
                .order_by(Team.full_name.asc())
                .all()
            )

        team_map_data = []
        team_map_positions = get_team_map_positions()
        for team in teams:
            pos = team_map_positions.get(team.abbr)
            if not pos:
                continue
            team_map_data.append(
                {
                    "abbr": team.abbr,
                    "full_name": get_display_team_name()(team),
                    "team_id": team.team_id,
                    "slug": team.slug,
                    "lat": pos[0],
                    "lon": pos[1],
                }
            )

        east_chips = sorted(
            [{"abbr": t.abbr, "slug": t.slug} for t in teams if t.team_id in east_ids],
            key=lambda x: x["abbr"] or "",
        )
        west_chips = sorted(
            [{"abbr": t.abbr, "slug": t.slug} for t in teams if t.team_id not in east_ids],
            key=lambda x: x["abbr"] or "",
        )

        # ── Build "ghost pins" for former franchise cities ────────────────
        from collections import defaultdict
        from web.historical_team_locations import (
            FRANCHISE_HISTORY,
            get_logo_for_year,
        )

        team_slug_by_id = {t.team_id: t.slug for t in teams}
        current_city_by_team = {
            era["team_id"]: era["city"]
            for era in FRANCHISE_HISTORY
            if era["year_end"] is None
        }

        grouped_eras: dict[tuple[str, str], list] = defaultdict(list)
        for era in FRANCHISE_HISTORY:
            grouped_eras[(era["team_id"], era["city"])].append(era)

        ghost_pins = []
        for (team_id, city), eras in grouped_eras.items():
            if current_city_by_team.get(team_id) == city:
                continue  # current city — main pin already covers it
            if team_id not in team_slug_by_id:
                continue  # team not in current 30 (shouldn't happen)
            eras.sort(key=lambda e: e["year_start"])
            year_start = eras[0]["year_start"]
            year_ends = [e["year_end"] for e in eras if e["year_end"] is not None]
            if not year_ends:
                continue
            year_end = max(year_ends)
            # Main era = longest-span entry in the group (for label / lat-lon)
            main_era = max(
                eras,
                key=lambda e: ((e["year_end"] or year_end) - e["year_start"]),
            )
            era_names = [e["era_name"] for e in eras]
            # Representative logo: try midpoint, then end, then start year
            mid = (year_start + year_end) // 2
            # Strict year lookup first; fall back to the current-era local
            # logo entry (year 9999 is the sentinel we use for "still current").
            # This guarantees every ghost pin gets a stable local file and
            # never depends on a remote CDN URL that might disappear.
            logo = (
                get_logo_for_year(team_id, mid)
                or get_logo_for_year(team_id, year_end)
                or get_logo_for_year(team_id, year_start)
                or get_logo_for_year(team_id, 9999)
            )
            logo_url = None
            if logo is not None:
                # logo['path'] is like 'static/team_logos/historical/1610612747/1947_1959.png'
                # Strip the leading 'static/' so url_for('static', filename=...) works.
                rel = logo["path"]
                if rel.startswith("static/"):
                    rel = rel[len("static/"):]
                from flask import url_for
                logo_url = url_for("static", filename=rel)
            ghost_pins.append(
                {
                    "team_id": team_id,
                    "slug": team_slug_by_id[team_id],
                    "franchise": main_era["franchise"],
                    "era_name": main_era["era_name"],
                    "era_names": era_names,
                    "city": city,
                    "state": main_era["state"],
                    "year_start": year_start,
                    "year_end": year_end,
                    "lat": main_era["lat"],
                    "lon": main_era["lon"],
                    "logo_url": logo_url,
                }
            )

        return get_render_template()(
            "teams_list.html",
            team_map_data=team_map_data,
            east_chips=east_chips,
            west_chips=west_chips,
            legacy_teams=legacy_teams,
            ghost_pins=ghost_pins,
        )

    def _build_top_scorers(limit: int = 5) -> dict:
        """Top-N pts leaders from the most recent completed game date.

        Returns {'game_date': date, 'rows': [{player_id, full_name, slug, pts, team_abbr, team_slug}]}
        or empty dict if no recent games.
        """
        SessionLocal = get_session_local()
        Game = get_game_model()
        PlayerGameStats = get_player_game_stats_model()
        Player = get_player_model()
        Team = get_team_model()

        with SessionLocal() as session:
            last_game_date = (
                session.query(func.max(Game.game_date))
                .filter(Game.home_team_score.isnot(None))
                .scalar()
            )
            if not last_game_date:
                return {}
            game_ids_sq = (
                session.query(Game.game_id)
                .filter(Game.game_date == last_game_date)
                .subquery()
            )
            rows = (
                session.query(
                    PlayerGameStats.player_id,
                    PlayerGameStats.pts,
                    PlayerGameStats.team_id,
                    Player.full_name,
                    Player.full_name_zh,
                    Player.slug.label("player_slug"),
                    Team.abbr,
                    Team.slug.label("team_slug"),
                )
                .join(Player, PlayerGameStats.player_id == Player.player_id)
                .outerjoin(Team, PlayerGameStats.team_id == Team.team_id)
                .filter(
                    PlayerGameStats.game_id.in_(session.query(game_ids_sq.c.game_id)),
                    PlayerGameStats.pts.isnot(None),
                )
                .order_by(PlayerGameStats.pts.desc())
                .limit(limit)
                .all()
            )
            headshot_fn = get_player_headshot_url()
            return {
                "game_date": last_game_date,
                "rows": [
                    {
                        "player_id": r.player_id,
                        "full_name": (r.full_name_zh if get_is_zh() and r.full_name_zh else r.full_name),
                        "slug": r.player_slug,
                        "pts": int(r.pts or 0),
                        "team_abbr": r.abbr,
                        "team_slug": r.team_slug,
                        "team_id": r.team_id,
                        "headshot_url": headshot_fn(r.player_id) if headshot_fn else None,
                    }
                    for r in rows
                ],
            }

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
            legacy_teams = (
                session.query(Team)
                .filter(Team.is_legacy.is_(True))
                .order_by(Team.full_name.asc())
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
                        "slug": team.slug if team else None,
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
                        "slug": team.slug,
                        "lat": pos[0],
                        "lon": pos[1],
                    }
                )

        today_games_data = _build_today_games(team_lookup)
        news_entries = _build_home_news(team_lookup)
        top_scorers = _build_top_scorers()

        games_active = [g for g in today_games_data if g.get("status") in (GAME_STATUS_LIVE, GAME_STATUS_COMPLETED)]
        upcoming_games = [
            g for g in today_games_data
            if g.get("status") == GAME_STATUS_UPCOMING
            and (g.get("home_team_id") or g.get("road_team_id"))
        ]

        return get_render_template()(
            "home.html",
            teams=teams,
            legacy_teams=legacy_teams,
            team_map_data=team_map_data,
            east_standings=east_standings,
            west_standings=west_standings,
            standing_season_ids=standing_season_ids,
            selected_standing_season=selected_standing_season,
            fmt_season=get_season_label(),
            today_games=today_games_data,
            games_active=games_active,
            upcoming_games=upcoming_games,
            news_entries=news_entries,
            top_scorers=top_scorers,
        )

    def _build_home_news(team_lookup: dict) -> list[dict]:
        """Top-scored news clusters for the home feed."""
        SessionLocal = get_session_local()
        Player = get_player_model()
        Team = get_team_model()
        from db.models import (
            NewsArticle,
            NewsArticlePlayer,
            NewsArticleTeam,
            NewsCluster,
        )

        with SessionLocal() as session:
            clusters = (
                session.query(NewsCluster)
                .filter(NewsCluster.representative_article_id.isnot(None))
                .order_by(NewsCluster.score.desc())
                .limit(15)
                .all()
            )
            if not clusters:
                return []

            rep_ids = [c.representative_article_id for c in clusters if c.representative_article_id]
            rep_rows = (
                session.query(NewsArticle)
                .filter(NewsArticle.id.in_(rep_ids))
                .all()
            )
            rep_by_id = {a.id: a for a in rep_rows}

            player_rows = (
                session.query(NewsArticlePlayer.article_id, Player.player_id, Player.full_name, Player.full_name_zh, Player.slug)
                .join(Player, NewsArticlePlayer.player_id == Player.player_id)
                .filter(NewsArticlePlayer.article_id.in_(rep_ids))
                .all()
            )
            players_by_article: dict[int, list[dict]] = defaultdict(list)
            for row in player_rows:
                players_by_article[row.article_id].append(
                    {"player_id": row.player_id, "full_name": row.full_name, "full_name_zh": row.full_name_zh, "slug": row.slug}
                )

            team_rows = (
                session.query(NewsArticleTeam.article_id, Team.team_id, Team.full_name, Team.full_name_zh, Team.abbr, Team.slug)
                .join(Team, NewsArticleTeam.team_id == Team.team_id)
                .filter(NewsArticleTeam.article_id.in_(rep_ids))
                .all()
            )
            teams_by_article: dict[int, list[dict]] = defaultdict(list)
            for row in team_rows:
                teams_by_article[row.article_id].append(
                    {"team_id": row.team_id, "full_name": row.full_name, "full_name_zh": row.full_name_zh, "abbr": row.abbr, "slug": row.slug}
                )

            entries: list[dict] = []
            for cluster in clusters:
                rep = rep_by_id.get(cluster.representative_article_id)
                if rep is None:
                    continue
                entries.append(
                    {
                        "cluster_id": cluster.id,
                        "article_id": rep.id,
                        "title": rep.title,
                        "summary": rep.summary or "",
                        "source": rep.source,
                        "url": rep.url,
                        "thumbnail_url": rep.thumbnail_url,
                        "published_at": rep.published_at,
                        "article_count": cluster.article_count or 1,
                        "unique_view_count": cluster.unique_view_count or 0,
                        "players": players_by_article.get(rep.id, []),
                        "teams": teams_by_article.get(rep.id, []),
                    }
                )
            return entries

    def news_detail(cluster_id: int):
        SessionLocal = get_session_local()
        Player = get_player_model()
        Team = get_team_model()
        from db.models import (
            NewsArticle,
            NewsArticlePlayer,
            NewsArticleTeam,
            NewsCluster,
        )

        with SessionLocal() as session:
            cluster = session.get(NewsCluster, cluster_id)
            if cluster is None:
                abort(404)
            rep = session.get(NewsArticle, cluster.representative_article_id) if cluster.representative_article_id else None
            if rep is None:
                abort(404)

            siblings = (
                session.query(NewsArticle)
                .filter(NewsArticle.cluster_id == cluster.id, NewsArticle.id != rep.id)
                .order_by(NewsArticle.published_at.desc())
                .all()
            )

            cluster_article_ids = [rep.id] + [s.id for s in siblings]
            player_rows = (
                session.query(Player.player_id, Player.full_name, Player.full_name_zh, Player.slug)
                .join(NewsArticlePlayer, NewsArticlePlayer.player_id == Player.player_id)
                .filter(NewsArticlePlayer.article_id.in_(cluster_article_ids))
                .distinct()
                .all()
            )
            team_rows = (
                session.query(Team.team_id, Team.full_name, Team.full_name_zh, Team.abbr, Team.slug)
                .join(NewsArticleTeam, NewsArticleTeam.team_id == Team.team_id)
                .filter(NewsArticleTeam.article_id.in_(cluster_article_ids))
                .distinct()
                .all()
            )

            entry = {
                "cluster": {
                    "id": cluster.id,
                    "article_count": cluster.article_count or 1,
                    "unique_view_count": cluster.unique_view_count or 0,
                },
                "article": {
                    "id": rep.id,
                    "title": rep.title,
                    "summary": rep.summary or "",
                    "source": rep.source,
                    "url": rep.url,
                    "thumbnail_url": rep.thumbnail_url,
                    "published_at": rep.published_at,
                },
                "siblings": [
                    {
                        "id": s.id,
                        "title": s.title,
                        "source": s.source,
                        "url": s.url,
                        "published_at": s.published_at,
                    }
                    for s in siblings
                ],
                "players": [
                    {"player_id": r.player_id, "full_name": r.full_name, "full_name_zh": r.full_name_zh, "slug": r.slug}
                    for r in player_rows
                ],
                "teams": [
                    {"team_id": r.team_id, "full_name": r.full_name, "full_name_zh": r.full_name_zh, "abbr": r.abbr, "slug": r.slug}
                    for r in team_rows
                ],
            }

        return get_render_template()("news_detail.html", entry=entry)

    def _group_by_date(entries):
        """Group a pre-sorted list of game entries by game_date."""
        return [(dt, list(g)) for dt, g in _groupby(entries, key=lambda e: e.game_date)]

    _WD_EN = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    _WD_ZH = ["\u5468\u4e00", "\u5468\u4e8c", "\u5468\u4e09", "\u5468\u56db", "\u5468\u4e94", "\u5468\u516d", "\u5468\u65e5"]
    _MON_EN = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

    def games_list():
        SessionLocal = get_session_local()
        Game = get_game_model()
        Team = get_team_model()

        page_size = 30
        today = date.today()
        is_zh = get_is_zh()

        def fmt_date_header(d):
            if d is None:
                return "-"
            wd = _WD_ZH[d.weekday()] if is_zh else _WD_EN[d.weekday()]
            if is_zh:
                label = f"{d.month}\u6708{d.day}\u65e5 {wd}"
            else:
                label = f"{_MON_EN[d.month - 1]} {d.day}, {d.year} ({wd})"
            if d == today:
                label += " \u00b7 " + ("\u4eca\u5929" if is_zh else "Today")
            elif d == today - timedelta(days=1):
                label += " \u00b7 " + ("\u6628\u5929" if is_zh else "Yesterday")
            return label

        with SessionLocal() as session:
            live_map = fetch_live_scoreboard_map()
            all_season_ids = sorted(
                {row.season for row in session.query(Game.season).filter(Game.season.isnot(None)).all()},
                key=get_season_sort_key(),
                reverse=True,
            )

            def _pick_active_season() -> str | None:
                """Pick the season whose phase is most relevant today.

                Looks for games within a ±3 day window around today. If any are
                play-in or playoffs, prefer those over regular season. Falls
                back to the newest season otherwise.
                """
                if not all_season_ids:
                    return None
                window_start = today - timedelta(days=3)
                window_end = today + timedelta(days=3)
                recent = (
                    session.query(Game.season)
                    .filter(
                        Game.game_date >= window_start,
                        Game.game_date <= window_end,
                        Game.season.isnot(None),
                    )
                    .distinct()
                    .all()
                )
                recent_seasons = {row.season for row in recent}
                if not recent_seasons:
                    return all_season_ids[0]
                # Prefer later phase: Playoffs > PlayIn > Regular.
                phase_rank = {"4": 3, "5": 2, "2": 1}
                scored = [
                    (phase_rank.get(str(s)[:1], 0), s)
                    for s in recent_seasons
                ]
                scored.sort(reverse=True)
                return scored[0][1]

            selected_season = request.args.get("season") or _pick_active_season()
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

            all_games = games_q.all()
            all_games = _supplement_missing_live_games(
                all_games,
                live_map,
                selected_season=selected_season,
                selected_team=selected_team,
            )
            live_games = []
            completed_all = []
            upcoming_games = []
            for game in all_games:
                entry = _build_game_list_entry(game, live_map.get(game.game_id))
                if entry.status == GAME_STATUS_LIVE:
                    live_games.append(entry)
                elif entry.status == GAME_STATUS_UPCOMING:
                    upcoming_games.append(entry)
                else:
                    completed_all.append(entry)

            live_games.sort(key=lambda item: (item.game_date, item.game_id))
            completed_all.sort(key=lambda item: (item.game_date, item.game_id), reverse=True)
            upcoming_games.sort(key=lambda item: (item.game_date, item.game_id))

            # Determine active view
            view = request.args.get("view", "").strip().lower()
            has_live = bool(live_games)
            if view not in ("results", "schedule", "live"):
                view = "live" if has_live else "results"
            if view == "live" and not has_live:
                view = "results"

            # Paginate based on active view
            if view == "results":
                paginate_source = completed_all
            elif view == "schedule":
                paginate_source = upcoming_games
            else:
                paginate_source = []

            total = len(paginate_source)
            total_pages = max(1, (total + page_size - 1) // page_size) if paginate_source else 1
            page = min(page, total_pages)
            start = (page - 1) * page_size
            end = start + page_size
            paginated = paginate_source[start:end] if paginate_source else []

            date_groups = _group_by_date(paginated)

            # Live view extras: live games grouped + today's completed
            live_date_groups = _group_by_date(live_games) if view == "live" else []
            today_completed = [e for e in completed_all if e.game_date == today] if view == "live" else []
            today_completed_groups = _group_by_date(today_completed) if today_completed else []

            team_lookup = get_team_map(session)
            selected_team_obj = next((team for team in all_teams if team.team_id == selected_team), None)
            if selected_team_obj is None and selected_team:
                selected_team_obj = team_lookup.get(selected_team)

        return get_render_template()(
            "games_list.html",
            view=view,
            has_live=has_live,
            date_groups=date_groups,
            live_date_groups=live_date_groups,
            today_completed_groups=today_completed_groups,
            live_count=len(live_games),
            results_count=len(completed_all),
            schedule_count=len(upcoming_games),
            team_lookup=team_lookup,
            all_teams=all_teams,
            all_season_ids=all_season_ids,
            selected_season=selected_season,
            selected_team=selected_team,
            selected_team_obj=selected_team_obj,
            fmt_date=get_fmt_date(),
            fmt_date_header=fmt_date_header,
            fmt_season=get_season_label(),
            today=today,
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
                "slug": player.slug or f"player-{player.player_id}",
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

    def players_browse():
        SessionLocal = get_session_local()
        Player = get_player_model()
        PlayerGameStats = get_player_game_stats_model()
        Team = get_team_model()
        Game = get_game_model()

        with SessionLocal() as session:
            latest_season = _latest_regular_season(session)

            # Available regular seasons for the dropdown
            season_ids = sorted(
                [
                    row.season
                    for row in session.query(Game.season).filter(Game.season.like("2%")).distinct().all()
                ],
                key=get_season_sort_key(),
                reverse=True,
            )
            selected_season = request.args.get("season") or latest_season

            # Get current teams (non-legacy)
            teams = (
                session.query(Team)
                .filter(Team.is_legacy == False)
                .order_by(Team.full_name.asc())
                .all()
            )
            team_lookup = {t.team_id: t for t in teams}

            # Get each player's most recent team in the selected season
            latest_team_sub = (
                session.query(
                    PlayerGameStats.player_id,
                    PlayerGameStats.team_id,
                    func.max(Game.game_date).label("last_date"),
                )
                .join(Game, Game.game_id == PlayerGameStats.game_id)
                .filter(
                    Game.season == selected_season,
                    PlayerGameStats.team_id.isnot(None),
                )
                .group_by(PlayerGameStats.player_id, PlayerGameStats.team_id)
                .subquery()
            )

            ranked_sub = (
                session.query(
                    latest_team_sub.c.player_id,
                    latest_team_sub.c.team_id,
                    func.row_number()
                    .over(
                        partition_by=latest_team_sub.c.player_id,
                        order_by=latest_team_sub.c.last_date.desc(),
                    )
                    .label("rn"),
                )
                .subquery()
            )

            player_team_rows = (
                session.query(ranked_sub.c.player_id, ranked_sub.c.team_id)
                .filter(ranked_sub.c.rn == 1)
                .all()
            )
            player_team_map = {row.player_id: row.team_id for row in player_team_rows}

            # All non-team players that have stats in this season
            player_ids_in_season = set(player_team_map.keys())
            players = (
                session.query(Player)
                .filter(
                    Player.player_id.in_(player_ids_in_season),
                    Player.is_team == False,
                )
                .order_by(Player.full_name.asc())
                .all()
            )

            # Bulk season averages: GP, PPG, RPG, APG
            played_condition = or_(
                PlayerGameStats.min > 0,
                PlayerGameStats.sec > 0,
                PlayerGameStats.pts > 0,
            )
            stats_rows = (
                session.query(
                    PlayerGameStats.player_id,
                    func.sum(case((played_condition, 1), else_=0)).label("gp"),
                    func.sum(func.coalesce(PlayerGameStats.pts, 0)).label("pts"),
                    func.sum(func.coalesce(PlayerGameStats.reb, 0)).label("reb"),
                    func.sum(func.coalesce(PlayerGameStats.ast, 0)).label("ast"),
                )
                .join(Game, Game.game_id == PlayerGameStats.game_id)
                .filter(
                    Game.season == selected_season,
                    PlayerGameStats.player_id.in_(player_ids_in_season),
                )
                .group_by(PlayerGameStats.player_id)
                .all()
            )
            player_stats = {}
            for row in stats_rows:
                gp = int(row.gp or 0)
                if gp > 0:
                    player_stats[row.player_id] = {
                        "gp": gp,
                        "ppg": f"{int(row.pts or 0) / gp:.1f}",
                        "rpg": f"{int(row.reb or 0) / gp:.1f}",
                        "apg": f"{int(row.ast or 0) / gp:.1f}",
                    }
                else:
                    player_stats[row.player_id] = {"gp": 0, "ppg": "-", "rpg": "-", "apg": "-"}
            empty_stats = {"gp": 0, "ppg": "-", "rpg": "-", "apg": "-"}

            # Build team→players map
            teams_with_players = []
            is_zh = get_is_zh()
            for team in teams:
                team_players = []
                for p in players:
                    if player_team_map.get(p.player_id) == team.team_id:
                        st = player_stats.get(p.player_id, empty_stats)
                        team_players.append({
                            "player_id": p.player_id,
                            "full_name": p.full_name_zh if is_zh and p.full_name_zh else p.full_name,
                            "position": p.position or "",
                            "jersey": p.jersey or "",
                            **st,
                        })
                if team_players:
                    team_players.sort(key=lambda x: x["full_name"])
                    teams_with_players.append({
                        "team_id": team.team_id,
                        "abbr": team.abbr,
                        "full_name": team.full_name_zh if is_zh and team.full_name_zh else team.full_name,
                        "players": team_players,
                    })

            player_count = sum(len(t["players"]) for t in teams_with_players)

        t = get_t()
        render_template = get_render_template()
        return render_template(
            "players.html",
            teams_with_players=teams_with_players,
            player_count=player_count,
            selected_season=selected_season,
            season_ids=season_ids,
        )

    def api_games_live():
        scoreboard = fetch_live_scoreboard_map()
        games = []
        for game_id, snapshot in scoreboard.items():
            entry = dict(snapshot)
            if snapshot.get("status") == GAME_STATUS_LIVE:
                card = fetch_live_card(game_id)
                if card:
                    entry.update({
                        "home_fg_pct": card.get("home_fg_pct"),
                        "road_fg_pct": card.get("road_fg_pct"),
                        "home_fg3_pct": card.get("home_fg3_pct"),
                        "road_fg3_pct": card.get("road_fg3_pct"),
                        "home_scorer": card.get("home_scorer"),
                        "road_scorer": card.get("road_scorer"),
                        "home_rebounder": card.get("home_rebounder"),
                        "road_rebounder": card.get("road_rebounder"),
                        "home_assister": card.get("home_assister"),
                        "road_assister": card.get("road_assister"),
                        "home_win_probability": card.get("home_win_probability"),
                        "road_win_probability": card.get("road_win_probability"),
                        "hot_player_ids": card.get("hot_player_ids", []),
                    })
            games.append(entry)
        return jsonify({"games": games})

    def api_game_live(game_id: str):
        payload = fetch_live_game_detail(game_id)
        if payload is None:
            return jsonify({"ok": False, "game_id": game_id, "error": "live_data_unavailable"}), 503
        # Also merge in the cached live_card (leaders, WP, hot player ids,
        # shooting percentages) so the game-page live panel can update in
        # the same round-trip as the scoreboard.
        try:
            card = fetch_live_card(game_id)
        except Exception:
            card = None
        if card:
            for key in (
                "home_scorer", "road_scorer",
                "home_rebounder", "road_rebounder",
                "home_assister", "road_assister",
                "home_fg_pct", "road_fg_pct",
                "home_fg3_pct", "road_fg3_pct",
                "home_win_probability", "road_win_probability",
                "hot_player_ids",
            ):
                payload[key] = card.get(key)
        return jsonify({"ok": True, **payload})

    app.add_url_rule("/cn/", endpoint="home_zh", view_func=home)
    app.add_url_rule("/", endpoint="home", view_func=home)
    app.add_url_rule("/teams", endpoint="teams_list_page", view_func=teams_list_page)
    app.add_url_rule("/cn/teams", endpoint="teams_list_page_zh", view_func=teams_list_page)
    app.add_url_rule("/news/<int:cluster_id>", endpoint="news_detail", view_func=news_detail)
    app.add_url_rule("/cn/news/<int:cluster_id>", endpoint="news_detail_zh", view_func=news_detail)
    app.add_url_rule("/cn/games", endpoint="games_list_zh", view_func=games_list)
    app.add_url_rule("/games", endpoint="games_list", view_func=games_list)
    app.add_url_rule("/cn/awards", endpoint="awards_page_zh", view_func=awards_page)
    app.add_url_rule("/awards", endpoint="awards_page", view_func=awards_page)
    app.add_url_rule("/api/players/hints", endpoint="player_hints_api", view_func=limiter.limit("60 per minute")(player_hints_api))
    app.add_url_rule("/cn/players", endpoint="players_browse_zh", view_func=players_browse)
    app.add_url_rule("/players", endpoint="players_browse", view_func=players_browse)
    app.add_url_rule("/api/games/live", endpoint="api_games_live", view_func=api_games_live)
    app.add_url_rule("/api/games/<game_id>/live", endpoint="api_game_live", view_func=api_game_live)
    app.add_url_rule("/cn/players/compare", endpoint="players_compare_zh", view_func=players_compare)
    app.add_url_rule("/players/compare", endpoint="players_compare", view_func=players_compare)
    app.add_url_rule("/cn/draft/<int:year>", endpoint="draft_page_zh", view_func=draft_page)
    app.add_url_rule("/draft/<int:year>", endpoint="draft_page", view_func=draft_page)

    return SimpleNamespace(
        home=home,
        games_list=games_list,
        awards_page=awards_page,
        players_browse=players_browse,
        api_games_live=api_games_live,
        api_game_live=api_game_live,
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
