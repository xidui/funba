from __future__ import annotations

import json
import threading
from collections import defaultdict
from datetime import date, timedelta
from itertools import groupby as _groupby
from types import SimpleNamespace
from typing import Any, Callable

from flask import abort, jsonify, make_response, request
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


def _coerce_game_date(value):
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return value
    return value


def _home_cached_payload_needs_curated_refresh(game, payload: dict | None) -> bool:
    if not isinstance(payload, dict) or payload.get("_curated_merged"):
        return False
    return bool(
        getattr(game, "highlights_curated_json", None)
        or getattr(game, "highlights_curated_player_json", None)
        or getattr(game, "highlights_curated_team_json", None)
    )


def _load_news_models():
    """Best-effort loader for optional news models.

    Some unit tests install a lightweight fake `db.models` module that omits
    news tables entirely. The home/news pages should degrade cleanly instead of
    failing import-time inside a request.
    """
    try:
        from db.models import NewsArticle, NewsArticlePlayer, NewsArticleTeam, NewsCluster
    except (ImportError, AttributeError):
        return None
    return NewsArticle, NewsArticlePlayer, NewsArticleTeam, NewsCluster


def _build_bracket_series(games):
    """Build a single series summary from a list of games in that series."""
    if not games:
        return None
    games.sort(key=lambda g: str(g.game_id))
    top = games[0].home_team_id  # higher seed has home court in game 1
    bot = games[0].road_team_id
    tw = sum(1 for g in games if str(getattr(g, "wining_team_id", "") or "") == str(top))
    bw = sum(1 for g in games if str(getattr(g, "wining_team_id", "") or "") == str(bot))
    winner = top if tw >= 4 else (bot if bw >= 4 else None)
    return dict(top=top, bot=bot, tw=tw, bw=bw, winner=winner, ts=None, bs=None)


_EMPTY_BRACKET = dict(
    er1=[None] * 4, er2=[None] * 2, ecf=None,
    wr1=[None] * 4, wr2=[None] * 2, wcf=None,
    finals=None,
)


# Eastern Conference franchise IDs (post-1971 conference era). Used to assign
# topology-derived bracket halves to the East/West labels in the template
# when the game-ID format pre-dates the modern round/series encoding.
# Hornets/Pelicans (1610612740) is intentionally omitted — they were East
# only in 2002-04, but those years use the modern game-ID format anyway.
_EASTERN_CONFERENCE_TEAM_IDS = {
    "1610612737",  # ATL
    "1610612738",  # BOS
    "1610612751",  # NJN/BKN
    "1610612766",  # CHA (Hornets/Bobcats)
    "1610612741",  # CHI
    "1610612739",  # CLE
    "1610612765",  # DET
    "1610612754",  # IND
    "1610612748",  # MIA
    "1610612749",  # MIL
    "1610612752",  # NYK
    "1610612753",  # ORL
    "1610612755",  # PHI
    "1610612761",  # TOR
    "1610612764",  # WAS/WSB
}


def _has_modern_playoff_id_encoding(games) -> bool:
    """Modern (~2001+) playoff game IDs put the round digit at position 7
    (e.g. `0042200101`); older IDs (pre-2001) are sequential and always have
    `0` there (e.g. `0049200001`)."""
    for g in games:
        gid = str(getattr(g, "game_id", "") or "")
        if len(gid) >= 10 and gid[7] in "1234":
            return True
    return False


def _build_playoff_bracket(games):
    """Parse playoff game list into bracket structure keyed by round/series.

    Falls back to topology-based construction for older seasons whose game
    IDs do not encode round/series at positions 7/8.
    """
    if not games:
        return dict(_EMPTY_BRACKET)
    if not _has_modern_playoff_id_encoding(games):
        return _build_playoff_bracket_from_topology(games)

    _R1_SEEDS = {
        0: (1, 8), 1: (4, 5), 2: (3, 6), 3: (2, 7),
        4: (1, 8), 5: (4, 5), 6: (3, 6), 7: (2, 7),
    }
    series_map: dict[tuple[int, int], list] = {}
    for g in games:
        gid = str(g.game_id)
        if len(gid) < 10:
            continue
        rnd, sidx = int(gid[7]), int(gid[8])
        series_map.setdefault((rnd, sidx), []).append(g)

    def _s(rnd, idx):
        s = _build_bracket_series(series_map.get((rnd, idx), []))
        if s and rnd == 1:
            seeds = _R1_SEEDS.get(idx)
            if seeds:
                s["ts"], s["bs"] = seeds
        return s

    return dict(
        er1=[_s(1, i) for i in range(4)],
        er2=[_s(2, i) for i in range(2)],
        ecf=_s(3, 0),
        wr1=[_s(1, i + 4) for i in range(4)],
        wr2=[_s(2, i + 2) for i in range(2)],
        wcf=_s(3, 1),
        finals=_s(4, 0),
    )


def _build_playoff_bracket_from_topology(games):
    """Derive bracket from team-pair grouping + chronology.

    For older seasons where game IDs are sequential, we can't read the round
    off the ID. Instead: group games into series (one per team pair), find
    the chronologically last series (= NBA Finals), then walk backwards
    through each finalist's prior series to reconstruct R2 and R1.
    """
    pair_games: dict[tuple[str, str], list] = defaultdict(list)
    for g in games:
        if not g.home_team_id or not g.road_team_id:
            continue
        pair = tuple(sorted([str(g.home_team_id), str(g.road_team_id)]))
        pair_games[pair].append(g)

    def _series(pair, gs):
        gs.sort(key=lambda g: ((g.game_date or date.min), str(g.game_id)))
        first = gs[0]
        top = str(first.home_team_id)
        bot = str(first.road_team_id)
        tw = sum(1 for g in gs if str(getattr(g, "wining_team_id", "") or "") == top)
        bw = sum(1 for g in gs if str(getattr(g, "wining_team_id", "") or "") == bot)
        winner = top if tw > bw else (bot if bw > tw else None)
        return {
            "pair": pair, "top": top, "bot": bot,
            "tw": tw, "bw": bw, "winner": winner,
            "ts": None, "bs": None,
            "start": gs[0].game_date, "end": gs[-1].game_date,
        }

    series = [_series(p, gs) for p, gs in pair_games.items()]
    if not series:
        return dict(_EMPTY_BRACKET)
    series.sort(key=lambda s: (s["end"] or date.min, s["start"] or date.min))

    finals = series[-1]
    used = {finals["pair"]}

    def _won_before(team, before):
        if before is None:
            return None
        cand = [
            s for s in series
            if s["pair"] not in used
            and s["winner"] == team
            and s["end"] is not None
            and s["end"] < before
        ]
        return max(cand, key=lambda s: s["end"]) if cand else None

    def _build_side(finalist):
        cf = _won_before(finalist, finals["start"])
        if cf is None:
            return [None] * 4, [None] * 2, None
        used.add(cf["pair"])
        # Each CF participant is the winner of an R2 series.
        r2_list = []
        for participant in (cf["top"], cf["bot"]):
            r2 = _won_before(participant, cf["start"])
            if r2:
                used.add(r2["pair"])
            r2_list.append(r2)
        # Each R2 participant is the winner of an R1 series. Order: for each
        # R2 series, push its top participant's R1 then its bot's — keeps
        # siblings adjacent in the rendered bracket tree.
        r1_list = []
        for r2 in r2_list:
            if r2 is None:
                r1_list.extend([None, None])
                continue
            for participant in (r2["top"], r2["bot"]):
                r1 = _won_before(participant, r2["start"])
                if r1:
                    used.add(r1["pair"])
                r1_list.append(r1)
        return r1_list, r2_list, cf

    top_side = _build_side(finals["top"])
    bot_side = _build_side(finals["bot"])

    # Place each side under the East/West label using the historical
    # conference of the finals participants. Default top→East, bot→West when
    # neither (or both) is in the East lookup.
    top_east = finals["top"] in _EASTERN_CONFERENCE_TEAM_IDS
    bot_east = finals["bot"] in _EASTERN_CONFERENCE_TEAM_IDS
    if bot_east and not top_east:
        east, west = bot_side, top_side
    else:
        east, west = top_side, bot_side

    return dict(
        er1=east[0], er2=east[1], ecf=east[2],
        wr1=west[0], wr2=west[1], wcf=west[2],
        finals=finals,
    )


def _build_playin_bracket(games):
    """Parse play-in games into bracket structure."""
    series_map: dict[tuple[int, int], list] = {}
    for g in games:
        gid = str(g.game_id)
        if len(gid) < 10:
            continue
        rnd, sidx = int(gid[7]), int(gid[8])
        series_map.setdefault((rnd, sidx), []).append(g)

    def _g(rnd, idx):
        """Build a single-game 'series' for play-in."""
        gs = series_map.get((rnd, idx), [])
        if not gs:
            return None
        g = gs[0]
        winner = getattr(g, "wining_team_id", None)
        return dict(
            top=g.home_team_id, bot=g.road_team_id,
            top_score=g.home_team_score, bot_score=g.road_team_score,
            winner=str(winner) if winner else None,
            game_date=g.game_date,
        )

    return dict(
        east_78=_g(1, 0), east_910=_g(1, 1), east_final=_g(2, 0),
        west_78=_g(1, 2), west_910=_g(1, 3), west_final=_g(2, 1),
    )


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
        game_date=_coerce_game_date(game.game_date),
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
    allowed_seasons: set[str] | None = None,
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
        if allowed_seasons and snapshot.get("season") not in allowed_seasons:
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
    get_cached_game_metrics_payload: Callable[[str], dict | None] | None = None,
    get_load_game_metrics_payload: Callable[[str], dict] | None = None,
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
        """/teams — Timeline scrubber over franchise history.

        Single dynamic map: drag the year slider to see the NBA as it was
        that season (teams at their era city, era logos, era names). Below
        the map: division standings card for any year where we have results,
        plus East/West chip quick-links at the bottom.
        """
        SessionLocal = get_session_local()
        Team = get_team_model()
        Game = get_game_model()

        from collections import defaultdict as _dd
        from flask import url_for
        from web.historical_team_locations import (
            FRANCHISE_HISTORY,
            FRANCHISE_LOGOS,
            DEFUNCT_FRANCHISES,
            get_logo_for_year,
            get_current_logo,
        )

        # Current NBA 6-division layout (in effect 2004-present).
        DIVISIONS_2004 = {
            # Atlantic
            "1610612738": ("E", "Atlantic"),   # BOS
            "1610612751": ("E", "Atlantic"),   # BKN
            "1610612752": ("E", "Atlantic"),   # NYK
            "1610612755": ("E", "Atlantic"),   # PHI
            "1610612761": ("E", "Atlantic"),   # TOR
            # Central
            "1610612741": ("E", "Central"),    # CHI
            "1610612739": ("E", "Central"),    # CLE
            "1610612765": ("E", "Central"),    # DET
            "1610612754": ("E", "Central"),    # IND
            "1610612749": ("E", "Central"),    # MIL
            # Southeast
            "1610612737": ("E", "Southeast"),  # ATL
            "1610612766": ("E", "Southeast"),  # CHA
            "1610612748": ("E", "Southeast"),  # MIA
            "1610612753": ("E", "Southeast"),  # ORL
            "1610612764": ("E", "Southeast"),  # WAS
            # Northwest
            "1610612743": ("W", "Northwest"),  # DEN
            "1610612750": ("W", "Northwest"),  # MIN
            "1610612760": ("W", "Northwest"),  # OKC
            "1610612757": ("W", "Northwest"),  # POR
            "1610612762": ("W", "Northwest"),  # UTA
            # Pacific
            "1610612744": ("W", "Pacific"),    # GSW
            "1610612746": ("W", "Pacific"),    # LAC
            "1610612747": ("W", "Pacific"),    # LAL
            "1610612756": ("W", "Pacific"),    # PHX
            "1610612758": ("W", "Pacific"),    # SAC
            # Southwest
            "1610612742": ("W", "Southwest"),  # DAL
            "1610612745": ("W", "Southwest"),  # HOU
            "1610612763": ("W", "Southwest"),  # MEM
            "1610612740": ("W", "Southwest"),  # NOP
            "1610612759": ("W", "Southwest"),  # SAS
        }

        def _resolve_logo_url(team_id: str, year: int) -> str | None:
            logo = (
                get_logo_for_year(team_id, year)
                or get_current_logo(team_id)
            )
            if logo is None:
                return None
            rel = logo["path"]
            if rel.startswith("static/"):
                rel = rel[len("static/"):]
            return url_for("static", filename=rel)

        with SessionLocal() as session:
            # Slugs we will link to. Keyed by team_id for the 30 current
            # franchises; defunct franchises use their legacy Team row slug.
            current_teams = (
                session.query(Team)
                .filter(Team.is_legacy.is_(False))
                .all()
            )
            current_team_by_id = {t.team_id: t for t in current_teams}

            defunct_team_ids = [d["team_id"] for d in DEFUNCT_FRANCHISES]
            legacy_teams = (
                session.query(Team)
                .filter(Team.team_id.in_(defunct_team_ids))
                .all()
            ) if defunct_team_ids else []
            legacy_by_id = {t.team_id: t for t in legacy_teams}

            # Per-season regular-season W-L for every team that played
            # (across ALL seasons with results). Season code "22025" = 22025
            # regular season starting in 2025. We derive the start year from
            # the trailing 4 digits.
            all_games = (
                session.query(
                    Game.season,
                    Game.home_team_id,
                    Game.road_team_id,
                    Game.wining_team_id,
                )
                .filter(Game.wining_team_id.isnot(None))
                .filter(Game.season.like("2%"))  # regular season only
                .all()
            )

        # ── Per-team logo timeline (frontend picks by year) ──────────────
        # Ship every FRANCHISE_LOGOS entry grouped by team_id as
        # {team_id: [{year_start, year_end, url}, ...]}. The frontend selects
        # the most specific entry covering the slider year. A one-shot per-era
        # representative logo picked on the backend can't track within-era
        # rebrands (Lakers 1960 vs 2001 vs 2018 are all the same "LA Lakers"
        # era), so year-accurate logo resolution has to happen client-side.
        team_logos_out: dict[str, list] = _dd(list)
        for entry in FRANCHISE_LOGOS:
            rel = entry["path"]
            if rel.startswith("static/"):
                rel = rel[len("static/"):]
            team_logos_out[entry["team_id"]].append({
                "year_start": entry["year_start"],
                "year_end": entry["year_end"],  # may be None for current
                "url": url_for("static", filename=rel),
            })
        for lst in team_logos_out.values():
            lst.sort(key=lambda e: (e["year_start"], -(e["year_end"] or 10**6)))

        # ── Build the flat list of eras shipped to the browser ───────────
        def _era_rows(source, slug_lookup, kind):
            rows = []
            for era in source:
                team_id = era["team_id"]
                team = slug_lookup.get(team_id)
                if team is None or not team.slug:
                    continue  # no navigable slug — skip
                year_start = era["year_start"]
                year_end = era["year_end"]  # may be None for current era
                # Fallback logo for defunct teams (no FRANCHISE_LOGOS entry):
                # use the mid-era year against the lookup helpers, which will
                # typically return None → frontend renders an abbr label.
                rep_year = year_start if year_end is None else (year_start + year_end) // 2
                rows.append({
                    "team_id": team_id,
                    "slug": team.slug,
                    "kind": kind,  # "current" or "defunct"
                    "franchise": era.get("franchise", ""),
                    "era_name": era["era_name"],
                    "abbr": era.get("abbr") or team.abbr or "",
                    "city": era["city"],
                    "state": era.get("state", ""),
                    "year_start": year_start,
                    "year_end": year_end,  # None → still active
                    "lat": era["lat"],
                    "lon": era["lon"],
                    "logo_url": _resolve_logo_url(team_id, rep_year),
                })
            return rows

        eras = (
            _era_rows(FRANCHISE_HISTORY, current_team_by_id, "current")
            + _era_rows(DEFUNCT_FRANCHISES, legacy_by_id, "defunct")
        )

        # ── Franchise journeys (ordered era sequence per surviving team) ──
        journeys: dict[str, list] = _dd(list)
        for era in eras:
            if era["kind"] != "current":
                continue
            journeys[era["team_id"]].append({
                "lat": era["lat"],
                "lon": era["lon"],
                "year_start": era["year_start"],
                "year_end": era["year_end"],
                "city": era["city"],
                "era_name": era["era_name"],
            })
        for seq in journeys.values():
            seq.sort(key=lambda e: e["year_start"])

        # ── Per-season W-L records keyed by start year ───────────────────
        records_by_year: dict[int, dict[str, dict]] = _dd(lambda: _dd(lambda: {"w": 0, "l": 0}))
        for season, home_id, road_id, winner_id in all_games:
            if not season or len(season) < 5:
                continue
            try:
                start_year = int(season[-4:])
            except ValueError:
                continue
            bucket = records_by_year[start_year]
            for tid in (home_id, road_id):
                if not tid:
                    continue
                if tid == winner_id:
                    bucket[tid]["w"] += 1
                else:
                    bucket[tid]["l"] += 1

        # Convert defaultdicts so jinja/json serialize cleanly.
        records_by_year_out = {
            str(year): {tid: rec for tid, rec in teams_rec.items()}
            for year, teams_rec in records_by_year.items()
        }

        # Year bounds for the slider. Oldest era wins start. Latest = latest
        # season in records OR the open current-era year.
        min_year = min((e["year_start"] for e in eras), default=1946)
        latest_record_year = max(records_by_year.keys(), default=min_year)
        from datetime import date as _date
        today_year = _date.today().year
        # Season-start convention: 2025 == 2025-26 season.
        max_year = max(latest_record_year, today_year)

        default_year = latest_record_year or today_year

        # Division layout dicts used by both the timeline info strip and the
        # standings card. Grouped east→west, atlantic→southwest.
        divisions_groups = [
            ("E", "Atlantic", "大西洋"),
            ("E", "Central", "中央"),
            ("E", "Southeast", "东南"),
            ("W", "Northwest", "西北"),
            ("W", "Pacific", "太平洋"),
            ("W", "Southwest", "西南"),
        ]
        divisions_lookup = {tid: div for tid, div in DIVISIONS_2004.items()}

        # East/West quick-link chips stay at the bottom of the page.
        east_ids = {tid for tid, (conf, _) in DIVISIONS_2004.items() if conf == "E"}
        east_chips = sorted(
            (
                {"abbr": t.abbr, "slug": t.slug}
                for t in current_teams
                if t.team_id in east_ids and t.slug
            ),
            key=lambda x: x["abbr"] or "",
        )
        west_chips = sorted(
            (
                {"abbr": t.abbr, "slug": t.slug}
                for t in current_teams
                if t.team_id not in east_ids and t.slug
            ),
            key=lambda x: x["abbr"] or "",
        )

        return get_render_template()(
            "teams_list.html",
            eras=eras,
            journeys=journeys,
            team_logos=dict(team_logos_out),
            records_by_year=records_by_year_out,
            divisions_map=divisions_lookup,
            divisions_groups=divisions_groups,
            min_year=min_year,
            max_year=max_year,
            default_year=default_year,
            east_chips=east_chips,
            west_chips=west_chips,
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

    def _parse_hero_topic(topic: str) -> dict:
        """Decode 'Hero Highlight — {game_id} — {scope} — {metric_key} — {entity_id}'."""
        if not topic:
            return {}
        parts = [p.strip() for p in str(topic).split("—")]
        if len(parts) < 5 or parts[0] != "Hero Highlight":
            return {}
        return {
            "source_game_id": parts[1],
            "source_scope": parts[2],
            "source_metric_key": parts[3],
            "source_entity_id": parts[4],
        }

    def _build_featured_highlights(team_lookup: dict, limit: int = 12) -> list[dict]:
        """Hero card posts published to Funba's home feed (platform=funba).

        Server-side dedup: a metric can have several SocialPost rows (race
        conditions on concurrent curator runs, manual retriggers, …); the
        feed should show each (game, metric, entity) story exactly once,
        keeping the most recently published delivery.
        """
        from pathlib import Path

        SessionLocal = get_session_local()
        Game = get_game_model()
        from db.models import (
            NewsArticle,
            SocialPost,
            SocialPostDelivery,
            SocialPostImage,
            SocialPostVariant,
        )

        with SessionLocal() as session:
            # Pull more than `limit` so dedup has room to drop dupes.
            rows = (
                session.query(SocialPostDelivery, SocialPostVariant, SocialPost)
                .join(SocialPostVariant, SocialPostDelivery.variant_id == SocialPostVariant.id)
                .join(SocialPost, SocialPostVariant.post_id == SocialPost.id)
                .filter(
                    SocialPostDelivery.platform == "funba",
                    SocialPostDelivery.status == "published",
                    SocialPost.status != "archived",
                )
                .order_by(SocialPostDelivery.published_at.is_(None), SocialPostDelivery.published_at.desc(), SocialPostDelivery.id.desc())
                .limit(limit * 5)
                .all()
            )
            if not rows:
                return []

            post_ids = [int(r[2].id) for r in rows]
            poster_rows = (
                session.query(SocialPostImage)
                .filter(
                    SocialPostImage.post_id.in_(post_ids),
                    SocialPostImage.slot == "poster",
                    SocialPostImage.is_enabled.is_(True),
                )
                .all()
            )
            posters_by_post = {int(img.post_id): img for img in poster_rows}

            news_rows = (
                session.query(NewsArticle.internal_social_post_id, NewsArticle.cluster_id)
                .filter(
                    NewsArticle.source == "funba",
                    NewsArticle.internal_social_post_id.in_(post_ids),
                    NewsArticle.cluster_id.isnot(None),
                )
                .all()
            )
            news_cluster_by_post = {int(r[0]): int(r[1]) for r in news_rows if r[0] is not None}

            game_ids: set[str] = set()
            for _delivery, _variant, post in rows:
                try:
                    ids = json.loads(post.source_game_ids or "[]")
                except Exception:
                    ids = []
                for gid in ids:
                    if gid:
                        game_ids.add(str(gid))
            games_by_id: dict[str, Any] = {}
            if game_ids:
                for g in session.query(Game).filter(Game.game_id.in_(game_ids)).all():
                    games_by_id[str(g.game_id)] = g

            entries: list[dict] = []
            for delivery, variant, post in rows:
                try:
                    source_game_ids = json.loads(post.source_game_ids or "[]")
                except Exception:
                    source_game_ids = []
                game = games_by_id.get(str(source_game_ids[0])) if source_game_ids else None

                game_url = ""
                game_title = ""
                if game is not None:
                    slug = game.slug or game.game_id
                    game_url = f"/games/{slug}"
                    home_team = team_lookup.get(str(game.home_team_id)) if team_lookup else None
                    road_team = team_lookup.get(str(game.road_team_id)) if team_lookup else None
                    home_abbr = getattr(home_team, "abbr", None) or "?"
                    road_abbr = getattr(road_team, "abbr", None) or "?"
                    if game.home_team_score is not None and game.road_team_score is not None:
                        game_title = f"{road_abbr} {game.road_team_score} @ {home_abbr} {game.home_team_score}"
                    else:
                        game_title = f"{road_abbr} @ {home_abbr}"

                poster = posters_by_post.get(int(post.id))
                poster_url: str | None = None
                if poster and poster.file_path:
                    src_path = Path(str(poster.file_path))
                    thumb_path = src_path.with_suffix(".thumb.webp")
                    fname = thumb_path.name if thumb_path.exists() else src_path.name
                    poster_url = f"/media/social_posts/{int(post.id)}/{fname}"

                # Variant content already has [[IMAGE:slot=poster]] tag at the top
                # — strip that for the home-feed teaser body, since the home card
                # renders the poster image directly above the text.
                body = (variant.content_raw or "").replace("[[IMAGE:slot=poster]]", "").strip()

                source = _parse_hero_topic(post.topic)
                metric_url = ""
                if source.get("source_metric_key"):
                    # /metrics/{key} (no leading /cn — Flask language middleware
                    # adds the /cn prefix automatically when the user is in zh).
                    mk = source["source_metric_key"]
                    metric_url = f"/metrics/{mk}"
                cluster_id = news_cluster_by_post.get(int(post.id))
                news_url = f"/news/{cluster_id}" if cluster_id else ""
                entries.append(
                    {
                        "post_id": int(post.id),
                        "title": variant.title,
                        "body": body,
                        "poster_url": poster_url,
                        "game_url": game_url,
                        "metric_url": metric_url,
                        "news_url": news_url,
                        "game_title": game_title,
                        "published_at": delivery.published_at,
                        "game_date": getattr(game, "game_date", None) if game is not None else None,
                        **source,
                    }
                )

            # Dedup by (game_id, scope, metric_key, entity_id). The list is
            # already ordered newest-first; first hit per key wins.
            deduped: list[dict] = []
            seen: set[tuple[str, str, str, str]] = set()
            for entry in entries:
                key = (
                    str(entry.get("source_game_id") or ""),
                    str(entry.get("source_scope") or ""),
                    str(entry.get("source_metric_key") or ""),
                    str(entry.get("source_entity_id") or ""),
                )
                if not all(key):
                    deduped.append(entry)
                    continue
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(entry)
            return deduped[:limit]

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
        notable_recent = _build_recent_notable_metrics(team_lookup)
        top_scorers = _build_top_scorers()
        featured_highlights = _build_featured_highlights(team_lookup)

        # ── merge text-only notable cards with image-dominant featured posters
        # into one waterfall, with dedup so a metric that already has a poster
        # doesn't also appear as its plain-text card.
        covered: set[tuple[str, str, str, str]] = set()
        for fh in featured_highlights:
            key = (
                str(fh.get("source_game_id") or ""),
                str(fh.get("source_scope") or ""),
                str(fh.get("source_metric_key") or ""),
                str(fh.get("source_entity_id") or ""),
            )
            if all(key):
                covered.add(key)

        def _notable_match_keys(card: dict) -> list[tuple[str, str, str, str]]:
            scope = str(card.get("subject_kind") or "")
            metric_key = str(card.get("metric_key") or "")
            game_id = str(card.get("game_id") or card.get("home_game_id") or "")
            entity = str(card.get("entity_id") or card.get("player_id") or card.get("team_id") or "")
            keys: list[tuple[str, str, str, str]] = []
            if game_id and metric_key:
                keys.append((game_id, scope, metric_key, entity))
            return keys

        feed_items: list[dict] = [{"kind": "image", **fh} for fh in featured_highlights]
        for card in (notable_recent.get("cards") or []):
            if any(k in covered for k in _notable_match_keys(card)):
                continue
            feed_items.append({"kind": "notable", **card})

        # Sort: most recent game first; within a game, image cards before text.
        def _feed_sort_key(item: dict) -> tuple:
            gd = item.get("game_date")
            ts = gd.toordinal() if hasattr(gd, "toordinal") else 0
            kind_rank = 0 if item.get("kind") == "image" else 1
            ratio = item.get("ratio") or item.get("best_ratio") or 9999
            return (-ts, kind_rank, ratio)

        feed_items.sort(key=_feed_sort_key)
        feed_items = feed_items[:_NOTABLE_MAX_CARDS + len(featured_highlights)]

        games_active = [g for g in today_games_data if g.get("status") in (GAME_STATUS_LIVE, GAME_STATUS_COMPLETED)]
        upcoming_games = [
            g for g in today_games_data
            if g.get("status") == GAME_STATUS_UPCOMING
            and (g.get("home_team_id") or g.get("road_team_id"))
        ]

        rendered = get_render_template()(
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
            notable_recent=notable_recent,
            top_scorers=top_scorers,
            featured_highlights=featured_highlights,
            feed_items=feed_items,
            feed_has_more=True,
        )
        # Force a fresh fetch every visit so live scores aren't served from a
        # stale HTML cache. The 15s JS poller still updates an open tab in
        # place; this header just prevents a returning visitor from seeing the
        # render snapshot the browser cached an hour ago.
        response = make_response(rendered)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        return response

    def home_feed_more():
        """Infinite-scroll endpoint: rendered notable cards for older games."""
        SessionLocal = get_session_local()
        Game = get_game_model()
        try:
            page = max(2, int(request.args.get("page", "2")))
        except (ValueError, TypeError):
            page = 2
        per_page_games = 15
        offset = (page - 1) * per_page_games
        with SessionLocal() as session:
            team_lookup = get_team_map(session)
            games = (
                session.query(Game)
                .filter(Game.home_team_score.isnot(None), Game.game_date.isnot(None))
                .order_by(Game.game_date.desc(), Game.game_id.desc())
                .offset(offset)
                .limit(per_page_games + 1)
                .all()
            )
        has_more = len(games) > per_page_games
        games = games[:per_page_games]
        cards: list[dict] = []
        for idx, game in enumerate(games):
            payload = get_cached_game_metrics_payload(game.game_id) if get_cached_game_metrics_payload else None
            if payload is None and get_load_game_metrics_payload is not None:
                try:
                    payload = get_load_game_metrics_payload(game.game_id)
                except Exception:
                    payload = None
            if payload is None:
                continue
            cards.extend(_home_highlight_cards_from_payload(game, payload, team_lookup, idx))
        cards.sort(key=lambda c: c.get("_sort", (9999, 1.0, 9999)))
        for card in cards:
            card.pop("_sort", None)
        cards = cards[:_NOTABLE_MAX_CARDS]
        items = [{"kind": "notable", **card} for card in cards]
        render = get_render_template()
        html = "".join(render("_feed_card.html", item=item) for item in items)
        return jsonify({
            "html": html,
            "next_page": page + 1 if has_more else None,
        })

    def _build_home_news(team_lookup: dict, limit: int = 15) -> list[dict]:
        """Top-scored news clusters for the home feed."""
        SessionLocal = get_session_local()
        Player = get_player_model()
        Team = get_team_model()
        news_models = _load_news_models()
        if news_models is None:
            return []
        NewsArticle, NewsArticlePlayer, NewsArticleTeam, NewsCluster = news_models

        with SessionLocal() as session:
            clusters = (
                session.query(NewsCluster)
                .filter(NewsCluster.representative_article_id.isnot(None))
                .order_by(NewsCluster.score.desc())
                .limit(limit)
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

    _NOTABLE_MAX_CARDS = 60
    _recent_highlight_warm_lock = threading.Lock()

    def _highlight_metric_ratio(card: dict) -> float:
        for rank_key, total_key in (
            ("all_games_rank", "all_games_total"),
            ("all_rank", "all_total"),
            ("rank", "total"),
            ("last3_rank", "last3_total"),
            ("last5_rank", "last5_total"),
        ):
            rank = card.get(rank_key)
            total = card.get(total_key)
            if rank is not None and total:
                try:
                    return int(rank) / int(total)
                except (TypeError, ValueError, ZeroDivisionError):
                    continue
        value = card.get("best_ratio") or card.get("ratio")
        try:
            return float(value)
        except (TypeError, ValueError):
            return 1.0

    def _game_metadata_for_highlight(game, team_lookup: dict) -> dict:
        home_team = team_lookup.get(game.home_team_id)
        road_team = team_lookup.get(game.road_team_id)
        return {
            "game_id": game.game_id,
            "game_slug": game.slug,
            "game_date": game.game_date,
            "game_season": game.season,
            "home_team_id": game.home_team_id,
            "road_team_id": game.road_team_id,
            "home_abbr": home_team.abbr if home_team else (game.home_team_id or ""),
            "road_abbr": road_team.abbr if road_team else (game.road_team_id or ""),
            "home_score": game.home_team_score,
            "road_score": game.road_team_score,
            "winner_id": str(game.wining_team_id) if getattr(game, "wining_team_id", None) else None,
        }

    def _home_highlight_cards_from_payload(game, payload: dict, team_lookup: dict, game_idx: int) -> list[dict]:
        """Homepage notable-stats strip: curator-picked entries only.

        The homepage has no room for raw rule-based noise (tied ranks, early-
        season "1 loss = rank 1" artifacts, etc.), so we limit it to cards the
        LLM curator explicitly selected. Games that haven't been curated yet
        contribute nothing here. The full rule-based view still shows up on
        the individual game page.
        """
        curated_flag = bool((payload or {}).get("_curated_merged"))
        if not curated_flag:
            return []

        metadata = _game_metadata_for_highlight(game, team_lookup)
        cards: list[dict] = []

        game_metrics = ((payload or {}).get("game_metrics") or {}).get("season") or []
        for metric in game_metrics:
            if not metric.get("is_curated"):
                continue
            card = dict(metric)
            card.update(metadata)
            card["subject_kind"] = "game"
            card["ratio"] = _highlight_metric_ratio(card)
            card["_sort"] = (game_idx, 0 if card.get("is_hero") else 1, card["ratio"], int(card.get("best_rank") or card.get("rank") or 9999))
            cards.append(card)

        # Two player slots per game so a career milestone (e.g. surpassing a
        # Hall-of-Famer on a career leaderboard) can ride alongside a season
        # hero (e.g. a game-high scoring outburst).
        player_slots_remaining = 2
        player_candidates = [m for m in ((payload or {}).get("triggered_player_metrics") or []) if m.get("is_curated")]

        def _player_sort_key(metric: dict) -> tuple:
            # career > last5 > last3 > concrete season. Within a window tier,
            # hero beats notable, then by rank ratio.
            metric_key = str(metric.get("metric_key") or "")
            if metric_key.endswith("_career"):
                window_tier = 0
            elif metric_key.endswith("_last5"):
                window_tier = 1
            elif metric_key.endswith("_last3"):
                window_tier = 2
            else:
                window_tier = 3
            return (
                window_tier,
                0 if metric.get("is_hero") else 1,
                _highlight_metric_ratio(metric),
                int(metric.get("rank") or 9999),
            )

        for metric in sorted(player_candidates, key=_player_sort_key):
            if player_slots_remaining <= 0:
                break
            card = dict(metric)
            card.update(metadata)
            card["subject_kind"] = "player"
            card["ratio"] = _highlight_metric_ratio(card)
            card["_sort"] = (game_idx, 0 if card.get("is_hero") else 1, card["ratio"], int(card.get("rank") or 9999))
            cards.append(card)
            player_slots_remaining -= 1

        for metric in (payload or {}).get("triggered_team_metrics") or []:
            if not metric.get("is_curated"):
                continue
            card = dict(metric)
            card.update(metadata)
            card["subject_kind"] = "team"
            card["ratio"] = _highlight_metric_ratio(card)
            card["_sort"] = (game_idx, 0 if card.get("is_hero") else 1, card["ratio"], int(card.get("rank") or 9999))
            cards.append(card)
            break  # one team card per game

        return cards

    def _warm_recent_game_highlights_async(game_ids: list[str]) -> None:
        if not game_ids or get_load_game_metrics_payload is None:
            return
        if not _recent_highlight_warm_lock.acquire(blocking=False):
            return

        def _run():
            try:
                for gid in game_ids:
                    try:
                        get_load_game_metrics_payload(gid)
                    except Exception:
                        continue
            finally:
                _recent_highlight_warm_lock.release()

        threading.Thread(target=_run, daemon=True).start()

    def _build_recent_notable_metrics(team_lookup: dict) -> dict:
        SessionLocal = get_session_local()
        Game = get_game_model()
        with SessionLocal() as session:
            games = (
                session.query(Game)
                .filter(Game.home_team_score.isnot(None), Game.game_date.isnot(None))
                .order_by(Game.game_date.desc(), Game.game_id.desc())
                .limit(15)
                .all()
            )
        cards: list[dict] = []
        missing_game_ids: list[str] = []
        sync_miss_budget = 1
        for idx, game in enumerate(games):
            payload = get_cached_game_metrics_payload(game.game_id) if get_cached_game_metrics_payload else None
            if _home_cached_payload_needs_curated_refresh(game, payload) and get_load_game_metrics_payload is not None:
                try:
                    payload = get_load_game_metrics_payload(game.game_id)
                except Exception:
                    payload = None
            if payload is None and not cards and sync_miss_budget and get_load_game_metrics_payload is not None:
                try:
                    payload = get_load_game_metrics_payload(game.game_id)
                    sync_miss_budget -= 1
                except Exception:
                    payload = None
            elif payload is None:
                missing_game_ids.append(game.game_id)
            if payload is None:
                continue
            cards.extend(_home_highlight_cards_from_payload(game, payload, team_lookup, idx))

        if missing_game_ids:
            _warm_recent_game_highlights_async(missing_game_ids)

        cards.sort(key=lambda c: c.get("_sort", (9999, 1.0, 9999)))
        for card in cards:
            card.pop("_sort", None)
        dates = []
        seen_dates = set()
        for game in games:
            if game.game_date and game.game_date not in seen_dates:
                dates.append(game.game_date)
                seen_dates.add(game.game_date)
        return {"cards": cards[:_NOTABLE_MAX_CARDS], "game_dates": dates}

    def news_page():
        SessionLocal = get_session_local()
        with SessionLocal() as session:
            team_lookup = get_team_map(session)
        entries = _build_home_news(team_lookup, limit=60)
        return get_render_template()("news.html", news_entries=entries)

    def news_detail(cluster_id: int):
        SessionLocal = get_session_local()
        Player = get_player_model()
        Team = get_team_model()
        Game = get_game_model()
        news_models = _load_news_models()
        if news_models is None:
            abort(404)
        NewsArticle, NewsArticlePlayer, NewsArticleTeam, NewsCluster = news_models

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

            metric_url: str | None = None
            game_url: str | None = None
            game_label: str | None = None
            original_image_url: str | None = None
            if rep.source == "funba" and rep.internal_social_post_id:
                from db.models import SocialPost as _SocialPost, SocialPostImage as _SocialPostImage
                post = session.get(_SocialPost, rep.internal_social_post_id)
                if post is not None:
                    parsed = _parse_hero_topic(post.topic or "")
                    mk = parsed.get("source_metric_key")
                    if mk:
                        metric_url = f"/metrics/{mk}"
                    try:
                        gids = json.loads(post.source_game_ids or "[]")
                    except Exception:
                        gids = []
                    if gids:
                        game = session.query(Game).filter(Game.game_id == str(gids[0])).first()
                        if game is not None:
                            slug = game.slug or game.game_id
                            game_url = f"/games/{slug}"
                            home_team = session.query(Team).filter(Team.team_id == game.home_team_id).first() if game.home_team_id else None
                            road_team = session.query(Team).filter(Team.team_id == game.road_team_id).first() if game.road_team_id else None
                            home_abbr = (home_team.abbr if home_team else None) or "?"
                            road_abbr = (road_team.abbr if road_team else None) or "?"
                            game_label = f"{road_abbr} @ {home_abbr}"
                    poster = (
                        session.query(_SocialPostImage)
                        .filter(
                            _SocialPostImage.post_id == post.id,
                            _SocialPostImage.slot == "poster",
                            _SocialPostImage.is_enabled.is_(True),
                        )
                        .first()
                    )
                    if poster and poster.file_path:
                        from os.path import basename as _basename
                        fname = _basename(str(poster.file_path))
                        original_image_url = f"/media/social_posts/{int(post.id)}/{fname}"

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
                    "original_image_url": original_image_url,
                    "published_at": rep.published_at,
                    "metric_url": metric_url,
                    "game_url": game_url,
                    "game_label": game_label,
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

            # Group season ids by year. Each year typically has Regular,
            # PlayIn, Playoffs (and sometimes Pre/All-Star). The games-list
            # dropdown is year-only — the per-game phase chip on each row
            # tells users which phase a specific game belongs to.
            def _season_year(sid: str | None) -> str | None:
                if not sid:
                    return None
                s = str(sid).strip()
                if len(s) == 5 and s.isdigit():
                    return s[1:]
                return None

            year_to_season_ids: dict[str, list[str]] = {}
            for sid in all_season_ids:
                year = _season_year(sid)
                if year is None:
                    continue
                year_to_season_ids.setdefault(year, []).append(sid)
            all_years = sorted(year_to_season_ids.keys(), reverse=True)

            # Prefer `year`; keep `season` as a legacy alias so bookmarks /
            # external links with ?season=22025 still land on the same page.
            selected_year = (request.args.get("year") or "").strip() or None
            legacy_season = None
            if not selected_year:
                legacy_season = (request.args.get("season") or "").strip()
                if legacy_season:
                    selected_year = _season_year(legacy_season)
            if not selected_year:
                selected_year = all_years[0] if all_years else None
            if selected_year and selected_year not in year_to_season_ids:
                selected_year = all_years[0] if all_years else None

            exact_season_filter = legacy_season if legacy_season in all_season_ids else None
            if exact_season_filter:
                season_ids_for_filter = [exact_season_filter]
            else:
                season_ids_for_filter = list(year_to_season_ids.get(selected_year, [])) if selected_year else []

            def _pick_default_phase(season_ids: list[str]) -> str | None:
                phases_present = {
                    ("4" if str(sid).startswith("5") else str(sid)[:1])
                    for sid in season_ids
                    if sid
                }
                if "4" in phases_present:
                    return "4"
                for prefix in ("3", "2", "1"):
                    if prefix in phases_present:
                        return prefix
                return None

            # Phase filter: "2" = Regular, "4" = Playoffs (incl. Play-In), etc.
            # `phase=all` is the explicit "show everything" sentinel — used so
            # that choosing "All Phases" from the dropdown survives the round
            # trip without being silently replaced by the auto-pick below.
            # A missing `phase` param triggers auto-pick of the current phase.
            # Legacy `phase=5` is folded into `phase=4` (play-in rolled up).
            raw_phase = (request.args.get("phase") or "").strip()
            if raw_phase == "all":
                selected_phase = "all"
            elif raw_phase == "5":
                selected_phase = "4"
            elif raw_phase in {"1", "2", "3", "4"}:
                selected_phase = raw_phase
            elif exact_season_filter:
                selected_phase = "all"
            else:
                selected_phase = _pick_default_phase(season_ids_for_filter)

            if selected_phase and selected_phase != "all" and season_ids_for_filter:
                allowed_prefixes = {"4", "5"} if selected_phase == "4" else {selected_phase}
                season_ids_for_filter = [
                    sid for sid in season_ids_for_filter if str(sid)[:1] in allowed_prefixes
                ]

            # Build bracket (before team filter so it shows full tournament)
            bracket = None
            if selected_phase == "4" and season_ids_for_filter:
                bracket_games = (
                    session.query(Game)
                    .filter(Game.season.in_(season_ids_for_filter), Game.game_date.isnot(None))
                    .all()
                )
                playoff_games = [g for g in bracket_games if str(g.season).startswith("4")]
                playin_games = [g for g in bracket_games if str(g.season).startswith("5")]
                bracket = _build_playoff_bracket(playoff_games)
                if playin_games:
                    bracket["playin"] = _build_playin_bracket(playin_games)

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
            if exact_season_filter:
                games_q = games_q.filter(Game.season == exact_season_filter)
            elif season_ids_for_filter:
                games_q = games_q.filter(Game.season.in_(season_ids_for_filter))
            if selected_team:
                games_q = games_q.filter(or_(Game.home_team_id == selected_team, Game.road_team_id == selected_team))
            games_q = games_q.order_by(Game.game_date.desc(), Game.game_id.desc())

            all_games = games_q.all()
            all_games = _supplement_missing_live_games(
                all_games,
                live_map,
                allowed_seasons=set(season_ids_for_filter) if season_ids_for_filter else None,
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

            # Single unified list: live, upcoming, and completed games sorted
            # newest→oldest. Within the same date, live games float to the
            # top so ongoing action is immediately visible; upcoming comes
            # next, completed last. Legacy ?view=live / ?view=results /
            # ?view=schedule all fold into this view (kept for bookmarks).
            has_live = bool(live_games)
            view = "games"

            def _status_rank(entry):
                if entry.status == GAME_STATUS_LIVE:
                    return 2
                if entry.status == GAME_STATUS_UPCOMING:
                    return 1
                return 0

            combined_games = sorted(
                upcoming_games + live_games + completed_all,
                key=lambda item: (item.game_date, _status_rank(item), item.game_id),
                reverse=True,
            )
            paginate_source = combined_games

            # With the playoff bracket visible, skip pagination so clicking a
            # series card filters across the full playoff+play-in set (the
            # client-side filter only sees rows currently in the DOM). Total
            # is bounded (~100 games), fine to render as a single list.
            if bracket and paginate_source:
                page_size = max(page_size, len(paginate_source))

            total = len(paginate_source)
            total_pages = max(1, (total + page_size - 1) // page_size) if paginate_source else 1

            default_page = 1
            if paginate_source:
                today_start_idx = next(
                    (
                        i for i, entry in enumerate(paginate_source)
                        if entry.game_date is not None and entry.game_date <= today
                    ),
                    len(paginate_source) - 1,
                )
                default_page = (today_start_idx // page_size) + 1

            if "page" not in request.args:
                page = default_page
            page = min(page, total_pages)
            start = (page - 1) * page_size
            end = start + page_size
            paginated = paginate_source[start:end] if paginate_source else []

            date_groups = _group_by_date(paginated)

            # Split the list so the template can collapse the "Upcoming"
            # section by default — otherwise users scroll past dozens of
            # scheduled playoff placeholders to reach today's games.
            future_date_groups = [(dt, entries) for dt, entries in date_groups if dt is not None and dt > today]
            past_date_groups = [(dt, entries) for dt, entries in date_groups if dt is None or dt <= today]
            future_games_count = sum(len(entries) for _, entries in future_date_groups)

            team_lookup_rows = session.query(Team).all()
            team_lookup = {team.team_id: team for team in team_lookup_rows if getattr(team, "team_id", None)}
            selected_team_obj = next((team for team in all_teams if team.team_id == selected_team), None)
            if selected_team_obj is None and selected_team:
                selected_team_obj = team_lookup.get(selected_team)

        def _year_label(year: str) -> str:
            try:
                next_two = str(int(year) + 1)[-2:]
                return f"{year}-{next_two}"
            except ValueError:
                return year

        # Map season-id prefix -> phase label (bilingual, matches the
        # existing _season_label mapping).
        _t = get_t()
        phase_label_map = {
            "1": _t("Pre Season", "季前赛"),
            "2": _t("Regular Season", "常规赛"),
            "3": _t("All Star", "全明星"),
            "4": _t("Playoffs", "季后赛"),
            "5": _t("Play-In", "附加赛"),
        }

        def phase_label_for(season_id: str | None) -> str:
            if not season_id:
                return ""
            s = str(season_id).strip()
            if len(s) == 5 and s.isdigit():
                return phase_label_map.get(s[0], "")
            return ""

        # Available phases for the selected year (for the phase dropdown).
        # Order: Regular (2), Playoffs (4), Pre (1), All-Star (3).
        # Play-In (5) is rolled into Playoffs, so it is not a separate option —
        # a year that has only play-in games still gets a Playoffs entry.
        _phase_order = {"2": 0, "4": 1, "1": 2, "3": 3}
        available_phases = []
        if selected_year and selected_year in year_to_season_ids:
            seen = set()
            for sid in year_to_season_ids[selected_year]:
                prefix = str(sid)[0]
                if prefix == "5":
                    prefix = "4"  # Fold play-in into playoffs
                if prefix not in seen and prefix in phase_label_map:
                    seen.add(prefix)
                    available_phases.append((prefix, phase_label_map[prefix]))
            available_phases.sort(key=lambda x: _phase_order.get(x[0], 9))

        return get_render_template()(
            "games_list.html",
            view=view,
            has_live=has_live,
            date_groups=date_groups,
            future_date_groups=future_date_groups,
            past_date_groups=past_date_groups,
            future_games_count=future_games_count,
            live_count=len(live_games),
            results_count=len(completed_all),
            schedule_count=len(upcoming_games),
            team_lookup=team_lookup,
            all_teams=all_teams,
            all_years=all_years,
            selected_year=selected_year,
            selected_season=exact_season_filter or (season_ids_for_filter[0] if season_ids_for_filter else None),
            year_label=_year_label,
            phase_label_for=phase_label_for,
            available_phases=available_phases,
            selected_phase=selected_phase,
            bracket=bracket,
            selected_team=selected_team,
            selected_team_obj=selected_team_obj,
            games=[entry.game for entry in paginated],
            completed_games=[entry.game for entry in completed_all],
            live_games=live_games,
            upcoming_games=upcoming_games,
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
            func.sum(func.coalesce(PlayerGameStats.oreb, 0)).label("oreb"),
            func.sum(func.coalesce(PlayerGameStats.dreb, 0)).label("dreb"),
            func.sum(func.coalesce(PlayerGameStats.pf, 0)).label("pf"),
            func.sum(func.coalesce(PlayerGameStats.plus, 0)).label("plus_minus"),
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
            "oreb": int(raw_row.oreb or 0),
            "dreb": int(raw_row.dreb or 0),
            "pf": int(raw_row.pf or 0),
            "plus_minus": int(raw_row.plus_minus or 0),
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
            summary["orpg"] = f"{summary['oreb'] / games_played:.1f}"
            summary["drpg"] = f"{summary['dreb'] / games_played:.1f}"
            summary["fpg"] = f"{summary['pf'] / games_played:.1f}"
        else:
            summary["mpg"] = "-"
            summary["ppg"] = "-"
            summary["rpg"] = "-"
            summary["apg"] = "-"
            summary["spg"] = "-"
            summary["bpg"] = "-"
            summary["tpg"] = "-"
            summary["orpg"] = "-"
            summary["drpg"] = "-"
            summary["fpg"] = "-"
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

    def _derive_player_top_rankings(session, metric_results: dict, *, current_season: str | None, limit: int = 3) -> list[dict]:
        """Pick top rankings out of an already-computed _get_metric_results output.

        Reuses the rank/total the framework already computed for the player
        card so the compare page doesn't pay for a second full-table scan.
        """
        candidates: list[dict] = []
        for entry in (metric_results.get("season") or []):
            candidates.append({"entry": entry, "season": current_season})
        for entry in (metric_results.get("alltime") or []):
            candidates.append({"entry": entry, "season": entry.get("season")})

        def sort_key(item):
            entry = item["entry"]
            rank = entry.get("rank") or 10**9
            total = entry.get("total") or 0
            return (rank, -total, entry.get("metric_key") or "")

        candidates.sort(key=sort_key)

        rankings = []
        for item in candidates[:limit]:
            entry = item["entry"]
            metric_key = entry.get("metric_key")
            if not metric_key:
                continue
            label = entry.get("metric_name") or _compare_metric_label(session, metric_key)
            rank = entry.get("rank")
            total = entry.get("total")
            badge = f"#{int(rank)} of {int(total)} · {label}" if rank and total else label
            season_for_link = item["season"]
            rankings.append(
                {
                    "metric_key": metric_key,
                    "label": label,
                    "badge": badge,
                    "scope_label": _compare_metric_scope_label({"season": season_for_link}),
                    "href": get_localized_url_for()("metric_detail", metric_key=metric_key, season=season_for_link)
                    if season_for_link
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
                    }
                )
            for card in player_cards:
                card["top_rankings"] = _derive_player_top_rankings(session, card["metrics"], current_season=current_season)

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

            # Teams actually active in the selected season — includes
            # legacy franchises (Seattle SuperSonics, NJ Nets, etc.) for
            # historical seasons, so their rosters and historical logos
            # show up under their real franchise identity.
            team_ids_in_season = [
                row[0] for row in (
                    session.query(PlayerGameStats.team_id)
                    .join(Game, Game.game_id == PlayerGameStats.game_id)
                    .filter(
                        Game.season == selected_season,
                        PlayerGameStats.team_id.isnot(None),
                    )
                    .distinct()
                    .all()
                )
            ]
            teams = (
                session.query(Team)
                .filter(Team.team_id.in_(team_ids_in_season))
                .order_by(Team.full_name.asc())
                .all()
            ) if team_ids_in_season else []
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

        # Year the season started — e.g. "22002" → "2002". Used by the
        # template so team_logo resolves to the historical era.
        season_year = None
        if selected_season and len(str(selected_season)) == 5 and str(selected_season).isdigit():
            season_year = str(selected_season)[1:]

        t = get_t()
        render_template = get_render_template()
        return render_template(
            "players.html",
            teams_with_players=teams_with_players,
            player_count=player_count,
            selected_season=selected_season,
            season_year=season_year,
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
        from web.app import _player_url, _is_zh, _display_player_name  # lazy to avoid circular import
        for rows in (payload.get("players_by_team") or {}).values():
            for row in rows:
                pid = row.get("player_id")
                if pid:
                    row["player_url"] = _player_url(pid)
        # Localize player names to Chinese when applicable.
        if _is_zh():
            pids = set()
            for rows in (payload.get("players_by_team") or {}).values():
                for row in rows:
                    if row.get("player_id"):
                        pids.add(str(row["player_id"]))
            if pids:
                _SessionLocal = get_session_local()
                _Player = get_player_model()
                with _SessionLocal() as sess:
                    db_players = sess.query(_Player).filter(_Player.player_id.in_(pids)).all()
                    name_map = {str(p.player_id): _display_player_name(p) for p in db_players}
                for rows in (payload.get("players_by_team") or {}).values():
                    for row in rows:
                        zh = name_map.get(str(row.get("player_id") or ""))
                        if zh:
                            row["player_name"] = zh
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
    app.add_url_rule("/api/home/feed", endpoint="home_feed_more", view_func=home_feed_more)
    app.add_url_rule("/teams", endpoint="teams_list_page", view_func=teams_list_page)
    app.add_url_rule("/cn/teams", endpoint="teams_list_page_zh", view_func=teams_list_page)
    app.add_url_rule("/news", endpoint="news_page", view_func=news_page)
    app.add_url_rule("/cn/news", endpoint="news_page_zh", view_func=news_page)
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
        news_page=news_page,
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
        derive_player_top_rankings=_derive_player_top_rankings,
    )
