"""Shared helpers available to generated metric code.

These functions encapsulate tricky data-parsing logic so that LLM-generated
metric code can call them instead of reimplementing the logic each time.
"""
from __future__ import annotations

import json
from collections import defaultdict

from sqlalchemy.orm import Session

from db.models import Game, GameLineScore, GamePlayByPlay, PlayerGameStats, ShotRecord, Team, TeamGameStats
from metrics.framework.base import career_season_type_code, is_career_season


def _metric_helper_cache(session: Session) -> dict:
    return session.info.setdefault("_metric_helper_cache", {})


def _game_line_score_rows(session: Session, game_id: str) -> list[GameLineScore]:
    return (
        session.query(GameLineScore)
        .filter(GameLineScore.game_id == game_id)
        .all()
    )


def game_row(session: Session, game_id: str) -> Game | None:
    """Return the Game row for one game, cached per Session/game."""
    cache = _metric_helper_cache(session)
    cache_key = ("game_row", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    row = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    cache[cache_key] = row
    return row


def _player_game_stats_by_game(session: Session, game_id: str) -> dict[str, PlayerGameStats]:
    cache = _metric_helper_cache(session)
    cache_key = ("player_game_stats_by_game", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    rows = (
        session.query(PlayerGameStats)
        .filter(
            PlayerGameStats.game_id == game_id,
            PlayerGameStats.player_id.isnot(None),
        )
        .all()
    )
    result = {str(row.player_id): row for row in rows if row.player_id is not None}
    cache[cache_key] = result
    return result


def player_game_stat(session: Session, game_id: str, player_id: str) -> PlayerGameStats | None:
    """Return the PlayerGameStats row for one player in a game, cached per Session/game."""
    return _player_game_stats_by_game(session, game_id).get(str(player_id))


def team_player_stats(session: Session, game_id: str, team_id: str) -> list[PlayerGameStats]:
    """Return PlayerGameStats rows for one team in a game, cached per Session/game."""
    cache = _metric_helper_cache(session)
    cache_key = ("team_player_stats", str(game_id))
    cached = cache.get(cache_key)
    if cached is None:
        grouped: dict[str, list[PlayerGameStats]] = defaultdict(list)
        for row in _player_game_stats_by_game(session, game_id).values():
            if row.team_id is not None:
                grouped[str(row.team_id)].append(row)
        cached = dict(grouped)
        cache[cache_key] = cached
    return cached.get(str(team_id), [])


def _team_game_stats_by_game(session: Session, game_id: str) -> dict[str, TeamGameStats]:
    cache = _metric_helper_cache(session)
    cache_key = ("team_game_stats_by_game", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    rows = (
        session.query(TeamGameStats)
        .filter(
            TeamGameStats.game_id == game_id,
            TeamGameStats.team_id.isnot(None),
        )
        .all()
    )
    result = {str(row.team_id): row for row in rows if row.team_id is not None}
    cache[cache_key] = result
    return result


def team_game_stat(session: Session, game_id: str, team_id: str) -> TeamGameStats | None:
    """Return the TeamGameStats row for one team in a game, cached per Session/game."""
    return _team_game_stats_by_game(session, game_id).get(str(team_id))


def _attempted_shots_by_game(session: Session, game_id: str) -> dict[str, list[ShotRecord]]:
    cache = _metric_helper_cache(session)
    cache_key = ("attempted_shots_by_game", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    rows = (
        session.query(ShotRecord)
        .filter(
            ShotRecord.game_id == game_id,
            ShotRecord.player_id.isnot(None),
            ShotRecord.shot_attempted.is_(True),
        )
        .order_by(
            ShotRecord.player_id.asc(),
            ShotRecord.period.asc(),
            ShotRecord.min.desc(),
            ShotRecord.sec.desc(),
            ShotRecord.id.asc(),
        )
        .all()
    )
    grouped: dict[str, list[ShotRecord]] = defaultdict(list)
    for row in rows:
        grouped[str(row.player_id)].append(row)
    result = dict(grouped)
    cache[cache_key] = result
    return result


def player_attempted_shots(session: Session, game_id: str, player_id: str) -> list[ShotRecord]:
    """Return attempted shots for one player in a game, cached per Session/game."""
    return _attempted_shots_by_game(session, game_id).get(str(player_id), [])


def game_pbp_rows(session: Session, game_id: str) -> list[GamePlayByPlay]:
    """Return all play-by-play rows for a game, cached per Session/game."""
    cache = _metric_helper_cache(session)
    cache_key = ("game_pbp_rows", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    rows = (
        session.query(GamePlayByPlay)
        .filter(GamePlayByPlay.game_id == game_id)
        .order_by(
            GamePlayByPlay.period.asc(),
            GamePlayByPlay.event_num.asc(),
            GamePlayByPlay.id.asc(),
        )
        .all()
    )
    cache[cache_key] = rows
    return rows


def _pbp_description(row: GamePlayByPlay) -> str:
    return str(
        row.home_description
        or row.visitor_description
        or row.neutral_description
        or ""
    ).strip()


def _is_offensive_foul_row(row: GamePlayByPlay) -> bool:
    action_type = int(row.event_msg_action_type or 0)
    text = _pbp_description(row)
    return action_type in {4, 26} or "OFF.Foul" in text or "Offensive Charge Foul" in text


def _is_charge_row(row: GamePlayByPlay) -> bool:
    action_type = int(row.event_msg_action_type or 0)
    text = _pbp_description(row)
    return action_type == 26 or "Offensive Charge Foul" in text


def pbp_offensive_foul_events(session: Session, game_id: str) -> list[dict]:
    """Return normalized offensive-foul events for one game, cached per Session/game.

    Each item includes:
    {
      "game_id": "...",
      "event_num": 123,
      "period": 4,
      "pc_time": "2:31",
      "description": "...",
      "action_type": 4|26|...,
      "foul_player_id": "player1_id",
      "drawn_by_player_id": "player2_id" | None,
      "is_charge": bool,
    }
    """
    cache = _metric_helper_cache(session)
    cache_key = ("pbp_offensive_foul_events", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    events = [
        {
            "game_id": str(game_id),
            "event_num": int(row.event_num or 0),
            "period": int(row.period or 0),
            "pc_time": row.pc_time,
            "description": _pbp_description(row),
            "action_type": int(row.event_msg_action_type or 0),
            "foul_player_id": str(row.player1_id) if row.player1_id else None,
            "drawn_by_player_id": str(row.player2_id) if row.player2_id else None,
            "is_charge": _is_charge_row(row),
        }
        for row in game_pbp_rows(session, game_id)
        if (row.event_msg_type or 0) == 6 and _is_offensive_foul_row(row)
    ]
    cache[cache_key] = events
    return events


def pbp_charge_events(session: Session, game_id: str) -> list[dict]:
    """Return normalized offensive-charge events for one game, cached per Session/game."""
    cache = _metric_helper_cache(session)
    cache_key = ("pbp_charge_events", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    events = [
        event
        for event in pbp_offensive_foul_events(session, game_id)
        if event["is_charge"]
    ]
    cache[cache_key] = events
    return events


def season_pbp_offensive_foul_events(session: Session, season: str) -> list[dict]:
    """Return normalized offensive-foul events for an entire season, cached per Session/season."""
    cache = _metric_helper_cache(session)
    cache_key = ("season_pbp_offensive_foul_events", str(season))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    query = (
        session.query(GamePlayByPlay, Game)
        .join(Game, Game.game_id == GamePlayByPlay.game_id)
        .filter(GamePlayByPlay.event_msg_type == 6)
    )
    if is_career_season(season):
        code = career_season_type_code(season)
        if not code:
            cache[cache_key] = []
            return []
        query = query.filter(Game.season.like(f"{code}%"))
    else:
        query = query.filter(Game.season == season)

    rows = (
        query.order_by(
            Game.game_date.asc(),
            GamePlayByPlay.game_id.asc(),
            GamePlayByPlay.period.asc(),
            GamePlayByPlay.event_num.asc(),
            GamePlayByPlay.id.asc(),
        ).all()
    )
    events = [
        {
            "game_id": str(game.game_id),
            "season": str(game.season) if game.season is not None else None,
            "game_date": game.game_date.isoformat() if game.game_date else None,
            "event_num": int(row.event_num or 0),
            "period": int(row.period or 0),
            "pc_time": row.pc_time,
            "description": _pbp_description(row),
            "action_type": int(row.event_msg_action_type or 0),
            "foul_player_id": str(row.player1_id) if row.player1_id else None,
            "drawn_by_player_id": str(row.player2_id) if row.player2_id else None,
            "is_charge": _is_charge_row(row),
        }
        for row, game in rows
        if _is_offensive_foul_row(row)
    ]
    cache[cache_key] = events
    return events


def season_pbp_charge_events(session: Session, season: str) -> list[dict]:
    """Return normalized offensive-charge events for an entire season, cached per Session/season."""
    cache = _metric_helper_cache(session)
    cache_key = ("season_pbp_charge_events", str(season))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    events = [
        event
        for event in season_pbp_offensive_foul_events(session, season)
        if event["is_charge"]
    ]
    cache[cache_key] = events
    return events


def pbp_clock_seconds_left(pc_time: str | None) -> int | None:
    """Parse a PBP clock string like '1:23' into seconds remaining."""
    if not pc_time:
        return None
    try:
        mm, ss = pc_time.split(":")
        return int(mm) * 60 + int(ss)
    except (ValueError, TypeError, AttributeError):
        return None


def game_score_margin_rows(session: Session, game_id: str) -> list[GamePlayByPlay]:
    """Return PBP rows with score_margin for a game, cached and sorted by period/event."""
    cache = _metric_helper_cache(session)
    cache_key = ("game_score_margin_rows", str(game_id))
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    rows = [
        row
        for row in game_pbp_rows(session, game_id)
        if row.score_margin not in (None, "", "null")
    ]
    cache[cache_key] = rows
    return rows


def period_ending_pbp_row(session: Session, game_id: str, period: int) -> GamePlayByPlay | None:
    """Return the latest score_margin row in a period, if available."""
    rows = [row for row in game_score_margin_rows(session, game_id) if (row.period or 0) == int(period)]
    if not rows:
        return None
    rows.sort(key=lambda row: ((row.event_num or 0), pbp_clock_seconds_left(row.pc_time) or -1), reverse=True)
    return rows[0]


def late_final_score_margin_rows(session: Session, game_id: str, seconds_left: int = 10) -> list[GamePlayByPlay]:
    """Return score_margin rows from the final period within the last N seconds."""
    rows = game_score_margin_rows(session, game_id)
    max_period = max(((row.period or 0) for row in rows), default=0)
    if max_period <= 0:
        return []
    return [
        row
        for row in rows
        if (row.period or 0) == max_period and (pbp_clock_seconds_left(row.pc_time) or 10**9) <= seconds_left
    ]


def _line_score_period_map(row: GameLineScore) -> dict[int, int]:
    period_map: dict[int, int] = {}
    for period, value in (
        (1, row.q1_pts),
        (2, row.q2_pts),
        (3, row.q3_pts),
        (4, row.q4_pts),
        (5, row.ot1_pts),
        (6, row.ot2_pts),
        (7, row.ot3_pts),
    ):
        if value is not None:
            period_map[period] = int(value)
    if row.ot_extra_json:
        try:
            extra = json.loads(row.ot_extra_json)
        except json.JSONDecodeError:
            extra = []
        for idx, value in enumerate(extra, start=8):
            if value is not None:
                period_map[idx] = int(value)
    return period_map


def _quarter_scores_from_line_score(session: Session, game_id: str) -> list[dict]:
    rows = _game_line_score_rows(session, game_id)
    if len(rows) < 2:
        return []

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return []

    home_row = next((row for row in rows if str(row.team_id) == str(game.home_team_id)), None)
    road_row = next((row for row in rows if str(row.team_id) == str(game.road_team_id)), None)
    if home_row is None or road_row is None:
        return []

    home_periods = _line_score_period_map(home_row)
    road_periods = _line_score_period_map(road_row)
    periods = sorted(set(home_periods) | set(road_periods))
    if not periods:
        return []

    return [
        {
            "period": period,
            "home_pts": home_periods.get(period, 0),
            "road_pts": road_periods.get(period, 0),
            "home_team_id": game.home_team_id,
            "road_team_id": game.road_team_id,
        }
        for period in periods
    ]


def _half_scores_from_line_score(session: Session, game_id: str) -> dict | None:
    rows = _game_line_score_rows(session, game_id)
    if len(rows) < 2:
        return None

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return None

    home_row = next((row for row in rows if str(row.team_id) == str(game.home_team_id)), None)
    road_row = next((row for row in rows if str(row.team_id) == str(game.road_team_id)), None)
    if home_row is None or road_row is None:
        return None

    if home_row.first_half_pts is None or road_row.first_half_pts is None:
        return None
    if home_row.second_half_pts is None or road_row.second_half_pts is None:
        return None

    return {
        "home_team_id": game.home_team_id,
        "road_team_id": game.road_team_id,
        "home_first_half": int(home_row.first_half_pts),
        "road_first_half": int(road_row.first_half_pts),
        "home_second_half": int(home_row.second_half_pts),
        "road_second_half": int(road_row.second_half_pts),
    }


def get_quarter_scores(session: Session, game_id: str) -> list[dict]:
    """Parse PBP cumulative scores into per-quarter per-team point totals.

    Returns a list of dicts sorted by period:
    [
        {"period": 1, "home_pts": 28, "road_pts": 31, "home_team_id": "...", "road_team_id": "..."},
        {"period": 2, "home_pts": 25, "road_pts": 22, ...},
        ...
    ]

    Returns empty list if no PBP score data is available.
    """
    line_scores = _quarter_scores_from_line_score(session, game_id)
    if line_scores:
        return line_scores

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return []

    # Get score rows, reorder by (period, clock DESC) to handle misplaced events
    rows = (
        session.query(GamePlayByPlay.period, GamePlayByPlay.score,
                       GamePlayByPlay.pc_time, GamePlayByPlay.event_num)
        .filter(
            GamePlayByPlay.game_id == game_id,
            GamePlayByPlay.score.isnot(None),
        )
        .all()
    )

    def _clock_seconds(pc_time):
        if not pc_time:
            return 0
        try:
            parts = pc_time.split(":")
            return int(parts[0]) * 60 + int(parts[1])
        except (ValueError, IndexError):
            return 0

    # Sort: period ASC, clock DESC (12:00→0:00), event_num ASC
    rows.sort(key=lambda r: (r.period or 0, -_clock_seconds(r.pc_time), r.event_num or 0))

    period_end: dict[int, tuple[int, int]] = {}
    for r in rows:
        if r.period is None or not r.score:
            continue
        parts = r.score.split("-")
        if len(parts) != 2:
            continue
        try:
            h, rd = int(parts[0].strip()), int(parts[1].strip())
            p = int(r.period)
            # Keep the highest cumulative total for each period to avoid
            # end-of-period marker rows that carry the previous period's score.
            prev = period_end.get(p)
            if prev is None or (h + rd) > (prev[0] + prev[1]):
                period_end[p] = (h, rd)
        except (ValueError, TypeError):
            continue

    if not period_end:
        return []

    result = []
    prev_h, prev_r = 0, 0
    for p in sorted(period_end.keys()):
        cum_h, cum_r = period_end[p]
        result.append({
            "period": p,
            "home_pts": cum_h - prev_h,
            "road_pts": cum_r - prev_r,
            "home_team_id": game.home_team_id,
            "road_team_id": game.road_team_id,
        })
        prev_h, prev_r = cum_h, cum_r

    return result


def get_half_scores(session: Session, game_id: str) -> dict | None:
    """Get first-half and second-half scores for both teams.

    Returns:
    {
        "home_team_id": "...", "road_team_id": "...",
        "home_first_half": 55, "road_first_half": 48,
        "home_second_half": 52, "road_second_half": 60,
    }

    Returns None if insufficient PBP data.
    """
    line_scores = _half_scores_from_line_score(session, game_id)
    if line_scores is not None:
        return line_scores

    quarters = get_quarter_scores(session, game_id)
    if len(quarters) < 4:
        return None

    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return None

    home_first = sum(q["home_pts"] for q in quarters if q["period"] <= 2)
    road_first = sum(q["road_pts"] for q in quarters if q["period"] <= 2)
    home_second = sum(q["home_pts"] for q in quarters if q["period"] > 2)
    road_second = sum(q["road_pts"] for q in quarters if q["period"] > 2)

    return {
        "home_team_id": game.home_team_id,
        "road_team_id": game.road_team_id,
        "home_first_half": home_first,
        "road_first_half": road_first,
        "home_second_half": home_second,
        "road_second_half": road_second,
    }


def team_abbr(session: Session, team_id: str) -> str:
    """Look up a team's abbreviation. Returns team_id if not found."""
    row = session.query(Team.abbr).filter(Team.team_id == team_id).first()
    return row.abbr if row else team_id
