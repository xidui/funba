"""Shared helpers available to generated metric code.

These functions encapsulate tricky data-parsing logic so that LLM-generated
metric code can call them instead of reimplementing the logic each time.
"""
from __future__ import annotations

import json

from sqlalchemy.orm import Session

from db.models import Game, GameLineScore, GamePlayByPlay, Team


def _game_line_score_rows(session: Session, game_id: str) -> list[GameLineScore]:
    return (
        session.query(GameLineScore)
        .filter(GameLineScore.game_id == game_id)
        .all()
    )


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
