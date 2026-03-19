"""Shared helpers available to generated metric code.

These functions encapsulate tricky data-parsing logic so that LLM-generated
metric code can call them instead of reimplementing the logic each time.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from db.models import Game, GamePlayByPlay, Team


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
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if not game:
        return []

    # Get last score row per period
    rows = (
        session.query(GamePlayByPlay.period, GamePlayByPlay.score)
        .filter(
            GamePlayByPlay.game_id == game_id,
            GamePlayByPlay.score.isnot(None),
        )
        .order_by(GamePlayByPlay.period, GamePlayByPlay.event_num)
        .all()
    )

    period_end: dict[int, tuple[int, int]] = {}
    for r in rows:
        if r.period is None or not r.score:
            continue
        parts = r.score.split("-")
        if len(parts) != 2:
            continue
        try:
            h, rd = int(parts[0].strip()), int(parts[1].strip())
            period_end[int(r.period)] = (h, rd)
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
