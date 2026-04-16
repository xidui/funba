from __future__ import annotations

import json
import logging
from datetime import datetime

from nba_api.stats.endpoints import boxscoresummaryv3
from requests.exceptions import ConnectionError, Timeout
from sqlalchemy.orm import Session, sessionmaker
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from db.models import Game, GameLineScore, engine

logger = logging.getLogger(__name__)

SessionLocal = sessionmaker(bind=engine)


@retry(
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def fetch_game_line_score_payload(game_id: str) -> dict:
    response = boxscoresummaryv3.BoxScoreSummaryV3(game_id=game_id)
    return response.get_dict().get("boxScoreSummary", {})


def has_game_line_score(session: Session, game_id: str) -> bool:
    rows = (
        session.query(GameLineScore)
        .filter(GameLineScore.game_id == game_id)
        .all()
    )
    if len(rows) < 2:
        return False
    # Q4 must be present (game finished regulation).
    if not all(row.q4_pts is not None for row in rows):
        return False
    # Cross-check total_pts against Game final score. If they don't match,
    # the line score is stale (e.g. fetched before OT finished).
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game and game.home_team_score and game.road_team_score:
        score_map = {str(game.home_team_id): int(game.home_team_score),
                     str(game.road_team_id): int(game.road_team_score)}
        for row in rows:
            expected = score_map.get(str(row.team_id))
            if expected and row.total_pts != expected:
                return False
    return True


def _period_score_map(periods: list[dict]) -> tuple[dict[int, int], list[int]]:
    regular: dict[int, int] = {}
    overtime_scores: list[int] = []

    for period in periods or []:
        try:
            period_num = int(period.get("period"))
            score = int(period.get("score"))
        except (TypeError, ValueError):
            continue
        if period_num <= 4:
            regular[period_num] = score
        else:
            overtime_scores.append(score)

    return regular, overtime_scores


def _normalize_team_line_score(game: Game, team_payload: dict, *, on_road: bool) -> dict:
    regular, overtime_scores = _period_score_map(team_payload.get("periods") or [])
    ot1 = overtime_scores[0] if len(overtime_scores) > 0 else None
    ot2 = overtime_scores[1] if len(overtime_scores) > 1 else None
    ot3 = overtime_scores[2] if len(overtime_scores) > 2 else None
    ot_extra = overtime_scores[3:] if len(overtime_scores) > 3 else []

    q1 = regular.get(1)
    q2 = regular.get(2)
    q3 = regular.get(3)
    q4 = regular.get(4)
    first_half = (q1 or 0) + (q2 or 0) if q1 is not None or q2 is not None else None
    second_half = (q3 or 0) + (q4 or 0) if q3 is not None or q4 is not None else None
    regulation_total = sum(v for v in (q1, q2, q3, q4) if v is not None) if regular else None

    return {
        "game_id": game.game_id,
        "team_id": str(team_payload["teamId"]),
        "on_road": on_road,
        "q1_pts": q1,
        "q2_pts": q2,
        "q3_pts": q3,
        "q4_pts": q4,
        "ot1_pts": ot1,
        "ot2_pts": ot2,
        "ot3_pts": ot3,
        "ot_extra_json": json.dumps(ot_extra) if ot_extra else None,
        "first_half_pts": first_half,
        "second_half_pts": second_half,
        "regulation_total_pts": regulation_total,
        "total_pts": int(team_payload["score"]),
    }


def normalize_game_line_score_payload(game: Game, payload: dict) -> list[dict]:
    away = payload.get("awayTeam") or {}
    home = payload.get("homeTeam") or {}
    rows = [
        _normalize_team_line_score(game, away, on_road=True),
        _normalize_team_line_score(game, home, on_road=False),
    ]

    team_ids = {row["team_id"] for row in rows}
    if game.home_team_id and game.road_team_id and team_ids != {str(game.home_team_id), str(game.road_team_id)}:
        raise ValueError(
            f"Line score team mismatch for game {game.game_id}: expected "
            f"{game.road_team_id}/{game.home_team_id}, got {sorted(team_ids)}"
        )

    return rows


def back_fill_game_line_score(
    session: Session,
    game_id: str,
    *,
    commit: bool = False,
    replace_existing: bool = False,
) -> int:
    game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
    if game is None:
        raise ValueError(f"Game {game_id!r} not found")

    if not replace_existing and has_game_line_score(session, game_id):
        logger.info("skip line score for game %s; already backfilled", game_id)
        return 0

    payload = fetch_game_line_score_payload(game_id)
    rows = normalize_game_line_score_payload(game, payload)
    if not rows:
        logger.warning("No line score rows found for game_id=%s", game_id)
        return 0

    if replace_existing:
        session.query(GameLineScore).filter(GameLineScore.game_id == game_id).delete(synchronize_session=False)

    existing = {
        row.team_id: row
        for row in session.query(GameLineScore).filter(GameLineScore.game_id == game_id).all()
    }
    now = datetime.utcnow()
    upserted = 0
    for row in rows:
        record = existing.get(row["team_id"])
        if record is None:
            record = GameLineScore(
                game_id=game_id,
                team_id=row["team_id"],
                fetched_at=now,
                updated_at=now,
            )
            session.add(record)
        else:
            record.updated_at = now
        record.on_road = row["on_road"]
        record.q1_pts = row["q1_pts"]
        record.q2_pts = row["q2_pts"]
        record.q3_pts = row["q3_pts"]
        record.q4_pts = row["q4_pts"]
        record.ot1_pts = row["ot1_pts"]
        record.ot2_pts = row["ot2_pts"]
        record.ot3_pts = row["ot3_pts"]
        record.ot_extra_json = row["ot_extra_json"]
        record.first_half_pts = row["first_half_pts"]
        record.second_half_pts = row["second_half_pts"]
        record.regulation_total_pts = row["regulation_total_pts"]
        record.total_pts = row["total_pts"]
        record.source = "nba_api_boxscoresummaryv3"
        if record.fetched_at is None:
            record.fetched_at = now
        upserted += 1

    if commit:
        session.commit()
    return upserted
