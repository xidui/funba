"""MetricRunner: compute and persist all metrics triggered by a game."""
from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy.orm import Session

from db.models import Game, MetricResult as MetricResultModel, MetricRunLog, PlayerGameStats
from metrics.framework import registry
from metrics.framework.base import MetricResult
from metrics.framework import scorer as scorer_module

logger = logging.getLogger(__name__)



def _persist(session: Session, result: MetricResult) -> None:
    """Atomic upsert using INSERT ... ON DUPLICATE KEY UPDATE.

    The unique index on (metric_key, entity_type, entity_id, season) ensures
    concurrent writes for the same entity are handled without deadlocks.
    """
    from sqlalchemy.dialects.mysql import insert

    stmt = insert(MetricResultModel).values(
        metric_key=result.metric_key,
        entity_type=result.entity_type,
        entity_id=result.entity_id,
        season=result.season,
        game_id=result.game_id,
        value_num=result.value_num,
        value_str=result.value_str,
        context_json=json.dumps(result.context) if result.context else None,
        noteworthiness=result.noteworthiness,
        notable_reason=result.notable_reason,
        computed_at=datetime.utcnow(),
    )
    stmt = stmt.on_duplicate_key_update(
        game_id=stmt.inserted.game_id,
        value_num=stmt.inserted.value_num,
        value_str=stmt.inserted.value_str,
        context_json=stmt.inserted.context_json,
        noteworthiness=stmt.inserted.noteworthiness,
        notable_reason=stmt.inserted.notable_reason,
        computed_at=stmt.inserted.computed_at,
    )
    session.execute(stmt)


def run_for_game(
    session: Session,
    game_id: str,
    do_score: bool = True,
    commit: bool = True,
) -> list[MetricResult]:
    """Run all active metrics for all entities touched by a game.

    Returns the list of MetricResult objects that were produced and persisted.
    """
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        logger.warning("Game %s not found; skipping metric run.", game_id)
        return []

    season = game.season

    # Collect distinct player_ids that appeared in this game
    player_ids: list[str] = [
        row.player_id
        for row in session.query(PlayerGameStats.player_id)
        .filter(PlayerGameStats.game_id == game_id)
        .distinct()
        .all()
    ]
    team_ids: list[str] = [t for t in [game.home_team_id, game.road_team_id] if t]

    all_metrics = registry.get_all()
    results: list[MetricResult] = []

    # Step 1: compute all metrics
    for metric_def in all_metrics:
        targets: list[tuple[str, str | None]] = []

        if metric_def.scope == "player":
            targets = [("player", pid) for pid in player_ids]
        elif metric_def.scope == "team":
            targets = [("team", tid) for tid in team_ids]
        elif metric_def.scope == "game":
            targets = [("game", game_id)]
        elif metric_def.scope == "league":
            targets = [("league", None)]

        for entity_type, entity_id in targets:
            try:
                result = metric_def.compute(session, entity_id, season, game_id)
            except Exception as exc:
                logger.error(
                    "Metric %s failed for %s %s: %s",
                    metric_def.key, entity_type, entity_id, exc,
                    exc_info=True,
                )
                continue

            if result is None:
                continue

            results.append(result)

    # Step 2: persist results
    result_keys = {r.metric_key for r in results}
    for result in results:
        _persist(session, result)

    # Step 3: write one log row per metric so new metrics added later will be detected
    now = datetime.utcnow()
    for metric_def in all_metrics:
        session.merge(MetricRunLog(
            game_id=game_id,
            metric_key=metric_def.key,
            computed_at=now,
            produced_result=metric_def.key in result_keys,
        ))

    # Step 4: rank-based noteworthiness (needs results in DB first)
    if do_score and results:
        session.flush()  # write to DB without committing so ranking query sees new rows
        scorer_module.rank_noteworthiness(session, results)

    if commit:
        session.commit()

    notable = [r for r in results if scorer_module.is_notable(r.noteworthiness)]
    logger.info(
        "Game %s: computed %d metric results, %d notable.",
        game_id, len(results), len(notable),
    )
    return results


def already_processed(session: Session, game_id: str) -> bool:
    """Return True if every currently registered metric has a log entry for this game."""
    registered_keys = {m.key for m in registry.get_all()}
    logged_keys = {
        r.metric_key
        for r in session.query(MetricRunLog.metric_key)
        .filter(MetricRunLog.game_id == game_id)
        .all()
    }
    return registered_keys.issubset(logged_keys)
