"""MetricRunner: compute and persist all metrics triggered by a game."""
from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy.orm import Session

from db.models import Game, MetricResult as MetricResultModel, Player, PlayerGameStats, Team
from metrics.framework import registry
from metrics.framework.base import MetricResult
from metrics.framework import scorer as scorer_module

logger = logging.getLogger(__name__)


def _entity_name(session: Session, entity_type: str, entity_id: str | None) -> str:
    if not entity_id:
        return "League"
    if entity_type == "player":
        p = session.query(Player.full_name).filter(Player.player_id == entity_id).scalar()
        return p or entity_id
    if entity_type in ("team", "game"):
        t = session.query(Team.full_name).filter(Team.team_id == entity_id).scalar()
        return t or entity_id
    return entity_id


def _persist(session: Session, result: MetricResult) -> None:
    """Upsert: delete existing result for same key/entity/season, then insert."""
    session.query(MetricResultModel).filter(
        MetricResultModel.metric_key == result.metric_key,
        MetricResultModel.entity_type == result.entity_type,
        MetricResultModel.entity_id == result.entity_id,
        MetricResultModel.season == result.season,
    ).delete(synchronize_session=False)

    row = MetricResultModel(
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
    session.add(row)


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

            # AI noteworthiness scoring
            if do_score:
                name = _entity_name(session, entity_type, entity_id)
                try:
                    score, reason = scorer_module.score(result, metric_def, name)
                    result.noteworthiness = score
                    result.notable_reason = reason
                except Exception as exc:
                    logger.warning("Scoring failed for %s: %s", metric_def.key, exc)

            _persist(session, result)
            results.append(result)

    if commit:
        session.commit()

    notable = [r for r in results if scorer_module.is_notable(r.noteworthiness)]
    logger.info(
        "Game %s: computed %d metric results, %d notable.",
        game_id, len(results), len(notable),
    )
    return results
