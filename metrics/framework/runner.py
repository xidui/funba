"""MetricRunner: compute and persist all metrics triggered by a game."""
from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import Game, MetricResult as MetricResultModel, MetricRunLog, PlayerGameStats, Team
from metrics.framework.base import CAREER_SEASON, MetricResult, merge_totals, subtract_delta
from metrics.framework.runtime import get_all_metrics, get_metric

logger = logging.getLogger(__name__)


def _get_existing_totals(
    session: Session,
    metric_key: str,
    entity_type: str,
    entity_id: str,
    season: str,
) -> dict:
    """Read current running totals from MetricResult.context_json with a row lock."""
    row = (
        session.query(MetricResultModel)
        .filter(
            MetricResultModel.metric_key == metric_key,
            MetricResultModel.entity_type == entity_type,
            MetricResultModel.entity_id == entity_id,
            MetricResultModel.season == season,
        )
        .with_for_update()
        .first()
    )
    if row is None or not row.context_json:
        return {}
    try:
        return json.loads(row.context_json)
    except (ValueError, TypeError):
        return {}


def _upsert_result(session: Session, result: MetricResult) -> None:
    """INSERT or UPDATE a MetricResult row."""
    from sqlalchemy.dialects.mysql import insert

    stmt = insert(MetricResultModel).values(
        metric_key=result.metric_key,
        entity_type=result.entity_type,
        entity_id=result.entity_id,
        season=result.season,
        rank_group=result.rank_group,
        game_id=result.game_id,
        value_num=result.value_num,
        value_str=result.value_str,
        context_json=json.dumps(result.context) if result.context else None,
        noteworthiness=result.noteworthiness,
        notable_reason=result.notable_reason,
        computed_at=datetime.utcnow(),
    )
    stmt = stmt.on_duplicate_key_update(
        rank_group=stmt.inserted.rank_group,
        game_id=stmt.inserted.game_id,
        value_num=stmt.inserted.value_num,
        value_str=stmt.inserted.value_str,
        context_json=stmt.inserted.context_json,
        computed_at=stmt.inserted.computed_at,
    )
    session.execute(stmt)


def _log_run(
    session: Session,
    game_id: str,
    metric_key: str,
    entity_type: str,
    entity_id: str,
    season: str,
    delta: dict | None,
    produced: bool,
) -> None:
    from sqlalchemy.dialects.mysql import insert

    stmt = insert(MetricRunLog).values(
        game_id=game_id,
        metric_key=metric_key,
        entity_type=entity_type,
        entity_id=entity_id,
        season=season,
        computed_at=datetime.utcnow(),
        produced_result=produced,
        delta_json=json.dumps(delta) if delta is not None else None,
    )
    stmt = stmt.on_duplicate_key_update(
        computed_at=stmt.inserted.computed_at,
        produced_result=stmt.inserted.produced_result,
        delta_json=stmt.inserted.delta_json,
    )
    session.execute(stmt)


def _get_old_delta(
    session: Session,
    game_id: str,
    metric_key: str,
    entity_type: str,
    entity_id: str,
    season: str,
) -> dict:
    """Return the delta previously recorded for this (game, metric, entity) in MetricRunLog."""
    row = (
        session.query(MetricRunLog)
        .filter(
            MetricRunLog.game_id == game_id,
            MetricRunLog.metric_key == metric_key,
            MetricRunLog.entity_type == entity_type,
            MetricRunLog.entity_id == entity_id,
            MetricRunLog.season == season,
        )
        .first()
    )
    if row is None or not row.delta_json:
        return {}
    try:
        return json.loads(row.delta_json)
    except (ValueError, TypeError):
        return {}


def _get_targets(session: Session, scope: str, game: Game, player_ids: list[str], team_ids: list[str]):
    if scope == "player":
        return [("player", pid) for pid in player_ids]
    if scope == "player_franchise":
        rows = (
            session.query(
                PlayerGameStats.player_id,
                func.coalesce(Team.canonical_team_id, Team.team_id).label("franchise_id"),
            )
            .outerjoin(Team, PlayerGameStats.team_id == Team.team_id)
            .filter(
                PlayerGameStats.game_id == game.game_id,
                PlayerGameStats.player_id.isnot(None),
            )
            .distinct()
            .all()
        )
        return [
            ("player_franchise", f"{row.player_id}:{row.franchise_id}")
            for row in rows
            if row.player_id and row.franchise_id
        ]
    if scope == "team":
        return [("team", tid) for tid in team_ids]
    if scope == "game":
        return [("game", game.game_id)]
    if scope == "league":
        return [("league", None)]
    return []


def run_for_game(
    session: Session,
    game_id: str,
    commit: bool = True,
) -> list[MetricResult]:
    """Run all active metrics for all entities touched by a game."""
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        logger.warning("Game %s not found; skipping.", game_id)
        return []

    season = game.season

    player_ids: list[str] = [
        row.player_id
        for row in session.query(PlayerGameStats.player_id)
        .filter(PlayerGameStats.game_id == game_id)
        .distinct()
        .all()
    ]
    team_ids: list[str] = [t for t in [game.home_team_id, game.road_team_id] if t]

    all_metrics = get_all_metrics(session=session)
    results: list[MetricResult] = []

    for metric_def in all_metrics:
        targets = _get_targets(session, metric_def.scope, game, player_ids, team_ids)

        if not metric_def.incremental:
            # Full-recompute path (game-scope and rank-based metrics)
            for entity_type, entity_id in targets:
                try:
                    result = metric_def.compute(session, entity_id, season, game_id)
                except Exception as exc:
                    logger.error("Metric %s failed for %s %s: %s",
                                 metric_def.key, entity_type, entity_id, exc, exc_info=True)
                    continue
                if result:
                    _upsert_result(session, result)
                    results.append(result)
                _log_run(session, game_id, metric_def.key, entity_type,
                         entity_id or "", season, None, result is not None)
            continue

        # Incremental path — career metrics accumulate in CAREER_SEASON bucket
        bucket_season = CAREER_SEASON if metric_def.career else season

        for entity_type, entity_id in targets:
            try:
                delta = metric_def.compute_delta(session, entity_id, game_id)
            except Exception as exc:
                logger.error("compute_delta %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue

            if delta is None:
                continue

            existing_totals = _get_existing_totals(
                session, metric_def.key, entity_type, entity_id, bucket_season
            )
            new_totals = merge_totals(existing_totals, delta)

            try:
                result = metric_def.compute_value(new_totals, bucket_season, entity_id)
            except Exception as exc:
                logger.error("compute_value %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue

            if result:
                result.context = new_totals
                _upsert_result(session, result)
                results.append(result)
            else:
                # Below min_sample — persist totals anyway so they accumulate
                # toward the threshold in future games (value_num stays NULL)
                _upsert_result(session, MetricResult(
                    metric_key=metric_def.key,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    season=bucket_season,
                    game_id=None,
                    value_num=None,
                    context=new_totals,
                ))

            _log_run(session, game_id, metric_def.key, entity_type,
                     entity_id or "", bucket_season, delta, result is not None)

    if commit:
        session.commit()

    logger.info("Game %s: %d metric results.", game_id, len(results))
    return results


def run_for_game_single_metric(
    session: Session,
    game_id: str,
    metric_key: str,
    commit: bool = True,
    force: bool = False,
) -> list[MetricResult]:
    """Run a single named metric for all entities touched by a game.

    Same logic as run_for_game() but filters to only the one metric.
    Keeps run_for_game() untouched for daily_job.py local fallback.

    force=True (undo-redo):
        For incremental metrics, subtracts the previously recorded delta
        (from MetricRunLog.delta_json) before applying the new one. This
        corrects running totals without reprocessing all historical games.
        Non-incremental metrics always do a full recompute so force is a no-op.
    """
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        logger.warning("Game %s not found; skipping.", game_id)
        return []

    season = game.season

    player_ids: list[str] = [
        row.player_id
        for row in session.query(PlayerGameStats.player_id)
        .filter(PlayerGameStats.game_id == game_id)
        .distinct()
        .all()
    ]
    team_ids: list[str] = [t for t in [game.home_team_id, game.road_team_id] if t]

    metric_def = get_metric(metric_key, session=session)
    if metric_def is None:
        logger.warning("Metric key %r not found in registry; skipping.", metric_key)
        return []

    results: list[MetricResult] = []
    targets = _get_targets(session, metric_def.scope, game, player_ids, team_ids)

    if not metric_def.incremental:
        for entity_type, entity_id in targets:
            try:
                result = metric_def.compute(session, entity_id, season, game_id)
            except Exception as exc:
                logger.error("Metric %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue
            if result:
                _upsert_result(session, result)
                results.append(result)
            _log_run(session, game_id, metric_def.key, entity_type,
                     entity_id or "", season, None, result is not None)
    else:
        bucket_season = CAREER_SEASON if metric_def.career else season

        for entity_type, entity_id in targets:
            try:
                delta = metric_def.compute_delta(session, entity_id, game_id)
            except Exception as exc:
                logger.error("compute_delta %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue

            if delta is None:
                continue

            existing_totals = _get_existing_totals(
                session, metric_def.key, entity_type, entity_id, bucket_season
            )

            if force:
                # Undo-redo: subtract old delta before adding new one so running
                # totals are not double-counted on reprocessing.
                old_delta = _get_old_delta(
                    session, game_id, metric_def.key,
                    entity_type, entity_id or "", bucket_season,
                )
                if old_delta:
                    existing_totals = subtract_delta(existing_totals, old_delta)
                    logger.debug(
                        "force: subtracted old delta for %s/%s entity=%s",
                        game_id, metric_def.key, entity_id,
                    )

            new_totals = merge_totals(existing_totals, delta)

            try:
                result = metric_def.compute_value(new_totals, bucket_season, entity_id)
            except Exception as exc:
                logger.error("compute_value %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue

            if result:
                result.context = new_totals
                _upsert_result(session, result)
                results.append(result)
            else:
                _upsert_result(session, MetricResult(
                    metric_key=metric_def.key,
                    entity_type=entity_type,
                    entity_id=entity_id,
                    season=bucket_season,
                    game_id=None,
                    value_num=None,
                    context=new_totals,
                ))

            _log_run(session, game_id, metric_def.key, entity_type,
                     entity_id or "", bucket_season, delta, result is not None)

    if commit:
        session.commit()

    logger.info("Game %s metric %s: %d results.", game_id, metric_key, len(results))
    return results


def already_processed(session: Session, game_id: str) -> bool:
    """Return True if every registered metric has a log entry for this game."""
    registered_keys = {m.key for m in get_all_metrics(session=session)}
    logged_keys = {
        r.metric_key
        for r in session.query(MetricRunLog.metric_key)
        .filter(MetricRunLog.game_id == game_id)
        .distinct()
        .all()
    }
    return registered_keys.issubset(logged_keys)
