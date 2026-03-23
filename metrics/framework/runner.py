"""MetricRunner: two-phase MapReduce pipeline for metric computation.

Phase 1 (Map): compute_delta per game → write MetricRunLog only (no locks).
Phase 2 (Reduce): aggregate all deltas per entity → write MetricResult once.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import Game, MetricResult as MetricResultModel, MetricRunLog, PlayerGameStats, Team
from metrics.framework.base import MetricResult, career_season_for, merge_totals
from metrics.framework.runtime import get_all_metrics, get_metric

logger = logging.getLogger(__name__)


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
    game_id: str,
    metric_key: str,
    entity_type: str,
    entity_id: str,
    season: str,
    delta: dict | None,
    produced: bool,
    qualified: bool | None = None,
) -> dict:
    return {
        "game_id": game_id,
        "metric_key": metric_key,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "season": season,
        "computed_at": datetime.utcnow(),
        "produced_result": produced,
        "delta_json": json.dumps(delta) if delta is not None else None,
        "qualified": qualified,
    }


def _flush_run_logs(session: Session, rows: list[dict]) -> None:
    """Bulk INSERT ... ON DUPLICATE KEY UPDATE for MetricRunLog rows."""
    if not rows:
        return

    from sqlalchemy.dialects.mysql import insert

    stmt = insert(MetricRunLog).values(rows)
    stmt = stmt.on_duplicate_key_update(
        computed_at=stmt.inserted.computed_at,
        produced_result=stmt.inserted.produced_result,
        delta_json=stmt.inserted.delta_json,
        qualified=stmt.inserted.qualified,
    )
    session.execute(stmt)


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


# ── Phase 1 (Map): compute delta only ────────────────────────────────────────

def run_delta_only(
    session: Session,
    game_id: str,
    metric_key: str,
    commit: bool = True,
) -> bool:
    """Compute per-game deltas and write MetricRunLog only.

    No MetricResult reads, writes, or locks. Returns True if any delta was produced.
    Non-incremental metrics write their full-recompute result directly to MetricResult
    (they have no delta concept and no lock contention).
    """
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        logger.warning("Game %s not found; skipping.", game_id)
        return False

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
        return False

    targets = _get_targets(session, metric_def.scope, game, player_ids, team_ids)
    produced_any = False
    run_log_rows: list[dict] = []

    if not metric_def.incremental:
        # Non-incremental metrics (game-scope, rank-based) do a full recompute.
        # No running totals → no lock contention → write result directly.
        for entity_type, entity_id in targets:
            try:
                result = metric_def.compute(session, entity_id, season, game_id)
            except Exception as exc:
                logger.error("Metric %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue
            result_list = result if isinstance(result, list) else [result] if result else []
            for r in result_list:
                _upsert_result(session, r)
            run_log_rows.append(
                _log_run(
                    game_id,
                    metric_def.key,
                    entity_type,
                    entity_id or "",
                    season,
                    None,
                    bool(result_list),
                )
            )
            if result_list:
                produced_any = True
    else:
        # Incremental metrics: compute delta → write MetricRunLog only.
        if metric_def.career:
            bucket_season = career_season_for(season)
            if bucket_season is None:
                if commit:
                    session.commit()
                return False  # skip preseason / all-star for career
        else:
            bucket_season = season

        for entity_type, entity_id in targets:
            try:
                delta = metric_def.compute_delta(session, entity_id, game_id)
            except Exception as exc:
                logger.error("compute_delta %s failed for %s %s: %s",
                             metric_def.key, entity_type, entity_id, exc, exc_info=True)
                continue

            if delta is None:
                continue

            produced_any = True
            run_log_rows.append(
                _log_run(
                    game_id,
                    metric_def.key,
                    entity_type,
                    entity_id or "",
                    bucket_season,
                    delta,
                    True,
                    qualified=metric_def.is_qualifying(delta),
                )
            )

    _flush_run_logs(session, run_log_rows)

    if commit:
        session.commit()

    logger.info("Game %s metric %s: delta_only done (produced=%s).",
                game_id, metric_key, produced_any)
    return produced_any


# ── Phase 2 (Reduce): aggregate deltas → write MetricResult ──────────────────

def reduce_metric(
    session: Session,
    metric_key: str,
    season: str,
    commit: bool = True,
) -> int:
    """Aggregate all deltas for a (metric_key, season) and write MetricResults.

    Reads all MetricRunLog rows, groups by entity, merges all deltas,
    calls compute_value(), and upserts one MetricResult per entity.
    Returns the number of MetricResult rows written.
    """
    metric_def = get_metric(metric_key, session=session)
    if metric_def is None:
        logger.warning("reduce: metric %r not found; skipping.", metric_key)
        return 0

    if not metric_def.incremental:
        # Non-incremental metrics are fully computed in Phase 1; nothing to reduce.
        return 0

    # Find all distinct entities that have deltas for this (metric, season)
    entity_rows = (
        session.query(MetricRunLog.entity_type, MetricRunLog.entity_id)
        .filter(
            MetricRunLog.metric_key == metric_key,
            MetricRunLog.season == season,
        )
        .distinct()
        .all()
    )

    results_written = 0
    for entity_type, entity_id in entity_rows:
        # Read all deltas for this entity, ordered by game_id (chronological)
        delta_rows = (
            session.query(MetricRunLog.delta_json)
            .filter(
                MetricRunLog.metric_key == metric_key,
                MetricRunLog.entity_type == entity_type,
                MetricRunLog.entity_id == entity_id,
                MetricRunLog.season == season,
                MetricRunLog.delta_json.isnot(None),
            )
            .order_by(MetricRunLog.game_id)
            .all()
        )

        # Merge all deltas into final totals
        totals: dict = {}
        for (delta_json,) in delta_rows:
            try:
                delta = json.loads(delta_json)
            except (ValueError, TypeError):
                continue
            totals = merge_totals(totals, delta)

        # Compute final value
        try:
            result = metric_def.compute_value(totals, season, entity_id)
        except Exception as exc:
            logger.error("reduce compute_value %s failed for %s %s: %s",
                         metric_key, entity_type, entity_id, exc, exc_info=True)
            continue

        if result:
            result.context = totals
            _upsert_result(session, result)
            results_written += 1
        else:
            # Below min_sample — persist totals so they are visible
            _upsert_result(session, MetricResult(
                metric_key=metric_key,
                entity_type=entity_type,
                entity_id=entity_id,
                season=season,
                game_id=None,
                value_num=None,
                context=totals,
            ))

    if commit:
        session.commit()

    logger.info("reduce %s season=%s: %d results written (%d entities).",
                metric_key, season, results_written, len(entity_rows))
    return results_written


# ── Utility ───────────────────────────────────────────────────────────────────

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
