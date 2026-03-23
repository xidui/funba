"""Celery tasks for metric computation.

Two-phase MapReduce pipeline:
  Phase 1 (metrics queue): compute_game_delta — compute delta, write MetricRunLog only.
  Phase 2 (reduce queue):  reduce_metric_season — aggregate deltas, write MetricResult.

Claim lifecycle (Phase 1)
-------------------------
1. INSERT IGNORE into MetricJobClaim with status='in_progress'.
   - rowcount=1  → this worker owns the job, proceed.
   - rowcount=0  → another row exists; check its status:
       - 'done'        → computation already committed, skip safely.
       - 'in_progress' → another worker is processing (or crashed); skip.
         Crashed 'in_progress' rows are cleared via --force in dispatch.

2. Computation succeeds → UPDATE status='done'.
   Check if all claims for this metric_key are done → auto-trigger reduce.

3. Computation fails → DELETE claim row. Celery retries.

4. Worker crash → claim stays 'in_progress'.
   After _LEASE_SECONDS the next delivery auto-recovers.
"""
from __future__ import annotations

import logging
from datetime import datetime

from celery import shared_task
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from db.models import MetricJobClaim, MetricRunLog, engine
from metrics.framework.runner import run_delta_only, reduce_metric

logger = logging.getLogger(__name__)

_STATUS_IN_PROGRESS = "in_progress"
_STATUS_DONE = "done"
_LEASE_SECONDS = 600


def _session_factory():
    return sessionmaker(bind=engine)


def _try_claim(session, game_id: str, metric_key: str, worker_id: str) -> tuple[bool, str | None]:
    """Atomically try to claim (game_id, metric_key).

    Returns (owned, existing_status):
      (True,  None)          — successfully claimed, proceed
      (False, 'done')        — already computed, skip
      (False, 'in_progress') — another worker owns it and lease is still fresh, skip
    """
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    stmt = mysql_insert(MetricJobClaim).values(
        game_id=game_id,
        metric_key=metric_key,
        claimed_at=datetime.utcnow(),
        worker_id=worker_id,
        status=_STATUS_IN_PROGRESS,
    ).prefix_with("IGNORE")
    result = session.execute(stmt)
    session.commit()

    if result.rowcount == 1:
        return True, None

    row = (
        session.query(MetricJobClaim)
        .filter(
            MetricJobClaim.game_id == game_id,
            MetricJobClaim.metric_key == metric_key,
        )
        .first()
    )
    if row is None:
        return _try_claim(session, game_id, metric_key, worker_id)

    if row.status == _STATUS_DONE:
        return False, _STATUS_DONE

    age = (datetime.utcnow() - row.claimed_at).total_seconds()
    if age < _LEASE_SECONDS:
        return False, _STATUS_IN_PROGRESS

    # Lease expired — check if MetricRunLog rows exist (work committed before crash)
    existing_run = (
        session.query(MetricRunLog.game_id)
        .filter(
            MetricRunLog.game_id == game_id,
            MetricRunLog.metric_key == metric_key,
        )
        .first()
    )
    if existing_run is not None:
        updated_done = (
            session.query(MetricJobClaim)
            .filter(
                MetricJobClaim.game_id == game_id,
                MetricJobClaim.metric_key == metric_key,
                MetricJobClaim.status == _STATUS_IN_PROGRESS,
                MetricJobClaim.claimed_at == row.claimed_at,
            )
            .update({"status": _STATUS_DONE}, synchronize_session=False)
        )
        session.commit()
        if updated_done == 1:
            logger.warning(
                "promoted stale claim to done: game=%s metric=%s",
                game_id, metric_key,
            )
            return False, _STATUS_DONE
        return False, _STATUS_IN_PROGRESS

    # Lease expired, no MetricRunLog → reclaim
    updated = (
        session.query(MetricJobClaim)
        .filter(
            MetricJobClaim.game_id == game_id,
            MetricJobClaim.metric_key == metric_key,
            MetricJobClaim.status == _STATUS_IN_PROGRESS,
            MetricJobClaim.claimed_at == row.claimed_at,
        )
        .update(
            {"claimed_at": datetime.utcnow(), "worker_id": worker_id},
            synchronize_session=False,
        )
    )
    session.commit()

    if updated == 1:
        logger.warning(
            "reclaimed expired claim: game=%s metric=%s (age=%.0fs)",
            game_id, metric_key, age,
        )
        return True, None
    return False, _STATUS_IN_PROGRESS


def _mark_done(session, game_id: str, metric_key: str) -> None:
    session.query(MetricJobClaim).filter(
        MetricJobClaim.game_id == game_id,
        MetricJobClaim.metric_key == metric_key,
    ).update({"status": _STATUS_DONE})
    session.commit()


def _release_claim(session, game_id: str, metric_key: str) -> None:
    session.query(MetricJobClaim).filter(
        MetricJobClaim.game_id == game_id,
        MetricJobClaim.metric_key == metric_key,
    ).delete()
    session.commit()


def _maybe_trigger_reduce(session, metric_key: str) -> list[str]:
    """If all games have been processed for this metric, enqueue reduce tasks.

    Compares done claims against the total number of games in the DB
    (not just the number of claims), so it won't fire prematurely during
    backfill when claims are being created incrementally.

    Returns list of seasons for which reduce was triggered.
    """
    from db.models import Game

    total_games = (
        session.query(func.count())
        .select_from(Game)
        .filter(Game.game_date.isnot(None))
        .scalar()
    )
    done = (
        session.query(func.count())
        .select_from(MetricJobClaim)
        .filter(
            MetricJobClaim.metric_key == metric_key,
            MetricJobClaim.status == _STATUS_DONE,
        )
        .scalar()
    )
    if total_games == 0 or done < total_games:
        return []

    # All done — find distinct seasons and enqueue reduce
    seasons = [
        r.season
        for r in session.query(MetricRunLog.season)
        .filter(MetricRunLog.metric_key == metric_key)
        .distinct()
        .all()
    ]
    for season in seasons:
        reduce_metric_season_task.delay(metric_key, season)
    logger.info(
        "auto-triggered reduce for metric=%s (%d seasons): %s",
        metric_key, len(seasons), seasons,
    )
    return seasons


# ── Phase 1: Map (metrics queue) ─────────────────────────────────────────────

@shared_task(
    bind=True,
    name="tasks.metrics.compute_game_delta",
    max_retries=3,
    default_retry_delay=10,
    queue="metrics",
)
def compute_game_delta(self, game_id: str, metric_key: str) -> dict:
    """Phase 1: compute delta for one (game, metric) and write MetricRunLog only."""
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        owned, existing_status = _try_claim(session, game_id, metric_key, self.request.id or "")

    if not owned:
        return {
            "game_id": game_id,
            "metric_key": metric_key,
            "skipped": True,
            "reason": existing_status,
        }

    try:
        with SessionLocal() as session:
            produced = run_delta_only(session, game_id, metric_key, commit=True)
    except Exception as exc:
        with SessionLocal() as session:
            _release_claim(session, game_id, metric_key)
        logger.error(
            "compute_game_delta: game=%s metric=%s failed: %s",
            game_id, metric_key, exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=10)

    with SessionLocal() as session:
        _mark_done(session, game_id, metric_key)
        triggered_seasons = _maybe_trigger_reduce(session, metric_key)

    return {
        "game_id": game_id,
        "metric_key": metric_key,
        "produced": produced,
        "reduce_triggered": triggered_seasons,
    }


# ── Phase 2: Reduce (reduce queue) ───────────────────────────────────────────

@shared_task(
    bind=True,
    name="tasks.metrics.reduce_metric_season",
    max_retries=2,
    default_retry_delay=30,
    queue="reduce",
)
def reduce_metric_season_task(self, metric_key: str, season: str) -> dict:
    """Phase 2: aggregate all deltas for (metric_key, season) → write MetricResults."""
    SessionLocal = _session_factory()

    try:
        with SessionLocal() as session:
            count = reduce_metric(session, metric_key, season, commit=True)
    except Exception as exc:
        logger.error(
            "reduce_metric_season: metric=%s season=%s failed: %s",
            metric_key, season, exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=30)

    return {
        "metric_key": metric_key,
        "season": season,
        "results_written": count,
    }
