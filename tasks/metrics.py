"""Celery tasks for metric computation (Queue: metrics).

Claim lifecycle
---------------
1. INSERT IGNORE into MetricJobClaim with status='in_progress'.
   - rowcount=1  → this worker owns the job, proceed.
   - rowcount=0  → another row exists; check its status:
       - 'done'        → computation already committed, skip safely.
       - 'in_progress' → another worker is processing (or crashed); skip.
         Crashed 'in_progress' rows are cleared via --force in dispatch.

2. Computation succeeds → UPDATE status='done'.
   Future deliveries of the same task see status='done' and skip.

3. Computation fails (caught exception) → DELETE claim row.
   Celery retries the task; the retry can INSERT and claim afresh.

4. Worker crash (process killed mid-task) → claim row stays 'in_progress'.
   task_acks_late=True causes RabbitMQ to redeliver; the redelivered task
   sees 'in_progress' and skips. Use dispatch --force to clear and requeue.
"""
from __future__ import annotations

import logging
import random
import time
from datetime import datetime

from celery import shared_task
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from db.models import MetricJobClaim, engine
from metrics.framework.runner import run_for_game_single_metric

logger = logging.getLogger(__name__)

_STATUS_IN_PROGRESS = "in_progress"
_STATUS_DONE = "done"
_LEASE_SECONDS = 600  # treat in_progress claims older than 10 min as abandoned


def _session_factory():
    return sessionmaker(bind=engine)


def _try_claim(session, game_id: str, metric_key: str, worker_id: str) -> tuple[bool, str | None]:
    """Atomically try to claim (game_id, metric_key).

    Returns (owned, existing_status):
      (True,  None)          — successfully claimed, proceed
      (False, 'done')        — already computed, skip
      (False, 'in_progress') — another worker owns it and lease is still fresh, skip

    Lease timeout: if an in_progress claim is older than _LEASE_SECONDS, it is
    treated as abandoned (worker crashed before deleting it). We reclaim it via
    a conditional UPDATE so only one concurrent recovery worker wins.
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
        return True, None  # we own it

    # Row already exists — check its status and age
    row = (
        session.query(MetricJobClaim)
        .filter(
            MetricJobClaim.game_id == game_id,
            MetricJobClaim.metric_key == metric_key,
        )
        .first()
    )
    if row is None:
        # Race: row disappeared between INSERT and SELECT (very unlikely) — retry claim
        return _try_claim(session, game_id, metric_key, worker_id)

    if row.status == _STATUS_DONE:
        return False, _STATUS_DONE

    # status == in_progress — check lease age
    age = (datetime.utcnow() - row.claimed_at).total_seconds()
    if age < _LEASE_SECONDS:
        return False, _STATUS_IN_PROGRESS

    # Lease expired — attempt to reclaim via conditional UPDATE (only one worker wins)
    updated = (
        session.query(MetricJobClaim)
        .filter(
            MetricJobClaim.game_id == game_id,
            MetricJobClaim.metric_key == metric_key,
            MetricJobClaim.status == _STATUS_IN_PROGRESS,
            MetricJobClaim.claimed_at == row.claimed_at,  # exact match prevents double-reclaim
        )
        .update(
            {"claimed_at": datetime.utcnow(), "worker_id": worker_id},
            synchronize_session=False,
        )
    )
    session.commit()

    if updated == 1:
        logger.warning(
            "_try_claim: reclaimed expired in_progress claim for game=%s metric=%s (age=%.0fs)",
            game_id, metric_key, age,
        )
        return True, None
    # Another worker beat us to the reclaim
    return False, _STATUS_IN_PROGRESS


def _mark_done(session, game_id: str, metric_key: str) -> None:
    session.query(MetricJobClaim).filter(
        MetricJobClaim.game_id == game_id,
        MetricJobClaim.metric_key == metric_key,
    ).update({"status": _STATUS_DONE})
    session.commit()


def _release_claim(session, game_id: str, metric_key: str) -> None:
    """Delete claim on failure so the next retry can reclaim."""
    session.query(MetricJobClaim).filter(
        MetricJobClaim.game_id == game_id,
        MetricJobClaim.metric_key == metric_key,
    ).delete()
    session.commit()


@shared_task(
    bind=True,
    name="tasks.metrics.compute_game_metrics",
    max_retries=3,
    default_retry_delay=10,
    queue="metrics",
)
def compute_game_metrics(self, game_id: str, metric_key: str, force: bool = False) -> dict:
    """Compute one metric for one game and persist MetricResult + MetricRunLog."""
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        owned, existing_status = _try_claim(session, game_id, metric_key, self.request.id or "")

    if not owned:
        logger.info(
            "compute_game_metrics: game=%s metric=%s status=%s — skipping.",
            game_id, metric_key, existing_status,
        )
        return {
            "game_id": game_id,
            "metric_key": metric_key,
            "results_written": 0,
            "skipped": True,
            "reason": existing_status,
        }

    # We own the claim — compute, then update status or release on failure
    try:
        results = _compute_with_deadlock_retry(SessionLocal, game_id, metric_key, force=force)
    except Exception as exc:
        # Release claim so retry (or future dispatch) can reclaim
        with SessionLocal() as session:
            _release_claim(session, game_id, metric_key)
        logger.error(
            "compute_game_metrics: game=%s metric=%s failed, claim released: %s",
            game_id, metric_key, exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=10)

    # Mark done — future duplicate deliveries will skip
    with SessionLocal() as session:
        _mark_done(session, game_id, metric_key)

    logger.info(
        "compute_game_metrics: game=%s metric=%s produced %d results.",
        game_id, metric_key, len(results),
    )
    return {
        "game_id": game_id,
        "metric_key": metric_key,
        "results_written": len(results),
    }


def _compute_with_deadlock_retry(SessionLocal, game_id: str, metric_key: str, force: bool = False) -> list:
    for attempt in range(3):
        try:
            with SessionLocal() as session:
                return run_for_game_single_metric(session, game_id, metric_key, commit=True, force=force)
        except OperationalError as exc:
            if "1213" in str(exc) and attempt < 2:
                wait = 0.5 * (attempt + 1) + random.random()
                logger.warning(
                    "compute_game_metrics: deadlock on %s/%s, retrying in %.1fs (attempt %d)…",
                    game_id, metric_key, wait, attempt + 1,
                )
                time.sleep(wait)
            else:
                raise
    return []  # unreachable, but satisfies type checker
