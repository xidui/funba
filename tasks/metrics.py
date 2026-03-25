"""Celery tasks for metric computation.

Two-phase MapReduce pipeline:
  Phase 1 (metrics queue): compute_game_delta — compute delta, write MetricRunLog only.
  Phase 2 (reduce queue):  reduce_metric_season / reduce_metric_compute_run — aggregate deltas, write MetricResult.

Completion detection:
  - **Chord path (backfill)**: dispatch uses Celery chord. When all map tasks finish,
    chord_reduce_callback fires automatically and enqueues reduce. No polling needed.
  - **Ingest path (daily/single-game)**: tasks check MetricRunLog existence for
    idempotency. No claim table needed.
  - **Sweep fallback**: runs every 120s. Repairs stale reducing runs and force-promotes
    mapping runs stuck > 2 hours (chord counter lost due to Redis restart, etc.).
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import logging
from datetime import datetime

from celery import shared_task
from sqlalchemy import func, text
from sqlalchemy.orm import sessionmaker

from db.models import MetricComputeRun, MetricResult, MetricRunLog, engine
from metrics.framework.runner import run_delta_only, reduce_metric

logger = logging.getLogger(__name__)

_RUN_STATUS_MAPPING = "mapping"
_RUN_STATUS_REDUCING = "reducing"
_RUN_STATUS_COMPLETE = "complete"
_RUN_STATUS_FAILED = "failed"
_REDUCE_LOCK_PREFIX = "mr_"


def _reduce_lock_name(metric_key: str) -> str:
    """Build a per-metric advisory lock name that fits MySQL's 64-char limit."""
    name = _REDUCE_LOCK_PREFIX + metric_key
    if len(name) <= 64:
        return name
    # Hash to stay within limit while keeping prefix readable
    h = hashlib.md5(metric_key.encode()).hexdigest()[:16]
    return _REDUCE_LOCK_PREFIX + h

_REDUCE_STALE_REQUEUE_SECONDS = 1800


class ReduceLockUnavailable(RuntimeError):
    """Raised when a per-metric reduce lock is already held."""


_SessionLocal = sessionmaker(bind=engine)


def _session_factory():
    return _SessionLocal


@contextmanager
def _reduce_locked_session_factory(lock_name: str, timeout_seconds: int = 30):
    """Hold a per-metric MySQL advisory lock while yielding normal Session factories.

    The lock itself is connection-scoped in MySQL, so keep a dedicated lock
    connection open for the duration of the context. The ORM sessions then use
    normal pooled engine connections, which avoids binding write transactions to
    the advisory-lock connection.

    Different metrics use different lock names, allowing parallel reduce across metrics.
    """
    with engine.connect() as lock_conn:
        acquired = lock_conn.execute(
            text("SELECT GET_LOCK(:name, :timeout_seconds)"),
            {"name": lock_name, "timeout_seconds": int(timeout_seconds)},
        ).scalar()
        if acquired != 1:
            raise ReduceLockUnavailable(f"Failed to acquire reduce lock {lock_name!r}")
        SessionLocked = _session_factory()
        try:
            yield SessionLocked
        finally:
            try:
                lock_conn.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name})
            except Exception:
                logger.exception("Failed to release reduce lock %s", lock_name)


def _is_already_computed(session, game_id: str, metric_key: str) -> bool:
    """Check if MetricRunLog already has data for this (game, metric) pair."""
    return (
        session.query(MetricRunLog.game_id)
        .filter(MetricRunLog.game_id == game_id, MetricRunLog.metric_key == metric_key)
        .first()
    ) is not None


def _metric_seasons(session, metric_key: str) -> list[str]:
    return [
        r.season
        for r in session.query(MetricRunLog.season)
        .filter(MetricRunLog.metric_key == metric_key)
        .distinct()
        .all()
    ]


def _run_game_ids_query(session, run: MetricComputeRun):
    from db.models import Game

    q = session.query(Game.game_id).filter(Game.game_date.isnot(None))
    if run.target_season:
        q = q.filter(Game.season.like(f"{run.target_season}%"))
    if run.target_date_from:
        q = q.filter(Game.game_date >= run.target_date_from)
    if run.target_date_to:
        q = q.filter(Game.game_date <= run.target_date_to)
    return q



def _fresh_result_season_count_for_run(session, run: MetricComputeRun, seasons: list[str]) -> int:
    if not run.reduce_enqueued_at or not seasons:
        return 0
    return int(
        session.query(func.count(func.distinct(MetricResult.season)))
        .filter(
            MetricResult.metric_key == run.metric_key,
            MetricResult.season.in_(seasons),
            MetricResult.computed_at >= run.reduce_enqueued_at,
        )
        .scalar()
        or 0
    )


def _finalize_reducing_run_if_complete(session, run: MetricComputeRun) -> bool:
    seasons = _metric_seasons(session, run.metric_key)
    if not seasons:
        return False
    if _done_claim_count_for_run(session, run) < int(run.target_game_count or 0):
        return False
    if _active_claim_count_for_run(session, run) > 0:
        return False
    if _fresh_result_season_count_for_run(session, run, seasons) < len(seasons):
        return False
    _mark_run_complete(session, run.id)
    logger.info("finalized reducing run run_id=%s metric=%s from persisted reduce output", run.id, run.metric_key)
    return True


def _requeue_stale_reducing_run(session, run: MetricComputeRun, *, now: datetime | None = None) -> bool:
    now = now or datetime.utcnow()
    seasons = _metric_seasons(session, run.metric_key)
    if not seasons:
        return False
    if _done_claim_count_for_run(session, run) < int(run.target_game_count or 0):
        return False
    if _active_claim_count_for_run(session, run) > 0:
        return False
    if not run.reduce_enqueued_at:
        age_seconds = _REDUCE_STALE_REQUEUE_SECONDS
    else:
        age_seconds = int((now - run.reduce_enqueued_at).total_seconds())
    if age_seconds < _REDUCE_STALE_REQUEUE_SECONDS:
        return False
    if _fresh_result_season_count_for_run(session, run, seasons) >= len(seasons):
        return False

    updated = (
        session.query(MetricComputeRun)
        .filter(
            MetricComputeRun.id == run.id,
            MetricComputeRun.status == _RUN_STATUS_REDUCING,
        )
        .update(
            {
                "reduce_enqueued_at": now,
                "error_text": "re-enqueued stale reducing run",
            },
            synchronize_session=False,
        )
    )
    session.commit()
    if updated != 1:
        return False

    reduce_metric_compute_run_task.delay(run.id)
    logger.warning("re-enqueued stale reducing run run_id=%s metric=%s", run.id, run.metric_key)
    return True


def _promote_run_to_reducing(session, run_id: str) -> bool:
    updated = (
        session.query(MetricComputeRun)
        .filter(
            MetricComputeRun.id == run_id,
            MetricComputeRun.status == _RUN_STATUS_MAPPING,
        )
        .update(
            {
                "status": _RUN_STATUS_REDUCING,
                "reduce_enqueued_at": datetime.utcnow(),
            },
            synchronize_session=False,
        )
    )
    session.commit()
    return updated == 1


def _mark_run_complete(session, run_id: str) -> None:
    session.query(MetricComputeRun).filter(
        MetricComputeRun.id == run_id,
    ).update(
        {
            "status": _RUN_STATUS_COMPLETE,
            "completed_at": datetime.utcnow(),
            "error_text": None,
        },
        synchronize_session=False,
    )
    session.commit()
    _notify_owner_on_complete(session, run_id)


def _notify_owner_on_complete(session, run_id: str) -> None:
    """Send email to metric owner when backfill completes. Best-effort, never raises."""
    import os
    try:
        from db.models import MetricDefinition, User
        run = session.query(MetricComputeRun).filter(MetricComputeRun.id == run_id).first()
        if not run:
            return
        metric = session.query(MetricDefinition).filter(MetricDefinition.key == run.metric_key).first()
        if not metric or not metric.created_by_user_id:
            return
        user = session.query(User).filter(User.id == metric.created_by_user_id).first()
        if not user or not user.email:
            return
        resend_key = os.environ.get("RESEND_API_KEY")
        if not resend_key:
            return
        import resend
        resend.api_key = resend_key
        metric_url = f"https://funba.app/metrics/{metric.key}"
        resend.Emails.send({
            "from": "Funba <noreply@funba.app>",
            "to": [user.email],
            "subject": f"Your metric \"{metric.name}\" is ready",
            "html": (
                f'<div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:40px 20px;">'
                f'<h2 style="color:#f97316;margin-bottom:24px;">Funba</h2>'
                f'<p>Your metric <strong>{metric.name}</strong> has finished computing across all games.</p>'
                f'<a href="{metric_url}" style="display:inline-block;margin:24px 0;padding:12px 32px;'
                f'background:#f97316;color:#fff;text-decoration:none;border-radius:8px;font-weight:600;">'
                f'View Results</a>'
                f'</div>'
            ),
        })
        logger.info("sent metric-ready email to %s for metric %s", user.email, metric.key)
    except Exception:
        logger.exception("failed to send metric-ready email for run %s", run_id)


def _mark_run_failed(session, run_id: str, error_text: str) -> None:
    session.query(MetricComputeRun).filter(
        MetricComputeRun.id == run_id,
    ).update(
        {
            "status": _RUN_STATUS_FAILED,
            "failed_at": datetime.utcnow(),
            "error_text": error_text[:4000],
        },
        synchronize_session=False,
    )
    session.commit()


# ── Phase 1: Map (metrics queue) ─────────────────────────────────────────────

@shared_task(
    bind=True,
    name="tasks.metrics.compute_game_delta",
    max_retries=3,
    default_retry_delay=10,
    queue="metrics",
    ignore_result=False,
)
def compute_game_delta(self, game_id: str, metric_key: str, run_id: str | None = None) -> dict:
    """Phase 1: compute delta for one (game, metric) and write MetricRunLog only.

    Idempotency is via MetricRunLog existence check — if the delta was already
    written, the task is skipped.  For chord backfill (run_id set), the chord
    callback triggers reduce.  For the legacy ingest path (run_id=None), reduce
    is not triggered inline (the daily ingest handles it separately).
    """
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        if _is_already_computed(session, game_id, metric_key):
            return {"game_id": game_id, "metric_key": metric_key, "skipped": True, "reason": "already_computed"}

    try:
        with SessionLocal() as session:
            produced = run_delta_only(session, game_id, metric_key, commit=True)
    except Exception as exc:
        logger.error(
            "compute_game_delta: game=%s metric=%s failed: %s",
            game_id, metric_key, exc, exc_info=True,
        )
        raise self.retry(exc=exc, countdown=10)

    return {
        "game_id": game_id,
        "metric_key": metric_key,
        "produced": produced,
        "reduce_triggered": [],
    }


_CHORD_FALLBACK_SECONDS = 7200  # 2 hours — sweep promotes stuck mapping runs


@shared_task(
    bind=True,
    name="tasks.metrics.chord_reduce_callback",
    max_retries=2,
    default_retry_delay=30,
    queue="reduce",
    ignore_result=True,
)
def chord_reduce_callback(self, results: list, run_id: str) -> dict:
    """Chord callback: all map tasks finished. Promote run and trigger reduce."""
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        run = session.query(MetricComputeRun).filter(MetricComputeRun.id == run_id).first()
        if run is None:
            logger.warning("chord_reduce_callback: run %s not found", run_id)
            return {"run_id": run_id, "skipped": True, "reason": "missing"}
        if run.status != _RUN_STATUS_MAPPING:
            logger.info("chord_reduce_callback: run %s already %s", run_id, run.status)
            return {"run_id": run_id, "skipped": True, "reason": run.status}

    with SessionLocal() as session:
        promoted = _promote_run_to_reducing(session, run_id)
    if not promoted:
        logger.warning("chord_reduce_callback: promote failed for run %s", run_id)
        return {"run_id": run_id, "skipped": True, "reason": "promote_failed"}

    reduce_metric_compute_run_task.delay(run_id)
    logger.info("chord_reduce_callback: enqueued reduce for run %s", run_id)
    return {"run_id": run_id, "status": "reduce_enqueued"}


@shared_task(
    bind=True,
    name="tasks.metrics.sweep_metric_compute_runs",
    max_retries=1,
    default_retry_delay=30,
    queue="reduce",
)
def sweep_metric_compute_runs_task(self) -> dict:
    """Safety-net sweeper for metric compute runs.

    Primary completion detection is now via Celery chord callbacks.
    This sweeper handles two fallback scenarios:
    1. Stale reducing runs — reduce worker crashed, requeue.
    2. Stuck mapping runs — chord counter lost (Redis restart), force promote
       after _CHORD_FALLBACK_SECONDS (2 hours).
    """
    SessionLocal = _session_factory()
    promoted: list[str] = []
    finalized: list[str] = []
    requeued: list[str] = []
    checked = 0

    try:
        with SessionLocal() as session:
            # --- Repair stale reducing runs (unchanged) ---
            reducing_runs = (
                session.query(MetricComputeRun)
                .filter(MetricComputeRun.status == _RUN_STATUS_REDUCING)
                .order_by(MetricComputeRun.reduce_enqueued_at.asc(), MetricComputeRun.created_at.asc())
                .all()
            )
            now = datetime.utcnow()
            for run in reducing_runs:
                if _finalize_reducing_run_if_complete(session, run):
                    finalized.append(run.id)
                    continue
                if _requeue_stale_reducing_run(session, run, now=now):
                    requeued.append(run.id)

            # Collect metric_keys that already have an active reducing run.
            active_reducing_keys = set(
                row[0] for row in
                session.query(MetricComputeRun.metric_key)
                .filter(MetricComputeRun.status == _RUN_STATUS_REDUCING)
                .all()
            )

            # --- Fallback: promote mapping runs stuck beyond chord timeout ---
            mapping_runs = (
                session.query(MetricComputeRun)
                .filter(MetricComputeRun.status == _RUN_STATUS_MAPPING)
                .order_by(MetricComputeRun.created_at.asc())
                .all()
            )

            for run in mapping_runs:
                checked += 1
                if run.metric_key in active_reducing_keys:
                    continue
                age = (now - run.created_at).total_seconds()
                if age < _CHORD_FALLBACK_SECONDS:
                    continue
                # Chord appears lost — check MetricRunLog coverage
                log_count = (
                    session.query(func.count(func.distinct(MetricRunLog.game_id)))
                    .filter(MetricRunLog.metric_key == run.metric_key)
                    .scalar() or 0
                )
                target = int(run.target_game_count)
                if log_count < target * 0.5:
                    # Less than 50% done after 2h — likely a real problem, skip
                    logger.warning(
                        "sweep: run %s stuck in mapping for %.0fs but only %d/%d games done, skipping",
                        run.id, age, log_count, target,
                    )
                    continue
                logger.warning(
                    "sweep: force-promoting run %s after %.0fs (%d/%d games done)",
                    run.id, age, log_count, target,
                )
                if not _promote_run_to_reducing(session, run.id):
                    continue
                try:
                    reduce_metric_compute_run_task.delay(run.id)
                except Exception as exc:
                    with SessionLocal() as revert_session:
                        revert_session.query(MetricComputeRun).filter(
                            MetricComputeRun.id == run.id,
                            MetricComputeRun.status == _RUN_STATUS_REDUCING,
                        ).update(
                            {
                                "status": _RUN_STATUS_MAPPING,
                                "reduce_enqueued_at": None,
                                "error_text": f"enqueue reduce failed: {exc}"[:4000],
                            },
                            synchronize_session=False,
                        )
                        revert_session.commit()
                    raise
                promoted.append(run.id)
                active_reducing_keys.add(run.metric_key)
    except Exception as exc:
        logger.error("sweep_metric_compute_runs failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=30)

    return {
        "checked_runs": checked,
        "promoted_runs": promoted,
        "finalized_runs": finalized,
        "requeued_runs": requeued,
    }


# ── Phase 2: Reduce (reduce queue) ───────────────────────────────────────────


@shared_task(
    bind=True,
    name="tasks.metrics.reduce_metric_compute_run",
    max_retries=2,
    default_retry_delay=30,
    queue="reduce",
)
def reduce_metric_compute_run_task(self, run_id: str) -> dict:
    """Reduce all seasons for one MetricComputeRun and mark the run complete."""
    metric_key = ""
    seasons: list[str] = []

    try:
        # Look up the run's metric_key before acquiring the per-metric lock.
        with _session_factory()() as session:
            run = session.query(MetricComputeRun).filter(MetricComputeRun.id == run_id).first()
            if run is None:
                return {"run_id": run_id, "skipped": True, "reason": "missing"}
            if run.status == _RUN_STATUS_COMPLETE:
                return {"run_id": run_id, "skipped": True, "reason": _RUN_STATUS_COMPLETE}
            if run.status != _RUN_STATUS_REDUCING:
                return {"run_id": run_id, "skipped": True, "reason": run.status}
            metric_key = run.metric_key

        lock_name = _reduce_lock_name(metric_key)
        with _reduce_locked_session_factory(lock_name, timeout_seconds=0) as SessionLocked:
            with SessionLocked() as session:
                seasons = _metric_seasons(session, metric_key)

            results_written = 0
            for season in seasons:
                with SessionLocked() as session:
                    results_written += reduce_metric(session, metric_key, season, commit=True)

            with SessionLocked() as session:
                _mark_run_complete(session, run_id)
    except ReduceLockUnavailable as exc:
        logger.info(
            "reduce_metric_compute_run: run_id=%s metric=%s waiting for reduce lock",
            run_id, metric_key,
        )
        raise self.retry(exc=exc, countdown=5, max_retries=1000)
    except Exception as exc:
        logger.error(
            "reduce_metric_compute_run: run_id=%s failed: %s",
            run_id, exc, exc_info=True,
        )
        if self.request.retries >= self.max_retries:
            with _session_factory()() as session:
                _mark_run_failed(session, run_id, str(exc))
            raise
        raise self.retry(exc=exc, countdown=30)

    return {
        "run_id": run_id,
        "metric_key": metric_key,
        "seasons_reduced": seasons,
        "results_written": results_written,
    }

@shared_task(
    bind=True,
    name="tasks.metrics.reduce_metric_season",
    max_retries=2,
    default_retry_delay=30,
    queue="reduce",
)
def reduce_metric_season_task(self, metric_key: str, season: str) -> dict:
    """Phase 2: aggregate all deltas for (metric_key, season) → write MetricResults."""
    try:
        lock_name = _reduce_lock_name(metric_key)
        with _reduce_locked_session_factory(lock_name, timeout_seconds=0) as SessionLocked:
            with SessionLocked() as session:
                count = reduce_metric(session, metric_key, season, commit=True)
    except ReduceLockUnavailable as exc:
        logger.info(
            "reduce_metric_season: metric=%s season=%s waiting for reduce lock",
            metric_key, season,
        )
        raise self.retry(exc=exc, countdown=5, max_retries=1000)
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
