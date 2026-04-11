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
from datetime import date, datetime
from random import randint
import uuid

from celery import chord, shared_task
from sqlalchemy import func, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError as SAOperationalError

from db.game_status import completed_game_clause
from db.models import Game, MetricComputeRun, MetricResult, MetricRunLog, engine
from metrics.framework.base import is_career_season
from metrics.framework.runner import run_delta_only, reduce_metric, run_season_metric

logger = logging.getLogger(__name__)

_RUN_STATUS_MAPPING = "mapping"
_RUN_STATUS_REDUCING = "reducing"
_RUN_STATUS_COMPLETE = "complete"
_RUN_STATUS_FAILED = "failed"
_REDUCE_LOCK_PREFIX = "mr_"
_SEASON_COMPUTE_LOCK_PREFIX = "ms_"
_CAREER_BUCKET_LOCK_PREFIX = "msb_"


def _lock_name(prefix: str, *parts: str) -> str:
    """Build an advisory lock name that fits MySQL's 64-char limit."""
    raw = ":".join(str(part) for part in parts if part)
    name = prefix + raw
    if len(name) <= 64:
        return name
    h = hashlib.md5(raw.encode()).hexdigest()[:16]
    return prefix + h


def _reduce_lock_name(metric_key: str) -> str:
    return _lock_name(_REDUCE_LOCK_PREFIX, metric_key)


def _season_compute_lock_name(metric_key: str, season: str) -> str:
    return _lock_name(_SEASON_COMPUTE_LOCK_PREFIX, metric_key, season)


def _career_bucket_lock_name(season: str) -> str:
    return _lock_name(_CAREER_BUCKET_LOCK_PREFIX, season)

_REDUCE_STALE_REQUEUE_SECONDS = 1800
_SEASON_METRIC_DEADLOCK_MAX_RETRIES = 5


def _is_retryable_mysql_deadlock(exc: Exception) -> bool:
    """Return True for transient MySQL deadlocks that are worth retrying."""
    if not isinstance(exc, SAOperationalError):
        return False
    orig = getattr(exc, "orig", None)
    args = getattr(orig, "args", ()) if orig is not None else ()
    if args and args[0] == 1213:
        return True
    return "deadlock found when trying to get lock" in str(exc).lower()


def _deadlock_retry_countdown(retries: int) -> int:
    """Back off retries with slight jitter to reduce synchronized contention."""
    base = min(300, 15 * (2 ** max(retries, 0)))
    return base + randint(0, 7)


def _increment_compute_run_progress(run_id: str) -> None:
    """Atomically increment done_game_count; mark complete when target reached."""
    sess = sessionmaker(bind=engine)()
    try:
        sess.execute(
            text(
                "UPDATE MetricComputeRun "
                "SET done_game_count = done_game_count + 1, "
                "    status = CASE WHEN done_game_count + 1 >= target_game_count "
                "                  THEN 'complete' ELSE status END, "
                "    completed_at = CASE WHEN done_game_count + 1 >= target_game_count "
                "                       THEN NOW() ELSE completed_at END "
                "WHERE id = :run_id AND status = 'mapping'"
            ),
            {"run_id": run_id},
        )
        sess.commit()
    except Exception:
        logger.exception("Failed to increment compute run progress for %s", run_id)
        sess.rollback()
    finally:
        sess.close()


class AdvisoryLockUnavailable(RuntimeError):
    """Raised when an advisory lock is already held."""


_SessionLocal = sessionmaker(bind=engine)


def _session_factory():
    return _SessionLocal


def _parse_optional_date(value: str | None):
    return date.fromisoformat(value) if value else None


def create_metric_compute_run(
    metric_key: str,
    target_game_count: int,
    *,
    season: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[MetricComputeRun, bool]:
    """Create a new MetricComputeRun unless one is already active for this metric."""
    with _session_factory()() as session:
        existing = (
            session.query(MetricComputeRun)
            .filter(
                MetricComputeRun.metric_key == metric_key,
                MetricComputeRun.status.in_(("mapping", "reducing")),
            )
            .order_by(MetricComputeRun.created_at.desc())
            .first()
        )
        if existing is not None:
            session.expunge(existing)
            return existing, False

        session.query(MetricComputeRun).filter(
            MetricComputeRun.metric_key == metric_key,
            MetricComputeRun.status.in_(("complete", "failed")),
        ).delete(synchronize_session=False)

        run = MetricComputeRun(
            id=str(uuid.uuid4()),
            metric_key=metric_key,
            status="mapping",
            target_season=season,
            target_date_from=_parse_optional_date(date_from),
            target_date_to=_parse_optional_date(date_to),
            target_game_count=int(target_game_count),
            created_at=datetime.utcnow(),
            started_at=datetime.utcnow(),
        )
        session.add(run)
        session.commit()
        session.refresh(run)
        session.expunge(run)
        return run, True


def enqueue_season_metric_refresh(
    seasons: list[str] | set[str],
    *,
    metrics: list | None = None,
) -> dict:
    from metrics.framework.base import CAREER_SEASONS, career_season_for, season_matches_metric_types
    from metrics.framework.runtime import get_all_metrics

    affected_seasons = sorted({season for season in seasons if season})
    if not affected_seasons:
        return {"status": "no_seasons"}

    if metrics is None:
        metrics = [
            m for m in get_all_metrics()
            if getattr(m, "trigger", "game") == "season" and not getattr(m, "career", False)
        ]

    career_buckets = {career_season_for(season) for season in affected_seasons if career_season_for(season)}

    enqueued = 0
    callbacks = 0
    scheduled_metrics = 0
    for m in metrics:
        eligible_seasons = [
            season for season in affected_seasons
            if season_matches_metric_types(season, getattr(m, "season_types", None))
        ]
        eligible_career_buckets = [
            bucket for bucket in sorted(CAREER_SEASONS)
            if bucket in career_buckets and season_matches_metric_types(bucket, getattr(m, "season_types", None))
        ]
        if not eligible_seasons and not (getattr(m, "supports_career", False) and eligible_career_buckets):
            continue

        scheduled_metrics += 1
        has_career = getattr(m, "supports_career", False) and bool(eligible_career_buckets)
        task_count = len(eligible_seasons) + (len(eligible_career_buckets) if has_career else 0)
        run, created = create_metric_compute_run(m.key, task_count)
        run_id = run.id if created else None
        if not created:
            logger.info("season metric refresh: active compute run exists for %s (%s)", m.key, run.id)

        if has_career:
            season_tasks = [compute_season_metric_task.s(m.key, season, run_id=run_id) for season in eligible_seasons]
            chord(season_tasks)(
                enqueue_career_metric_family_task.s(
                    metric_key=m.key,
                    run_id=run_id,
                    buckets=eligible_career_buckets,
                )
            )
            enqueued += len(season_tasks)
            callbacks += 1
        else:
            for season in eligible_seasons:
                compute_season_metric_task.delay(m.key, season, run_id=run_id)
                enqueued += 1

    logger.info(
        "enqueue_season_metric_refresh: seasons=%s, career_buckets=%s, enqueued %d season task(s) with %d career callback(s) for %d metric(s).",
        affected_seasons,
        sorted(career_buckets),
        enqueued,
        callbacks,
        scheduled_metrics,
    )
    return {
        "seasons": affected_seasons,
        "career_buckets": sorted(career_buckets),
        "metrics": scheduled_metrics,
        "enqueued": enqueued,
        "callbacks": callbacks,
    }


@contextmanager
def _reduce_locked_session_factory(lock_name: str, timeout_seconds: int = 30):
    """Hold a MySQL advisory lock while yielding normal Session factories.

    The lock itself is connection-scoped in MySQL, so keep a dedicated lock
    connection open for the duration of the context. The ORM sessions then use
    normal pooled engine connections, which avoids binding write transactions to
    the advisory-lock connection.

    Different lock names allow independent work to proceed in parallel.
    """
    with engine.connect() as lock_conn:
        acquired = lock_conn.execute(
            text("SELECT GET_LOCK(:name, :timeout_seconds)"),
            {"name": lock_name, "timeout_seconds": int(timeout_seconds)},
        ).scalar()
        if acquired != 1:
            raise AdvisoryLockUnavailable(f"Failed to acquire advisory lock {lock_name!r}")
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

    q = session.query(Game.game_id).filter(
        Game.game_date.isnot(None),
        completed_game_clause(Game),
    )
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
    """Mark a reducing run as complete if all seasons have fresh results."""
    seasons = _metric_seasons(session, run.metric_key)
    if not seasons:
        return False
    if _fresh_result_season_count_for_run(session, run, seasons) < len(seasons):
        return False
    _mark_run_complete(session, run.id)
    logger.info("finalized reducing run run_id=%s metric=%s from persisted reduce output", run.id, run.metric_key)
    return True


def _requeue_stale_reducing_run(session, run: MetricComputeRun, *, now: datetime | None = None) -> bool:
    """Requeue a reducing run if it's been stuck for too long."""
    now = now or datetime.utcnow()
    seasons = _metric_seasons(session, run.metric_key)
    if not seasons:
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
    callback triggers reduce.  For the ingest path (run_id=None), the ingest
    chord callback (reduce_after_ingest) triggers reduce.
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

    # Increment done counter on the compute run (atomic UPDATE ... + 1)
    if run_id is not None and produced:
        with SessionLocal() as session:
            session.query(MetricComputeRun).filter(
                MetricComputeRun.id == run_id,
            ).update(
                {"done_game_count": MetricComputeRun.done_game_count + 1},
                synchronize_session=False,
            )
            session.commit()

    return {
        "game_id": game_id,
        "metric_key": metric_key,
        "produced": produced,
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
    except AdvisoryLockUnavailable as exc:
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
    name="tasks.metrics.reduce_after_ingest",
    max_retries=2,
    default_retry_delay=30,
    queue="reduce",
    ignore_result=True,
)
def reduce_after_ingest(self, results: list, game_id: str) -> dict:
    """Chord callback after all metric deltas for one ingested game complete.

    Collects the (metric_key, season) pairs that produced data, deduplicates,
    and dispatches one reduce_metric_season per unique pair. Also updates
    target_game_count on complete MetricComputeRuns.
    """
    from db.models import Game
    SessionLocal = _session_factory()

    with SessionLocal() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if not game or not game.season:
            return {"game_id": game_id, "skipped": True, "reason": "game_not_found"}
        season = game.season

    # Collect metric keys that produced data
    metric_keys = set()
    for r in (results or []):
        if isinstance(r, dict) and r.get("produced") and not r.get("skipped"):
            metric_keys.add(r["metric_key"])

    enqueued = 0
    for key in metric_keys:
        reduce_metric_season_task.delay(key, season)
        enqueued += 1

    # Update target_game_count on complete runs so progress stays at 100%
    if metric_keys:
        with SessionLocal() as session:
            total_games = (
                session.query(func.count(Game.game_id))
                .filter(Game.game_date.isnot(None), completed_game_clause(Game))
                .scalar() or 0
            )
            session.query(MetricComputeRun).filter(
                MetricComputeRun.status == "complete",
                MetricComputeRun.metric_key.in_(metric_keys),
            ).update({"target_game_count": total_games}, synchronize_session=False)
            session.commit()

    # Season-triggered metrics are batch jobs, not per-game fanout.
    logger.info("reduce_after_ingest: game=%s enqueued %d reduce task(s) for season %s", game_id, enqueued, season)
    return {"game_id": game_id, "season": season, "enqueued": enqueued}


@shared_task(
    bind=True,
    name="tasks.metrics.compute_season_metric",
    max_retries=2,
    default_retry_delay=30,
    queue="metrics",
    ignore_result=False,
)
def compute_season_metric_task(self, metric_key: str, season: str, run_id: str | None = None) -> dict:
    """Compute a season-triggered metric for one (metric_key, season)."""
    try:
        lock_name = _season_compute_lock_name(metric_key, season)
        with _reduce_locked_session_factory(lock_name, timeout_seconds=0) as SessionLocked:
            with SessionLocked() as session:
                count = run_season_metric(session, metric_key, season, commit=True)
    except AdvisoryLockUnavailable:
        logger.info(
            "compute_season_metric: metric=%s season=%s already running; skipping duplicate dispatch",
            metric_key,
            season,
        )
        return {"metric_key": metric_key, "season": season, "skipped": True, "reason": "already_running"}
    except Exception as exc:
        logger.error("compute_season_metric: metric=%s season=%s failed: %s",
                     metric_key, season, exc, exc_info=True)
        if _is_retryable_mysql_deadlock(exc):
            if self.request.retries >= _SEASON_METRIC_DEADLOCK_MAX_RETRIES:
                if run_id:
                    with _session_factory()() as session:
                        _mark_run_failed(session, run_id, f"season {season} failed after deadlock retries: {exc}")
                raise
            raise self.retry(
                exc=exc,
                countdown=_deadlock_retry_countdown(self.request.retries),
                max_retries=_SEASON_METRIC_DEADLOCK_MAX_RETRIES,
            )
        if self.request.retries >= self.max_retries:
            if run_id:
                with _session_factory()() as session:
                    _mark_run_failed(session, run_id, f"season {season} failed: {exc}")
            raise
        raise self.retry(exc=exc, countdown=30)

    if run_id:
        _increment_compute_run_progress(run_id)

    if season and str(season).startswith(("2", "4")):
        try:
            from tasks.content import ensure_recent_content_analysis_for_season_task

            ensure_recent_content_analysis_for_season_task.delay(season)
        except Exception as exc:
            logger.warning(
                "compute_season_metric: failed to enqueue content readiness check for season=%s: %s",
                season,
                exc,
            )

    return {"metric_key": metric_key, "season": season, "results_written": count}


@shared_task(
    bind=True,
    name="tasks.metrics.enqueue_career_metric_family",
    max_retries=1,
    default_retry_delay=30,
    queue="metrics",
    ignore_result=True,
)
def enqueue_career_metric_family_task(
    self,
    results: list,
    metric_key: str,
    run_id: str | None = None,
    buckets: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """Enqueue the three career buckets after all concrete seasons finish."""
    from metrics.framework.family import family_career_key
    from metrics.framework.base import season_matches_metric_types
    from metrics.framework.runtime import get_metric

    base_metric = get_metric(metric_key)
    if base_metric is None:
        return {"metric_key": metric_key, "skipped": True, "reason": "missing_base_metric"}
    if getattr(base_metric, "career", False) or not getattr(base_metric, "supports_career", False):
        return {"metric_key": metric_key, "skipped": True, "reason": "no_career_variant"}

    career_key = family_career_key(metric_key)
    career_metric = get_metric(career_key)
    if career_metric is None:
        return {"metric_key": metric_key, "skipped": True, "reason": "missing_career_metric"}

    enqueued = 0
    candidate_buckets = list(buckets) if buckets is not None else ["all_regular", "all_playoffs", "all_playin"]
    for bucket in candidate_buckets:
        if not season_matches_metric_types(bucket, getattr(career_metric, "season_types", None)):
            continue
        compute_season_metric_task.delay(career_key, bucket, run_id=run_id)
        enqueued += 1

    logger.info(
        "enqueue_career_metric_family: metric=%s career_metric=%s enqueued %d bucket(s)",
        metric_key,
        career_key,
        enqueued,
    )
    return {"metric_key": metric_key, "career_metric": career_key, "enqueued": enqueued}


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
    except AdvisoryLockUnavailable as exc:
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


@shared_task(
    bind=True,
    name="tasks.metrics.refresh_current_season_metrics",
    max_retries=1,
    default_retry_delay=60,
    queue="metrics",
)
def refresh_current_season_metrics(self, ingest_results: list | None = None) -> dict:
    """Recompute all season-triggered metrics for seasons that were just ingested.

    Designed as a chord callback after ingest_yesterday completes all games.
    Detects which seasons were affected from the ingested game IDs, then
    refreshes those seasons plus their corresponding career buckets.
    """
    Session = sessionmaker(bind=engine)

    # Extract game_ids from ingest results; skip if nothing actually changed
    game_ids = []
    any_changed = False
    for r in (ingest_results or []):
        if isinstance(r, dict) and r.get("game_id"):
            game_ids.append(r["game_id"])
            if r.get("new_game") or r.get("detail_pbp_refreshed") or r.get("shot_refreshed"):
                any_changed = True

    if game_ids and not any_changed:
        logger.info("refresh_current_season_metrics: %d games checked, none had new data — skipping.", len(game_ids))
        return {"status": "no_changes", "games_checked": len(game_ids)}

    with Session() as session:
        if game_ids:
            # Query actual seasons from ingested games
            affected_seasons = set(
                r[0] for r in session.query(Game.season).filter(
                    Game.game_id.in_(game_ids),
                    Game.season.isnot(None),
                ).distinct().all()
            )
        else:
            # Fallback: use the latest regular season
            all_seasons = [
                r[0] for r in session.query(Game.season).distinct().all()
                if r[0] and str(r[0]).startswith("2")
            ]
            affected_seasons = {max(all_seasons)} if all_seasons else set()

    if not affected_seasons:
        return {"status": "no_seasons"}
    return enqueue_season_metric_refresh(affected_seasons)
