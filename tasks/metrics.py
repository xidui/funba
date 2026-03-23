"""Celery tasks for metric computation.

Two-phase MapReduce pipeline:
  Phase 1 (metrics queue): compute_game_delta — compute delta, write MetricRunLog only.
  Phase 2 (reduce queue):  reduce_metric_season / reduce_metric_compute_run — aggregate deltas, write MetricResult.

Claim lifecycle (Phase 1)
-------------------------
1. INSERT IGNORE into MetricJobClaim with status='in_progress'.
   - rowcount=1  → this worker owns the job, proceed.
   - rowcount=0  → another row exists; check its status:
       - 'done'        → computation already committed, skip safely.
       - 'in_progress' → another worker is processing (or crashed); skip.
         Crashed 'in_progress' rows are cleared via --force in dispatch.

2. Computation succeeds → UPDATE status='done'.
   Legacy/daily paths still auto-trigger reduce inline. Bulk backfill paths
   register a MetricComputeRun and let the sweeper trigger reduce off the hot path.

3. Computation fails → DELETE claim row. Celery retries.

4. Worker crash → claim stays 'in_progress'.
   After _LEASE_SECONDS the next delivery auto-recovers.
"""
from __future__ import annotations

from contextlib import contextmanager
import logging
from datetime import datetime

from celery import shared_task
from sqlalchemy import func, text
from sqlalchemy.orm import sessionmaker

from db.models import MetricComputeRun, MetricJobClaim, MetricResult, MetricRunLog, engine
from metrics.framework.runner import run_delta_only, reduce_metric

logger = logging.getLogger(__name__)

_STATUS_IN_PROGRESS = "in_progress"
_STATUS_DONE = "done"
_LEASE_SECONDS = 600
_RUN_STATUS_MAPPING = "mapping"
_RUN_STATUS_REDUCING = "reducing"
_RUN_STATUS_COMPLETE = "complete"
_RUN_STATUS_FAILED = "failed"
_REDUCE_LOCK_PREFIX = "metric_reduce_"
_REDUCE_STALE_REQUEUE_SECONDS = 1800


class ReduceLockUnavailable(RuntimeError):
    """Raised when a per-metric reduce lock is already held."""


def _session_factory():
    return sessionmaker(bind=engine)


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


def _has_mapping_compute_run(session, metric_key: str) -> bool:
    return (
        session.query(MetricComputeRun.id)
        .filter(
            MetricComputeRun.metric_key == metric_key,
            MetricComputeRun.status == _RUN_STATUS_MAPPING,
        )
        .first()
        is not None
    )


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


def _done_claim_count_for_run(session, run: MetricComputeRun) -> int:
    game_ids = _run_game_ids_query(session, run).subquery()
    return int(
        session.query(func.count())
        .select_from(MetricJobClaim)
        .filter(
            MetricJobClaim.metric_key == run.metric_key,
            MetricJobClaim.status == _STATUS_DONE,
            MetricJobClaim.game_id.in_(session.query(game_ids.c.game_id)),
        )
        .scalar()
        or 0
    )


def _active_claim_count_for_run(session, run: MetricComputeRun) -> int:
    game_ids = _run_game_ids_query(session, run).subquery()
    return int(
        session.query(func.count())
        .select_from(MetricJobClaim)
        .filter(
            MetricJobClaim.metric_key == run.metric_key,
            MetricJobClaim.status == _STATUS_IN_PROGRESS,
            MetricJobClaim.game_id.in_(session.query(game_ids.c.game_id)),
        )
        .scalar()
        or 0
    )


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
        if _has_mapping_compute_run(session, metric_key):
            triggered_seasons = []
        else:
            triggered_seasons = _maybe_trigger_reduce(session, metric_key)

    return {
        "game_id": game_id,
        "metric_key": metric_key,
        "produced": produced,
        "reduce_triggered": triggered_seasons,
    }


@shared_task(
    bind=True,
    name="tasks.metrics.sweep_metric_compute_runs",
    max_retries=1,
    default_retry_delay=30,
    queue="reduce",
)
def sweep_metric_compute_runs_task(self) -> dict:
    """Promote completed mapping runs into the reduce queue exactly once.

    Reduce work is globally serialized by an advisory lock, so keep at most one
    active backlog of reducing runs at a time. Existing reducing runs are
    repaired first; only when none remain do we promote the next completed
    mapping run.
    """
    SessionLocal = _session_factory()
    promoted: list[str] = []
    finalized: list[str] = []
    requeued: list[str] = []
    checked = 0

    try:
        with SessionLocal() as session:
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
                    break

            if (
                session.query(MetricComputeRun.id)
                .filter(MetricComputeRun.status == _RUN_STATUS_REDUCING)
                .first()
                is not None
            ):
                return {
                    "checked_runs": checked,
                    "promoted_runs": promoted,
                    "finalized_runs": finalized,
                    "requeued_runs": requeued,
                }

            runs = (
                session.query(MetricComputeRun)
                .filter(MetricComputeRun.status == _RUN_STATUS_MAPPING)
                .order_by(MetricComputeRun.created_at.asc())
                .all()
            )

            for run in runs:
                checked += 1
                if _done_claim_count_for_run(session, run) < int(run.target_game_count):
                    continue
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
                break
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

        lock_name = _REDUCE_LOCK_PREFIX + metric_key
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
        lock_name = _REDUCE_LOCK_PREFIX + metric_key
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
