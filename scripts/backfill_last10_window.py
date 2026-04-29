"""One-shot: populate the last10 window sibling for every eligible metric.

For each published MetricDefinition with supports_career=True, trigger=season,
and scope in {player, team, player_franchise}, runs the auto-registered
*_last10 sibling against last10_regular / last10_playoffs / last10_playin
buckets (filtered by the metric's season_types).

Existing rows get overwritten — replace_existing=True in run_season_metric
already prunes anything that drops out of the new top-N.

Usage:
    NBA_DB_URL=mysql+pymysql://... \\
        .venv/bin/python -m scripts.backfill_last10_window [--dry-run] [--metric KEY]

Flags:
    --dry-run        list which (metric, bucket) pairs would run without doing it
    --metric KEY     restrict to a single metric key (base key, no _last10 suffix)
    --workers N      parallel workers for independent (metric, bucket) tasks (default 4)
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy.orm import Session

from db.models import MetricDefinition, engine
from metrics.framework.base import WINDOW_SEASONS, season_matches_metric_types
from metrics.framework.family import family_window_key
from metrics.framework.runner import run_season_metric
from metrics.framework.runtime import get_metric


_LAST10_BUCKETS = WINDOW_SEASONS["last10"]


logger = logging.getLogger("backfill_last10")


def _eligible_metrics(session: Session, only_key: str | None = None) -> list[MetricDefinition]:
    """All published, non-career, non-game/season scope rows. Whether the row
    actually supports a last10 sibling is decided per-row via get_metric()
    (the runtime layer parses definition_json/code_python and decides)."""
    q = session.query(MetricDefinition).filter(
        MetricDefinition.status == "published",
        MetricDefinition.scope.in_(("player", "team", "player_franchise")),
    )
    if only_key:
        q = q.filter(MetricDefinition.key == only_key)
    return q.order_by(MetricDefinition.key.asc()).all()


def _plan(session: Session, metric: MetricDefinition) -> list[tuple[str, str]]:
    """Return (window_key, bucket) pairs that should run for this metric.

    Returns [] when the metric doesn't support a last10 sibling (e.g. game-
    or season-scope, no supports_career, trigger != season, etc.)."""
    base_runtime = get_metric(metric.key, session=session)
    if base_runtime is None:
        return []
    if getattr(base_runtime, "career", False):
        return []
    if getattr(base_runtime, "trigger", "game") != "season":
        return []
    if not getattr(base_runtime, "supports_career", False):
        return []

    window_key = family_window_key(metric.key, "last10")
    runtime_metric = get_metric(window_key, session=session)
    if runtime_metric is None:
        return []
    season_types = getattr(runtime_metric, "season_types", None)
    return [
        (window_key, bucket)
        for bucket in _LAST10_BUCKETS
        if season_matches_metric_types(bucket, season_types)
    ]


def _run_one(window_key: str, bucket: str) -> tuple[str, str, int, str | None]:
    """Run a single (metric_key, bucket) and return (key, bucket, rows_written, error)."""
    started = time.perf_counter()
    with Session(engine) as session:
        try:
            count = run_season_metric(session, window_key, bucket)
            elapsed = time.perf_counter() - started
            logger.info("ran %s @ %s → %d rows in %.1fs", window_key, bucket, count, elapsed)
            return window_key, bucket, count, None
        except Exception as exc:
            logger.exception("FAILED %s @ %s", window_key, bucket)
            return window_key, bucket, 0, repr(exc)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="list pairs without running")
    parser.add_argument("--metric", default=None, help="restrict to one base metric key")
    parser.add_argument("--workers", type=int, default=4, help="parallel workers (default 4)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with Session(engine) as session:
        metrics = _eligible_metrics(session, only_key=args.metric)
        plans: list[tuple[str, str]] = []
        for m in metrics:
            plans.extend(_plan(session, m))

    if not plans:
        logger.info("no eligible (metric, bucket) pairs — nothing to do.")
        return 0

    logger.info(
        "planned %d (metric, bucket) pairs across %d metrics (workers=%d, dry_run=%s)",
        len(plans), len({p[0] for p in plans}), args.workers, args.dry_run,
    )
    if args.dry_run:
        for window_key, bucket in plans:
            print(f"{window_key}\t{bucket}")
        return 0

    total_rows = 0
    failures: list[tuple[str, str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(_run_one, wk, b) for wk, b in plans]
        for fut in as_completed(futures):
            window_key, bucket, count, err = fut.result()
            if err:
                failures.append((window_key, bucket, err))
            else:
                total_rows += count

    logger.info(
        "done: wrote %d rows across %d successful pairs; %d failures.",
        total_rows, len(plans) - len(failures), len(failures),
    )
    if failures:
        for wk, b, err in failures[:20]:
            logger.error("  %s @ %s: %s", wk, b, err)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
