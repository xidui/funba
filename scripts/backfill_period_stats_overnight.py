#!/usr/bin/env python3
"""Run period backfill season-by-season with automatic batching and resume support."""

import subprocess
import time
from datetime import datetime
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from sqlalchemy import create_engine, text

from db.config import get_database_url


def get_default_seasons():
    return ["22024", "22023", "22022", "22021"]


def remaining_for_season(conn, season: str) -> int:
    """Return how many completed games are still missing 4+ period rows."""
    total = conn.execute(
        text(
            "SELECT COUNT(*) "
            "FROM `Game` "
            "WHERE season=:s AND game_date IS NOT NULL AND home_team_score IS NOT NULL"
        ),
        {"s": season},
    ).scalar()

    done = conn.execute(
        text(
            "SELECT COUNT(*) FROM ("
            "  SELECT pgs.game_id "
            "  FROM PlayerGamePeriodStats pgs "
            "  JOIN `Game` g ON g.game_id = pgs.game_id "
            "  WHERE g.season=:s "
            "  GROUP BY pgs.game_id "
            "  HAVING COUNT(DISTINCT pgs.period) >= 4"
            ") t"
        ),
        {"s": season},
    ).scalar()

    return (total or 0) - (done or 0)


def run_backfill_batch(season: str, batch_limit: int, workers: int) -> int:
    cmd = [
        sys.executable,
        "-m",
        "db.backfill_period_stats",
        "--season",
        season,
        "--limit",
        str(batch_limit),
        "--workers",
        str(workers),
    ]
    result = subprocess.run(cmd, cwd=PROJECT_ROOT, check=False)
    return result.returncode


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Backfill period stats season-by-season in batched passes."
    )
    parser.add_argument(
        "seasons",
        nargs="*",
        default=get_default_seasons(),
        help="Seasons to process, e.g. 22024 22023",
    )
    parser.add_argument("--limit", type=int, default=194, help="Batch size per season pass.")
    parser.add_argument("--workers", type=int, default=1, help="Backfill workers per batch.")
    parser.add_argument(
        "--max-stagnant",
        type=int,
        default=5,
        help="How many batches with no progress before pausing.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=int,
        default=60,
        help="Pause duration in seconds after stagnant streak.",
    )

    args = parser.parse_args()
    seasons = args.seasons or get_default_seasons()
    batch_limit = args.limit
    workers = args.workers
    max_stagnant = args.max_stagnant
    pause_seconds = args.pause_seconds

    db_url = get_database_url()
    engine = create_engine(db_url)

    for season in seasons:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] start season {season}")
        stagnant = 0

        while True:
            with engine.connect() as conn:
                remaining = remaining_for_season(conn, season)

            if remaining <= 0:
                print(f"[{datetime.now().isoformat(timespec='seconds')}] season {season} complete")
                break

            current_limit = min(batch_limit, remaining)
            print(
                f"[{datetime.now().isoformat(timespec='seconds')}] season {season}: "
                f"remaining={remaining}, next batch={current_limit}, workers={workers}"
            )
            ret = run_backfill_batch(season, current_limit, workers)
            print(
                f"[{datetime.now().isoformat(timespec='seconds')}] season {season}: "
                f"batch exit={ret}"
            )

            with engine.connect() as conn:
                new_remaining = remaining_for_season(conn, season)

            if new_remaining >= remaining:
                stagnant += 1
                print(
                    f"[{datetime.now().isoformat(timespec='seconds')}] season {season}: "
                    f"remaining no-down ({remaining} -> {new_remaining}), "
                    f"stagnant={stagnant}/{max_stagnant}"
                )
                if stagnant >= max_stagnant:
                    print(
                        f"[{datetime.now().isoformat(timespec='seconds')}] "
                        f"season {season}: high stagnation, pause {pause_seconds}s"
                    )
                    time.sleep(pause_seconds)
                    stagnant = 0
            else:
                stagnant = 0

            time.sleep(2)

    print(f"[{datetime.now().isoformat(timespec='seconds')}] all requested seasons finished")


if __name__ == "__main__":
    main()
