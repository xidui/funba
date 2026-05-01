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


DEFAULT_MIN_PERIOD_STATS_SEASON = "21996"


def get_default_seasons():
    return ["22024", "22023", "22022", "22021"]


def _season_year_value(season: str) -> int:
    season = str(season)
    season_year = season[1:] if season[:1].isdigit() and len(season) > 4 else season
    try:
        return int(season_year)
    except ValueError:
        return -1


def _season_sort_key(season: str) -> tuple[int, int, str]:
    season = str(season)
    season_type = season[:1]
    year_value = _season_year_value(season)
    type_priority = {
        "2": 0,  # regular season
        "4": 1,  # playoffs
        "5": 2,  # preseason
        "3": 3,  # special / all-star
    }.get(season_type, 9)
    return (-year_value, type_priority, season)


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


def get_incomplete_seasons(engine, min_season: str | None = DEFAULT_MIN_PERIOD_STATS_SEASON) -> list[str]:
    min_year = _season_year_value(min_season) if min_season else None
    with engine.connect() as conn:
        seasons = [
            row[0]
            for row in conn.execute(
                text(
                    "SELECT DISTINCT season "
                    "FROM `Game` "
                    "WHERE season IS NOT NULL "
                    "  AND game_date IS NOT NULL "
                    "  AND home_team_score IS NOT NULL"
                )
            ).fetchall()
        ]

        incomplete = [
            season
            for season in seasons
            if (min_year is None or _season_year_value(season) >= min_year)
            and remaining_for_season(conn, season) > 0
        ]

    return sorted(incomplete, key=_season_sort_key)


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
    parser.add_argument(
        "--all-incomplete",
        action="store_true",
        help="Process every incomplete season, ordered by year desc and season type priority.",
    )
    parser.add_argument(
        "--min-season",
        default=DEFAULT_MIN_PERIOD_STATS_SEASON,
        help=(
            "Lowest season id/year included by --all-incomplete "
            f"(default {DEFAULT_MIN_PERIOD_STATS_SEASON}; use 0 to include every season)."
        ),
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
    parser.add_argument(
        "--max-pause-cycles",
        type=int,
        default=3,
        help="Skip a season after this many no-progress pause cycles.",
    )

    args = parser.parse_args()
    batch_limit = args.limit
    workers = args.workers
    max_stagnant = args.max_stagnant
    pause_seconds = args.pause_seconds
    max_pause_cycles = max(1, args.max_pause_cycles)

    db_url = get_database_url()
    engine = create_engine(db_url)
    min_season = None if str(args.min_season).strip() in {"", "0"} else args.min_season
    seasons = get_incomplete_seasons(engine, min_season) if args.all_incomplete else (args.seasons or get_default_seasons())

    print(
        f"[{datetime.now().isoformat(timespec='seconds')}] selected seasons="
        f"{len(seasons)}"
    )
    if seasons:
        print(
            f"[{datetime.now().isoformat(timespec='seconds')}] first seasons: "
            f"{' '.join(seasons[:10])}"
        )
    else:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] nothing to do")
        return

    for season in seasons:
        print(f"[{datetime.now().isoformat(timespec='seconds')}] start season {season}")
        stagnant = 0
        pause_cycles = 0

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
                    pause_cycles += 1
                    if pause_cycles >= max_pause_cycles:
                        print(
                            f"[{datetime.now().isoformat(timespec='seconds')}] "
                            f"season {season}: skipping after {pause_cycles} no-progress pauses"
                        )
                        break
                    print(
                        f"[{datetime.now().isoformat(timespec='seconds')}] "
                        f"season {season}: high stagnation, pause {pause_seconds}s "
                        f"(cycle {pause_cycles}/{max_pause_cycles})"
                    )
                    time.sleep(pause_seconds)
                    stagnant = 0
            else:
                stagnant = 0
                pause_cycles = 0

            time.sleep(2)

    print(f"[{datetime.now().isoformat(timespec='seconds')}] all requested seasons finished")


if __name__ == "__main__":
    main()
