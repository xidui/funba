"""Daily metric job: find yesterday's (or a given date's) games and run metrics.

Usage:
    python -m metrics.framework.daily_job                  # yesterday
    python -m metrics.framework.daily_job --date 2026-03-04
    python -m metrics.framework.daily_job --date 2026-03-04 --no-score
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from sqlalchemy.orm import sessionmaker

from db.models import Game, engine
from metrics.framework.runner import run_for_game

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_job")


def run(target_date: date, do_score: bool = True) -> None:
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        games = (
            session.query(Game)
            .filter(Game.game_date == target_date)
            .all()
        )

    if not games:
        logger.info("No games found for %s.", target_date)
        return

    logger.info("Found %d game(s) on %s.", len(games), target_date)

    total_results = 0
    total_notable = 0

    for game in games:
        logger.info("Running metrics for game %s …", game.game_id)
        with SessionLocal() as session:
            results = run_for_game(session, game.game_id, do_score=do_score, commit=True)
        notable = [r for r in results if r.noteworthiness and r.noteworthiness >= 0.75]
        total_results += len(results)
        total_notable += len(notable)

        if notable:
            logger.info("  Notable results:")
            for r in notable:
                logger.info(
                    "    [%.2f] %s — %s: %s",
                    r.noteworthiness or 0,
                    r.metric_key,
                    r.entity_id,
                    r.notable_reason or "",
                )

    logger.info(
        "Daily job done: %d total metric results, %d notable (score threshold 0.75).",
        total_results, total_notable,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily metrics for a given date.")
    parser.add_argument(
        "--date",
        default=None,
        help="Date to process (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--no-score",
        action="store_true",
        help="Skip AI noteworthiness scoring (faster, no API calls).",
    )
    args = parser.parse_args()

    if args.date:
        try:
            target = date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date: {args.date!r}. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
    else:
        target = date.today() - timedelta(days=1)

    run(target, do_score=not args.no_score)


if __name__ == "__main__":
    main()
