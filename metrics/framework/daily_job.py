"""Daily metric job: find yesterday's (or a given date's) games and run metrics.

Usage:
    python -m metrics.framework.daily_job                        # yesterday
    python -m metrics.framework.daily_job --date 2026-03-04
    python -m metrics.framework.daily_job --date 2026-03-04 --no-score
    python -m metrics.framework.daily_job --season 2025          # all games in a season year
    python -m metrics.framework.daily_job --since 2026-01-01     # all games from a date onward
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from sqlalchemy.orm import sessionmaker

from db.models import Game, engine
from metrics.framework.runner import already_processed, run_for_game

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_job")


def _run_games(games: list, do_score: bool, skip_existing: bool = False) -> None:
    SessionLocal = sessionmaker(bind=engine)
    total_results = 0
    total_notable = 0
    skipped = 0

    for i, game in enumerate(games, 1):
        if skip_existing:
            with SessionLocal() as session:
                if already_processed(session, game.game_id):
                    skipped += 1
                    continue

        logger.info("[%d/%d] Running metrics for game %s (%s) …", i, len(games), game.game_id, game.game_date)
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

    processed = len(games) - skipped
    logger.info(
        "Done: %d games processed, %d skipped, %d total metric results, %d notable.",
        processed, skipped, total_results, total_notable,
    )


def run_date(target_date: date, do_score: bool = True, skip_existing: bool = False) -> None:
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        games = session.query(Game).filter(Game.game_date == target_date).all()

    if not games:
        logger.info("No games found for %s.", target_date)
        return

    logger.info("Found %d game(s) on %s.", len(games), target_date)
    _run_games(games, do_score, skip_existing=skip_existing)


def run_season(season_year: str, do_score: bool = True, skip_existing: bool = False) -> None:
    """Run metrics for all games whose season starts with season_year (e.g. '22025')."""
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        games = (
            session.query(Game)
            .filter(Game.season.like(f"{season_year}%"), Game.game_date.isnot(None))
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .all()
        )

    if not games:
        logger.info("No games found for season year %s.", season_year)
        return

    logger.info("Found %d games for season year %s.", len(games), season_year)
    _run_games(games, do_score, skip_existing=skip_existing)


def run_since(since_date: date, do_score: bool = True, skip_existing: bool = False) -> None:
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        games = (
            session.query(Game)
            .filter(Game.game_date >= since_date, Game.game_date.isnot(None))
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .all()
        )

    if not games:
        logger.info("No games found since %s.", since_date)
        return

    logger.info("Found %d games since %s.", len(games), since_date)
    _run_games(games, do_score, skip_existing=skip_existing)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run metrics for games.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", default=None, help="Single date (YYYY-MM-DD). Defaults to yesterday.")
    group.add_argument("--season", default=None, help="Season year prefix, e.g. 2025 for 2025-26 season.")
    group.add_argument("--since", default=None, help="Run all games from this date onward (YYYY-MM-DD).")
    parser.add_argument("--no-score", action="store_true", help="Skip AI noteworthiness scoring.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip games already in MetricRunLog.")
    args = parser.parse_args()

    do_score = not args.no_score
    skip_existing = args.skip_existing

    if args.season:
        run_season(args.season, do_score=do_score, skip_existing=skip_existing)
    elif args.since:
        try:
            since = date.fromisoformat(args.since)
        except ValueError:
            print(f"Invalid date: {args.since!r}. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
        run_since(since, do_score=do_score, skip_existing=skip_existing)
    else:
        if args.date:
            try:
                target = date.fromisoformat(args.date)
            except ValueError:
                print(f"Invalid date: {args.date!r}. Use YYYY-MM-DD.", file=sys.stderr)
                sys.exit(1)
        else:
            target = date.today() - timedelta(days=1)
        run_date(target, do_score=do_score, skip_existing=skip_existing)


if __name__ == "__main__":
    main()
