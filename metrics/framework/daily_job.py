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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from threading import Lock

from sqlalchemy.orm import sessionmaker

from db.models import Game, engine
from metrics.framework.runner import already_processed, run_for_game

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("daily_job")

_DEFAULT_WORKERS = 4


def _process_one(game, total: int, do_score: bool, skip_existing: bool, counter: list, lock: Lock) -> tuple:
    """Process a single game. Returns (n_results, n_notable, skipped).

    Retries up to 3 times on MySQL deadlock (errno 1213).
    """
    import random
    import time
    from sqlalchemy.exc import OperationalError

    SessionLocal = sessionmaker(bind=engine)

    if skip_existing:
        with SessionLocal() as session:
            if already_processed(session, game.game_id):
                with lock:
                    counter[0] += 1
                    logger.info("[%d/%d] Skipping %s (already processed)", counter[0], total, game.game_id)
                return 0, 0, True

    with lock:
        counter[0] += 1
        idx = counter[0]
    logger.info("[%d/%d] Running metrics for game %s (%s) …", idx, total, game.game_id, game.game_date)

    for attempt in range(3):
        try:
            with SessionLocal() as session:
                results = run_for_game(session, game.game_id, do_score=do_score, commit=True)
            break
        except OperationalError as exc:
            if "1213" in str(exc) and attempt < 2:
                wait = 0.5 * (attempt + 1) + random.random()
                logger.warning("Deadlock on %s, retrying in %.1fs (attempt %d)…", game.game_id, wait, attempt + 1)
                time.sleep(wait)
            else:
                raise

    notable = [r for r in results if r.noteworthiness and r.noteworthiness >= 0.75]
    if notable:
        for r in notable:
            logger.info(
                "    [%.2f] %s — %s: %s",
                r.noteworthiness or 0, r.metric_key, r.entity_id, r.notable_reason or "",
            )
    return len(results), len(notable), False


def _run_games(games: list, do_score: bool, skip_existing: bool = False, workers: int = _DEFAULT_WORKERS) -> None:
    total = len(games)
    total_results = 0
    total_notable = 0
    skipped = 0
    counter = [0]  # mutable for use inside threads
    lock = Lock()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_process_one, game, total, do_score, skip_existing, counter, lock): game
            for game in games
        }
        for future in as_completed(futures):
            try:
                n_results, n_notable, was_skipped = future.result()
                if was_skipped:
                    skipped += 1
                else:
                    total_results += n_results
                    total_notable += n_notable
            except Exception as exc:
                game = futures[future]
                logger.error("Game %s failed: %s", game.game_id, exc, exc_info=True)

    processed = total - skipped
    logger.info(
        "Done: %d games processed, %d skipped, %d total metric results, %d notable.",
        processed, skipped, total_results, total_notable,
    )


def run_date(target_date: date, do_score: bool = True, skip_existing: bool = False, workers: int = _DEFAULT_WORKERS) -> None:
    SessionLocal = sessionmaker(bind=engine)
    with SessionLocal() as session:
        games = session.query(Game).filter(Game.game_date == target_date).all()

    if not games:
        logger.info("No games found for %s.", target_date)
        return

    logger.info("Found %d game(s) on %s.", len(games), target_date)
    _run_games(games, do_score, skip_existing=skip_existing, workers=workers)


def run_season(season_year: str, do_score: bool = True, skip_existing: bool = False, workers: int = _DEFAULT_WORKERS) -> None:
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
    _run_games(games, do_score, skip_existing=skip_existing, workers=workers)


def run_since(since_date: date, do_score: bool = True, skip_existing: bool = False, workers: int = _DEFAULT_WORKERS) -> None:
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
    _run_games(games, do_score, skip_existing=skip_existing, workers=workers)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run metrics for games.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", default=None, help="Single date (YYYY-MM-DD). Defaults to yesterday.")
    group.add_argument("--season", default=None, help="Season year prefix, e.g. 2025 for 2025-26 season.")
    group.add_argument("--since", default=None, help="Run all games from this date onward (YYYY-MM-DD).")
    parser.add_argument("--no-score", action="store_true", help="Skip per-game noteworthiness scoring.")
    parser.add_argument("--skip-existing", action="store_true", help="Skip games already in MetricRunLog.")
    parser.add_argument("--workers", type=int, default=_DEFAULT_WORKERS, help=f"Parallel workers (default: {_DEFAULT_WORKERS}).")
    parser.add_argument("--rerank", default=None, metavar="SEASON", help="Rerank all results for a season (e.g. 22025) then exit.")
    args = parser.parse_args()

    do_score = not args.no_score
    skip_existing = args.skip_existing
    workers = args.workers

    if args.rerank:
        from sqlalchemy.orm import sessionmaker as _SM
        from metrics.framework import scorer as _scorer
        SessionLocal = _SM(bind=engine)
        logger.info("Reranking all results for season %s …", args.rerank)
        with SessionLocal() as session:
            n = _scorer.rerank_all(session, args.rerank)
        logger.info("Done: %d rows updated.", n)
        return

    if args.season:
        run_season(args.season, do_score=do_score, skip_existing=skip_existing, workers=workers)
    elif args.since:
        try:
            since = date.fromisoformat(args.since)
        except ValueError:
            print(f"Invalid date: {args.since!r}. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
        run_since(since, do_score=do_score, skip_existing=skip_existing, workers=workers)
    else:
        if args.date:
            try:
                target = date.fromisoformat(args.date)
            except ValueError:
                print(f"Invalid date: {args.date!r}. Use YYYY-MM-DD.", file=sys.stderr)
                sys.exit(1)
        else:
            target = date.today() - timedelta(days=1)
        run_date(target, do_score=do_score, skip_existing=skip_existing, workers=workers)


if __name__ == "__main__":
    main()
