"""Backfill PlayerGamePeriodStats for historical games.

Usage:
    .venv/bin/python -m db.backfill_period_stats
    .venv/bin/python -m db.backfill_period_stats --season 22025
    .venv/bin/python -m db.backfill_period_stats --season 22025 --limit 100
    .venv/bin/python -m db.backfill_period_stats --season 22025 --force
    .venv/bin/python -m db.backfill_period_stats --season 22025 --min-periods 5 --force
"""
from __future__ import annotations

import argparse
import concurrent.futures
import logging
import sys
import time

from sqlalchemy import func
from sqlalchemy.exc import OperationalError

from db.backfill_nba_game_detail import (
    create_player_period_stats,
    fetch_all_period_stats,
)
from db.models import Game, PlayerGamePeriodStats, engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def _backfill_one(session_factory, game_id: str, slug: str):
    """Backfill one game and return status metadata."""
    for attempt in range(1, 4):
        try:
            periods = fetch_all_period_stats(game_id)
            if not periods:
                return {
                    "slug": slug,
                    "game_id": game_id,
                    "status": "skipped",
                    "period_count": 0,
                }

            with session_factory() as session:
                session.query(PlayerGamePeriodStats).filter(
                    PlayerGamePeriodStats.game_id == game_id,
                ).delete(synchronize_session=False)

                for period, rows in periods.items():
                    for ps in rows:
                        create_player_period_stats(session, game_id, period, ps)

                session.commit()

            return {
                "slug": slug,
                "game_id": game_id,
                "status": "done",
                "period_count": len(periods),
            }
        except OperationalError as exc:
            if (
                getattr(exc, "orig", None) is not None
                and len(getattr(exc.orig, "args", ())) > 0
                and exc.orig.args[0] == 1213
                and attempt < 3
            ):
                logger.warning(
                    "deadlock inserting %s, retrying (%d/3)",
                    game_id,
                    attempt,
                )
                time.sleep(0.5 * attempt)
                continue
            return {
                "slug": slug,
                "game_id": game_id,
                "status": "failed",
                "error": exc,
            }
        except Exception as exc:
            if attempt < 3:
                logger.warning(
                    "period backfill failed for %s, retrying (%d/3): %s",
                    game_id,
                    attempt,
                    exc,
                )
                time.sleep(0.5 * attempt)
                continue
            return {
                "slug": slug,
                "game_id": game_id,
                "status": "failed",
                "error": exc,
            }

    return {
        "slug": slug,
        "game_id": game_id,
        "status": "failed",
        "error": "exhausted retries",
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill per-period player stats")
    parser.add_argument("--season", help="Limit to a single season, e.g. 22025")
    parser.add_argument("--limit", type=int, default=0, help="Max games to process (0 = all)")
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Recompute period stats even for games already having "
            "at least 4 period rows. Useful to recover partial/outdated data."
        ),
    )
    parser.add_argument(
        "--min-periods",
        type=int,
        default=4,
        help="Treat a game as complete only if it has at least this many distinct periods.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent backfill workers (default 1).",
    )
    args = parser.parse_args()

    from sqlalchemy.orm import sessionmaker

    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        # All completed game_ids (have box score data)
        games_q = (
            session.query(Game.game_id, Game.slug)
            .filter(
                Game.game_date.isnot(None),
                Game.home_team_score.isnot(None),
            )
        )
        if args.season:
            games_q = games_q.filter(Game.season == args.season)
        games_q = games_q.order_by(Game.season.desc(), Game.game_date.desc())
        all_games = games_q.all()

        # Already done: games that already have enough period rows for all players.
        # Keep default at 4 (Q1-Q4) for historical backfills.
        # Set --force to rebuild all selected games.
        done_ids: set[str] = set()

        if not args.force:
            done_q = (
                session.query(PlayerGamePeriodStats.game_id)
                .group_by(PlayerGamePeriodStats.game_id)
                .having(func.count(func.distinct(PlayerGamePeriodStats.period)) >= args.min_periods)
            )
            if args.season:
                done_q = done_q.join(Game, Game.game_id == PlayerGamePeriodStats.game_id).filter(Game.season == args.season)
            done_ids = {row[0] for row in done_q.all()}

            logger.info(
                "force disabled: skipping games with >=%d distinct periods (default Q1-Q4).",
                args.min_periods,
            )
        else:
            logger.info("force enabled: will recompute period rows for all selected games.")

    remaining = [(gid, slug) for gid, slug in all_games if gid not in done_ids]
    if args.limit:
        remaining = remaining[: args.limit]

    total = len(remaining)
    logger.info("Games to backfill: %d (already done: %d)", total, len(done_ids))
    if not total:
        logger.info("Nothing to do.")
        return

    done = 0
    skipped = 0
    failed = 0

    max_workers = max(1, args.workers)
    if max_workers == 1:
        jobs = [(game_id, slug) for game_id, slug in remaining]
        worker_iter = ((i, _backfill_one(SessionLocal, game_id, slug)) for i, (game_id, slug) in enumerate(jobs, 1))
    else:
        logger.info("running with %d workers", max_workers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_game = {
                pool.submit(_backfill_one, SessionLocal, game_id, slug): (i, game_id, slug)
                for i, (game_id, slug) in enumerate(remaining, 1)
            }
            for future in concurrent.futures.as_completed(future_to_game):
                i, game_id, slug = future_to_game[future]
                result = future.result()
                status = result["status"]
                if status == "done":
                    done += 1
                    if i % 50 == 0 or i == total:
                        logger.info(
                            "[%d/%d] %s done (%d periods)",
                            i,
                            total,
                            result["slug"] or result["game_id"],
                            result["period_count"],
                        )
                elif status == "skipped":
                    skipped += 1
                    if i % 50 == 0 or i == total:
                        logger.info("[%d/%d] %s skipped (no period data)", i, total, result["slug"] or result["game_id"])
                else:
                    failed += 1
                    logger.error(
                        "[%d/%d] %s FAILED: %s",
                        i,
                        total,
                        result["slug"] or result["game_id"],
                        result.get("error"),
                    )
            # exit threadpool and continue
            worker_iter = None

    if worker_iter:
        try:
            for i, result in worker_iter:
                if result["status"] == "done":
                    done += 1
                    if i % 50 == 0 or i == total:
                        logger.info(
                            "[%d/%d] %s done (%d periods)",
                            i,
                            total,
                            result["slug"] or result["game_id"],
                            result["period_count"],
                        )
                elif result["status"] == "skipped":
                    skipped += 1
                    if i % 50 == 0 or i == total:
                        logger.info("[%d/%d] %s skipped (no period data)", i, total, result["slug"] or result["game_id"])
                else:
                    failed += 1
                    logger.error(
                        "[%d/%d] %s FAILED: %s",
                        i,
                        total,
                        result["slug"] or result["game_id"],
                        result.get("error"),
                    )
        except KeyboardInterrupt:
            logger.info(
                "Interrupted at %d/%d. Done: %d, Skipped: %d, Failed: %d",
                done + skipped + failed,
                total,
                done,
                skipped,
                failed,
            )
            sys.exit(1)

    logger.info("Finished. Done: %d, Skipped: %d, Failed: %d, Total: %d", done, skipped, failed, total)


if __name__ == "__main__":
    main()
