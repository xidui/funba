"""Backfill PlayerGamePeriodStats for historical games.

Usage:
    .venv/bin/python -m db.backfill_period_stats
    .venv/bin/python -m db.backfill_period_stats --season 22025
    .venv/bin/python -m db.backfill_period_stats --season 22025 --limit 100
"""
from __future__ import annotations

import argparse
import logging
import sys
import time

from sqlalchemy import func, text

from db.backfill_nba_game_detail import (
    create_player_period_stats,
    fetch_all_period_stats,
)
from db.models import Game, PlayerGamePeriodStats, engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Backfill per-period player stats")
    parser.add_argument("--season", help="Limit to a single season, e.g. 22025")
    parser.add_argument("--limit", type=int, default=0, help="Max games to process (0 = all)")
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

        # Already done: games that have period stats for at least 4 periods (Q1-Q4).
        from sqlalchemy import func as sqla_func
        done_q = (
            session.query(PlayerGamePeriodStats.game_id)
            .group_by(PlayerGamePeriodStats.game_id)
            .having(sqla_func.count(sqla_func.distinct(PlayerGamePeriodStats.period)) >= 4)
        )
        if args.season:
            done_q = done_q.join(Game, Game.game_id == PlayerGamePeriodStats.game_id).filter(Game.season == args.season)
        done_ids = {row[0] for row in done_q.all()}

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

    for i, (game_id, slug) in enumerate(remaining, 1):
        try:
            periods = fetch_all_period_stats(game_id)
            if not periods:
                skipped += 1
                if i % 50 == 0 or i == total:
                    logger.info("[%d/%d] %s skipped (no period data)", i, total, slug or game_id)
                continue

            with SessionLocal() as session:
                # Delete any partial data from previous attempts, then insert fresh.
                session.query(PlayerGamePeriodStats).filter(
                    PlayerGamePeriodStats.game_id == game_id,
                ).delete(synchronize_session=False)
                for period, rows in periods.items():
                    for ps in rows:
                        create_player_period_stats(session, game_id, period, ps)
                session.commit()

            done += 1
            if i % 50 == 0 or i == total:
                logger.info("[%d/%d] %s done (%d periods)", i, total, slug or game_id, len(periods))

        except KeyboardInterrupt:
            logger.info("Interrupted at %d/%d. Done: %d, Skipped: %d, Failed: %d", i, total, done, skipped, failed)
            sys.exit(1)
        except Exception:
            failed += 1
            logger.exception("[%d/%d] %s FAILED", i, total, slug or game_id)

    logger.info("Finished. Done: %d, Skipped: %d, Failed: %d, Total: %d", done, skipped, failed, total)


if __name__ == "__main__":
    main()
