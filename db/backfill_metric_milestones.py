"""Backfill sparse rank-crossing MetricMilestone rows.

Default scope:
  - career buckets: all_regular and all_playoffs
  - every metric that declares additive_accumulator=True

Usage:
  python -m db.backfill_metric_milestones
  python -m db.backfill_metric_milestones --bucket all_playoffs --replace
"""
from __future__ import annotations

import argparse
import logging
from collections import Counter

from sqlalchemy.orm import sessionmaker

from db.game_status import completed_game_clause
from db.models import Game, MetricMilestone, engine
from metrics.framework.base import career_season_type_code
from metrics.framework.milestones import (
    InMemoryBatchProvider,
    detect_milestones_for_game,
)


logger = logging.getLogger(__name__)
SessionLocal = sessionmaker(bind=engine)

DEFAULT_BUCKETS = ("all_playoffs", "all_regular")


def _game_ids_for_bucket(session, bucket: str, limit: int | None = None) -> list[str]:
    season_type_code = career_season_type_code(bucket)
    if not season_type_code:
        raise ValueError(f"Unsupported career bucket: {bucket!r}")
    query = (
        session.query(Game.game_id)
        .filter(
            Game.season.like(f"{season_type_code}%"),
            completed_game_clause(Game),
        )
        .order_by(Game.game_date.asc(), Game.game_id.asc())
    )
    if limit:
        query = query.limit(limit)
    return [str(row.game_id) for row in query.all()]


def backfill_bucket(
    bucket: str,
    *,
    replace: bool = False,
    dry_run: bool = False,
    commit_every: int = 250,
    limit: int | None = None,
) -> dict:
    provider = InMemoryBatchProvider(event_lookup_authoritative=False)
    counts: Counter[str] = Counter()

    with SessionLocal() as session:
        if replace:
            deleted = (
                session.query(MetricMilestone)
                .filter(
                    MetricMilestone.season == bucket,
                    MetricMilestone.event_type.in_(("rank_crossing", "approaching_target")),
                )
                .delete(synchronize_session=False)
            )
            counts["deleted"] = deleted
            if not dry_run:
                session.commit()
            provider.event_lookup_authoritative = True
        else:
            warmed = provider.warm_existing_events(session, season=bucket)
            provider.event_lookup_authoritative = True
            counts["warmed"] = warmed
            if warmed:
                logger.warning(
                    "backfill_metric_milestones bucket=%s warmed %d existing milestone event keys; use --replace to rebuild from scratch.",
                    bucket,
                    warmed,
                )

        game_ids = _game_ids_for_bucket(session, bucket, limit=limit)
        counts["games"] = len(game_ids)
        for idx, game_id in enumerate(game_ids, start=1):
            events = detect_milestones_for_game(
                session,
                game_id,
                prev_values_provider=provider,
                seasons=[bucket],
            )
            counts["events"] += len(events)
            if dry_run:
                session.rollback()
            elif commit_every and idx % commit_every == 0:
                session.commit()
                logger.info(
                    "backfill_metric_milestones bucket=%s progress=%d/%d events=%d",
                    bucket,
                    idx,
                    len(game_ids),
                    counts["events"],
                )
        if dry_run:
            session.rollback()
        else:
            session.commit()

    return {"bucket": bucket, **dict(counts)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill rank-crossing MetricMilestone rows.")
    parser.add_argument(
        "--bucket",
        action="append",
        choices=("all_playoffs", "all_regular"),
        help="Career bucket to backfill. Defaults to all_playoffs and all_regular.",
    )
    parser.add_argument("--replace", action="store_true", help="Delete existing rank-crossing milestones for the selected buckets first.")
    parser.add_argument("--dry-run", action="store_true", help="Run detection without committing changes.")
    parser.add_argument("--commit-every", type=int, default=250, help="Commit interval in games. Default: 250.")
    parser.add_argument("--limit", type=int, default=None, help="Optional game limit per bucket for smoke tests.")
    parser.add_argument("--verbose", action="store_true", help="Enable INFO logging.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING)
    buckets = tuple(args.bucket or DEFAULT_BUCKETS)
    summaries = [
        backfill_bucket(
            bucket,
            replace=args.replace,
            dry_run=args.dry_run,
            commit_every=args.commit_every,
            limit=args.limit,
        )
        for bucket in buckets
    ]
    for summary in summaries:
        print(summary)


if __name__ == "__main__":
    main()
