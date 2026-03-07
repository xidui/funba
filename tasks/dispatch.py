"""CLI entrypoint for enqueueing Celery tasks.

Usage examples:
  # Backfill all games in a season
  python -m tasks.dispatch backfill --season 22025

  # Backfill a date range
  python -m tasks.dispatch backfill --date-from 2025-01-01 --date-to 2025-03-06

  # Single game
  python -m tasks.dispatch game 0022400909

  # Backfill one metric across all games (routes through ingest to ensure artifacts exist)
  python -m tasks.dispatch metric-backfill --metric clutch_fg_pct

  # Backfill all metrics across all games
  python -m tasks.dispatch metric-backfill

  # Force recompute (clears existing claims so workers rerun even for 'done' games)
  python -m tasks.dispatch metric-backfill --metric clutch_fg_pct --force
  python -m tasks.dispatch backfill --season 22025 --force
"""
from __future__ import annotations

import argparse
import sys

from sqlalchemy.orm import sessionmaker

from db.models import Game, MetricJobClaim, engine
from tasks.celery_app import app  # noqa: F401 — ensures tasks are registered

# Import so Celery knows about them before we call apply_async
from tasks.ingest import ingest_game  # noqa: F401


def _session():
    return sessionmaker(bind=engine)()


def _query_games(
    season: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[str]:
    sess = _session()
    try:
        q = sess.query(Game.game_id).filter(Game.game_date.isnot(None))
        if season:
            q = q.filter(Game.season.like(f"{season}%"))
        if date_from:
            q = q.filter(Game.game_date >= date_from)
        if date_to:
            q = q.filter(Game.game_date <= date_to)
        return [row.game_id for row in q.all()]
    finally:
        sess.close()


def _all_game_ids() -> list[str]:
    sess = _session()
    try:
        return [row.game_id for row in sess.query(Game.game_id).filter(Game.game_date.isnot(None)).all()]
    finally:
        sess.close()


def _clear_claims(game_ids: list[str], metric_keys: list[str] | None = None) -> int:
    """Delete MetricJobClaim rows so workers can reprocess.

    If metric_keys is None, clears ALL claims for the given games.
    Returns count of deleted rows.
    """
    sess = _session()
    try:
        q = sess.query(MetricJobClaim).filter(MetricJobClaim.game_id.in_(game_ids))
        if metric_keys is not None:
            q = q.filter(MetricJobClaim.metric_key.in_(metric_keys))
        count = q.delete(synchronize_session=False)
        sess.commit()
        return count
    finally:
        sess.close()


def cmd_game(args: argparse.Namespace) -> None:
    game_id = args.game_id
    if args.force:
        deleted = _clear_claims([game_id])
        print(f"--force: cleared {deleted} claim(s) for game {game_id}.")
    ingest_game.apply_async(args=[game_id], kwargs={"force": args.force}, queue="ingest")
    print(f"Enqueued 1 ingest task for game {game_id}.")


def cmd_backfill(args: argparse.Namespace) -> None:
    game_ids = _query_games(
        season=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    if not game_ids:
        print("No games found for the given filters.")
        return
    if args.force:
        deleted = _clear_claims(game_ids)
        print(f"--force: cleared {deleted} claim(s) for {len(game_ids)} games.")
    for gid in game_ids:
        ingest_game.apply_async(args=[gid], kwargs={"force": args.force}, queue="ingest")
    print(f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest.")


def cmd_metric_backfill(args: argparse.Namespace) -> None:
    from metrics.framework import registry

    if args.metric:
        all_keys = [m.key for m in registry.get_all()]
        if args.metric not in all_keys:
            print(f"Unknown metric key: {args.metric!r}. Known keys:", file=sys.stderr)
            for k in sorted(all_keys):
                print(f"  {k}", file=sys.stderr)
            sys.exit(1)
        metric_keys = [args.metric]
    else:
        metric_keys = [m.key for m in registry.get_all()]

    game_ids = _query_games(
        season=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
    ) if (args.season or args.date_from or args.date_to) else _all_game_ids()

    if not game_ids:
        print("No games found.")
        return

    if args.force:
        deleted = _clear_claims(game_ids, metric_keys)
        print(f"--force: cleared {deleted} claim(s).")

    # Route through the ingest queue so artifact presence (PBP, shot detail)
    # is verified (and fetched if missing) before metric computation runs.
    for gid in game_ids:
        ingest_game.apply_async(
            args=[gid],
            kwargs={"metric_keys": metric_keys, "force": args.force},
            queue="ingest",
        )

    print(
        f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest "
        f"(each will fan out {len(metric_keys)} metric(s) after artifact check)."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tasks.dispatch",
        description="Enqueue Celery tasks for game ingestion and metric computation.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- game <game_id> ---
    p_game = sub.add_parser("game", help="Enqueue ingest for a single game.")
    p_game.add_argument("game_id", help="NBA game ID, e.g. 0022400909")
    p_game.add_argument("--force", action="store_true",
                        help="Clear existing claims so workers reprocess even completed games.")
    p_game.set_defaults(func=cmd_game)

    # --- backfill ---
    p_bf = sub.add_parser("backfill", help="Enqueue ingest tasks for multiple games.")
    p_bf.add_argument("--season", help="Season prefix, e.g. 22025")
    p_bf.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_bf.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_bf.add_argument("--force", action="store_true",
                      help="Clear existing claims so workers reprocess even completed games.")
    p_bf.set_defaults(func=cmd_backfill)

    # --- metric-backfill ---
    p_mb = sub.add_parser(
        "metric-backfill",
        help="Enqueue metric compute tasks (verifies artifacts via ingest queue first).",
    )
    p_mb.add_argument("--metric", default=None, help="Single metric key, or omit for all.")
    p_mb.add_argument("--season", help="Limit to a season prefix, e.g. 22025")
    p_mb.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_mb.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_mb.add_argument("--force", action="store_true",
                      help="Clear existing claims so workers recompute even completed games.")
    p_mb.set_defaults(func=cmd_metric_backfill)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
