"""Backfill / recompute curated game highlights via the LLM curator.

Usage:
    .venv/bin/python -m scripts.recompute_game_highlights --game-id 0042500151
    .venv/bin/python -m scripts.recompute_game_highlights --slug 20260419-por-sas
    .venv/bin/python -m scripts.recompute_game_highlights --season 22025 --limit 10
    .venv/bin/python -m scripts.recompute_game_highlights --season 22025 --force

Without --force, games that already have highlights_curated_json are skipped.
"""
from __future__ import annotations

import argparse
import logging
import sys

from sqlalchemy.orm import Session

from db.models import Game, engine
from metrics.highlights.curator import run_curator_for_game

logger = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--game-id")
    group.add_argument("--slug")
    group.add_argument("--season", help="e.g. 22025 for 2025-26 Regular Season")
    p.add_argument("--limit", type=int, default=None, help="Cap how many games to process (season mode)")
    p.add_argument("--force", action="store_true", help="Re-curate even if already cached")
    p.add_argument("--model", default=None, help="Override LLM model")
    return p.parse_args()


def _iter_games(session: Session, args) -> list[Game]:
    if args.game_id:
        g = session.query(Game).filter_by(game_id=args.game_id).one_or_none()
        return [g] if g else []
    if args.slug:
        g = session.query(Game).filter_by(slug=args.slug).one_or_none()
        return [g] if g else []
    q = session.query(Game).filter(Game.season == args.season)
    if not args.force:
        q = q.filter(Game.highlights_curated_json.is_(None))
    q = q.order_by(Game.game_date.desc())
    if args.limit:
        q = q.limit(args.limit)
    return q.all()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args()
    with Session(engine) as session:
        games = _iter_games(session, args)
        if not games:
            print("no games matched", file=sys.stderr)
            sys.exit(1)
        for i, game in enumerate(games, 1):
            if game.highlights_curated_json and not args.force:
                print(f"[{i}/{len(games)}] skip {game.game_id} (already curated)")
                continue
            try:
                result = run_curator_for_game(session, game, model=args.model)
                counts = {
                    scope: (
                        len(result.get(scope, {}).get("hero") or []),
                        len(result.get(scope, {}).get("notable") or []),
                    )
                    for scope in ("game", "player", "team")
                }
                summary = "  ".join(f"{s}={h}+{n}" for s, (h, n) in counts.items())
                print(f"[{i}/{len(games)}] {game.game_id}  {summary}")
            except Exception as exc:
                logger.exception("curator failed for %s", game.game_id)
                print(f"[{i}/{len(games)}] {game.game_id}  FAILED: {exc}")


if __name__ == "__main__":
    main()
