"""Smoke test: run the LLM highlight curator against one real game end-to-end.

Usage: .venv/bin/python -m scripts.smoke_curate_game_highlights 20260419-por-sas
"""
from __future__ import annotations

import json
import sys

from sqlalchemy.orm import Session

from db.models import Game, Team, engine
from metrics.highlights.curator import build_game_context, curate_game_highlights
from metrics.highlights.prefilter import prefilter_candidates


def _resolve_game(session: Session, identifier: str) -> Game:
    g = session.query(Game).filter_by(slug=identifier).one_or_none()
    if g is None:
        g = session.query(Game).filter_by(game_id=identifier).one_or_none()
    if g is None:
        raise SystemExit(f"game not found: {identifier}")
    return g


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: smoke_curate_game_highlights.py <slug-or-id>", file=sys.stderr)
        sys.exit(2)
    identifier = sys.argv[1]

    with Session(engine) as session:
        game = _resolve_game(session, identifier)

        from web.app import _build_game_raw_metric_candidates

        raw = _build_game_raw_metric_candidates(session, game.game_id, game.season)
        print(f"[raw candidates] {len(raw)} entries")

        candidates = prefilter_candidates(raw)
        print(f"[prefilter]      {len(candidates)} kept")
        for c in candidates:
            print(f"  - {c['metric_key']:40} entity={c.get('entity_id')} value={c.get('value_str') or c.get('value_num')} rank={c.get('rank')}/{c.get('total')}")

        team_lookup = {
            t.team_id: t.full_name for t in session.query(Team).all()
        }
        ctx = build_game_context(game, team_lookup)

        result = curate_game_highlights(game_context=ctx, candidates=candidates)
        print("\n[curated]")
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
