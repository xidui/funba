"""Incremental background job for filling missing player birth dates.

Designed for launchd-style periodic execution:
- processes a small batch each run
- remembers players that had no Wikidata match
- stops early when the source rate limits us
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from requests.exceptions import HTTPError
from sqlalchemy import distinct
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_player_info import (
    fetch_player_info_from_wikidata,
    update_player_info,
)
from db.models import Player, PlayerGameStats, engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

Session = sessionmaker(bind=engine)


def _default_state() -> dict[str, object]:
    return {
        "no_match_ids": [],
        "runs": 0,
        "updated_total": 0,
        "skipped_total": 0,
        "errors_total": 0,
        "rate_limited_total": 0,
    }


def _load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return _default_state()
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return _default_state()
    state = _default_state()
    if isinstance(payload, dict):
        state.update(payload)
    if not isinstance(state.get("no_match_ids"), list):
        state["no_match_ids"] = []
    return state


def _save_state(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True))
    tmp_path.replace(path)


def _candidate_players(session, *, limit: int, no_match_ids: set[str], with_games_only: bool, inactive_only: bool) -> list[Player]:
    query = session.query(Player).filter(
        Player.is_team == False,
        Player.birth_date.is_(None),
        Player.full_name.isnot(None),
    )
    if inactive_only:
        query = query.filter(Player.is_active == False)
    if no_match_ids:
        query = query.filter(~Player.player_id.in_(sorted(no_match_ids)))
    if with_games_only:
        player_ids_subq = session.query(distinct(PlayerGameStats.player_id).label("player_id")).subquery()
        query = query.join(player_ids_subq, player_ids_subq.c.player_id == Player.player_id)
    return query.order_by(Player.full_name.asc()).limit(limit).all()


def run_batch(
    *,
    state_path: Path,
    limit: int,
    with_games_only: bool = False,
    inactive_only: bool = True,
) -> dict[str, int]:
    state = _load_state(state_path)
    no_match_ids = {str(value) for value in state.get("no_match_ids", []) if str(value).strip()}

    updated = skipped = errors = rate_limited = 0
    processed = 0

    with Session() as session:
        players = _candidate_players(
            session,
            limit=limit,
            no_match_ids=no_match_ids,
            with_games_only=with_games_only,
            inactive_only=inactive_only,
        )
        logger.info(
            "player bio background batch: candidates=%s no_match_cache=%s limit=%s",
            len(players),
            len(no_match_ids),
            limit,
        )

        for player in players:
            processed += 1
            try:
                info = fetch_player_info_from_wikidata(player.full_name or "")
                if info and update_player_info(session, player, info):
                    session.commit()
                    updated += 1
                    logger.info("updated %s (%s)", player.full_name, player.player_id)
                else:
                    no_match_ids.add(str(player.player_id))
                    skipped += 1
            except HTTPError as exc:
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                session.rollback()
                if status_code in {403, 429}:
                    rate_limited += 1
                    logger.warning("rate limited while processing %s (%s): %s", player.full_name, player.player_id, status_code)
                    break
                errors += 1
                logger.warning("HTTP error while processing %s (%s): %s", player.full_name, player.player_id, exc)
            except Exception as exc:
                session.rollback()
                errors += 1
                logger.warning("error while processing %s (%s): %s", player.full_name, player.player_id, exc)

    state["no_match_ids"] = sorted(no_match_ids)
    state["runs"] = int(state.get("runs", 0)) + 1
    state["updated_total"] = int(state.get("updated_total", 0)) + updated
    state["skipped_total"] = int(state.get("skipped_total", 0)) + skipped
    state["errors_total"] = int(state.get("errors_total", 0)) + errors
    state["rate_limited_total"] = int(state.get("rate_limited_total", 0)) + rate_limited
    _save_state(state_path, state)

    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "rate_limited": rate_limited,
        "cached_no_match": len(no_match_ids),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental player birth-date background job")
    parser.add_argument("--state-path", type=Path, required=True, help="JSON state file path")
    parser.add_argument("--limit", type=int, default=30, help="Max players to process this run")
    parser.add_argument("--with-games-only", action="store_true", help="Only consider players present in PlayerGameStats")
    parser.add_argument("--include-active", action="store_true", help="Include active players as candidates")
    args = parser.parse_args()

    result = run_batch(
        state_path=args.state_path,
        limit=max(1, args.limit),
        with_games_only=args.with_games_only,
        inactive_only=not args.include_active,
    )
    logger.info("background batch result: %s", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
