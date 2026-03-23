"""Backfill Player draft data from nba_api DraftHistory endpoint.

Usage:
    python -m db.backfill_nba_draft --year 2009
    python -m db.backfill_nba_draft --all
"""
from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass

from requests.exceptions import ConnectionError, Timeout
from sqlalchemy.orm import sessionmaker
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from db.models import Player, engine

try:
    from nba_api.stats.endpoints import drafthistory
except ImportError:  # pragma: no cover - exercised only in misconfigured runtimes
    drafthistory = None


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)

DRAFT_START_YEAR = 1947
DRAFT_END_YEAR = 2024
RATE_LIMIT_SECONDS = 0.6


@dataclass
class BackfillCounts:
    updated: int = 0
    created: int = 0
    skipped: int = 0
    errors: int = 0

    def add(self, other: "BackfillCounts") -> None:
        self.updated += other.updated
        self.created += other.created
        self.skipped += other.skipped
        self.errors += other.errors


@retry(
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def fetch_draft_history(year: int) -> list[dict]:
    """Fetch all draft picks for a single year."""
    if drafthistory is None:
        raise RuntimeError("nba_api DraftHistory endpoint is unavailable")

    history = drafthistory.DraftHistory(season_year_nullable=year)
    payload = history.draft_history.get_dict()
    headers = payload.get("headers", [])
    rows = payload.get("data", [])
    return [dict(zip(headers, row)) for row in rows]


def _int_or_none(value) -> int | None:
    if value in (None, "", "Undrafted"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _target_years(year: int | None = None, refresh_all: bool = False) -> list[int]:
    if year is not None:
        return [year]
    if refresh_all:
        return list(range(DRAFT_START_YEAR, DRAFT_END_YEAR + 1))
    raise ValueError("Either a single year or --all is required")


def _apply_draft_fields(player: Player, row: dict) -> bool:
    changed = False

    values = {
        "full_name": str(row.get("PLAYER_NAME") or "").strip() or None,
        "draft_year": _int_or_none(row.get("SEASON")),
        "draft_round": _int_or_none(row.get("ROUND_NUMBER")),
        "draft_number": _int_or_none(row.get("ROUND_PICK")),
    }

    for field, value in values.items():
        if value is None:
            continue
        if getattr(player, field) != value:
            setattr(player, field, value)
            changed = True

    return changed


def upsert_draft_row(session, row: dict) -> str:
    """Create or update a Player row from a draft history row."""
    player_id = str(row.get("PERSON_ID") or "").strip()
    full_name = str(row.get("PLAYER_NAME") or "").strip() or None
    draft_year = _int_or_none(row.get("SEASON"))
    draft_round = _int_or_none(row.get("ROUND_NUMBER"))
    draft_number = _int_or_none(row.get("ROUND_PICK"))

    if not player_id or draft_year is None or draft_round is None or draft_number is None:
        return "skipped"

    player = session.query(Player).filter(Player.player_id == player_id).first()
    if player is None:
        session.add(
            Player(
                player_id=player_id,
                full_name=full_name,
                is_active=False,
                is_team=False,
                draft_year=draft_year,
                draft_round=draft_round,
                draft_number=draft_number,
            )
        )
        return "created"

    if _apply_draft_fields(player, row):
        session.add(player)
        return "updated"

    return "skipped"


def backfill_year(session, year: int) -> BackfillCounts:
    """Backfill one draft class into the Player table."""
    counts = BackfillCounts()
    rows = fetch_draft_history(year)

    for row in rows:
        outcome = upsert_draft_row(session, row)
        if outcome == "created":
            counts.created += 1
        elif outcome == "updated":
            counts.updated += 1
        else:
            counts.skipped += 1

    session.commit()
    logger.info(
        "Year %s complete. Updated: %s, Created: %s, Skipped: %s",
        year,
        counts.updated,
        counts.created,
        counts.skipped,
    )
    return counts


def run(year: int | None = None, refresh_all: bool = False) -> BackfillCounts:
    years = _target_years(year=year, refresh_all=refresh_all)
    total = BackfillCounts()
    session = Session()

    try:
        total_years = len(years)
        logger.info("Processing %s draft year(s)", total_years)

        for index, draft_year in enumerate(years, 1):
            try:
                year_counts = backfill_year(session, draft_year)
                total.add(year_counts)
            except RetryError:
                logger.error("[%s/%s] Failed after retries for draft year %s", index, total_years, draft_year)
                session.rollback()
                total.errors += 1
            except Exception as exc:
                logger.error("[%s/%s] Error for draft year %s: %s", index, total_years, draft_year, exc)
                session.rollback()
                total.errors += 1

            if index < total_years:
                time.sleep(RATE_LIMIT_SECONDS)

        logger.info(
            "Done. Updated: %s, Created: %s, Skipped: %s, Errors: %s",
            total.updated,
            total.created,
            total.skipped,
            total.errors,
        )
        return total
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill NBA draft history into Player rows")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--year", type=int, help="Draft year to backfill")
    group.add_argument("--all", action="store_true", help="Backfill all draft years from 1947-2024")
    args = parser.parse_args()
    run(year=args.year, refresh_all=args.all)
