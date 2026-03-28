"""Backfill Player bio/demographic data from nba_api commonplayerinfo endpoint.

Usage:
    python -m db.backfill_nba_player_info              # only players missing bio
    python -m db.backfill_nba_player_info --all         # refresh all players
    python -m db.backfill_nba_player_info --player 201939  # single player
"""
import argparse
import logging
import time
from datetime import datetime

from nba_api.stats.endpoints import commonplayerinfo
from requests.exceptions import ConnectionError, Timeout
from sqlalchemy import or_
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


@retry(
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def fetch_player_info(player_id: str) -> dict:
    """Fetch commonplayerinfo for a single player."""
    info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
    rows = info.common_player_info.get_dict()
    headers = rows["headers"]
    data = rows["data"]
    if not data:
        return {}
    row = dict(zip(headers, data[0]))
    return row


def _parse_date(val):
    """Parse ISO date string from API (e.g. '1984-12-30T00:00:00')."""
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _int_or_none(val):
    if val is None or val == "" or val == "Undrafted":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _bool_or_none(val):
    if val is None or val == "":
        return None
    if isinstance(val, bool):
        return val

    normalized = str(val).strip().lower()
    if normalized in {"1", "true", "active", "yes", "y"}:
        return True
    if normalized in {"0", "false", "inactive", "no", "n"}:
        return False
    return None


def update_player_info(session, player: Player, info: dict) -> bool:
    """Apply bio fields from API response to a Player row. Returns True if changed."""
    changed = False

    field_map = {
        "height": ("HEIGHT", str),
        "weight": ("WEIGHT", _int_or_none),
        "birth_date": ("BIRTHDATE", _parse_date),
        "country": ("COUNTRY", str),
        "school": ("SCHOOL", str),
        "draft_year": ("DRAFT_YEAR", _int_or_none),
        "draft_round": ("DRAFT_ROUND", _int_or_none),
        "draft_number": ("DRAFT_NUMBER", _int_or_none),
        "jersey": ("JERSEY", str),
        "position": ("POSITION", str),
        "from_year": ("FROM_YEAR", _int_or_none),
        "to_year": ("TO_YEAR", _int_or_none),
        "season_exp": ("SEASON_EXP", _int_or_none),
        "greatest_75_flag": ("GREATEST_75_FLAG", _bool_or_none),
        "is_active": ("ROSTERSTATUS", _bool_or_none),
    }

    for col, (api_key, converter) in field_map.items():
        raw = info.get(api_key)
        if raw is None or raw == "":
            continue
        val = converter(raw)
        if val is not None and val != "" and getattr(player, col) != val:
            setattr(player, col, val)
            changed = True

    return changed


def run(player_id: str | None = None, refresh_all: bool = False):
    session = Session()
    try:
        if player_id:
            players = session.query(Player).filter(Player.player_id == player_id).all()
        elif refresh_all:
            players = session.query(Player).filter(Player.is_team == False).all()
        else:
            # Only players missing bio data
            players = (
                session.query(Player)
                .filter(
                    Player.is_team == False,
                    or_(
                        Player.height.is_(None),
                        Player.position.is_(None),
                        Player.season_exp.is_(None),
                    ),
                )
                .all()
            )

        total = len(players)
        logger.info(f"Found {total} players to process")
        updated = 0
        skipped = 0
        errors = 0

        for i, player in enumerate(players, 1):
            try:
                info = fetch_player_info(player.player_id)
                if not info:
                    logger.warning(f"[{i}/{total}] No data for {player.full_name} ({player.player_id})")
                    skipped += 1
                    continue

                if update_player_info(session, player, info):
                    session.commit()
                    updated += 1
                    logger.info(f"[{i}/{total}] Updated {player.full_name}")
                else:
                    skipped += 1
                    logger.debug(f"[{i}/{total}] No changes for {player.full_name}")

                # Rate limit: nba_api recommends ~0.6s between calls
                time.sleep(0.6)

            except RetryError:
                logger.error(f"[{i}/{total}] Failed after retries: {player.full_name} ({player.player_id})")
                errors += 1
            except Exception as e:
                logger.error(f"[{i}/{total}] Error for {player.full_name}: {e}")
                session.rollback()
                errors += 1

        logger.info(f"Done. Updated: {updated}, Skipped: {skipped}, Errors: {errors}")
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill player bio info from NBA API")
    parser.add_argument("--player", type=str, help="Single player_id to update")
    parser.add_argument("--all", action="store_true", help="Refresh all players (not just missing)")
    args = parser.parse_args()
    run(player_id=args.player, refresh_all=args.all)
