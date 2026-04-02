"""Backfill Player bio/demographic data from nba_api commonplayerinfo endpoint.

Usage:
    python -m db.backfill_nba_player_info
    python -m db.backfill_nba_player_info --all
    python -m db.backfill_nba_player_info --player 201939
    python -m db.backfill_nba_player_info --missing-field birth_date --active-only
"""
import argparse
import logging
import re
import time
import unicodedata
from datetime import datetime
from urllib.parse import quote

from nba_api.stats.endpoints import commonplayerinfo
from requests.exceptions import ConnectionError
from sqlalchemy import distinct, or_
from sqlalchemy.orm import sessionmaker
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from db.models import Player, PlayerGameStats, engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)
_WIKIDATA_HEADERS = {
    "User-Agent": "funba-bio-backfill/1.0 (local maintenance script)",
}


@retry(
    wait=wait_exponential(multiplier=1, max=8),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((ConnectionError,)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def fetch_player_info(player_id: str) -> dict:
    """Fetch commonplayerinfo for a single player."""
    info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=10)
    payload = getattr(info, "common_player_info", None)
    if payload is None:
        return {}
    rows = payload.get_dict() or {}
    headers = rows.get("headers") or []
    data = rows.get("data") or []
    if not data:
        return {}
    row = dict(zip(headers, data[0]))
    return row


def _parse_date(val):
    """Parse ISO date string from API (e.g. '1984-12-30T00:00:00')."""
    if not val:
        return None
    try:
        text = str(val).strip()
        if text.startswith("+"):
            text = text[1:]
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
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


def _normalize_person_name(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("’", "'")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def fetch_player_info_from_wikidata(player_name: str) -> dict:
    """Fallback source for hard-to-fetch player bio data.

    Currently only returns birth_date when Wikidata has a basketball-player match.
    """
    import requests

    name = str(player_name or "").strip()
    if not name:
        return {}
    normalized_name = _normalize_person_name(name)

    response = requests.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbsearchentities",
            "search": name,
            "language": "en",
            "format": "json",
            "limit": 10,
        },
        headers=_WIKIDATA_HEADERS,
        timeout=20,
    )
    response.raise_for_status()
    results = response.json().get("search", [])

    for result in results:
        label = str(result.get("label") or "").strip().lower()
        normalized_label = _normalize_person_name(label)
        description = str(result.get("description") or "").strip().lower()
        entity_id = str(result.get("id") or "").strip()
        if not entity_id:
            continue
        if normalized_label != normalized_name:
            continue
        if "basketball" not in description:
            continue

        entity_resp = requests.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{quote(entity_id)}.json",
            headers=_WIKIDATA_HEADERS,
            timeout=20,
        )
        entity_resp.raise_for_status()
        entity = entity_resp.json().get("entities", {}).get(entity_id, {})
        birth_claims = entity.get("claims", {}).get("P569") or []
        if not birth_claims:
            continue
        value = birth_claims[0].get("mainsnak", {}).get("datavalue", {}).get("value", {})
        birth_time = str(value.get("time") or "").strip()
        birth_date = _parse_date(birth_time)
        if birth_date:
            return {"birth_date": birth_date}
    return {}


_FIELD_MAP = {
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

_DEFAULT_MISSING_FIELDS = ("height", "position", "season_exp")


def update_player_info(session, player: Player, info: dict) -> bool:
    """Apply bio fields from API response to a Player row. Returns True if changed."""
    changed = False

    if "birth_date" in info:
        birth_date_value = info.get("birth_date")
        if birth_date_value is not None and getattr(player, "birth_date", None) != birth_date_value:
            setattr(player, "birth_date", birth_date_value)
            changed = True

    for col, (api_key, converter) in _FIELD_MAP.items():
        raw = info.get(api_key)
        if raw is None or raw == "":
            continue
        val = converter(raw)
        if val is not None and val != "" and getattr(player, col) != val:
            setattr(player, col, val)
            changed = True

    return changed


def _missing_field_filters(missing_fields: tuple[str, ...]):
    filters = []
    for field in missing_fields:
        if field not in _FIELD_MAP:
            raise ValueError(f"Unsupported missing field: {field}")
        filters.append(getattr(Player, field).is_(None))
    return filters


def _players_to_process(session, *, player_id: str | None, refresh_all: bool, active_only: bool, missing_fields: tuple[str, ...]):
    query = session.query(Player).filter(Player.is_team == False)
    if active_only:
        query = query.filter(Player.is_active == True)

    if player_id:
        return query.filter(Player.player_id == player_id).all()
    if refresh_all:
        return query.all()

    return query.filter(or_(*_missing_field_filters(missing_fields))).all()


def _resolve_player_info(player: Player, *, source_mode: str) -> dict:
    if source_mode == "nba_api":
        return fetch_player_info(player.player_id)
    if source_mode == "wikidata":
        return fetch_player_info_from_wikidata(player.full_name or "")
    if source_mode == "hybrid":
        info = fetch_player_info(player.player_id)
        birth_date = info.get("BIRTHDATE") if isinstance(info, dict) else None
        if birth_date:
            return info
        fallback = fetch_player_info_from_wikidata(player.full_name or "")
        if fallback:
            merged = dict(info or {})
            merged.update(fallback)
            return merged
        return info or {}
    raise ValueError(f"Unsupported source_mode: {source_mode}")


def run(
    player_id: str | None = None,
    refresh_all: bool = False,
    *,
    active_only: bool = False,
    inactive_only: bool = False,
    with_games_only: bool = False,
    missing_fields: tuple[str, ...] = _DEFAULT_MISSING_FIELDS,
    sleep_seconds: float = 0.6,
    limit: int | None = None,
    source_mode: str = "nba_api",
):
    session = Session()
    try:
        if active_only and inactive_only:
            raise ValueError("Cannot combine active_only and inactive_only")

        if with_games_only:
            player_ids_subq = session.query(distinct(PlayerGameStats.player_id).label("player_id")).subquery()
            query = (
                session.query(Player)
                .join(player_ids_subq, player_ids_subq.c.player_id == Player.player_id)
                .filter(Player.is_team == False)
            )
            if active_only:
                query = query.filter(Player.is_active == True)
            if inactive_only:
                query = query.filter(Player.is_active == False)
            if player_id:
                query = query.filter(Player.player_id == player_id)
            elif not refresh_all:
                query = query.filter(or_(*_missing_field_filters(missing_fields)))
            players = query.all()
        else:
            players = _players_to_process(
                session,
                player_id=player_id,
                refresh_all=refresh_all,
                active_only=active_only,
                missing_fields=missing_fields,
            )
            if inactive_only:
                players = [player for player in players if player.is_active is False]
        if limit is not None and limit > 0:
            players = players[:limit]

        total = len(players)
        logger.info(f"Found {total} players to process")
        updated = 0
        skipped = 0
        errors = 0

        for i, player in enumerate(players, 1):
            try:
                info = _resolve_player_info(player, source_mode=source_mode)
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
                time.sleep(max(0.0, sleep_seconds))

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
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only process players currently marked active",
    )
    parser.add_argument(
        "--inactive-only",
        action="store_true",
        help="Only process players currently marked inactive",
    )
    parser.add_argument(
        "--with-games-only",
        action="store_true",
        help="Only process players that appear in PlayerGameStats",
    )
    parser.add_argument(
        "--missing-field",
        action="append",
        choices=sorted(_FIELD_MAP.keys()),
        help="When not using --all or --player, select players missing any of these fields. Repeatable.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.6,
        help="Delay between API calls. Default 0.6 seconds.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional max number of players to process this run.",
    )
    parser.add_argument(
        "--source-mode",
        choices=("nba_api", "wikidata", "hybrid"),
        default="nba_api",
        help="Source selection mode. Default nba_api.",
    )
    args = parser.parse_args()
    run(
        player_id=args.player,
        refresh_all=args.all,
        active_only=args.active_only,
        inactive_only=args.inactive_only,
        with_games_only=args.with_games_only,
        missing_fields=tuple(args.missing_field or _DEFAULT_MISSING_FIELDS),
        sleep_seconds=args.sleep_seconds,
        limit=args.limit,
        source_mode=args.source_mode,
    )
