"""Scrape NBA transactions from Basketball-Reference.

BR's per-season transaction page is structured HTML — each <li> is a date,
each <p> within is one transaction. Anchor tags carry directional metadata
(`data-attr-from` / `data-attr-to`) and stable BR player slugs in their
hrefs, so we can extract clean structured data without natural-language
parsing.

Stores:
  TeamTransaction      one row per <p>
  TransactionAsset     one row per moving piece (player / pick / cash / exception)

Idempotent — uses (transaction_date, sha1(raw_text)) as the unique key.

Usage:
    python -m db.scrape_br_transactions                  # current season
    python -m db.scrape_br_transactions --season 2024    # specific BR year
    python -m db.scrape_br_transactions --from 2010 --to 2026  # range
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import re
import time
from datetime import datetime, date, timezone
from typing import Iterable

import httpx
from bs4 import BeautifulSoup, Tag
from sqlalchemy.orm import sessionmaker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from db.models import (
    Player,
    Team,
    TeamTransaction,
    TransactionAsset,
    engine,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)

BASE_URL = "https://www.basketball-reference.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_DELAY = 2.5

# BR uses a few codes that don't match our Team.abbr verbatim.
_BR_TEAM_OVERRIDES: dict[str, str] = {
    # Modern aliases
    "BRK": "BKN",  # Brooklyn
    "CHO": "CHA",  # Charlotte
    "PHO": "PHX",  # Phoenix
    # Legacy → canonical (BR keeps using the era code in old transaction pages)
    "NJN": "BKN",
    "NOH": "NOP",
    "NOK": "NOP",
    "SEA": "OKC",
    "VAN": "MEM",
    "CHH": "CHA",
    "KCK": "SAC",
    "SDC": "LAC",
    "BAL": "WAS",  # Modern Wizards (originally Baltimore Bullets→Capital→Washington)
}

# Date format used by BR (e.g. "April 12, 2026")
_DATE_FMT = "%B %d, %Y"

# Pick patterns
_PICK_RE = re.compile(r"(\d{4})\s*(?:1st|2nd|first|second)[\s-]*(?:rd|nd)?[\s-]*(?:round)?\s*(?:draft\s+)?pick", re.I)
_PICK_ORDINAL_RE = re.compile(r"(\d{4})\s*(1st|2nd|first|second)", re.I)
_PROTECTION_TAIL_RE = re.compile(r"\d{4}\s*(?:1st|2nd)[\s-]*(?:rd|nd)[\s-]*pick\s+is\s+([^.]+)", re.I)


# ---------------- HTTP ----------------


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    retry=retry_if_exception_type((httpx.HTTPError,)),
    reraise=True,
)
def _fetch(client: httpx.Client, url: str) -> str:
    r = client.get(url)
    r.raise_for_status()
    return r.text


# ---------------- Helpers ----------------


def _resolve_team_id(br_code: str | None, team_by_abbr: dict[str, str]) -> str | None:
    if not br_code:
        return None
    code = br_code.upper()
    canonical = _BR_TEAM_OVERRIDES.get(code, code)
    return team_by_abbr.get(canonical)


def _extract_br_slug(href: str) -> str | None:
    m = re.search(r"/players/[a-z]/([a-z0-9]+)\.html", href)
    return m.group(1) if m else None


def _classify(text: str) -> tuple[str, int]:
    """Return (transaction_type, multi_team_count)."""
    t = text.lower().strip()
    multi_match = re.match(r"in a (\d+)-team trade", t)
    multi = int(multi_match.group(1)) if multi_match else 1

    if "traded" in t:
        return "trade", multi
    if "stepped down" in t or "fired" in t or "dismissed" in t or "hired" in t or "head coach" in t:
        return "coach_change", 1
    if "fined" in t or "suspended" in t:
        return "fine_or_suspension", 1
    if "signed" in t and "extension" in t:
        return "extension", 1
    if "extension" in t and "signed" not in t:
        return "extension", 1
    if "10-day" in t or "ten-day" in t or "second 10-day" in t or "2nd 10-day" in t:
        return "ten_day", 1
    if "converted" in t and ("two-way" in t or "regular contract" in t):
        return "conversion", 1
    if "signed" in t:
        if "two-way" in t:
            return "two_way", 1
        return "signing", 1
    if "waived" in t or "released" in t or "cut" in t:
        return "waive", 1
    if "claimed" in t and "waivers" in t:
        return "waive_claim", 1
    if "draft" in t and ("selected" in t or "drafted" in t):
        return "draft", 1
    return "other", 1


def _split_trade_legs(p_node: Tag) -> list[Tag]:
    """A multi-team trade <p> packs several "the X traded Y to the Z" segments
    separated by ; into a single tag. Split into per-leg sub-fragments so each
    can be parsed independently. Single-team trades return one fragment."""
    # Extract inner HTML (without the <p> wrapper)
    inner = "".join(str(c) for c in p_node.contents)
    # Drop the leading "In a N-team trade, " preamble if present
    inner = re.sub(r"^In a \d+-team trade,\s*", "", inner, flags=re.I)

    # Split on ';' and 'and the' / '. ' boundaries that separate legs
    raw_parts = re.split(r"(?<=</a>);\s*(?:and\s+)?|(?<=\.)\s+(?=the\s)", inner, flags=re.I)
    legs: list[Tag] = []
    for part in raw_parts:
        if not part.strip():
            continue
        wrapped = BeautifulSoup(f"<div>{part}</div>", "html.parser").div
        if wrapped is not None:
            legs.append(wrapped)
    return legs


def _parse_picks(text: str) -> list[dict]:
    """Find all draft-pick mentions in a sub-fragment text."""
    picks = []
    for m in _PICK_RE.finditer(text):
        year = int(m.group(1))
        ord_m = _PICK_ORDINAL_RE.search(m.group(0))
        round_str = ord_m.group(2).lower() if ord_m else ""
        rnd = 1 if round_str.startswith(("1", "first")) else 2
        picks.append({"pick_year": year, "pick_round": rnd})
    return picks


def _build_player_lookups(session) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Return (br_slug → player_id, normalized_name → player_id, casefold_name → player_id)."""
    by_slug: dict[str, str] = {}
    by_norm: dict[str, str] = {}
    by_cf: dict[str, str] = {}
    import unicodedata

    def normalize(name: str) -> str:
        if not name:
            return ""
        f = unicodedata.normalize("NFKD", name)
        f = "".join(ch for ch in f if not unicodedata.combining(ch))
        f = re.sub(r"[.'’`]+", "", f)
        f = f.replace("-", " ")
        f = re.sub(r"\s+", " ", f).strip().casefold()
        f = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", "", f).strip()
        return f

    for p in session.query(Player).filter(Player.full_name.isnot(None)).all():
        if p.br_slug:
            by_slug[p.br_slug] = p.player_id
        if p.full_name:
            by_norm.setdefault(normalize(p.full_name), p.player_id)
            by_cf.setdefault(p.full_name.casefold(), p.player_id)
    return by_slug, by_norm, by_cf


# ---------------- Per-transaction parser ----------------


def _parse_transaction(
    p_node: Tag,
    *,
    season: int,
    transaction_date: date,
    source_url: str,
    team_by_abbr: dict[str, str],
    by_slug: dict[str, str],
    by_norm: dict[str, str],
    by_cf: dict[str, str],
) -> tuple[dict, list[dict]]:
    raw_html = str(p_node)
    raw_text = re.sub(r"\s+", " ", p_node.get_text(" ", strip=True))
    text_hash = hashlib.sha1(raw_text.encode("utf-8")).hexdigest()
    transaction_type, multi_count = _classify(raw_text)

    record = {
        "transaction_date": transaction_date,
        "season": season,
        "transaction_type": transaction_type,
        "multi_team_count": multi_count,
        "raw_text": raw_text,
        "raw_html": raw_html,
        "text_hash": text_hash,
        "source_url": source_url,
        "scraped_at": datetime.now(timezone.utc).replace(tzinfo=None),
    }

    assets: list[dict] = []

    # Trades — split into legs (multi-team or single)
    if transaction_type == "trade":
        legs = _split_trade_legs(p_node)

        for leg in legs:
            from_t = leg.find("a", attrs={"data-attr-from": True})
            to_t = leg.find("a", attrs={"data-attr-to": True})
            from_id = _resolve_team_id(from_t["data-attr-from"] if from_t else None, team_by_abbr)
            to_id = _resolve_team_id(to_t["data-attr-to"] if to_t else None, team_by_abbr)

            # BR phrasing: "X traded {SENT} to Y for {RECEIVED}" — assets after
            # the word "for" come back from Y to X (reverse direction).
            # We split the leg into two slices so we can apply the right
            # direction to the assets on each side.
            inner_html = "".join(str(c) for c in leg.contents)
            for_idx = re.search(r"\bfor\b", inner_html, re.I)
            sent_html = inner_html[:for_idx.start()] if for_idx else inner_html
            received_html = inner_html[for_idx.end():] if for_idx else ""

            sent_frag = BeautifulSoup(f"<div>{sent_html}</div>", "html.parser").div
            received_frag = BeautifulSoup(f"<div>{received_html}</div>", "html.parser").div

            def _emit(frag: Tag | None, src: str | None, dst: str | None) -> None:
                if frag is None:
                    return
                frag_text = frag.get_text(" ", strip=True)
                # Players
                for a in frag.find_all("a", href=True):
                    if "/players/" not in a["href"]:
                        continue
                    slug = _extract_br_slug(a["href"])
                    name = a.get_text(" ", strip=True)
                    pid = (by_slug.get(slug) if slug else None) or by_norm.get(_normalize_name_local(name)) or by_cf.get(name.casefold())
                    assets.append({
                        "asset_type": "player",
                        "from_team_id": src,
                        "to_team_id": dst,
                        "player_id": pid,
                        "player_br_slug": slug,
                        "player_name_raw": name,
                    })
                # Cash
                if re.search(r"\bcash\b", frag_text, re.I) and not re.search(r"\bcash considerations? only\b", frag_text, re.I):
                    # Skip if "cash" appears only inside a player name (rare); guard with word boundary.
                    assets.append({
                        "asset_type": "cash",
                        "from_team_id": src,
                        "to_team_id": dst,
                    })
                # Picks. Trailing prose like "2032 2nd-rd pick is TOR own"
                # mentions the same pick again to describe protection — dedupe
                # within this leg to avoid double-counting it.
                seen_picks: set[tuple] = set()
                for pk in _parse_picks(frag_text):
                    key = (pk["pick_year"], pk["pick_round"])
                    if key in seen_picks:
                        continue
                    seen_picks.add(key)
                    prot = None
                    year_str = str(pk["pick_year"])
                    m2 = re.search(
                        rf"{year_str}\s*(?:1st|2nd|first|second)[\s\-]*(?:rd|nd|round)?\s*pick\s+is\s+([^.]+)",
                        raw_text, re.I,
                    )
                    if m2:
                        prot = m2.group(1).strip()
                    assets.append({
                        "asset_type": "pick",
                        "from_team_id": src,
                        "to_team_id": dst,
                        "pick_year": pk["pick_year"],
                        "pick_round": pk["pick_round"],
                        "pick_protection": prot,
                    })

            # Sent assets go from→to; received assets reverse direction
            _emit(sent_frag, from_id, to_id)
            _emit(received_frag, to_id, from_id)

            # Trade exceptions are leg-level — credited to the receiving side
            # ("Toronto also received a trade exception").
            if re.search(r"trade exception", leg.get_text(" ", strip=True), re.I):
                assets.append({
                    "asset_type": "exception",
                    "from_team_id": None,
                    "to_team_id": to_id,
                    "notes": "trade_exception",
                })

        return record, assets

    # Non-trade events: one team plus one (usually) player.
    # Find the single team anchor on the page (no data-attr-from/to needed —
    # signings/waives use the plain team link).
    team_anchor = None
    for a in p_node.find_all("a", href=True):
        if "/teams/" in a["href"]:
            team_anchor = a
            break
    team_code = None
    if team_anchor is not None:
        m = re.search(r"/teams/([A-Z]{3})/", team_anchor["href"])
        if m:
            team_code = m.group(1)
    team_id = _resolve_team_id(team_code, team_by_abbr)

    # All player anchors involved
    for a in p_node.find_all("a", href=True):
        if "/players/" not in a["href"]:
            continue
        slug = _extract_br_slug(a["href"])
        name = a.get_text(" ", strip=True)
        pid = (by_slug.get(slug) if slug else None) or by_norm.get(_normalize_name_local(name)) or by_cf.get(name.casefold())
        if transaction_type in ("waive", "waive_claim"):
            from_id, to_id = team_id, None
        else:
            from_id, to_id = None, team_id
        assets.append({
            "asset_type": "player",
            "from_team_id": from_id,
            "to_team_id": to_id,
            "player_id": pid,
            "player_br_slug": slug,
            "player_name_raw": name,
        })

    return record, assets


# A second normalize function so we don't import from a sibling module.
def _normalize_name_local(name: str) -> str:
    if not name:
        return ""
    import unicodedata
    f = unicodedata.normalize("NFKD", name)
    f = "".join(ch for ch in f if not unicodedata.combining(ch))
    f = re.sub(r"[.'’`]+", "", f)
    f = f.replace("-", " ")
    f = re.sub(r"\s+", " ", f).strip().casefold()
    f = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", "", f).strip()
    return f


# ---------------- Page-level parse + upsert ----------------


def _br_year_to_season(br_year: int) -> int:
    """BR URL year is the season-end year (NBA_2026 = 2025-26). Our season
    code is 2 + start_year, so 22025 = 2025-26."""
    return 20000 + (br_year - 1)


def _upsert_transaction(session, record: dict, assets: list[dict]) -> bool:
    existing = (
        session.query(TeamTransaction)
        .filter(
            TeamTransaction.transaction_date == record["transaction_date"],
            TeamTransaction.text_hash == record["text_hash"],
        )
        .first()
    )
    if existing is not None:
        # Refresh asset rows; replace wholesale on each scrape (cheap, predictable).
        # But avoid churn if assets unchanged.
        existing.scraped_at = record["scraped_at"]
        existing.transaction_type = record["transaction_type"]
        existing.multi_team_count = record["multi_team_count"]
        existing.raw_html = record["raw_html"]
        existing.source_url = record["source_url"]
        # Re-attach assets only if count differs (cheap heuristic)
        existing_assets_count = (
            session.query(TransactionAsset)
            .filter(TransactionAsset.transaction_id == existing.id)
            .count()
        )
        if existing_assets_count != len(assets):
            session.query(TransactionAsset).filter(TransactionAsset.transaction_id == existing.id).delete()
            for a in assets:
                session.add(TransactionAsset(transaction_id=existing.id, **a))
        return False

    tr = TeamTransaction(**record)
    session.add(tr)
    session.flush()  # need tr.id for assets
    for a in assets:
        session.add(TransactionAsset(transaction_id=tr.id, **a))
    return True


def _parse_year_page(html: str, br_year: int, source_url: str, session) -> tuple[int, int, int]:
    """Return (transactions_added, transactions_updated, parse_errors)."""
    soup = BeautifulSoup(html, "html.parser")
    # The transactions live in a <ul> with <li> per date
    items = soup.find_all("li")
    season = _br_year_to_season(br_year)
    team_by_abbr = {t.abbr.upper(): t.team_id for t in session.query(Team).all() if t.abbr}
    by_slug, by_norm, by_cf = _build_player_lookups(session)

    added = updated = errors = 0
    for li in items:
        span = li.find("span")
        if span is None:
            continue
        date_str = span.get_text(" ", strip=True)
        try:
            transaction_date = datetime.strptime(date_str, _DATE_FMT).date()
        except ValueError:
            continue  # not a date <li>

        ps = li.find_all("p")
        for p_node in ps:
            try:
                record, assets = _parse_transaction(
                    p_node,
                    season=season,
                    transaction_date=transaction_date,
                    source_url=source_url,
                    team_by_abbr=team_by_abbr,
                    by_slug=by_slug,
                    by_norm=by_norm,
                    by_cf=by_cf,
                )
                inserted = _upsert_transaction(session, record, assets)
                added += int(inserted)
                if not inserted:
                    updated += 1
            except Exception as exc:
                errors += 1
                logger.exception("parse error on %s: %s", transaction_date, exc)
        session.commit()

    return added, updated, errors


def run(year_from: int, year_to: int) -> None:
    session = Session()
    try:
        with httpx.Client(
            headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
            follow_redirects=True,
            timeout=30,
        ) as client:
            total_added = total_updated = total_errors = 0
            first = True
            for year in range(year_from, year_to + 1):
                if not first:
                    time.sleep(REQUEST_DELAY)
                first = False
                url = f"{BASE_URL}/leagues/NBA_{year}_transactions.html"
                logger.info("fetching %s", url)
                try:
                    html = _fetch(client, url)
                except httpx.HTTPError as exc:
                    logger.warning("fetch failed %s: %s", url, exc)
                    continue
                added, updated, errors = _parse_year_page(html, year, url, session)
                total_added += added
                total_updated += updated
                total_errors += errors
                logger.info("year %d: added=%d updated=%d errors=%d", year, added, updated, errors)
            logger.info("done: added=%d updated=%d errors=%d", total_added, total_updated, total_errors)
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, default=None, help="Single BR season-end year (e.g. 2026 = 2025-26)")
    parser.add_argument("--from", dest="year_from", type=int, default=None)
    parser.add_argument("--to", dest="year_to", type=int, default=None)
    args = parser.parse_args()
    if args.season:
        run(args.season, args.season)
    elif args.year_from and args.year_to:
        run(args.year_from, args.year_to)
    else:
        # Default: current and previous season
        cur_year = datetime.now().year
        # If we're in fall, current BR season = next calendar year
        cur_season_end = cur_year if datetime.now().month <= 7 else cur_year + 1
        run(cur_season_end - 1, cur_season_end)


if __name__ == "__main__":
    main()
