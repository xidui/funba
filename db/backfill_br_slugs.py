"""Backfill Player.br_slug from Basketball-Reference's A-Z player index.

BR's slug (e.g. "kuminjo01") is a stable per-player identifier formed from
the last name + first-name initials + a collision suffix. It's how their
transaction page wires `<a href="/players/k/kuminjo01.html">` to a player,
so storing it on each Player row lets us match transactions deterministically
without name-fuzzing every time.

Strategy:
  1. Walk /players/{a..z}/ — 26 pages, each lists every player whose last
     name starts with that letter.
  2. For each row, extract slug + name + active years (From/To columns).
  3. Match to our Player table:
       - Exact full_name → match
       - Casefold + diacritic-folded normalize → match
       - Tie-break by active-year overlap (BR's From/To vs our from_year/to_year)
  4. Write br_slug to the matched Player.

Usage:
    python -m db.backfill_br_slugs
    python -m db.backfill_br_slugs --letter k     # one letter at a time
"""
from __future__ import annotations

import argparse
import logging
import re
import time
import unicodedata
from string import ascii_lowercase

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.orm import sessionmaker
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from db.models import Player, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)

BASE_URL = "https://www.basketball-reference.com"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_DELAY = 2.5  # BR is strict about rate limits


def _normalize(name: str) -> str:
    if not name:
        return ""
    folded = unicodedata.normalize("NFKD", name)
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = re.sub(r"[.'’`]+", "", folded)
    folded = folded.replace("-", " ")
    folded = re.sub(r"\s+", " ", folded).strip().casefold()
    folded = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", "", folded).strip()
    return folded


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


def _parse_index(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", id="players")
    if table is None:
        return []
    out = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all(["th", "td"])
        if len(cells) < 3:
            continue
        a = cells[0].find("a", href=True)
        if a is None:
            continue
        href = a["href"]
        slug = href.rsplit("/", 1)[-1].replace(".html", "")
        name = a.get_text(" ", strip=True)
        try:
            year_from = int(cells[1].get_text(strip=True))
        except (ValueError, IndexError):
            year_from = None
        try:
            year_to = int(cells[2].get_text(strip=True))
        except (ValueError, IndexError):
            year_to = None
        out.append({
            "slug": slug,
            "name": name,
            "year_from": year_from,
            "year_to": year_to,
        })
    return out


def _build_player_index(session) -> dict[str, list[Player]]:
    """Index every Player by normalized name (multiple players may share a name)."""
    by_norm: dict[str, list[Player]] = {}
    for p in session.query(Player).filter(Player.full_name.isnot(None)).all():
        key = _normalize(p.full_name or "")
        if not key:
            continue
        by_norm.setdefault(key, []).append(p)
    return by_norm


def _resolve(entry: dict, by_norm: dict[str, list[Player]]) -> Player | None:
    candidates = by_norm.get(_normalize(entry["name"]), [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    # Tie-break by active-year overlap.
    # BR's "From"/"To" is the season-end year (1948 = 1947-48); our Player.from_year /
    # to_year is also year_end-style. Pick the candidate whose range best overlaps.
    bf, bt = entry.get("year_from"), entry.get("year_to")

    def score(p: Player) -> int:
        pf, pt = p.from_year, p.to_year
        if bf is None or bt is None or pf is None or pt is None:
            return 0
        lo = max(bf, pf)
        hi = min(bt, pt)
        return max(0, hi - lo + 1)

    return max(candidates, key=score)


def run(letters: str = ascii_lowercase) -> None:
    session = Session()
    by_norm = _build_player_index(session)
    matched = unmatched = skipped_existing = 0
    unmatched_examples: list[str] = []

    with httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
        follow_redirects=True,
        timeout=20,
    ) as client:
        first = True
        for letter in letters:
            url = f"{BASE_URL}/players/{letter}/"
            if not first:
                time.sleep(REQUEST_DELAY)
            first = False
            try:
                html = _fetch(client, url)
            except httpx.HTTPError as exc:
                logger.warning("fetch failed %s: %s", url, exc)
                continue

            entries = _parse_index(html)
            logger.info("letter %s: %d entries", letter, len(entries))

            for e in entries:
                player = _resolve(e, by_norm)
                if player is None:
                    unmatched += 1
                    if len(unmatched_examples) < 20:
                        unmatched_examples.append(f"{e['name']} ({e['slug']}, {e.get('year_from')}-{e.get('year_to')})")
                    continue
                if player.br_slug == e["slug"]:
                    skipped_existing += 1
                    continue
                player.br_slug = e["slug"]
                matched += 1

            session.commit()

    logger.info(
        "done: matched=%d skipped_existing=%d unmatched=%d",
        matched, skipped_existing, unmatched,
    )
    if unmatched_examples:
        logger.info("unmatched examples: %s", unmatched_examples[:10])
    session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--letter", default=None, help="Single letter to process (default: a-z)")
    args = parser.parse_args()
    letters = args.letter.lower() if args.letter else ascii_lowercase
    run(letters)


if __name__ == "__main__":
    main()
