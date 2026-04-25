"""Daily refresh of active NBA contracts via Spotrac's player sitemap.

The sitemap at /sitemaps/sitemap-players-nba.xml lists ~550 currently-active
or near-active players. This is much narrower than what `discover_historical_rosters`
captures, but perfect for a daily incremental refresh: one cheap fetch reveals
every newly-signed deal across the league, and re-scraping each listed player's
page picks up extensions, options being exercised, etc.

Usage:
    python -m db.refresh_active_contracts                   # default daily run
    python -m db.refresh_active_contracts --max-age-hours 6 # only re-scrape
                                                            # players last touched
                                                            # more than 6h ago
"""
from __future__ import annotations

import argparse
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from db.backfill_player_contract import (
    BASE_URL,
    SpotracClient,
    USER_AGENT,
    _normalize_name,
    _players_by_name,
    _teams_by_abbr,
    _upsert_contract,
    _upsert_contract_years,
    parse_player_contracts,
)
from db.models import PlayerContract, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)

SITEMAP_URL = f"{BASE_URL}/sitemaps/sitemap-players-nba.xml"
_LOC_RE = re.compile(r"<loc>([^<]+)</loc>")
_TRANSACTIONS_SUFFIX = re.compile(r"/transactions/?$")


def _fetch_sitemap_urls() -> list[tuple[str, str | None]]:
    """Return [(player_page_url, name_from_url)] from the NBA player sitemap."""
    r = httpx.get(SITEMAP_URL, headers={"User-Agent": USER_AGENT}, timeout=30, follow_redirects=True)
    r.raise_for_status()
    seen: dict[str, str | None] = {}
    for raw in _LOC_RE.findall(r.text):
        # Drop /transactions/ subpages — same player, just deeper view
        url = _TRANSACTIONS_SUFFIX.sub("", raw.strip())
        # Normalize to the URL form parse_player_contracts expects
        # (/nba/player/_/id/N/name, not /nba/player/contracts/_/id/N/name)
        url = url.replace("/nba/player/contracts/_/id/", "/nba/player/_/id/")
        # Best-effort name from URL slug for matching (e.g. "aaron-gordon" → "Aaron Gordon")
        slug_match = re.search(r"/id/\d+/([^/]+)$", url)
        name = None
        if slug_match:
            name = " ".join(part.capitalize() for part in slug_match.group(1).split("-"))
        seen.setdefault(url, name)
    return list(seen.items())


def run(max_age_hours: float | None = None, dry_run: bool = False) -> dict:
    """Refresh active NBA contracts via the Spotrac sitemap.

    Returns a stats dict suitable for surfacing as a Celery task result.
    """
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    session = Session()
    try:
        logger.info("fetching sitemap %s", SITEMAP_URL)
        urls = _fetch_sitemap_urls()
        logger.info("sitemap unique players: %d", len(urls))

        # Optionally skip URLs whose contracts were scraped within max_age_hours
        if max_age_hours is not None:
            cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=max_age_hours)
            recent = {
                url for (url,) in session.query(PlayerContract.source_url)
                .filter(PlayerContract.source_url.isnot(None))
                .filter(PlayerContract.scraped_at >= cutoff)
                .distinct()
            }
            urls = [(u, n) for u, n in urls if u not in recent]
            logger.info("after age filter (>%dh): %d to refresh", max_age_hours, len(urls))

        exact, casefold, normalized = _players_by_name(session)
        team_by_abbr = _teams_by_abbr(session)

        if dry_run:
            logger.info("dry-run: skipping fetches")
            return {
                "sitemap_urls": len(urls),
                "dry_run": True,
                "started_at": started_at.isoformat(),
            }

        local = threading.local()

        def _ensure():
            if not hasattr(local, "client"):
                local.client = SpotracClient()
                local.session = Session()

        progress = {"done": 0, "ok": 0, "err": 0, "unmatched": 0}
        lock = threading.Lock()

        def _process(url: str, name_hint: str | None) -> None:
            _ensure()
            try:
                soup = local.client.get_soup(url)
                # Spotrac shows the canonical name as the page <h1>; prefer it
                # over the slug-derived hint for matching.
                h1 = soup.find("h1")
                name = h1.get_text(" ", strip=True) if h1 else (name_hint or "")
                player = (
                    exact.get(name)
                    or casefold.get(name.casefold())
                    or normalized.get(_normalize_name(name))
                )
                if player is None:
                    with lock:
                        progress["done"] += 1
                        progress["unmatched"] += 1
                    logger.warning("unmatched: %s (%s)", name, url)
                    return

                contracts = parse_player_contracts(soup)
                for info in contracts:
                    contract = _upsert_contract(
                        local.session,
                        player.player_id,
                        info,
                        spotrac_id=None,
                        source_url=url,
                        team_by_abbr=team_by_abbr,
                        signed_at_age=None,
                    )
                    _upsert_contract_years(local.session, contract.id, player.player_id, info.years_rows)
                local.session.commit()
                with lock:
                    progress["done"] += 1
                    progress["ok"] += 1
                    n = progress["done"]
                logger.info("[%d/%d] %s -> %d contracts", n, len(urls), name, len(contracts))
            except Exception as exc:
                local.session.rollback()
                with lock:
                    progress["done"] += 1
                    progress["err"] += 1
                logger.exception("failed for %s: %s", url, exc)

        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = [pool.submit(_process, url, name) for url, name in urls]
            for f in as_completed(futures):
                pass  # exceptions are caught inside _process

        logger.info(
            "done: ok=%d errors=%d unmatched=%d total=%d",
            progress["ok"], progress["err"], progress["unmatched"], len(urls),
        )
        finished_at = datetime.now(timezone.utc).replace(tzinfo=None)
        return {
            "sitemap_urls": len(urls),
            "ok": progress["ok"],
            "errors": progress["err"],
            "unmatched": progress["unmatched"],
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": (finished_at - started_at).total_seconds(),
        }
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh active NBA contracts via Spotrac sitemap")
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=None,
        help="Skip players whose contracts were scraped within this many hours",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(max_age_hours=args.max_age_hours, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
