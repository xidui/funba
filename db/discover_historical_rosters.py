"""Discover player URLs from historical team cap pages to scrape contracts for
retired/departed players not covered by the yearly contracts index.

Spotrac's /nba/contracts/_/year/Y only lists still-active deals, so deals that
expired before 2025 (e.g. Hayward's 2017 BOS contract) never appear there.
Each team's /nba/<slug>/cap/_/year/Y page, however, lists every rostered and
dead-cap player for that team-season — a much richer discovery source for
historical coverage.

Usage:
    python -m db.discover_historical_rosters                   # 2010-2018
    python -m db.discover_historical_rosters --from 2005 --to 2018
"""
from __future__ import annotations

import argparse
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy.orm import sessionmaker
from urllib.parse import urljoin

from db.backfill_player_contract import (
    BASE_URL,
    SpotracClient,
    _normalize_name,
    _players_by_name,
    _teams_by_abbr,
    _upsert_contract,
    _upsert_contract_years,
    parse_player_contracts,
)
from db.models import PlayerContract, Team, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


# A few of our Team.slug values don't match Spotrac's URL slugs.
_SPOTRAC_SLUG_OVERRIDES = {
    "los-angeles-clippers": "la-clippers",
}


def _team_cap_url(slug: str, year: int) -> str:
    spotrac_slug = _SPOTRAC_SLUG_OVERRIDES.get(slug, slug)
    return f"{BASE_URL}/nba/{spotrac_slug}/cap/_/year/{year}"


def _extract_player_urls(soup) -> list[tuple[str, str]]:
    """Return [(player_name, player_url)] from anchors on a team-cap page."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/nba/player/" not in href:
            continue
        url = href if href.startswith("http") else urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        name = a.get_text(" ", strip=True)
        if not name:
            continue
        # Team cap pages render names like "Hayward Gordon Hayward" (last-name
        # logo alt + real name); keep only the last (longest) segment.
        tokens = name.split()
        if len(tokens) >= 4 and tokens[0] == tokens[-2]:
            name = " ".join(tokens[2:])
        out.append((name, url))
    return out


def run(year_from: int, year_to: int) -> None:
    session = Session()
    client = SpotracClient()
    try:
        teams = session.query(Team).filter(Team.active.is_(True), Team.is_legacy.is_(False)).all()
        logger.info("iterating %d teams x %d years = %d cap pages", len(teams), year_to - year_from + 1, len(teams) * (year_to - year_from + 1))

        # Collect unique URLs across all team-years
        seen_urls: dict[str, str] = {}  # url -> any name
        for year in range(year_from, year_to + 1):
            for t in teams:
                url = _team_cap_url(t.slug, year)
                try:
                    soup = client.get_soup(url)
                except Exception as exc:
                    logger.warning("cap page failed %s/%d: %s", t.slug, year, exc)
                    continue
                for name, player_url in _extract_player_urls(soup):
                    seen_urls.setdefault(player_url, name)
            logger.info("year %d complete — cumulative %d unique URLs", year, len(seen_urls))

        logger.info("discovery done: %d unique player URLs across %d-%d", len(seen_urls), year_from, year_to)

        # Filter to URLs we haven't already scraped
        handled = {
            url for (url,) in session.query(PlayerContract.source_url)
            .filter(PlayerContract.source_url.isnot(None)).distinct()
        }
        new_urls = [u for u in seen_urls if u not in handled]
        logger.info("%d new URLs (skipping %d already in DB)", len(new_urls), len(seen_urls) - len(new_urls))

        exact, casefold, normalized = _players_by_name(session)
        team_by_abbr = _teams_by_abbr(session)

        # Pre-resolve player matches so the worker pool only does I/O + DB writes.
        # Matching against in-memory dicts is cheap and lets us log unmatched
        # cases up-front instead of per-worker.
        resolved: list[tuple[str, str, object]] = []  # (url, name, player)
        unmatched = 0
        for url in new_urls:
            name = seen_urls[url]
            player = (
                exact.get(name)
                or casefold.get(name.casefold())
                or normalized.get(_normalize_name(name))
            )
            if player is None:
                unmatched += 1
                logger.warning("unmatched: %s", name)
                continue
            resolved.append((url, name, player))
        logger.info("%d resolved, %d unmatched — dispatching to workers", len(resolved), unmatched)

        # Each worker has its own SpotracClient (sleep state is per-thread) and
        # its own DB session. Workers commit independently; URLs are unique so
        # there's no upsert race.
        local = threading.local()

        def _worker_setup():
            local.client = SpotracClient()
            local.session = Session()

        def _worker_teardown():
            try:
                local.session.close()
            finally:
                local.client.close()

        progress = {"done": 0, "ok": 0, "err": 0}
        progress_lock = threading.Lock()

        def _process(url: str, name: str, player) -> None:
            if not hasattr(local, "client"):
                _worker_setup()
            try:
                soup = local.client.get_soup(url)
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
                with progress_lock:
                    progress["done"] += 1
                    progress["ok"] += 1
                    n = progress["done"]
                logger.info("[%d/%d] %s (%s) -> %d contracts", n, len(resolved), name, player.player_id, len(contracts))
            except Exception as exc:
                local.session.rollback()
                with progress_lock:
                    progress["done"] += 1
                    progress["err"] += 1
                logger.exception("failed for %s: %s", name, exc)

        workers = 3
        logger.info("starting %d workers", workers)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_process, url, name, player) for url, name, player in resolved]
            for f in as_completed(futures):
                # propagate unexpected exceptions in the executor itself
                exc = f.exception()
                if exc is not None and not isinstance(exc, Exception):
                    logger.error("executor exception: %s", exc)

        # Workers' thread-locals are GC'd after the pool shuts down, so the
        # per-thread sessions/clients close on their own when threads exit.
        logger.info("done: ok=%d errors=%d unmatched=%d", progress["ok"], progress["err"], unmatched)
    finally:
        client.close()
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Discover historical contracts from Spotrac team cap pages")
    parser.add_argument("--from", dest="year_from", type=int, default=2010)
    parser.add_argument("--to", dest="year_to", type=int, default=2018)
    args = parser.parse_args()
    run(args.year_from, args.year_to)


if __name__ == "__main__":
    main()
