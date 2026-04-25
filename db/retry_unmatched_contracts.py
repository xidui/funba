"""Retry scraping player pages that were previously unmatched.

After expanding name normalization (diacritic folding, nickname aliases),
this script re-runs just the set of player URLs that failed on the first
pass instead of re-scraping all ~500 players.

Usage:
    python -m db.retry_unmatched_contracts
"""
from __future__ import annotations

import logging

from sqlalchemy.orm import sessionmaker

from db.backfill_player_contract import (
    CONTRACTS_INDEX_URL_FMT,
    DISCOVERY_YEARS,
    SpotracClient,
    _normalize_name,
    _players_by_name,
    _teams_by_abbr,
    _upsert_contract,
    _upsert_contract_years,
    parse_contracts_index,
    parse_player_contracts,
)
from db.models import PlayerContract, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


def run() -> None:
    session = Session()
    client = SpotracClient()
    try:
        # Collect every (url, index_row) across DISCOVERY_YEARS
        rows_by_url: dict[str, list] = {}
        for year in DISCOVERY_YEARS:
            url = CONTRACTS_INDEX_URL_FMT.format(year=year)
            logger.info("Fetching contracts index year=%d", year)
            soup = client.get_soup(url)
            for r in parse_contracts_index(soup):
                rows_by_url.setdefault(r.spotrac_url, []).append(r)

        exact, casefold, normalized = _players_by_name(session)
        team_by_abbr = _teams_by_abbr(session)

        # A URL is "already handled" if any contract in the DB points at it.
        handled_urls = {
            url for (url,) in session.query(PlayerContract.source_url).filter(
                PlayerContract.source_url.isnot(None)
            ).distinct()
        }
        unhandled = [u for u in rows_by_url if u not in handled_urls]
        logger.info("unhandled URLs: %d (of %d total)", len(unhandled), len(rows_by_url))

        matched = unmatched = 0
        for i, url in enumerate(unhandled, 1):
            rows = rows_by_url[url]
            primary = rows[0]
            player = (
                exact.get(primary.name)
                or casefold.get(primary.name.casefold())
                or normalized.get(_normalize_name(primary.name))
            )
            if player is None:
                unmatched += 1
                logger.warning("[%d/%d] still unmatched: %s", i, len(unhandled), primary.name)
                continue
            matched += 1

            age_by_start = {r.start_season: r.signed_at_age for r in rows if r.start_season is not None}
            try:
                soup = client.get_soup(url)
                contracts = parse_player_contracts(soup)
                logger.info("[%d/%d] %s -> %d contracts", i, len(unhandled), primary.name, len(contracts))
                for info in contracts:
                    contract = _upsert_contract(
                        session,
                        player.player_id,
                        info,
                        spotrac_id=primary.spotrac_id,
                        source_url=url,
                        team_by_abbr=team_by_abbr,
                        signed_at_age=age_by_start.get(info.start_season),
                    )
                    _upsert_contract_years(session, contract.id, player.player_id, info.years_rows)
                session.commit()
            except Exception as exc:
                session.rollback()
                logger.exception("failed for %s: %s", primary.name, exc)

        logger.info("done: matched=%d unmatched=%d", matched, unmatched)
    finally:
        client.close()
        session.close()


if __name__ == "__main__":
    run()
