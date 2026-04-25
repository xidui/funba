"""Re-parse Spotrac player pages to fill in NULL signed_with_team_id values.

After the initial backfill_player_contract run, ~33% of contracts have NULL
signed_with_team_id because Spotrac's logo filenames use codes that didn't map
cleanly (e.g. nba_cle1.png, nba_gs.png). The main backfill was updated with a
richer code map; this script re-fetches each affected player page once, parses
every contract-wrapper h2 again, and UPDATEs signed_with_team_id in place.

Usage:
    python -m db.enrich_contract_teams                # process all null rows
    python -m db.enrich_contract_teams --player "LeBron"
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy.orm import sessionmaker

from db.backfill_player_contract import (
    SpotracClient,
    _parse_contract_header,
    _teams_by_abbr,
)
from db.models import Player, PlayerContract, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


def run(player_filter: str | None = None) -> None:
    session = Session()
    client = SpotracClient()
    team_by_abbr = _teams_by_abbr(session)
    try:
        q = (
            session.query(PlayerContract, Player)
            .join(Player, PlayerContract.player_id == Player.player_id)
            .filter(PlayerContract.signed_with_team_id.is_(None))
            .filter(PlayerContract.source_url.isnot(None))
        )
        if player_filter:
            q = q.filter(Player.full_name.ilike(f"%{player_filter}%"))
        null_contracts = q.all()
        logger.info("%d null-team contracts to enrich", len(null_contracts))

        # Group by URL so we fetch each page once
        by_url: dict[str, list] = {}
        for contract, player in null_contracts:
            by_url.setdefault(contract.source_url, []).append((contract, player))

        from bs4 import BeautifulSoup  # local import to keep module import fast

        fixed = 0
        unchanged = 0
        for i, (url, entries) in enumerate(by_url.items(), 1):
            name = entries[0][1].full_name
            try:
                soup = client.get_soup(url)
            except Exception as exc:
                logger.warning("[%d/%d] fetch failed for %s: %s", i, len(by_url), name, exc)
                continue

            # Build start_season -> team_abbr map from the page
            team_by_start: dict[int, str | None] = {}
            for wrapper in soup.find_all("div", class_="contract-wrapper"):
                start, end, _type, _current, abbr = _parse_contract_header(wrapper)
                if start is not None:
                    team_by_start[start] = abbr

            page_fixed = 0
            for contract, player in entries:
                abbr = team_by_start.get(contract.start_season)
                if not abbr:
                    unchanged += 1
                    continue
                team_id = team_by_abbr.get(abbr.upper())
                if not team_id:
                    logger.warning("unknown team code %r for %s (%d-%d)", abbr, name, contract.start_season, contract.end_season)
                    unchanged += 1
                    continue
                contract.signed_with_team_id = team_id
                fixed += 1
                page_fixed += 1
            session.commit()
            logger.info("[%d/%d] %s: fixed %d/%d", i, len(by_url), name, page_fixed, len(entries))

        logger.info("done: fixed=%d unchanged=%d", fixed, unchanged)
    finally:
        client.close()
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich signed_with_team_id on existing contracts")
    parser.add_argument("--player", type=str, help="Only process players whose name contains this")
    args = parser.parse_args()
    run(player_filter=args.player)


if __name__ == "__main__":
    main()
