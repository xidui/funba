"""Backfill NBA championships from 1947-1982.

Our Award table only had champions from 1983-84 onward (the era nba_api
exposes via leaguepassersleaders/awards endpoints). Anything older — most
of the Celtics dynasty, the Minneapolis Lakers, Bill Russell era — was
missing, which made team-page championship strips look very thin for
storied franchises.

This module hardcodes the canonical list of pre-1983 champions and
upserts them into Award. The list itself never changes, so a one-shot
run is enough; rerunning is idempotent.

Season encoding follows the existing convention: a 5-digit code with
leading "2" (regular-season type) plus the year the season *started*.
The 1957 trophy is for the 1956-57 season, so it lives at season=21956.

Usage:
    python -m db.backfill_pre1983_championships
"""
from __future__ import annotations

import argparse
import logging

from sqlalchemy.orm import sessionmaker

from db.models import Award, Team, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


# (season_start_year, current_team_abbr_or_legacy_id, era_team_name)
# season_start_year + 1 is the calendar year of the trophy.
_CHAMPIONS: list[tuple[int, str, str]] = [
    (1946, "GSW", "Philadelphia Warriors"),
    (1947, "1610610024", "Baltimore Bullets"),  # original BAL franchise (defunct), not modern Wizards
    (1948, "LAL", "Minneapolis Lakers"),
    (1949, "LAL", "Minneapolis Lakers"),
    (1950, "SAC", "Rochester Royals"),
    (1951, "LAL", "Minneapolis Lakers"),
    (1952, "LAL", "Minneapolis Lakers"),
    (1953, "LAL", "Minneapolis Lakers"),
    (1954, "PHI", "Syracuse Nationals"),
    (1955, "GSW", "Philadelphia Warriors"),
    (1956, "BOS", "Boston Celtics"),
    (1957, "ATL", "St. Louis Hawks"),
    (1958, "BOS", "Boston Celtics"),
    (1959, "BOS", "Boston Celtics"),
    (1960, "BOS", "Boston Celtics"),
    (1961, "BOS", "Boston Celtics"),
    (1962, "BOS", "Boston Celtics"),
    (1963, "BOS", "Boston Celtics"),
    (1964, "BOS", "Boston Celtics"),
    (1965, "BOS", "Boston Celtics"),
    (1966, "PHI", "Philadelphia 76ers"),
    (1967, "BOS", "Boston Celtics"),
    (1968, "BOS", "Boston Celtics"),
    (1969, "NYK", "New York Knicks"),
    (1970, "MIL", "Milwaukee Bucks"),
    (1971, "LAL", "Los Angeles Lakers"),
    (1972, "NYK", "New York Knicks"),
    (1973, "BOS", "Boston Celtics"),
    (1974, "GSW", "Golden State Warriors"),
    (1975, "BOS", "Boston Celtics"),
    (1976, "POR", "Portland Trail Blazers"),
    (1977, "WAS", "Washington Bullets"),
    (1978, "OKC", "Seattle SuperSonics"),
    (1979, "LAL", "Los Angeles Lakers"),
    (1980, "BOS", "Boston Celtics"),
    (1981, "LAL", "Los Angeles Lakers"),
    (1982, "PHI", "Philadelphia 76ers"),
]


def _resolve_team_id(session, abbr_or_id: str) -> str | None:
    # Hardcoded numeric IDs (defunct franchises) bypass the abbr lookup
    if abbr_or_id.isdigit():
        return abbr_or_id
    team = session.query(Team).filter(Team.abbr == abbr_or_id, Team.active.is_(True)).first()
    return team.team_id if team else None


def run() -> None:
    session = Session()
    try:
        added = skipped = unresolved = 0
        for year, abbr_or_id, era_name in _CHAMPIONS:
            season_code = 20000 + year  # "2" prefix + year_start
            team_id = _resolve_team_id(session, abbr_or_id)
            if team_id is None:
                logger.warning("could not resolve team for %d %s", year, abbr_or_id)
                unresolved += 1
                continue

            existing = (
                session.query(Award)
                .filter(
                    Award.award_type == "champion",
                    Award.season == season_code,
                    Award.team_id == team_id,
                )
                .first()
            )
            if existing is not None:
                skipped += 1
                continue

            session.add(Award(
                award_type="champion",
                season=season_code,
                team_id=team_id,
                notes=f"Won as {era_name}" if era_name else None,
            ))
            added += 1

        session.commit()
        logger.info("done: added=%d skipped=%d unresolved=%d", added, skipped, unresolved)
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
