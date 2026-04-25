"""Backfill player salary history from Basketball Reference.

Usage:
    python -m db.backfill_player_salary
    python -m db.backfill_player_salary --season 2024
    python -m db.backfill_player_salary --all-seasons
"""
from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Comment
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import sessionmaker

from db.models import Player, PlayerSalary, engine


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)

BASE_URL = "https://www.basketball-reference.com"
PLAYER_CONTRACTS_URL = f"{BASE_URL}/contracts/players.html"
REQUEST_DELAY_SECONDS = 2.0
REQUEST_TIMEOUT_SECONDS = 30
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


def _current_nba_season_start_year(now: datetime | None = None) -> int:
    current = now or datetime.now()
    return current.year if current.month >= 7 else current.year - 1


CURRENT_SEASON = _current_nba_season_start_year()
DEFAULT_SEASON = CURRENT_SEASON


@dataclass(frozen=True)
class ContractPlayerEntry:
    full_name: str
    player_url: str
    current_season_salary: int | None = None


@dataclass(frozen=True)
class SalaryRecord:
    season: int
    salary_usd: int


@dataclass
class BackfillCounts:
    matched: int = 0
    unmatched: int = 0
    updated: int = 0
    errors: int = 0


class BasketballReferenceClient:
    def __init__(self, session: requests.Session | None = None, delay_seconds: float = REQUEST_DELAY_SECONDS):
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", USER_AGENT)
        self.delay_seconds = delay_seconds
        self._made_request = False

    def get_soup(self, url: str) -> BeautifulSoup:
        if self._made_request:
            time.sleep(self.delay_seconds)

        response = self.session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        self._made_request = True
        return BeautifulSoup(response.text, "html.parser")


def _extract_anchor(cell) -> tuple[str | None, str | None]:
    if cell is None:
        return None, None

    for anchor in reversed(cell.find_all("a", href=True)):
        href = str(anchor.get("href") or "").strip()
        text = anchor.get_text(" ", strip=True)
        if href.startswith("/players/") and text:
            return text, urljoin(BASE_URL, href)

    return None, None


def _find_table(soup: BeautifulSoup, table_id: str):
    table = soup.find("table", id=table_id)
    if table is not None:
        return table

    for comment in soup.find_all(string=lambda value: isinstance(value, Comment)):
        if table_id not in comment:
            continue
        commented = BeautifulSoup(comment, "html.parser")
        table = commented.find("table", id=table_id)
        if table is not None:
            return table

    return None


def _season_start_year(season_text: str | None) -> int | None:
    if not season_text:
        return None

    match = re.match(r"^\s*(\d{4})-\d{2}\s*$", season_text)
    if match is None:
        return None

    return int(match.group(1))


def _salary_value(cell) -> int | None:
    if cell is None:
        return None

    csk = str(cell.get("csk") or "").strip()
    if csk.isdigit():
        return int(csk)

    text = re.sub(r"[^\d]", "", cell.get_text(" ", strip=True))
    if not text:
        return None

    return int(text)


def _contract_salary_column(table) -> tuple[str | None, int | None]:
    thead = table.find("thead") if table is not None else None
    if thead is None:
        return None, None

    fallback: tuple[str | None, int | None] = (None, None)
    for header in thead.find_all(["th", "td"]):
        season = _season_start_year(header.get_text(" ", strip=True))
        data_stat = str(header.get("data-stat") or "").strip()
        if season is None or not data_stat:
            continue
        if fallback == (None, None):
            fallback = (data_stat, season)
        if season == CURRENT_SEASON:
            return data_stat, season

    return fallback


def _dedupe_salary_rows(salary_rows: Iterable[SalaryRecord]) -> list[SalaryRecord]:
    deduped: dict[int, SalaryRecord] = {}
    for row in salary_rows:
        deduped[row.season] = row
    return list(deduped.values())


def fetch_contract_players(client: BasketballReferenceClient) -> list[ContractPlayerEntry]:
    soup = client.get_soup(PLAYER_CONTRACTS_URL)
    table = _find_table(soup, "player-contracts")
    if table is None:
        logger.warning("Missing player contracts table on %s", PLAYER_CONTRACTS_URL)
        return []

    salary_data_stat, table_season = _contract_salary_column(table)
    seen_urls: set[str] = set()
    players: list[ContractPlayerEntry] = []
    tbody = table.find("tbody")
    if tbody is None:
        return players

    if table_season is not None and table_season != CURRENT_SEASON:
        logger.warning(
            "Contracts table current-season header is %s but scraper expects %s",
            table_season,
            CURRENT_SEASON,
        )

    for row in tbody.find_all("tr", recursive=False):
        classes = set(row.get("class", []))
        if "thead" in classes or "over_header" in classes:
            continue

        name, player_url = _extract_anchor(row.find("td", attrs={"data-stat": "player"}))
        if not name or not player_url or player_url in seen_urls:
            continue

        seen_urls.add(player_url)
        salary_cell = row.find("td", attrs={"data-stat": salary_data_stat}) if salary_data_stat else None
        players.append(
            ContractPlayerEntry(
                full_name=name,
                player_url=player_url,
                current_season_salary=_salary_value(salary_cell),
            )
        )

    return players


def fetch_salary_history(client: BasketballReferenceClient, player_url: str) -> list[SalaryRecord]:
    soup = client.get_soup(player_url)
    table = _find_table(soup, "all_salaries")
    if table is None:
        logger.warning("Missing salary table for %s", player_url)
        return []

    salary_rows: list[SalaryRecord] = []
    tbody = table.find("tbody")
    if tbody is None:
        return salary_rows

    for row in tbody.find_all("tr", recursive=False):
        classes = set(row.get("class", []))
        if "thead" in classes or "over_header" in classes:
            continue

        season_cell = row.find("th", attrs={"data-stat": "season"})
        salary_cell = row.find("td", attrs={"data-stat": "salary"})
        season = _season_start_year(season_cell.get_text(" ", strip=True) if season_cell else None)
        salary_usd = _salary_value(salary_cell)
        if season is None or salary_usd is None:
            continue

        salary_rows.append(SalaryRecord(season=season, salary_usd=salary_usd))

    return salary_rows


def _players_by_name(session) -> tuple[dict[str, Player], dict[str, Player]]:
    players = session.query(Player).filter(Player.full_name.isnot(None)).all()
    exact: dict[str, Player] = {}
    casefolded: dict[str, Player] = {}

    for player in players:
        name = (player.full_name or "").strip()
        if not name:
            continue
        exact.setdefault(name, player)
        casefolded.setdefault(name.casefold(), player)

    return exact, casefolded


def _upsert_salary_rows(session, player_id: str, salary_rows: Iterable[SalaryRecord]) -> int:
    rows = [
        {"player_id": player_id, "season": row.season, "salary_usd": row.salary_usd}
        for row in salary_rows
    ]
    if not rows:
        return 0

    table = PlayerSalary.__table__
    dialect_name = session.get_bind().dialect.name

    if dialect_name == "mysql":
        stmt = mysql_insert(table).values(rows)
        stmt = stmt.on_duplicate_key_update(salary_usd=stmt.inserted.salary_usd)
        session.execute(stmt)
        return len(rows)

    if dialect_name == "sqlite":
        stmt = sqlite_insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["player_id", "season"],
            set_={"salary_usd": stmt.excluded.salary_usd},
        )
        session.execute(stmt)
        return len(rows)

    updated = 0
    for row in rows:
        existing = (
            session.query(PlayerSalary)
            .filter(PlayerSalary.player_id == row["player_id"], PlayerSalary.season == row["season"])
            .first()
        )
        if existing is None:
            session.add(PlayerSalary(**row))
        else:
            existing.salary_usd = row["salary_usd"]
        updated += 1

    return updated


def run(season: int | None = DEFAULT_SEASON) -> BackfillCounts:
    counts = BackfillCounts()
    client = BasketballReferenceClient()
    session = Session()

    try:
        exact_players, casefold_players = _players_by_name(session)
        contract_players = fetch_contract_players(client)

        for entry in contract_players:
            player = exact_players.get(entry.full_name)
            if player is None:
                player = casefold_players.get(entry.full_name.casefold())

            if player is None:
                counts.unmatched += 1
                logger.warning("Unmatched salary row for '%s'", entry.full_name)
                continue

            try:
                salary_rows = fetch_salary_history(client, entry.player_url)
                if entry.current_season_salary is not None:
                    salary_rows.append(
                        SalaryRecord(season=CURRENT_SEASON, salary_usd=entry.current_season_salary)
                    )
                if season is not None:
                    salary_rows = [row for row in salary_rows if row.season == season]
                salary_rows = _dedupe_salary_rows(salary_rows)
                if not salary_rows:
                    continue

                counts.matched += 1
                counts.updated += _upsert_salary_rows(session, player.player_id, salary_rows)
                session.commit()
            except Exception as exc:
                session.rollback()
                counts.errors += 1
                logger.exception("Failed salary backfill for %s (%s): %s", entry.full_name, entry.player_url, exc)

        print(
            f"matched={counts.matched} unmatched={counts.unmatched} "
            f"updated={counts.updated} errors={counts.errors}"
        )
        return counts
    finally:
        session.close()


def run_from_db(skip_existing: bool = True) -> BackfillCounts:
    """Iterate every Player.br_slug and backfill that player's full salary
    history. Covers retired players (the BR /contracts/players.html page only
    lists currently-signed players, so the default `run()` skips everyone
    who's no longer under contract)."""
    counts = BackfillCounts()
    client = BasketballReferenceClient()
    session = Session()

    try:
        existing = set()
        if skip_existing:
            existing = {r[0] for r in session.query(PlayerSalary.player_id).distinct().all()}

        players = (
            session.query(Player)
            .filter(Player.br_slug.isnot(None))
            .order_by(Player.full_name)
            .all()
        )
        targets = [p for p in players if not skip_existing or p.player_id not in existing]
        logger.info("from-db backfill: %d candidate players (skip_existing=%s)", len(targets), skip_existing)

        for i, p in enumerate(targets, 1):
            slug = p.br_slug
            url = f"{BASE_URL}/players/{slug[0]}/{slug}.html"
            try:
                salary_rows = fetch_salary_history(client, url)
                salary_rows = _dedupe_salary_rows(salary_rows)
                if not salary_rows:
                    continue

                counts.matched += 1
                counts.updated += _upsert_salary_rows(session, p.player_id, salary_rows)
                session.commit()
                if i % 50 == 0:
                    logger.info("[%d/%d] %s -> %d salary rows (running totals: matched=%d updated=%d)",
                                i, len(targets), p.full_name, len(salary_rows),
                                counts.matched, counts.updated)
            except Exception as exc:
                session.rollback()
                counts.errors += 1
                logger.exception("Failed for %s (%s): %s", p.full_name, url, exc)

        print(f"from-db done: matched={counts.matched} updated={counts.updated} errors={counts.errors}")
        return counts
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill player salary history from Basketball Reference")
    # Default behavior is the full salary history (= --all-seasons): the
    # `all_salaries` table on each player page is cheap to grab and our
    # upsert is idempotent. The previous default of "current season only"
    # left long-tenured players with one-row histories, which made the
    # player-page chart fall back to AAV-distributed estimates for years
    # the player was actually paid for.
    parser.add_argument(
        "--season",
        type=int,
        default=None,
        help=(
            "Restrict to a single starting season year (e.g. 2024). "
            "Default: full career history for each matched player."
        ),
    )
    parser.add_argument(
        "--all-seasons",
        action="store_true",
        help="(Deprecated; this is now the default.) Backfill every salary season.",
    )
    parser.add_argument(
        "--from-db",
        action="store_true",
        help=(
            "Iterate every Player.br_slug in the DB instead of the BR contracts "
            "page (which only lists currently-signed players). Use this to backfill "
            "retired players' historical salaries."
        ),
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="With --from-db, also re-fetch players that already have any salary row.",
    )
    args = parser.parse_args()
    if args.from_db:
        run_from_db(skip_existing=not args.no_skip_existing)
    else:
        run(season=None if args.all_seasons else args.season)
