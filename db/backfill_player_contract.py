"""Backfill NBA player contracts from Spotrac.

Usage:
    python -m db.backfill_player_contract --limit 5      # sample run
    python -m db.backfill_player_contract                # full run
    python -m db.backfill_player_contract --player "Jayson Tatum"
"""
from __future__ import annotations

import argparse
import logging
import random
import re
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from db.models import Player, PlayerContract, PlayerContractYear, Team, engine


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)

BASE_URL = "https://www.spotrac.com"
CONTRACTS_INDEX_URL_FMT = f"{BASE_URL}/nba/contracts/_/year/{{year}}/limit/1000"
# Years to iterate for multi-year-contract discovery. 2019 is a safe floor:
# any currently-active deal was signed at most 5-6 years ago (rookie + 1 ext).
DISCOVERY_YEARS = range(2019, 2026)
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
REQUEST_TIMEOUT = 30
MIN_DELAY = 0.5
MAX_DELAY = 1.0


@dataclass
class IndexRow:
    name: str
    spotrac_url: str
    spotrac_id: int | None
    position: str | None
    team_abbr: str | None
    signed_at_age: int | None
    start_season: int | None
    end_season: int | None
    years: int | None
    total_value_usd: int | None
    aav_usd: int | None


@dataclass
class YearRow:
    season: int
    age: int | None = None
    status: str | None = None
    cap_hit_usd: int | None = None
    cash_annual_usd: int | None = None
    cash_guaranteed_usd: int | None = None
    base_salary_usd: int | None = None
    incentives_likely_usd: int | None = None
    incentives_unlikely_usd: int | None = None


@dataclass
class ContractInfo:
    start_season: int
    end_season: int
    years: int | None = None
    contract_type: str | None = None
    is_current: bool = False
    total_value_usd: int | None = None
    aav_usd: int | None = None
    guaranteed_usd: int | None = None
    guaranteed_at_sign_usd: int | None = None
    signed_using: str | None = None
    signed_with_team_abbr: str | None = None
    years_rows: list[YearRow] = field(default_factory=list)


@dataclass
class Counts:
    index_rows: int = 0
    matched_player: int = 0
    unmatched_player: int = 0
    contracts_upserted: int = 0
    years_upserted: int = 0
    errors: int = 0
    unmatched_names: list[str] = field(default_factory=list)


# ---------- HTTP ----------


class SpotracClient:
    def __init__(self):
        self._client = httpx.Client(
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                "Accept-Language": "en-US,en;q=0.9",
            },
            follow_redirects=True,
            timeout=REQUEST_TIMEOUT,
        )
        self._first_request = True

    def close(self):
        self._client.close()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((httpx.HTTPError,)),
        reraise=True,
    )
    def _get(self, url: str) -> httpx.Response:
        if not self._first_request:
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        self._first_request = False
        r = self._client.get(url)
        r.raise_for_status()
        return r

    def get_soup(self, url: str) -> BeautifulSoup:
        r = self._get(url)
        return BeautifulSoup(r.text, "html.parser")


# ---------- Parse helpers ----------


_MONEY_RE = re.compile(r"-?\$\s*([\d,]+)")
_BARE_NUM_RE = re.compile(r"-?([\d,]+)")
_SPOTRAC_ID_RE = re.compile(r"/id/(\d+)/")
_SEASON_RE = re.compile(r"(\d{4})")


def _money(text: str | None) -> int | None:
    if not text:
        return None
    text = text.strip()
    if text in ("-", "", "—"):
        return None
    # Prefer an explicit $-prefixed value; only fall back to a bare number
    # when the text looks purely numeric (no mixed tokens like "5 yr(s)").
    m = _MONEY_RE.search(text)
    if m is None:
        if re.fullmatch(r"[\s\-\$]*[\d,]+[\s\-\$]*", text):
            m = _BARE_NUM_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _int(text: str | None) -> int | None:
    if not text:
        return None
    text = text.strip()
    if not text or text == "-":
        return None
    try:
        return int(text)
    except ValueError:
        m = re.search(r"-?\d+", text)
        return int(m.group(0)) if m else None


def _season_start_from_range(text: str | None) -> int | None:
    """Parse '2025-26' -> 2025, or '2025' -> 2025."""
    if not text:
        return None
    m = _SEASON_RE.search(text)
    return int(m.group(1)) if m else None


def _spotrac_id(url: str) -> int | None:
    m = _SPOTRAC_ID_RE.search(url)
    return int(m.group(1)) if m else None


# ---------- Index page ----------


def parse_contracts_index(soup: BeautifulSoup) -> list[IndexRow]:
    tables = soup.find_all("table")
    if not tables:
        return []
    table = tables[0]
    rows: list[IndexRow] = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 9:
            continue
        player_cell = tds[0]
        anchor = player_cell.find("a", href=True)
        if not anchor:
            continue
        name = anchor.get_text(" ", strip=True)
        href = anchor["href"]
        spotrac_url = href if href.startswith("http") else urljoin(BASE_URL, href)
        team_abbr = None
        team_cell_text = tds[2].get_text(" ", strip=True)
        if team_cell_text:
            # "BOS BOS" or "BOS" — split and take first token
            team_abbr = team_cell_text.split()[0]
        rows.append(
            IndexRow(
                name=name,
                spotrac_url=spotrac_url,
                spotrac_id=_spotrac_id(spotrac_url),
                position=tds[1].get_text(" ", strip=True) or None,
                team_abbr=team_abbr,
                signed_at_age=_int(tds[3].get_text(" ", strip=True)),
                start_season=_season_start_from_range(tds[4].get_text(" ", strip=True)),
                end_season=_season_start_from_range(tds[5].get_text(" ", strip=True)),
                years=_int(tds[6].get_text(" ", strip=True)),
                total_value_usd=_money(tds[7].get_text(" ", strip=True)),
                aav_usd=_money(tds[8].get_text(" ", strip=True)),
            )
        )
    return rows


# ---------- Player page ----------


_OPTION_CLASS_MAP = {
    "option-player": "Player Option",
    "option-team": "Team Option",
    "option-nonguar": "Non-Guaranteed",
    "option-non-guar": "Non-Guaranteed",
    "option-partial": "Partial Guarantee",
    "option-eto": "Early Termination Option",
    "option-ufa": "UFA",
}


def _parse_status_cell(td) -> str | None:
    """Extract contract status from a Spotrac Status <td>. Reads the div's option class."""
    if td is None:
        return None
    div = td.find("div", class_=lambda c: bool(c) and "option" in (c if isinstance(c, str) else " ".join(c)))
    if div is None:
        text = td.get_text(" ", strip=True)
        if not text or text in ("-", "—"):
            return None
        return text
    classes = div.get("class") or []
    for cls in classes:
        if cls in _OPTION_CLASS_MAP:
            return _OPTION_CLASS_MAP[cls]
    text = div.get_text(" ", strip=True)
    if not text or text in ("-", "—"):
        return None
    return text


def _table_rows_with_headers(table) -> tuple[list[str], list[list[str]], list[list]]:
    headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
    rows_text: list[list[str]] = []
    rows_tds: list[list] = []
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if not tds:
            continue
        rows_text.append([td.get_text(" ", strip=True) for td in tds])
        rows_tds.append(tds)
    return headers, rows_text, rows_tds


def _find_header_idx(headers: list[str], *needles: str) -> int | None:
    """Find first header matching all needles (case-insensitive substring)."""
    for i, h in enumerate(headers):
        hl = h.lower()
        if all(n.lower() in hl for n in needles):
            return i
    return None


_H2_CONTRACT_RE = re.compile(r"(\d{4})\s*-\s*(\d{4})\s*(.*?)\s*(\(CURRENT\))?\s*$")
# Spotrac uses two logo filename conventions:
#   "nba_<code>.png" (or "nba_<code><digit>.png" like nba_cle1.png) — modern
#   "<code>_<year>.png" (e.g. orl_20251.png) — older year-suffixed era logos
_TEAM_LOGO_RE = re.compile(r"nba_([a-z]+)\d*\.png", re.I)
_TEAM_LOGO_RE_LEGACY = re.compile(r"/([a-z]+)_\d+\.png", re.I)
_YEARS_TERMS_RE = re.compile(r"(\d+)\s*yr", re.I)

# Spotrac uses its own short codes which don't always line up with NBA abbr.
# Map each code Spotrac actually serves -> the canonical Team.abbr in our DB.
# Legacy franchises are collapsed to their current canonical team.
_SPOTRAC_TO_ABBR = {
    # atlantic
    "bos": "BOS", "bkn": "BKN", "nj": "BKN", "njn": "BKN",
    "nyk": "NYK", "ny": "NYK",
    "phi": "PHI", "tor": "TOR",
    # central
    "chi": "CHI", "cle": "CLE", "det": "DET", "ind": "IND", "mil": "MIL",
    # southeast
    "atl": "ATL",
    "cha": "CHA", "chh": "CHA", "bob": "CHA",
    "mia": "MIA", "orl": "ORL",
    "was": "WAS", "wsh": "WAS",
    # northwest
    "den": "DEN", "min": "MIN", "por": "POR",
    "okc": "OKC", "sea": "OKC",
    "uta": "UTA", "utah": "UTA",
    # pacific
    "gs": "GSW", "gsw": "GSW",
    "lac": "LAC", "lal": "LAL", "phx": "PHX", "pho": "PHX", "sac": "SAC",
    # southwest
    "dal": "DAL", "hou": "HOU",
    "mem": "MEM", "van": "MEM",
    "nop": "NOP", "nor": "NOP", "nok": "NOP", "no": "NOP",
    "sas": "SAS", "sa": "SAS",
}


def _parse_wrapper_metadata(wrapper) -> dict[str, str]:
    """Parse the label/value cells in .contract-details into a dict."""
    out: dict[str, str] = {}
    details = wrapper.find("div", class_="contract-details")
    if details is None:
        return out
    for cell in details.find_all("div", class_="cell"):
        label_el = cell.find("div", class_="label")
        value_el = cell.find("div", class_="value")
        if label_el and value_el:
            out[label_el.get_text(" ", strip=True).rstrip(":").strip()] = value_el.get_text(" ", strip=True)
    return out


def _parse_contract_header(wrapper) -> tuple[int | None, int | None, str | None, bool, str | None]:
    """Parse the h2 of a contract-wrapper -> (start, end, type_label, is_current, team_abbr)."""
    h2 = wrapper.find("h2")
    if h2 is None:
        return None, None, None, False, None

    # Team abbr from the logo image (try modern then legacy filename pattern)
    team_abbr = None
    img = h2.find("img", src=True)
    if img is not None:
        src = img["src"]
        m = _TEAM_LOGO_RE.search(src) or _TEAM_LOGO_RE_LEGACY.search(src)
        if m:
            team_abbr = m.group(1).upper()

    # Year range + type label are in <span class="years">
    span = h2.find("span", class_="years")
    text = span.get_text(" ", strip=True) if span else h2.get_text(" ", strip=True)

    m = _H2_CONTRACT_RE.search(text)
    if not m:
        return None, None, None, False, team_abbr
    start = int(m.group(1))
    end = int(m.group(2))
    # Spotrac occasionally renders typoed h2 like "2016-22016 ten-day" where
    # the two adjacent numbers are concatenated. Clamp impossible ranges to a
    # single-season deal — there are no NBA contracts longer than 7 years.
    if end < start or end - start > 8:
        end = start
    type_label = (m.group(3) or "").strip() or None
    is_current = bool(m.group(4))
    return start, end, type_label, is_current, team_abbr


def _parse_current_contract_tables(wrapper, start_season: int, end_season: int) -> dict[int, YearRow]:
    """For the current contract, parse cap-hit/base/incentives tables inside the wrapper."""
    years_by_season: dict[int, YearRow] = {}
    for table in wrapper.find_all("table"):
        headers, rows_text, rows_tds = _table_rows_with_headers(table)
        year_idx = _find_header_idx(headers, "year")
        if year_idx is None:
            continue

        age_idx = _find_header_idx(headers, "age")
        status_idx = _find_header_idx(headers, "status")
        cap_hit_idx = _find_header_idx(headers, "cap hit")
        cash_annual_idx = _find_header_idx(headers, "cash annual")
        cash_guar_idx = _find_header_idx(headers, "cash guaranteed")
        base_idx = _find_header_idx(headers, "base salary")
        inc_likely_idx = _find_header_idx(headers, "incentives likely")
        inc_unlikely_idx = _find_header_idx(headers, "incentives unlikely")

        for cells, tds in zip(rows_text, rows_tds):
            if year_idx >= len(cells):
                continue
            season = _season_start_from_range(cells[year_idx])
            if season is None or season < start_season or season > end_season:
                continue
            yr = years_by_season.setdefault(season, YearRow(season=season))
            if age_idx is not None and age_idx < len(cells) and yr.age is None:
                yr.age = _int(cells[age_idx])
            if status_idx is not None and status_idx < len(tds) and not yr.status:
                sv = _parse_status_cell(tds[status_idx])
                if sv:
                    yr.status = sv
            if cap_hit_idx is not None and cap_hit_idx < len(cells) and yr.cap_hit_usd is None:
                yr.cap_hit_usd = _money(cells[cap_hit_idx])
            if cash_annual_idx is not None and cash_annual_idx < len(cells) and yr.cash_annual_usd is None:
                yr.cash_annual_usd = _money(cells[cash_annual_idx])
            if cash_guar_idx is not None and cash_guar_idx < len(cells) and yr.cash_guaranteed_usd is None:
                yr.cash_guaranteed_usd = _money(cells[cash_guar_idx])
            if base_idx is not None and base_idx < len(cells) and yr.base_salary_usd is None:
                yr.base_salary_usd = _money(cells[base_idx])
            if inc_likely_idx is not None and inc_likely_idx < len(cells) and yr.incentives_likely_usd is None:
                yr.incentives_likely_usd = _money(cells[inc_likely_idx])
            if inc_unlikely_idx is not None and inc_unlikely_idx < len(cells) and yr.incentives_unlikely_usd is None:
                yr.incentives_unlikely_usd = _money(cells[inc_unlikely_idx])
    return years_by_season


def _parse_career_earnings(soup: BeautifulSoup) -> dict[int, dict]:
    """Find the 'Earnings Per Year' table and return {season: {cash_annual, age, team_abbr}}."""
    out: dict[int, dict] = {}
    banner = None
    for h in soup.find_all(["h2", "h3", "h4"]):
        if h.get_text(" ", strip=True).strip().lower() == "earnings per year":
            banner = h
            break
    if banner is None:
        return out
    table = banner.find_next("table")
    if table is None:
        return out
    headers, rows_text, rows_tds = _table_rows_with_headers(table)
    year_idx = _find_header_idx(headers, "year")
    age_idx = _find_header_idx(headers, "age")
    team_idx = _find_header_idx(headers, "team")
    cash_idx = _find_header_idx(headers, "cash total")
    if year_idx is None or cash_idx is None:
        return out
    for cells, tds in zip(rows_text, rows_tds):
        if year_idx >= len(cells):
            continue
        season = _season_start_from_range(cells[year_idx])
        if season is None:
            continue
        team_abbr = None
        if team_idx is not None and team_idx < len(tds):
            img = tds[team_idx].find("img", src=True)
            if img is not None:
                src = img["src"]
                m = _TEAM_LOGO_RE.search(src) or _TEAM_LOGO_RE_LEGACY.search(src)
                if m:
                    team_abbr = m.group(1).upper()
            if not team_abbr:
                t = cells[team_idx].strip()
                if t and t != "-":
                    team_abbr = t.split()[0]
        out[season] = {
            "cash_annual_usd": _money(cells[cash_idx]) if cash_idx < len(cells) else None,
            "age": _int(cells[age_idx]) if age_idx is not None and age_idx < len(cells) else None,
            "team_abbr": team_abbr,
        }
    return out


def parse_player_contracts(soup: BeautifulSoup) -> list[ContractInfo]:
    """Parse all contract-wrapper divs on a Spotrac player page."""
    wrappers = soup.find_all("div", class_="contract-wrapper")
    if not wrappers:
        return []

    career_earnings = _parse_career_earnings(soup)
    contracts: list[ContractInfo] = []

    for w in wrappers:
        start, end, type_label, is_current, team_abbr = _parse_contract_header(w)
        if start is None or end is None:
            continue
        meta = _parse_wrapper_metadata(w)

        years = None
        total_value = None
        terms = meta.get("Contract Terms")
        if terms:
            ym = _YEARS_TERMS_RE.search(terms)
            if ym:
                years = int(ym.group(1))
            total_value = _money(terms)
        if years is None:
            years = end - start + 1

        info = ContractInfo(
            start_season=start,
            end_season=end,
            years=years,
            contract_type=type_label,
            is_current=is_current,
            total_value_usd=total_value,
            aav_usd=_money(meta.get("Average Salary")),
            guaranteed_usd=_money(meta.get("Total GTD")),
            guaranteed_at_sign_usd=_money(meta.get("GTD at Sign")),
            signed_using=(meta.get("Signed Using") or None),
            signed_with_team_abbr=team_abbr,
        )

        year_dict: dict[int, YearRow] = {}
        if is_current:
            year_dict = _parse_current_contract_tables(w, start, end)

        for season in range(start, end + 1):
            yr = year_dict.get(season) or YearRow(season=season)
            earnings = career_earnings.get(season)
            if earnings:
                if yr.cash_annual_usd is None:
                    yr.cash_annual_usd = earnings.get("cash_annual_usd")
                if yr.age is None:
                    yr.age = earnings.get("age")
            year_dict[season] = yr

        info.years_rows = [year_dict[s] for s in sorted(year_dict)]
        contracts.append(info)

    # Trim overlapping end-seasons: when a newer contract starts at/before an
    # older one's listed end, the older deal was superseded. Cap end_season to
    # (next.start_season - 1) and drop years outside the new range.
    contracts.sort(key=lambda c: c.start_season)
    for i, c in enumerate(contracts[:-1]):
        nxt = contracts[i + 1]
        if nxt.start_season <= c.end_season:
            new_end = nxt.start_season - 1
            if new_end < c.start_season:
                # pathological case: keep original
                continue
            c.end_season = new_end
            c.years_rows = [y for y in c.years_rows if y.season <= new_end]
            if c.years:
                c.years = c.end_season - c.start_season + 1

    return contracts


# ---------- DB helpers ----------


_NICKNAME_ALIASES = {
    "herb jones": "herbert jones",
    "nicolas claxton": "nic claxton",
    "ron holland": "ronald holland",
    "sviatoslav mykhailiuk": "svi mykhailiuk",
    "cameron christie": "cam christie",
    "nahshon hyland": "bones hyland",
}


def _normalize_name(name: str) -> str:
    """Fold diacritics, drop punctuation, collapse whitespace — for fuzzy matches only."""
    if not name:
        return ""
    # NFKD splits "ö" into "o" + combining diaeresis; drop the combining marks.
    folded = unicodedata.normalize("NFKD", name)
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    folded = re.sub(r"[.'’`]+", "", folded)
    folded = folded.replace("-", " ")
    folded = re.sub(r"\s+", " ", folded).strip().casefold()
    # Drop trailing Jr/Sr/II/III/IV suffixes so "Bruce Brown Jr." == "Bruce Brown".
    folded = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", "", folded).strip()
    return _NICKNAME_ALIASES.get(folded, folded)


def _players_by_name(session) -> tuple[dict[str, Player], dict[str, Player], dict[str, Player]]:
    players = session.query(Player).filter(Player.full_name.isnot(None)).all()
    exact: dict[str, Player] = {}
    casefold: dict[str, Player] = {}
    normalized: dict[str, Player] = {}
    for p in players:
        name = (p.full_name or "").strip()
        if not name:
            continue
        exact.setdefault(name, p)
        casefold.setdefault(name.casefold(), p)
        norm = _normalize_name(name)
        if norm:
            normalized.setdefault(norm, p)
    return exact, casefold, normalized


def _teams_by_abbr(session) -> dict[str, str]:
    """Map both our Team.abbr and Spotrac-style codes (including legacy) to team_id."""
    canonical: dict[str, str] = {}
    for t in session.query(Team).filter(Team.active.is_(True), Team.is_legacy.is_(False)).all():
        if t.abbr:
            canonical[t.abbr.upper()] = t.team_id
    out = dict(canonical)
    for spotrac_code, canonical_abbr in _SPOTRAC_TO_ABBR.items():
        team_id = canonical.get(canonical_abbr.upper())
        if team_id:
            out[spotrac_code.upper()] = team_id
    return out


def _upsert_contract(
    session,
    player_id: str,
    info: ContractInfo,
    *,
    spotrac_id: int | None,
    source_url: str | None,
    team_by_abbr: dict[str, str],
    signed_at_age: int | None = None,
) -> PlayerContract:
    existing = (
        session.query(PlayerContract)
        .filter(
            PlayerContract.player_id == player_id,
            PlayerContract.start_season == info.start_season,
            PlayerContract.end_season == info.end_season,
        )
        .first()
    )
    team_id = team_by_abbr.get(info.signed_with_team_abbr.upper()) if info.signed_with_team_abbr else None
    payload = dict(
        player_id=player_id,
        spotrac_id=spotrac_id,
        signed_with_team_id=team_id,
        signed_at_age=signed_at_age,
        start_season=info.start_season,
        end_season=info.end_season,
        years=info.years or (info.end_season - info.start_season + 1),
        total_value_usd=info.total_value_usd,
        aav_usd=info.aav_usd,
        guaranteed_usd=info.guaranteed_usd,
        guaranteed_at_sign_usd=info.guaranteed_at_sign_usd,
        contract_type=info.contract_type,
        signed_using=info.signed_using,
        source_url=source_url,
        scraped_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    if existing is None:
        contract = PlayerContract(**payload)
        session.add(contract)
        session.flush()
        return contract
    for k, v in payload.items():
        setattr(existing, k, v)
    session.flush()
    return existing


def _upsert_contract_years(session, contract_id: int, player_id: str, years: Iterable[YearRow]) -> int:
    dialect = session.get_bind().dialect.name
    rows = [
        dict(
            contract_id=contract_id,
            player_id=player_id,
            season=y.season,
            age=y.age,
            status=y.status,
            cap_hit_usd=y.cap_hit_usd,
            base_salary_usd=y.base_salary_usd,
            incentives_likely_usd=y.incentives_likely_usd,
            incentives_unlikely_usd=y.incentives_unlikely_usd,
            cash_guaranteed_usd=y.cash_guaranteed_usd,
            cash_annual_usd=y.cash_annual_usd,
        )
        for y in years
    ]
    if not rows:
        return 0
    if dialect == "mysql":
        table = PlayerContractYear.__table__
        stmt = mysql_insert(table).values(rows)
        stmt = stmt.on_duplicate_key_update(
            age=stmt.inserted.age,
            status=stmt.inserted.status,
            cap_hit_usd=stmt.inserted.cap_hit_usd,
            base_salary_usd=stmt.inserted.base_salary_usd,
            incentives_likely_usd=stmt.inserted.incentives_likely_usd,
            incentives_unlikely_usd=stmt.inserted.incentives_unlikely_usd,
            cash_guaranteed_usd=stmt.inserted.cash_guaranteed_usd,
            cash_annual_usd=stmt.inserted.cash_annual_usd,
        )
        session.execute(stmt)
        return len(rows)
    # fallback
    for r in rows:
        existing = (
            session.query(PlayerContractYear)
            .filter(
                PlayerContractYear.contract_id == r["contract_id"],
                PlayerContractYear.season == r["season"],
            )
            .first()
        )
        if existing is None:
            session.add(PlayerContractYear(**r))
        else:
            for k, v in r.items():
                setattr(existing, k, v)
    return len(rows)


# ---------- Main ----------


def run(limit: int | None = None, player_filter: str | None = None) -> Counts:
    counts = Counts()
    client = SpotracClient()
    session = Session()

    try:
        # Aggregate contract rows across multiple signing-year indexes.
        # Spotrac's /nba/contracts/_/year/Y lists deals whose start_season == Y,
        # so one year alone misses every active multi-year deal signed earlier.
        rows_by_url: dict[str, list[IndexRow]] = {}
        for year in DISCOVERY_YEARS:
            url = CONTRACTS_INDEX_URL_FMT.format(year=year)
            logger.info("Fetching contracts index year=%d", year)
            soup = client.get_soup(url)
            year_rows = parse_contracts_index(soup)
            logger.info("  year=%d parsed %d rows", year, len(year_rows))
            for r in year_rows:
                rows_by_url.setdefault(r.spotrac_url, []).append(r)

        counts.index_rows = sum(len(v) for v in rows_by_url.values())
        logger.info(
            "Discovery complete: %d unique player URLs, %d total contract rows",
            len(rows_by_url),
            counts.index_rows,
        )

        urls = list(rows_by_url.keys())
        if player_filter:
            pf = player_filter.casefold()
            urls = [u for u in urls if any(pf in r.name.casefold() for r in rows_by_url[u])]
            logger.info("Filtered to %d URLs matching %r", len(urls), player_filter)
        if limit is not None:
            urls = urls[:limit]
            logger.info("Limited to first %d URLs", limit)

        exact_players, casefold_players, normalized_players = _players_by_name(session)
        team_by_abbr = _teams_by_abbr(session)

        for i, url in enumerate(urls, 1):
            rows = rows_by_url[url]
            primary = rows[0]
            player = (
                exact_players.get(primary.name)
                or casefold_players.get(primary.name.casefold())
                or normalized_players.get(_normalize_name(primary.name))
            )
            if player is None:
                counts.unmatched_player += 1
                counts.unmatched_names.append(primary.name)
                logger.warning("[%d/%d] unmatched: %s", i, len(urls), primary.name)
                continue
            counts.matched_player += 1

            # Build start_season -> signed_at_age lookup for this player
            age_by_start = {r.start_season: r.signed_at_age for r in rows if r.start_season is not None}

            try:
                player_soup = client.get_soup(url)
                contracts = parse_player_contracts(player_soup)
                logger.info(
                    "[%d/%d] %s (%s) -> %d contracts",
                    i,
                    len(urls),
                    primary.name,
                    player.player_id,
                    len(contracts),
                )
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
                    n_years = _upsert_contract_years(session, contract.id, player.player_id, info.years_rows)
                    counts.contracts_upserted += 1
                    counts.years_upserted += n_years
                session.commit()
            except Exception as exc:
                session.rollback()
                counts.errors += 1
                logger.exception("Failed contract for %s (%s): %s", primary.name, url, exc)

        logger.info(
            "Done. index_rows=%d matched=%d unmatched=%d contracts=%d years=%d errors=%d",
            counts.index_rows,
            counts.matched_player,
            counts.unmatched_player,
            counts.contracts_upserted,
            counts.years_upserted,
            counts.errors,
        )
        if counts.unmatched_names:
            logger.info("Unmatched names (first 20): %s", counts.unmatched_names[:20])
        return counts
    finally:
        client.close()
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill NBA player contracts from Spotrac")
    parser.add_argument("--limit", type=int, help="Only process first N rows from the index")
    parser.add_argument("--player", type=str, help="Only process players whose name contains this")
    args = parser.parse_args()
    run(limit=args.limit, player_filter=args.player)


if __name__ == "__main__":
    main()
