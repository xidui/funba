"""Backfill TeamRosterStint + TeamCoachStint.

Two phases:

Phase 1 (SQL only, no API):
  Derive historical TeamRosterStint rows from PlayerGameStats using
  run-length encoding on team_id over game_date per player. Every player
  who ever played a game gets at least one stint; players with trades get
  multiple stints. left_at = last game date of that stint (left_at = NULL
  for a stint that is still active in the current season).

Phase 2 (API, ~1789 commonteamroster calls):
  For every (team_id, start_year) that actually played games in our DB,
  call commonteamroster once to get:
    - coach staff (DataFrame 1) → TeamCoachStint rows, merged across
      consecutive seasons for the same coach on the same team.
    - final roster (DataFrame 0) → TeamRosterStint rows ONLY for players
      that never appear in PlayerGameStats for that (team, season) —
      those are the "rostered but never played" edge cases. Source is
      'roster_snapshot', joined_at/left_at span the season. Jersey,
      position, and how_acquired from the snapshot are also written back
      onto game-derived stints that overlap the same season.

Usage:
    python -m db.backfill_team_roster                # both phases, all seasons
    python -m db.backfill_team_roster --skip-api     # phase 1 only
    python -m db.backfill_team_roster --seasons 2020-2025
    python -m db.backfill_team_roster --dry-run      # print counts, no writes
    python -m db.backfill_team_roster --current-only # only current start-year

Output: progress logs + final counts. Idempotent — wipes TeamRosterStint
and TeamCoachStint at start unless --incremental is passed (the daily
sync script uses a different code path; this script is for one-shot
backfill).
"""
from __future__ import annotations

import argparse
import logging
import time
from collections import defaultdict
from datetime import date, datetime
from typing import Iterable

from nba_api.stats.endpoints import commonteamroster
from requests.exceptions import ConnectionError, Timeout
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from db.models import TeamCoachStint, TeamRosterStint, engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

Session = sessionmaker(bind=engine)


def _start_year(season_code: str) -> int | None:
    """Season code is 5 digits, e.g. '22025' = start year 2025."""
    s = str(season_code or "").strip()
    if len(s) == 5 and s.isdigit():
        return int(s[1:])
    return None


def _nba_season_str(start_year: int) -> str:
    """start_year=2025 → '2025-26'."""
    return f"{start_year}-{str((start_year + 1) % 100).zfill(2)}"


def _current_start_year() -> int:
    today = date.today()
    return today.year if today.month >= 7 else today.year - 1


# ─────────────────────────────────────────────────────────────────────────
# Phase 1 — derive historical stints from PlayerGameStats
# ─────────────────────────────────────────────────────────────────────────

def derive_stints_from_games(session) -> tuple[int, dict]:
    """Run-length encode (team_id, game_date) per player across all games.

    Returns (num_stints_inserted, per_player_stint_counts_dict).
    """
    logger.info("Phase 1: deriving stints from PlayerGameStats…")
    rows = session.execute(text("""
        SELECT pgs.player_id, pgs.team_id, g.game_date
        FROM PlayerGameStats pgs
        JOIN Game g ON g.game_id = pgs.game_id
        WHERE g.game_date IS NOT NULL
        ORDER BY pgs.player_id, g.game_date, pgs.team_id
    """)).fetchall()
    logger.info("  loaded %d player-game rows", len(rows))

    current_start_year = _current_start_year()
    current_season_start = date(current_start_year, 7, 1)

    stints: list[dict] = []
    per_player_last_stint_index: dict[str, int] = {}

    current_player = None
    current_team = None
    current_joined: date | None = None
    current_last: date | None = None

    def _flush():
        nonlocal current_player, current_team, current_joined, current_last
        if current_player is None:
            return
        stints.append({
            "player_id": current_player,
            "team_id": current_team,
            "joined_at": current_joined,
            "left_at": current_last,
        })
        per_player_last_stint_index[current_player] = len(stints) - 1

    for player_id, team_id, game_date in rows:
        if (
            current_player == player_id
            and current_team == team_id
        ):
            current_last = game_date
            continue
        _flush()
        current_player = player_id
        current_team = team_id
        current_joined = game_date
        current_last = game_date
    _flush()

    # For each player's LAST stint, if the last game is within the current
    # season window, leave left_at = NULL (still active). Daily sync will
    # take over from there.
    for idx in per_player_last_stint_index.values():
        if stints[idx]["left_at"] >= current_season_start:
            stints[idx]["left_at"] = None

    logger.info("  derived %d stints across %d players",
                len(stints), len(per_player_last_stint_index))
    return stints


def insert_derived_stints(session, stints: list[dict]) -> int:
    now = datetime.utcnow()
    rows = [
        {
            "team_id": s["team_id"],
            "player_id": s["player_id"],
            "joined_at": s["joined_at"],
            "left_at": s["left_at"],
            "source": "game_derived",
            "created_at": now,
            "updated_at": now,
        }
        for s in stints
    ]
    if not rows:
        return 0
    # Batch insert
    batch_size = 2000
    inserted = 0
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        session.execute(
            text("""
                INSERT INTO TeamRosterStint
                  (team_id, player_id, joined_at, left_at, source, created_at, updated_at)
                VALUES
                  (:team_id, :player_id, :joined_at, :left_at, :source, :created_at, :updated_at)
            """),
            chunk,
        )
        inserted += len(chunk)
        if inserted % 10000 == 0:
            logger.info("  inserted %d / %d", inserted, len(rows))
    session.commit()
    return inserted


# ─────────────────────────────────────────────────────────────────────────
# Phase 2 — commonteamroster per (team, season)
# ─────────────────────────────────────────────────────────────────────────


@retry(
    wait=wait_exponential(multiplier=1, max=60),
    stop=stop_after_attempt(6),
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def _fetch_roster(team_id: int, season_str: str):
    r = commonteamroster.CommonTeamRoster(team_id=team_id, season=season_str, timeout=20)
    dfs = r.get_data_frames()
    return dfs[0], dfs[1] if len(dfs) > 1 else None


def _season_bounds(session, team_id: str, start_year: int) -> tuple[date | None, date | None]:
    row = session.execute(text("""
        SELECT MIN(g.game_date) AS lo, MAX(g.game_date) AS hi
        FROM Game g
        WHERE (g.home_team_id = :tid OR g.road_team_id = :tid)
          AND CAST(SUBSTRING(CAST(g.season AS CHAR), 2) AS UNSIGNED) = :yr
    """), {"tid": team_id, "yr": start_year}).first()
    if row is None:
        return None, None
    return row.lo, row.hi


def _team_season_pairs(session) -> list[tuple[str, int]]:
    rows = session.execute(text("""
        SELECT DISTINCT t.team_id, CAST(SUBSTRING(CAST(s.season AS CHAR), 2) AS UNSIGNED) AS yr
        FROM (
            SELECT season, home_team_id AS team_id FROM Game
            UNION
            SELECT season, road_team_id AS team_id FROM Game
        ) s
        JOIN Team t ON t.team_id = s.team_id
        WHERE s.team_id IS NOT NULL
        ORDER BY yr, t.team_id
    """)).fetchall()
    return [(r.team_id, int(r.yr)) for r in rows]


def fetch_rosters_and_write_stints(
    session,
    pairs: list[tuple[str, int]],
    *,
    sleep_secs: float = 0.6,
) -> dict:
    """Iterate (team_id, start_year), call commonteamroster, emit stints.

    Coach stints: one row per (team, coach) merged across consecutive
    seasons. Extra roster stints: inserted only for players who don't
    appear in PlayerGameStats for that (team, season).
    """
    logger.info("Phase 2: %d commonteamroster calls", len(pairs))

    # Precompute which (team_id, start_year, player_id) triples already
    # have a game-derived stint (so we skip duplicates).
    logger.info("  loading existing derived (team, season, player) tuples…")
    derived_rows = session.execute(text("""
        SELECT DISTINCT pgs.team_id,
               CAST(SUBSTRING(CAST(g.season AS CHAR), 2) AS UNSIGNED) AS yr,
               pgs.player_id
        FROM PlayerGameStats pgs
        JOIN Game g ON g.game_id = pgs.game_id
    """)).fetchall()
    derived_set: set[tuple[str, int, str]] = {
        (r.team_id, int(r.yr), r.player_id) for r in derived_rows
    }
    logger.info("  %d game-derived (team, season, player) triples",
                len(derived_set))

    # In-memory coach accumulators: (team_id, coach_id) → current stint dict.
    # We process pairs sorted by (team_id, start_year) so consecutive-season
    # merging works naturally.
    pairs_sorted = sorted(pairs, key=lambda p: (p[0], p[1]))
    coach_open: dict[tuple[str, str], dict] = {}
    coach_stints_closed: list[dict] = []
    extra_player_stints: list[dict] = []

    counts = {"api_calls": 0, "api_failures": 0, "extra_roster_stints": 0, "coach_rows": 0}

    prev_team = None
    for idx, (team_id, start_year) in enumerate(pairs_sorted, 1):
        if prev_team is not None and team_id != prev_team:
            # Flush any open coach stints for the previous team.
            for key, st in list(coach_open.items()):
                if key[0] == prev_team:
                    coach_stints_closed.append(st)
                    del coach_open[key]
        prev_team = team_id

        season_str = _nba_season_str(start_year)
        lo, hi = _season_bounds(session, team_id, start_year)
        if lo is None or hi is None:
            # No games recorded; use crude Oct-June default.
            lo = date(start_year, 10, 1)
            hi = date(start_year + 1, 6, 30)

        try:
            try:
                tid_int = int(team_id)
            except (TypeError, ValueError):
                logger.warning("skip non-int team_id %s", team_id)
                continue
            players_df, coaches_df = _fetch_roster(tid_int, season_str)
            counts["api_calls"] += 1
        except Exception as exc:
            counts["api_failures"] += 1
            logger.warning("  API fail %s %s: %s", team_id, season_str, exc)
            time.sleep(sleep_secs)
            continue

        # Players → add stints for those NOT in game-derived set
        if players_df is not None and len(players_df) > 0:
            for _, row in players_df.iterrows():
                pid = str(row.get("PLAYER_ID"))
                if not pid:
                    continue
                triple = (team_id, start_year, pid)
                if triple in derived_set:
                    continue  # already have a stint from game data
                extra_player_stints.append({
                    "team_id": team_id,
                    "player_id": pid,
                    "joined_at": lo,
                    "left_at": hi,
                    "jersey": str(row.get("NUM") or "")[:10] or None,
                    "position": str(row.get("POSITION") or "")[:30] or None,
                    "how_acquired": str(row.get("HOW_ACQUIRED") or "")[:255] or None,
                    "source": "roster_snapshot",
                })

        # Coaches → merge by (team, coach_id) across consecutive years
        if coaches_df is not None and len(coaches_df) > 0:
            seen_coaches_this_season: set[str] = set()
            for _, row in coaches_df.iterrows():
                cid = str(row.get("COACH_ID") or "")
                if not cid:
                    continue
                seen_coaches_this_season.add(cid)
                key = (team_id, cid)
                if key in coach_open:
                    coach_open[key]["left_at"] = hi  # extend
                else:
                    coach_open[key] = {
                        "team_id": team_id,
                        "coach_id": cid,
                        "coach_name": str(row.get("COACH_NAME") or "")[:255],
                        "coach_type": str(row.get("COACH_TYPE") or "")[:64] or None,
                        "is_assistant": bool(row.get("IS_ASSISTANT")),
                        "joined_at": lo,
                        "left_at": hi,
                        "source": "roster_snapshot",
                    }
            # Close any coaches that were open for this team but absent now
            for key in list(coach_open.keys()):
                if key[0] == team_id and key[1] not in seen_coaches_this_season:
                    coach_stints_closed.append(coach_open.pop(key))

        if idx % 25 == 0:
            logger.info("  [%d/%d] %s %s  extras=%d coaches_open=%d",
                        idx, len(pairs_sorted), team_id, season_str,
                        len(extra_player_stints), len(coach_open))

        time.sleep(sleep_secs)

    # Flush any still-open coach stints at the end.
    for st in coach_open.values():
        coach_stints_closed.append(st)
    coach_open.clear()

    # Insert extras
    if extra_player_stints:
        now = datetime.utcnow()
        for s in extra_player_stints:
            s["created_at"] = now
            s["updated_at"] = now
        batch = 1000
        for i in range(0, len(extra_player_stints), batch):
            session.execute(
                text("""
                    INSERT INTO TeamRosterStint
                      (team_id, player_id, joined_at, left_at, jersey, position,
                       how_acquired, source, created_at, updated_at)
                    VALUES
                      (:team_id, :player_id, :joined_at, :left_at, :jersey, :position,
                       :how_acquired, :source, :created_at, :updated_at)
                """),
                extra_player_stints[i:i + batch],
            )
        session.commit()
        counts["extra_roster_stints"] = len(extra_player_stints)

    if coach_stints_closed:
        now = datetime.utcnow()
        for s in coach_stints_closed:
            s["created_at"] = now
            s["updated_at"] = now
        batch = 500
        for i in range(0, len(coach_stints_closed), batch):
            session.execute(
                text("""
                    INSERT INTO TeamCoachStint
                      (team_id, coach_id, coach_name, coach_type, is_assistant,
                       joined_at, left_at, source, created_at, updated_at)
                    VALUES
                      (:team_id, :coach_id, :coach_name, :coach_type, :is_assistant,
                       :joined_at, :left_at, :source, :created_at, :updated_at)
                """),
                coach_stints_closed[i:i + batch],
            )
        session.commit()
        counts["coach_rows"] = len(coach_stints_closed)

    return counts


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-api", action="store_true",
                        help="Phase 1 only (no commonteamroster calls)")
    parser.add_argument("--seasons", type=str, default=None,
                        help="Year range like '2020-2025' (inclusive)")
    parser.add_argument("--current-only", action="store_true",
                        help="Limit phase 2 to the current start-year")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.6,
                        help="Sleep seconds between API calls")
    args = parser.parse_args()

    year_lo, year_hi = None, None
    if args.seasons:
        parts = args.seasons.split("-")
        year_lo, year_hi = int(parts[0]), int(parts[1])
    if args.current_only:
        cy = _current_start_year()
        year_lo, year_hi = cy, cy

    with Session() as session:
        if args.dry_run:
            pairs = _team_season_pairs(session)
            if year_lo is not None:
                pairs = [p for p in pairs if year_lo <= p[1] <= year_hi]
            logger.info("DRY RUN: would process %d (team, season) pairs", len(pairs))
            return

        logger.info("Wiping existing stints (one-shot backfill)…")
        session.execute(text("DELETE FROM TeamRosterStint"))
        session.execute(text("DELETE FROM TeamCoachStint"))
        session.commit()

        # Phase 1
        stints = derive_stints_from_games(session)
        inserted = insert_derived_stints(session, stints)
        logger.info("Phase 1 done: %d TeamRosterStint rows", inserted)

        if args.skip_api:
            logger.info("--skip-api set, done.")
            return

        # Phase 2
        pairs = _team_season_pairs(session)
        if year_lo is not None:
            pairs = [p for p in pairs if year_lo <= p[1] <= year_hi]
        logger.info("Phase 2: %d (team, season) pairs queued", len(pairs))
        counts = fetch_rosters_and_write_stints(session, pairs, sleep_secs=args.sleep)
        logger.info("Phase 2 done: %s", counts)


if __name__ == "__main__":
    main()
