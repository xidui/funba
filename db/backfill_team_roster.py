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
    force=True,
)
logger = logging.getLogger(__name__)
# Unbuffered stdout so progress streams live when redirected to a file.
import sys as _sys
try:
    _sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

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
    wait=wait_exponential(multiplier=1, max=30),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((ConnectionError, Timeout, Exception)),
    before_sleep=before_sleep_log(logger, logging.INFO),
)
def _fetch_roster(team_id: int, season_str: str):
    r = commonteamroster.CommonTeamRoster(team_id=team_id, season=season_str, timeout=30)
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


def _flush_extras(session, rows: list[dict]) -> None:
    if not rows:
        return
    now = datetime.utcnow()
    for s in rows:
        s.setdefault("created_at", now)
        s.setdefault("updated_at", now)
    session.execute(
        text("""
            INSERT INTO TeamRosterStint
              (team_id, player_id, joined_at, left_at, jersey, position,
               how_acquired, source, created_at, updated_at)
            VALUES
              (:team_id, :player_id, :joined_at, :left_at, :jersey, :position,
               :how_acquired, :source, :created_at, :updated_at)
        """),
        rows,
    )


def _flush_coaches(session, rows: list[dict]) -> None:
    if not rows:
        return
    now = datetime.utcnow()
    for s in rows:
        s.setdefault("created_at", now)
        s.setdefault("updated_at", now)
    session.execute(
        text("""
            INSERT INTO TeamCoachStint
              (team_id, coach_id, coach_name, coach_type, is_assistant,
               joined_at, left_at, source, created_at, updated_at)
            VALUES
              (:team_id, :coach_id, :coach_name, :coach_type, :is_assistant,
               :joined_at, :left_at, :source, :created_at, :updated_at)
        """),
        rows,
    )


def _already_done_team_ids(session) -> set[str]:
    """Teams with at least one roster_snapshot or any coach stint — assumed
    fully processed in an earlier run. Used by --resume."""
    rows = session.execute(text("""
        SELECT DISTINCT team_id FROM TeamRosterStint WHERE source = 'roster_snapshot'
        UNION
        SELECT DISTINCT team_id FROM TeamCoachStint
    """)).fetchall()
    return {r[0] for r in rows}


def fetch_rosters_and_write_stints(
    session,
    pairs: list[tuple[str, int]],
    *,
    sleep_secs: float = 1.5,
    resume: bool = False,
) -> dict:
    """Iterate pairs grouped by team, call commonteamroster, commit per team.

    Coach stints: one row per (team, coach) merged across consecutive seasons
    (bounded to a single team, so per-team commit is safe). Extra roster
    stints: inserted only for players who don't appear in PlayerGameStats
    for that (team, season).

    Circuit breaker: pause 60s after 3 consecutive pair-level failures to
    let NBA stats API throttle subside.
    """
    logger.info("Phase 2: %d commonteamroster calls", len(pairs))

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

    known_player_ids: set[str] = {
        r[0] for r in session.execute(text("SELECT player_id FROM Player")).fetchall()
    }
    logger.info("  %d known player_ids", len(known_player_ids))

    done_team_ids: set[str] = set()
    if resume:
        done_team_ids = _already_done_team_ids(session)
        logger.info("  resume: skipping %d already-done teams", len(done_team_ids))

    # Group pairs by team_id
    per_team: dict[str, list[int]] = defaultdict(list)
    for team_id, year in pairs:
        if team_id in done_team_ids:
            continue
        per_team[team_id].append(year)
    for tid in per_team:
        per_team[tid].sort()

    team_ids_sorted = sorted(per_team.keys())
    counts = {"api_calls": 0, "api_failures": 0, "extra_roster_stints": 0,
              "coach_rows": 0, "teams_processed": 0, "teams_skipped": len(done_team_ids)}
    consecutive_failures = 0
    processed_pairs = 0
    total_pairs = sum(len(v) for v in per_team.values())

    for team_idx, team_id in enumerate(team_ids_sorted, 1):
        years = per_team[team_id]
        coach_open: dict[str, dict] = {}
        coach_rows: list[dict] = []
        extras: list[dict] = []
        team_had_any_success = False

        try:
            tid_int = int(team_id)
        except (TypeError, ValueError):
            logger.warning("skip non-int team_id %s", team_id)
            continue

        for start_year in years:
            processed_pairs += 1
            season_str = _nba_season_str(start_year)
            lo, hi = _season_bounds(session, team_id, start_year)
            if lo is None or hi is None:
                lo = date(start_year, 10, 1)
                hi = date(start_year + 1, 6, 30)

            try:
                players_df, coaches_df = _fetch_roster(tid_int, season_str)
                counts["api_calls"] += 1
                consecutive_failures = 0
                team_had_any_success = True
            except Exception as exc:
                counts["api_failures"] += 1
                consecutive_failures += 1
                logger.warning("  API fail %s %s: %s",
                               team_id, season_str, type(exc).__name__)
                if consecutive_failures >= 3:
                    # Exponentially escalating pauses: 60s, 180s, 300s cap
                    pause = min(60 * (2 ** (consecutive_failures // 3 - 1)), 300)
                    logger.warning(
                        "  circuit breaker: %d consecutive fails, pausing %ds",
                        consecutive_failures, pause)
                    time.sleep(pause)
                time.sleep(sleep_secs)
                continue

            # Players
            if players_df is not None and len(players_df) > 0:
                for _, row in players_df.iterrows():
                    pid = str(row.get("PLAYER_ID") or "")
                    if not pid:
                        continue
                    if pid not in known_player_ids:
                        continue  # skip players not in our Player table (FK)
                    if (team_id, start_year, pid) in derived_set:
                        continue
                    def _clean(v, limit):
                        if v is None:
                            return None
                        s = str(v).strip()
                        if s in ("", "nan", "NaN", "None"):
                            return None
                        return s[:limit]
                    extras.append({
                        "team_id": team_id,
                        "player_id": pid,
                        "joined_at": lo,
                        "left_at": hi,
                        "jersey": _clean(row.get("NUM"), 10),
                        "position": _clean(row.get("POSITION"), 30),
                        "how_acquired": _clean(row.get("HOW_ACQUIRED"), 255),
                        "source": "roster_snapshot",
                    })

            # Coaches
            if coaches_df is not None and len(coaches_df) > 0:
                seen_this_season: set[str] = set()
                for _, row in coaches_df.iterrows():
                    cid = str(row.get("COACH_ID") or "")
                    if not cid:
                        continue
                    seen_this_season.add(cid)
                    if cid in coach_open:
                        coach_open[cid]["left_at"] = hi
                    else:
                        coach_open[cid] = {
                            "team_id": team_id,
                            "coach_id": cid,
                            "coach_name": str(row.get("COACH_NAME") or "")[:255],
                            "coach_type": str(row.get("COACH_TYPE") or "")[:64] or None,
                            "is_assistant": bool(row.get("IS_ASSISTANT")),
                            "joined_at": lo,
                            "left_at": hi,
                            "source": "roster_snapshot",
                        }
                for cid in list(coach_open.keys()):
                    if cid not in seen_this_season:
                        coach_rows.append(coach_open.pop(cid))

            if processed_pairs % 10 == 0 or processed_pairs == 1:
                logger.info("  [%d/%d] team=%s %s extras=%d coaches_open=%d",
                            processed_pairs, total_pairs, team_id, season_str,
                            len(extras), len(coach_open))

            time.sleep(sleep_secs)

        # End of team — flush any still-open coach stints. If the last year
        # processed is the current start_year, those coaches/players are
        # presumed currently active → leave left_at NULL so the daily sync
        # extends them instead of inserting duplicates.
        current_sy = _current_start_year()
        last_year_processed = max(years) if years else None
        team_is_current = (last_year_processed == current_sy)
        for st in coach_open.values():
            if team_is_current:
                st["left_at"] = None
            coach_rows.append(st)
        if team_is_current:
            for s in extras:
                # Only the current-season extras should be left open; the
                # snapshot fills lo/hi from current-season game dates so
                # those rows always carry today-ish left_at.
                if s.get("joined_at") and s["joined_at"].year >= current_sy:
                    s["left_at"] = None

        if team_had_any_success:
            _flush_extras(session, extras)
            _flush_coaches(session, coach_rows)
            session.commit()
            counts["extra_roster_stints"] += len(extras)
            counts["coach_rows"] += len(coach_rows)
            counts["teams_processed"] += 1
            logger.info("  ★ team %s committed: extras=%d coaches=%d (%d/%d teams)",
                        team_id, len(extras), len(coach_rows),
                        team_idx, len(team_ids_sorted))
        else:
            logger.warning("  team %s: all pairs failed, nothing committed",
                           team_id)

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
    parser.add_argument("--sleep", type=float, default=1.5,
                        help="Sleep seconds between API calls")
    parser.add_argument("--resume", action="store_true",
                        help="Don't wipe tables; skip Phase 1; skip teams already done")
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

        if not args.resume:
            logger.info("Wiping existing stints (one-shot backfill)…")
            session.execute(text("DELETE FROM TeamRosterStint"))
            session.execute(text("DELETE FROM TeamCoachStint"))
            session.commit()

            # Phase 1
            stints = derive_stints_from_games(session)
            inserted = insert_derived_stints(session, stints)
            logger.info("Phase 1 done: %d TeamRosterStint rows", inserted)
        else:
            logger.info("--resume: skipping wipe + Phase 1")

        if args.skip_api:
            logger.info("--skip-api set, done.")
            return

        # Phase 2
        pairs = _team_season_pairs(session)
        if year_lo is not None:
            pairs = [p for p in pairs if year_lo <= p[1] <= year_hi]
        logger.info("Phase 2: %d (team, season) pairs queued", len(pairs))
        counts = fetch_rosters_and_write_stints(
            session, pairs, sleep_secs=args.sleep, resume=args.resume
        )
        logger.info("Phase 2 done: %s", counts)


if __name__ == "__main__":
    main()
