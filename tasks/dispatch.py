"""CLI entrypoint for enqueueing Celery tasks.

Usage examples:
  # Discover and ingest games from NBA API for a date range (replaces backfill_nba_games_targeted)
  python -m tasks.dispatch discover --date-from 2026-03-02 --date-to 2026-03-07

  # Reprocess known games already in DB
  python -m tasks.dispatch backfill --season 22025

  # Single game
  python -m tasks.dispatch game 0022400909

  # Backfill one metric across all games (routes through ingest to ensure artifacts exist)
  python -m tasks.dispatch metric-backfill --metric clutch_fg_pct

  # Backfill all metrics across all games
  python -m tasks.dispatch metric-backfill

  # Force recompute (clears existing claims so workers rerun even for 'done' games)
  python -m tasks.dispatch metric-backfill --metric clutch_fg_pct --force
  python -m tasks.dispatch backfill --season 22025 --force

  # Compute season-triggered metrics (salary, awards, etc.)
  python -m tasks.dispatch season-metrics --metric player_salary --season 22025
  python -m tasks.dispatch season-metrics  # all season metrics, all seasons
"""
from __future__ import annotations

import argparse
import re
import sys
import uuid
from datetime import date as _date, datetime

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from db.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_LIVE,
    GAME_STATUS_UPCOMING,
    completed_game_clause,
    infer_game_status,
)
from db.models import Game, MetricComputeRun, MetricRunLog, Team, engine
from tasks.celery_app import app as celery_app  # noqa: F401 — ensures tasks are registered

# Import so Celery knows about them before we call apply_async
from tasks.ingest import ingest_game  # noqa: F401


def _session():
    return sessionmaker(bind=engine)()


def _matchup_team_role(matchup: str, team_abbr: str | None = None) -> str | None:
    """Infer whether a team row is home or road from NBA API matchup text."""
    normalized = str(matchup or "").strip().lower()
    normalized_abbr = str(team_abbr or "").strip().lower()
    matchup_match = re.match(r"^(?P<road>[A-Z0-9]+)\s*@\s*(?P<home>[A-Z0-9]+)$", str(matchup or "").strip(), re.IGNORECASE)
    if matchup_match and normalized_abbr:
        road_abbr = matchup_match.group("road").lower()
        home_abbr = matchup_match.group("home").lower()
        if normalized_abbr == road_abbr:
            return "road"
        if normalized_abbr == home_abbr:
            return "home"
    if "@" in normalized:
        return "road"
    if "vs." in normalized or "vs " in normalized or normalized.endswith("vs"):
        return "home"
    return None


def _query_games(
    season: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[str]:
    sess = _session()
    try:
        q = sess.query(Game.game_id).filter(
            Game.game_date.isnot(None),
            completed_game_clause(Game),
        )
        if season:
            q = q.filter(Game.season.like(f"{season}%"))
        if date_from:
            q = q.filter(Game.game_date >= date_from)
        if date_to:
            q = q.filter(Game.game_date <= date_to)
        return [row.game_id for row in q.all()]
    finally:
        sess.close()


def _all_game_ids() -> list[str]:
    sess = _session()
    try:
        return [
            row.game_id
            for row in sess.query(Game.game_id)
            .filter(Game.game_date.isnot(None), completed_game_clause(Game))
            .all()
        ]
    finally:
        sess.close()


def _clear_run_logs(game_ids: list[str], metric_keys: list[str] | None = None) -> int:
    """Delete MetricRunLog rows so workers recompute deltas.

    If metric_keys is None, clears ALL run logs for the given games.
    Returns count of deleted rows.
    """
    sess = _session()
    try:
        q = sess.query(MetricRunLog).filter(MetricRunLog.game_id.in_(game_ids))
        if metric_keys is not None:
            q = q.filter(MetricRunLog.metric_key.in_(metric_keys))
        count = q.delete(synchronize_session=False)
        sess.commit()
        return count
    finally:
        sess.close()


def _parse_optional_date(value: str | None):
    return _date.fromisoformat(value) if value else None


def _normalize_stats_season(season: str | None) -> str | None:
    text = str(season or "").strip()
    if not text:
        return None
    if len(text) == 5 and text.isdigit() and text[0] in {"1", "2", "4", "5"}:
        start_year = int(text[1:])
        return f"{start_year}-{str(start_year + 1)[-2:]}"
    return text


def _season_start_year_for_date(target_date: _date) -> int:
    # NBA seasons span autumn to the following spring.
    return target_date.year if target_date.month >= 7 else target_date.year - 1


def _parse_nba_api_mmddyyyy(value: str | None) -> _date | None:
    text = str(value or "").strip()
    if not text:
        return None
    return datetime.strptime(text, "%m/%d/%Y").date()


def _schedule_seasons(
    season: str | None = None,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[str]:
    normalized = _normalize_stats_season(season)
    if normalized:
        return [normalized]

    parsed_from = _parse_nba_api_mmddyyyy(date_from)
    parsed_to = _parse_nba_api_mmddyyyy(date_to)
    start_date = parsed_from or parsed_to or _date.today()
    end_date = parsed_to or parsed_from or start_date

    start_year = _season_start_year_for_date(min(start_date, end_date))
    end_year = _season_start_year_for_date(max(start_date, end_date))
    return [f"{year}-{str(year + 1)[-2:]}" for year in range(start_year, end_year + 1)]


def _schedule_type_code(game_id: str | None) -> str | None:
    text = str(game_id or "").strip()
    if len(text) < 3 or not text[:3].isdigit():
        return None
    return text[2]


def _schedule_season_code(game_id: str | None) -> str | None:
    text = str(game_id or "").strip()
    if len(text) < 5 or not text[:5].isdigit():
        return None
    season_type = text[2]
    season_year = text[3:5]
    if season_type not in {"1", "2", "4", "5"}:
        return None
    return f"{season_type}20{season_year}"


def _schedule_status_from_code(game_status: int | str | None) -> str:
    try:
        code = int(float(game_status)) if game_status not in (None, "") else 1
    except (TypeError, ValueError):
        code = 1
    if code >= 3:
        return GAME_STATUS_COMPLETED
    if code == 2:
        return GAME_STATUS_LIVE
    return GAME_STATUS_UPCOMING


_SCHEDULE_SEASON_TYPE_CODES = {
    "pre season": "1",
    "preseason": "1",
    "regular season": "2",
    "playoffs": "4",
    "playin": "5",
    "play-in": "5",
}


def _allowed_schedule_type_codes(season_types: list[str] | None) -> set[str]:
    if not season_types:
        return {"2", "4", "5"}
    allowed: set[str] = set()
    for season_type in season_types:
        code = _SCHEDULE_SEASON_TYPE_CODES.get(str(season_type or "").strip().lower())
        if code:
            allowed.add(code)
    return allowed or {"2", "4", "5"}


def _parse_schedule_game_date(value) -> _date | None:
    text = str(value or "").strip()
    if not text:
        return None
    text = text.split(" ", 1)[0]
    try:
        return datetime.strptime(text, "%m/%d/%Y").date()
    except ValueError:
        return None


def _schedule_team_id(value) -> str | None:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return str(int(float(text)))
    except (TypeError, ValueError):
        return text


def _schedule_score(value, *, status: str) -> int | None:
    if status == GAME_STATUS_UPCOMING:
        return None
    if value in (None, "", "nan"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _schedule_game_rows_from_frame(
    frame,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    season_types: list[str] | None = None,
) -> dict[str, dict]:
    allowed_type_codes = _allowed_schedule_type_codes(season_types)
    parsed_from = _parse_nba_api_mmddyyyy(date_from)
    parsed_to = _parse_nba_api_mmddyyyy(date_to)

    game_rows: dict[str, dict] = {}
    for _, row in frame.iterrows():
        gid = str(row.get("gameId") or "").strip()
        if not gid:
            continue

        type_code = _schedule_type_code(gid)
        if type_code not in allowed_type_codes:
            continue

        game_date = _parse_schedule_game_date(row.get("gameDate"))
        if game_date is None:
            continue
        if parsed_from and game_date < parsed_from:
            continue
        if parsed_to and game_date > parsed_to:
            continue

        status = _schedule_status_from_code(row.get("gameStatus"))
        home_team_id = _schedule_team_id(row.get("homeTeam_teamId"))
        road_team_id = _schedule_team_id(row.get("awayTeam_teamId"))
        home_score = _schedule_score(row.get("homeTeam_score"), status=status)
        road_score = _schedule_score(row.get("awayTeam_score"), status=status)

        winner_id = None
        if (
            status == GAME_STATUS_COMPLETED
            and home_team_id
            and road_team_id
            and home_score is not None
            and road_score is not None
            and home_score != road_score
        ):
            winner_id = home_team_id if home_score > road_score else road_team_id

        game_rows[gid] = {
            "game_id": gid,
            "season": _schedule_season_code(gid),
            "game_date": game_date,
            "home_team_id": home_team_id,
            "road_team_id": road_team_id,
            "home_team_score": home_score,
            "road_team_score": road_score,
            "wining_team_id": winner_id,
            "game_status": status,
            "backfill_mismatch": False,
            "data_source": "nba_api_scheduleleaguev2",
        }

    return game_rows


def discover_and_insert_games(
    season: str | None = None,
    season_types: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    *,
    include_unplayed: bool = False,
    upsert_existing: bool = False,
) -> set[str]:
    """Fetch games from NBA API, bulk-insert missing Game rows, return all game_ids.

    date_from / date_to should already be in MM/DD/YYYY format (nba_api convention).
    Returns the full set of game_ids discovered (existing + newly inserted).
    """
    import pandas as pd
    from nba_api.stats.endpoints import leaguegamefinder
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    from datetime import date as _date

    if season_types is None:
        season_types = ["Regular Season", "Playoffs", "PlayIn"]

    frames = []
    for season_type in season_types:
        try:
            finder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season or "",
                season_type_nullable=season_type,
                date_from_nullable=date_from or "",
                date_to_nullable=date_to or "",
                league_id_nullable="00",
            )
            df = finder.get_data_frames()[0]
            if not include_unplayed and "WL" in df.columns:
                df = df[df["WL"].notna()]
            frames.append(df)
        except Exception as exc:
            print(f"Warning: LeagueGameFinder failed for {season_type}: {exc}", file=sys.stderr)

    if not frames or all(f.empty for f in frames):
        return set()

    full_df = pd.concat(frames, ignore_index=True)

    game_rows: dict[str, dict] = {}
    for _, row in full_df.iterrows():
        gid = str(row["GAME_ID"])
        matchup = str(row.get("MATCHUP", ""))
        team_id = str(int(row["TEAM_ID"]))
        team_abbr = str(row.get("TEAM_ABBREVIATION", ""))
        pts = int(row["PTS"]) if pd.notna(row.get("PTS")) else None
        wl = str(row.get("WL", ""))
        season_id = str(row.get("SEASON_ID", ""))
        try:
            game_date = _date.fromisoformat(str(row.get("GAME_DATE", ""))[:10])
        except (ValueError, TypeError):
            game_date = None

        if gid not in game_rows:
            game_rows[gid] = {
                "game_id": gid,
                "season": season_id,
                "game_date": game_date,
                "home_team_id": None,
                "road_team_id": None,
                "home_team_score": None,
                "road_team_score": None,
                "wining_team_id": None,
                "game_status": None,
                "backfill_mismatch": False,
            }
        g = game_rows[gid]
        role = _matchup_team_role(matchup, team_abbr=team_abbr)
        if role == "home":
            g["home_team_id"] = team_id
            g["home_team_score"] = pts
        elif role == "road":
            g["road_team_id"] = team_id
            g["road_team_score"] = pts
        if wl == "W":
            g["wining_team_id"] = team_id

    for game_row in game_rows.values():
        game_row["game_status"] = infer_game_status(
            game_date=game_row["game_date"],
            wining_team_id=game_row["wining_team_id"],
            home_team_score=game_row["home_team_score"],
            road_team_score=game_row["road_team_score"],
        )

    game_ids = set(game_rows.keys())
    if not game_ids:
        return set()

    sess = _session()
    try:
        existing_ids = {
            r.game_id
            for r in sess.query(Game.game_id).filter(Game.game_id.in_(game_ids)).all()
        }
        inserted_count = len(game_ids - existing_ids)
        updated_count = len(game_ids & existing_ids) if upsert_existing else 0
        if upsert_existing:
            stmt = mysql_insert(Game).values(list(game_rows.values()))
            stmt = stmt.on_duplicate_key_update(
                season=stmt.inserted.season,
                game_date=stmt.inserted.game_date,
                home_team_id=stmt.inserted.home_team_id,
                road_team_id=stmt.inserted.road_team_id,
                game_status=stmt.inserted.game_status,
                home_team_score=func.coalesce(stmt.inserted.home_team_score, Game.home_team_score),
                road_team_score=func.coalesce(stmt.inserted.road_team_score, Game.road_team_score),
                wining_team_id=func.coalesce(stmt.inserted.wining_team_id, Game.wining_team_id),
            )
            sess.execute(stmt)
            sess.commit()
            print(f"Synced {len(game_rows)} Game record(s) ({inserted_count} inserted, {updated_count} updated).")
        else:
            new_rows = [g for g in game_rows.values() if g["game_id"] not in existing_ids]
            if new_rows:
                sess.execute(mysql_insert(Game).prefix_with("IGNORE").values(new_rows))
                sess.commit()
                print(f"Inserted {len(new_rows)} new Game record(s).")
    finally:
        sess.close()

    return game_ids


def sync_schedule_games(
    season: str | None = None,
    season_types: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> set[str]:
    """Sync schedule rows, including future / not-yet-played games, into Game."""
    import pandas as pd
    from nba_api.stats.endpoints import scheduleleaguev2
    from sqlalchemy.dialects.mysql import insert as mysql_insert

    frames = []
    for season_value in _schedule_seasons(season, date_from=date_from, date_to=date_to):
        try:
            schedule = scheduleleaguev2.ScheduleLeagueV2(season=season_value, league_id="00")
            frame = schedule.get_data_frames()[0]
            if frame is not None and not frame.empty:
                frames.append(frame)
        except Exception as exc:
            print(f"Warning: ScheduleLeagueV2 failed for {season_value}: {exc}", file=sys.stderr)

    if not frames:
        return set()

    full_df = pd.concat(frames, ignore_index=True)
    game_rows = _schedule_game_rows_from_frame(
        full_df,
        date_from=date_from,
        date_to=date_to,
        season_types=season_types,
    )
    if not game_rows:
        return set()

    sess = _session()
    try:
        valid_team_ids = {row.team_id for row in sess.query(Team.team_id).all()}
        # ScheduleLeagueV2 occasionally returns preseason / exhibition games
        # against non-NBA teams (e.g. FIBA opponents) whose team_ids are not in
        # our Team table. Drop those entirely — the FK constraint would
        # otherwise fail the whole batch insert.
        skipped_unknown_team = 0
        filtered_rows = {}
        for gid, row in game_rows.items():
            home_id = row.get("home_team_id")
            road_id = row.get("road_team_id")
            if home_id and home_id not in valid_team_ids:
                skipped_unknown_team += 1
                continue
            if road_id and road_id not in valid_team_ids:
                skipped_unknown_team += 1
                continue
            filtered_rows[gid] = row
        game_rows = filtered_rows
        if skipped_unknown_team:
            print(f"Skipped {skipped_unknown_team} schedule row(s) with unknown team_ids.")
        if not game_rows:
            return set()

        game_ids = set(game_rows.keys())
        existing_ids = {
            r.game_id
            for r in sess.query(Game.game_id).filter(Game.game_id.in_(game_ids)).all()
        }
        inserted_count = len(game_ids - existing_ids)
        updated_count = len(game_ids & existing_ids)

        stmt = mysql_insert(Game).values(list(game_rows.values()))
        stmt = stmt.on_duplicate_key_update(
            season=stmt.inserted.season,
            game_date=stmt.inserted.game_date,
            home_team_id=func.coalesce(stmt.inserted.home_team_id, Game.home_team_id),
            road_team_id=func.coalesce(stmt.inserted.road_team_id, Game.road_team_id),
            game_status=stmt.inserted.game_status,
            home_team_score=func.coalesce(stmt.inserted.home_team_score, Game.home_team_score),
            road_team_score=func.coalesce(stmt.inserted.road_team_score, Game.road_team_score),
            wining_team_id=func.coalesce(stmt.inserted.wining_team_id, Game.wining_team_id),
        )
        sess.execute(stmt)
        sess.commit()
        print(f"Synced {len(game_rows)} schedule Game record(s) ({inserted_count} inserted, {updated_count} updated).")
    finally:
        sess.close()

    return game_ids


def cmd_discover(args: argparse.Namespace) -> None:
    """Discover games from NBA API, pre-populate Game table, then enqueue ingest for each."""
    from datetime import date as _date

    def _fmt(d: str | None) -> str:
        if not d:
            return ""
        return _date.fromisoformat(d).strftime("%m/%d/%Y")

    game_ids = discover_and_insert_games(
        season=args.season,
        season_types=args.season_type or None,
        date_from=_fmt(args.date_from),
        date_to=_fmt(args.date_to),
    )

    if not game_ids:
        print("No games found from NBA API for the given filters.")
        return

    if args.force:
        deleted = _clear_run_logs(list(game_ids))
        print(f"--force: cleared {deleted} run log(s).")

    for gid in sorted(game_ids):
        ingest_game.apply_async(args=[gid], kwargs={"force": args.force}, )

    print(f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest.")


def cmd_schedule_sync(args: argparse.Namespace) -> None:
    """Sync recent + upcoming schedule rows into the local Game table."""
    from datetime import date as _date, timedelta as _timedelta

    def _fmt(d: str | None) -> str:
        if not d:
            return ""
        return _date.fromisoformat(d).strftime("%m/%d/%Y")

    if args.date_from or args.date_to:
        date_from = _fmt(args.date_from)
        date_to = _fmt(args.date_to)
    else:
        today = _date.today()
        date_from = (today - _timedelta(days=args.lookback_days)).strftime("%m/%d/%Y")
        date_to = (today + _timedelta(days=args.lookahead_days)).strftime("%m/%d/%Y")

    game_ids = sync_schedule_games(
        season=args.season,
        season_types=args.season_type or None,
        date_from=date_from,
        date_to=date_to,
    )
    if not game_ids:
        print("No schedule rows found from NBA API for the given filters.")
        return
    print(f"Synced schedule for {len(game_ids)} game(s).")


def cmd_game(args: argparse.Namespace) -> None:
    game_id = args.game_id
    if args.force:
        deleted = _clear_run_logs([game_id])
        print(f"--force: cleared {deleted} run log(s) for game {game_id}.")
    ingest_game.apply_async(args=[game_id], kwargs={"force": args.force}, )
    print(f"Enqueued 1 ingest task for game {game_id}.")


def cmd_backfill(args: argparse.Namespace) -> None:
    game_ids = _query_games(
        season=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
    )
    if not game_ids:
        print("No games found for the given filters.")
        return
    if args.force:
        deleted = _clear_run_logs(game_ids)
        print(f"--force: cleared {deleted} run log(s) for {len(game_ids)} games.")
    for gid in game_ids:
        ingest_game.apply_async(args=[gid], kwargs={"force": args.force}, )
    print(f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest.")


def cmd_metric_backfill(args: argparse.Namespace) -> None:
    """Enqueue Phase 1 (map) delta tasks via Celery chord.

    Skips the ingest queue since artifacts should already exist for backfill.
    Use 'backfill' or 'discover' commands first if data is missing.
    Reduce (Phase 2) is triggered automatically by the chord callback when
    all map tasks finish.
    """
    from celery import chord

    from tasks.metrics import chord_reduce_callback, compute_game_delta, create_metric_compute_run
    from metrics.framework.runtime import expand_metric_keys, get_all_metrics

    if args.metric:
        all_keys = [m.key for m in get_all_metrics()]
        if args.metric not in all_keys:
            print(f"Unknown metric key: {args.metric!r}. Known keys:", file=sys.stderr)
            for k in sorted(all_keys):
                print(f"  {k}", file=sys.stderr)
            sys.exit(1)
        m = next(m for m in get_all_metrics() if m.key == args.metric)
        if getattr(m, "trigger", "game") == "season":
            print(f"Metric {args.metric!r} is trigger=season. Use 'season-metrics' command instead.")
            return
        metric_keys = expand_metric_keys([args.metric])
    else:
        metric_keys = [m.key for m in get_all_metrics() if getattr(m, "trigger", "game") != "season"]

    game_ids = _query_games(
        season=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
    ) if (args.season or args.date_from or args.date_to) else _all_game_ids()

    if not game_ids:
        print("No games found.")
        return

    if args.force:
        deleted = _clear_run_logs(game_ids, metric_keys)
        print(f"--force: cleared {deleted} run log(s).")

    # Register one MetricComputeRun per concrete metric key, then enqueue
    # map tasks as a Celery chord with a reduce callback.
    task_count = 0
    run_count = 0
    skipped_active: list[str] = []
    for key in metric_keys:
        run, created = create_metric_compute_run(
            key,
            len(game_ids),
            season=args.season,
            date_from=args.date_from,
            date_to=args.date_to,
        )
        if not created:
            skipped_active.append(f"{key} ({run.id})")
            continue
        run_count += 1

        map_tasks = [
            compute_game_delta.s(gid, key, run_id=run.id)
            for gid in game_ids
        ]
        callback = chord_reduce_callback.s(run_id=run.id)
        chord(map_tasks)(callback)
        task_count += len(game_ids)

    print(
        f"Enqueued {task_count} delta task(s) as chord(s) → Queue: metrics "
        f"for {run_count} compute run(s). "
        f"Reduce will be triggered automatically by chord callback."
    )
    if skipped_active:
        print("Skipped metrics with an active compute run:")
        for item in skipped_active:
            print(f"  {item}")


def cmd_metric_reduce(args: argparse.Namespace) -> None:
    """Manually trigger Phase 2 (reduce) for metrics that have delta data."""
    from db.models import MetricRunLog
    from tasks.metrics import reduce_metric_season_task

    sess = _session()
    try:
        q = sess.query(MetricRunLog.metric_key, MetricRunLog.season).distinct()
        if args.metric:
            q = q.filter(MetricRunLog.metric_key == args.metric)
        if args.season:
            q = q.filter(MetricRunLog.season.like(f"{args.season}%"))
        pairs = q.all()
    finally:
        sess.close()

    if not pairs:
        print("No MetricRunLog data found for the given filters.")
        return

    for metric_key, season in pairs:
        reduce_metric_season_task.delay(metric_key, season)

    print(f"Enqueued {len(pairs)} reduce task(s) → Queue: reduce.")


def cmd_metric_retry_failed(args: argparse.Namespace) -> None:
    """Re-enqueue reduce for failed MetricComputeRun rows after resetting status."""
    from tasks.metrics import reduce_metric_compute_run_task

    sess = _session()
    try:
        q = sess.query(MetricComputeRun).filter(MetricComputeRun.status == "failed")
        if args.metric:
            q = q.filter(MetricComputeRun.metric_key == args.metric)
        runs = q.order_by(MetricComputeRun.created_at.asc()).all()

        if not runs:
            print("No failed MetricComputeRun rows found for the given filters.")
            return

        run_ids = [run.id for run in runs]
        q.update(
            {
                "status": "reducing",
                "failed_at": None,
                "error_text": None,
            },
            synchronize_session=False,
        )
        sess.commit()
    finally:
        sess.close()

    for run_id in run_ids:
        reduce_metric_compute_run_task.delay(run_id)

    print(f"Re-enqueued {len(run_ids)} failed compute run(s) → Queue: reduce.")


def cmd_season_metrics(args: argparse.Namespace) -> None:
    """Enqueue season-triggered metric computation tasks."""
    from metrics.framework.runtime import get_all_metrics, get_metric
    from tasks.metrics import enqueue_season_metric_refresh

    if args.metric:
        m = get_metric(args.metric)
        if m is None:
            print(f"Unknown metric key: {args.metric!r}", file=sys.stderr)
            sys.exit(1)
        if getattr(m, "trigger", "game") != "season":
            print(f"Metric {args.metric!r} is not trigger=season.", file=sys.stderr)
            sys.exit(1)
        metrics = [m]
    else:
        metrics = [
            m for m in get_all_metrics()
            if getattr(m, "trigger", "game") == "season" and not getattr(m, "career", False)
        ]

    if not metrics:
        print("No season-triggered metrics found.")
        return

    # Determine seasons to run
    if args.season:
        seasons = [args.season]
    else:
        sess = _session()
        try:
            seasons = sorted(
                r.season
                for r in sess.query(Game.season)
                .filter(Game.season.isnot(None), completed_game_clause(Game))
                .distinct()
                .all()
            )
        finally:
            sess.close()

    result = enqueue_season_metric_refresh(seasons, metrics=metrics, reset_tracking=args.reset_tracking)

    if result.get("callbacks"):
        print(
            f"Enqueued {result['enqueued']} season metric task(s) for {len(metrics)} metric(s) "
            f"with {result['callbacks']} career callback chord(s)."
        )
    else:
        print(f"Enqueued {result['enqueued']} season metric task(s) for {len(metrics)} metric(s).")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tasks.dispatch",
        description="Enqueue Celery tasks for game ingestion and metric computation.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- discover ---
    p_disc = sub.add_parser(
        "discover",
        help="Discover game IDs from NBA API and enqueue ingest for each (replaces backfill_nba_games_targeted).",
    )
    p_disc.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_disc.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_disc.add_argument("--season", default=None, help="Season string passed to LeagueGameFinder, e.g. '2025-26'")
    p_disc.add_argument(
        "--season-type",
        dest="season_type",
        action="append",
        metavar="TYPE",
        help="Season type (default: Regular Season, Playoffs, PlayIn). Repeatable.",
    )
    p_disc.add_argument("--force", action="store_true",
                        help="Clear existing claims so workers reprocess even completed games.")
    p_disc.set_defaults(func=cmd_discover)

    # --- schedule-sync ---
    p_sched = sub.add_parser(
        "schedule-sync",
        help="Sync recent + upcoming schedule rows into the local Game table without enqueueing ingest.",
    )
    p_sched.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_sched.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_sched.add_argument("--season", default=None, help="Season string passed to LeagueGameFinder, e.g. '2025-26'")
    p_sched.add_argument(
        "--season-type",
        dest="season_type",
        action="append",
        metavar="TYPE",
        help="Season type (default: Regular Season, Playoffs, PlayIn). Repeatable.",
    )
    p_sched.add_argument("--lookback-days", dest="lookback_days", type=int, default=3)
    p_sched.add_argument("--lookahead-days", dest="lookahead_days", type=int, default=30)
    p_sched.set_defaults(func=cmd_schedule_sync)

    # --- game <game_id> ---
    p_game = sub.add_parser("game", help="Enqueue ingest for a single game.")
    p_game.add_argument("game_id", help="NBA game ID, e.g. 0022400909")
    p_game.add_argument("--force", action="store_true",
                        help="Clear existing claims so workers reprocess even completed games.")
    p_game.set_defaults(func=cmd_game)

    # --- backfill ---
    p_bf = sub.add_parser("backfill", help="Enqueue ingest tasks for multiple games.")
    p_bf.add_argument("--season", help="Season prefix, e.g. 22025")
    p_bf.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_bf.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_bf.add_argument("--force", action="store_true",
                      help="Clear existing claims so workers reprocess even completed games.")
    p_bf.set_defaults(func=cmd_backfill)

    # --- metric-backfill ---
    p_mb = sub.add_parser(
        "metric-backfill",
        help="Enqueue Phase 1 (map) delta tasks. Reduce is promoted by the sweeper on completion.",
    )
    p_mb.add_argument("--metric", default=None, help="Single metric key, or omit for all.")
    p_mb.add_argument("--season", help="Limit to a season prefix, e.g. 22025")
    p_mb.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_mb.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_mb.add_argument("--force", action="store_true",
                      help="Clear existing claims so workers recompute even completed games.")
    p_mb.set_defaults(func=cmd_metric_backfill)

    # --- metric-reduce ---
    p_mr = sub.add_parser(
        "metric-reduce",
        help="Manually trigger Phase 2 (reduce) for metrics with delta data.",
    )
    p_mr.add_argument("--metric", default=None, help="Single metric key, or omit for all.")
    p_mr.add_argument("--season", default=None, help="Season filter, e.g. 22025 or all_regular")
    p_mr.set_defaults(func=cmd_metric_reduce)

    # --- metric-retry-failed ---
    p_mrf = sub.add_parser(
        "metric-retry-failed",
        help="Re-enqueue failed MetricComputeRun reduce tasks.",
    )
    p_mrf.add_argument("--metric", default=None, help="Single metric key, or omit for all failed runs.")
    p_mrf.set_defaults(func=cmd_metric_retry_failed)

    # --- season-metrics ---
    p_sm = sub.add_parser(
        "season-metrics",
        help="Compute season-triggered metrics (salary, awards, whole-season aggregations).",
    )
    p_sm.add_argument("--metric", default=None, help="Single metric key, or omit for all season metrics.")
    p_sm.add_argument("--season", default=None, help="Single season (e.g. 22025), or omit for all seasons.")
    p_sm.add_argument(
        "--reset-tracking",
        action="store_true",
        help="Force-replace any existing MetricComputeRun (including stuck mapping/reducing rows) "
             "with a fresh one. Use when a prior dispatch left tracking stale.",
    )
    p_sm.set_defaults(func=cmd_season_metrics)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
