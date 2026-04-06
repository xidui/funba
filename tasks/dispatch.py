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

from db.models import Game, MetricComputeRun, MetricRunLog, engine
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
        q = sess.query(Game.game_id).filter(Game.game_date.isnot(None))
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
        return [row.game_id for row in sess.query(Game.game_id).filter(Game.game_date.isnot(None)).all()]
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


def _create_metric_compute_run(
    metric_key: str,
    target_game_count: int,
    *,
    season: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[MetricComputeRun, bool]:
    """Create a new MetricComputeRun unless one is already active for this metric."""
    sess = _session()
    try:
        existing = (
            sess.query(MetricComputeRun)
            .filter(
                MetricComputeRun.metric_key == metric_key,
                MetricComputeRun.status.in_(("mapping", "reducing")),
            )
            .order_by(MetricComputeRun.created_at.desc())
            .first()
        )
        if existing is not None:
            run_id = existing.id
            sess.expunge(existing)
            return existing, False

        # Delete old completed/failed runs — keep only one run per metric
        sess.query(MetricComputeRun).filter(
            MetricComputeRun.metric_key == metric_key,
            MetricComputeRun.status.in_(("complete", "failed")),
        ).delete(synchronize_session=False)

        run_id = str(uuid.uuid4())
        now = datetime.utcnow()
        run = MetricComputeRun(
            id=run_id,
            metric_key=metric_key,
            status="mapping",
            target_season=season,
            target_date_from=_parse_optional_date(date_from),
            target_date_to=_parse_optional_date(date_to),
            target_game_count=int(target_game_count),
            created_at=now,
            started_at=now,
        )
        sess.add(run)
        sess.commit()
        sess.refresh(run)
        sess.expunge(run)
        return run, True
    finally:
        sess.close()


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
    return discover_and_insert_games(
        season=season,
        season_types=season_types,
        date_from=date_from,
        date_to=date_to,
        include_unplayed=True,
        upsert_existing=True,
    )


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

    from tasks.metrics import chord_reduce_callback, compute_game_delta
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
        run, created = _create_metric_compute_run(
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
    from celery import chord

    from metrics.framework.base import CAREER_SEASONS, career_season_for, season_matches_metric_types
    from metrics.framework.runtime import get_all_metrics, get_metric
    from tasks.metrics import compute_season_metric_task, enqueue_career_metric_family_task

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
                r.season for r in sess.query(Game.season).distinct().filter(Game.season.isnot(None)).all()
            )
        finally:
            sess.close()

    enqueued = 0
    callbacks = 0
    for m in metrics:
        eligible_seasons = [season for season in seasons if season_matches_metric_types(season, getattr(m, "season_types", None))]
        eligible_career_buckets = [
            cb for cb in sorted(CAREER_SEASONS)
            if season_matches_metric_types(cb, getattr(m, "season_types", None))
        ]
        if getattr(m, "career", False):
            # Career variant — dispatch with career seasons only
            if args.season:
                career_bucket = career_season_for(args.season)
                if career_bucket and season_matches_metric_types(career_bucket, getattr(m, "season_types", None)):
                    compute_season_metric_task.delay(m.key, career_bucket)
                    enqueued += 1
            else:
                for cb in eligible_career_buckets:
                    compute_season_metric_task.delay(m.key, cb)
                    enqueued += 1
        else:
            # Base metric — dispatch with concrete seasons
            has_career = getattr(m, "supports_career", False) and not args.season and bool(eligible_career_buckets)
            task_count = len(eligible_seasons) + (len(eligible_career_buckets) if has_career else 0)
            if task_count == 0:
                print(f"  {m.key}: no supported seasons for configured season_types, skipping")
                continue
            run, created = _create_metric_compute_run(m.key, task_count)
            run_id = run.id if created else None
            if not created:
                print(f"  {m.key}: active compute run exists ({run.id}), skipping progress tracking")

            if has_career:
                season_tasks = [compute_season_metric_task.s(m.key, season, run_id=run_id) for season in eligible_seasons]
                chord(season_tasks)(enqueue_career_metric_family_task.s(metric_key=m.key, run_id=run_id))
                enqueued += len(season_tasks)
                callbacks += 1
            else:
                for season in eligible_seasons:
                    compute_season_metric_task.delay(m.key, season, run_id=run_id)
                    enqueued += 1

    if callbacks:
        print(
            f"Enqueued {enqueued} season metric task(s) for {len(metrics)} metric(s) "
            f"with {callbacks} career callback chord(s)."
        )
    else:
        print(f"Enqueued {enqueued} season metric task(s) for {len(metrics)} metric(s).")


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
    p_sm.set_defaults(func=cmd_season_metrics)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
