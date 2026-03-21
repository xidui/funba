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

  # Backfill official line score data across games
  python -m tasks.dispatch line-backfill --season 22025

  # Backfill all metrics across all games
  python -m tasks.dispatch metric-backfill

  # Force recompute (clears existing claims so workers rerun even for 'done' games)
  python -m tasks.dispatch metric-backfill --metric clutch_fg_pct --force
  python -m tasks.dispatch backfill --season 22025 --force
"""
from __future__ import annotations

import argparse
import sys

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from db.models import Game, GameLineScore, MetricJobClaim, engine
from tasks.celery_app import app as celery_app  # noqa: F401 — ensures tasks are registered

# Import so Celery knows about them before we call apply_async
from tasks.ingest import backfill_game_line_score, ingest_game  # noqa: F401


def _queue(name: str):
    return next(q for q in celery_app.conf.task_queues if q.name == name)


def _session():
    return sessionmaker(bind=engine)()


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


def _query_games_missing_line_score(
    season: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[str]:
    sess = _session()
    try:
        completed_line_games = (
            sess.query(GameLineScore.game_id.label("game_id"))
            .group_by(GameLineScore.game_id)
            .having(func.count() >= 2)
            .subquery()
        )
        q = (
            sess.query(Game.game_id)
            .outerjoin(completed_line_games, completed_line_games.c.game_id == Game.game_id)
            .filter(
                Game.game_date.isnot(None),
                completed_line_games.c.game_id.is_(None),
            )
            .order_by(Game.game_date.asc(), Game.game_id.asc())
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


def _clear_claims(game_ids: list[str], metric_keys: list[str] | None = None) -> int:
    """Delete MetricJobClaim rows so workers can reprocess.

    If metric_keys is None, clears ALL claims for the given games.
    Returns count of deleted rows.
    """
    sess = _session()
    try:
        q = sess.query(MetricJobClaim).filter(MetricJobClaim.game_id.in_(game_ids))
        if metric_keys is not None:
            q = q.filter(MetricJobClaim.metric_key.in_(metric_keys))
        count = q.delete(synchronize_session=False)
        sess.commit()
        return count
    finally:
        sess.close()


def discover_and_insert_games(
    season: str | None = None,
    season_types: list[str] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
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
            if "WL" in df.columns:
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
        if "vs." in matchup:
            g["home_team_id"] = team_id
            g["home_team_score"] = pts
        elif "@" in matchup:
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
        new_rows = [g for g in game_rows.values() if g["game_id"] not in existing_ids]
        if new_rows:
            sess.execute(mysql_insert(Game).prefix_with("IGNORE").values(new_rows))
            sess.commit()
            print(f"Inserted {len(new_rows)} new Game record(s).")
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
        deleted = _clear_claims(list(game_ids))
        print(f"--force: cleared {deleted} claim(s).")

    for gid in sorted(game_ids):
        ingest_game.apply_async(args=[gid], kwargs={"force": args.force}, queue="ingest")

    print(f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest.")


def cmd_game(args: argparse.Namespace) -> None:
    game_id = args.game_id
    if args.force:
        deleted = _clear_claims([game_id])
        print(f"--force: cleared {deleted} claim(s) for game {game_id}.")
    ingest_game.apply_async(args=[game_id], kwargs={"force": args.force}, queue="ingest")
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
        deleted = _clear_claims(game_ids)
        print(f"--force: cleared {deleted} claim(s) for {len(game_ids)} games.")
    for gid in game_ids:
        ingest_game.apply_async(args=[gid], kwargs={"force": args.force}, queue="ingest")
    print(f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest.")


def cmd_metric_backfill(args: argparse.Namespace) -> None:
    from metrics.framework.runtime import expand_metric_keys, get_all_metrics

    if args.metric:
        all_keys = [m.key for m in get_all_metrics()]
        if args.metric not in all_keys:
            print(f"Unknown metric key: {args.metric!r}. Known keys:", file=sys.stderr)
            for k in sorted(all_keys):
                print(f"  {k}", file=sys.stderr)
            sys.exit(1)
        metric_keys = expand_metric_keys([args.metric])
    else:
        metric_keys = [m.key for m in get_all_metrics()]

    game_ids = _query_games(
        season=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
    ) if (args.season or args.date_from or args.date_to) else _all_game_ids()

    if not game_ids:
        print("No games found.")
        return

    if args.force:
        deleted = _clear_claims(game_ids, metric_keys)
        print(f"--force: cleared {deleted} claim(s).")

    # Route through the ingest queue so artifact presence (PBP, shot detail)
    # is verified (and fetched if missing) before metric computation runs.
    for gid in game_ids:
        ingest_game.apply_async(
            args=[gid],
            kwargs={"metric_keys": metric_keys, "force": args.force},
            queue="ingest",
        )

    print(
        f"Enqueued {len(game_ids)} ingest task(s) → Queue: ingest "
        f"(each will fan out {len(metric_keys)} metric(s) after artifact check)."
    )


def cmd_line_backfill(args: argparse.Namespace) -> None:
    game_ids = _query_games_missing_line_score(
        season=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
    )

    if not game_ids:
        print("No games missing line score.")
        return

    line_score_q = _queue("line_score")
    for gid in game_ids:
        backfill_game_line_score.apply_async(args=[gid], queue="line_score", declare=[line_score_q])

    print(f"Enqueued {len(game_ids)} line-score backfill task(s) → Queue: line_score.")


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
        help="Enqueue metric compute tasks (verifies artifacts via ingest queue first).",
    )
    p_mb.add_argument("--metric", default=None, help="Single metric key, or omit for all.")
    p_mb.add_argument("--season", help="Limit to a season prefix, e.g. 22025")
    p_mb.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_mb.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_mb.add_argument("--force", action="store_true",
                      help="Clear existing claims so workers recompute even completed games.")
    p_mb.set_defaults(func=cmd_metric_backfill)

    # --- line-backfill ---
    p_lb = sub.add_parser(
        "line-backfill",
        help="Enqueue official line-score fetch tasks for stored games.",
    )
    p_lb.add_argument("--season", help="Limit to a season prefix, e.g. 22025")
    p_lb.add_argument("--date-from", dest="date_from", help="Start date YYYY-MM-DD")
    p_lb.add_argument("--date-to", dest="date_to", help="End date YYYY-MM-DD")
    p_lb.set_defaults(func=cmd_line_backfill)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
