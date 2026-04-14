"""Celery tasks for NBA game data ingestion (Queue: ingest).

Flow per task:
  ingest_game(game_id)
    1. Check what data is already present (game detail, PBP, shot records)
    2. Fetch only what's missing from NBA API
    3. Legacy-only: optional game fan-out for deprecated per-game metric pipeline
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from celery import shared_task
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_line_score import back_fill_game_line_score, has_game_line_score
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.backfill_nba_games import process_and_store_game
from db.backfill_nba_player_shot_detail import (
    back_fill_game_shot_record,
    is_game_shot_back_filled,
)
from db.game_status import GAME_STATUS_COMPLETED, completed_game_clause, get_game_status, infer_game_status
from db.models import Game, Team, TeamGameStats, engine
from metrics.framework.runtime import expand_metric_keys, get_all_metrics
from runtime_flags import get_runtime_flag

logger = logging.getLogger(__name__)


def _session_factory():
    return sessionmaker(bind=engine)


def _fetch_api_row(game_id: str) -> dict | None:
    """Fetch one game row from LeagueGameFinder (used to refresh game detail/PBP).

    Note: game_id_nullable is ignored by the NBA Stats API (nba_api issue #446),
    so we filter client-side after fetching.
    """
    from nba_api.stats.endpoints import leaguegamefinder

    finder = leaguegamefinder.LeagueGameFinder(
        game_id_nullable=game_id,
        league_id_nullable="00",
    )
    df = finder.get_data_frames()[0]
    if "WL" in df.columns:
        df = df[df["WL"].notna()]
    # Client-side filter since game_id_nullable is ignored by the API
    df = df[df["GAME_ID"].astype(str) == str(game_id)]
    df = df.drop_duplicates(subset=["GAME_ID"])
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def _build_existing_game_row(sess, game_id: str) -> dict | None:
    """Build the minimal game row shape needed by process_and_store_game.

    When schedule sync has already inserted a Game row, we can avoid a fragile
    single-game LeagueGameFinder refresh and instead reuse the stored date/team
    identity to fetch box score + PBP directly.
    """
    game = sess.query(Game).filter(Game.game_id == game_id).first()
    if game is None or not game.game_date or not game.home_team_id or not game.road_team_id:
        return None

    home_team = sess.query(Team).filter(Team.team_id == game.home_team_id).first()
    road_team = sess.query(Team).filter(Team.team_id == game.road_team_id).first()
    if home_team is None or road_team is None or not home_team.abbr or not road_team.abbr:
        return None

    return {
        "GAME_ID": game.game_id,
        "SEASON_ID": game.season,
        "GAME_DATE": game.game_date.strftime("%Y-%m-%d"),
        "MATCHUP": f"{road_team.abbr} @ {home_team.abbr}",
    }



def _season_start_year(season: str | None) -> int | None:
    """Convert DB season code like '21996' to start year 1996."""
    if not season:
        return None
    try:
        return int(str(season)[1:])
    except (TypeError, ValueError, IndexError):
        return None


def _artifacts_available_from_nba_api(season: str | None) -> bool:
    """PBP and shot detail are only available from 1996-97 onward."""
    start_year = _season_start_year(season)
    if start_year is None:
        return True
    return start_year >= 1996


def _load_game_artifact_status(sess, game_id: str, *, season_hint: str | None = None) -> dict:
    """Return current ingest completeness for one game."""
    game = sess.query(Game).filter(Game.game_id == game_id).first()
    season = season_hint or (game.season if game is not None else None)
    artifacts_supported = _artifacts_available_from_nba_api(season)

    has_detail = has_pbp = has_shot = False
    if game is not None:
        has_detail = is_game_detail_back_filled(game_id, sess)
        has_pbp = True if not artifacts_supported else is_game_pbp_back_filled(game_id, sess)
        has_shot = True if not artifacts_supported else is_game_shot_back_filled(sess, game_id)

    return {
        "game_id": game_id,
        "season": season,
        "exists_game": game is not None,
        "game_status": get_game_status(game) if game is not None else None,
        "artifacts_supported": artifacts_supported,
        "has_detail": has_detail,
        "has_pbp": has_pbp,
        "has_shot": has_shot,
        "complete": bool(game is not None and has_detail and has_pbp and has_shot),
    }


def _missing_artifacts(status: dict) -> list[str]:
    missing: list[str] = []
    if not status.get("exists_game"):
        missing.append("Game")
    if not status.get("has_detail"):
        missing.append("detail")
    if status.get("artifacts_supported", True):
        if not status.get("has_pbp"):
            missing.append("PBP")
        if not status.get("has_shot"):
            missing.append("shot")
    return missing


def _list_incomplete_game_ids(game_ids: list[str]) -> list[str]:
    SessionLocal = _session_factory()
    with SessionLocal() as sess:
        return [
            gid
            for gid in sorted(set(game_ids))
            if not _load_game_artifact_status(sess, gid).get("complete")
        ]


@shared_task(
    bind=True,
    name="tasks.ingest.ingest_game",
    max_retries=3,
    queue="ingest",
)
def ingest_game(self, game_id: str, metric_keys: list[str] | None = None, force: bool = False) -> dict:
    """Ingest all data for one game, then fan out metric compute tasks.

    Args:
        game_id:     NBA game ID.
        metric_keys: If provided, only fan out these metric keys (used by
                     metric-backfill to target specific metrics while still
                     ensuring artifacts exist). None means all registered metrics.

    Handles both new games (not yet in DB) and existing games with missing data.
    Retries are explicit (no autoretry_for) so fan-out only happens after all
    ingestion steps succeed — preventing duplicate metric tasks on retry.
    """
    SessionLocal = _session_factory()

    try:
        # Step 1: check what's present
        with SessionLocal() as sess:
            status_before = _load_game_artifact_status(sess, game_id)
            game_exists = status_before["exists_game"]
            game_status = status_before.get("game_status")
            artifacts_supported = status_before["artifacts_supported"]
            has_detail = status_before["has_detail"]
            has_pbp = status_before["has_pbp"]
            has_shot = status_before["has_shot"]
            existing_game_row = _build_existing_game_row(sess, game_id) if game_exists else None

        if game_exists and game_status != GAME_STATUS_COMPLETED and not force:
            logger.info("ingest_game %s: skipping %s game already stored in DB.", game_id, game_status or "non-completed")
            return {
                "game_id": game_id,
                "status": "skipped",
                "skip_reason": game_status or "non_completed",
                "new_game": False,
                "detail_pbp_refreshed": False,
                "shot_refreshed": False,
                "line_score_rows": 0,
                "metric_tasks_enqueued": 0,
            }
        if game_exists and game_status != GAME_STATUS_COMPLETED and force:
            logger.info(
                "ingest_game %s: force-refreshing %s game already stored in DB.",
                game_id,
                game_status or "non-completed",
            )

        needs_detail_pbp = not (has_detail and has_pbp)
        needs_shot = not has_shot

        if game_exists and not artifacts_supported:
            # NBA API does not provide PBP / shot detail before 1996-97.
            # For those seasons, treat missing PBP/shot as permanently unavailable
            # so ingest can move on and still fan out computable metrics.
            needs_detail_pbp = not has_detail
            needs_shot = False
            logger.info(
                "ingest_game %s: skipping PBP/shot fetch for pre-1996 season %s.",
                game_id, status_before.get("season"),
            )

        # Step 2: fetch from API if anything is missing (covers new games too)
        if needs_detail_pbp or not game_exists:
            logger.info("ingest_game %s: fetching game+detail+PBP from NBA API …", game_id)
            row = existing_game_row or _fetch_api_row(game_id)
            if row is None:
                raise RuntimeError(f"No API data for game {game_id}")
            with SessionLocal() as sess:
                process_and_store_game(sess, row)

        # Step 3: shot records
        if needs_shot:
            logger.info("ingest_game %s: backfilling shot records …", game_id)
            with SessionLocal() as sess:
                back_fill_game_shot_record(sess, game_id, False)
                sess.commit()

        with SessionLocal() as sess:
            status_after = _load_game_artifact_status(sess, game_id)
        missing_after = _missing_artifacts(status_after)
        if missing_after:
            raise RuntimeError(
                f"Artifacts not ready for game {game_id}: missing {', '.join(missing_after)}"
            )

        # Step 3b: fix zero-score Game rows left by discover when API had no data
        with SessionLocal() as sess:
            game = sess.query(Game).filter(Game.game_id == game_id).first()
            if game and (not game.home_team_score or not game.road_team_score):
                tgs = (
                    sess.query(TeamGameStats)
                    .filter(TeamGameStats.game_id == game_id)
                    .all()
                )
                for t in tgs:
                    if str(t.team_id) == str(game.home_team_id):
                        game.home_team_score = t.pts
                    elif str(t.team_id) == str(game.road_team_id):
                        game.road_team_score = t.pts
                if game.home_team_score and game.road_team_score:
                    game.wining_team_id = (
                        game.home_team_id if game.home_team_score > game.road_team_score
                        else game.road_team_id
                    )
                game.game_status = infer_game_status(
                    game_date=game.game_date,
                    wining_team_id=game.wining_team_id,
                    home_team_score=game.home_team_score,
                    road_team_score=game.road_team_score,
                )
                sess.commit()
                logger.info("ingest_game %s: backfilled zero-score Game row from TeamGameStats.", game_id)

    except Exception as exc:
        # Explicit retry with exponential backoff — fan-out has NOT happened yet
        wait = 30 * (3 ** self.request.retries)  # 30s, 90s, 270s
        logger.warning("ingest_game %s: failed (attempt %d): %s — retrying in %ds",
                       game_id, self.request.retries + 1, exc, wait)
        raise self.retry(exc=exc, countdown=wait)

    line_score_rows = 0
    if metric_keys is None:
        with SessionLocal() as sess:
            has_line = has_game_line_score(sess, game_id)
        if not has_line:
            try:
                with SessionLocal() as sess:
                    line_score_rows = back_fill_game_line_score(sess, game_id, commit=True)
                logger.info(
                    "ingest_game %s: ensured line score inline (%d rows).",
                    game_id,
                    line_score_rows,
                )
            except Exception as exc:
                logger.warning(
                    "ingest_game %s: line-score fetch failed (non-fatal): %s",
                    game_id,
                    exc,
                )

    # Step 4: legacy fan-out — retained only as an optional fallback for the
    # deprecated per-game metric pipeline. Current production metrics are
    # season-triggered, so this path is off by default.
    from celery import chord
    from tasks.metrics import compute_game_delta, reduce_after_ingest  # local import avoids circular at module load

    legacy_game_metric_fanout = get_runtime_flag("legacy_game_metric_fanout")
    keys_to_run = []
    if legacy_game_metric_fanout:
        keys_to_run = (
            expand_metric_keys(metric_keys)
            if metric_keys is not None
            else [m.key for m in get_all_metrics() if getattr(m, "trigger", "game") != "season"]
        )
    if keys_to_run:
        map_tasks = [compute_game_delta.s(game_id, key) for key in keys_to_run]
        chord(map_tasks)(reduce_after_ingest.s(game_id=game_id))
    elif metric_keys is not None and not legacy_game_metric_fanout:
        logger.info(
            "ingest_game %s: legacy game metric fan-out disabled; skipped explicit metric_keys=%s",
            game_id,
            metric_keys,
        )

    result = {
        "game_id": game_id,
        "status": "ok",
        "new_game": not game_exists,
        "detail_pbp_refreshed": needs_detail_pbp,
        "shot_refreshed": needs_shot,
        "line_score_rows": int(line_score_rows),
        "metric_tasks_enqueued": len(keys_to_run),
    }

    if result["new_game"] or result["detail_pbp_refreshed"] or result["shot_refreshed"]:
        try:
            from tasks.metrics import refresh_current_season_metrics

            refresh_current_season_metrics.delay([result])
        except Exception as exc:
            logger.warning(
                "ingest_game %s: failed to enqueue season metric refresh: %s",
                game_id,
                exc,
                exc_info=True,
            )

    logger.info(
        "ingest_game %s: done (new_game=%s, detail_pbp_refreshed=%s, shot_refreshed=%s, line_score_rows=%d, legacy_game_metric_fanout=%s) → %d metric tasks enqueued.",
        game_id, not game_exists, needs_detail_pbp, needs_shot, line_score_rows, legacy_game_metric_fanout, len(keys_to_run),
    )
    return result


def _recent_target_dates(lookback_days: int) -> list[date]:
    days = max(int(lookback_days), 1)
    return [date.today() - timedelta(days=offset) for offset in range(days)]


def _discover_game_ids_for_date(target_date: date) -> list[str]:
    """Discover game IDs from NBA API for a given date (finds newly finished games)."""
    from nba_api.stats.endpoints import leaguegamefinder

    date_str = target_date.strftime("%m/%d/%Y")
    game_ids: set[str] = set()
    for season_type in ("Regular Season", "Playoffs", "PlayIn"):
        try:
            finder = leaguegamefinder.LeagueGameFinder(
                date_from_nullable=date_str,
                date_to_nullable=date_str,
                season_type_nullable=season_type,
                league_id_nullable="00",
            )
            df = finder.get_data_frames()[0]
            if "WL" in df.columns:
                df = df[df["WL"].notna()]
            for gid in df["GAME_ID"].astype(str).unique():
                game_ids.add(gid)
        except Exception as exc:
            logger.warning("ingest_yesterday: LeagueGameFinder failed for %s/%s: %s",
                           season_type, target_date, exc)
    return list(game_ids)


@shared_task(
    bind=True,
    name="tasks.ingest.ingest_yesterday",
    max_retries=1,
    queue="ingest",
)
def ingest_yesterday(self) -> dict:
    """Celery Beat entry point: discover and enqueue all games played yesterday.

    Uses LeagueGameFinder (not DB) so newly finished games that haven't been
    ingested yet are included — not just games already present in the DB.
    After all games finish ingesting, a chord callback triggers season metric
    refresh for the current season.
    """
    yesterday = date.today() - timedelta(days=1)

    # Discover from NBA API first — this catches brand-new games
    game_ids = _discover_game_ids_for_date(yesterday)

    if not game_ids:
        logger.info("ingest_yesterday: no completed games found for %s.", yesterday)
        return {"date": str(yesterday), "enqueued": 0}

    for gid in game_ids:
        ingest_game.apply_async(args=[gid])

    logger.info("ingest_yesterday: enqueued %d games for %s.", len(game_ids), yesterday)
    return {"date": str(yesterday), "enqueued": len(game_ids)}


@shared_task(
    bind=True,
    name="tasks.ingest.ingest_recent_games",
    max_retries=1,
    queue="ingest",
)
def ingest_recent_games(self, lookback_days: int = 3) -> dict:
    """Periodically scan recent completed games and ingest only incomplete ones.

    This handles the common case where a game has finished but one or more NBA
    API artifacts (box/PBP/shot) are still delayed. The task keeps retrying on
    later scans until the game becomes complete.
    """
    discovered_by_date: dict[str, list[str]] = {}
    discovered_ids: set[str] = set()
    for target_date in _recent_target_dates(lookback_days):
        game_ids = sorted(_discover_game_ids_for_date(target_date))
        if game_ids:
            discovered_by_date[target_date.isoformat()] = game_ids
            discovered_ids.update(game_ids)

    if not discovered_ids:
        logger.info("ingest_recent_games: no completed games found in last %d day(s).", lookback_days)
        return {"lookback_days": int(lookback_days), "discovered": 0, "enqueued": 0, "dates": []}

    incomplete_game_ids = _list_incomplete_game_ids(list(discovered_ids))
    if not incomplete_game_ids:
        logger.info(
            "ingest_recent_games: all %d discovered game(s) already complete for last %d day(s).",
            len(discovered_ids),
            lookback_days,
        )
        return {
            "lookback_days": int(lookback_days),
            "discovered": len(discovered_ids),
            "enqueued": 0,
            "dates": sorted(discovered_by_date.keys()),
        }

    for gid in incomplete_game_ids:
        # These game ids come from LeagueGameFinder with non-null WL, so they
        # are already completed according to the NBA API even if our local
        # schedule snapshot still says upcoming/live.
        ingest_game.apply_async(args=[gid], kwargs={"force": True})
    logger.info(
        "ingest_recent_games: discovered=%d incomplete=%d dates=%s",
        len(discovered_ids),
        len(incomplete_game_ids),
        sorted(discovered_by_date.keys()),
    )
    return {
        "lookback_days": int(lookback_days),
        "discovered": len(discovered_ids),
        "enqueued": len(incomplete_game_ids),
        "dates": sorted(discovered_by_date.keys()),
        "game_ids": incomplete_game_ids,
    }


@shared_task(
    bind=True,
    name="tasks.ingest.sync_schedule_window",
    max_retries=1,
    queue="ingest",
)
def sync_schedule_window(
    self,
    lookback_days: int = 3,
    lookahead_days: int = 30,
    season_types: list[str] | None = None,
) -> dict:
    """Persist recent + upcoming schedule rows into the local Game table."""
    from tasks.dispatch import sync_schedule_games

    today = date.today()
    date_from = (today - timedelta(days=max(int(lookback_days), 0))).strftime("%m/%d/%Y")
    date_to = (today + timedelta(days=max(int(lookahead_days), 0))).strftime("%m/%d/%Y")

    try:
        game_ids = sync_schedule_games(
            date_from=date_from,
            date_to=date_to,
            season_types=season_types,
        )
    except Exception as exc:
        logger.warning(
            "sync_schedule_window failed for %s -> %s: %s",
            date_from,
            date_to,
            exc,
            exc_info=True,
        )
        raise self.retry(exc=exc, countdown=300)

    # Patch today's rows from the live scoreboard CDN. ScheduleLeagueV2 lags
    # several hours behind on play-in / playoff matchup updates, while the
    # cdn.nba.com live scoreboard reflects them within minutes. We only fill
    # NULL fields so a real schedule value is never overwritten by live noise.
    live_patched = 0
    try:
        live_patched = _patch_today_meta_from_live()
    except Exception as exc:
        logger.warning("sync_schedule_window: live meta patch failed: %s", exc, exc_info=True)

    logger.info(
        "sync_schedule_window: synced %d game(s) for %s -> %s, live_patched=%d",
        len(game_ids),
        date_from,
        date_to,
        live_patched,
    )
    return {
        "date_from": date_from,
        "date_to": date_to,
        "season_types": list(season_types or []),
        "synced_games": len(game_ids),
        "live_patched": live_patched,
    }


def _patch_today_meta_from_live() -> int:
    """Backfill NULL meta fields on today's Game rows from the live scoreboard.

    Returns the number of rows touched. Only fields that are NULL in DB are
    overwritten — schedule-API values always win when present.
    """
    from sqlalchemy import update
    from web.live_game_data import fetch_live_scoreboard_map

    snapshots = fetch_live_scoreboard_map()
    if not snapshots:
        return 0

    SessionLocal = sessionmaker(bind=engine)
    touched = 0
    with SessionLocal() as session:
        for game_id, snap in snapshots.items():
            home_id = snap.get("home_team_id") or None
            road_id = snap.get("road_team_id") or None
            if not home_id and not road_id:
                continue
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game is None:
                continue
            changed = False
            if not game.home_team_id and home_id:
                game.home_team_id = home_id
                changed = True
            if not game.road_team_id and road_id:
                game.road_team_id = road_id
                changed = True
            if changed:
                touched += 1
        if touched:
            session.commit()
    return touched


@shared_task(
    bind=True,
    name="tasks.ingest.enqueue_metric_backfill",
    max_retries=1,
    queue="ingest",
)
def enqueue_metric_backfill(self, metric_key: str, force: bool = False) -> dict:
    """Enqueue ingest tasks for every stored game for a single metric key."""
    metric_keys = expand_metric_keys([metric_key])
    SessionLocal = _session_factory()

    with SessionLocal() as sess:
        game_ids = [
            row.game_id
            for row in sess.query(Game.game_id)
            .filter(Game.game_date.isnot(None), completed_game_clause(Game))
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .all()
        ]

    for gid in game_ids:
        ingest_game.apply_async(
            args=[gid],
            kwargs={"metric_keys": metric_keys, "force": force},
            queue="ingest",
        )

    logger.info(
        "enqueue_metric_backfill: metric=%s expanded_to=%s enqueued %d ingest task(s).",
        metric_key, metric_keys, len(game_ids),
    )
    return {"metric_key": metric_key, "expanded_metric_keys": metric_keys, "enqueued_games": len(game_ids)}


# ---------------------------------------------------------------------------
# News feed (Queue: news)
# ---------------------------------------------------------------------------

@shared_task(name="tasks.ingest.scrape_nba_news", queue="news")
def scrape_nba_news():
    """Hourly scrape: ESPN NBA RSS + NBA.com content API -> NewsArticle."""
    from db.news_ingest import scrape_all
    return scrape_all()


@shared_task(name="tasks.ingest.refresh_news_scores", queue="news")
def refresh_news_scores():
    """Every-5-min refresh of unique_view_count + ranking score for recent clusters."""
    from sqlalchemy.orm import Session
    from db.models import engine
    from db.news_ingest import refresh_all_recent_scores

    with Session(engine) as session:
        count = refresh_all_recent_scores(session)
        session.commit()
    return {"refreshed": count}
