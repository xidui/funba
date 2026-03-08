"""Celery tasks for NBA game data ingestion (Queue: ingest).

Flow per task:
  ingest_game(game_id)
    1. Check what data is already present (game detail, PBP, shot records)
    2. Fetch only what's missing from NBA API
    3. On success → fan out compute_game_metrics for every registered metric key
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

from celery import shared_task
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.backfill_nba_games import process_and_store_game
from db.backfill_nba_player_shot_detail import (
    back_fill_game_shot_record,
    is_game_shot_back_filled,
)
from db.models import Game, engine
from metrics.framework.runtime import get_all_metrics

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
            game_exists = sess.query(Game.game_id).filter(Game.game_id == game_id).first() is not None
            if game_exists:
                has_detail = is_game_detail_back_filled(game_id, sess)
                has_pbp = is_game_pbp_back_filled(game_id, sess)
                has_shot = is_game_shot_back_filled(sess, game_id)
            else:
                has_detail = has_pbp = has_shot = False

        needs_detail_pbp = not (has_detail and has_pbp)
        needs_shot = not has_shot

        # Step 2: fetch from API if anything is missing (covers new games too)
        if needs_detail_pbp or not game_exists:
            logger.info("ingest_game %s: fetching game+detail+PBP from NBA API …", game_id)
            row = _fetch_api_row(game_id)
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

    except Exception as exc:
        # Explicit retry with exponential backoff — fan-out has NOT happened yet
        wait = 30 * (3 ** self.request.retries)  # 30s, 90s, 270s
        logger.warning("ingest_game %s: failed (attempt %d): %s — retrying in %ds",
                       game_id, self.request.retries + 1, exc, wait)
        raise self.retry(exc=exc, countdown=wait)

    # Step 4: fan out — only reached after all ingestion steps succeed
    from tasks.metrics import compute_game_metrics  # local import avoids circular at module load

    keys_to_run = metric_keys if metric_keys is not None else [m.key for m in get_all_metrics()]
    for key in keys_to_run:
        compute_game_metrics.apply_async(args=[game_id, key], kwargs={"force": force}, queue="metrics")

    logger.info(
        "ingest_game %s: done (new_game=%s, detail_pbp_refreshed=%s, shot_refreshed=%s) → %d metric tasks enqueued.",
        game_id, not game_exists, needs_detail_pbp, needs_shot, len(keys_to_run),
    )
    return {
        "game_id": game_id,
        "status": "ok",
        "new_game": not game_exists,
        "detail_pbp_refreshed": needs_detail_pbp,
        "shot_refreshed": needs_shot,
        "metric_tasks_enqueued": len(keys_to_run),
    }


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
    """
    yesterday = date.today() - timedelta(days=1)

    # Discover from NBA API first — this catches brand-new games
    game_ids = _discover_game_ids_for_date(yesterday)

    if not game_ids:
        logger.info("ingest_yesterday: no completed games found for %s.", yesterday)
        return {"date": str(yesterday), "enqueued": 0}

    for gid in game_ids:
        ingest_game.apply_async(args=[gid], queue="ingest")

    logger.info("ingest_yesterday: enqueued %d games for %s.", len(game_ids), yesterday)
    return {"date": str(yesterday), "enqueued": len(game_ids)}


@shared_task(
    bind=True,
    name="tasks.ingest.enqueue_metric_backfill",
    max_retries=1,
    queue="ingest",
)
def enqueue_metric_backfill(self, metric_key: str, force: bool = False) -> dict:
    """Enqueue ingest tasks for every stored game for a single metric key."""
    SessionLocal = _session_factory()

    with SessionLocal() as sess:
        game_ids = [
            row.game_id
            for row in sess.query(Game.game_id)
            .filter(Game.game_date.isnot(None))
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .all()
        ]

    for gid in game_ids:
        ingest_game.apply_async(
            args=[gid],
            kwargs={"metric_keys": [metric_key], "force": force},
            queue="ingest",
        )

    logger.info(
        "enqueue_metric_backfill: metric=%s enqueued %d ingest task(s).",
        metric_key, len(game_ids),
    )
    return {"metric_key": metric_key, "enqueued_games": len(game_ids)}
