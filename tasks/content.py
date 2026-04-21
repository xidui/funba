from __future__ import annotations

import logging
import os
import sys
from datetime import date, timedelta
from importlib import import_module

from celery import shared_task

logger = logging.getLogger(__name__)


def _game_analysis_issues_module():
    # Celery prefork workers may drop '' from sys.path, so ensure CWD is importable.
    cwd = os.getcwd()
    if cwd not in sys.path and "" not in sys.path:
        sys.path.insert(0, cwd)
    return import_module("content_pipeline.game_analysis_issues")


# Backward-compatible wrappers while callers migrate off the task module.
def ensure_daily_content_analysis_issue(*args, **kwargs):
    return _game_analysis_issues_module().ensure_game_content_analysis_issues(*args, **kwargs)


def ensure_recent_content_analysis(*args, **kwargs):
    return _game_analysis_issues_module().ensure_recent_game_content_analysis(*args, **kwargs)


@shared_task(
    bind=True,
    name="tasks.content.ensure_daily_content_analysis",
    queue="ingest",
    max_retries=1,
)
def ensure_daily_content_analysis_task(self, source_date: str | None = None, force: bool = False) -> dict:
    target_date = date.fromisoformat(source_date) if source_date else (date.today() - timedelta(days=1))
    try:
        result = ensure_daily_content_analysis_issue(target_date, force=force)
        logger.info("game content analysis readiness for %s -> %s", target_date.isoformat(), result.get("status"))
        return result
    except Exception as exc:
        logger.warning("game content analysis readiness failed for %s: %s", target_date.isoformat(), exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)


@shared_task(
    bind=True,
    name="tasks.content.ensure_recent_content_analysis",
    queue="ingest",
    max_retries=1,
)
def ensure_recent_content_analysis_task(
    self,
    source_dates: list[str] | None = None,
    lookback_days: int = 3,
    force: bool = False,
) -> dict:
    target_dates = (
        [date.fromisoformat(value) for value in source_dates]
        if source_dates
        else _game_analysis_issues_module().recent_target_dates(lookback_days)
    )
    try:
        result = ensure_recent_content_analysis(target_dates, force=force)
        logger.info("recent game content analysis readiness checked for %s", result.get("checked_dates"))
        return result
    except Exception as exc:
        logger.warning("recent game content analysis readiness failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)


@shared_task(
    bind=True,
    name="tasks.content.curate_then_analyze_for_season",
    queue="ingest",
    max_retries=1,
)
def curate_then_analyze_for_season_task(
    self,
    season: str,
    lookback_days: int = 3,
    force_curator: bool = False,
    force_analysis: bool = False,
) -> dict:
    """Run the LLM highlight curator on recent games of a season, then enqueue
    the content analysis job.

    For each recent game (within lookback_days), curate game + player + team
    highlights — unless the game already has `highlights_curated_at` set and
    force_curator is False. Afterwards, enqueue the analysis task so the
    content-analyst agent can use the frozen narratives.

    Errors during any one game's curation are logged but don't abort the
    rest of the batch; the analysis task always runs at the end.
    """
    import os
    from sqlalchemy.orm import Session

    from db.models import Game, engine
    from metrics.highlights.curator import run_curator_for_game

    game_dates = _game_analysis_issues_module().recent_game_dates_for_season(
        season, lookback_days=lookback_days,
    )
    curated = 0
    skipped = 0
    failed: list[str] = []
    if not os.getenv("OPENAI_API_KEY"):
        logger.warning(
            "curate_then_analyze: OPENAI_API_KEY not set in worker env; skipping curation for season=%s",
            season,
        )
    else:
        with Session(engine) as session:
            games = (
                session.query(Game)
                .filter(
                    Game.season == season,
                    Game.game_date.in_(game_dates) if game_dates else False,
                    Game.home_team_score.isnot(None),
                )
                .all()
            )
            for game in games:
                if game.highlights_curated_at is not None and not force_curator:
                    skipped += 1
                    continue
                try:
                    run_curator_for_game(session, game)
                    curated += 1
                except Exception as exc:
                    logger.warning(
                        "curate_then_analyze: curator failed game=%s season=%s: %s",
                        game.game_id, season, exc, exc_info=True,
                    )
                    failed.append(game.game_id)

    ensure_recent_content_analysis_for_season_task.delay(
        season,
        lookback_days=lookback_days,
        force=force_analysis,
    )
    logger.info(
        "curate_then_analyze season=%s curated=%d skipped=%d failed=%d — analysis enqueued",
        season, curated, skipped, len(failed),
    )
    return {
        "season": season,
        "curated": curated,
        "skipped": skipped,
        "failed_game_ids": failed,
    }


@shared_task(
    bind=True,
    name="tasks.content.ensure_recent_content_analysis_for_season",
    queue="ingest",
    max_retries=1,
)
def ensure_recent_content_analysis_for_season_task(
    self,
    season: str,
    lookback_days: int = 3,
    force: bool = False,
) -> dict:
    target_dates = _game_analysis_issues_module().recent_game_dates_for_season(
        season,
        lookback_days=lookback_days,
    )
    try:
        result = ensure_recent_content_analysis(target_dates, force=force)
        logger.info(
            "season game content analysis readiness checked for season=%s dates=%s",
            season,
            result.get("checked_dates"),
        )
        return {
            **result,
            "season": season,
        }
    except Exception as exc:
        logger.warning("season game content analysis readiness failed for %s: %s", season, exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)
