from __future__ import annotations

import logging
from datetime import date, timedelta

from celery import shared_task

from content_pipeline.game_analysis_issues import (
    ensure_game_content_analysis_issues,
    ensure_recent_game_content_analysis,
    recent_game_dates_for_season,
    recent_target_dates,
)

logger = logging.getLogger(__name__)


# Backward-compatible aliases while callers migrate off the task module.
ensure_daily_content_analysis_issue = ensure_game_content_analysis_issues
ensure_recent_content_analysis = ensure_recent_game_content_analysis


@shared_task(
    bind=True,
    name="tasks.content.ensure_daily_content_analysis",
    queue="ingest",
    max_retries=1,
)
def ensure_daily_content_analysis_task(self, source_date: str | None = None, force: bool = False) -> dict:
    target_date = date.fromisoformat(source_date) if source_date else (date.today() - timedelta(days=1))
    try:
        result = ensure_game_content_analysis_issues(target_date, force=force)
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
        else recent_target_dates(lookback_days)
    )
    try:
        result = ensure_recent_game_content_analysis(target_dates, force=force)
        logger.info("recent game content analysis readiness checked for %s", result.get("checked_dates"))
        return result
    except Exception as exc:
        logger.warning("recent game content analysis readiness failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)


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
    target_dates = recent_game_dates_for_season(season, lookback_days=lookback_days)
    try:
        result = ensure_recent_game_content_analysis(target_dates, force=force)
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
