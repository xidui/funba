from __future__ import annotations

import logging
import os
from pathlib import Path
import subprocess
import sys
from datetime import date, timedelta
from importlib import import_module

from celery import shared_task

logger = logging.getLogger(__name__)

_PUBLISH_PLATFORM_ALIASES = {"x": "twitter"}
_PUBLISHER_SCRIPTS = {
    "twitter": "funba_twitter_publish.py",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _project_python(repo_root: Path) -> str:
    venv_python = repo_root / ".venv" / "bin" / "python"
    return str(venv_python if venv_python.exists() else Path(sys.executable))


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _trim_task_output(output: str, limit: int = 1200) -> str:
    text = " ".join(str(output or "").split())
    return text[:limit]


def _normalize_publish_platform(platform: str) -> str:
    raw = str(platform or "").strip().lower()
    return _PUBLISH_PLATFORM_ALIASES.get(raw, raw)


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


def _enqueue_curator_for_pending_dates(target_dates: list[date], *, lookback_days: int = 3) -> dict:
    issues_module = _game_analysis_issues_module()
    pending_game_ids: set[str] = set()
    for target_date in target_dates:
        pipeline = issues_module.game_pipeline_status_for_date(target_date)
        pending_game_ids.update(str(game_id) for game_id in (pipeline.get("pending_curator_game_ids") or []) if game_id)
    if not pending_game_ids:
        return {"enqueued": 0, "pending_game_ids": [], "seasons": []}

    from sqlalchemy.orm import Session

    from db.models import Game, engine

    season_dates: dict[str, set[str]] = {}
    with Session(engine) as session:
        rows = (
            session.query(Game.season, Game.game_date)
            .filter(
                Game.game_id.in_(sorted(pending_game_ids)),
                Game.season.isnot(None),
                Game.game_date.isnot(None),
            )
            .distinct()
            .all()
        )
        for season, game_date in rows:
            season_dates.setdefault(str(season), set()).add(game_date.isoformat())

    enqueued = 0
    for season, source_dates in sorted(season_dates.items()):
        curate_then_analyze_for_season_task.delay(
            season,
            lookback_days=lookback_days,
            force_curator=False,
            source_dates=sorted(source_dates),
        )
        enqueued += 1

    return {
        "enqueued": enqueued,
        "pending_game_ids": sorted(pending_game_ids),
        "seasons": sorted(season_dates.keys()),
    }


@shared_task(
    bind=True,
    name="tasks.content.ensure_daily_content_analysis",
    queue="ingest",
    max_retries=1,
)
def ensure_daily_content_analysis_task(self, source_date: str | None = None, force: bool = False) -> dict:
    target_date = date.fromisoformat(source_date) if source_date else (date.today() - timedelta(days=1))
    try:
        curator_backfill = _enqueue_curator_for_pending_dates([target_date], lookback_days=1)
        result = ensure_daily_content_analysis_issue(target_date, force=force)
        if curator_backfill.get("enqueued"):
            result["curator_backfill"] = curator_backfill
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
    enqueue_curator: bool = True,
) -> dict:
    target_dates = (
        [date.fromisoformat(value) for value in source_dates]
        if source_dates
        else _game_analysis_issues_module().recent_target_dates(lookback_days)
    )
    try:
        curator_backfill = (
            _enqueue_curator_for_pending_dates(target_dates, lookback_days=lookback_days)
            if enqueue_curator
            else {"enqueued": 0, "pending_game_ids": [], "seasons": []}
        )
        result = ensure_recent_content_analysis(target_dates, force=force)
        if curator_backfill.get("enqueued"):
            result["curator_backfill"] = curator_backfill
        logger.info("recent game content analysis readiness checked for %s", result.get("checked_dates"))
        return result
    except Exception as exc:
        logger.warning("recent game content analysis readiness failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)


@shared_task(
    bind=True,
    name="tasks.content.publish_social_delivery",
    queue="ingest",
    max_retries=0,
)
def publish_social_delivery_task(
    self,
    post_id: int,
    delivery_id: int,
    platform: str = "twitter",
    timeout_seconds: int | None = None,
    max_attempts: int | None = None,
    funba_base_url: str | None = None,
) -> dict:
    """Publish a SocialPostDelivery through the platform-specific script.

    The task intentionally delegates browser automation to the existing
    script-level publishers so DB status updates and artifacts stay consistent
    with manual publishes.
    """
    normalized_platform = _normalize_publish_platform(platform)
    script_name = _PUBLISHER_SCRIPTS.get(normalized_platform)
    if not script_name:
        return {
            "ok": False,
            "post_id": post_id,
            "delivery_id": delivery_id,
            "platform": normalized_platform,
            "error": "unsupported_platform",
        }

    repo_root = _project_root()
    script_path = repo_root / "scripts" / script_name
    cmd = [
        _project_python(repo_root),
        "-u",
        str(script_path),
        "--post-id",
        str(int(post_id)),
        "--delivery-id",
        str(int(delivery_id)),
        "--submit",
        "--timeout-seconds",
        str(timeout_seconds or _env_int("FUNBA_SOCIAL_PUBLISH_TIMEOUT_SECONDS", 120)),
        "--max-attempts",
        str(max_attempts or _env_int("FUNBA_SOCIAL_PUBLISH_MAX_ATTEMPTS", 3)),
        "--funba-base-url",
        (
            funba_base_url
            or os.getenv("FUNBA_SOCIAL_PUBLISH_BASE_URL")
            or os.getenv("FUNBA_ADMIN_BASE_URL")
            or "http://127.0.0.1:5001"
        ),
        "--funba-repo-root",
        str(repo_root),
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
        timeout=_env_int("FUNBA_SOCIAL_PUBLISH_TASK_TIMEOUT_SECONDS", 300),
    )
    output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    result = {
        "ok": proc.returncode == 0,
        "post_id": int(post_id),
        "delivery_id": int(delivery_id),
        "platform": normalized_platform,
        "returncode": proc.returncode,
        "output": _trim_task_output(output),
    }
    if proc.returncode != 0:
        logger.warning(
            "social delivery publish failed post_id=%s delivery_id=%s platform=%s returncode=%s output=%s",
            post_id,
            delivery_id,
            normalized_platform,
            proc.returncode,
            result["output"],
        )
    else:
        logger.info(
            "social delivery published post_id=%s delivery_id=%s platform=%s",
            post_id,
            delivery_id,
            normalized_platform,
        )
    return result


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
    source_dates: list[str] | None = None,
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

    issues_module = _game_analysis_issues_module()
    game_dates = (
        [date.fromisoformat(value) for value in source_dates]
        if source_dates
        else issues_module.recent_game_dates_for_season(season, lookback_days=lookback_days)
    )
    metric_blockers = issues_module.metric_compute_run_blockers()
    active_metric_runs = int(metric_blockers.get("active_metric_run_count") or 0)
    failed_metric_runs = int(metric_blockers.get("failed_metric_run_count") or 0)
    if active_metric_runs or failed_metric_runs:
        status = "blocked_failed_metrics" if failed_metric_runs else "waiting_for_metrics"
        logger.warning(
            "curate_then_analyze season=%s blocked by MetricComputeRun state active=%d failed=%d",
            season,
            active_metric_runs,
            failed_metric_runs,
        )
        return {
            "season": season,
            "status": status,
            "game_dates": [target.isoformat() for target in game_dates],
            "curated": 0,
            "skipped": 0,
            "failed_game_ids": [],
            **metric_blockers,
        }

    curated = 0
    skipped = 0
    waiting = 0
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
                readiness = issues_module.game_analysis_readiness_detail(game.game_id)
                if readiness.get("pipeline_stage") not in ("curator", "ready"):
                    waiting += 1
                    logger.info(
                        "curate_then_analyze: waiting game=%s season=%s stage=%s",
                        game.game_id,
                        season,
                        readiness.get("pipeline_stage"),
                    )
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

    if source_dates:
        ensure_recent_content_analysis_task.delay(
            source_dates=sorted({value for value in source_dates if value}),
            lookback_days=lookback_days,
            force=force_analysis,
            enqueue_curator=False,
        )
    else:
        ensure_recent_content_analysis_for_season_task.delay(
            season,
            lookback_days=lookback_days,
            force=force_analysis,
        )
    logger.info(
        "curate_then_analyze season=%s curated=%d skipped=%d waiting=%d failed=%d — analysis enqueued",
        season, curated, skipped, waiting, len(failed),
    )
    return {
        "season": season,
        "game_dates": [target.isoformat() for target in game_dates],
        "curated": curated,
        "skipped": skipped,
        "waiting": waiting,
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
