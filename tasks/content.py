from __future__ import annotations

import logging
from datetime import date, timedelta

from celery import shared_task
from sqlalchemy.orm import sessionmaker

from db.models import Game, MetricRunLog, engine
from web.paperclip_bridge import PaperclipBridgeError, PaperclipClient, load_paperclip_bridge_config

logger = logging.getLogger(__name__)

_SessionLocal = sessionmaker(bind=engine)


def _session_factory():
    return _SessionLocal


def _all_games_have_metrics(game_ids: list[str]) -> bool:
    """Check whether every game_id has at least one MetricRunLog entry."""
    if not game_ids:
        return False
    with _session_factory()() as session:
        computed_game_ids = {
            row.game_id
            for row in session.query(MetricRunLog.game_id)
            .filter(MetricRunLog.game_id.in_(game_ids))
            .distinct()
            .all()
        }
    missing = set(game_ids) - computed_game_ids
    if missing:
        logger.info("content readiness: games missing metrics: %s", sorted(missing))
    return len(missing) == 0


def _games_for_date(target_date: date) -> list[str]:
    with _session_factory()() as session:
        return [
            row.game_id
            for row in session.query(Game.game_id)
            .filter(Game.game_date == target_date)
            .order_by(Game.game_id.asc())
            .all()
        ]


def _build_daily_analysis_title(target_date: date) -> str:
    return f"Daily content analysis — funba — {target_date.isoformat()}"


def _build_daily_analysis_description(target_date: date, game_ids: list[str]) -> str:
    joined_game_ids = ", ".join(game_ids) if game_ids else "(none)"
    return (
        "Run the daily Funba content analysis pass once NBA ingest and metric computation are stable.\n\n"
        f"Source date: {target_date.isoformat()}\n"
        f"Game count: {len(game_ids)}\n"
        f"Game IDs: {joined_game_ids}\n\n"
        "Required work:\n"
        "1. Read yesterday's games and triggered metrics from Funba localhost APIs.\n"
        "2. Select 3-6 high-signal story angles.\n"
        "3. Create SocialPost entries with Chinese variants for different audiences.\n"
        "4. Leave the resulting posts in Funba for human review.\n"
        "5. Do not publish to external platforms from this issue.\n"
    )


def _build_daily_analysis_rerun_comment(target_date: date, game_ids: list[str]) -> str:
    joined_game_ids = ", ".join(game_ids) if game_ids else "(none)"
    return (
        "## Rerun Requested\n\n"
        "Force rerun requested from the Funba admin content UI.\n\n"
        f"- Source date: {target_date.isoformat()}\n"
        f"- Game count: {len(game_ids)}\n"
        f"- Game IDs: {joined_game_ids}\n"
        "- Expected action: rerun the daily analysis for this date and refresh the review-ready SocialPosts.\n"
    )


def ensure_daily_content_analysis_issue(target_date: date, *, force: bool = False) -> dict:
    cfg = load_paperclip_bridge_config()
    if cfg is None:
        raise PaperclipBridgeError("Paperclip bridge is unavailable.")

    client = PaperclipClient(cfg)
    cfg = client.discover_defaults()
    if not cfg.project_id:
        raise PaperclipBridgeError("Could not resolve Funba project in Paperclip.")
    if not cfg.content_analyst_agent_id:
        raise PaperclipBridgeError("Could not resolve Content Analyst agent in Paperclip.")

    game_ids = _games_for_date(target_date)
    if not game_ids:
        return {"ok": False, "status": "no_games", "source_date": target_date.isoformat(), "game_ids": []}

    if not force and not _all_games_have_metrics(game_ids):
        return {
            "ok": False,
            "status": "waiting_for_pipeline",
            "source_date": target_date.isoformat(),
            "game_ids": game_ids,
        }

    title = _build_daily_analysis_title(target_date)
    existing = client.list_issues(q=title, project_id=cfg.project_id)
    exact_matches = [
        issue
        for issue in existing
        if str(issue.get("title") or "").strip() == title
        and str(issue.get("status") or "").strip() in {"backlog", "todo", "in_progress", "in_review", "done", "blocked"}
    ]
    if exact_matches:
        chosen = exact_matches[0]
        if force:
            reopened = client.update_issue(
                chosen.get("id"),
                {
                    "status": "todo",
                    "assigneeAgentId": cfg.content_analyst_agent_id,
                    "assigneeUserId": None,
                    "comment": _build_daily_analysis_rerun_comment(target_date, game_ids),
                },
            )
            return {
                "ok": True,
                "status": "reopened",
                "source_date": target_date.isoformat(),
                "issue_id": reopened.get("id") or chosen.get("id"),
                "issue_identifier": reopened.get("identifier") or chosen.get("identifier"),
                "game_ids": game_ids,
            }
        return {
            "ok": True,
            "status": "exists",
            "source_date": target_date.isoformat(),
            "issue_id": chosen.get("id"),
            "issue_identifier": chosen.get("identifier"),
            "game_ids": game_ids,
        }

    issue = client.create_issue(
        {
            "projectId": cfg.project_id,
            "title": title,
            "description": _build_daily_analysis_description(target_date, game_ids),
            "status": "todo",
            "priority": "medium",
            "assigneeAgentId": cfg.content_analyst_agent_id,
        }
    )
    return {
        "ok": True,
        "status": "created",
        "source_date": target_date.isoformat(),
        "issue_id": issue.get("id"),
        "issue_identifier": issue.get("identifier"),
        "game_ids": game_ids,
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
        result = ensure_daily_content_analysis_issue(target_date, force=force)
        logger.info("daily content analysis readiness for %s -> %s", target_date.isoformat(), result.get("status"))
        return result
    except Exception as exc:
        logger.warning("daily content analysis readiness failed for %s: %s", target_date.isoformat(), exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)
