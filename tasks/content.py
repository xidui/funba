from __future__ import annotations

import logging
from datetime import date, timedelta

from celery import shared_task
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.models import Game, MetricRunLog, engine
from web.paperclip_bridge import PaperclipBridgeError, PaperclipClient, load_paperclip_bridge_config

logger = logging.getLogger(__name__)

_SessionLocal = sessionmaker(bind=engine)


def _session_factory():
    return _SessionLocal


def _season_start_year(season: str | None) -> int | None:
    if not season:
        return None
    try:
        return int(str(season)[1:])
    except (TypeError, ValueError, IndexError):
        return None


def _artifacts_available_from_nba_api(season: str | None) -> bool:
    start_year = _season_start_year(season)
    if start_year is None:
        return True
    return start_year >= 1996


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


def _pipeline_status_for_date(target_date: date) -> dict:
    with _session_factory()() as session:
        games = (
            session.query(Game)
            .filter(Game.game_date == target_date)
            .order_by(Game.game_id.asc())
            .all()
        )
        if not games:
            return {
                "game_ids": [],
                "artifacts_ready": False,
                "pending_game_ids": [],
            }

        pending_game_ids: list[str] = []
        for game in games:
            artifacts_supported = _artifacts_available_from_nba_api(game.season)
            has_detail = is_game_detail_back_filled(game.game_id, session)
            has_pbp = True if not artifacts_supported else is_game_pbp_back_filled(game.game_id, session)
            # Shot data is best-effort — don't block content analysis for it
            if not (has_detail and has_pbp):
                pending_game_ids.append(game.game_id)

        return {
            "game_ids": [game.game_id for game in games],
            "artifacts_ready": len(pending_game_ids) == 0,
            "pending_game_ids": pending_game_ids,
        }


def _recent_target_dates(lookback_days: int) -> list[date]:
    days = max(int(lookback_days), 1)
    return [date.today() - timedelta(days=offset) for offset in range(days)]


def _recent_game_dates_for_season(season: str, lookback_days: int = 3) -> list[date]:
    if not season or not str(season).startswith(("2", "4")):
        return []

    cutoff = date.today() - timedelta(days=max(int(lookback_days), 1) - 1)
    with _session_factory()() as session:
        rows = (
            session.query(Game.game_date)
            .filter(
                Game.season == season,
                Game.game_date.isnot(None),
                Game.game_date >= cutoff,
                Game.game_date <= date.today(),
            )
            .distinct()
            .order_by(Game.game_date.desc())
            .all()
        )
    return [row[0] for row in rows if row[0] is not None]


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
        "1. Read the source-date games and triggered metrics from Funba localhost APIs.\n"
        "2. Before creating a post, check existing posts for the same source date via `GET /api/content/posts?date=YYYY-MM-DD` and avoid duplicating the same game + angle if a similar post already exists.\n"
        "3. Select 3-6 high-signal story angles, but avoid using routine high-frequency metrics as the main title hook when they trigger for star players almost every game.\n"
        "4. Create SocialPost entries with Chinese variants for different audiences.\n"
        "5. Build a large image pool for each post: at least 10 images total, mixing screenshots, real photos, and stylized AI variants.\n"
        "6. End each post with 6-8 metric / page links. Every metric mentioned in the body must appear in that ending section, then add extras until you reach 6-8 total.\n"
        "7. Leave the resulting posts in Funba for human review.\n"
        "8. Do not publish to external platforms from this issue.\n\n"
        "## Topic Selection Rules\n\n"
        "- Avoid duplicate same-day coverage. If another post already covers the same game with a very similar angle, skip it or choose a materially different angle.\n"
        "- Do not keep using always-on metrics like common double-doubles / 20+5+5 style triggers as the title hook for the same stars every game.\n"
        "- Use those routine metrics only when there is a real milestone, streak, leaderboard movement, unusual efficiency, or broader context.\n"
        "- Prefer titles built around what changed, what is rare, what is newly meaningful, or what reshapes the season narrative.\n\n"
        "## Metric Link Rules\n\n"
        "- The ending \"you may also like\" section should contain 6-8 links.\n"
        "- If the body mentions a metric, ranking, leaderboard, streak, or player/game page, include it again in the ending list.\n"
        "- Then add adjacent useful links until the list reaches 6-8 items.\n\n"
        "## Image Pool\n\n"
        "Each post supports an image pool. Include an `images` array in the POST /api/content/posts payload.\n"
        "You MUST reference the intended images in `content_raw` with slot placeholders like `[[IMAGE:slot=img1]]`.\n"
        "If you create images but omit the placeholders, the images may be unused and the published post can end up text-only.\n\n"
        "Available image types:\n"
        "- `player_headshot`: Official NBA player headshot. Provide `player_id`. Optional: `player_name` for review context.\n"
        "- `ai_generated`: Stylized supporting art only. Provide a `prompt` in English.\n"
        "  When possible, also provide `reference_query` or `reference_url` so the AI image is based on a real game photo first, then stylized.\n"
        "  Do not use it for photorealistic player portraits, exact jersey numbers, or exact team logos.\n"
        "- `screenshot`: Funba page capture. Provide a `target` URL (e.g. metric ranking page, game page).\n"
        "- `web_search`: Search for a real photo. Provide a `query` in English.\n"
        "  Avoid watermarked sources. Prefer official/editorial photos over social screenshots.\n\n"
        "Per post, aim for a mix like:\n"
        "- 3-4 `screenshot` images (rankings, game pages, player pages, metric pages)\n"
        "- 3-4 `web_search` real game/editorial photos\n"
        "- 2-3 `ai_generated` stylized variants, preferably with `reference_query` or `reference_url`\n"
        "- `player_headshot` is allowed, but do not rely on it as the main image set because it feels repetitive.\n\n"
        "For player-focused posts, the first image should usually be `web_search`, `screenshot`, or a strong stylized image derived from a real game photo.\n"
        "Example images array:\n"
        "```json\n"
        "\"images\": [\n"
        "  {\"slot\": \"img1\", \"type\": \"web_search\", \"query\": \"Tyrese Maxey driving to the basket vs Wizards April 2026\", \"max\": 3, \"note\": \"马克西比赛现场图\"},\n"
        "  {\"slot\": \"img2\", \"type\": \"screenshot\", \"target\": \"https://funba.app/metrics/scoring_consistency?season=22025\", \"note\": \"得分稳定性排行\"},\n"
        "  {\"slot\": \"img3\", \"type\": \"ai_generated\", \"prompt\": \"Tyrese Maxey exploding through the lane, comic-book energy, dramatic arena lights, dynamic motion, editorial sports poster\", \"reference_query\": \"Tyrese Maxey driving to the basket vs Wizards April 2026\", \"note\": \"马克西风格化赛场图\"}\n"
        "]\n"
        "```\n\n"
        "Each image gets a `note` (Chinese) shown to the admin reviewer. Admin can enable/disable individual images before publishing.\n"
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
    pipeline = _pipeline_status_for_date(target_date)
    game_ids = pipeline["game_ids"]
    if not game_ids:
        return {"ok": False, "status": "no_games", "source_date": target_date.isoformat(), "game_ids": []}

    if not force and not pipeline["artifacts_ready"]:
        return {
            "ok": False,
            "status": "waiting_for_pipeline",
            "pipeline_stage": "artifacts",
            "source_date": target_date.isoformat(),
            "game_ids": game_ids,
            "pending_game_ids": pipeline["pending_game_ids"],
        }

    if not force and not _all_games_have_metrics(game_ids):
        return {
            "ok": False,
            "status": "waiting_for_pipeline",
            "pipeline_stage": "metrics",
            "source_date": target_date.isoformat(),
            "game_ids": game_ids,
        }

    cfg = load_paperclip_bridge_config()
    if cfg is None:
        raise PaperclipBridgeError("Paperclip bridge is unavailable.")

    client = PaperclipClient(cfg)
    cfg = client.discover_defaults()
    if not cfg.project_id:
        raise PaperclipBridgeError("Could not resolve Funba project in Paperclip.")
    if not cfg.content_analyst_agent_id:
        raise PaperclipBridgeError("Could not resolve Content Analyst agent in Paperclip.")

    title = _build_daily_analysis_title(target_date)
    existing = client.list_issues(q=title, project_id=cfg.project_id)
    exact_matches = [
        issue
        for issue in existing
        if str(issue.get("title") or "").strip() == title
        and str(issue.get("status") or "").strip() in {"backlog", "todo", "in_progress", "in_review", "done", "blocked"}
    ]
    if exact_matches and not force:
        chosen = exact_matches[0]
        return {
            "ok": True,
            "status": "exists",
            "source_date": target_date.isoformat(),
            "issue_id": chosen.get("id"),
            "issue_identifier": chosen.get("identifier"),
            "game_ids": game_ids,
        }

    if exact_matches and force:
        # Close the old issue so the agent gets a fresh context
        chosen = exact_matches[0]
        client.update_issue(chosen.get("id"), {"status": "cancelled"})

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


def ensure_recent_content_analysis(source_dates: list[date], *, force: bool = False) -> dict:
    deduped_dates = []
    seen: set[date] = set()
    for target_date in source_dates:
        if target_date in seen:
            continue
        seen.add(target_date)
        deduped_dates.append(target_date)

    results = []
    for target_date in deduped_dates:
        results.append(ensure_daily_content_analysis_issue(target_date, force=force))

    return {
        "ok": True,
        "checked_dates": [d.isoformat() for d in deduped_dates],
        "results": results,
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
        else _recent_target_dates(lookback_days)
    )
    try:
        result = ensure_recent_content_analysis(target_dates, force=force)
        logger.info("recent content analysis readiness checked for %s", result.get("checked_dates"))
        return result
    except Exception as exc:
        logger.warning("recent content analysis readiness failed: %s", exc, exc_info=True)
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
    target_dates = _recent_game_dates_for_season(season, lookback_days=lookback_days)
    try:
        result = ensure_recent_content_analysis(target_dates, force=force)
        logger.info(
            "season content analysis readiness checked for season=%s dates=%s",
            season,
            result.get("checked_dates"),
        )
        return {
            **result,
            "season": season,
        }
    except Exception as exc:
        logger.warning("season content analysis readiness failed for %s: %s", season, exc, exc_info=True)
        raise self.retry(exc=exc, countdown=60)
