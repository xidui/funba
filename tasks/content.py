from __future__ import annotations

import logging
import re
from datetime import date, timedelta

from celery import shared_task
from sqlalchemy.orm import sessionmaker

from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.models import Game, MetricRunLog, SocialPost, Team, engine
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


def _game_pipeline_status_for_date(target_date: date) -> dict:
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
                "ready_game_ids": [],
                "pending_artifact_game_ids": [],
                "pending_metric_game_ids": [],
            }

        game_ids = [game.game_id for game in games]
        computed_game_ids = {
            row.game_id
            for row in session.query(MetricRunLog.game_id)
            .filter(MetricRunLog.game_id.in_(game_ids))
            .distinct()
            .all()
        }

        ready_game_ids: list[str] = []
        pending_artifact_game_ids: list[str] = []
        pending_metric_game_ids: list[str] = []
        for game in games:
            artifacts_supported = _artifacts_available_from_nba_api(game.season)
            has_detail = is_game_detail_back_filled(game.game_id, session)
            has_pbp = True if not artifacts_supported else is_game_pbp_back_filled(game.game_id, session)
            if not (has_detail and has_pbp):
                pending_artifact_game_ids.append(game.game_id)
                continue
            if game.game_id not in computed_game_ids:
                pending_metric_game_ids.append(game.game_id)
                continue
            ready_game_ids.append(game.game_id)

        return {
            "game_ids": game_ids,
            "ready_game_ids": ready_game_ids,
            "pending_artifact_game_ids": pending_artifact_game_ids,
            "pending_metric_game_ids": pending_metric_game_ids,
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


def _game_analysis_title_base(target_date: date, game_id: str) -> str:
    return f"Game content analysis — funba — {target_date.isoformat()} — {game_id}"


def _game_context(game_id: str) -> dict:
    with _session_factory()() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            return {"game_id": game_id, "matchup": game_id, "season": None}
        team_ids = [tid for tid in [game.road_team_id, game.home_team_id] if tid]
        teams = {}
        if team_ids:
            rows = session.query(Team.team_id, Team.abbr).filter(Team.team_id.in_(team_ids)).all()
            teams = {str(team_id): abbr for team_id, abbr in rows if team_id}
        road = teams.get(str(game.road_team_id), str(game.road_team_id or "?"))
        home = teams.get(str(game.home_team_id), str(game.home_team_id or "?"))
        return {
            "game_id": game_id,
            "matchup": f"{road} @ {home}",
            "season": game.season,
        }


def _build_game_analysis_title(target_date: date, game_id: str) -> str:
    ctx = _game_context(game_id)
    return f"{_game_analysis_title_base(target_date, game_id)} — {ctx['matchup']}"


def _build_game_analysis_description(target_date: date, game_id: str) -> str:
    ctx = _game_context(game_id)
    return (
        "Run a fresh analysis for this single game only.\n\n"
        f"Source date: {target_date.isoformat()}\n"
        f"Game ID: {game_id}\n"
        f"Matchup: {ctx['matchup']}\n"
        f"Season: {ctx['season'] or '(unknown)'}\n\n"
        "Rules:\n"
        "1. Start only from current Funba localhost APIs.\n"
        "2. Create at most 1-2 strong posts for this game.\n"
        "3. Keep all variants tied to this same game only.\n"
        "4. Avoid duplicate angles against existing posts for the same game via `GET /api/content/posts?date=YYYY-MM-DD`.\n"
        "5. Before calling `POST /api/content/posts`, prepare all image assets yourself and pass them through `images[].file_path`.\n"
        "6. Use slot-based placeholders only: `[[IMAGE:slot=img1]]`.\n"
        "7. End each post with 6-8 metric / page links. Every metric or page mentioned in the body should appear in that ending list.\n"
        "8. Leave resulting posts in Funba as `ai_review`.\n"
        "9. Do not publish externally from this issue.\n\n"
        "Image notes:\n"
        "- Funba stores provided image files; it does not search, generate, or capture them for you.\n"
        "- `type`, `query`, `target`, `prompt`, `player_id`, and `player_name` are provenance metadata only.\n"
        "- `file_path` is required for each image asset.\n"
    )


def _covered_game_ids_for_date(target_date: date) -> set[str]:
    with _session_factory()() as session:
        rows = session.query(SocialPost.source_game_ids).filter(
            SocialPost.source_date == target_date,
            SocialPost.status != "archived",
        ).all()
    covered: set[str] = set()
    for (raw_game_ids,) in rows:
        try:
            parsed = json.loads(raw_game_ids) if raw_game_ids else []
        except Exception:
            parsed = []
        for game_id in parsed:
            if game_id:
                covered.add(str(game_id))
    return covered


_GAME_ANALYSIS_RE = re.compile(r"^Game content analysis — funba — (\d{4}-\d{2}-\d{2}) — (\d+)(?:\s+—\s+.+)?$")


def _matching_game_analysis_issues(target_date: date, game_id: str, issues: list[dict]) -> list[dict]:
    expected_date = target_date.isoformat()
    matched = []
    for issue in issues:
        title = str(issue.get("title") or "").strip()
        status = str(issue.get("status") or "").strip()
        if status not in {"backlog", "todo", "in_progress", "in_review", "done", "blocked"}:
            continue
        m = _GAME_ANALYSIS_RE.match(title)
        if not m or m.group(1) != expected_date or m.group(2) != str(game_id):
            continue
        matched.append(issue)
    return matched


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


def ensure_game_content_analysis_issues(target_date: date, *, force: bool = False) -> dict:
    pipeline = _game_pipeline_status_for_date(target_date)
    game_ids = pipeline["game_ids"]
    if not game_ids:
        return {"ok": False, "status": "no_games", "source_date": target_date.isoformat(), "game_ids": []}

    cfg = load_paperclip_bridge_config()
    if cfg is None:
        raise PaperclipBridgeError("Paperclip bridge is unavailable.")

    client = PaperclipClient(cfg)
    cfg = client.discover_defaults()
    if not cfg.project_id:
        raise PaperclipBridgeError("Could not resolve Funba project in Paperclip.")
    if not cfg.content_analyst_agent_id:
        raise PaperclipBridgeError("Could not resolve Content Analyst agent in Paperclip.")

    issues = client.list_issues(q=f"Game content analysis — funba — {target_date.isoformat()}", project_id=cfg.project_id)
    covered_game_ids = _covered_game_ids_for_date(target_date)
    results: list[dict] = []

    for game_id in game_ids:
        if game_id in pipeline["pending_artifact_game_ids"]:
            results.append({
                "ok": False,
                "status": "waiting_for_pipeline",
                "pipeline_stage": "artifacts",
                "source_date": target_date.isoformat(),
                "game_id": game_id,
            })
            continue
        if game_id in pipeline["pending_metric_game_ids"]:
            results.append({
                "ok": False,
                "status": "waiting_for_pipeline",
                "pipeline_stage": "metrics",
                "source_date": target_date.isoformat(),
                "game_id": game_id,
            })
            continue

        matching_issues = _matching_game_analysis_issues(target_date, game_id, issues)
        if not force and game_id in covered_game_ids:
            chosen = matching_issues[0] if matching_issues else None
            results.append({
                "ok": True,
                "status": "already_covered",
                "source_date": target_date.isoformat(),
                "game_id": game_id,
                "issue_id": chosen.get("id") if chosen else None,
                "issue_identifier": chosen.get("identifier") if chosen else None,
            })
            continue
        if matching_issues and not force:
            chosen = matching_issues[0]
            results.append({
                "ok": True,
                "status": "exists",
                "source_date": target_date.isoformat(),
                "game_id": game_id,
                "issue_id": chosen.get("id"),
                "issue_identifier": chosen.get("identifier"),
            })
            continue
        if matching_issues and force:
            for issue in matching_issues:
                client.update_issue(issue.get("id"), {"status": "cancelled"})

        issue = client.create_issue(
            {
                "projectId": cfg.project_id,
                "title": _build_game_analysis_title(target_date, game_id),
                "description": _build_game_analysis_description(target_date, game_id),
                "status": "todo",
                "priority": "medium",
                "assigneeAgentId": cfg.content_analyst_agent_id,
            }
        )
        results.append({
            "ok": True,
            "status": "created",
            "source_date": target_date.isoformat(),
            "game_id": game_id,
            "issue_id": issue.get("id"),
            "issue_identifier": issue.get("identifier"),
        })

    if not results:
        return {"ok": False, "status": "no_games", "source_date": target_date.isoformat(), "game_ids": []}

    created = [r for r in results if r.get("status") == "created"]
    existing = [r for r in results if r.get("status") == "exists"]
    already_covered = [r for r in results if r.get("status") == "already_covered"]
    waiting = [r for r in results if r.get("status") == "waiting_for_pipeline"]

    overall_status = "created" if created else "exists" if existing else "already_covered" if already_covered else "waiting_for_pipeline"
    first_issue = next((r for r in results if r.get("issue_identifier")), None)
    return {
        "ok": True if created or existing or already_covered else False,
        "status": overall_status,
        "source_date": target_date.isoformat(),
        "game_ids": game_ids,
        "results": results,
        "created_count": len(created),
        "existing_count": len(existing),
        "covered_count": len(already_covered),
        "waiting_count": len(waiting),
        "issue_id": first_issue.get("issue_id") if first_issue else None,
        "issue_identifier": first_issue.get("issue_identifier") if first_issue else None,
    }


# Backward-compatible alias while callers migrate off the old name.
ensure_daily_content_analysis_issue = ensure_game_content_analysis_issues


def ensure_recent_game_content_analysis(source_dates: list[date], *, force: bool = False) -> dict:
    deduped_dates = []
    seen: set[date] = set()
    for target_date in source_dates:
        if target_date in seen:
            continue
        seen.add(target_date)
        deduped_dates.append(target_date)

    results = []
    for target_date in deduped_dates:
        results.append(ensure_game_content_analysis_issues(target_date, force=force))

    return {
        "ok": True,
        "checked_dates": [d.isoformat() for d in deduped_dates],
        "results": results,
    }


# Backward-compatible alias while callers migrate off the old name.
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
        else _recent_target_dates(lookback_days)
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
    target_dates = _recent_game_dates_for_season(season, lookback_days=lookback_days)
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
