from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from sqlalchemy.orm import sessionmaker

from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.models import Game, MetricRunLog, SocialPost, Team, engine
from web.paperclip_bridge import PaperclipBridgeError, PaperclipClient, load_paperclip_bridge_config

logger = logging.getLogger(__name__)

_SessionLocal = sessionmaker(bind=engine)
_ISSUE_TEMPLATE_PATH = Path(__file__).resolve().with_name("game_content_analysis_issue.md")
_SUPPORTED_ISSUE_STATUSES = {"backlog", "todo", "in_progress", "in_review", "done", "blocked"}
_TITLE_FIELD_PREFIX = "TITLE:"
_LEGACY_GAME_ANALYSIS_TITLE_RE = re.compile(
    r"^Game content analysis — funba — (?P<source_date>\d{4}-\d{2}-\d{2}) — (?P<game_id>\d+)(?:\s+—\s+.+)?$"
)
_TITLE_PLACEHOLDER_PATTERNS = {
    "{source_date}": r"(?P<source_date>\d{4}-\d{2}-\d{2})",
    "{game_id}": r"(?P<game_id>\d+)",
    "{matchup}": r"(?P<matchup>.+?)",
    "{season_label}": r"(?P<season_label>.+?)",
}
_REQUIRED_TITLE_PLACEHOLDERS = ("{source_date}", "{game_id}", "{matchup}")


@dataclass(frozen=True)
class GameAnalysisIssueTemplate:
    title_template: str
    body_template: str


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


def game_pipeline_status_for_date(target_date: date) -> dict:
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


def recent_target_dates(lookback_days: int) -> list[date]:
    days = max(int(lookback_days), 1)
    return [date.today() - timedelta(days=offset) for offset in range(days)]


def recent_game_dates_for_season(season: str, lookback_days: int = 3) -> list[date]:
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


def game_context(game_id: str) -> dict:
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


@lru_cache(maxsize=1)
def load_game_analysis_issue_template() -> GameAnalysisIssueTemplate:
    raw_text = _ISSUE_TEMPLATE_PATH.read_text(encoding="utf-8")
    first_line, sep, remainder = raw_text.partition("\n")
    if not sep:
        raise RuntimeError(f"Game analysis issue template is missing a body: {_ISSUE_TEMPLATE_PATH}")
    if not first_line.startswith(_TITLE_FIELD_PREFIX):
        raise RuntimeError(
            f"Game analysis issue template must start with '{_TITLE_FIELD_PREFIX}': {_ISSUE_TEMPLATE_PATH}"
        )
    title_template = first_line[len(_TITLE_FIELD_PREFIX):].strip()
    body_template = remainder.lstrip("\n")
    if not title_template or not body_template.strip():
        raise RuntimeError(f"Game analysis issue template is incomplete: {_ISSUE_TEMPLATE_PATH}")
    missing_title_placeholders = [token for token in _REQUIRED_TITLE_PLACEHOLDERS if token not in title_template]
    if missing_title_placeholders:
        missing_text = ", ".join(missing_title_placeholders)
        raise RuntimeError(
            f"Game analysis issue title template is missing required placeholders ({missing_text}): {_ISSUE_TEMPLATE_PATH}"
        )
    return GameAnalysisIssueTemplate(title_template=title_template, body_template=body_template)


@lru_cache(maxsize=1)
def game_analysis_issue_title_regex() -> re.Pattern[str]:
    title_template = load_game_analysis_issue_template().title_template
    pattern = re.escape(title_template)
    for placeholder, replacement in _TITLE_PLACEHOLDER_PATTERNS.items():
        pattern = pattern.replace(re.escape(placeholder), replacement)
    return re.compile(rf"^{pattern}$")


def build_game_analysis_issue_title(target_date: date, game_id: str) -> str:
    ctx = game_context(game_id)
    return load_game_analysis_issue_template().title_template.format(
        source_date=target_date.isoformat(),
        game_id=game_id,
        matchup=ctx["matchup"],
        season_label=ctx["season"] or "(unknown)",
    )


def build_game_analysis_issue_description(target_date: date, game_id: str) -> str:
    ctx = game_context(game_id)
    return load_game_analysis_issue_template().body_template.format(
        source_date=target_date.isoformat(),
        game_id=game_id,
        matchup=ctx["matchup"],
        season_label=ctx["season"] or "(unknown)",
    )


def game_analysis_issue_search_query(target_date: date) -> str:
    title_template = load_game_analysis_issue_template().title_template
    prefix, separator, _suffix = title_template.partition("{source_date}")
    if separator:
        return f"{prefix}{target_date.isoformat()}".strip()
    return target_date.isoformat()


def covered_game_ids_for_date(target_date: date) -> set[str]:
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


def matching_game_analysis_issues(target_date: date, game_id: str, issues: list[dict]) -> list[dict]:
    expected_date = target_date.isoformat()
    expected_game_id = str(game_id)
    title_regex = game_analysis_issue_title_regex()
    matched = []
    for issue in issues:
        title = str(issue.get("title") or "").strip()
        status = str(issue.get("status") or "").strip()
        if status not in _SUPPORTED_ISSUE_STATUSES:
            continue
        match = title_regex.match(title) or _LEGACY_GAME_ANALYSIS_TITLE_RE.match(title)
        if not match:
            continue
        if match.groupdict().get("source_date") != expected_date:
            continue
        if match.groupdict().get("game_id") != expected_game_id:
            continue
        matched.append(issue)
    return matched


def build_daily_analysis_rerun_comment(target_date: date, game_ids: list[str]) -> str:
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
    pipeline = game_pipeline_status_for_date(target_date)
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

    issues = client.list_issues(q=game_analysis_issue_search_query(target_date), project_id=cfg.project_id)
    covered_game_ids = covered_game_ids_for_date(target_date)
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

        matching_issues = matching_game_analysis_issues(target_date, game_id, issues)
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
                "title": build_game_analysis_issue_title(target_date, game_id),
                "description": build_game_analysis_issue_description(target_date, game_id),
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

    overall_status = (
        "created"
        if created
        else "exists"
        if existing
        else "already_covered"
        if already_covered
        else "waiting_for_pipeline"
    )
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


__all__ = [
    "build_daily_analysis_rerun_comment",
    "build_game_analysis_issue_description",
    "build_game_analysis_issue_title",
    "covered_game_ids_for_date",
    "ensure_game_content_analysis_issues",
    "ensure_recent_game_content_analysis",
    "game_analysis_issue_search_query",
    "game_analysis_issue_title_regex",
    "game_context",
    "game_pipeline_status_for_date",
    "load_game_analysis_issue_template",
    "matching_game_analysis_issues",
    "recent_game_dates_for_season",
    "recent_target_dates",
]
