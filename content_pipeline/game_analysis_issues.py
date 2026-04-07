from __future__ import annotations

import json
import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError as SAOperationalError
from sqlalchemy.exc import ProgrammingError as SAProgrammingError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from db.backfill_nba_game_detail import is_game_detail_back_filled
from db.backfill_nba_game_pbp import is_game_pbp_back_filled
from db.config import get_database_url
from db.models import (
    Game,
    GameContentAnalysisIssue,
    GameContentAnalysisIssuePost,
    MetricRunLog,
    SocialPost,
    Team,
    engine,
)
from web.paperclip_bridge import PaperclipBridgeError, PaperclipClient, load_paperclip_bridge_config

logger = logging.getLogger(__name__)

_SessionLocal = sessionmaker(bind=engine)
# Workers can run with DB_POOL_SIZE=1. Advisory locks must not occupy that
# single pooled connection while the analysis code opens ORM sessions.
_LOCK_ENGINE = create_engine(get_database_url(), poolclass=NullPool, pool_pre_ping=True)
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


@dataclass(frozen=True)
class GameAnalysisIssueRecordView:
    id: int
    game_id: str
    source_date: str
    issue_id: str
    issue_identifier: str | None
    issue_status: str | None
    title: str
    trigger_source: str
    created_at: str
    updated_at: str
    posts: tuple[dict[str, object], ...] = ()


class AdvisoryLockUnavailable(RuntimeError):
    """Raised when the game-analysis creation lock cannot be acquired."""


def _session_factory():
    return _SessionLocal


def _game_analysis_lock_name(target_date: date) -> str:
    return f"gca:{target_date.isoformat()}"


@contextmanager
def _game_analysis_issue_creation_lock(target_date: date, timeout_seconds: int = 15):
    lock_name = _game_analysis_lock_name(target_date)
    with _LOCK_ENGINE.connect() as lock_conn:
        acquired = lock_conn.execute(
            text("SELECT GET_LOCK(:name, :timeout_seconds)"),
            {"name": lock_name, "timeout_seconds": int(timeout_seconds)},
        ).scalar()
        if acquired != 1:
            raise AdvisoryLockUnavailable(f"Failed to acquire game-analysis lock {lock_name!r}")
        try:
            yield
        finally:
            try:
                lock_conn.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name})
            except Exception:
                logger.exception("Failed to release game-analysis lock %s", lock_name)


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


def game_analysis_readiness_detail(game_id: str) -> dict[str, object]:
    with _session_factory()() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            return {
                "game_id": game_id,
                "ready": False,
                "pipeline_stage": "artifacts",
                "message": f"Game {game_id} is not stored in the local Game table yet.",
                "missing_artifacts": ["Game"],
                "metric_run_count": 0,
            }

        artifacts_supported = _artifacts_available_from_nba_api(game.season)
        has_detail = is_game_detail_back_filled(game_id, session)
        has_pbp = True if not artifacts_supported else is_game_pbp_back_filled(game_id, session)
        missing_artifacts: list[str] = []
        if not has_detail:
            missing_artifacts.append("detail")
        if artifacts_supported and not has_pbp:
            missing_artifacts.append("PBP")

        metric_run_count = int(
            session.query(MetricRunLog.game_id)
            .filter(MetricRunLog.game_id == game_id)
            .count()
            or 0
        )

        if missing_artifacts:
            artifact_list = ", ".join(missing_artifacts)
            return {
                "game_id": game_id,
                "season": game.season,
                "ready": False,
                "pipeline_stage": "artifacts",
                "artifacts_supported": artifacts_supported,
                "has_detail": has_detail,
                "has_pbp": has_pbp,
                "missing_artifacts": missing_artifacts,
                "metric_run_count": metric_run_count,
                "message": f"Pipeline not ready: missing {artifact_list} for game {game_id}.",
            }

        if metric_run_count <= 0:
            return {
                "game_id": game_id,
                "season": game.season,
                "ready": False,
                "pipeline_stage": "metrics",
                "artifacts_supported": artifacts_supported,
                "has_detail": has_detail,
                "has_pbp": has_pbp,
                "missing_artifacts": [],
                "metric_run_count": 0,
                "message": f"Pipeline not ready: game {game_id} has detail/PBP, but no MetricRunLog rows yet.",
            }

        return {
            "game_id": game_id,
            "season": game.season,
            "ready": True,
            "pipeline_stage": "ready",
            "artifacts_supported": artifacts_supported,
            "has_detail": has_detail,
            "has_pbp": has_pbp,
            "missing_artifacts": [],
            "metric_run_count": metric_run_count,
            "message": f"Pipeline ready: game {game_id} has artifacts and {metric_run_count} MetricRunLog rows.",
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


def parse_game_analysis_issue_title(title: str) -> dict[str, str] | None:
    match = game_analysis_issue_title_regex().match(title) or _LEGACY_GAME_ANALYSIS_TITLE_RE.match(title)
    if not match:
        return None
    return {
        key: str(value)
        for key, value in match.groupdict().items()
        if value is not None
    }


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


def _normalize_issue_id(issue: dict) -> str | None:
    issue_id = str(issue.get("id") or "").strip()
    return issue_id or None


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _is_missing_issue_table_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return isinstance(exc, (SAProgrammingError, SAOperationalError)) and (
        "gamecontentanalysisissue" in text and ("doesn't exist" in text or "no such table" in text)
    )


def _game_analysis_issue_rows(game_id: str, *, source_date: date | None = None) -> list[GameContentAnalysisIssue]:
    try:
        with _session_factory()() as session:
            query = session.query(GameContentAnalysisIssue).filter(GameContentAnalysisIssue.game_id == game_id)
            if source_date is not None:
                query = query.filter(GameContentAnalysisIssue.source_date == source_date)
            rows = (
                query.order_by(
                    GameContentAnalysisIssue.created_at.desc(),
                    GameContentAnalysisIssue.id.desc(),
                ).all()
            )
            return rows
    except Exception as exc:
        if _is_missing_issue_table_error(exc):
            logger.warning("GameContentAnalysisIssue table is not available yet; falling back to remote-only lookup.")
            return []
        raise


def game_analysis_issue_history(game_id: str) -> list[GameAnalysisIssueRecordView]:
    rows = _game_analysis_issue_rows(game_id)
    issue_ids = [int(row.id) for row in rows]
    linked_posts_by_issue: dict[int, list[dict[str, object]]] = {}
    if issue_ids:
        try:
            with _session_factory()() as session:
                joined_rows = (
                    session.query(GameContentAnalysisIssuePost, SocialPost)
                    .join(SocialPost, SocialPost.id == GameContentAnalysisIssuePost.post_id)
                    .filter(GameContentAnalysisIssuePost.issue_record_id.in_(issue_ids))
                    .order_by(GameContentAnalysisIssuePost.issue_record_id.asc(), SocialPost.id.asc())
                    .all()
                )
            for link_row, post in joined_rows:
                linked_posts_by_issue.setdefault(int(link_row.issue_record_id), []).append(
                    {
                        "post_id": int(post.id),
                        "topic": str(post.topic or ""),
                        "status": str(post.status or ""),
                        "source_date": post.source_date.isoformat() if post.source_date else "",
                        "discovered_via": str(link_row.discovered_via or "unknown"),
                    }
                )
        except Exception as exc:
            if not _is_missing_issue_table_error(exc):
                raise
            logger.warning("GameContentAnalysisIssuePost table is not available yet; issue history will omit linked posts.")
    history: list[GameAnalysisIssueRecordView] = []
    for row in rows:
        history.append(
            GameAnalysisIssueRecordView(
                id=int(row.id),
                game_id=str(row.game_id),
                source_date=row.source_date.isoformat() if row.source_date else "",
                issue_id=str(row.paperclip_issue_id),
                issue_identifier=str(row.paperclip_issue_identifier) if row.paperclip_issue_identifier else None,
                issue_status=str(row.paperclip_issue_status) if row.paperclip_issue_status else None,
                title=str(row.title),
                trigger_source=str(row.trigger_source or "automatic"),
                created_at=row.created_at.isoformat() if row.created_at else "",
                updated_at=row.updated_at.isoformat() if row.updated_at else "",
                posts=tuple(linked_posts_by_issue.get(int(row.id), [])),
            )
        )
    return history


def _record_issue_snapshot(
    game_id: str,
    source_date: date,
    issue: dict,
    *,
    trigger_source: str,
) -> GameContentAnalysisIssue | None:
    issue_id = _normalize_issue_id(issue)
    if not issue_id:
        return None

    now = _utc_now_naive()
    try:
        with _session_factory()() as session:
            row = (
                session.query(GameContentAnalysisIssue)
                .filter(GameContentAnalysisIssue.paperclip_issue_id == issue_id)
                .first()
            )
            if row is None:
                row = GameContentAnalysisIssue(
                    game_id=game_id,
                    source_date=source_date,
                    paperclip_issue_id=issue_id,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            row.game_id = game_id
            row.source_date = source_date
            row.paperclip_issue_identifier = str(issue.get("identifier") or "").strip() or None
            row.paperclip_issue_status = str(issue.get("status") or "").strip() or None
            row.title = str(issue.get("title") or "").strip() or build_game_analysis_issue_title(source_date, game_id)
            row.trigger_source = trigger_source or row.trigger_source or "automatic"
            row.updated_at = now
            session.commit()
            session.refresh(row)
            return row
    except Exception as exc:
        if _is_missing_issue_table_error(exc):
            logger.warning("GameContentAnalysisIssue table is not available yet; skipping issue record write.")
            return None
        raise


def _record_issue_snapshots(
    game_id: str,
    source_date: date,
    issues: list[dict],
    *,
    trigger_source: str,
) -> list[GameContentAnalysisIssue]:
    recorded = []
    for issue in issues:
        row = _record_issue_snapshot(game_id, source_date, issue, trigger_source=trigger_source)
        if row is not None:
            recorded.append(row)
    return recorded


def _latest_issue_row(game_id: str, source_date: date) -> GameContentAnalysisIssue | None:
    rows = _game_analysis_issue_rows(game_id, source_date=source_date)
    return rows[0] if rows else None


def resolve_game_analysis_issue_record(
    *,
    analysis_issue_id: str | None = None,
    analysis_issue_identifier: str | None = None,
) -> GameContentAnalysisIssue | None:
    issue_id = str(analysis_issue_id or "").strip() or None
    issue_identifier = str(analysis_issue_identifier or "").strip() or None
    if not issue_id and not issue_identifier:
        return None

    try:
        with _session_factory()() as session:
            row = None
            if issue_id:
                row = (
                    session.query(GameContentAnalysisIssue)
                    .filter(GameContentAnalysisIssue.paperclip_issue_id == issue_id)
                    .first()
                )
            if row is None and issue_identifier:
                row = (
                    session.query(GameContentAnalysisIssue)
                    .filter(GameContentAnalysisIssue.paperclip_issue_identifier == issue_identifier)
                    .first()
                )
            if row is not None:
                session.expunge(row)
                return row
    except Exception as exc:
        if not _is_missing_issue_table_error(exc):
            raise

    cfg = load_paperclip_bridge_config()
    if cfg is None:
        raise PaperclipBridgeError("Paperclip bridge is unavailable.")
    client = PaperclipClient(cfg)
    cfg = client.discover_defaults()

    issue = None
    if issue_id:
        issue = client.get_issue(issue_id)
    elif issue_identifier:
        candidates = client.list_issues(q=issue_identifier, project_id=cfg.project_id)
        issue = next((item for item in candidates if str(item.get("identifier") or "").strip() == issue_identifier), None)

    if not issue:
        return None

    title = str(issue.get("title") or "").strip()
    parsed = parse_game_analysis_issue_title(title)
    if not parsed:
        return None

    source_date = datetime.strptime(parsed["source_date"], "%Y-%m-%d").date()
    game_id = parsed["game_id"]
    return _record_issue_snapshot(game_id, source_date, issue, trigger_source="backfill")


def link_post_to_game_analysis_issue(
    post_id: int,
    *,
    analysis_issue_id: str | None = None,
    analysis_issue_identifier: str | None = None,
    discovered_via: str = "api_create",
) -> GameContentAnalysisIssuePost | None:
    issue_row = resolve_game_analysis_issue_record(
        analysis_issue_id=analysis_issue_id,
        analysis_issue_identifier=analysis_issue_identifier,
    )
    if issue_row is None:
        return None

    now = _utc_now_naive()
    try:
        with _session_factory()() as session:
            row = (
                session.query(GameContentAnalysisIssuePost)
                .filter(
                    GameContentAnalysisIssuePost.issue_record_id == issue_row.id,
                    GameContentAnalysisIssuePost.post_id == int(post_id),
                )
                .first()
            )
            if row is None:
                row = GameContentAnalysisIssuePost(
                    issue_record_id=issue_row.id,
                    post_id=int(post_id),
                    discovered_via=discovered_via,
                    created_at=now,
                )
                session.add(row)
                session.commit()
                session.refresh(row)
            return row
    except Exception as exc:
        if _is_missing_issue_table_error(exc):
            logger.warning("GameContentAnalysisIssuePost table is not available yet; skipping issue/post link write.")
            return None
        raise


def _result_from_issue_row(
    row: GameContentAnalysisIssue,
    *,
    status: str,
    target_date: date,
    game_id: str,
) -> dict:
    return {
        "ok": True,
        "status": status,
        "source_date": target_date.isoformat(),
        "game_id": game_id,
        "issue_id": row.paperclip_issue_id,
        "issue_identifier": row.paperclip_issue_identifier,
        "issue_status": row.paperclip_issue_status,
        "db_issue_record_id": row.id,
    }


def _game_source_date_or_raise(game_id: str) -> date:
    with _session_factory()() as session:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None:
            raise ValueError(f"Game {game_id} not found")
        if game.game_date is None:
            raise ValueError(f"Game {game_id} does not have a game_date")
        return game.game_date


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


def _ensure_game_content_analysis_issue_for_game(
    *,
    target_date: date,
    game_id: str,
    force: bool,
    trigger_source: str,
    pipeline: dict,
    client: PaperclipClient,
    cfg,
    issues: list[dict],
    covered_game_ids: set[str],
) -> dict:
    if game_id in pipeline["pending_artifact_game_ids"]:
        return {
            "ok": False,
            "status": "waiting_for_pipeline",
            "pipeline_stage": "artifacts",
            "source_date": target_date.isoformat(),
            "game_id": game_id,
        }
    if game_id in pipeline["pending_metric_game_ids"]:
        return {
            "ok": False,
            "status": "waiting_for_pipeline",
            "pipeline_stage": "metrics",
            "source_date": target_date.isoformat(),
            "game_id": game_id,
        }

    matching_issues = matching_game_analysis_issues(target_date, game_id, issues)
    if matching_issues:
        _record_issue_snapshots(game_id, target_date, matching_issues, trigger_source=trigger_source)

    if not force and game_id in covered_game_ids:
        latest_row = _latest_issue_row(game_id, target_date)
        if latest_row is not None:
            return _result_from_issue_row(latest_row, status="already_covered", target_date=target_date, game_id=game_id)
        chosen = matching_issues[0] if matching_issues else None
        return {
            "ok": True,
            "status": "already_covered",
            "source_date": target_date.isoformat(),
            "game_id": game_id,
            "issue_id": chosen.get("id") if chosen else None,
            "issue_identifier": chosen.get("identifier") if chosen else None,
            "issue_status": chosen.get("status") if chosen else None,
        }

    latest_row = _latest_issue_row(game_id, target_date)
    if latest_row is not None and not force:
        return _result_from_issue_row(latest_row, status="exists", target_date=target_date, game_id=game_id)

    if matching_issues and not force:
        chosen = matching_issues[0]
        return {
            "ok": True,
            "status": "exists",
            "source_date": target_date.isoformat(),
            "game_id": game_id,
            "issue_id": chosen.get("id"),
            "issue_identifier": chosen.get("identifier"),
            "issue_status": chosen.get("status"),
        }

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
    recorded = _record_issue_snapshot(game_id, target_date, issue, trigger_source=trigger_source)
    if recorded is not None:
        result = _result_from_issue_row(recorded, status="created", target_date=target_date, game_id=game_id)
        result["created_count"] = 1
        return result
    return {
        "ok": True,
        "status": "created",
        "source_date": target_date.isoformat(),
        "game_id": game_id,
        "issue_id": issue.get("id"),
        "issue_identifier": issue.get("identifier"),
        "issue_status": issue.get("status"),
    }


def ensure_game_content_analysis_issues(target_date: date, *, force: bool = False) -> dict:
    with _game_analysis_issue_creation_lock(target_date):
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
            results.append(
                _ensure_game_content_analysis_issue_for_game(
                    target_date=target_date,
                    game_id=game_id,
                    force=force,
                    trigger_source="automatic",
                    pipeline=pipeline,
                    client=client,
                    cfg=cfg,
                    issues=issues,
                    covered_game_ids=covered_game_ids,
                )
            )

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


def ensure_game_content_analysis_issue_for_game(
    game_id: str,
    *,
    force: bool = False,
    trigger_source: str = "manual",
) -> dict:
    target_date = _game_source_date_or_raise(game_id)
    with _game_analysis_issue_creation_lock(target_date):
        pipeline = game_pipeline_status_for_date(target_date)
        if game_id not in set(pipeline.get("game_ids") or []):
            raise ValueError(f"Game {game_id} not found in pipeline for {target_date.isoformat()}")

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
        return _ensure_game_content_analysis_issue_for_game(
            target_date=target_date,
            game_id=game_id,
            force=force,
            trigger_source=trigger_source,
            pipeline=pipeline,
            client=client,
            cfg=cfg,
            issues=issues,
            covered_game_ids=covered_game_ids,
        )


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
    "ensure_game_content_analysis_issue_for_game",
    "ensure_game_content_analysis_issues",
    "ensure_recent_game_content_analysis",
    "game_analysis_issue_history",
    "game_analysis_issue_search_query",
    "game_analysis_issue_title_regex",
    "game_analysis_readiness_detail",
    "game_context",
    "game_pipeline_status_for_date",
    "link_post_to_game_analysis_issue",
    "load_game_analysis_issue_template",
    "matching_game_analysis_issues",
    "parse_game_analysis_issue_title",
    "recent_game_dates_for_season",
    "recent_target_dates",
    "resolve_game_analysis_issue_record",
]
