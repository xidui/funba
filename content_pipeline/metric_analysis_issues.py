"""Metric content analysis issue creation and highlight computation.

Parallel to game_analysis_issues.py but for the metric data series.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from db.models import MetricResult as MetricResultModel, Player, Team

logger = logging.getLogger(__name__)

_ISSUE_TEMPLATE_PATH = Path(__file__).resolve().with_name("metric_content_analysis_issue.md")
_TITLE_FIELD_PREFIX = "TITLE:"


@dataclass
class MetricAnalysisIssueTemplate:
    title_template: str
    body_template: str


def load_metric_analysis_issue_template() -> MetricAnalysisIssueTemplate:
    raw_text = _ISSUE_TEMPLATE_PATH.read_text(encoding="utf-8")
    first_line, sep, remainder = raw_text.partition("\n")
    if not sep:
        raise RuntimeError(f"Metric analysis issue template is missing a body: {_ISSUE_TEMPLATE_PATH}")
    if not first_line.startswith(_TITLE_FIELD_PREFIX):
        raise RuntimeError(
            f"Metric analysis issue template must start with '{_TITLE_FIELD_PREFIX}': {_ISSUE_TEMPLATE_PATH}"
        )
    title_template = first_line[len(_TITLE_FIELD_PREFIX):].strip()
    body_template = remainder.lstrip("\n")
    return MetricAnalysisIssueTemplate(title_template=title_template, body_template=body_template)


def build_metric_analysis_issue_title(metric_key: str, metric_name: str) -> str:
    tpl = load_metric_analysis_issue_template()
    return tpl.title_template.format(metric_key=metric_key, metric_name=metric_name)


def compute_metric_highlights(session: Session, metric_key: str) -> dict:
    """Pre-compute top results across season views for the metric brief.

    Returns a dict with keys: current_season, all_regular, all_playoff, career.
    Each value is a list of {rank, entity_id, entity_label, season, value_num, value_str}.
    """
    from metrics.framework.base import is_career_season
    from metrics.framework.runtime import get_metric as _get_metric

    highlights: dict[str, list[dict]] = {}

    # Determine career key
    base_key = metric_key.removesuffix("_career")
    career_key = base_key + "_career"
    runtime_metric = _get_metric(base_key, session=session)
    has_career = runtime_metric is not None and getattr(runtime_metric, "supports_career", False)
    scope = getattr(runtime_metric, "scope", "player") if runtime_metric else "player"
    rank_order = getattr(runtime_metric, "rank_order", "desc") if runtime_metric else "desc"

    # Find current season (latest regular season with data)
    regular_seasons = sorted(
        [
            r[0] for r in session.query(MetricResultModel.season)
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.season.isnot(None),
                MetricResultModel.value_num.isnot(None),
            )
            .distinct()
            .all()
            if r[0] and len(r[0]) == 5 and r[0].isdigit() and r[0][0] == "2"
        ]
    )
    current_season = regular_seasons[-1] if regular_seasons else None

    def _top_n(metric_k: str, season_filter=None, season_prefix=None, limit: int = 5) -> list[dict]:
        q = session.query(MetricResultModel).filter(
            MetricResultModel.metric_key == metric_k,
            MetricResultModel.value_num.isnot(None),
        )
        if season_filter:
            q = q.filter(MetricResultModel.season == season_filter)
        elif season_prefix:
            q = q.filter(MetricResultModel.season.like(f"{season_prefix}%"))
        order_col = MetricResultModel.value_num.asc() if rank_order == "asc" else MetricResultModel.value_num.desc()
        q = q.order_by(order_col).limit(limit)
        results = []
        for i, r in enumerate(q.all(), 1):
            results.append({
                "rank": i,
                "entity_id": r.entity_id,
                "season": r.season,
                "value_num": r.value_num,
                "value_str": r.value_str,
            })
        return results

    # Current season top 5
    if current_season:
        highlights["current_season"] = {
            "season": current_season,
            "results": _top_n(metric_key, season_filter=current_season),
        }

    # All regular seasons top 5
    highlights["all_regular"] = {
        "label": "All Regular Seasons",
        "results": _top_n(metric_key, season_prefix="2"),
    }

    # All playoffs top 5
    playoff_results = _top_n(metric_key, season_prefix="4")
    if playoff_results:
        highlights["all_playoff"] = {
            "label": "All Playoffs",
            "results": playoff_results,
        }

    # Career top 5 (if career variant exists)
    if has_career:
        career_results = _top_n(career_key, season_filter="all_regular")
        if career_results:
            highlights["career"] = {
                "label": "Career (All Regular Seasons)",
                "results": career_results,
            }

    return {
        "metric_key": metric_key,
        "current_season": current_season,
        "has_career": has_career,
        "scope": scope,
        "highlights": highlights,
    }


def _resolve_entity_labels(session: Session, scope: str, highlights_data: dict) -> dict[str, str]:
    """Bulk-resolve entity_ids to human-readable labels."""
    all_ids: set[str] = set()
    for view in highlights_data.get("highlights", {}).values():
        for r in view.get("results", []):
            if r.get("entity_id"):
                all_ids.add(r["entity_id"])
    if not all_ids:
        return {}

    labels: dict[str, str] = {}
    if scope in ("player", "player_franchise"):
        player_ids = {eid.split(":")[0] for eid in all_ids}
        for p in session.query(Player.player_id, Player.full_name).filter(Player.player_id.in_(player_ids)).all():
            labels[str(p.player_id)] = p.full_name
        if scope == "player_franchise":
            team_ids = {eid.split(":")[1] for eid in all_ids if ":" in eid}
            team_map = {str(t.team_id): (t.full_name or t.abbr) for t in session.query(Team).filter(Team.team_id.in_(team_ids)).all()}
            for eid in all_ids:
                if ":" in eid:
                    pid, tid = eid.split(":", 1)
                    labels[eid] = f"{labels.get(pid, pid)} — {team_map.get(tid, tid)}"
    elif scope == "team":
        for t in session.query(Team.team_id, Team.full_name, Team.abbr).filter(Team.team_id.in_(all_ids)).all():
            labels[str(t.team_id)] = t.full_name or t.abbr
    elif scope == "season":
        for eid in all_ids:
            if len(eid) == 5 and eid.isdigit():
                type_names = {"1": "Pre Season", "2": "Regular Season", "3": "All Star", "4": "Playoffs", "5": "Play-In"}
                year = eid[1:]
                try:
                    next_yr = str(int(year) + 1)[-2:]
                    labels[eid] = f"{year}-{next_yr} {type_names.get(eid[0], '')}"
                except ValueError:
                    labels[eid] = eid
            else:
                labels[eid] = eid
    return labels


def _format_highlights_text(highlights_data: dict, labels: dict[str, str]) -> str:
    """Format highlights dict into human-readable text for the issue description."""
    def _label(entity_id: str) -> str:
        return labels.get(entity_id, entity_id)

    lines = []
    hl = highlights_data.get("highlights", {})

    if "current_season" in hl:
        cs = hl["current_season"]
        lines.append(f"### Current Season ({cs['season']})")
        for r in cs["results"]:
            lines.append(f"  {r['rank']}. {_label(r['entity_id'])} — {r['value_str'] or r['value_num']}")
        lines.append("")

    if "all_regular" in hl:
        ar = hl["all_regular"]
        lines.append(f"### {ar['label']}")
        for r in ar["results"]:
            season_label = r.get("season", "")
            lines.append(f"  {r['rank']}. {_label(r['entity_id'])} ({season_label}) — {r['value_str'] or r['value_num']}")
        lines.append("")

    if "all_playoff" in hl:
        ap = hl["all_playoff"]
        lines.append(f"### {ap['label']}")
        for r in ap["results"]:
            season_label = r.get("season", "")
            lines.append(f"  {r['rank']}. {_label(r['entity_id'])} ({season_label}) — {r['value_str'] or r['value_num']}")
        lines.append("")

    if "career" in hl:
        c = hl["career"]
        lines.append(f"### {c['label']}")
        for r in c["results"]:
            lines.append(f"  {r['rank']}. {_label(r['entity_id'])} — {r['value_str'] or r['value_num']}")
        lines.append("")

    return "\n".join(lines) if lines else "(no highlights available)"


def build_metric_analysis_issue_description(
    session: Session,
    metric_key: str,
    metric_name: str,
    metric_name_zh: str,
    metric_description: str,
    metric_scope: str,
    metric_page_url: str,
    enabled_platforms: list[str],
) -> str:
    """Build the full issue description with pre-computed highlights."""
    highlights_data = compute_metric_highlights(session, metric_key)
    labels = _resolve_entity_labels(session, highlights_data["scope"], highlights_data)
    highlights_text = _format_highlights_text(highlights_data, labels)

    tpl = load_metric_analysis_issue_template()
    return tpl.body_template.format(
        metric_key=metric_key,
        metric_name=metric_name,
        metric_name_zh=metric_name_zh or metric_name,
        metric_description=metric_description,
        metric_scope=metric_scope,
        metric_page_url=metric_page_url,
        has_career=str(highlights_data["has_career"]),
        highlights_text=highlights_text,
        enabled_platforms=", ".join(enabled_platforms) if enabled_platforms else "(none)",
    )
