"""Metric content analysis issue creation and highlight computation.

Parallel to game_analysis_issues.py but for the metric data series.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import func
from sqlalchemy.orm import Session

from db.models import MetricResult as MetricResultModel, MetricDefinition as MetricDefinitionModel, Game

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
        q = q.order_by(MetricResultModel.value_num.desc()).limit(limit)
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


def _format_highlights_text(highlights_data: dict) -> str:
    """Format highlights dict into human-readable text for the issue description."""
    lines = []
    hl = highlights_data.get("highlights", {})

    if "current_season" in hl:
        cs = hl["current_season"]
        lines.append(f"### Current Season ({cs['season']})")
        for r in cs["results"]:
            lines.append(f"  {r['rank']}. {r['entity_id']} — {r['value_str'] or r['value_num']}")
        lines.append("")

    if "all_regular" in hl:
        ar = hl["all_regular"]
        lines.append(f"### {ar['label']}")
        for r in ar["results"]:
            season_label = r.get("season", "")
            lines.append(f"  {r['rank']}. {r['entity_id']} ({season_label}) — {r['value_str'] or r['value_num']}")
        lines.append("")

    if "all_playoff" in hl:
        ap = hl["all_playoff"]
        lines.append(f"### {ap['label']}")
        for r in ap["results"]:
            season_label = r.get("season", "")
            lines.append(f"  {r['rank']}. {r['entity_id']} ({season_label}) — {r['value_str'] or r['value_num']}")
        lines.append("")

    if "career" in hl:
        c = hl["career"]
        lines.append(f"### {c['label']}")
        for r in c["results"]:
            lines.append(f"  {r['rank']}. {r['entity_id']} — {r['value_str'] or r['value_num']}")
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
    highlights_text = _format_highlights_text(highlights_data)

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
