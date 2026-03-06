"""Rank-based noteworthiness scoring.

After metric results are persisted, compute each entity's percentile rank within
its (metric_key, entity_type, season) group. No AI required.

Score = 1 - (rank - 1) / total  → #1 scores 1.0, last scores ~0.
"""
from __future__ import annotations

import logging
import os

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from db.models import MetricResult as MetricResultModel
from metrics.framework.base import MetricResult

logger = logging.getLogger(__name__)

_SCORE_THRESHOLD = float(os.getenv("FUNBA_NOTEWORTHINESS_THRESHOLD", "0.75"))

# Top-N labels for notable_reason
_RANK_LABELS = {1: "Best", 2: "2nd best", 3: "3rd best"}


def rank_noteworthiness(
    session: Session,
    results: list[MetricResult],
) -> None:
    """Compute and persist rank-based noteworthiness for a batch of results.

    Queries the full distribution for each affected (metric_key, entity_type, season)
    group and updates the DB rows in bulk. Modifies result objects in-place too.
    """
    if not results:
        return

    # Collect distinct groups we need to rank
    groups: set[tuple[str, str, str | None]] = {
        (r.metric_key, r.entity_type, r.season) for r in results
    }

    # Build an index of result objects for quick lookup
    result_index: dict[tuple[str, str, str | None], MetricResult] = {
        (r.metric_key, r.entity_type, r.entity_id): r for r in results
    }

    for metric_key, entity_type, season in groups:
        # Query all rows for this group ordered by value descending
        rows = (
            session.query(
                MetricResultModel.entity_id,
                MetricResultModel.value_num,
            )
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.entity_type == entity_type,
                MetricResultModel.season == season,
                MetricResultModel.value_num.isnot(None),
            )
            .order_by(MetricResultModel.value_num.desc())
            .all()
        )

        total = len(rows)
        if total == 0:
            continue

        scope_label = {"player": "players", "team": "teams", "game": "games"}.get(entity_type, "entities")

        for rank, row in enumerate(rows, start=1):
            score = 1.0 - (rank - 1) / total

            label = _RANK_LABELS.get(rank, f"#{rank}")
            reason = f"{label} of {total} {scope_label} in this metric this season."

            # Update DB row
            session.query(MetricResultModel).filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.entity_type == entity_type,
                MetricResultModel.entity_id == row.entity_id,
                MetricResultModel.season == season,
            ).update(
                {"noteworthiness": round(score, 4), "notable_reason": reason},
                synchronize_session=False,
            )

            # Update in-memory result if it's one we just computed
            key = (metric_key, entity_type, row.entity_id)
            if key in result_index:
                result_index[key].noteworthiness = round(score, 4)
                result_index[key].notable_reason = reason


def rerank_all(session: Session, season: str) -> int:
    """Recompute noteworthiness for every MetricResult row in a season in one pass.

    More efficient than per-game ranking for bulk backfills — runs a single
    ranked query per (metric_key, entity_type) group and bulk-updates all rows.
    Returns the number of rows updated.
    """
    from sqlalchemy import text

    # Get distinct groups
    groups = (
        session.query(MetricResultModel.metric_key, MetricResultModel.entity_type)
        .filter(MetricResultModel.season == season)
        .distinct()
        .all()
    )

    total_updated = 0
    for metric_key, entity_type in groups:
        rows = (
            session.query(MetricResultModel.id, MetricResultModel.entity_id, MetricResultModel.value_num)
            .filter(
                MetricResultModel.metric_key == metric_key,
                MetricResultModel.entity_type == entity_type,
                MetricResultModel.season == season,
                MetricResultModel.value_num.isnot(None),
            )
            .order_by(MetricResultModel.value_num.desc())
            .all()
        )

        total = len(rows)
        if total == 0:
            continue

        scope_label = {"player": "players", "team": "teams", "game": "games"}.get(entity_type, "entities")

        for rank, row in enumerate(rows, start=1):
            score = round(1.0 - (rank - 1) / total, 4)
            label = _RANK_LABELS.get(rank, f"#{rank}")
            reason = f"{label} of {total} {scope_label} in this metric this season."
            session.query(MetricResultModel).filter(MetricResultModel.id == row.id).update(
                {"noteworthiness": score, "notable_reason": reason},
                synchronize_session=False,
            )
            total_updated += 1

        session.flush()

    session.commit()
    return total_updated


def is_notable(noteworthiness: float | None) -> bool:
    return noteworthiness is not None and noteworthiness >= _SCORE_THRESHOLD
