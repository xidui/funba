"""Ranking math for NewsCluster home-page ordering.

score = boost * decay

  boost = 1 + log10(1 + unique_view_count + ARTICLE_COUNT_WEIGHT * article_count)
  decay = exp(-age_hours / TIME_DECAY_HOURS)

Time dominates the formula: with TIME_DECAY_HOURS=8, a 24-hour-old cluster
decays by exp(-3) ≈ 0.050, so yesterday's important story can't outrank
today's mediocre one. See the sanity-check table in the plan.
"""
from __future__ import annotations

import math
from datetime import datetime

TIME_DECAY_HOURS = 8.0
ARTICLE_COUNT_WEIGHT = 3.0


def compute_score(
    *,
    last_seen_at: datetime,
    article_count: int,
    unique_view_count: int,
    now: datetime | None = None,
) -> float:
    now = now or datetime.utcnow()
    age_seconds = max(0.0, (now - last_seen_at).total_seconds())
    age_hours = age_seconds / 3600.0
    boost = 1.0 + math.log10(1.0 + max(0, unique_view_count) + ARTICLE_COUNT_WEIGHT * max(0, article_count))
    decay = math.exp(-age_hours / TIME_DECAY_HOURS)
    return boost * decay


def recompute_cluster_score(cluster, now: datetime | None = None) -> float:
    """Mutate cluster.score and score_refreshed_at. Returns the new score."""
    now = now or datetime.utcnow()
    score = compute_score(
        last_seen_at=cluster.last_seen_at,
        article_count=cluster.article_count or 0,
        unique_view_count=cluster.unique_view_count or 0,
        now=now,
    )
    cluster.score = score
    cluster.score_refreshed_at = now
    return score
