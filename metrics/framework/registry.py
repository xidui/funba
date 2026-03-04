"""Central registry of all active MetricDefinitions.

Import this module to access the global registry. Metric files register
themselves by calling `register()` at module import time.
"""
from __future__ import annotations

from typing import Iterator

from metrics.framework.base import MetricDefinition

_registry: dict[str, MetricDefinition] = {}


def register(metric: MetricDefinition) -> None:
    """Register a metric instance. Called once per metric module."""
    if metric.key in _registry:
        raise ValueError(f"Metric key already registered: {metric.key!r}")
    _registry[metric.key] = metric


def get(key: str) -> MetricDefinition | None:
    return _registry.get(key)


def get_all() -> list[MetricDefinition]:
    return list(_registry.values())


def get_by_scope(scope: str) -> list[MetricDefinition]:
    return [m for m in _registry.values() if m.scope == scope]


def _load_all() -> None:
    """Import all definition modules so they self-register."""
    import metrics.definitions.player.hot_hand              # noqa: F401
    import metrics.definitions.player.cold_streak_recovery  # noqa: F401
    import metrics.definitions.player.clutch_fg_pct         # noqa: F401
    import metrics.definitions.player.scoring_consistency   # noqa: F401
    import metrics.definitions.player.double_double_rate    # noqa: F401
    import metrics.definitions.player.franchise_scoring_rank  # noqa: F401
    import metrics.definitions.team.win_pct_leading_at_half  # noqa: F401
    import metrics.definitions.team.close_game_record       # noqa: F401
    import metrics.definitions.team.bench_scoring_share     # noqa: F401
    import metrics.definitions.game.multi_20pt_game         # noqa: F401


# Load all metrics on first import of the registry
_load_all()
