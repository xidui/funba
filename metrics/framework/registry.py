"""Central registry of all active MetricDefinitions."""
from __future__ import annotations

from metrics.framework.base import CAREER_SEASON, MetricDefinition

_registry: dict[str, MetricDefinition] = {}


def _make_career_sibling(season_metric: MetricDefinition) -> MetricDefinition:
    """Auto-create a career variant of a season metric."""
    base_cls = type(season_metric)
    career_min = (
        season_metric.career_min_sample
        if season_metric.career_min_sample is not None
        else season_metric.min_sample * 5
    )

    class _CareerVariant(base_cls):  # type: ignore[valid-type]
        key = season_metric.key + "_career"
        name = season_metric.name + season_metric.career_name_suffix
        description = season_metric.description + " Computed across all seasons."
        career = True
        supports_career = False
        min_sample = career_min

    _CareerVariant.__name__ = base_cls.__name__ + "Career"
    _CareerVariant.__qualname__ = base_cls.__qualname__ + "Career"
    return _CareerVariant()


def register(metric: MetricDefinition) -> None:
    """Register a metric instance. Auto-registers career sibling if supports_career=True."""
    if metric.key in _registry:
        raise ValueError(f"Metric key already registered: {metric.key!r}")
    _registry[metric.key] = metric

    if metric.supports_career and not metric.career:
        sibling = _make_career_sibling(metric)
        if sibling.key not in _registry:
            _registry[sibling.key] = sibling


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
    import metrics.definitions.player.true_shooting_pct     # noqa: F401
    import metrics.definitions.player.assist_to_turnover_ratio  # noqa: F401
    import metrics.definitions.player.three_point_reliance  # noqa: F401
    import metrics.definitions.player.paint_scoring_share   # noqa: F401
    import metrics.definitions.team.win_pct_leading_at_half  # noqa: F401
    import metrics.definitions.team.close_game_record       # noqa: F401
    import metrics.definitions.team.bench_scoring_share     # noqa: F401
    import metrics.definitions.team.home_court_advantage    # noqa: F401
    import metrics.definitions.team.blowout_rate            # noqa: F401
    import metrics.definitions.team.road_win_pct            # noqa: F401
    import metrics.definitions.team.comeback_win_pct        # noqa: F401
    import metrics.definitions.game.multi_20pt_game         # noqa: F401
    import metrics.definitions.game.combined_score          # noqa: F401
    import metrics.definitions.game.lead_changes            # noqa: F401


_load_all()
