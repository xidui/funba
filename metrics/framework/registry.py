"""Legacy compatibility shims for old metric code imports.

Runtime metric discovery is DB-only. This module remains only so older
code_metric source strings that import `register` do not crash when loaded.
"""
from __future__ import annotations

from metrics.framework.base import MetricDefinition


def register(metric: MetricDefinition) -> None:
    """No-op compatibility shim."""
    return None


def get(key: str) -> MetricDefinition | None:
    return None


def get_all() -> list[MetricDefinition]:
    return []


def get_by_scope(scope: str) -> list[MetricDefinition]:
    return []


def get_asc_metric_keys() -> set[str]:
    return set()
