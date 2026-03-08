"""Runtime metric catalog: built-ins from the registry plus published DB rule metrics."""
from __future__ import annotations

import json
from typing import Iterable

from sqlalchemy.orm import Session, sessionmaker

from db.models import MetricDefinition as MetricDefinitionModel, engine
from metrics.framework import registry
from metrics.framework.base import CAREER_SEASON, MetricDefinition, MetricResult

SessionLocal = sessionmaker(bind=engine)


class RuleMetricDefinition(MetricDefinition):
    """Adapter that makes a DB-backed rule metric runnable by the existing runner."""

    incremental = False
    supports_career = False
    career = False
    career_name_suffix = " (Career)"
    career_min_sample: int | None = None

    def __init__(self, row: MetricDefinitionModel, *, career: bool = False):
        self._base_row = row
        self._base_key = row.key
        self._base_name = row.name
        self._base_description = row.description or ""
        self._base_min_sample = int(row.min_sample or 1)
        self.key = row.key
        self.name = row.name
        self.description = row.description or ""
        self.scope = row.scope
        self.category = row.category or ""
        self.min_sample = self._base_min_sample
        self.group_key = row.group_key
        self.source_type = row.source_type
        self.status = row.status
        self.definition = json.loads(row.definition_json or "{}")
        self.supports_career = bool(self.definition.get("supports_career", False))
        self.career_name_suffix = str(self.definition.get("career_name_suffix") or " (Career)")
        career_min_sample = self.definition.get("career_min_sample")
        self.career_min_sample = int(career_min_sample) if career_min_sample is not None else None
        self.career = career

        if self.career:
            self.key = self._base_key + "_career"
            self.name = self._base_name + self.career_name_suffix
            suffix = " Computed across all seasons."
            self.description = (self.description + suffix).strip() if self.description else suffix.strip()
            self.supports_career = False
            if self.career_min_sample is not None:
                self.min_sample = self.career_min_sample
            else:
                self.min_sample = max(self.min_sample * 5, self.min_sample)

    def make_career_sibling(self) -> RuleMetricDefinition | None:
        if not self.supports_career or self.scope == "game":
            return None
        return RuleMetricDefinition(self._base_row, career=True)

    def compute(
        self,
        session,
        entity_id: str | None,
        season: str | None,
        game_id: str | None = None,
    ) -> MetricResult | None:
        from metrics.framework.rule_engine import compute as rule_compute, compute_baseline

        if entity_id is None or season is None:
            return None

        target_season = CAREER_SEASON if self.career else season
        value = rule_compute(session, self.definition, entity_id, target_season, self.scope)
        if value is None:
            return None

        baseline = compute_baseline(session, self.definition, entity_id, target_season, self.scope)
        context = {}
        if baseline is not None:
            context["baseline"] = baseline

        return MetricResult(
            metric_key=self.key,
            entity_type=self.scope,
            entity_id=entity_id,
            season=target_season,
            game_id=game_id if self.scope == "game" else None,
            value_num=float(value),
            context=context,
        )


def _load_published_rule_metrics(session: Session) -> list[RuleMetricDefinition]:
    rows = (
        session.query(MetricDefinitionModel)
        .filter(
            MetricDefinitionModel.status == "published",
            MetricDefinitionModel.source_type == "rule",
        )
        .order_by(MetricDefinitionModel.created_at.asc(), MetricDefinitionModel.id.asc())
        .all()
    )
    metrics: list[RuleMetricDefinition] = []
    for row in rows:
        metric = RuleMetricDefinition(row)
        metrics.append(metric)
        sibling = metric.make_career_sibling()
        if sibling is not None:
            metrics.append(sibling)
    return metrics


def _dedupe_by_key(metrics: Iterable[MetricDefinition]) -> list[MetricDefinition]:
    seen: set[str] = set()
    merged: list[MetricDefinition] = []
    for metric in metrics:
        if metric.key in seen:
            continue
        seen.add(metric.key)
        merged.append(metric)
    return merged


def get_all_metrics(session: Session | None = None) -> list[MetricDefinition]:
    builtins = registry.get_all()
    if session is not None:
        return _dedupe_by_key([*builtins, *_load_published_rule_metrics(session)])

    with SessionLocal() as owned:
        return _dedupe_by_key([*builtins, *_load_published_rule_metrics(owned)])


def get_metric(key: str, session: Session | None = None) -> MetricDefinition | None:
    builtin = registry.get(key)
    if builtin is not None:
        return builtin

    def _load(sess: Session) -> MetricDefinition | None:
        for metric in _load_published_rule_metrics(sess):
            if metric.key == key:
                return metric
        return None

    if session is not None:
        return _load(session)

    with SessionLocal() as owned:
        return _load(owned)


def expand_metric_keys(metric_keys: Iterable[str], session: Session | None = None) -> list[str]:
    metrics = {m.key: m for m in get_all_metrics(session=session)}
    expanded: list[str] = []
    seen: set[str] = set()
    for key in metric_keys:
        if key in seen:
            continue
        seen.add(key)
        expanded.append(key)
        metric = metrics.get(key)
        if metric is None or metric.career or not getattr(metric, "supports_career", False):
            continue
        career_key = key + "_career"
        if career_key in metrics and career_key not in seen:
            seen.add(career_key)
            expanded.append(career_key)
    return expanded
