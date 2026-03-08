"""Runtime metric catalog: built-ins from the registry plus published DB rule metrics."""
from __future__ import annotations

import json
from typing import Iterable

from sqlalchemy.orm import Session, sessionmaker

from db.models import MetricDefinition as MetricDefinitionModel, engine
from metrics.framework import registry
from metrics.framework.base import MetricDefinition, MetricResult

SessionLocal = sessionmaker(bind=engine)


class RuleMetricDefinition(MetricDefinition):
    """Adapter that makes a DB-backed rule metric runnable by the existing runner."""

    incremental = False
    supports_career = False
    career = False

    def __init__(self, row: MetricDefinitionModel):
        self.key = row.key
        self.name = row.name
        self.description = row.description or ""
        self.scope = row.scope
        self.category = row.category or ""
        self.min_sample = int(row.min_sample or 1)
        self.group_key = row.group_key
        self.source_type = row.source_type
        self.status = row.status
        self.definition = json.loads(row.definition_json or "{}")

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

        value = rule_compute(session, self.definition, entity_id, season, self.scope)
        if value is None:
            return None

        baseline = compute_baseline(session, self.definition, entity_id, season, self.scope)
        context = {}
        if baseline is not None:
            context["baseline"] = baseline

        return MetricResult(
            metric_key=self.key,
            entity_type=self.scope,
            entity_id=entity_id,
            season=season,
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
    return [RuleMetricDefinition(row) for row in rows]


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
        row = (
            sess.query(MetricDefinitionModel)
            .filter(
                MetricDefinitionModel.key == key,
                MetricDefinitionModel.status == "published",
                MetricDefinitionModel.source_type == "rule",
            )
            .first()
        )
        return RuleMetricDefinition(row) if row is not None else None

    if session is not None:
        return _load(session)

    with SessionLocal() as owned:
        return _load(owned)
