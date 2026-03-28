"""Runtime metric catalog backed by published DB rule/code metrics."""
from __future__ import annotations

import ast
import builtins as py_builtins
import json
import logging
from functools import lru_cache
from typing import Iterable

logger = logging.getLogger(__name__)

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from db.models import MetricDefinition as MetricDefinitionModel, MetricResult as MetricResultModel, MetricRunLog, Game, engine
from metrics.framework.base import (
    CAREER_SEASON,
    MetricDefinition,
    MetricResult,
    career_season_for,
    career_season_type_code,
    is_career_season,
)
from metrics.framework.family import (
    FAMILY_VARIANT_CAREER,
    FAMILY_VARIANT_SEASON,
    derive_career_description,
    derive_career_min_sample,
    derive_career_name,
    family_base_key,
    family_career_key,
    rule_supports_career,
)

SessionLocal = sessionmaker(bind=engine)

_ALLOWED_IMPORT_ROOTS = {
    "__future__",
    "collections",
    "dataclasses",
    "datetime",
    "db",
    "decimal",
    "enum",
    "fractions",
    "functools",
    "itertools",
    "json",
    "math",
    "metrics",
    "numpy",
    "operator",
    "pandas",
    "re",
    "sqlalchemy",
    "statistics",
    "string",
    "typing",
}
# The runtime already owns the SQLAlchemy session used by generated metrics, so they do
# not need direct engine construction helpers like `create_engine`. If we ever allow
# direct `sqlalchemy` imports here, the module would still have to be explicitly
# imported by generated code, generated metrics are constrained to read-only queries by
# convention, and any stricter ORM-specific control should be scoped as a follow-on
# ticket.
_BLOCKED_IMPORT_ROOTS = {"importlib", "os", "socket", "subprocess"}
_SAFE_BUILTINS = {
    "__build_class__": py_builtins.__build_class__,
    "Exception": Exception,
    "RuntimeError": RuntimeError,
    "TypeError": TypeError,
    "ValueError": ValueError,
    "abs": abs,
    "all": all,
    "any": any,
    "bool": bool,
    "classmethod": classmethod,
    "dict": dict,
    "enumerate": enumerate,
    "filter": filter,
    "float": float,
    "getattr": getattr,
    "hasattr": hasattr,
    "int": int,
    "isinstance": isinstance,
    "issubclass": issubclass,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "next": next,
    "object": object,
    "property": property,
    "range": range,
    "reversed": reversed,
    "round": round,
    "set": set,
    "setattr": setattr,
    "sorted": sorted,
    "staticmethod": staticmethod,
    "str": str,
    "sum": sum,
    "super": super,
    "tuple": tuple,
    "zip": zip,
}


class ReadOnlySession:
    """Proxy that exposes only read operations on a SQLAlchemy Session.

    Generated metric code receives this instead of the real session so it
    cannot INSERT, UPDATE, DELETE, or COMMIT — even via raw SQL.
    """

    _BLOCKED = frozenset({
        "add", "add_all", "delete", "merge", "bulk_save_objects",
        "bulk_insert_mappings", "bulk_update_mappings",
        "commit", "flush", "rollback",
        "execute",  # blocks raw text("DROP TABLE ...")
    })

    def __init__(self, session: Session):
        self._session = session

    def query(self, *args, **kwargs):
        return self._session.query(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self._session.get(*args, **kwargs)

    def scalar(self, *args, **kwargs):
        return self._session.scalar(*args, **kwargs)

    def scalars(self, *args, **kwargs):
        return self._session.scalars(*args, **kwargs)

    # Expose info/bind for helpers that inspect the session
    @property
    def info(self):
        return self._session.info

    @property
    def bind(self):
        return self._session.bind

    def __getattr__(self, name: str):
        if name in self._BLOCKED:
            raise PermissionError(
                f"Code metrics are read-only: session.{name}() is not allowed"
            )
        return getattr(self._session, name)


def _module_root(name: str | None) -> str:
    return (name or "").split(".", 1)[0]


def _raise_for_disallowed_import(module_name: str | None) -> None:
    root = _module_root(module_name)
    if root in _BLOCKED_IMPORT_ROOTS:
        raise ValueError(f"Import of {root!r} is not allowed in code metrics")
    if root and root not in _ALLOWED_IMPORT_ROOTS:
        raise ValueError(f"Import of {module_name!r} is not allowed in code metrics")


def _safe_import(name, globals=None, locals=None, fromlist=(), level=0):
    if level:
        raise ValueError("Relative imports are not allowed in code metrics")
    _raise_for_disallowed_import(name)
    return py_builtins.__import__(name, globals, locals, fromlist, level)


_SAFE_BUILTINS["__import__"] = _safe_import


def _validate_code_metric_ast(code: str) -> None:
    try:
        tree = ast.parse(code)
    except SyntaxError as exc:
        raise ValueError(f"Generated code has invalid syntax: {exc}") from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                _raise_for_disallowed_import(alias.name)
        elif isinstance(node, ast.ImportFrom):
            _raise_for_disallowed_import(node.module)
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "__import__":
            raise ValueError("Use of '__import__' is not allowed in code metrics")


def _validate_metric_instance(metric: MetricDefinition) -> None:
    missing = [
        attr for attr in ("key", "name", "description", "scope", "category")
        if not getattr(metric, attr, None)
    ]
    if missing:
        raise ValueError(
            "Generated metric is missing required attributes: " + ", ".join(missing)
        )

    if metric.scope not in {"game", "league", "player", "player_franchise", "team"}:
        raise ValueError(f"Generated metric has invalid scope: {metric.scope!r}")

    trigger = getattr(metric, "trigger", "game")
    if trigger not in {"game", "season"}:
        raise ValueError(f"Generated metric has invalid trigger: {trigger!r}")

    metric_cls = type(metric)
    if trigger == "season":
        if metric_cls.compute_season is MetricDefinition.compute_season:
            raise ValueError("Season-triggered metric must implement compute_season()")
    elif getattr(metric, "incremental", True):
        if metric_cls.compute_delta is MetricDefinition.compute_delta:
            raise ValueError("Generated incremental metric must implement compute_delta()")
        if metric_cls.compute_value is MetricDefinition.compute_value:
            raise ValueError("Generated incremental metric must implement compute_value()")
    elif metric_cls.compute is MetricDefinition.compute:
        raise ValueError("Generated non-incremental metric must implement compute()")


def _career_base_key(metric: MetricDefinition) -> str:
    return family_base_key(getattr(metric, "base_metric_key", None) or metric.key)


def _career_season_prefix(career_season: str) -> str | None:
    code = career_season_type_code(career_season)
    if not code:
        return None
    return f"{code}%"


def _career_ready_from_season_results(session: Session, base_key: str, career_season: str) -> bool:
    prefix = _career_season_prefix(career_season)
    if not prefix:
        return False

    expected = {
        season
        for (season,) in session.query(Game.season)
        .filter(Game.game_date.isnot(None), Game.season.like(prefix))
        .distinct()
        .all()
        if season
    }
    if not expected:
        return True

    available = {
        season
        for (season,) in session.query(MetricResultModel.season)
        .filter(MetricResultModel.metric_key == base_key, MetricResultModel.season.like(prefix))
        .distinct()
        .all()
        if season
    }
    return expected.issubset(available)


def _career_context_rows(session: Session, base_key: str, career_season: str) -> list[tuple[str, dict]]:
    prefix = _career_season_prefix(career_season)
    if not prefix:
        return []
    rows = (
        session.query(MetricResultModel.entity_id, MetricResultModel.context_json)
        .filter(
            MetricResultModel.metric_key == base_key,
            MetricResultModel.season.like(prefix),
            MetricResultModel.entity_id.isnot(None),
        )
        .all()
    )
    parsed: list[tuple[str, dict]] = []
    for entity_id, context_json in rows:
        if not entity_id:
            continue
        try:
            context = json.loads(context_json) if context_json else {}
        except Exception:
            context = {}
        parsed.append((str(entity_id), context))
    return parsed


def _career_context_rows_by_season(session: Session, base_key: str, career_season: str) -> list[tuple[str, str, dict]]:
    prefix = _career_season_prefix(career_season)
    if not prefix:
        return []
    rows = (
        session.query(MetricResultModel.entity_id, MetricResultModel.season, MetricResultModel.context_json)
        .filter(
            MetricResultModel.metric_key == base_key,
            MetricResultModel.season.like(prefix),
            MetricResultModel.entity_id.isnot(None),
        )
        .all()
    )
    parsed: list[tuple[str, str, dict]] = []
    for entity_id, season_value, context_json in rows:
        if not entity_id or not season_value:
            continue
        try:
            context = json.loads(context_json) if context_json else {}
        except Exception:
            context = {}
        parsed.append((str(entity_id), str(season_value), context))
    return parsed


def _coerce_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _aggregate_contexts(
    rows: list[tuple[str, dict]],
    *,
    sum_keys: tuple[str, ...],
    max_keys: tuple[str, ...] = (),
) -> dict[str, dict]:
    sum_key_set = set(sum_keys)
    max_key_set = set(max_keys)
    totals_by_entity: dict[str, dict] = {}
    for entity_id, context in rows:
        totals = totals_by_entity.setdefault(entity_id, {})
        for key in sum_key_set:
            number = _coerce_number(context.get(key))
            if number is None:
                continue
            totals[key] = totals.get(key, 0.0) + number
        for key in max_key_set:
            number = _coerce_number(context.get(key))
            if number is None:
                continue
            current = totals.get(key)
            totals[key] = number if current is None else max(current, number)
    return totals_by_entity


def _metric_declares_career_reducer(metric: MetricDefinition) -> bool:
    inner = getattr(metric, "_inner", None)
    has_custom_method = inner is not None and type(inner).compute_career_value is not MetricDefinition.compute_career_value
    return (
        getattr(metric, "career_aggregate_mode", None) == "season_results"
        or bool(getattr(metric, "career_sum_keys", ()))
        or bool(getattr(metric, "career_max_keys", ()))
        or has_custom_method
    )


def _aggregate_declared_career_metric(session: Session, metric: MetricDefinition, season: str) -> list[MetricResult]:
    base_key = _career_base_key(metric)
    rows = _career_context_rows(session, base_key, season)
    totals_by_entity = _aggregate_contexts(
        rows,
        sum_keys=tuple(getattr(metric, "career_sum_keys", ()) or ()),
        max_keys=tuple(getattr(metric, "career_max_keys", ()) or ()),
    )

    results: list[MetricResult] = []
    for entity_id in sorted(totals_by_entity):
        result = metric.compute_career_value(totals_by_entity[entity_id], season, entity_id)
        if result is not None:
            result.metric_key = metric.key
            results.append(result)
    return results


def _aggregate_career_metric_from_season_results(session: Session, metric: MetricDefinition, season: str) -> list[MetricResult]:
    if not _metric_declares_career_reducer(metric):
        raise RuntimeError(f"Metric {metric.key} does not declare a season-result career reducer")
    return _aggregate_declared_career_metric(session, metric, season)


def _aggregate_career_qualifications_from_season_logs(session: Session, metric: MetricDefinition, season: str) -> list[dict] | None:
    base_key = _career_base_key(metric)
    prefix = _career_season_prefix(season)
    if not _metric_declares_career_reducer(metric) or not prefix:
        return None
    max_keys = tuple(getattr(metric, "career_max_keys", ()) or ())
    eligible_entity_seasons: set[tuple[str, str]] | None = None
    if max_keys:
        context_rows = _career_context_rows_by_season(session, base_key, season)
        totals_by_entity = _aggregate_contexts(
            [(entity_id, context) for entity_id, _, context in context_rows],
            sum_keys=tuple(getattr(metric, "career_sum_keys", ()) or ()),
            max_keys=max_keys,
        )
        eligible_entity_seasons = set()
        for entity_id, season_value, context in context_rows:
            totals = totals_by_entity.get(entity_id) or {}
            if all(
                _coerce_number(context.get(key)) is not None
                and _coerce_number(context.get(key)) == _coerce_number(totals.get(key))
                for key in max_keys
            ):
                eligible_entity_seasons.add((entity_id, season_value))
    rows = (
        session.query(MetricRunLog.entity_id, MetricRunLog.season, MetricRunLog.game_id)
        .filter(
            MetricRunLog.metric_key == base_key,
            MetricRunLog.season.like(prefix),
            MetricRunLog.qualified.is_(True),
        )
        .all()
    )
    if not rows:
        return None
    return [
        {"entity_id": str(entity_id), "game_id": str(game_id), "qualified": True}
        for entity_id, season_value, game_id in rows
        if entity_id
        and season_value
        and game_id
        and (eligible_entity_seasons is None or (str(entity_id), str(season_value)) in eligible_entity_seasons)
    ]


class RuleMetricDefinition(MetricDefinition):
    """Adapter that makes a DB-backed rule metric runnable by the existing runner."""

    incremental = False
    supports_career = False
    career = False
    career_name_suffix = " (Career)"
    career_min_sample: int | None = None

    def __init__(self, row: MetricDefinitionModel, *, career: bool | None = None):
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
        self.family_key = getattr(row, "family_key", None) or row.key
        self.variant = getattr(row, "variant", None)
        self.base_metric_key = getattr(row, "base_metric_key", None)
        self.managed_family = bool(getattr(row, "managed_family", False))
        self.definition = json.loads(row.definition_json or "{}")
        self.trigger = str(self.definition.get("trigger") or "game").strip().lower()
        self.time_scope = str(self.definition.get("time_scope") or "season").strip().lower()
        self.supports_career = rule_supports_career(self.definition, row.scope)
        self.career_name_suffix = str(self.definition.get("career_name_suffix") or " (Career)")
        career_min_sample = self.definition.get("career_min_sample")
        self.career_min_sample = int(career_min_sample) if career_min_sample is not None else None
        self.qualifying_field = self.definition.get("qualifying_field")  # legacy, unused
        explicit_career = self.variant == FAMILY_VARIANT_CAREER
        self.career = explicit_career if career is None else career

        if self.time_scope == "career":
            self.career = True
            self.supports_career = False
        elif self.time_scope == "season_and_career":
            self.supports_career = True

        if self.career:
            if not explicit_career and self.time_scope != "career":
                self.key = family_career_key(self._base_key)
                self.name = derive_career_name(self._base_name, self.career_name_suffix)
                self.description = derive_career_description(self.description)
                self.supports_career = False
            self.min_sample = derive_career_min_sample(self.min_sample, self.career_min_sample)

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
        from metrics.framework.rule_engine import compute_baseline, compute_result

        if entity_id is None or season is None:
            return None

        if self.career:
            target_season = career_season_for(season) if season else None
            if target_season is None:
                return None
        else:
            target_season = season
        rule_result = compute_result(session, self.definition, entity_id, target_season, self.scope)
        if rule_result is None:
            return None

        baseline = compute_baseline(session, self.definition, entity_id, target_season, self.scope)
        context = dict(rule_result.get("context") or {})
        if baseline is not None:
            context["baseline"] = baseline

        return MetricResult(
            metric_key=self.key,
            entity_type=self.scope,
            entity_id=entity_id,
            season=target_season,
            game_id=game_id if self.scope == "game" else None,
            rank_group=rule_result.get("rank_group"),
            value_num=float(rule_result["value_num"]),
            value_str=rule_result.get("value_str"),
            context=context,
        )


def load_code_metric(code: str) -> MetricDefinition:
    """Return a fresh MetricDefinition instance for a generated code metric."""
    metric_cls = _load_code_metric_class(code)
    metric = metric_cls()
    _validate_metric_instance(metric)
    return metric


@lru_cache(maxsize=256)
def _load_code_metric_class(code: str) -> type[MetricDefinition]:
    """Compile generated code once and return its MetricDefinition subclass."""
    _validate_code_metric_ast(code)

    # Defense-in-depth namespace restriction — not a full sandbox (seccomp follow-on).
    # Use a single namespace so imports are visible inside class methods.
    ns: dict = {"__builtins__": _SAFE_BUILTINS, "__name__": "__metric_code__"}
    try:
        exec(code, ns)
    except Exception as exc:
        raise ValueError(f"Code execution failed: {exc}") from exc

    # Find all MetricDefinition subclasses defined in the code
    metric_classes = [
        v for v in ns.values()
        if isinstance(v, type)
        and issubclass(v, MetricDefinition)
        and v is not MetricDefinition
    ]

    if not metric_classes:
        raise ValueError("Generated code does not define a MetricDefinition subclass")
    if len(metric_classes) > 1:
        raise ValueError("Generated code must define exactly one MetricDefinition subclass")

    return metric_classes[0]


class CodeMetricDefinition(MetricDefinition):
    """Adapter that wraps a DB-backed code metric (source_type='code')."""

    def __init__(self, row: MetricDefinitionModel, *, career: bool | None = None):
        self._inner = load_code_metric(row.code_python)
        self._base_row = row
        # Copy attributes from the inner metric
        self.key = row.key or self._inner.key
        self.name = row.name or self._inner.name
        self.description = row.description or self._inner.description
        self.scope = row.scope or self._inner.scope
        self.category = getattr(self._inner, "category", row.category or "")
        self.min_sample = int(row.min_sample or self._inner.min_sample or 1)
        self.incremental = self._inner.incremental
        self.supports_career = getattr(self._inner, "supports_career", False)
        self.rank_order = getattr(self._inner, "rank_order", "desc")
        self.group_key = row.group_key
        self.source_type = row.source_type
        self.status = row.status
        self.family_key = getattr(row, "family_key", None) or row.key
        self.variant = getattr(row, "variant", None)
        self.base_metric_key = getattr(row, "base_metric_key", None)
        self.managed_family = bool(getattr(row, "managed_family", False))
        explicit_career = self.variant == FAMILY_VARIANT_CAREER or bool(getattr(self._inner, "career", False))
        self.career = explicit_career if career is None else career
        self.career_name_suffix = getattr(self._inner, "career_name_suffix", " (Career)")
        self.career_min_sample = getattr(self._inner, "career_min_sample", None)
        self.career_aggregate_mode = getattr(self._inner, "career_aggregate_mode", None)
        self.career_sum_keys = tuple(getattr(self._inner, "career_sum_keys", ()) or ())
        self.career_max_keys = tuple(getattr(self._inner, "career_max_keys", ()) or ())
        self.context_label_template = getattr(self._inner, "context_label_template", None)
        self.trigger = getattr(self._inner, "trigger", "game")
        self.per_game = getattr(self._inner, "per_game", True)
        self.qualifying_field = getattr(self._inner, "qualifying_field", None)  # legacy, unused

        if self.career:
            if not explicit_career:
                self.key = family_career_key(self._base_row.key)
                self.name = derive_career_name(self._inner.name, self.career_name_suffix)
                self.description = derive_career_description(self._inner.description)
            self.supports_career = False
            self.min_sample = derive_career_min_sample(self.min_sample, self.career_min_sample)

    def make_career_sibling(self) -> CodeMetricDefinition | None:
        if not self.supports_career or self.scope == "game":
            return None
        return CodeMetricDefinition(self._base_row, career=True)

    def compute_delta(self, session, entity_id, game_id):
        return self._inner.compute_delta(ReadOnlySession(session), entity_id, game_id)

    def compute_value(self, totals, season, entity_id):
        result = self._inner.compute_value(totals, season, entity_id)
        if result and self.career:
            result.metric_key = self.key
        return result

    def compute(self, session, entity_id, season, game_id=None):
        result = self._inner.compute(ReadOnlySession(session), entity_id, season, game_id)
        if result and self.career:
            result.metric_key = self.key
        return result

    def compute_season(self, session, season):
        if self.career and self.trigger == "season" and is_career_season(season):
            return _aggregate_career_metric_from_season_results(session, self, season)
        return self._inner.compute_season(ReadOnlySession(session), season)

    def compute_qualifications(self, session, season):
        if self.career and self.trigger == "season" and is_career_season(season):
            quals = _aggregate_career_qualifications_from_season_logs(session, self, season)
            if quals is not None:
                return quals
        fn = getattr(self._inner, "compute_qualifications", None)
        if fn is None:
            return None
        return fn(ReadOnlySession(session), season)

    def compute_career_value(self, totals, season, entity_id):
        result = self._inner.compute_career_value(totals, season, entity_id)
        if result and self.career:
            result.metric_key = self.key
        return result


def _load_published_code_metrics(session: Session) -> list[CodeMetricDefinition]:
    rows = (
        session.query(MetricDefinitionModel)
        .filter(
            MetricDefinitionModel.status == "published",
            MetricDefinitionModel.source_type == "code",
            MetricDefinitionModel.code_python.isnot(None),
        )
        .order_by(MetricDefinitionModel.created_at.asc(), MetricDefinitionModel.id.asc())
        .all()
    )
    metrics: list[CodeMetricDefinition] = []
    existing_keys = {row.key for row in rows}
    for row in rows:
        try:
            metric = CodeMetricDefinition(row)
            metrics.append(metric)
            sibling = metric.make_career_sibling()
            if sibling is not None and sibling.key not in existing_keys:
                metrics.append(sibling)
        except Exception as exc:
            logger.error("Failed to load code metric %s: %s", row.key, exc)
    return metrics


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
    existing_keys = {row.key for row in rows}
    for row in rows:
        metric = RuleMetricDefinition(row)
        metrics.append(metric)
        sibling = metric.make_career_sibling()
        if sibling is not None and sibling.key not in existing_keys:
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


def _load_all_db_metrics(session: Session) -> list[MetricDefinition]:
    return [*_load_published_rule_metrics(session), *_load_published_code_metrics(session)]


def _build_runtime_metric(
    row: MetricDefinitionModel,
    *,
    career: bool | None = None,
) -> MetricDefinition:
    if row.source_type == "code":
        return CodeMetricDefinition(row, career=career)
    return RuleMetricDefinition(row, career=career)


def _lookup_published_metric_row(session: Session, key: str) -> MetricDefinitionModel | None:
    return (
        session.query(MetricDefinitionModel)
        .filter(
            MetricDefinitionModel.status == "published",
            MetricDefinitionModel.key == key,
        )
        .first()
    )


def _load_metric_by_key(session: Session, key: str) -> MetricDefinition | None:
    row = _lookup_published_metric_row(session, key)
    if row is not None:
        return _build_runtime_metric(row)

    if not key.endswith("_career"):
        return None

    base_key = family_base_key(key)
    if base_key == key:
        return None

    base_row = _lookup_published_metric_row(session, base_key)
    if base_row is None:
        return None

    base_metric = _build_runtime_metric(base_row)
    if base_metric.career or not getattr(base_metric, "supports_career", False):
        return None

    return _build_runtime_metric(base_row, career=True)


def get_all_metrics(session: Session | None = None) -> list[MetricDefinition]:
    if session is not None:
        return _dedupe_by_key(_load_all_db_metrics(session))

    with SessionLocal() as owned:
        return _dedupe_by_key(_load_all_db_metrics(owned))


def get_metric(key: str, session: Session | None = None) -> MetricDefinition | None:
    if session is not None:
        return _load_metric_by_key(session, key)

    with SessionLocal() as owned:
        return _load_metric_by_key(owned, key)


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
