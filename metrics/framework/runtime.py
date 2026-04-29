"""Runtime metric catalog backed by published DB rule/code metrics."""
from __future__ import annotations

import ast
import builtins as py_builtins
import json
import logging
from contextlib import contextmanager
from functools import lru_cache
from typing import Iterable

logger = logging.getLogger(__name__)

from sqlalchemy import func
from sqlalchemy.orm import Session, sessionmaker

from db import models as db_models
from db.game_status import completed_game_clause
from metrics.framework.base import (
    MetricDefinition,
    MetricResult,
    career_season_type_code,
    is_career_season,
    normalize_metric_season_types,
    season_type_for,
    window_season_for,
    window_size_from_season,
)
from metrics.framework.family import (
    FAMILY_VARIANT_SEASON,
    derive_window_description,
    derive_window_min_sample,
    derive_window_name,
    family_base_key,
    family_window_key,
    rule_supports_career,
    window_type_from_key,
)

MetricDefinitionModel = db_models.MetricDefinition
MetricResultModel = getattr(db_models, "MetricResult", None)
MetricRunLog = getattr(db_models, "MetricRunLog", None)
Game = getattr(db_models, "Game", None)
engine = db_models.engine

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
    metric.validate()

    missing = [
        attr for attr in ("key", "name", "description", "scope", "category")
        if not getattr(metric, attr, None)
    ]
    if missing:
        raise ValueError(
            "Generated metric is missing required attributes: " + ", ".join(missing)
        )

    if metric.scope not in {"game", "league", "player", "player_franchise", "season", "team"}:
        raise ValueError(f"Generated metric has invalid scope: {metric.scope!r}")

    trigger = getattr(metric, "trigger", "game")
    if trigger not in {"game", "season"}:
        raise ValueError(f"Generated metric has invalid trigger: {trigger!r}")

    try:
        metric.season_types = normalize_metric_season_types(getattr(metric, "season_types", None))
    except ValueError as exc:
        raise ValueError(f"Generated metric has invalid season_types: {exc}") from exc

    if bool(getattr(metric, "additive_accumulator", False)):
        kind = str(getattr(metric, "metric_kind", "") or "").lower()
        bad_kinds = {"ratio", "win_pct", "per_game", "split_avg", "per_36", "streak", "single_game_record"}
        if kind in bad_kinds:
            raise ValueError(
                f"Generated metric {metric.key!r} cannot set additive_accumulator=True for metric_kind={kind!r}"
            )
        if not getattr(metric, "approaching_thresholds", None):
            raise ValueError(f"Generated metric {metric.key!r} sets additive_accumulator=True without approaching_thresholds")
        if getattr(metric, "absolute_approach_thresholds", None) and not getattr(metric, "absolute_thresholds", None):
            raise ValueError(f"Generated metric {metric.key!r} sets absolute_approach_thresholds without absolute_thresholds")

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


def _metric_window_types(metric: MetricDefinition) -> list[str]:
    if not getattr(metric, "supports_career", False) or getattr(metric, "scope", None) in ("game", "season"):
        return []
    window_types = ["career"]
    if getattr(metric, "trigger", "game") == "season":
        window_types.extend(["last10", "last5", "last3"])
    return window_types


def _career_season_prefix(career_season: str) -> str | None:
    code = career_season_type_code(career_season)
    if not code:
        return None
    return f"{code}%"


def _window_target_seasons(session: Session, base_key: str, career_season: str) -> list[str] | None:
    prefix = _career_season_prefix(career_season)
    window_size = window_size_from_season(career_season)
    if not prefix or window_size is None:
        return None
    if MetricResultModel is None:
        return []
    rows = (
        session.query(MetricResultModel.season)
        .filter(
            MetricResultModel.metric_key == base_key,
            MetricResultModel.season.like(prefix),
        )
        .distinct()
        .order_by(MetricResultModel.season.desc())
        .limit(window_size)
        .all()
    )
    return [season for (season,) in rows if season]


def _career_ready_from_season_results(session: Session, base_key: str, career_season: str) -> bool:
    prefix = _career_season_prefix(career_season)
    if not prefix:
        return False
    target_seasons = _window_target_seasons(session, base_key, career_season)
    if target_seasons is not None:
        return bool(target_seasons)

    expected = {
        season
        for (season,) in session.query(Game.season)
        .filter(Game.game_date.isnot(None), Game.season.like(prefix), completed_game_clause(Game))
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


def _career_context_rows(session: Session, base_key: str, career_season: str) -> list[tuple[str, str, dict]]:
    """Load all concrete-season MetricResult rows feeding into a career bucket.

    Returns 3-tuples of (entity_id, sub_key, context_dict). sub_key defaults
    to "" when the metric is not split.
    """
    prefix = _career_season_prefix(career_season)
    if not prefix:
        return []
    target_seasons = _window_target_seasons(session, base_key, career_season)
    filters = [
        MetricResultModel.metric_key == base_key,
        MetricResultModel.entity_id.isnot(None),
    ]
    if target_seasons is not None:
        if not target_seasons:
            return []
        filters.append(MetricResultModel.season.in_(target_seasons))
    else:
        filters.append(MetricResultModel.season.like(prefix))
    rows = (
        session.query(
            MetricResultModel.entity_id,
            MetricResultModel.sub_key,
            MetricResultModel.context_json,
        )
        .filter(*filters)
        .all()
    )
    parsed: list[tuple[str, str, dict]] = []
    for entity_id, sub_key, context_json in rows:
        if not entity_id:
            continue
        try:
            context = json.loads(context_json) if context_json else {}
        except Exception:
            context = {}
        parsed.append((str(entity_id), str(sub_key or ""), context))
    return parsed


def _career_context_rows_by_season(session: Session, base_key: str, career_season: str) -> list[tuple[str, str, dict]]:
    prefix = _career_season_prefix(career_season)
    if not prefix:
        return []
    target_seasons = _window_target_seasons(session, base_key, career_season)
    filters = [
        MetricResultModel.metric_key == base_key,
        MetricResultModel.entity_id.isnot(None),
    ]
    if target_seasons is not None:
        if not target_seasons:
            return []
        filters.append(MetricResultModel.season.in_(target_seasons))
    else:
        filters.append(MetricResultModel.season.like(prefix))
    rows = (
        session.query(MetricResultModel.entity_id, MetricResultModel.season, MetricResultModel.context_json)
        .filter(*filters)
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
    rows: list[tuple[str, str, dict]],
    *,
    sum_keys: tuple[str, ...],
    max_keys: tuple[str, ...] = (),
    min_keys: tuple[str, ...] = (),
    group_by_sub_key: bool = False,
):
    """Aggregate season context rows into totals for career reduction.

    Rows are 3-tuples (entity_id, sub_key, context). When ``group_by_sub_key``
    is False the aggregator collapses by entity_id (original behavior);
    when True it groups by (entity_id, sub_key) so split-metric slices stay
    separate. Return type differs accordingly:
      - False → dict[str, dict]           keyed by entity_id
      - True  → dict[tuple[str, str], dict] keyed by (entity_id, sub_key)
    """
    sum_key_set = set(sum_keys)
    max_key_set = set(max_keys)
    min_key_set = set(min_keys)
    totals_by_key: dict = {}
    for entity_id, sub_key, context in rows:
        key = (entity_id, sub_key) if group_by_sub_key else entity_id
        totals = totals_by_key.setdefault(key, {})
        for k in sum_key_set:
            number = _coerce_number(context.get(k))
            if number is None:
                continue
            totals[k] = totals.get(k, 0.0) + number
        for k in max_key_set:
            number = _coerce_number(context.get(k))
            if number is None:
                continue
            current = totals.get(k)
            totals[k] = number if current is None else max(current, number)
        for k in min_key_set:
            number = _coerce_number(context.get(k))
            if number is None:
                continue
            current = totals.get(k)
            totals[k] = number if current is None else min(current, number)
    return totals_by_key


def _metric_declares_career_reducer(metric: MetricDefinition) -> bool:
    return (
        getattr(metric, "career_aggregate_mode", None) == "season_results"
        or bool(getattr(metric, "career_sum_keys", ()))
        or bool(getattr(metric, "career_max_keys", ()))
        or bool(getattr(metric, "career_min_keys", ()))
    )


def _effective_window_min_sample(metric: MetricDefinition, season: str) -> int | None:
    """Compute the season-type-aware min_sample for this metric/season.

    Returns None if no override is needed (e.g. regular-season buckets, or
    metrics that lack a base min_sample to start from).
    """
    base_min = getattr(metric, "_base_min_sample", None)
    if base_min is None:
        return None
    season_type = season_type_for(season)
    if season_type not in ("playoffs", "playin"):
        return None
    window_type = getattr(metric, "window_type", None) or "career"
    return derive_window_min_sample(
        base_min,
        window_type,
        career_min_sample=getattr(metric, "career_min_sample", None),
        season_type=season_type,
    )


@contextmanager
def _swap_min_sample(metric: MetricDefinition, season: str):
    """Temporarily replace metric.min_sample for the duration of one reduce.

    User-defined compute_career_value implementations read self.min_sample
    directly. Swapping the attribute lets the same code emit looser results
    for playoffs/play-in pseudo-seasons without each metric having to know.
    Code metrics delegate compute to an inner instance, so swap both layers.
    """
    new_min = _effective_window_min_sample(metric, season)
    if new_min is None:
        yield
        return
    targets: list[tuple[object, int]] = [(metric, metric.min_sample)]
    inner = getattr(metric, "_inner", None)
    if inner is not None and hasattr(inner, "min_sample"):
        targets.append((inner, inner.min_sample))
    for obj, _ in targets:
        obj.min_sample = new_min
    try:
        yield
    finally:
        for obj, original in targets:
            obj.min_sample = original


def _aggregate_declared_career_metric(session: Session, metric: MetricDefinition, season: str) -> list[MetricResult]:
    base_key = _career_base_key(metric)
    rows = _career_context_rows(session, base_key, season)
    group_by_sub_key = bool(getattr(metric, "career_group_by_sub_key", False))
    totals_by_key = _aggregate_contexts(
        rows,
        sum_keys=tuple(getattr(metric, "career_sum_keys", ()) or ()),
        max_keys=tuple(getattr(metric, "career_max_keys", ()) or ()),
        min_keys=tuple(getattr(metric, "career_min_keys", ()) or ()),
        group_by_sub_key=group_by_sub_key,
    )

    results: list[MetricResult] = []

    with _swap_min_sample(metric, season):
        if group_by_sub_key:
            # Index raw rows by (entity, sub_key) so the metric can inspect
            # per-season context details in addition to aggregated totals.
            raw_by_key: dict[tuple[str, str], list[dict]] = {}
            for entity_id, sub_key, context in rows:
                raw_by_key.setdefault((entity_id, sub_key), []).append(context)
            for key in sorted(totals_by_key):
                entity_id, sub_key = key
                result = metric.compute_career_value(
                    totals_by_key[key],
                    season,
                    entity_id,
                    sub_key=sub_key,
                    rows=raw_by_key.get(key, []),
                )
                if result is not None:
                    result.metric_key = metric.key
                    if not result.sub_key:
                        result.sub_key = sub_key
                    results.append(result)
        else:
            for entity_id in sorted(totals_by_key):
                result = metric.compute_career_value(totals_by_key[entity_id], season, entity_id)
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
    target_seasons = _window_target_seasons(session, base_key, season)
    max_keys = tuple(getattr(metric, "career_max_keys", ()) or ())
    min_keys = tuple(getattr(metric, "career_min_keys", ()) or ())
    extrema_keys = max_keys + min_keys
    eligible_entity_seasons: set[tuple[str, str]] | None = None
    if extrema_keys:
        context_rows = _career_context_rows_by_season(session, base_key, season)
        totals_by_entity = _aggregate_contexts(
            [(entity_id, "", context) for entity_id, _, context in context_rows],
            sum_keys=tuple(getattr(metric, "career_sum_keys", ()) or ()),
            max_keys=max_keys,
            min_keys=min_keys,
        )
        eligible_entity_seasons = set()
        for entity_id, season_value, context in context_rows:
            totals = totals_by_entity.get(entity_id) or {}
            if all(
                _coerce_number(context.get(key)) is not None
                and _coerce_number(context.get(key)) == _coerce_number(totals.get(key))
                for key in extrema_keys
            ):
                eligible_entity_seasons.add((entity_id, season_value))
    rows = (
        session.query(MetricRunLog.entity_id, MetricRunLog.season, MetricRunLog.game_id)
        .filter(
            MetricRunLog.metric_key == base_key,
            MetricRunLog.qualified.is_(True),
            MetricRunLog.season.in_(target_seasons) if target_seasons is not None else MetricRunLog.season.like(prefix),
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


def _extract_game_ids_from_context(context: dict | None) -> list[str]:
    if not isinstance(context, dict):
        return []

    seen: set[str] = set()
    game_ids: list[str] = []

    def _append(value) -> None:
        if value is None:
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _append(item)
            return
        game_id = str(value).strip()
        if not game_id or game_id in seen:
            return
        seen.add(game_id)
        game_ids.append(game_id)

    for key in ("game_id", "best_game_id"):
        _append(context.get(key))
    for key in ("game_ids", "best_game_ids"):
        _append(context.get(key))
    return game_ids


def _fallback_career_result_context_game_ids(
    session: Session,
    metric: MetricDefinition,
    season: str,
    entity_id: str | None,
) -> list[str]:
    if MetricResultModel is None or not entity_id or not season:
        return []

    row = (
        session.query(MetricResultModel.context_json)
        .filter(
            MetricResultModel.metric_key == metric.key,
            MetricResultModel.entity_id == str(entity_id),
            MetricResultModel.season == season,
        )
        .first()
    )
    if not row:
        return []

    context_json = getattr(row, "context_json", None)
    if context_json is None and isinstance(row, (tuple, list)) and row:
        context_json = row[0]

    try:
        context = json.loads(context_json) if context_json else {}
    except Exception:
        context = {}
    return _extract_game_ids_from_context(context)


def _aggregated_career_qualification_game_ids(
    metric: MetricDefinition | None,
    session: Session,
    season: str | None,
    entity_id: str | None = None,
) -> list[str] | None:
    """Return deduped qualifying game_ids for career reducer metrics.

    Career season metrics backed by season-result reducers do not persist their
    own MetricRunLog rows. Drill-down callers should derive the qualifying games
    by reusing the metric's aggregated qualification logic instead of scanning
    the base metric's full season logs directly.
    """
    season_value = season or "all_regular"
    if (
        metric is None
        or not getattr(metric, "career", False)
        or not is_career_season(season_value)
        or not _metric_declares_career_reducer(metric)
    ):
        return None

    qualifications = metric.compute_qualifications(session, season_value) or []
    seen: set[str] = set()
    game_ids: list[str] = []
    entity_id_value = str(entity_id) if entity_id is not None else None

    for qualification in qualifications:
        if not qualification or qualification.get("qualified", True) is False:
            continue
        qualification_entity_id = qualification.get("entity_id")
        if entity_id_value is not None and str(qualification_entity_id) != entity_id_value:
            continue
        game_id = qualification.get("game_id")
        if not game_id:
            continue
        game_id_value = str(game_id)
        if game_id_value in seen:
            continue
        seen.add(game_id_value)
        game_ids.append(game_id_value)
    if game_ids:
        return game_ids
    return _fallback_career_result_context_game_ids(session, metric, season_value, entity_id_value)


def _resolve_window_type(
    *,
    variant: str | None,
    key: str | None,
    explicit_window_type: str | None,
    career: bool | None,
    inner_career: bool = False,
) -> str | None:
    if explicit_window_type is not None:
        return explicit_window_type
    if variant in {"career", "last3", "last5", "last10"}:
        return variant
    key_window = window_type_from_key(key)
    if key_window is not None:
        return key_window
    if career or inner_career:
        return "career"
    return None


class RuleMetricDefinition(MetricDefinition):
    """Adapter that makes a DB-backed rule metric runnable by the existing runner."""

    incremental = False
    supports_career = False
    career = False
    career_name_suffix = " (Career)"
    career_min_sample: int | None = None

    def __init__(
        self,
        row: MetricDefinitionModel,
        *,
        career: bool | None = None,
        window_type: str | None = None,
    ):
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
        self.additive_accumulator = False
        self.approaching_thresholds = []
        self.absolute_thresholds = []
        self.absolute_approach_thresholds = []
        self.trigger = str(self.definition.get("trigger") or "game").strip().lower()
        self.time_scope = str(self.definition.get("time_scope") or "season").strip().lower()
        self.supports_career = rule_supports_career(self.definition, row.scope)
        self.career_name_suffix = str(self.definition.get("career_name_suffix") or " (Career)")
        career_min_sample = self.definition.get("career_min_sample")
        self.career_min_sample = int(career_min_sample) if career_min_sample is not None else None
        self.season_types = normalize_metric_season_types(self.definition.get("season_types"))
        self.qualifying_field = self.definition.get("qualifying_field")  # legacy, unused
        persisted_window = _resolve_window_type(
            variant=self.variant,
            key=row.key,
            explicit_window_type=None,
            career=None,
        )
        self.window_type = _resolve_window_type(
            variant=self.variant,
            key=row.key,
            explicit_window_type=window_type,
            career=career,
        )
        self.career = self.window_type is not None

        if self.time_scope == "career":
            self.window_type = "career"
            self.career = True
            self.supports_career = False
        elif self.time_scope == "season_and_career":
            self.supports_career = True

        if self.window_type is not None:
            if persisted_window is None and self.time_scope != "career":
                self.key = family_window_key(self._base_key, self.window_type)
                self.name = derive_window_name(
                    self._base_name,
                    self.window_type,
                    suffix=self.career_name_suffix if self.window_type == "career" else None,
                )
                self.description = derive_window_description(self.description, self.window_type)
                self.supports_career = False
            self.min_sample = derive_window_min_sample(
                self.min_sample,
                self.window_type,
                career_min_sample=self.career_min_sample,
            )

    def make_window_siblings(self) -> list[RuleMetricDefinition]:
        return [
            RuleMetricDefinition(self._base_row, window_type=window_type)
            for window_type in _metric_window_types(self)
        ]

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
            target_season = window_season_for(season, self.window_type or "career")
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

    def __init__(
        self,
        row: MetricDefinitionModel,
        *,
        career: bool | None = None,
        window_type: str | None = None,
    ):
        self._inner = load_code_metric(row.code_python)
        self._base_row = row
        # Copy attributes from the inner metric
        self.key = row.key or self._inner.key
        self.name = row.name or self._inner.name
        self.description = row.description or self._inner.description
        self.scope = row.scope or self._inner.scope
        self.category = getattr(self._inner, "category", row.category or "")
        self.min_sample = int(row.min_sample or self._inner.min_sample or 1)
        self._base_min_sample = self.min_sample
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
        self.career_name_suffix = getattr(self._inner, "career_name_suffix", " (Career)")
        self.career_min_sample = getattr(self._inner, "career_min_sample", None)
        self.career_aggregate_mode = getattr(self._inner, "career_aggregate_mode", None)
        self.career_sum_keys = tuple(getattr(self._inner, "career_sum_keys", ()) or ())
        self.career_max_keys = tuple(getattr(self._inner, "career_max_keys", ()) or ())
        self.career_min_keys = tuple(getattr(self._inner, "career_min_keys", ()) or ())
        self.career_group_by_sub_key = bool(getattr(self._inner, "career_group_by_sub_key", False))
        self.additive_accumulator = bool(getattr(self._inner, "additive_accumulator", False))
        self.approaching_thresholds = list(getattr(self._inner, "approaching_thresholds", []) or [])
        self.absolute_thresholds = list(getattr(self._inner, "absolute_thresholds", []) or [])
        self.absolute_approach_thresholds = list(getattr(self._inner, "absolute_approach_thresholds", []) or [])
        self.season_types = normalize_metric_season_types(getattr(self._inner, "season_types", None))
        self.context_label_template = getattr(self._inner, "context_label_template", None)
        self.trigger = getattr(self._inner, "trigger", "game")
        self.per_game = getattr(self._inner, "per_game", True)
        self.sub_key_type = getattr(row, "sub_key_type", None)
        self.sub_key_label = getattr(row, "sub_key_label", None)
        self.sub_key_label_zh = getattr(row, "sub_key_label_zh", None)
        self.sub_key_rank_scope = getattr(row, "sub_key_rank_scope", None)
        self.fill_missing_sub_keys_with_zero = bool(getattr(row, "fill_missing_sub_keys_with_zero", False))
        self.qualifying_field = getattr(self._inner, "qualifying_field", None)  # legacy, unused
        self.max_results_per_season = getattr(row, "max_results_per_season", None) or getattr(self._inner, "max_results_per_season", None)
        persisted_window = _resolve_window_type(
            variant=self.variant,
            key=self.key,
            explicit_window_type=None,
            career=None,
            inner_career=bool(getattr(self._inner, "career", False)),
        )
        self.window_type = _resolve_window_type(
            variant=self.variant,
            key=self.key,
            explicit_window_type=window_type,
            career=career,
            inner_career=bool(getattr(self._inner, "career", False)),
        )
        self.career = self.window_type is not None

        if self.window_type is not None:
            if persisted_window is None:
                self.key = family_window_key(self._base_row.key, self.window_type)
                self.name = derive_window_name(
                    self._inner.name,
                    self.window_type,
                    suffix=self.career_name_suffix if self.window_type == "career" else None,
                )
                self.description = derive_window_description(self._inner.description, self.window_type)
            self.supports_career = False
            self.min_sample = derive_window_min_sample(
                self.min_sample,
                self.window_type,
                career_min_sample=self.career_min_sample,
            )

    def make_window_siblings(self) -> list[CodeMetricDefinition]:
        return [
            CodeMetricDefinition(self._base_row, window_type=window_type)
            for window_type in _metric_window_types(self)
        ]

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
            if _metric_declares_career_reducer(self):
                return _aggregate_career_metric_from_season_results(session, self, season)
            # No career reducer declared — let the inner handle career directly
        results = self._inner.compute_season(ReadOnlySession(session), season)
        if self.career and results:
            for r in (results if isinstance(results, list) else [results]):
                if r:
                    r.metric_key = self.key
        return results

    def compute_qualifications(self, session, season):
        if self.career and self.trigger == "season" and is_career_season(season):
            quals = _aggregate_career_qualifications_from_season_logs(session, self, season)
            if quals is not None:
                return quals
        fn = getattr(self._inner, "compute_qualifications", None)
        if fn is None:
            return None
        return fn(ReadOnlySession(session), season)

    def compute_career_value(self, totals, season, entity_id, **kwargs):
        original_supports_career = getattr(self._inner, "supports_career", False)
        if self.career and not original_supports_career and _metric_declares_career_reducer(self):
            self._inner.supports_career = True
        try:
            # Only forward sub_key/rows kwargs to inner if the metric opted into
            # sub_key grouping; old metrics keep their 3-arg signature.
            if self.career_group_by_sub_key:
                result = self._inner.compute_career_value(totals, season, entity_id, **kwargs)
            else:
                result = self._inner.compute_career_value(totals, season, entity_id)
        finally:
            if self.career and not original_supports_career and _metric_declares_career_reducer(self):
                self._inner.supports_career = original_supports_career
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
            for sibling in metric.make_window_siblings():
                if sibling.key not in existing_keys:
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
        for sibling in metric.make_window_siblings():
            if sibling.key not in existing_keys:
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
    window_type: str | None = None,
) -> MetricDefinition:
    if row.source_type == "code":
        return CodeMetricDefinition(row, career=career, window_type=window_type)
    return RuleMetricDefinition(row, career=career, window_type=window_type)


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

    window_type = window_type_from_key(key)
    if window_type is None:
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
    if window_type != "career" and getattr(base_metric, "trigger", "game") != "season":
        return None

    return _build_runtime_metric(base_row, career=True, window_type=window_type)


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
        for sibling in getattr(metric, "make_window_siblings", lambda: [])():
            if sibling.key in metrics and sibling.key not in seen:
                seen.add(sibling.key)
                expanded.append(sibling.key)
    return expanded
