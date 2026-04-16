"""Tests for loading generated code metrics safely."""

import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_ORIGINAL_DB_MODELS = sys.modules.get("db.models")
_ORIGINAL_DB_ATTR = getattr(sys.modules.get("db"), "models", None) if "db" in sys.modules else None


def _make_fake_db_models():
    module = types.ModuleType("db.models")
    module.engine = MagicMock()

    class FakeMetricDefinitionModel:
        pass

    module.MetricDefinition = FakeMetricDefinitionModel
    return module


class TestLoadCodeMetric(unittest.TestCase):
    def setUp(self):
        self.fake_db_models = _make_fake_db_models()

        sys.modules.pop("metrics.framework.runtime", None)
        sys.modules["db.models"] = self.fake_db_models

        if "db" in sys.modules:
            sys.modules["db"].models = self.fake_db_models

    def tearDown(self):
        sys.modules.pop("metrics.framework.runtime", None)

        if _ORIGINAL_DB_MODELS is not None:
            sys.modules["db.models"] = _ORIGINAL_DB_MODELS
        else:
            sys.modules.pop("db.models", None)

        if "db" in sys.modules:
            if _ORIGINAL_DB_ATTR is not None:
                sys.modules["db"].models = _ORIGINAL_DB_ATTR
            elif hasattr(sys.modules["db"], "models"):
                delattr(sys.modules["db"], "models")

    def _load_runtime(self):
        return importlib.import_module("metrics.framework.runtime")

    def test_valid_metric_class_loads_and_computes(self):
        runtime = self._load_runtime()

        metric = runtime.load_code_metric(
            """
from metrics.framework.base import MetricDefinition, MetricResult


class ValidMetric(MetricDefinition):
    key = "valid_metric"
    name = "Valid Metric"
    description = "Returns a constant for test coverage."
    scope = "player"
    category = "scoring"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=game_id,
            value_num=12.5,
            value_str="12.5 pts",
            context={"player_id": entity_id},
        )
"""
        )

        result = metric.compute(None, "player-1", "2025-26")
        self.assertEqual(metric.key, "valid_metric")
        self.assertEqual(result.value_num, 12.5)
        self.assertEqual(result.context["player_id"], "player-1")

    def test_missing_required_attribute_raises_value_error(self):
        runtime = self._load_runtime()

        with self.assertRaisesRegex(ValueError, "missing required attributes: key"):
            runtime.load_code_metric(
                """
from metrics.framework.base import MetricDefinition, MetricResult


class MissingKeyMetric(MetricDefinition):
    name = "Missing Key"
    description = "Missing key should fail validation."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return MetricResult(
            metric_key="missing_key",
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=game_id,
            value_num=1.0,
        )
"""
            )

    def test_importing_os_raises_value_error(self):
        runtime = self._load_runtime()

        with self.assertRaisesRegex(ValueError, "Import of 'os' is not allowed"):
            runtime.load_code_metric(
                """
import os
from metrics.framework.base import MetricDefinition


class BadMetric(MetricDefinition):
    key = "bad_metric"
    name = "Bad Metric"
    description = "Should never load."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
"""
            )

    def test_importing_subprocess_raises_value_error(self):
        runtime = self._load_runtime()

        with self.assertRaisesRegex(ValueError, "Import of 'subprocess' is not allowed"):
            runtime.load_code_metric(
                """
import subprocess
from metrics.framework.base import MetricDefinition


class BadMetric(MetricDefinition):
    key = "bad_metric"
    name = "Bad Metric"
    description = "Should never load."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
"""
            )

    def test_importing_sqlalchemy_func_is_allowed(self):
        runtime = self._load_runtime()

        metric = runtime.load_code_metric(
            """
from sqlalchemy import func
from metrics.framework.base import MetricDefinition, MetricResult


class SqlAlchemyMetric(MetricDefinition):
    key = "sqlalchemy_metric"
    name = "SqlAlchemy Metric"
    description = "Uses sqlalchemy func."
    scope = "game"
    category = "aggregate"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return MetricResult(
            metric_key=self.key,
            entity_type="game",
            entity_id=entity_id,
            season=season,
            game_id=game_id,
            value_num=float(func.count().name == 'count'),
            value_str="ok",
        )
"""
        )

        result = metric.compute(None, "game-1", "2025-26", "game-1")
        self.assertEqual(metric.key, "sqlalchemy_metric")
        self.assertEqual(result.value_str, "ok")

    def test_metric_runtime_exceptions_still_surface_on_compute(self):
        runtime = self._load_runtime()

        metric = runtime.load_code_metric(
            """
from metrics.framework.base import MetricDefinition


class ExplodingMetric(MetricDefinition):
    key = "exploding_metric"
    name = "Exploding Metric"
    description = "Raises during compute."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        raise RuntimeError("boom")
"""
        )

        with self.assertRaisesRegex(RuntimeError, "boom"):
            metric.compute(None, "player-1", "2025-26")

    def test_importing_from_blocked_module_inside_compute_raises_value_error(self):
        runtime = self._load_runtime()

        with self.assertRaisesRegex(ValueError, "Import of 'os' is not allowed"):
            runtime.load_code_metric(
                """
from metrics.framework.base import MetricDefinition


class BadMetric(MetricDefinition):
    key = "bad_metric"
    name = "Bad Metric"
    description = "Should never load."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        from os import system
        return system("echo should-not-run")
"""
            )

    def test_importing_unrecognized_module_raises_value_error(self):
        runtime = self._load_runtime()

        with self.assertRaisesRegex(ValueError, "Import of 'requests' is not allowed"):
            runtime.load_code_metric(
                """
import requests
from metrics.framework.base import MetricDefinition


class BadMetric(MetricDefinition):
    key = "bad_metric"
    name = "Bad Metric"
    description = "Should never load."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
"""
            )

    def test_runtime_safe_import_raises_value_error(self):
        runtime = self._load_runtime()

        metric = runtime.load_code_metric(
            """
from metrics.framework.base import MetricDefinition


class BadMetric(MetricDefinition):
    key = "bad_metric"
    name = "Bad Metric"
    description = "Should fail when compute hits __import__."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return __builtins__["__import__"]("os")
"""
        )

        with self.assertRaisesRegex(ValueError, "Import of 'os' is not allowed"):
            metric.compute(None, "player-1", "2025-26")

    def test_load_code_metric_reuses_compiled_class_cache(self):
        runtime = self._load_runtime()
        code = """
from metrics.framework.base import MetricDefinition


class CachedMetric(MetricDefinition):
    key = "cached_metric"
    name = "Cached Metric"
    description = "Uses the compiled class cache."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        return None
"""
        runtime._load_code_metric_class.cache_clear()

        first = runtime.load_code_metric(code)
        second = runtime.load_code_metric(code)
        info = runtime._load_code_metric_class.cache_info()

        self.assertIsNot(first, second)
        self.assertEqual(info.misses, 1)
        self.assertGreaterEqual(info.hits, 1)

    def test_get_metric_uses_exact_key_lookup(self):
        runtime = self._load_runtime()
        session = MagicMock()
        row = types.SimpleNamespace(source_type="code")
        metric = object()

        with patch.object(runtime, "_lookup_published_metric_row", return_value=row) as lookup, \
             patch.object(runtime, "_build_runtime_metric", return_value=metric) as build:
            result = runtime.get_metric("custom_metric", session=session)

        self.assertIs(result, metric)
        lookup.assert_called_once_with(session, "custom_metric")
        build.assert_called_once_with(row)

    def test_get_metric_builds_dynamic_career_metric_from_base_row(self):
        runtime = self._load_runtime()
        session = MagicMock()
        base_row = types.SimpleNamespace(source_type="code")
        base_metric = types.SimpleNamespace(career=False, supports_career=True)
        career_metric = object()

        with patch.object(runtime, "_lookup_published_metric_row", side_effect=[None, base_row]) as lookup, \
             patch.object(runtime, "_build_runtime_metric", side_effect=[base_metric, career_metric]) as build:
            result = runtime.get_metric("custom_metric_career", session=session)

        self.assertIs(result, career_metric)
        self.assertEqual(
            lookup.call_args_list,
            [
                unittest.mock.call(session, "custom_metric_career"),
                unittest.mock.call(session, "custom_metric"),
            ],
        )
        self.assertEqual(
            build.call_args_list,
            [
                unittest.mock.call(base_row),
                unittest.mock.call(base_row, career=True, window_type="career"),
            ],
        )

    def test_get_metric_builds_dynamic_last3_metric_for_season_trigger_base_row(self):
        runtime = self._load_runtime()
        session = MagicMock()
        base_row = types.SimpleNamespace(source_type="code")
        base_metric = types.SimpleNamespace(career=False, supports_career=True, trigger="season")
        last3_metric = object()

        with patch.object(runtime, "_lookup_published_metric_row", side_effect=[None, base_row]) as lookup, \
             patch.object(runtime, "_build_runtime_metric", side_effect=[base_metric, last3_metric]) as build:
            result = runtime.get_metric("custom_metric_last3", session=session)

        self.assertIs(result, last3_metric)
        self.assertEqual(
            lookup.call_args_list,
            [
                unittest.mock.call(session, "custom_metric_last3"),
                unittest.mock.call(session, "custom_metric"),
            ],
        )
        self.assertEqual(
            build.call_args_list,
            [
                unittest.mock.call(base_row),
                unittest.mock.call(base_row, career=True, window_type="last3"),
            ],
        )

    def test_code_metric_definition_career_reducer_works_when_career_variant_disables_supports_career(self):
        runtime = self._load_runtime()
        row = types.SimpleNamespace(
            key="custom_metric_career",
            name="Custom Metric (Career)",
            description="Career metric",
            scope="player",
            category="aggregate",
            min_sample=5,
            source_type="code",
            status="published",
            group_key=None,
            family_key="custom_metric",
            variant="career",
            base_metric_key="custom_metric",
            managed_family=True,
            max_results_per_season=None,
            code_python="""
from metrics.framework.base import MetricDefinition, MetricResult


class CustomMetricCareer(MetricDefinition):
    key = "custom_metric_career"
    name = "Custom Metric (Career)"
    description = "Career metric"
    scope = "player"
    category = "aggregate"
    min_sample = 5
    trigger = "season"
    incremental = False
    career = True
    supports_career = False
    career_aggregate_mode = "season_results"
    career_sum_keys = ("count", "games")

    def compute_season(self, session, season):
        return []

    def compute_career_value(self, totals, season, entity_id):
        if not self.supports_career:
            return None
        count = int(totals.get("count", 0))
        games = int(totals.get("games", 0))
        if games < self.min_sample or count == 0:
            return None
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(count),
            value_str=str(count),
            context={"count": count, "games": games},
        )
""",
        )

        metric = runtime.CodeMetricDefinition(row)
        result = metric.compute_career_value({"count": 7, "games": 82}, "all_regular", "p1")

        self.assertIsNotNone(result)
        self.assertEqual(result.metric_key, "custom_metric_career")
        self.assertEqual(result.entity_id, "p1")
        self.assertEqual(result.value_num, 7.0)


class TestReadOnlySession(unittest.TestCase):
    def setUp(self):
        self.fake_db_models = _make_fake_db_models()
        sys.modules.pop("metrics.framework.runtime", None)
        sys.modules["db.models"] = self.fake_db_models
        if "db" in sys.modules:
            sys.modules["db"].models = self.fake_db_models

    def tearDown(self):
        sys.modules.pop("metrics.framework.runtime", None)
        if _ORIGINAL_DB_MODELS is not None:
            sys.modules["db.models"] = _ORIGINAL_DB_MODELS
        else:
            sys.modules.pop("db.models", None)
        if "db" in sys.modules:
            if _ORIGINAL_DB_ATTR is not None:
                sys.modules["db"].models = _ORIGINAL_DB_ATTR
            elif hasattr(sys.modules["db"], "models"):
                delattr(sys.modules["db"], "models")

    def _load_runtime(self):
        return importlib.import_module("metrics.framework.runtime")

    def _make_ro_session(self):
        runtime = self._load_runtime()
        inner = MagicMock()
        return runtime.ReadOnlySession(inner), inner

    def test_query_is_forwarded(self):
        ro, inner = self._make_ro_session()
        ro.query("Game")
        inner.query.assert_called_once_with("Game")

    def test_get_is_forwarded(self):
        ro, inner = self._make_ro_session()
        ro.get("Game", 1)
        inner.get.assert_called_once_with("Game", 1)

    def test_scalar_is_forwarded(self):
        ro, inner = self._make_ro_session()
        ro.scalar("stmt")
        inner.scalar.assert_called_once_with("stmt")

    def test_execute_raises_permission_error(self):
        ro, _ = self._make_ro_session()
        with self.assertRaisesRegex(PermissionError, "read-only.*execute"):
            ro.execute("DROP TABLE game")

    def test_commit_raises_permission_error(self):
        ro, _ = self._make_ro_session()
        with self.assertRaisesRegex(PermissionError, "read-only.*commit"):
            ro.commit()

    def test_add_raises_permission_error(self):
        ro, _ = self._make_ro_session()
        with self.assertRaisesRegex(PermissionError, "read-only.*add"):
            ro.add(object())

    def test_delete_raises_permission_error(self):
        ro, _ = self._make_ro_session()
        with self.assertRaisesRegex(PermissionError, "read-only.*delete"):
            ro.delete(object())

    def test_flush_raises_permission_error(self):
        ro, _ = self._make_ro_session()
        with self.assertRaisesRegex(PermissionError, "read-only.*flush"):
            ro.flush()

    def test_merge_raises_permission_error(self):
        ro, _ = self._make_ro_session()
        with self.assertRaisesRegex(PermissionError, "read-only.*merge"):
            ro.merge(object())

    def test_metric_code_using_session_execute_is_blocked(self):
        runtime = self._load_runtime()
        metric = runtime.load_code_metric(
            """
from sqlalchemy import text
from metrics.framework.base import MetricDefinition, MetricResult


class MaliciousMetric(MetricDefinition):
    key = "malicious_metric"
    name = "Malicious Metric"
    description = "Tries to drop a table."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        session.execute(text("DROP TABLE game"))
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=game_id,
            value_num=0.0,
        )
"""
        )
        ro = runtime.ReadOnlySession(MagicMock())
        with self.assertRaises(PermissionError):
            metric.compute(ro, "player-1", "2025-26")

    def test_metric_code_using_session_commit_is_blocked(self):
        runtime = self._load_runtime()
        metric = runtime.load_code_metric(
            """
from metrics.framework.base import MetricDefinition, MetricResult


class CommitMetric(MetricDefinition):
    key = "commit_metric"
    name = "Commit Metric"
    description = "Tries to commit."
    scope = "player"
    category = "scoring"
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        session.commit()
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=game_id,
            value_num=0.0,
        )
"""
        )
        ro = runtime.ReadOnlySession(MagicMock())
        with self.assertRaises(PermissionError):
            metric.compute(ro, "player-1", "2025-26")

    def test_unblocked_attr_falls_through(self):
        """Attributes not in the blocked set should proxy to the real session."""
        ro, inner = self._make_ro_session()
        inner.some_custom_attr = "hello"
        self.assertEqual(ro.some_custom_attr, "hello")


if __name__ == "__main__":
    unittest.main()
