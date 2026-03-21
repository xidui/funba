"""Tests for loading generated code metrics safely."""

import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

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


if __name__ == "__main__":
    unittest.main()
