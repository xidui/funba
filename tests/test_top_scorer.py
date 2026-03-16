"""Tests for the top_scorer game metric.

Covers:
  (a) Normal top-scorer result — correct value_num, value_str, context fields.
  (b) No-data behavior — compute() returns None when no stats exist.
  (c) Tie-breaking determinism — ORDER BY uses a secondary key (player_id ASC).
  (d) Registry load — the metric self-registers as key="top_scorer", scope="game".
"""
import importlib
import sys
import types
import unittest
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Module stubs — installed before the metric module is imported
# ---------------------------------------------------------------------------

class _FakeCol:
    """Minimal fake SQLAlchemy Column supporting common filter/order operations."""
    def __gt__(self, other): return MagicMock()
    def __lt__(self, other): return MagicMock()
    def __ge__(self, other): return MagicMock()
    def __le__(self, other): return MagicMock()
    def __eq__(self, other): return MagicMock()
    def __ne__(self, other): return MagicMock()
    def isnot(self, other): return MagicMock()
    def is_(self, other): return MagicMock()
    def desc(self): return MagicMock()
    def asc(self): return MagicMock()
    def in_(self, other): return MagicMock()


def _install_stubs():
    """Install lightweight stubs for db.models and the metrics framework."""

    # ── metrics.framework.base ──────────────────────────────────────────────
    class FakeMetricDefinition:
        key = ""
        scope = ""
        category = ""
        min_sample = 1
        incremental = True
        supports_career = False
        career = False
        career_name_suffix = " (Career)"
        career_min_sample = None

        def compute(self, *a, **kw):
            return None

    class FakeMetricResult:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    fake_base = types.ModuleType("metrics.framework.base")
    fake_base.MetricDefinition = FakeMetricDefinition
    fake_base.MetricResult = FakeMetricResult
    fake_base.CAREER_SEASON = "all"
    sys.modules["metrics.framework.base"] = fake_base

    # ── metrics.framework.registry ──────────────────────────────────────────
    _registered: dict = {}

    def _register(m):
        _registered[m.key] = m

    fake_registry = types.ModuleType("metrics.framework.registry")
    fake_registry.register = _register
    fake_registry._registered = _registered
    sys.modules["metrics.framework.registry"] = fake_registry

    # ── db.models ───────────────────────────────────────────────────────────
    fake_models = types.ModuleType("db.models")

    class FakePlayerGameStats:
        pts = _FakeCol()
        game_id = _FakeCol()
        player_id = _FakeCol()

    class FakePlayer:
        player_id = _FakeCol()
        full_name = _FakeCol()

    fake_models.PlayerGameStats = FakePlayerGameStats
    fake_models.Player = FakePlayer
    sys.modules["db.models"] = fake_models

    return _registered


_REGISTERED = _install_stubs()


# ---------------------------------------------------------------------------
# Helper: import (or re-import) the metric module after stubs are in place
# ---------------------------------------------------------------------------

def _load_top_scorer_cls():
    sys.modules.pop("metrics.definitions.game.top_scorer", None)
    mod = importlib.import_module("metrics.definitions.game.top_scorer")
    return mod.TopScorer


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestTopScorerMetric(unittest.TestCase):

    def setUp(self):
        # Re-install stubs before each test: other test modules (e.g. test_auth.py)
        # may replace sys.modules["db.models"] with an incompatible stub.
        _install_stubs()

    def _make_session(self, first_result):
        """Return a mock session whose query chain resolves to first_result."""
        q = MagicMock()
        q.join.return_value = q
        q.filter.return_value = q
        q.order_by.return_value = q
        q.first.return_value = first_result

        session = MagicMock()
        session.query.return_value = q
        return session, q

    def _make_row(self, player_id, full_name, pts):
        row = MagicMock()
        row.pts = pts
        row.full_name = full_name
        row.player_id = player_id
        return row

    # (a) Normal result -------------------------------------------------------

    def test_normal_top_scorer_fields(self):
        TopScorer = _load_top_scorer_cls()
        row = self._make_row("p1", "Bam Adebayo", 83)
        session, _ = self._make_session(row)

        result = TopScorer().compute(session, "game123", "22025")

        self.assertIsNotNone(result)
        self.assertEqual(result.value_num, 83.0)
        self.assertEqual(result.value_str, "Bam Adebayo scored 83 pts")
        self.assertEqual(result.metric_key, "top_scorer")
        self.assertEqual(result.game_id, "game123")
        self.assertEqual(result.context["player_name"], "Bam Adebayo")
        self.assertEqual(result.context["player_id"], "p1")
        self.assertEqual(result.context["pts"], 83)

    def test_unknown_player_name_fallback(self):
        TopScorer = _load_top_scorer_cls()
        row = self._make_row("p_unknown", None, 20)
        session, _ = self._make_session(row)

        result = TopScorer().compute(session, "game456", "22025")

        self.assertIsNotNone(result)
        self.assertIn("Unknown", result.value_str)

    # (b) No-data behavior ----------------------------------------------------

    def test_no_data_returns_none(self):
        TopScorer = _load_top_scorer_cls()
        session, _ = self._make_session(None)

        result = TopScorer().compute(session, "game_empty", "22025")

        self.assertIsNone(result)

    # (c) Tie-breaking determinism --------------------------------------------

    def test_query_has_secondary_sort_for_tie_breaking(self):
        """order_by() must be called with exactly 2 args: pts DESC, player_id ASC."""
        TopScorer = _load_top_scorer_cls()
        session, q = self._make_session(None)

        TopScorer().compute(session, "game_tie", "22025")

        self.assertTrue(q.order_by.called, "order_by() should be called")
        args = q.order_by.call_args[0]
        self.assertEqual(
            len(args), 2,
            "Expected exactly 2 order_by args (pts DESC, player_id ASC) "
            f"for deterministic tie-breaking; got {len(args)}"
        )

    # (d) Registry load -------------------------------------------------------

    def test_registry_loads_top_scorer(self):
        _load_top_scorer_cls()  # triggers register() at module level
        # setUp() reinstalls stubs so the live registry is the one currently in
        # sys.modules["metrics.framework.registry"]._registered.
        live_reg = sys.modules["metrics.framework.registry"]._registered
        self.assertIn("top_scorer", live_reg)
        metric = live_reg["top_scorer"]
        self.assertEqual(metric.key, "top_scorer")
        self.assertEqual(metric.scope, "game")


if __name__ == "__main__":
    unittest.main()
