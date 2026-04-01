import importlib
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _import_runner_module():
    original_db = sys.modules.get("db")
    original_db_models = sys.modules.get("db.models")
    original_base = sys.modules.get("metrics.framework.base")
    original_runtime = sys.modules.get("metrics.framework.runtime")

    fake_models = types.ModuleType("db.models")
    for name in ("Game", "MetricPerfLog", "MetricResult", "MetricRunLog", "PlayerGameStats", "Team"):
        setattr(fake_models, name, MagicMock())
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

    fake_base = types.ModuleType("metrics.framework.base")
    fake_base.MetricResult = MagicMock()
    fake_base.career_season_for = MagicMock(return_value=None)
    fake_base.merge_totals = MagicMock(side_effect=lambda totals, delta: {**totals, **delta})
    sys.modules["metrics.framework.base"] = fake_base

    fake_runtime = types.ModuleType("metrics.framework.runtime")
    fake_runtime.get_all_metrics = MagicMock(return_value=[])
    fake_runtime.get_metric = MagicMock(return_value=None)
    sys.modules["metrics.framework.runtime"] = fake_runtime

    sys.modules.pop("metrics.framework.runner", None)
    module = importlib.import_module("metrics.framework.runner")

    if original_db is not None:
        sys.modules["db"] = original_db
    else:
        sys.modules.pop("db", None)

    if original_db_models is not None:
        sys.modules["db.models"] = original_db_models
    else:
        sys.modules.pop("db.models", None)

    if original_base is not None:
        sys.modules["metrics.framework.base"] = original_base
    else:
        sys.modules.pop("metrics.framework.base", None)

    if original_runtime is not None:
        sys.modules["metrics.framework.runtime"] = original_runtime
    else:
        sys.modules.pop("metrics.framework.runtime", None)

    return module


def _import_web_app():
    original_db = sys.modules.get("db")
    original_db_models = sys.modules.get("db.models")
    original_backfill = sys.modules.get("db.backfill_nba_player_shot_detail")

    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_flask_limiter = types.ModuleType("flask_limiter")

    class _FakeLimiter:
        def __init__(self, *args, **kwargs):
            pass

        def limit(self, *args, **kwargs):
            def _decorator(fn):
                return fn

            return _decorator

    fake_flask_limiter.Limiter = _FakeLimiter
    sys.modules["flask_limiter"] = fake_flask_limiter

    fake_authlib = types.ModuleType("authlib")
    fake_authlib_integrations = types.ModuleType("authlib.integrations")
    fake_authlib_flask_client = types.ModuleType("authlib.integrations.flask_client")

    class _FakeOAuth:
        def __init__(self, *args, **kwargs):
            pass

        def register(self, *args, **kwargs):
            return None

    fake_authlib_flask_client.OAuth = _FakeOAuth
    sys.modules["authlib"] = fake_authlib
    sys.modules["authlib.integrations"] = fake_authlib_integrations
    sys.modules["authlib.integrations.flask_client"] = fake_authlib_flask_client

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award", "Feedback", "Game", "GameLineScore", "GamePlayByPlay", "MagicToken", "MetricComputeRun",
        "MetricDefinition", "MetricPerfLog", "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "PlayerSalary", "ShotRecord", "SocialPost", "SocialPostImage", "SocialPostDelivery",
        "SocialPostVariant", "Team", "TeamGameStats",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.User = fake_user_cls
    fake_models.engine = fake_engine
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    for key in list(sys.modules):
        if key == "web.app" or key.startswith("web.app."):
            del sys.modules[key]

    module = importlib.import_module("web.app")

    if original_db is not None:
        sys.modules["db"] = original_db
    else:
        sys.modules.pop("db", None)

    if original_db_models is not None:
        sys.modules["db.models"] = original_db_models
    else:
        sys.modules.pop("db.models", None)

    if original_backfill is not None:
        sys.modules["db.backfill_nba_player_shot_detail"] = original_backfill
    else:
        sys.modules.pop("db.backfill_nba_player_shot_detail", None)

    return module


class TestMetricPerfRunnerHelpers(unittest.TestCase):
    def test_classify_sql_statement_tracks_reads_and_writes(self):
        runner = _import_runner_module()

        self.assertEqual(runner._classify_sql_statement("  select * from MetricRunLog"), "read")
        self.assertEqual(runner._classify_sql_statement("UPDATE MetricResult SET value_num = 1"), "write")
        self.assertEqual(runner._classify_sql_statement("DELETE FROM MetricPerfLog"), "write")
        self.assertIsNone(runner._classify_sql_statement("BEGIN"))


class TestMetricPerfAdminPanel(unittest.TestCase):
    def test_load_admin_metric_perf_panel_builds_latest_counts_and_samples(self):
        web_app = _import_web_app()

        stats_row = SimpleNamespace(
            metric_key="slow_metric",
            avg_ms=123.6,
            min_ms=90,
            max_ms=150,
            sample_count=5,
        )
        perf_rows = [
            SimpleNamespace(
                metric_key="slow_metric",
                duration_ms=150,
                recorded_at=datetime(2026, 3, 30, 1, 2, 3),
                db_reads=7,
                db_writes=2,
            ),
            SimpleNamespace(
                metric_key="slow_metric",
                duration_ms=130,
                recorded_at=datetime(2026, 3, 29, 1, 2, 3),
                db_reads=6,
                db_writes=1,
            ),
            SimpleNamespace(
                metric_key="slow_metric",
                duration_ms=120,
                recorded_at=datetime(2026, 3, 28, 1, 2, 3),
                db_reads=5,
                db_writes=1,
            ),
        ]

        stats_query = MagicMock()
        stats_query.group_by.return_value = stats_query
        stats_query.order_by.return_value = stats_query
        stats_query.count.return_value = 1
        stats_query.offset.return_value.limit.return_value.all.return_value = [stats_row]

        perf_query = MagicMock()
        perf_query.filter.return_value.order_by.return_value.all.return_value = perf_rows

        session = MagicMock()
        session.query.side_effect = [stats_query, perf_query]

        panel = web_app._load_admin_metric_perf_panel(session, perf_page=1, perf_page_size=20)

        self.assertEqual(panel["perf_page"], 1)
        self.assertEqual(panel["perf_total_pages"], 1)
        self.assertFalse(panel["perf_has_prev"])
        self.assertFalse(panel["perf_has_next"])
        self.assertEqual(len(panel["perf_data"]), 1)

        row = panel["perf_data"][0]
        self.assertEqual(row["metric_key"], "slow_metric")
        self.assertEqual(row["avg_ms"], 124)
        self.assertEqual(row["min_ms"], 90)
        self.assertEqual(row["max_ms"], 150)
        self.assertEqual(row["latest_ms"], 150)
        self.assertEqual(row["db_reads"], 7)
        self.assertEqual(row["db_writes"], 2)
        self.assertEqual(row["samples_ms"], [150, 130, 120])


if __name__ == "__main__":
    unittest.main()
