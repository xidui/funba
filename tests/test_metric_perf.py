import importlib
import sys
import types
import unittest
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests.db_model_stubs import install_fake_db_module


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
    fake_base.CAREER_SEASONS = {"all_regular", "all_playoffs", "all_playin"}
    fake_base.MetricResult = MagicMock()
    fake_base.career_season_for = MagicMock(return_value=None)
    fake_base.is_career_season = MagicMock(return_value=False)
    fake_base.merge_totals = MagicMock(side_effect=lambda totals, delta: {**totals, **delta})
    fake_base.season_matches_metric_types = MagicMock(return_value=True)
    sys.modules["metrics.framework.base"] = fake_base

    fake_runtime = types.ModuleType("metrics.framework.runtime")
    fake_runtime._metric_declares_career_reducer = MagicMock(return_value=False)
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

    install_fake_db_module(
        REPO_ROOT,
        user_cls=fake_user_cls,
        engine=fake_engine,
        extra_model_names=("GameContentAnalysisIssue",),
    )

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


class TestSeasonMetricRunnerFailures(unittest.TestCase):
    def test_metric_result_write_lock_acquires_and_releases_connection_lock(self):
        runner = _import_runner_module()
        acquire_result = MagicMock()
        acquire_result.scalar.return_value = 1
        connection = MagicMock()
        connection.execute.side_effect = [acquire_result, None]
        session = MagicMock()
        session.connection.return_value = connection

        with runner._metric_result_write_lock(session, timeout_seconds=42):
            pass

        self.assertEqual(connection.execute.call_count, 2)
        acquire_call, release_call = connection.execute.call_args_list
        self.assertIn("GET_LOCK", str(acquire_call.args[0]))
        self.assertEqual(acquire_call.args[1]["name"], runner._METRIC_RESULT_WRITE_LOCK_NAME)
        self.assertEqual(acquire_call.args[1]["timeout_seconds"], 42)
        self.assertIn("RELEASE_LOCK", str(release_call.args[0]))
        self.assertEqual(release_call.args[1]["name"], runner._METRIC_RESULT_WRITE_LOCK_NAME)

    def test_run_season_metric_raises_for_missing_metric(self):
        runner = _import_runner_module()

        with self.assertRaisesRegex(LookupError, "missing_metric"):
            runner.run_season_metric(MagicMock(), "missing_metric", "22025", commit=False)

    def test_run_season_metric_reraises_compute_season_errors(self):
        runner = _import_runner_module()
        metric = SimpleNamespace(
            trigger="season",
            compute_season=MagicMock(side_effect=ValueError("boom")),
        )

        @contextmanager
        def _fake_count_db_ops(_session):
            yield lambda: (0, 0)

        with patch.object(runner, "get_metric", return_value=metric), patch.object(
            runner,
            "_count_db_ops",
            side_effect=_fake_count_db_ops,
        ), patch.object(runner, "_record_metric_perf") as perf_mock:
            with self.assertRaisesRegex(ValueError, "boom"):
                runner.run_season_metric(MagicMock(), "metric_a", "22025", commit=False)

        perf_mock.assert_not_called()

    def test_run_season_metric_commits_results_before_run_logs(self):
        runner = _import_runner_module()
        metric = SimpleNamespace(
            trigger="season",
            scope="team",
            career=False,
            compute_season=MagicMock(return_value=[SimpleNamespace(value_num=1.0)]),
            compute_qualifications=MagicMock(
                return_value=[{"game_id": "g1", "entity_id": "1610612737"}]
            ),
        )
        session = MagicMock()
        events: list[str] = []

        run_log_query = MagicMock()
        run_log_query.filter.return_value.delete.side_effect = (
            lambda synchronize_session=False: events.append("delete_runlogs") or 0
        )
        session.query.side_effect = [run_log_query]
        session.commit.side_effect = lambda: events.append("commit")

        @contextmanager
        def _fake_count_db_ops(_session):
            yield lambda: (1, 2)

        with patch.object(runner, "get_metric", return_value=metric), patch.object(
            runner,
            "_count_db_ops",
            side_effect=_fake_count_db_ops,
        ), patch.object(
            runner,
            "_flush_results",
            side_effect=lambda *_args, **_kwargs: events.append("flush_results"),
        ), patch.object(
            runner,
            "_flush_run_logs",
            side_effect=lambda *_args, **_kwargs: events.append("flush_run_logs"),
        ), patch.object(
            runner,
            "_record_metric_perf",
            side_effect=lambda *_args, **_kwargs: events.append("record_perf"),
        ):
            count = runner.run_season_metric(session, "metric_a", "22025", commit=True)

        self.assertEqual(count, 1)
        metric.compute_qualifications.assert_called_once_with(session, "22025")
        self.assertEqual(
            events,
            [
                "flush_results",
                "commit",
                "delete_runlogs",
                "flush_run_logs",
                "record_perf",
                "commit",
            ],
        )

    def test_reduce_metric_batches_metric_result_writes(self):
        runner = _import_runner_module()
        metric = SimpleNamespace(
            incremental=True,
            compute_value=MagicMock(
                side_effect=[
                    SimpleNamespace(
                        metric_key="metric_a",
                        entity_type="player",
                        entity_id="p1",
                        season="22025",
                        sub_key="",
                        rank_group=None,
                        game_id=None,
                        value_num=5.0,
                        value_str="5",
                        context=None,
                        noteworthiness=None,
                        notable_reason=None,
                    ),
                    None,
                ]
            ),
        )

        entity_query = MagicMock()
        entity_query.filter.return_value.distinct.return_value.order_by.return_value.all.return_value = [
            ("player", "p1"),
            ("player", "p2"),
        ]
        delta_query_1 = MagicMock()
        delta_query_1.filter.return_value.order_by.return_value.all.return_value = [('{"pts": 5}',)]
        delta_query_2 = MagicMock()
        delta_query_2.filter.return_value.order_by.return_value.all.return_value = [('{"pts": 2}',)]
        session = MagicMock()
        session.query.side_effect = [entity_query, delta_query_1, delta_query_2]

        @contextmanager
        def _fake_count_db_ops(_session):
            yield lambda: (0, 0)

        with patch.object(runner, "get_metric", return_value=metric), \
             patch.object(runner, "_count_db_ops", side_effect=_fake_count_db_ops), \
             patch.object(runner, "_flush_results") as flush_mock, \
             patch.object(runner, "_record_metric_perf"):
            written = runner.reduce_metric(session, "metric_a", "22025", commit=False)

        self.assertEqual(written, 1)
        flush_mock.assert_called_once()
        persisted_results = flush_mock.call_args.args[1]
        self.assertEqual(len(persisted_results), 2)
        self.assertEqual(getattr(persisted_results[0], "entity_id", None), "p1")


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


class TestMetricPreviewHelpers(unittest.TestCase):
    def test_preview_code_metric_reraises_season_compute_errors(self):
        web_app = _import_web_app()
        metric = SimpleNamespace(
            trigger="season",
            compute_season=MagicMock(side_effect=ValueError("boom")),
        )

        with patch.object(
            web_app,
            "_code_metric_metadata_from_code",
            return_value={
                "code_python": "class BrokenMetric: pass",
                "rank_order": "desc",
                "season_types": ("regular",),
            },
        ), patch("metrics.framework.runtime.load_code_metric", return_value=metric):
            with self.assertRaisesRegex(ValueError, "boom"):
                web_app._preview_code_metric(MagicMock(), "ignored", "season", "22025")


if __name__ == "__main__":
    unittest.main()
