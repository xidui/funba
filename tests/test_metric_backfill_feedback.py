"""Tests for metric publish backfill feedback and status polling support."""
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_models = types.ModuleType("db.models")
    for name in (
        "Feedback", "Game", "GamePlayByPlay", "MetricJobClaim", "MetricDefinition",
        "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "ShotRecord", "Team", "TeamGameStats",
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

    import web.app as web_app

    web_app.app.config["TESTING"] = True
    return web_app


def _session_ctx(session):
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


def _query_mock(*, first=None, scalar=None):
    query = MagicMock()
    query.filter.return_value.first.return_value = first
    query.filter.return_value.scalar.return_value = scalar
    return query


class TestMetricBackfillFeedback(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()
        self.client = self.web_app.app.test_client()

    def test_publish_uses_shared_backfill_dispatch(self):
        metric = SimpleNamespace(status="draft", updated_at=None)
        session = _session_ctx(MagicMock())
        session.query.return_value.filter.return_value.first.return_value = metric

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "_dispatch_metric_backfill") as dispatch:
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "published")
        self.assertEqual(metric.status, "published")
        dispatch.assert_called_once_with("custom_metric")
        session.commit.assert_called_once()

    def test_publish_returns_warning_when_dispatch_fails(self):
        metric = SimpleNamespace(status="draft", updated_at=None)
        session = _session_ctx(MagicMock())
        session.query.return_value.filter.return_value.first.return_value = metric

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "_dispatch_metric_backfill", side_effect=RuntimeError("broker down")):
            response = self.client.post(
                "/api/metrics/custom_metric/publish",
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertIn("warning", body)
        self.assertEqual(metric.status, "published")

    def test_update_rebackfill_uses_shared_dispatch_helper(self):
        metric = SimpleNamespace(status="published", updated_at=None)
        session = _session_ctx(MagicMock())
        queries = [
            _query_mock(first=metric),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        ]
        session.query.side_effect = queries
        for query in queries[1:]:
            query.filter.return_value.delete.return_value = None

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(self.web_app, "_dispatch_metric_backfill") as dispatch:
            response = self.client.post(
                "/api/metrics/custom_metric/update",
                json={"rebackfill": True},
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])
        dispatch.assert_called_once_with("custom_metric")

    def test_backfill_status_endpoint_returns_combined_payload(self):
        backfill = {
            "status": "queued",
            "total_games": 120,
            "done_games": 0,
            "active_games": 0,
            "pending_games": 120,
            "progress_pct": 0.0,
            "latest_run_at": None,
            "components": [],
            "is_multi_component": False,
        }
        session = _session_ctx(MagicMock())

        with patch.object(self.web_app, "SessionLocal", return_value=session), \
             patch.object(
                 self.web_app,
                 "_build_metric_backfill_status",
                 return_value=(SimpleNamespace(key="custom_metric"), backfill),
             ):
            response = self.client.get("/api/metrics/custom_metric/backfill-status")

        body = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["ok"])
        self.assertEqual(body["metric_key"], "custom_metric")
        self.assertEqual(body["backfill"]["status"], "queued")

    def test_build_metric_backfill_status_formats_component_timestamps(self):
        db_metric = SimpleNamespace(
            key="custom_metric",
            name="Custom Metric",
            description="",
            scope="player",
            category="Shooting",
            status="published",
            source_type="rule",
            min_sample=1,
        )
        runtime_metric = SimpleNamespace(
            key="custom_metric",
            name="Custom Metric",
            description="",
            scope="player",
            category="Shooting",
            supports_career=True,
            career=False,
        )
        career_metric = SimpleNamespace(
            key="custom_metric_career",
            name="Custom Metric Career",
            description="",
            scope="player",
            category="Shooting",
            supports_career=False,
            career=True,
        )

        session = MagicMock()
        session.query.side_effect = [
            _query_mock(first=db_metric),
            _query_mock(scalar=100),
        ]

        fake_runtime = types.ModuleType("metrics.framework.runtime")
        fake_runtime.get_metric = MagicMock(side_effect=[runtime_metric, career_metric])

        component_results = [
            {
                "metric_key": "custom_metric",
                "status": "running",
                "done_games": 30,
                "active_games": 5,
                "pending_games": 65,
                "total_games": 100,
                "progress_pct": 30.0,
                "latest_run_at": datetime(2026, 3, 19, 12, 34, 56),
            },
            {
                "metric_key": "custom_metric_career",
                "status": "complete",
                "done_games": 100,
                "active_games": 0,
                "pending_games": 0,
                "total_games": 100,
                "progress_pct": 100.0,
                "latest_run_at": datetime(2026, 3, 19, 13, 0, 0),
            },
        ]

        with patch.dict(sys.modules, {"metrics.framework.runtime": fake_runtime}), \
             patch.object(self.web_app, "_metric_backfill_component", side_effect=component_results):
            metric_def, backfill = self.web_app._build_metric_backfill_status(session, "custom_metric")

        self.assertEqual(metric_def.key, "custom_metric")
        self.assertEqual(backfill["status"], "running")
        self.assertEqual(backfill["done_games"], 130)
        self.assertEqual(backfill["active_games"], 5)
        self.assertEqual(backfill["pending_games"], 65)
        self.assertEqual(backfill["latest_run_at"], "2026-03-19 13:00:00")
        self.assertEqual(backfill["components"][0]["latest_run_at"], "2026-03-19 12:34:56")
        self.assertEqual(backfill["components"][1]["label"], "Career")


if __name__ == "__main__":
    unittest.main()
