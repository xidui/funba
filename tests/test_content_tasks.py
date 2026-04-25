import sys
import unittest
from contextlib import nullcontext
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from content_pipeline.game_analysis_issues import _game_analysis_issue_creation_lock  # noqa: E402
from content_pipeline.game_analysis_issues import _classify_game_analysis_readiness  # noqa: E402
from content_pipeline.game_analysis_issues import AdvisoryLockUnavailable  # noqa: E402
from content_pipeline.game_analysis_issues import build_game_analysis_issue_description  # noqa: E402
from content_pipeline.game_analysis_issues import build_game_analysis_issue_title  # noqa: E402
from content_pipeline.game_analysis_issues import ensure_game_content_analysis_issue_for_game  # noqa: E402
from content_pipeline.game_analysis_issues import load_game_analysis_issue_template  # noqa: E402
from tasks import content as content_tasks  # noqa: E402
from tasks.content import ensure_daily_content_analysis_issue  # noqa: E402
from web.paperclip_bridge import PaperclipBridgeConfig  # noqa: E402


def _config():
    return PaperclipBridgeConfig(
        api_url="http://localhost:3100",
        api_key="test-key",
        company_id="company-1",
        project_id="project-1",
        content_analyst_agent_id="agent-analyst",
        content_reviewer_agent_id="agent-content-reviewer",
        delivery_publisher_agent_id="agent-delivery",
        review_user_id="user-review",
        content_analyst_name="Content Analyst",
        content_reviewer_name="Content Reviewer",
        delivery_publisher_name="Delivery Publisher",
        review_user_name="Reviewer",
        company_name="xixihaha",
        timeout_seconds=10.0,
    )


class TestGameAnalysisReadinessGates(unittest.TestCase):
    def test_requires_materialized_metric_results_before_curator(self):
        game = SimpleNamespace(
            game_id="0042500133",
            season="42025",
            highlights_curated_at=datetime(2026, 4, 23, 22, 0, 0),
        )

        detail = _classify_game_analysis_readiness(
            game,
            artifacts_supported=True,
            has_detail=True,
            has_pbp=True,
            entity_metric_count=20,
            metric_result_count=0,
            metric_run_count=343,
        )

        self.assertFalse(detail["ready"])
        self.assertEqual(detail["pipeline_stage"], "metrics")
        self.assertEqual(detail["metric_run_count"], 343)
        self.assertEqual(detail["entity_metric_count"], 20)
        self.assertEqual(detail["metric_result_count"], 0)

    def test_requires_curator_after_metrics_materialize(self):
        game = SimpleNamespace(
            game_id="0042500133",
            season="42025",
            highlights_curated_at=None,
        )

        detail = _classify_game_analysis_readiness(
            game,
            artifacts_supported=True,
            has_detail=True,
            has_pbp=True,
            entity_metric_count=20,
            metric_result_count=102,
            metric_run_count=343,
        )

        self.assertFalse(detail["ready"])
        self.assertEqual(detail["pipeline_stage"], "curator")

    def test_blocks_active_compute_runs_before_curator(self):
        game = SimpleNamespace(
            game_id="0042500133",
            season="42025",
            highlights_curated_at=datetime(2026, 4, 23, 22, 0, 0),
        )

        detail = _classify_game_analysis_readiness(
            game,
            artifacts_supported=True,
            has_detail=True,
            has_pbp=True,
            entity_metric_count=20,
            metric_result_count=102,
            active_metric_run_count=2,
            active_metric_keys=("metric_a", "metric_b"),
        )

        self.assertFalse(detail["ready"])
        self.assertEqual(detail["pipeline_stage"], "metrics")
        self.assertEqual(detail["active_metric_run_count"], 2)
        self.assertEqual(detail["active_metric_keys"], ["metric_a", "metric_b"])
        self.assertIn("mapping/reducing", detail["message"])

    def test_blocks_failed_compute_runs_before_curator(self):
        game = SimpleNamespace(
            game_id="0042500133",
            season="42025",
            highlights_curated_at=datetime(2026, 4, 23, 22, 0, 0),
        )

        detail = _classify_game_analysis_readiness(
            game,
            artifacts_supported=True,
            has_detail=True,
            has_pbp=True,
            entity_metric_count=20,
            metric_result_count=102,
            failed_metric_run_count=1,
            failed_metric_keys=("metric_busted",),
        )

        self.assertFalse(detail["ready"])
        self.assertEqual(detail["pipeline_stage"], "metrics")
        self.assertEqual(detail["failed_metric_run_count"], 1)
        self.assertEqual(detail["failed_metric_keys"], ["metric_busted"])
        self.assertIn("failed", detail["message"])

    def test_ready_only_when_all_gates_pass(self):
        game = SimpleNamespace(
            game_id="0042500133",
            season="42025",
            highlights_curated_at=datetime(2026, 4, 23, 22, 0, 0),
        )

        detail = _classify_game_analysis_readiness(
            game,
            artifacts_supported=True,
            has_detail=True,
            has_pbp=True,
            entity_metric_count=20,
            metric_result_count=102,
            metric_run_count=343,
        )

        self.assertTrue(detail["ready"])
        self.assertEqual(detail["pipeline_stage"], "ready")


class TestGameScopedContentAnalysisIssues(unittest.TestCase):
    @patch("content_pipeline.game_analysis_issues._LOCK_ENGINE")
    def test_game_analysis_lock_uses_dedicated_lock_engine(self, lock_engine):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.__exit__.return_value = False
        acquired_result = MagicMock()
        acquired_result.scalar.return_value = 1
        release_result = MagicMock()
        connection.execute.side_effect = [acquired_result, release_result]
        lock_engine.connect.return_value = connection

        with _game_analysis_issue_creation_lock(date.fromisoformat("2026-04-06")):
            pass

        lock_engine.connect.assert_called_once()

    @patch("content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock", return_value=nullcontext())
    @patch("content_pipeline.game_analysis_issues.covered_game_ids_for_date", return_value=set())
    @patch("content_pipeline.game_analysis_issues._latest_issue_row", return_value=None)
    @patch("content_pipeline.game_analysis_issues._record_issue_snapshot", return_value=None)
    @patch(
        "content_pipeline.game_analysis_issues.game_context",
        return_value={"game_id": "0022501082", "matchup": "LAL @ BOS", "season": "22025"},
    )
    @patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("content_pipeline.game_analysis_issues.load_paperclip_bridge_config")
    @patch("content_pipeline.game_analysis_issues.PaperclipClient")
    def test_batch_issue_creation_uses_date_lock(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_game_context,
        _mock_pipeline,
        _mock_recorded,
        _mock_latest,
        _mock_covered,
        mock_lock,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg
        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = []
        mock_client.create_issue.return_value = {
            "id": "issue-386",
            "identifier": "XIX-386",
            "status": "todo",
        }
        mock_client_cls.return_value = mock_client

        ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        mock_lock.assert_called_once_with(date.fromisoformat("2026-03-29"))

    @patch("content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock", return_value=nullcontext())
    @patch("content_pipeline.game_analysis_issues.covered_game_ids_for_date", return_value=set())
    @patch("content_pipeline.game_analysis_issues._latest_issue_row", return_value=None)
    @patch("content_pipeline.game_analysis_issues._record_issue_snapshot", return_value=None)
    @patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("content_pipeline.game_analysis_issues.load_paperclip_bridge_config")
    @patch("content_pipeline.game_analysis_issues.PaperclipClient")
    def test_returns_existing_game_issue_without_force(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_pipeline,
        _mock_recorded,
        _mock_latest,
        _mock_covered,
        _mock_lock,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = [
            {
                "id": "issue-386",
                "identifier": "XIX-386",
                "title": "Game content analysis — funba — 2026-03-29 — 0022501082",
                "status": "todo",
            }
        ]
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "exists")
        self.assertEqual(result["existing_count"], 1)
        self.assertEqual(result["results"][0]["game_id"], "0022501082")
        self.assertEqual(result["results"][0]["issue_identifier"], "XIX-386")
        mock_client.create_issue.assert_not_called()

    @patch("content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock", return_value=nullcontext())
    @patch("content_pipeline.game_analysis_issues.covered_game_ids_for_date", return_value=set())
    @patch("content_pipeline.game_analysis_issues._latest_issue_row", return_value=None)
    @patch("content_pipeline.game_analysis_issues._record_issue_snapshot", return_value=None)
    @patch(
        "content_pipeline.game_analysis_issues.game_context",
        return_value={"game_id": "0022501082", "matchup": "LAL @ BOS", "season": "22025"},
    )
    @patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082", "0022501083"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": ["0022501083"],
            "pending_metric_game_ids": [],
        },
    )
    @patch("content_pipeline.game_analysis_issues.load_paperclip_bridge_config")
    @patch("content_pipeline.game_analysis_issues.PaperclipClient")
    def test_creates_issue_only_for_ready_games(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_pipeline,
        _mock_game_context,
        _mock_record_issue,
        _mock_latest,
        _mock_covered,
        _mock_lock,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = []
        mock_client.create_issue.return_value = {
            "id": "issue-401",
            "identifier": "XIX-401",
            "status": "todo",
        }
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "created")
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["waiting_count"], 1)
        create_payload = mock_client.create_issue.call_args.args[0]
        description = create_payload["description"]
        self.assertTrue(create_payload["title"].startswith("Game content analysis — funba — 2026-03-29 — 0022501082"))
        self.assertIn("Game ID: 0022501082", description)
        self.assertIn("create variants for all platforms listed in the default target set", description)
        self.assertIn("Only create variants for currently enabled platforms: **hupu, xiaohongshu, reddit**.", description)
        waiting = next(row for row in result["results"] if row["status"] == "waiting_for_pipeline")
        self.assertEqual(waiting["game_id"], "0022501083")
        self.assertEqual(waiting["pipeline_stage"], "artifacts")

    @patch("content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock", return_value=nullcontext())
    @patch("content_pipeline.game_analysis_issues.covered_game_ids_for_date", return_value={"0022501082"})
    @patch("content_pipeline.game_analysis_issues._latest_issue_row", return_value=None)
    @patch("content_pipeline.game_analysis_issues._record_issue_snapshot", return_value=None)
    @patch(
        "content_pipeline.game_analysis_issues.game_context",
        return_value={"game_id": "0022501083", "matchup": "NYK @ PHI", "season": "22025"},
    )
    @patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082", "0022501083"],
            "ready_game_ids": ["0022501082", "0022501083"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("content_pipeline.game_analysis_issues.load_paperclip_bridge_config")
    @patch("content_pipeline.game_analysis_issues.PaperclipClient")
    def test_skips_game_with_existing_posts_but_creates_for_new_game(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_pipeline,
        _mock_game_context,
        _mock_record_issue,
        _mock_latest,
        _mock_covered,
        _mock_lock,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = []
        mock_client.create_issue.return_value = {
            "id": "issue-402",
            "identifier": "XIX-402",
            "status": "todo",
        }
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["covered_count"], 1)
        covered = next(row for row in result["results"] if row["status"] == "already_covered")
        created = next(row for row in result["results"] if row["status"] == "created")
        self.assertEqual(covered["game_id"], "0022501082")
        self.assertEqual(created["game_id"], "0022501083")

    @patch("content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock", return_value=nullcontext())
    @patch("content_pipeline.game_analysis_issues.covered_game_ids_for_date", return_value={"0022501082"})
    @patch("content_pipeline.game_analysis_issues._latest_issue_row", return_value=None)
    @patch("content_pipeline.game_analysis_issues._record_issue_snapshot", return_value=None)
    @patch(
        "content_pipeline.game_analysis_issues.game_context",
        return_value={"game_id": "0022501082", "matchup": "LAL @ BOS", "season": "22025"},
    )
    @patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("content_pipeline.game_analysis_issues.load_paperclip_bridge_config")
    @patch("content_pipeline.game_analysis_issues.PaperclipClient")
    def test_force_cancels_existing_game_issue_and_recreates_it(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_pipeline,
        _mock_game_context,
        _mock_record_issue,
        _mock_latest,
        _mock_covered,
        _mock_lock,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = [
            {
                "id": "issue-386",
                "identifier": "XIX-386",
                "title": "Game content analysis — funba — 2026-03-29 — 0022501082",
                "status": "done",
            }
        ]
        mock_client.create_issue.return_value = {
            "id": "issue-401",
            "identifier": "XIX-401",
            "status": "todo",
        }
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"), force=True)

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "created")
        mock_client.update_issue.assert_not_called()
        mock_client.create_issue.assert_called_once()

    @patch("content_pipeline.game_analysis_issues._latest_issue_row")
    @patch("content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock", return_value=nullcontext())
    @patch("content_pipeline.game_analysis_issues.covered_game_ids_for_date", return_value=set())
    @patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("content_pipeline.game_analysis_issues._game_source_date_or_raise", return_value=date.fromisoformat("2026-03-29"))
    @patch("content_pipeline.game_analysis_issues.load_paperclip_bridge_config")
    @patch("content_pipeline.game_analysis_issues.PaperclipClient")
    def test_single_game_trigger_uses_db_record_before_creating_new_issue(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_game_source_date,
        _mock_pipeline,
        _mock_lock,
        _mock_covered,
        mock_latest_issue_row,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_latest_issue_row.return_value = SimpleNamespace(
            id=9,
            paperclip_issue_id="issue-db-1",
            paperclip_issue_identifier="XIX-999",
            paperclip_issue_status="todo",
        )

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = []
        mock_client_cls.return_value = mock_client

        result = ensure_game_content_analysis_issue_for_game("0022501082")

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "exists")
        self.assertEqual(result["issue_identifier"], "XIX-999")
        self.assertEqual(result["db_issue_record_id"], 9)
        mock_client.create_issue.assert_not_called()


class TestCurateThenAnalyzeMetricGate(unittest.TestCase):
    def test_recent_content_analysis_enqueues_curator_backfill(self):
        with patch.object(
            content_tasks,
            "_enqueue_curator_for_pending_dates",
            return_value={
                "enqueued": 1,
                "pending_game_ids": ["0042500133"],
                "seasons": ["42025"],
            },
        ) as backfill_mock, patch.object(
            content_tasks,
            "ensure_recent_content_analysis",
            return_value={"ok": False, "checked_dates": ["2026-04-23"], "results": []},
        ):
            result = content_tasks.ensure_recent_content_analysis_task.run(
                source_dates=["2026-04-23"],
                lookback_days=3,
            )

        backfill_mock.assert_called_once_with([date.fromisoformat("2026-04-23")], lookback_days=3)
        self.assertEqual(result["curator_backfill"]["pending_game_ids"], ["0042500133"])

    def test_recent_content_analysis_can_skip_curator_backfill(self):
        with patch.object(content_tasks, "_enqueue_curator_for_pending_dates") as backfill_mock, patch.object(
            content_tasks,
            "ensure_recent_content_analysis",
            return_value={"ok": True, "checked_dates": ["2026-04-23"], "results": []},
        ):
            result = content_tasks.ensure_recent_content_analysis_task.run(
                source_dates=["2026-04-23"],
                lookback_days=3,
                enqueue_curator=False,
            )

        backfill_mock.assert_not_called()
        self.assertNotIn("curator_backfill", result)

    def test_waits_when_metric_compute_runs_are_active(self):
        issues_module = MagicMock()
        issues_module.recent_game_dates_for_season.return_value = [date.fromisoformat("2026-04-23")]
        issues_module.metric_compute_run_blockers.return_value = {
            "active_metric_run_count": 1,
            "failed_metric_run_count": 0,
            "active_metric_keys": ["metric_a"],
            "failed_metric_keys": [],
        }

        with patch.object(content_tasks, "_game_analysis_issues_module", return_value=issues_module), patch.object(
            content_tasks.ensure_recent_content_analysis_for_season_task,
            "delay",
        ) as analysis_delay:
            result = content_tasks.curate_then_analyze_for_season_task.run("42025", lookback_days=3)

        self.assertEqual(result["status"], "waiting_for_metrics")
        self.assertEqual(result["active_metric_run_count"], 1)
        self.assertEqual(result["game_dates"], ["2026-04-23"])
        analysis_delay.assert_not_called()

    def test_blocks_when_metric_compute_runs_failed(self):
        issues_module = MagicMock()
        issues_module.recent_game_dates_for_season.return_value = [date.fromisoformat("2026-04-23")]
        issues_module.metric_compute_run_blockers.return_value = {
            "active_metric_run_count": 0,
            "failed_metric_run_count": 1,
            "active_metric_keys": [],
            "failed_metric_keys": ["metric_busted"],
        }

        with patch.object(content_tasks, "_game_analysis_issues_module", return_value=issues_module), patch.object(
            content_tasks.ensure_recent_content_analysis_for_season_task,
            "delay",
        ) as analysis_delay:
            result = content_tasks.curate_then_analyze_for_season_task.run("42025", lookback_days=3)

        self.assertEqual(result["status"], "blocked_failed_metrics")
        self.assertEqual(result["failed_metric_run_count"], 1)
        self.assertEqual(result["failed_metric_keys"], ["metric_busted"])
        analysis_delay.assert_not_called()

    def test_curator_only_runs_games_at_curator_stage(self):
        ready_game = SimpleNamespace(game_id="ready-game", highlights_curated_at=None)
        list_session = MagicMock()
        list_session.query.return_value.filter.return_value.all.return_value = [("ready-game",), ("waiting-game",)]
        ready_session = MagicMock()
        ready_session.query.return_value.filter.return_value.first.return_value = ready_game
        list_session_cm = MagicMock()
        list_session_cm.__enter__.return_value = list_session
        list_session_cm.__exit__.return_value = False
        ready_session_cm = MagicMock()
        ready_session_cm.__enter__.return_value = ready_session
        ready_session_cm.__exit__.return_value = False

        issues_module = MagicMock()
        issues_module.recent_game_dates_for_season.return_value = [date.fromisoformat("2026-04-23")]
        issues_module.metric_compute_run_blockers.return_value = {
            "active_metric_run_count": 0,
            "failed_metric_run_count": 0,
            "active_metric_keys": [],
            "failed_metric_keys": [],
        }
        issues_module.game_analysis_readiness_detail.side_effect = lambda game_id: {
            "pipeline_stage": "curator" if game_id == "ready-game" else "metrics",
        }

        with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}), patch.object(
            content_tasks,
            "_game_analysis_issues_module",
            return_value=issues_module,
        ), patch("sqlalchemy.orm.Session", side_effect=[list_session_cm, ready_session_cm]), patch(
            "metrics.highlights.curator.run_curator_for_game",
        ) as curator_mock, patch.object(
            content_tasks.ensure_recent_content_analysis_for_season_task,
            "delay",
        ):
            result = content_tasks.curate_then_analyze_for_season_task.run("42025", lookback_days=3)

        curator_mock.assert_called_once_with(ready_session, ready_game)
        self.assertEqual(result["curated"], 1)
        self.assertEqual(result["waiting"], 1)

    def test_recent_content_analysis_for_season_skips_duplicate_lock_conflict(self):
        issues_module = MagicMock()
        issues_module.recent_game_dates_for_season.return_value = [date.fromisoformat("2026-04-25")]

        with patch.object(
            content_tasks,
            "_game_analysis_issues_module",
            return_value=issues_module,
        ), patch.object(
            content_tasks,
            "ensure_recent_content_analysis",
            side_effect=AdvisoryLockUnavailable("Failed to acquire game-analysis lock 'gca:2026-04-25'"),
        ), patch.object(
            content_tasks.ensure_recent_content_analysis_for_season_task,
            "retry",
            side_effect=AssertionError("retry not expected"),
        ):
            result = content_tasks.ensure_recent_content_analysis_for_season_task.run("42025", lookback_days=3)

        self.assertEqual(
            result,
            {
                "ok": True,
                "status": "lock_busy",
                "season": "42025",
                "checked_dates": ["2026-04-25"],
                "lookback_days": 3,
                "force": False,
            },
        )

    def test_recent_content_analysis_wraps_retry_exception(self):
        class CustomContentError(Exception):
            pass

        content_tasks.ensure_recent_content_analysis_task.push_request(id="worker-1", retries=0)
        try:
            with patch.object(
                content_tasks,
                "_enqueue_curator_for_pending_dates",
                return_value={"enqueued": 0, "pending_game_ids": [], "seasons": []},
            ), patch.object(
                content_tasks,
                "ensure_recent_content_analysis",
                side_effect=CustomContentError("boom"),
            ), patch.object(
                content_tasks.ensure_recent_content_analysis_task,
                "retry",
                side_effect=RuntimeError("retrying"),
            ) as retry_mock:
                with self.assertRaisesRegex(RuntimeError, "retrying"):
                    content_tasks.ensure_recent_content_analysis_task.run(
                        source_dates=["2026-04-25"],
                        lookback_days=3,
                    )
        finally:
            content_tasks.ensure_recent_content_analysis_task.pop_request()

        self.assertIsInstance(retry_mock.call_args.kwargs["exc"], RuntimeError)
        self.assertIn("CustomContentError", str(retry_mock.call_args.kwargs["exc"]))
        self.assertIn("boom", str(retry_mock.call_args.kwargs["exc"]))


class TestGameAnalysisIssueTemplate(unittest.TestCase):
    @patch(
        "content_pipeline.game_analysis_issues.game_context",
        return_value={"game_id": "0042500101", "matchup": "ORL @ DET", "season": "42025"},
    )
    def test_description_renders_without_stray_placeholders(self, _mock_context):
        # Guards against unescaped {foo} tokens in the markdown template —
        # str.format raises KeyError and kills the beat task if a placeholder
        # slips in that build_game_analysis_issue_description does not pass.
        load_game_analysis_issue_template.cache_clear()
        target = date.fromisoformat("2026-04-19")

        title = build_game_analysis_issue_title(target, "0042500101")
        body = build_game_analysis_issue_description(target, "0042500101")

        self.assertIn("0042500101", title)
        self.assertIn("ORL @ DET", title)
        self.assertIn("0042500101", body)
        self.assertIn("ORL @ DET", body)
        self.assertIn("42025", body)


if __name__ == "__main__":
    unittest.main()
