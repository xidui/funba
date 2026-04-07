import sys
import unittest
from contextlib import nullcontext
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from content_pipeline.game_analysis_issues import _game_analysis_issue_creation_lock  # noqa: E402
from content_pipeline.game_analysis_issues import ensure_game_content_analysis_issue_for_game  # noqa: E402
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
        self.assertIn("one Xiaohongshu note variant", description)
        self.assertIn("destination `xiaohongshu/graph_note`", description)
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


if __name__ == "__main__":
    unittest.main()
