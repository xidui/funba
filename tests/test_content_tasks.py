import sys
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


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


class TestDailyContentAnalysisIssue(unittest.TestCase):
    @patch("tasks.content._covered_game_ids_for_date", return_value={"0022501082", "0022501083"})
    @patch("tasks.content._all_games_have_metrics", return_value=True)
    @patch(
        "tasks.content._pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082", "0022501083"],
            "artifacts_ready": True,
            "pending_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_returns_existing_issue_without_force(self, mock_client_cls, mock_load_cfg, _mock_pipeline, _mock_metrics, _mock_covered):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = [
            {
                "id": "issue-386",
                "identifier": "XIX-386",
                "title": "Daily content analysis — funba — 2026-03-29",
                "status": "done",
            }
        ]
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "exists")
        self.assertEqual(result["issue_identifier"], "XIX-386")
        mock_client.update_issue.assert_not_called()

    @patch("tasks.content._all_games_have_metrics", return_value=True)
    @patch(
        "tasks.content._pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082", "0022501083"],
            "artifacts_ready": True,
            "pending_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_force_creates_fresh_issue(self, mock_client_cls, mock_load_cfg, _mock_pipeline, _mock_metrics):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = [
            {
                "id": "issue-386",
                "identifier": "XIX-386",
                "title": "Daily content analysis — funba — 2026-03-29",
                "status": "done",
            }
        ]
        mock_client.create_issue.return_value = {
            "id": "issue-401",
            "identifier": "XIX-401",
            "status": "todo",
        }
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(
            date.fromisoformat("2026-03-29"),
            force=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "created")
        self.assertEqual(result["issue_identifier"], "XIX-401")
        mock_client.update_issue.assert_called_once()
        issue_id, payload = mock_client.update_issue.call_args.args
        self.assertEqual(issue_id, "issue-386")
        self.assertEqual(payload["status"], "cancelled")
        mock_client.create_issue.assert_called_once()

    @patch("tasks.content._covered_game_ids_for_date", return_value={"g1"})
    @patch("tasks.content._all_games_have_metrics", return_value=True)
    @patch(
        "tasks.content._pipeline_status_for_date",
        return_value={
            "game_ids": ["g1", "g2", "g3"],
            "artifacts_ready": True,
            "pending_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_creates_incremental_batch_only_for_uncovered_unclaimed_games(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_pipeline,
        _mock_metrics,
        _mock_covered,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = [
            {
                "id": "issue-1",
                "identifier": "XIX-500",
                "title": "Daily content analysis — funba — 2026-03-29",
                "status": "in_progress",
                "description": "Source date: 2026-03-29\nGame IDs: g2\n",
            }
        ]
        mock_client.create_issue.return_value = {
            "id": "issue-2",
            "identifier": "XIX-501",
            "status": "todo",
        }
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "created")
        self.assertEqual(result["batch_number"], 2)
        self.assertEqual(result["game_ids"], ["g3"])
        create_payload = mock_client.create_issue.call_args.args[0]
        self.assertEqual(create_payload["title"], "Daily content analysis — funba — 2026-03-29 — batch 2")
        self.assertIn("Game IDs: g3", create_payload["description"])
        self.assertIn("Batch scope:", create_payload["description"])

    @patch("tasks.content._covered_game_ids_for_date", return_value={"g1", "g2"})
    @patch("tasks.content._all_games_have_metrics", return_value=True)
    @patch(
        "tasks.content._pipeline_status_for_date",
        return_value={
            "game_ids": ["g1", "g2"],
            "artifacts_ready": True,
            "pending_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_returns_already_covered_when_all_games_are_covered(
        self,
        mock_client_cls,
        mock_load_cfg,
        _mock_pipeline,
        _mock_metrics,
        _mock_covered,
    ):
        cfg = _config()
        mock_load_cfg.return_value = cfg

        mock_client = MagicMock()
        mock_client.discover_defaults.return_value = cfg
        mock_client.list_issues.return_value = []
        mock_client_cls.return_value = mock_client

        result = ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "already_covered")
        self.assertEqual(result["covered_game_ids"], ["g1", "g2"])
        mock_client.create_issue.assert_not_called()


if __name__ == "__main__":
    unittest.main()
