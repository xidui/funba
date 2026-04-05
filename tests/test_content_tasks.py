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


class TestGameScopedContentAnalysisIssues(unittest.TestCase):
    @patch("tasks.content._covered_game_ids_for_date", return_value=set())
    @patch(
        "tasks.content._game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_returns_existing_game_issue_without_force(self, mock_client_cls, mock_load_cfg, _mock_pipeline, _mock_covered):
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

    @patch("tasks.content._covered_game_ids_for_date", return_value=set())
    @patch(
        "tasks.content._game_context",
        return_value={"game_id": "0022501082", "matchup": "LAL @ BOS", "season": "22025"},
    )
    @patch(
        "tasks.content._game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082", "0022501083"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": ["0022501083"],
            "pending_metric_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_creates_issue_only_for_ready_games(self, mock_client_cls, mock_load_cfg, _mock_pipeline, _mock_game_context, _mock_covered):
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
        self.assertTrue(create_payload["title"].startswith("Game content analysis — funba — 2026-03-29 — 0022501082"))
        self.assertIn("Game ID: 0022501082", create_payload["description"])
        waiting = next(row for row in result["results"] if row["status"] == "waiting_for_pipeline")
        self.assertEqual(waiting["game_id"], "0022501083")
        self.assertEqual(waiting["pipeline_stage"], "artifacts")

    @patch("tasks.content._covered_game_ids_for_date", return_value={"0022501082"})
    @patch(
        "tasks.content._game_context",
        return_value={"game_id": "0022501083", "matchup": "NYK @ PHI", "season": "22025"},
    )
    @patch(
        "tasks.content._game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082", "0022501083"],
            "ready_game_ids": ["0022501082", "0022501083"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_skips_game_with_existing_posts_but_creates_for_new_game(self, mock_client_cls, mock_load_cfg, _mock_pipeline, _mock_game_context, _mock_covered):
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

    @patch("tasks.content._covered_game_ids_for_date", return_value={"0022501082"})
    @patch(
        "tasks.content._game_context",
        return_value={"game_id": "0022501082", "matchup": "LAL @ BOS", "season": "22025"},
    )
    @patch(
        "tasks.content._game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0022501082"],
            "ready_game_ids": ["0022501082"],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
        },
    )
    @patch("tasks.content.load_paperclip_bridge_config")
    @patch("tasks.content.PaperclipClient")
    def test_force_cancels_existing_game_issue_and_recreates_it(self, mock_client_cls, mock_load_cfg, _mock_pipeline, _mock_game_context, _mock_covered):
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
        mock_client.update_issue.assert_called_once()
        issue_id, payload = mock_client.update_issue.call_args.args
        self.assertEqual(issue_id, "issue-386")
        self.assertEqual(payload["status"], "cancelled")
        mock_client.create_issue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
