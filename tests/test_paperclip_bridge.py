import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from web.paperclip_bridge import (  # noqa: E402
    PaperclipBridgeConfig,
    append_admin_comment,
    build_post_issue_description,
    desired_issue_state_for_post,
    merge_paperclip_comments,
)


def _config():
    return PaperclipBridgeConfig(
        api_url="http://localhost:3000",
        api_key="test-key",
        company_id="company-1",
        project_id="project-1",
        content_analyst_agent_id="agent-analyst",
        delivery_publisher_agent_id="agent-delivery",
        review_user_id="user-review",
        content_analyst_name="Content Analyst",
        delivery_publisher_name="Delivery Publisher",
        review_user_name="Reviewer",
        company_name="xixihaha",
        timeout_seconds=10.0,
    )


class TestPaperclipBridgeHelpers(unittest.TestCase):
    def test_desired_issue_state_maps_funba_statuses(self):
        cfg = _config()

        draft_state = desired_issue_state_for_post({"status": "draft"}, cfg)
        self.assertEqual(draft_state.status, "todo")
        self.assertEqual(draft_state.assignee_agent_id, "agent-analyst")
        self.assertEqual(draft_state.owner_label, "Content Analyst")

        review_state = desired_issue_state_for_post({"status": "in_review"}, cfg)
        self.assertEqual(review_state.status, "in_review")
        self.assertEqual(review_state.assignee_user_id, "user-review")
        self.assertEqual(review_state.owner_label, "Reviewer")

        approved_state = desired_issue_state_for_post({"status": "approved"}, cfg)
        self.assertEqual(approved_state.status, "todo")
        self.assertEqual(approved_state.assignee_agent_id, "agent-delivery")
        self.assertEqual(approved_state.owner_label, "Delivery Publisher")

        archived_state = desired_issue_state_for_post({"status": "archived"}, cfg)
        self.assertEqual(archived_state.status, "cancelled")
        self.assertIsNone(archived_state.assignee_agent_id)
        self.assertEqual(archived_state.owner_label, "None")

    def test_build_post_issue_description_includes_variants_and_payload(self):
        description = build_post_issue_description(
            {
                "id": 42,
                "source_date": "2026-03-28",
                "topic": "雷霆大胜率排行分析",
                "status": "in_review",
                "priority": 30,
                "source_metrics": ["blowout_rate"],
                "source_game_ids": ["0022501066"],
                "variants": [
                    {
                        "title": "雷霆球迷向",
                        "audience_hint": "thunder fans",
                        "destinations": [{"platform": "hupu", "forum": "thunder"}],
                    }
                ],
            }
        )

        self.assertIn("Funba is the source of truth", description)
        self.assertIn("雷霆球迷向 [thunder fans] -> hupu/thunder", description)
        self.assertIn('"post_id": 42', description)
        self.assertIn('"source_metrics": [', description)

    def test_merge_paperclip_comments_appends_only_new_remote_comments(self):
        comments = []
        local_ts = append_admin_comment(
            comments,
            text="Please revise the hook.",
            author="yuewang",
            origin="funba_user",
        )
        self.assertTrue(local_ts)

        changed = merge_paperclip_comments(
            comments,
            [
                {
                    "id": "comment-1",
                    "body": "Revision drafted. Ready for review.",
                    "authorAgentId": "agent-analyst",
                    "authorUserId": None,
                    "createdAt": "2026-03-28T20:45:00Z",
                }
            ],
            cfg=_config(),
        )

        self.assertTrue(changed)
        self.assertEqual(len(comments), 2)
        self.assertEqual(comments[-1]["from"], "Content Analyst")
        self.assertEqual(comments[-1]["paperclip_comment_id"], "comment-1")

        changed_again = merge_paperclip_comments(
            comments,
            [
                {
                    "id": "comment-1",
                    "body": "Revision drafted. Ready for review.",
                    "authorAgentId": "agent-analyst",
                    "authorUserId": None,
                    "createdAt": "2026-03-28T20:45:00Z",
                }
            ],
            cfg=_config(),
        )
        self.assertFalse(changed_again)
        self.assertEqual(len(comments), 2)


if __name__ == "__main__":
    unittest.main()
