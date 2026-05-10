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
    review_profile_for_post,
)


def _config():
    return PaperclipBridgeConfig(
        api_url="http://localhost:3000",
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


class TestPaperclipBridgeHelpers(unittest.TestCase):
    def test_desired_issue_state_maps_funba_statuses(self):
        cfg = _config()

        draft_state = desired_issue_state_for_post({"status": "draft"}, cfg)
        self.assertEqual(draft_state.status, "todo")
        self.assertEqual(draft_state.assignee_agent_id, "agent-analyst")
        self.assertEqual(draft_state.owner_label, "Content Analyst")

        ai_review_state = desired_issue_state_for_post({"status": "ai_review"}, cfg)
        self.assertEqual(ai_review_state.status, "todo")
        self.assertEqual(ai_review_state.assignee_agent_id, "agent-content-reviewer")
        self.assertEqual(ai_review_state.owner_label, "Content Reviewer")

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
        self.assertIn("雷霆球迷向 [thunder fans] (status: in_review) -> hupu/thunder", description)
        self.assertIn('"post_id": 42', description)
        self.assertIn('"source_metrics": [', description)

    def test_build_post_issue_description_warns_when_images_exist_without_placeholders(self):
        description = build_post_issue_description(
            {
                "id": 81,
                "source_date": "2026-04-01",
                "topic": "马克西得分稳定性联盟第四",
                "status": "draft",
                "priority": 30,
                "source_metrics": ["scoring_consistency"],
                "source_game_ids": ["0022501105"],
                "images": [
                    {"slot": "img1", "is_enabled": True},
                    {"slot": "img2", "is_enabled": True},
                ],
                "variants": [
                    {
                        "title": "智趣NBA: 马克西得分稳定性联盟第四",
                        "audience_hint": "general nba",
                        "content_raw": "这里只有正文，没有图片占位符。",
                        "destinations": [{"platform": "hupu", "forum": "湿乎乎的话题"}],
                    }
                ],
            }
        )

        self.assertIn("Image Placeholder Rules", description)
        self.assertIn("Target image pool size for normal social posts is 10+ images", description)
        self.assertIn("Content Analyst must place slot-based placeholders", description)
        self.assertIn("Enabled slots: img1, img2", description)
        self.assertIn("Image pool is only 2 item(s); target is at least 10", description)
        self.assertIn("no `[[IMAGE:slot=...]]` placeholder", description)

    def test_build_post_issue_description_uses_hero_profile(self):
        post = {
            "id": 481,
            "source_date": "2026-05-04",
            "topic": "Hero Highlight — 0042500231 — game — stocks — 0042500231:1641705",
            "status": "ai_review",
            "priority": 25,
            "pipeline_type": "hero_highlight",
            "source_metrics": ["stocks"],
            "source_game_ids": ["0042500231"],
            "images": [
                {"slot": "poster", "is_enabled": True},
            ],
            "variants": [
                {
                    "title": "Victor Wembanyama's 12 stocks",
                    "audience_hint": "deterministic hero highlight / funba",
                    "content_raw": "[[IMAGE:slot=poster]]\nVictor Wembanyama had 12 stocks.",
                    "destinations": [{"platform": "funba", "forum": None}],
                }
            ],
        }
        description = build_post_issue_description(post)

        self.assertEqual(review_profile_for_post(post), "hero_highlight")
        self.assertIn("Review profile: `hero_highlight`", description)
        self.assertIn("Reviewer playbook: `agents/content-reviewer/profiles/hero_highlight.md`", description)
        self.assertIn("Hero Highlight Review Rules", description)
        self.assertIn("Expected hero image slots: poster", description)
        self.assertIn('"review_profile": "hero_highlight"', description)
        self.assertNotIn("Target image pool size for normal social posts is 10+ images", description)
        self.assertNotIn("Image pool is only 1 item(s); target is at least 10", description)

    def test_build_post_issue_description_isolates_twitter_engagement_spec(self):
        post = {
            "id": 612,
            "source_date": "2026-05-04",
            "topic": "Twitter Reply - @nba_analyst - 1900000000000000001",
            "status": "in_review",
            "priority": 20,
            "pipeline_type": "twitter_engagement",
            "source_metrics": [],
            "source_game_ids": ["0022500001"],
            "twitter_context": {
                "conversation": {
                    "id": 9,
                    "x_conversation_id": "1900000000000000001",
                },
                "current_message": {
                    "id": 14,
                    "tweet_url": "https://x.com/nba_analyst/status/1900000000000000001",
                },
                "metric_contexts": [
                    {
                        "game_id": "0022500001",
                        "hero_signals": [
                            {
                                "metric_key": "game_total_steals",
                                "narrative_en": "The teams combined for 25 steals.",
                            }
                        ],
                    }
                ],
                "messages": [
                    {
                        "direction": "inbound",
                        "status": "drafted",
                        "author_username": "nba_analyst",
                        "text": "The Warriors offense looked scary.",
                    }
                ],
            },
            "variants": [
                {
                    "id": 88,
                    "title": "Paperclip reply for @nba_analyst",
                    "audience_hint": "Paperclip-written X reply. Confirm with Yue before sending.",
                    "content_raw": "seed",
                    "status": "in_review",
                    "destinations": [{"platform": "twitter_reply", "forum": "@nba_analyst"}],
                }
            ],
        }
        description = build_post_issue_description(post)

        self.assertIn("Funba is the source of truth for this X/Twitter reply work item", description)
        self.assertIn("NBA data analysis expert", description)
        self.assertIn("/api/admin/content/612/variants/88/update", description)
        self.assertIn("The teams combined for 25 steals", description)
        self.assertIn('"twitter_context": {', description)
        self.assertNotIn("Reviewer playbook", description)
        self.assertNotIn("Publishing with images", description)
        self.assertNotIn("agents/social-media", description)

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
