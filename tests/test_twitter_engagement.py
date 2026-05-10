from __future__ import annotations

import json
import sys
import unittest
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from content_pipeline.twitter_engagement import (  # noqa: E402
    ENGAGEMENT_EVENT_TYPE,
    TWITTER_REPLY_DRAFT_PLATFORM,
    build_game_metric_contexts,
    build_twitter_engagement_issue_description,
    build_recent_search_query,
    discover_twitter_engagement_candidates,
    parse_recent_search_payload,
    wake_paperclip_twitter_engagement_agent,
)
from db.models import (  # noqa: E402
    Base,
    Game,
    MetricDefinition,
    MetricResult,
    Player,
    PlayerGameStats,
    SocialPost,
    SocialPostDelivery,
    SocialPostVariant,
    Team,
    TwitterEngagementConversation,
    TwitterEngagementMessage,
)
from web.app import _build_social_post_rows  # noqa: E402
from web.paperclip_bridge import PaperclipBridgeConfig  # noqa: E402


class TestTwitterEngagementDiscovery(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.now = datetime(2026, 5, 2, 18, 0, 0)

    def tearDown(self):
        self.engine.dispose()

    def _seed_game(self, session) -> None:
        curated_game_metrics = json.dumps(
            {
                "version": 1,
                "hero": [
                    {
                        "metric_key": "game_total_steals",
                        "entity_id": "0022500001",
                        "narrative_en": "The teams combined for 25 steals, 2nd over the last 5 playoff seasons.",
                        "narrative_zh": "双方合计25次抢断，近5季季后赛第2。",
                        "value_snapshot": 25,
                        "value_str_snapshot": "25",
                        "rank_snapshot": {"last5": 2, "last5_total": 353},
                        "rank_window": "last5",
                        "season": "42025",
                    }
                ],
                "notable": [
                    {
                        "metric_key": "warriors_transition_efficiency",
                        "entity_id": "1610612744",
                        "team_id": "1610612744",
                        "team_abbr": "GSW",
                        "narrative_en": "Golden State had its most efficient transition game of the season.",
                        "narrative_zh": "勇士打出本季最高效转换进攻。",
                        "value_snapshot": 1.42,
                        "value_str_snapshot": "1.42 PPP",
                        "rank_snapshot": {"season": 1, "season_total": 82},
                        "rank_window": "season",
                        "season": "22025",
                    }
                ],
            },
            ensure_ascii=False,
        )
        session.add_all(
            [
                Team(
                    team_id="1610612744",
                    slug="warriors",
                    full_name="Golden State Warriors",
                    abbr="GSW",
                    nick_name="Warriors",
                    city="Golden State",
                    active=True,
                ),
                Team(
                    team_id="1610612747",
                    slug="lakers",
                    full_name="Los Angeles Lakers",
                    abbr="LAL",
                    nick_name="Lakers",
                    city="Los Angeles",
                    active=True,
                ),
                Player(
                    player_id="201939",
                    full_name="Stephen Curry",
                    first_name="Stephen",
                    last_name="Curry",
                    is_active=True,
                ),
                Game(
                    game_id="0022500001",
                    slug="warriors-lakers-2026-05-01",
                    season="22025",
                    game_date=date.fromisoformat("2026-05-01"),
                    home_team_id="1610612744",
                    road_team_id="1610612747",
                    wining_team_id="1610612744",
                    game_status="completed",
                    home_team_score=122,
                    road_team_score=115,
                    highlights_curated_json=curated_game_metrics,
                    highlights_curated_player_json=json.dumps({"version": 1, "hero": [], "notable": []}),
                    highlights_curated_team_json=json.dumps({"version": 1, "hero": [], "notable": []}),
                    highlights_curated_at=self.now,
                ),
                MetricDefinition(
                    key="game_total_steals",
                    family_key="game_total_steals",
                    name="Combined Steals",
                    name_zh="合计抢断",
                    scope="game",
                    status="published",
                    source_type="rule",
                    created_at=self.now,
                    updated_at=self.now,
                ),
                MetricDefinition(
                    key="warriors_transition_efficiency",
                    family_key="warriors_transition_efficiency",
                    name="Warriors Transition Efficiency",
                    name_zh="勇士转换效率",
                    scope="team",
                    status="published",
                    source_type="rule",
                    created_at=self.now,
                    updated_at=self.now,
                ),
                MetricDefinition(
                    key="curry_pullup_threes",
                    family_key="curry_pullup_threes",
                    name="Curry Pull-Up Threes",
                    name_zh="库里急停三分",
                    scope="player",
                    status="published",
                    source_type="rule",
                    created_at=self.now,
                    updated_at=self.now,
                ),
                MetricResult(
                    metric_key="curry_pullup_threes",
                    entity_type="player",
                    entity_id="201939",
                    season="22025",
                    sub_key="",
                    rank_group="season",
                    game_id="0022500001",
                    value_num=9,
                    value_str="9 pull-up 3PM",
                    context_json=json.dumps({"rank": 1, "total": 82}),
                    noteworthiness=0.94,
                    notable_reason="Curry's highest pull-up three total of the season.",
                    computed_at=self.now,
                ),
                PlayerGameStats(
                    game_id="0022500001",
                    team_id="1610612744",
                    player_id="201939",
                    pts=37,
                ),
            ]
        )
        session.commit()

    def _payload(self) -> dict:
        return {
            "data": [
                {
                    "id": "1900000000000000001",
                    "text": "The Warriors offense looked scary against the Lakers tonight.",
                    "author_id": "100",
                    "conversation_id": "1900000000000000001",
                    "created_at": "2026-05-02T17:00:00Z",
                    "public_metrics": {
                        "retweet_count": 8,
                        "reply_count": 20,
                        "like_count": 120,
                        "quote_count": 3,
                    },
                }
            ],
            "includes": {
                "users": [
                    {
                        "id": "100",
                        "username": "nba_analyst",
                        "name": "NBA Analyst",
                        "verified": True,
                        "public_metrics": {"followers_count": 250000},
                    }
                ]
            },
            "meta": {"newest_id": "1900000000000000001", "result_count": 1},
        }

    def _thread_payload(self) -> dict:
        payload = self._payload()
        payload["data"] = [
            payload["data"][0],
            {
                "id": "1900000000000000002",
                "text": "@funba_app Any player page for that Warriors Lakers game?",
                "author_id": "100",
                "conversation_id": "1900000000000000001",
                "referenced_tweets": [
                    {"type": "replied_to", "id": "1900000000000000001"},
                ],
                "created_at": "2026-05-02T17:30:00Z",
                "public_metrics": {
                    "retweet_count": 1,
                    "reply_count": 3,
                    "like_count": 12,
                    "quote_count": 0,
                },
            },
        ]
        payload["meta"] = {"newest_id": "1900000000000000002", "result_count": 2}
        return payload

    def test_build_recent_search_query_accepts_target_handles(self):
        query = build_recent_search_query(handles=["@nba_analyst"], terms=["Warriors"])

        self.assertIn("from:nba_analyst", query)
        self.assertIn("Warriors", query)
        self.assertIn("-is:retweet", query)

    def test_build_recent_search_query_can_include_mentions_for_followups(self):
        query = build_recent_search_query(
            handles=["@nba_analyst"],
            terms=["Warriors"],
            account_handle="@funba_app",
            include_mentions=True,
        )

        self.assertIn("@funba_app", query)
        self.assertIn("to:funba_app", query)
        self.assertIn("from:nba_analyst", query)
        self.assertIn("-is:reply", query)
        self.assertIn("-is:retweet", query)

    def test_parse_recent_search_payload_expands_author_metrics(self):
        candidates = parse_recent_search_payload(self._payload())

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].author.username, "nba_analyst")
        self.assertTrue(candidates[0].author.verified)
        self.assertEqual(candidates[0].author.followers_count, 250000)
        self.assertEqual(candidates[0].conversation_id, "1900000000000000001")

    def test_discovery_creates_disabled_manual_confirmation_reply_draft(self):
        with self.SessionLocal() as session:
            self._seed_game(session)

            result = discover_twitter_engagement_candidates(
                session,
                search_payload=self._payload(),
                daily_limit=2,
                min_score=0,
                base_url="https://funba.test",
                paperclip=False,
                now_utc=self.now,
            )
            session.commit()

            self.assertTrue(result["ok"])
            self.assertTrue(result["manual_confirmation_required"])
            self.assertEqual(len(result["created_reply_post_ids"]), 1)
            self.assertEqual(len(result["stored_message_ids"]), 1)

            post = session.query(SocialPost).first()
            conversation = session.query(TwitterEngagementConversation).one()
            message = session.query(TwitterEngagementMessage).one()
            variant = session.query(SocialPostVariant).filter(SocialPostVariant.post_id == post.id).one()
            delivery = session.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id == variant.id).one()
            comments = json.loads(post.admin_comments)

            self.assertEqual(conversation.x_conversation_id, "1900000000000000001")
            self.assertEqual(message.conversation_id, conversation.id)
            self.assertEqual(message.tweet_id, "1900000000000000001")
            self.assertEqual(message.reply_post_id, post.id)
            self.assertEqual(message.status, "drafted")
            self.assertEqual(post.status, "in_review")
            self.assertEqual(json.loads(post.source_game_ids), ["0022500001"])
            self.assertEqual(comments[0]["event_type"], ENGAGEMENT_EVENT_TYPE)
            self.assertIn("Manual confirmation is required", comments[0]["text"])
            self.assertIn("[Paperclip seed - rewrite before sending]", variant.content_raw)
            self.assertIn("GSW 122", variant.content_raw)
            self.assertIn("Stephen Curry with 37", variant.content_raw)
            self.assertEqual(delivery.platform, TWITTER_REPLY_DRAFT_PLATFORM)
            self.assertFalse(delivery.is_enabled)
            self.assertIn("Manual confirmation required", delivery.error_message)

    def test_thread_interactions_share_one_conversation_with_message_linked_reply_posts(self):
        with self.SessionLocal() as session:
            self._seed_game(session)

            result = discover_twitter_engagement_candidates(
                session,
                search_payload=self._thread_payload(),
                daily_limit=10,
                min_score=0,
                base_url="https://funba.test",
                paperclip=False,
                now_utc=self.now,
            )
            session.commit()

            conversations = session.query(TwitterEngagementConversation).all()
            messages = session.query(TwitterEngagementMessage).order_by(TwitterEngagementMessage.tweet_id).all()
            posts = session.query(SocialPost).order_by(SocialPost.id).all()

            self.assertEqual(len(conversations), 1)
            self.assertEqual(conversations[0].x_conversation_id, "1900000000000000001")
            self.assertEqual(len(messages), 2)
            self.assertEqual(len(posts), 2)
            self.assertEqual(len(result["created_reply_post_ids"]), 2)
            self.assertEqual({message.conversation_id for message in messages}, {conversations[0].id})
            self.assertEqual({message.reply_post_id for message in messages}, {post.id for post in posts})
            self.assertEqual(messages[1].parent_tweet_id, "1900000000000000001")
            rows = _build_social_post_rows(session, posts)
            first_context = rows[0]["twitter_context"]
            self.assertEqual(first_context["conversation"]["message_count"], 2)
            self.assertEqual(first_context["conversation"]["inbound_count"], 2)
            self.assertEqual(first_context["conversation"]["outbound_count"], 0)
            self.assertEqual(first_context["metric_contexts"][0]["hero_signals"][0]["metric_key"], "game_total_steals")
            self.assertEqual(
                first_context["metric_contexts"][0]["notable_metric_results"][0]["notable_reason"],
                "Curry's highest pull-up three total of the season.",
            )
            self.assertEqual(len(first_context["messages"]), 2)

    def test_discovery_creates_paperclip_issue_for_llm_reply_rewrite(self):
        cfg = PaperclipBridgeConfig(
            api_url="https://paperclip.test",
            api_key="token",
            company_id="company-1",
            project_id="project-1",
            content_analyst_agent_id="agent-analyst",
            content_reviewer_agent_id=None,
            delivery_publisher_agent_id=None,
            review_user_id=None,
            content_analyst_name="Content Analyst",
            content_reviewer_name="Content Reviewer",
            delivery_publisher_name="Delivery Publisher",
            review_user_name="Reviewer",
            company_name="FUNBA",
            timeout_seconds=5,
        )
        created_payloads = []
        wake_calls = []

        class FakePaperclipClient:
            def __init__(self, config):
                self.cfg = config

            def discover_defaults(self):
                return self.cfg

            def create_issue(self, payload):
                created_payloads.append(payload)
                return {
                    "id": "issue-1",
                    "identifier": "FUN-1",
                    "status": "todo",
                    "assigneeAgentId": payload.get("assigneeAgentId"),
                    "assigneeUserId": payload.get("assigneeUserId"),
                }

            def update_issue(self, issue_id, payload):
                raise AssertionError("new draft should create, not update")

            def wake_agent(self, agent_id, *, reason, payload=None, force_fresh_session=False):
                wake_calls.append(
                    {
                        "agent_id": agent_id,
                        "reason": reason,
                        "payload": payload,
                        "force_fresh_session": force_fresh_session,
                    }
                )
                return {"ok": True}

        with self.SessionLocal() as session:
            self._seed_game(session)
            with patch(
                "content_pipeline.twitter_engagement.load_paperclip_bridge_config",
                return_value=cfg,
            ), patch("content_pipeline.twitter_engagement.PaperclipClient", FakePaperclipClient):
                result = discover_twitter_engagement_candidates(
                    session,
                    search_payload=self._payload(),
                    daily_limit=2,
                    min_score=0,
                    base_url="https://funba.test",
                    paperclip=True,
                    now_utc=self.now,
                )
                session.commit()
                wake_result = wake_paperclip_twitter_engagement_agent(result["paperclip_wakeup_requests"][0])

            post = session.query(SocialPost).first()
            variant = session.query(SocialPostVariant).filter(SocialPostVariant.post_id == post.id).one()

            self.assertEqual(post.paperclip_issue_id, "issue-1")
            self.assertEqual(post.paperclip_issue_identifier, "FUN-1")
            self.assertEqual(post.llm_model, "paperclip_content_analyst")
            message = session.query(TwitterEngagementMessage).one()

            self.assertEqual(result["paperclip_results"][0]["issue_id"], "issue-1")
            self.assertEqual(result["paperclip_wakeup_requests"][0]["variant_id"], variant.id)
            self.assertTrue(wake_result["ok"])
            self.assertEqual(wake_calls[0]["reason"], "twitter_engagement_reply_work_item")
            self.assertEqual(wake_calls[0]["payload"]["workflow"], "twitter_engagement")

            payload = created_payloads[0]
            self.assertEqual(payload["assigneeAgentId"], "agent-analyst")
            self.assertIn(f"/api/admin/content/{post.id}/variants/{variant.id}/update", payload["description"])
            self.assertIn(f'"message_db_id": {message.id}', payload["description"])
            self.assertIn('"x_conversation_id": "1900000000000000001"', payload["description"])
            self.assertIn("Do not publish", payload["description"])
            self.assertIn("NBA data analysis expert", payload["description"])
            self.assertIn("Funba Hero And Notable Metrics", payload["description"])
            self.assertIn("Do not load normal game-analysis phase docs", payload["description"])
            self.assertIn("The teams combined for 25 steals", payload["description"])
            self.assertIn("Golden State had its most efficient transition game", payload["description"])
            self.assertIn("Curry's highest pull-up three total", payload["description"])
            self.assertIn('"reply_persona": "NBA data analysis expert"', payload["description"])
            self.assertIn('"game_metric_contexts": [', payload["description"])
            self.assertIn("https://x.com/nba_analyst/status/1900000000000000001", payload["description"])
            self.assertIn("https://funba.test/games/warriors-lakers-2026-05-01", payload["description"])

    def test_twitter_engagement_issue_description_names_manual_boundaries(self):
        with self.SessionLocal() as session:
            self._seed_game(session)
            result = discover_twitter_engagement_candidates(
                session,
                search_payload=self._payload(),
                daily_limit=1,
                min_score=0,
                base_url="https://funba.test",
                paperclip=False,
                now_utc=self.now,
            )
            post = session.get(SocialPost, result["created_reply_post_ids"][0])
            variant = session.query(SocialPostVariant).filter(SocialPostVariant.post_id == post.id).one()
            delivery = session.query(SocialPostDelivery).filter(SocialPostDelivery.variant_id == variant.id).one()
            candidate = parse_recent_search_payload(self._payload())[0]

            description = build_twitter_engagement_issue_description(
                post=post,
                variant=variant,
                delivery=delivery,
                candidate=candidate,
                conversation=session.query(TwitterEngagementConversation).one(),
                message=session.query(TwitterEngagementMessage).one(),
                conversation_messages=session.query(TwitterEngagementMessage).all(),
                contexts=[],
                score=10.0,
                reason="test",
                query="NBA",
            )

            self.assertIn("Paperclip-written X reply", description)
            self.assertIn("NBA data analysis expert", description)
            self.assertIn("Conversation History", description)
            self.assertIn("Do not enable the `twitter_reply` delivery", description)
            self.assertIn(f"/api/admin/content/{post.id}/variants/{variant.id}/update", description)

    def test_discovery_skips_existing_candidate_post(self):
        with self.SessionLocal() as session:
            self._seed_game(session)
            first = discover_twitter_engagement_candidates(
                session,
                search_payload=self._payload(),
                daily_limit=2,
                min_score=0,
                paperclip=False,
                now_utc=self.now,
            )
            session.commit()

            second = discover_twitter_engagement_candidates(
                session,
                search_payload=self._payload(),
                daily_limit=2,
                min_score=0,
                paperclip=False,
                now_utc=self.now,
            )

            self.assertEqual(len(first["created_reply_post_ids"]), 1)
            self.assertEqual(second["created_reply_post_ids"], [])
            self.assertEqual(len(second["skipped_existing_message_ids"]), 1)


if __name__ == "__main__":
    unittest.main()
