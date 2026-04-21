import sys
import tempfile
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests.db_model_stubs import install_fake_db_module


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    original_game_analysis = sys.modules.get("content_pipeline.game_analysis_issues")
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    install_fake_db_module(
        REPO_ROOT,
        user_cls=fake_user_cls,
        engine=fake_engine,
        extra_model_names=("Setting",),
    )

    for key in list(sys.modules):
        if key == "web.app" or key.startswith("web.app."):
            del sys.modules[key]

    fake_game_analysis = types.ModuleType("content_pipeline.game_analysis_issues")
    fake_game_analysis.ensure_game_content_analysis_issue_for_game = MagicMock()
    fake_game_analysis.ensure_game_content_analysis_issues = MagicMock()
    fake_game_analysis.game_analysis_readiness_detail = MagicMock(return_value=None)
    fake_game_analysis.game_analysis_issue_history = MagicMock(return_value=[])
    fake_game_analysis.link_post_to_game_analysis_issue = MagicMock()
    fake_game_analysis.resolve_game_analysis_issue_record = MagicMock(return_value=None)
    sys.modules["content_pipeline.game_analysis_issues"] = fake_game_analysis

    import web.app as web_app

    if original_game_analysis is not None:
        sys.modules["content_pipeline.game_analysis_issues"] = original_game_analysis
    else:
        sys.modules.pop("content_pipeline.game_analysis_issues", None)

    web_app.app.config["TESTING"] = True
    return web_app


def _session_ctx(session):
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


class TestContentReviewValidation(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()

    def test_ai_review_to_in_review_blocks_bad_shot_line_and_quasi_triple_double(self):
        post = SimpleNamespace(id=1, status="ai_review", topic="Bad copy", priority=10)
        comments = []

        post_query = MagicMock()
        post_query.filter.return_value.first.return_value = post

        variant_query = MagicMock()
        variant_query.filter.return_value.all.return_value = [
            (11, "智趣NBA: 合法标题", "雷霆全场55投102中，命中率53.9%。塔图姆25分18篮板11助攻的准三双。")
        ]

        delivery_query = MagicMock()
        delivery_query.filter.return_value.all.return_value = []

        session = _session_ctx(MagicMock())
        session.query.side_effect = [post_query, variant_query, delivery_query]

        with self.web_app.app.test_client() as client:
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_require_admin_json", return_value=None), \
                 patch.object(self.web_app, "_social_post_comments", return_value=comments):
                resp = client.post(
                    "/api/admin/content/1/update",
                    json={"status": "in_review"},
                    headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                    environ_base={"REMOTE_ADDR": "127.0.0.1"},
                )

        self.assertEqual(resp.status_code, 400)
        payload = resp.get_json()
        self.assertEqual(payload["error"], "ai_review_validation_failed")
        joined = "\n".join(payload["details"])
        self.assertIn("Shot line looks inverted or impossible", joined)
        self.assertIn("准三双", joined)

    def test_ai_review_to_in_review_blocks_overlong_hupu_title(self):
        post = SimpleNamespace(id=1, status="ai_review", topic="Long Hupu title", priority=10)
        comments = []

        post_query = MagicMock()
        post_query.filter.return_value.first.return_value = post

        variant_query = MagicMock()
        variant_query.filter.return_value.all.return_value = [
            (21, "智趣NBA: 魔术附加赛首战赢31分，贝恩+30领跑本届单场正负值榜，黄蜂首节就被打穿", "正文正常。")
        ]

        delivery_query = MagicMock()
        delivery_query.filter.return_value.all.return_value = [(21,)]

        session = _session_ctx(MagicMock())
        session.query.side_effect = [post_query, variant_query, delivery_query]

        with self.web_app.app.test_client() as client:
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_require_admin_json", return_value=None), \
                 patch.object(self.web_app, "_social_post_comments", return_value=comments):
                resp = client.post(
                    "/api/admin/content/1/update",
                    json={"status": "in_review"},
                    headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                    environ_base={"REMOTE_ADDR": "127.0.0.1"},
                )

        self.assertEqual(resp.status_code, 400)
        payload = resp.get_json()
        self.assertEqual(payload["error"], "ai_review_validation_failed")
        joined = "\n".join(payload["details"])
        self.assertIn("Hupu title length must be 4-40 characters", joined)
        self.assertIn("current: 43", joined)

    def test_delivery_status_rejects_hupu_compose_url_false_positive(self):
        delivery = SimpleNamespace(
            id=273,
            platform="hupu",
            forum="湿乎乎的话题",
            is_enabled=True,
            status="publishing",
            content_final=None,
            published_url=None,
            published_at=None,
            error_message=None,
            updated_at=None,
        )

        delivery_query = MagicMock()
        delivery_query.filter.return_value.first.return_value = delivery

        session = _session_ctx(MagicMock())
        session.query.return_value = delivery_query

        with self.web_app.app.test_client() as client:
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_require_admin_json", return_value=None):
                resp = client.post(
                    "/api/content/deliveries/273/status",
                    json={
                        "status": "published",
                        "published_url": "https://bbs.hupu.com/newpost/179",
                    },
                    headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                    environ_base={"REMOTE_ADDR": "127.0.0.1"},
                )

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(delivery.status, "failed")
        self.assertIsNone(delivery.published_url)
        self.assertIn("Invalid Hupu published_url reported", delivery.error_message)
        self.assertIsNone(delivery.published_at)
        session.commit.assert_called_once()

    def test_social_post_delivery_view_masks_invalid_hupu_published_url(self):
        delivery = SimpleNamespace(
            id=274,
            platform="hupu",
            forum="火箭专区",
            is_enabled=True,
            status="published",
            content_final=None,
            published_url="https://bbs.hupu.com/newpost/179",
            published_at=None,
            error_message=None,
        )

        rendered = self.web_app._social_post_delivery_view(delivery)

        self.assertEqual(rendered["status"], "failed")
        self.assertIsNone(rendered["published_url"])
        self.assertIn("Invalid Hupu published_url recorded", rendered["error_message"])

    def test_add_reddit_destination_normalizes_forum_and_updates_audience_hint(self):
        variant = SimpleNamespace(
            id=11,
            post_id=1,
            audience_hint="general nba",
            updated_at=None,
        )

        variant_query = MagicMock()
        variant_query.filter.return_value.first.return_value = variant

        session = _session_ctx(MagicMock())
        session.query.return_value = variant_query
        session.add.side_effect = lambda obj: setattr(obj, "id", 501)

        with self.web_app.app.test_client() as client:
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_require_admin_json", return_value=None), \
                 patch.object(self.web_app, "SocialPostDelivery", side_effect=lambda **kwargs: SimpleNamespace(**kwargs)), \
                 patch.object(self.web_app, "_ensure_paperclip_issue_for_post"):
                resp = client.post(
                    "/api/admin/content/1/variants/11/destinations",
                    json={"platform": "reddit", "forum": "r/nba"},
                    headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                    environ_base={"REMOTE_ADDR": "127.0.0.1"},
                )

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        created_delivery = session.add.call_args[0][0]
        self.assertEqual(created_delivery.platform, "reddit")
        self.assertEqual(created_delivery.forum, "nba")
        self.assertIn("write in English for r/nba readers", variant.audience_hint)
        session.commit.assert_called_once()

    def test_social_post_image_error_view_classifies_auto_review_reason(self):
        rendered = self.web_app._social_post_image_error_view(
            "Auto-review rejected (gpt-5.4-mini): Contains visible agency branding/overlay and is a graphic draft card, not a clean editorial game photo.",
            is_enabled=False,
        )

        self.assertEqual(rendered["error_title"], "Auto-review: Watermark / branding")
        self.assertIn("branding", rendered["error_summary"])

    def test_social_post_image_error_view_classifies_generation_parameter_error(self):
        rendered = self.web_app._social_post_image_error_view(
            "Error code: 400 - {'error': {'message': \"Unknown parameter: 'input_fidelity'.\", 'type': 'invalid_request_error'}}",
            is_enabled=False,
        )

        self.assertEqual(rendered["error_title"], "AI generation failed")
        self.assertIn("input_fidelity", rendered["error_summary"])

    def test_social_post_image_view_includes_error_title_and_summary(self):
        image = SimpleNamespace(
            id=99,
            slot="img1",
            image_type="web_search",
            note="测试图",
            is_enabled=False,
            error_message="Auto-review rejected (gpt-5.4-mini): Wrong player/context for Neemias Queta.",
            review_decision="disable",
            review_reason="和段落不匹配",
            review_source="content_reviewer_agent",
            reviewed_at=None,
            file_path="/tmp/test.png",
            spec=None,
        )

        rendered = self.web_app._social_post_image_view(1, image)

        self.assertEqual(rendered["error_title"], "Auto-review: Wrong player / team")
        self.assertIn("requested player", rendered["error_summary"])
        self.assertEqual(rendered["review_decision"], "disable")
        self.assertEqual(rendered["review_reason"], "和段落不匹配")
        self.assertEqual(rendered["review_source"], "content_reviewer_agent")

    def test_toggle_image_can_store_manual_review_reason(self):
        image = SimpleNamespace(
            id=9,
            post_id=1,
            is_enabled=True,
            review_decision=None,
            review_reason=None,
            review_source=None,
            reviewed_at=None,
        )

        image_query = MagicMock()
        image_query.filter.return_value.first.return_value = image

        session = _session_ctx(MagicMock())
        session.query.return_value = image_query

        with self.web_app.app.test_client() as client:
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_require_admin_json", return_value=None):
                resp = client.post(
                    "/api/admin/content/1/images/9/toggle",
                    json={
                        "is_enabled": False,
                        "reason": "和正文论点不匹配",
                        "review_source": "human_reviewer",
                    },
                    headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                    environ_base={"REMOTE_ADDR": "127.0.0.1"},
                )

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertFalse(image.is_enabled)
        self.assertEqual(image.review_decision, "disable")
        self.assertEqual(image.review_reason, "和正文论点不匹配")
        self.assertEqual(image.review_source, "human_reviewer")
        self.assertIsNotNone(image.reviewed_at)

    def test_image_review_apply_updates_images_and_adds_summary_comment(self):
        post = SimpleNamespace(id=1, admin_comments=None, updated_at=None)
        image_a = SimpleNamespace(
            id=11,
            post_id=1,
            is_enabled=True,
            review_decision=None,
            review_reason=None,
            review_source=None,
            reviewed_at=None,
        )
        image_b = SimpleNamespace(
            id=12,
            post_id=1,
            is_enabled=True,
            review_decision=None,
            review_reason=None,
            review_source=None,
            reviewed_at=None,
        )

        post_query = MagicMock()
        post_query.filter.return_value.first.return_value = post
        image_query_a = MagicMock()
        image_query_a.filter.return_value.first.return_value = image_a
        image_query_b = MagicMock()
        image_query_b.filter.return_value.first.return_value = image_b

        session = _session_ctx(MagicMock())
        session.query.side_effect = [post_query, image_query_a, image_query_b]

        with self.web_app.app.test_client() as client:
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_require_admin_json", return_value=None):
                resp = client.post(
                    "/api/admin/content/1/image-review/apply",
                    json={
                        "review_source": "content_reviewer_agent",
                        "summary": "禁用一张错误页截图，保留一张排行榜截图。",
                        "image_decisions": [
                            {"image_id": 11, "action": "disable", "reason": "500错误页截图"},
                            {"image_id": 12, "action": "keep", "reason": "和正文段落直接对应"},
                        ],
                    },
                    headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                    environ_base={"REMOTE_ADDR": "127.0.0.1"},
                )

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertFalse(image_a.is_enabled)
        self.assertEqual(image_a.review_decision, "disable")
        self.assertEqual(image_a.review_reason, "500错误页截图")
        self.assertEqual(image_a.review_source, "content_reviewer_agent")
        self.assertTrue(image_b.is_enabled)
        self.assertEqual(image_b.review_decision, "keep")
        self.assertEqual(image_b.review_reason, "和正文段落直接对应")
        self.assertEqual(image_b.review_source, "content_reviewer_agent")
        self.assertIn("Image review (content_reviewer_agent)", post.admin_comments)
        session.commit.assert_called_once()

    def test_validate_prepared_image_specs_requires_file_path(self):
        with self.assertRaisesRegex(ValueError, "file_path required"):
            self.web_app._validate_prepared_image_specs(
                [{"slot": "img1", "type": "web_search", "query": "x", "note": "图"}]
            )

    def test_validate_prepared_image_specs_accepts_existing_file(self):
        with tempfile.TemporaryDirectory(prefix="funba_img_specs_") as tmpdir:
            source = Path(tmpdir) / "img1.png"
            source.write_bytes(b"png")

            prepared = self.web_app._validate_prepared_image_specs(
                [
                    {
                        "slot": "img1",
                        "type": "screenshot",
                        "file_path": str(source),
                        "target": "https://funba.app/players/1642843",
                        "note": "弗拉格球员页截图",
                    }
                ]
            )

        self.assertEqual(len(prepared), 1)
        self.assertEqual(prepared[0]["slot"], "img1")
        self.assertEqual(prepared[0]["image_type"], "screenshot")
        self.assertEqual(prepared[0]["source_path"], str(source))

    def test_admin_content_add_image_stores_prepared_asset(self):
        post = SimpleNamespace(id=1)
        existing_slot = None

        post_query = MagicMock()
        post_query.filter.return_value.first.return_value = post
        slot_query = MagicMock()
        slot_query.filter.return_value.first.return_value = existing_slot

        created_images = []

        class _FakeImage:
            _next_id = 100
            post_id = MagicMock()
            slot = MagicMock()
            id = MagicMock()

            def __init__(self, **kwargs):
                self.id = _FakeImage._next_id
                _FakeImage._next_id += 1
                for key, value in kwargs.items():
                    setattr(self, key, value)

        session = _session_ctx(MagicMock())
        session.query.side_effect = [post_query, slot_query]
        session.add.side_effect = lambda img: created_images.append(img)

        with tempfile.TemporaryDirectory(prefix="funba_add_img_") as tmpdir:
            source = Path(tmpdir) / "img1.png"
            source.write_bytes(b"png")
            with self.web_app.app.test_client() as client:
                with patch.object(self.web_app, "SessionLocal", return_value=session), \
                     patch.object(self.web_app, "_require_admin_json", return_value=None), \
                     patch.object(self.web_app, "_ensure_paperclip_issue_for_post"), \
                     patch.object(self.web_app, "SocialPostImage", _FakeImage), \
                     patch.object(self.web_app, "store_prepared_image", return_value="/tmp/stored_img1.png"):
                    resp = client.post(
                        "/api/admin/content/1/images",
                        json={
                            "slot": "img1",
                            "type": "screenshot",
                            "file_path": str(source),
                            "target": "https://funba.app/players/1642843",
                            "note": "弗拉格球员页截图",
                        },
                        headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                        environ_base={"REMOTE_ADDR": "127.0.0.1"},
                    )

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(len(created_images), 1)
        self.assertEqual(created_images[0].slot, "img1")
        self.assertEqual(created_images[0].file_path, "/tmp/stored_img1.png")
        self.assertTrue(created_images[0].is_enabled)

    def test_admin_content_replace_image_swaps_prepared_asset(self):
        image = SimpleNamespace(
            id=11,
            post_id=1,
            slot="img1",
            image_type="screenshot",
            note="旧图",
            spec='{"target":"https://funba.app/old"}',
            file_path="/tmp/old_img1.png",
            is_enabled=True,
            error_message="old error",
            review_decision="disable",
            review_reason="旧原因",
            review_source="content_reviewer_agent",
            reviewed_at="old",
        )

        image_query = MagicMock()
        image_query.filter.return_value.first.return_value = image

        session = _session_ctx(MagicMock())
        session.query.return_value = image_query

        with tempfile.TemporaryDirectory(prefix="funba_replace_img_") as tmpdir:
            source = Path(tmpdir) / "img1_new.png"
            source.write_bytes(b"png")
            with self.web_app.app.test_client() as client:
                with patch.object(self.web_app, "SessionLocal", return_value=session), \
                     patch.object(self.web_app, "_require_admin_json", return_value=None), \
                     patch.object(self.web_app, "_ensure_paperclip_issue_for_post"), \
                     patch.object(self.web_app, "store_prepared_image", return_value="/tmp/stored_img1_new.png"), \
                     patch.object(self.web_app, "_remove_managed_post_image_file"):
                    resp = client.post(
                        "/api/admin/content/1/images/11/replace",
                        json={
                            "slot": "img1",
                            "type": "screenshot",
                            "file_path": str(source),
                            "target": "https://funba.app/players/1642843",
                            "note": "新图",
                            "is_enabled": False,
                        },
                        headers={"User-Agent": "Mozilla/5.0 test browser long enough"},
                        environ_base={"REMOTE_ADDR": "127.0.0.1"},
                    )

        self.assertEqual(resp.status_code, 200)
        payload = resp.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(image.file_path, "/tmp/stored_img1_new.png")
        self.assertEqual(image.note, "新图")
        self.assertFalse(image.is_enabled)
        self.assertIsNone(image.error_message)
        self.assertIsNone(image.review_decision)
        self.assertIsNone(image.review_reason)
        self.assertIsNone(image.review_source)
        self.assertIsNone(image.reviewed_at)


if __name__ == "__main__":
    unittest.main()
