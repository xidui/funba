import sys
import types
import unittest
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
        "Award", "Feedback", "Game", "GamePlayByPlay", "MagicToken", "MetricComputeRun",
        "MetricDefinition", "MetricPerfLog", "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "PlayerSalary", "ShotRecord", "Team", "TeamGameStats", "SocialPost", "SocialPostImage",
        "SocialPostVariant", "SocialPostDelivery", "GameLineScore", "Setting",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.User = fake_user_cls
    fake_models.engine = fake_engine
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

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
            ("雷霆全场55投102中，命中率53.9%。塔图姆25分18篮板11助攻的准三双。",)
        ]

        session = _session_ctx(MagicMock())
        session.query.side_effect = [post_query, variant_query]

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
            file_path="/tmp/test.png",
            spec=None,
        )

        rendered = self.web_app._social_post_image_view(1, image)

        self.assertEqual(rendered["error_title"], "Auto-review: Wrong player / team")
        self.assertIn("requested player", rendered["error_summary"])


if __name__ == "__main__":
    unittest.main()
