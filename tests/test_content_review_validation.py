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


if __name__ == "__main__":
    unittest.main()
