import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy.sql import column

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    original_game_analysis = sys.modules.get("content_pipeline.game_analysis_issues")
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award", "Feedback", "Game", "GameContentAnalysisIssuePost", "GameLineScore", "GamePlayByPlay",
        "MagicToken", "MetricComputeRun", "MetricDefinition", "MetricPerfLog", "MetricResult",
        "MetricRunLog", "PageView", "Player", "PlayerGameStats", "PlayerSalary", "ShotRecord",
        "SocialPost", "SocialPostDelivery", "SocialPostImage", "SocialPostVariant", "Team",
        "TeamGameStats",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.User = fake_user_cls
    fake_models.engine = fake_engine
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    fake_game_analysis = types.ModuleType("content_pipeline.game_analysis_issues")
    for name in (
        "ensure_game_content_analysis_issue_for_game",
        "ensure_game_content_analysis_issues",
        "game_analysis_readiness_detail",
        "game_analysis_issue_history",
        "link_post_to_game_analysis_issue",
        "resolve_game_analysis_issue_record",
    ):
        setattr(fake_game_analysis, name, MagicMock())
    sys.modules["content_pipeline.game_analysis_issues"] = fake_game_analysis

    for key in list(sys.modules):
        if key == "web.app" or key.startswith("web.app."):
            del sys.modules[key]

    import web.app as web_app

    if original_game_analysis is not None:
        sys.modules["content_pipeline.game_analysis_issues"] = original_game_analysis
    else:
        sys.modules.pop("content_pipeline.game_analysis_issues", None)

    web_app.app.config["TESTING"] = True
    web_app.PageView = SimpleNamespace(
        id=column("id"),
        path=column("path"),
        referrer=column("referrer"),
        visitor_id=column("visitor_id"),
        created_at=column("created_at"),
    )
    return web_app


def _session_ctx(session):
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


class TestAdminTopPages(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()

    def test_extract_referrer_source_handles_direct_and_domains(self):
        self.assertEqual(self.web_app._extract_referrer_source(None), "Direct")
        self.assertEqual(self.web_app._extract_referrer_source(""), "Direct")
        self.assertEqual(
            self.web_app._extract_referrer_source("https://www.google.com/search?q=nba"),
            "google.com",
        )
        self.assertEqual(
            self.web_app._extract_referrer_source("hupu.com/thread/123"),
            "hupu.com",
        )

    def test_admin_fragment_top_pages_renders_panel_data(self):
        page_rows = [
            SimpleNamespace(path="/players/203999", views=12, unique_visitors=5),
            SimpleNamespace(path="/games/0022500001", views=8, unique_visitors=4),
        ]
        referrer_rows = [
            SimpleNamespace(referrer=None, views=3),
            SimpleNamespace(referrer="https://www.google.com/search?q=nba", views=4),
            SimpleNamespace(referrer="https://google.com/other", views=2),
            SimpleNamespace(referrer="hupu.com/thread/1", views=6),
        ]

        page_query = MagicMock()
        page_query.filter.return_value = page_query
        page_query.group_by.return_value = page_query
        page_query.order_by.return_value = page_query
        page_query.limit.return_value.all.return_value = page_rows

        referrer_query = MagicMock()
        referrer_query.filter.return_value = referrer_query
        referrer_query.group_by.return_value.all.return_value = referrer_rows

        session = _session_ctx(MagicMock())
        session.query.side_effect = [page_query, referrer_query]

        with self.web_app.app.test_request_context("/admin/fragment/top-pages?window=7d"):
            with patch.object(self.web_app, "_require_admin_page", return_value=None), \
                 patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                response = self.web_app.admin_fragment("top-pages")

        self.assertEqual(response, "rendered")
        render_template.assert_called_once()
        template_name = render_template.call_args.args[0]
        self.assertEqual(template_name, "_admin_top_pages.html")
        kwargs = render_template.call_args.kwargs
        self.assertEqual(kwargs["selected_window"], "7d")
        self.assertEqual(kwargs["top_pages"][0]["path"], "/players/203999")
        self.assertEqual(kwargs["top_pages"][0]["views"], 12)
        self.assertEqual(kwargs["top_pages"][0]["unique_visitors"], 5)
        self.assertEqual(
            kwargs["top_referrers"],
            [
                {"rank": 1, "source": "google.com", "views": 6},
                {"rank": 2, "source": "hupu.com", "views": 6},
                {"rank": 3, "source": "Direct", "views": 3},
            ],
        )

    def test_invalid_window_defaults_to_1d(self):
        page_query = MagicMock()
        page_query.filter.return_value = page_query
        page_query.group_by.return_value = page_query
        page_query.order_by.return_value = page_query
        page_query.limit.return_value.all.return_value = []

        referrer_query = MagicMock()
        referrer_query.filter.return_value = referrer_query
        referrer_query.group_by.return_value.all.return_value = []

        session = _session_ctx(MagicMock())
        session.query.side_effect = [page_query, referrer_query]

        with self.web_app.app.test_request_context("/admin/fragment/top-pages?window=30d"):
            with patch.object(self.web_app, "_require_admin_page", return_value=None), \
                 patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                self.web_app.admin_fragment("top-pages")

        kwargs = render_template.call_args.kwargs
        self.assertEqual(kwargs["selected_window"], "1d")


if __name__ == "__main__":
    unittest.main()
