import sys
import types
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy.sql import column
from werkzeug.exceptions import NotFound

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_flask_limiter = types.ModuleType("flask_limiter")

    class _FakeLimiter:
        def __init__(self, *args, **kwargs):
            pass

        def limit(self, *args, **kwargs):
            def _decorator(fn):
                return fn

            return _decorator

    fake_flask_limiter.Limiter = _FakeLimiter
    sys.modules["flask_limiter"] = fake_flask_limiter

    fake_flask_limiter_util = types.ModuleType("flask_limiter.util")
    fake_flask_limiter_util.get_remote_address = MagicMock(return_value="127.0.0.1")
    sys.modules["flask_limiter.util"] = fake_flask_limiter_util

    fake_authlib = types.ModuleType("authlib")
    fake_authlib_integrations = types.ModuleType("authlib.integrations")
    fake_authlib_flask_client = types.ModuleType("authlib.integrations.flask_client")

    class _FakeOAuth:
        def __init__(self, *args, **kwargs):
            pass

        def register(self, *args, **kwargs):
            return None

    fake_authlib_flask_client.OAuth = _FakeOAuth
    sys.modules["authlib"] = fake_authlib
    sys.modules["authlib.integrations"] = fake_authlib_integrations
    sys.modules["authlib.integrations.flask_client"] = fake_authlib_flask_client

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award", "Feedback", "Game", "GamePlayByPlay", "MagicToken", "MetricComputeRun",
        "MetricDefinition", "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "ShotRecord", "Team", "TeamGameStats", "GameLineScore",
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


class TestDraftPage(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()
        self.web_app.Player = SimpleNamespace(
            player_id=column("player_id"),
            draft_year=column("draft_year"),
            draft_round=column("draft_round"),
            draft_number=column("draft_number"),
            full_name=column("full_name"),
            position=column("position"),
        )

    def test_draft_page_passes_players_and_navigation_context(self):
        min_max_query = MagicMock()
        min_max_query.filter.return_value.one.return_value = (2008, 2010)

        players_query = MagicMock()
        players_query.filter.return_value.order_by.return_value.all.return_value = [
            SimpleNamespace(player_id="1", full_name="Player One", draft_round=1, draft_number=7, position="G"),
            SimpleNamespace(player_id="2", full_name="Player Two", draft_round=1, draft_number=12, position=None),
        ]

        session = _session_ctx(MagicMock())
        session.query.side_effect = [min_max_query, players_query]

        with self.web_app.app.test_request_context("/draft/2009"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                response = self.web_app.draft_page(2009)

        self.assertEqual(response, "rendered")
        _, kwargs = render_template.call_args
        self.assertEqual(kwargs["year"], 2009)
        self.assertEqual(kwargs["draft_count"], 2)
        self.assertEqual(kwargs["min_year"], 2008)
        self.assertEqual(kwargs["max_year"], 2010)
        self.assertNotIn("prev_year", kwargs)
        self.assertNotIn("next_year", kwargs)
        self.assertNotIn("show_position_column", kwargs)
        self.assertEqual([player.player_id for player in kwargs["draft_players"]], ["1", "2"])

    def test_draft_page_handles_empty_year_without_position_column(self):
        min_max_query = MagicMock()
        min_max_query.filter.return_value.one.return_value = (2003, 2003)

        players_query = MagicMock()
        players_query.filter.return_value.order_by.return_value.all.return_value = []

        session = _session_ctx(MagicMock())
        session.query.side_effect = [min_max_query, players_query]

        with self.web_app.app.test_request_context("/draft/2003"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                response = self.web_app.draft_page(2003)

        self.assertEqual(response, "rendered")
        _, kwargs = render_template.call_args
        self.assertEqual(kwargs["draft_players"], [])
        self.assertEqual(kwargs["draft_count"], 0)
        self.assertEqual(kwargs["min_year"], 2003)
        self.assertEqual(kwargs["max_year"], 2003)
        self.assertNotIn("prev_year", kwargs)
        self.assertNotIn("next_year", kwargs)
        self.assertNotIn("show_position_column", kwargs)

    def test_draft_page_rejects_years_outside_valid_range(self):
        with self.web_app.app.test_request_context("/draft/1946"):
            with self.assertRaises(NotFound):
                self.web_app.draft_page(1946)

        with self.web_app.app.test_request_context(f"/draft/{date.today().year + 1}"):
            with self.assertRaises(NotFound):
                self.web_app.draft_page(date.today().year + 1)

    def test_player_template_contains_draft_link_markup(self):
        player_template = (REPO_ROOT / "web" / "templates" / "player.html").read_text()
        self.assertIn('href="/draft/{{ player.draft_year }}"', player_template)
        self.assertIn('class="bio-value draft-link"', player_template)


if __name__ == "__main__":
    unittest.main()
