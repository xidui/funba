import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy.sql import column

from tests.db_model_stubs import install_fake_db_module

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    install_fake_db_module(
        REPO_ROOT,
        user_cls=fake_user_cls,
        engine=fake_engine,
    )

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


class TestGamePageFallback(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()
        self.web_app.Game = SimpleNamespace(game_id=column("game_id"))

    def test_game_page_renders_live_game_from_scoreboard_when_db_row_is_missing(self):
        session = _session_ctx(MagicMock())
        game_query = MagicMock()
        game_query.filter.return_value.first.return_value = None
        session.query.return_value = game_query

        team_map = {
            "1610612745": SimpleNamespace(team_id="1610612745", abbr="HOU", full_name="Houston Rockets"),
            "1610612750": SimpleNamespace(team_id="1610612750", abbr="MIN", full_name="Minnesota Timberwolves"),
        }
        live_summary = {
            "game_id": "0022501178",
            "season": "22025",
            "game_date": "2026-04-10",
            "status": "live",
            "summary": "Q2 4:26",
            "home_score": 61,
            "road_score": 61,
            "home_team_id": "1610612745",
            "road_team_id": "1610612750",
        }
        live_payload = {
            "summary": live_summary,
            "team_stats": [],
            "players_by_team": {},
            "ordered_team_ids": ["1610612750", "1610612745"],
            "quarter_scores": [],
            "pbp_rows": [],
        }

        with self.web_app.app.test_request_context("/games/0022501178"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_team_map", return_value=team_map), \
                 patch.object(self.web_app, "game_analysis_issue_history", return_value=[]), \
                 patch("web.detail_routes.fetch_live_game_detail", return_value=live_payload), \
                 patch("web.detail_routes.fetch_live_scoreboard_map", return_value={}), \
                 patch.object(self.web_app, "render_template", side_effect=lambda template, **kwargs: {"template": template, **kwargs}):
                response = self.web_app.game_page("0022501178")

        self.assertEqual(response["template"], "game.html")
        self.assertEqual(response["game_status"], "live")
        self.assertEqual(response["game"].game_id, "0022501178")
        self.assertEqual(response["game"].season, "22025")
        self.assertEqual(response["live_summary"]["summary"], "Q2 4:26")
        self.assertEqual(response["live_refresh_interval_ms"], 15000)

    def test_game_page_renders_upcoming_game_from_scoreboard_when_db_row_is_missing(self):
        session = _session_ctx(MagicMock())
        game_query = MagicMock()
        game_query.filter.return_value.first.return_value = None
        session.query.return_value = game_query

        team_map = {
            "1610612756": SimpleNamespace(team_id="1610612756", abbr="PHX", full_name="Phoenix Suns"),
            "1610612758": SimpleNamespace(team_id="1610612758", abbr="SAC", full_name="Sacramento Kings"),
        }
        live_summary = {
            "game_id": "0022501185",
            "season": "22025",
            "game_date": "2026-04-10",
            "status": "upcoming",
            "summary": "10:00 PM ET",
            "home_score": 0,
            "road_score": 0,
            "home_team_id": "1610612758",
            "road_team_id": "1610612756",
        }

        with self.web_app.app.test_request_context("/games/0022501185"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_team_map", return_value=team_map), \
                 patch.object(self.web_app, "game_analysis_issue_history", return_value=[]), \
                 patch("web.detail_routes.fetch_live_game_detail", return_value=None), \
                 patch("web.detail_routes.fetch_live_scoreboard_map", return_value={"0022501185": live_summary}), \
                 patch.object(self.web_app, "render_template", side_effect=lambda template, **kwargs: {"template": template, **kwargs}):
                response = self.web_app.game_page("0022501185")

        self.assertEqual(response["template"], "game.html")
        self.assertEqual(response["game_status"], "upcoming")
        self.assertEqual(response["game"].game_id, "0022501185")
        self.assertEqual(response["game"].season, "22025")
        self.assertIsNone(response["live_refresh_interval_ms"])

    def test_game_page_renders_degraded_live_view_when_live_detail_api_is_unavailable(self):
        session = _session_ctx(MagicMock())
        game_query = MagicMock()
        game_query.filter.return_value.first.return_value = None
        session.query.return_value = game_query

        team_map = {
            "1610612745": SimpleNamespace(team_id="1610612745", abbr="HOU", full_name="Houston Rockets"),
            "1610612750": SimpleNamespace(team_id="1610612750", abbr="MIN", full_name="Minnesota Timberwolves"),
        }
        live_summary = {
            "game_id": "0022501178",
            "season": "22025",
            "game_date": "2026-04-10",
            "status": "live",
            "summary": "Q2 4:26",
            "home_score": 61,
            "road_score": 61,
            "home_team_id": "1610612745",
            "road_team_id": "1610612750",
        }

        with self.web_app.app.test_request_context("/games/0022501178"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_team_map", return_value=team_map), \
                 patch.object(self.web_app, "game_analysis_issue_history", return_value=[]), \
                 patch("web.detail_routes.fetch_live_game_detail", return_value=None), \
                 patch("web.detail_routes.fetch_live_scoreboard_map", return_value={"0022501178": live_summary}), \
                 patch.object(self.web_app, "render_template", side_effect=lambda template, **kwargs: {"template": template, **kwargs}):
                response = self.web_app.game_page("0022501178")

        self.assertEqual(response["template"], "game.html")
        self.assertEqual(response["game_status"], "live")
        self.assertEqual(response["game"].game_id, "0022501178")
        self.assertEqual(response["live_summary"]["summary"], "Q2 4:26")
        self.assertEqual(response["live_refresh_interval_ms"], 15000)
        self.assertEqual(response["team_stats"], [])
        self.assertEqual(response["players_by_team"], {})
        self.assertEqual(response["pbp_rows"], [])
