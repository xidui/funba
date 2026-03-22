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
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award", "Feedback", "Game", "GamePlayByPlay", "MagicToken", "MetricJobClaim",
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


class TestGamesList(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()
        self.web_app.Game = SimpleNamespace(
            season=column("season"),
            game_date=column("game_date"),
            game_id=column("game_id"),
            home_team_id=column("home_team_id"),
            road_team_id=column("road_team_id"),
        )
        self.web_app.Team = SimpleNamespace(
            is_legacy=column("is_legacy"),
            full_name=column("full_name"),
            abbr=column("abbr"),
            team_id=column("team_id"),
        )

    def test_games_list_filters_by_team_and_passes_filter_context(self):
        selected_team = SimpleNamespace(team_id="1610612738", abbr="BOS", full_name="Boston Celtics")
        other_team = SimpleNamespace(team_id="1610612747", abbr="LAL", full_name="Los Angeles Lakers")
        game = SimpleNamespace(
            game_id="002",
            season="22025",
            game_date="2026-03-20",
            home_team_id="1610612738",
            road_team_id="1610612747",
            home_team_score=110,
            road_team_score=101,
            wining_team_id="1610612738",
        )

        season_rows = [SimpleNamespace(season="22024"), SimpleNamespace(season="22025")]

        season_query = MagicMock()
        season_query.filter.return_value.all.return_value = season_rows

        teams_query = MagicMock()
        teams_query.filter.return_value.order_by.return_value.all.return_value = [selected_team, other_team]

        games_query = MagicMock()
        games_query.filter.return_value = games_query
        games_query.order_by.return_value = games_query
        games_query.count.return_value = 1
        games_query.offset.return_value.limit.return_value.all.return_value = [game]

        team_map_query = MagicMock()
        team_map_query.all.return_value = [selected_team, other_team]

        session = _session_ctx(MagicMock())
        session.query.side_effect = [season_query, teams_query, games_query, team_map_query]

        with self.web_app.app.test_request_context("/games?season=22025&team=1610612738&page=2"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                response = self.web_app.games_list()

        self.assertEqual(response, "rendered")
        _, kwargs = render_template.call_args
        self.assertEqual(kwargs["selected_season"], "22025")
        self.assertEqual(kwargs["selected_team"], "1610612738")
        self.assertIs(kwargs["selected_team_obj"], selected_team)
        self.assertEqual(kwargs["all_teams"], [selected_team, other_team])
        self.assertEqual(kwargs["games"], [game])
        self.assertEqual(kwargs["page"], 1)
        self.assertEqual(kwargs["total_pages"], 1)

        teams_filter = teams_query.filter.call_args.args[0]
        self.assertIn("is_legacy", str(teams_filter))
        self.assertIn("false", str(teams_filter).lower())

        self.assertEqual(games_query.filter.call_count, 3)
        season_filter, team_filter = [call.args[0] for call in games_query.filter.call_args_list[1:]]
        self.assertIn("season", str(season_filter))
        self.assertEqual(season_filter.right.value, "22025")
        self.assertIn("home_team_id", str(team_filter))
        self.assertIn("road_team_id", str(team_filter))

    def test_games_list_falls_back_to_lookup_for_selected_team_not_in_dropdown(self):
        legacy_team = SimpleNamespace(team_id="1610612737", abbr="ATL", full_name="Atlanta Hawks")

        season_query = MagicMock()
        season_query.filter.return_value.all.return_value = [SimpleNamespace(season="22025")]

        teams_query = MagicMock()
        teams_query.filter.return_value.order_by.return_value.all.return_value = []

        games_query = MagicMock()
        games_query.filter.return_value = games_query
        games_query.order_by.return_value = games_query
        games_query.count.return_value = 0
        games_query.offset.return_value.limit.return_value.all.return_value = []

        team_map_query = MagicMock()
        team_map_query.all.return_value = [legacy_team]

        session = _session_ctx(MagicMock())
        session.query.side_effect = [season_query, teams_query, games_query, team_map_query]

        with self.web_app.app.test_request_context("/games?team=1610612737"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                self.web_app.games_list()

        _, kwargs = render_template.call_args
        self.assertIs(kwargs["selected_team_obj"], legacy_team)
