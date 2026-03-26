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
        "Award", "Feedback", "Game", "GamePlayByPlay", "MagicToken", "MetricComputeRun",
        "MetricDefinition", "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "PlayerSalary", "ShotRecord", "Team", "TeamGameStats", "GameLineScore",
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


class TestPctFormatting(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()

    def test_pct_fmt_formats_decimals_and_missing_values(self):
        self.assertEqual(self.web_app.pct_fmt("0.452"), "45.2%")
        self.assertEqual(self.web_app.pct_fmt(0), "0.0%")
        self.assertEqual(self.web_app.pct_fmt("45.2%"), "45.2%")
        self.assertEqual(self.web_app.pct_fmt("-"), "—")
        self.assertEqual(self.web_app.pct_fmt(None), "—")


class TestTeamPageSeasonSummary(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()
        self.web_app.Team = SimpleNamespace(
            team_id=column("team_id"),
        )
        self.web_app.Game = SimpleNamespace(
            season=column("season"),
            game_id=column("game_id"),
            game_date=column("game_date"),
        )
        self.web_app.Award = SimpleNamespace(
            award_type=column("award_type"),
            team_id=column("team_id"),
            season=column("season"),
        )
        self.web_app.TeamGameStats = SimpleNamespace(
            team_id=column("team_id"),
            game_id=column("game_id"),
            win=column("win"),
            fgm=column("fgm"),
            fga=column("fga"),
            fg3m=column("fg3m"),
            fg3a=column("fg3a"),
            ftm=column("ftm"),
            fta=column("fta"),
        )

    def test_team_page_builds_shooting_percentages_for_template(self):
        team = SimpleNamespace(team_id="1610612738", full_name="Boston Celtics")

        team_query = MagicMock()
        team_query.filter.return_value.first.return_value = team

        championships_query = MagicMock()
        championships_query.filter.return_value.order_by.return_value.all.return_value = []

        season_summary_query = MagicMock()
        season_summary_query.join.return_value.filter.return_value.group_by.return_value.order_by.return_value.all.return_value = [
            SimpleNamespace(
                season="22025",
                wins=50,
                losses=32,
                games=82,
                fgm=3100,
                fga=6500,
                fg3m=1200,
                fg3a=3200,
                ftm=1400,
                fta=1800,
            )
        ]

        current_games_query = MagicMock()
        current_games_query.join.return_value.filter.return_value.order_by.return_value.all.return_value = []

        session = _session_ctx(MagicMock())
        session.query.side_effect = [
            team_query,
            championships_query,
            season_summary_query,
            current_games_query,
        ]

        with self.web_app.app.test_request_context("/teams/1610612738"):
            with patch.object(self.web_app, "SessionLocal", return_value=session), \
                 patch.object(self.web_app, "_team_map", return_value={}), \
                 patch.object(self.web_app, "_get_metric_results", return_value={"season": [], "alltime": []}), \
                 patch.object(self.web_app, "is_pro", return_value=False), \
                 patch.object(self.web_app, "render_template", return_value="rendered") as render_template:
                response = self.web_app.team_page("1610612738")

        self.assertEqual(response, "rendered")
        _, kwargs = render_template.call_args
        self.assertEqual(kwargs["selected_games_season"], "22025")
        self.assertEqual(kwargs["season_summary"], [{
            "season": "22025",
            "wins": 50,
            "losses": 32,
            "games": 82,
            "fg_pct": "0.477",
            "fg3_pct": "0.375",
            "ft_pct": "0.778",
        }])


if __name__ == "__main__":
    unittest.main()
