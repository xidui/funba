import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _Field:
    def __eq__(self, other):
        return ("eq", other)


def _install_stubs():
    fake_nba_api = types.ModuleType("nba_api")
    fake_stats = types.ModuleType("nba_api.stats")
    fake_endpoints = types.ModuleType("nba_api.stats.endpoints")
    fake_boxscore = types.ModuleType("nba_api.stats.endpoints.boxscoretraditionalv3")

    class _FakeBoxScoreTraditionalV3:
        def __init__(self, *args, **kwargs):
            pass

        def get_dict(self):
            return {}

    fake_boxscore.BoxScoreTraditionalV3 = _FakeBoxScoreTraditionalV3
    fake_endpoints.boxscoretraditionalv3 = fake_boxscore
    fake_stats.endpoints = fake_endpoints
    fake_nba_api.stats = fake_stats

    sys.modules["nba_api"] = fake_nba_api
    sys.modules["nba_api.stats"] = fake_stats
    sys.modules["nba_api.stats.endpoints"] = fake_endpoints
    sys.modules["nba_api.stats.endpoints.boxscoretraditionalv3"] = fake_boxscore

    fake_tenacity = types.ModuleType("tenacity")

    def _retry(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator

    fake_tenacity.retry = _retry
    fake_tenacity.wait_exponential = lambda *args, **kwargs: None
    fake_tenacity.stop_after_attempt = lambda *args, **kwargs: None
    fake_tenacity.retry_if_exception_type = lambda *args, **kwargs: None
    fake_tenacity.before_sleep_log = lambda *args, **kwargs: None

    class _RetryError(Exception):
        pass

    fake_tenacity.RetryError = _RetryError
    sys.modules["tenacity"] = fake_tenacity

    fake_models = types.ModuleType("db.models")

    class Team:
        pass

    class TeamGameStats:
        game_id = _Field()
        team_id = _Field()
        pts = _Field()

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    class PlayerGameStats:
        game_id = _Field()
        team_id = _Field()
        player_id = _Field()

        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    class Player:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

    fake_models.Team = Team
    fake_models.TeamGameStats = TeamGameStats
    fake_models.PlayerGameStats = PlayerGameStats
    fake_models.Player = Player
    fake_models.engine = MagicMock()

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db
    sys.modules["db.models"] = fake_models


def _load_module():
    _install_stubs()
    sys.modules.pop("db.backfill_nba_game_detail", None)
    return importlib.import_module("db.backfill_nba_game_detail")


class TestCreateTeamGameStats(unittest.TestCase):
    def setUp(self):
        self.module = _load_module()

    def test_updates_existing_record_instead_of_skipping(self):
        existing = self.module.TeamGameStats(game_id="g1", team_id="t1", pts=0, win=False, min=0)
        query = MagicMock()
        query.filter_by.return_value.first.return_value = existing
        session = MagicMock()
        session.query.return_value = query

        self.module.create_team_game_stats(
            session,
            "g1",
            {
                "TEAM_ID": "t1",
                "MIN": "48:00",
                "PTS": 111,
                "FGM": 40,
                "FGA": 82,
                "FG_PCT": 0.49,
                "FG3M": 12,
                "FG3A": 30,
                "FG3_PCT": 0.4,
                "FTM": 19,
                "FTA": 22,
                "FT_PCT": 0.86,
                "OREB": 8,
                "DREB": 33,
                "REB": 41,
                "AST": 25,
                "STL": 7,
                "BLK": 5,
                "TO": 11,
                "PF": 18,
            },
            on_road=True,
            win=True,
        )

        self.assertEqual(existing.pts, 111)
        self.assertEqual(existing.min, 48)
        self.assertTrue(existing.on_road)
        self.assertTrue(existing.win)
        session.add.assert_called_once_with(existing)


class TestCreatePlayerGameStats(unittest.TestCase):
    def setUp(self):
        self.module = _load_module()

    def test_updates_existing_record_instead_of_skipping(self):
        existing_player = self.module.Player(player_id="p1")
        existing_stats = self.module.PlayerGameStats(
            game_id="g1",
            team_id="t1",
            player_id="p1",
            pts=0,
            starter=False,
            position="",
        )

        player_query = MagicMock()
        player_query.filter_by.return_value.first.return_value = existing_player
        stats_query = MagicMock()
        stats_query.filter_by.return_value.first.return_value = existing_stats
        session = MagicMock()
        session.query.side_effect = [player_query, stats_query]

        started = self.module.create_player_game_stats(
            session,
            {
                "GAME_ID": "g1",
                "TEAM_ID": "t1",
                "PLAYER_ID": "p1",
                "PLAYER_NAME": "Jane Doe",
                "NICKNAME": "J. Doe",
                "COMMENT": "",
                "MIN": "35:21",
                "START_POSITION": "G",
                "PTS": 27,
                "FGM": 9,
                "FGA": 18,
                "FG_PCT": 0.5,
                "FG3M": 3,
                "FG3A": 7,
                "FG3_PCT": 0.43,
                "FTM": 6,
                "FTA": 6,
                "FT_PCT": 1.0,
                "OREB": 1,
                "DREB": 4,
                "REB": 5,
                "AST": 8,
                "STL": 2,
                "BLK": 1,
                "TO": 3,
                "PF": 2,
                "PLUS_MINUS": 14,
            },
        )

        self.assertTrue(started)
        self.assertEqual(existing_stats.pts, 27)
        self.assertEqual(existing_stats.min, 35)
        self.assertEqual(existing_stats.sec, 21)
        self.assertTrue(existing_stats.starter)
        self.assertEqual(existing_stats.position, "G")
        self.assertEqual(existing_stats.plus, 14)
        session.add.assert_called_once_with(existing_stats)


class TestIsGameDetailBackFilled(unittest.TestCase):
    def setUp(self):
        self.module = _load_module()

    def test_returns_false_when_team_rows_sum_to_zero(self):
        player_query = MagicMock()
        player_query.filter_by.return_value.count.return_value = 10

        team_query = MagicMock()
        team_query.filter_by.return_value.count.return_value = 2

        points_query = MagicMock()
        points_query.filter.return_value.scalar.return_value = 0

        session = MagicMock()
        session.query.side_effect = [player_query, team_query, points_query]

        self.assertFalse(self.module.is_game_detail_back_filled("0022500958", session))

    def test_returns_true_when_rows_exist_and_points_are_non_zero(self):
        player_query = MagicMock()
        player_query.filter_by.return_value.count.return_value = 10

        team_query = MagicMock()
        team_query.filter_by.return_value.count.return_value = 2

        points_query = MagicMock()
        points_query.filter.return_value.scalar.return_value = 214

        session = MagicMock()
        session.query.side_effect = [player_query, team_query, points_query]

        self.assertTrue(self.module.is_game_detail_back_filled("g1", session))


if __name__ == "__main__":
    unittest.main()
