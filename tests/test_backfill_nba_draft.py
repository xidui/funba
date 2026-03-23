import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _load_real_db_models():
    sys.modules.pop("db.models", None)
    real_models = importlib.import_module("db.models")
    if "db" in sys.modules:
        sys.modules["db"].models = real_models
    return real_models


def _load_module():
    _load_real_db_models()
    sys.modules.pop("db.backfill_nba_draft", None)
    return importlib.import_module("db.backfill_nba_draft")


class TestBackfillNbaDraft(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()

        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _players(self):
        with self.SessionLocal() as session:
            return (
                session.query(self.models.Player)
                .order_by(self.models.Player.player_id.asc())
                .all()
            )

    def test_single_year_run_creates_players_from_api_rows(self):
        rows = [
            {
                "SEASON": "2009",
                "ROUND_NUMBER": "1",
                "ROUND_PICK": "7",
                "PLAYER_ID": "201935",
                "PLAYER_NAME": "Player One",
            },
            {
                "SEASON": "2009",
                "ROUND_NUMBER": "1",
                "ROUND_PICK": "8",
                "PLAYER_ID": "201939",
                "PLAYER_NAME": "Player Two",
            },
        ]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_draft_history", return_value=rows
        ) as fetch_mock, patch.object(self.module.time, "sleep") as sleep_mock, patch.object(
            self.module.logger, "info"
        ):
            counts = self.module.run(year=2009)

        players = self._players()
        self.assertEqual(fetch_mock.call_args_list[0].args, (2009,))
        self.assertEqual(len(players), 2)
        self.assertEqual(players[0].full_name, "Player One")
        self.assertEqual(players[0].draft_year, 2009)
        self.assertEqual(players[0].draft_round, 1)
        self.assertEqual(players[0].draft_number, 7)
        self.assertFalse(players[0].is_active)
        self.assertFalse(players[0].is_team)
        self.assertEqual(counts.created, 2)
        self.assertEqual(counts.updated, 0)
        self.assertEqual(counts.skipped, 0)
        sleep_mock.assert_not_called()

    def test_run_updates_existing_players_and_creates_missing_players(self):
        with self.SessionLocal() as session:
            session.add(
                self.models.Player(
                    player_id="201939",
                    full_name="Stephen Curry",
                    is_active=True,
                    is_team=False,
                )
            )
            session.commit()

        rows = [
            {
                "SEASON": "2009",
                "ROUND_NUMBER": "1",
                "ROUND_PICK": "7",
                "PLAYER_ID": "201935",
                "PLAYER_NAME": "James Harden",
            },
            {
                "SEASON": "2009",
                "ROUND_NUMBER": "1",
                "ROUND_PICK": "8",
                "PLAYER_ID": "201939",
                "PLAYER_NAME": "Stephen Curry",
            },
        ]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_draft_history", return_value=rows
        ), patch.object(self.module.time, "sleep"), patch.object(self.module.logger, "info"):
            counts = self.module.run(year=2009)

        players = self._players()
        curry = next(player for player in players if player.player_id == "201939")
        harden = next(player for player in players if player.player_id == "201935")

        self.assertEqual(len(players), 2)
        self.assertEqual(curry.draft_year, 2009)
        self.assertEqual(curry.draft_round, 1)
        self.assertEqual(curry.draft_number, 8)
        self.assertTrue(curry.is_active)
        self.assertEqual(harden.full_name, "James Harden")
        self.assertFalse(harden.is_active)
        self.assertEqual(counts.updated, 1)
        self.assertEqual(counts.created, 1)

    def test_run_is_idempotent_when_repeated(self):
        rows = [
            {
                "SEASON": "1996",
                "ROUND_NUMBER": "1",
                "ROUND_PICK": "13",
                "PLAYER_ID": "2544",
                "PLAYER_NAME": "Kobe Bryant",
            }
        ]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_draft_history", return_value=rows
        ), patch.object(self.module.time, "sleep"), patch.object(self.module.logger, "info"):
            first_counts = self.module.run(year=1996)
            second_counts = self.module.run(year=1996)

        players = self._players()
        self.assertEqual(len(players), 1)
        self.assertEqual(players[0].player_id, "2544")
        self.assertEqual(first_counts.created, 1)
        self.assertEqual(second_counts.created, 0)
        self.assertEqual(second_counts.updated, 0)
        self.assertEqual(second_counts.skipped, 1)

    def test_run_all_iterates_historical_year_range(self):
        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module, "fetch_draft_history", return_value=[]
        ) as fetch_mock, patch.object(self.module.time, "sleep") as sleep_mock, patch.object(
            self.module.logger, "info"
        ):
            self.module.run(refresh_all=True)

        self.assertEqual(fetch_mock.call_count, 2024 - 1947 + 1)
        self.assertEqual(fetch_mock.call_args_list[0].args, (1947,))
        self.assertEqual(fetch_mock.call_args_list[-1].args, (2024,))
        self.assertEqual(sleep_mock.call_count, fetch_mock.call_count - 1)


if __name__ == "__main__":
    unittest.main()
