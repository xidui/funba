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
    sys.modules.pop("db.backfill_nba_player", None)
    return importlib.import_module("db.backfill_nba_player")


class TestBackfillNbaPlayer(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()

        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_get_all_players_updates_existing_rows_instead_of_skipping_duplicates(self):
        with self.SessionLocal() as session:
            session.add(
                self.models.Player(
                    player_id="203999",
                    first_name="Old",
                    last_name="Name",
                    full_name="Old Name",
                    is_active=False,
                    is_team=False,
                )
            )
            session.commit()

        rows = [
            {
                "id": 203999,
                "first_name": "New",
                "last_name": "Name",
                "full_name": "New Name",
                "is_active": True,
            }
        ]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module.players, "get_players", return_value=rows
        ):
            counts = self.module.get_all_players()

        with self.SessionLocal() as session:
            player = session.get(self.models.Player, "203999")

        self.assertEqual(counts["created"], 0)
        self.assertEqual(counts["updated"], 1)
        self.assertEqual(counts["skipped"], 0)
        self.assertIsNotNone(player)
        self.assertEqual(player.first_name, "New")
        self.assertEqual(player.full_name, "New Name")
        self.assertTrue(player.is_active)

    def test_get_all_players_creates_missing_players(self):
        rows = [
            {
                "id": 204000,
                "first_name": "Rookie",
                "last_name": "Example",
                "full_name": "Rookie Example",
                "is_active": True,
            }
        ]

        with patch.object(self.module, "Session", self.SessionLocal), patch.object(
            self.module.players, "get_players", return_value=rows
        ):
            counts = self.module.get_all_players()

        with self.SessionLocal() as session:
            player = session.get(self.models.Player, "204000")

        self.assertEqual(counts["created"], 1)
        self.assertEqual(counts["updated"], 0)
        self.assertEqual(counts["skipped"], 0)
        self.assertIsNotNone(player)
        self.assertEqual(player.full_name, "Rookie Example")
        self.assertTrue(player.is_active)


if __name__ == "__main__":
    unittest.main()
