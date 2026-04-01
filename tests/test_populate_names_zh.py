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
    sys.modules.pop("scripts.populate_names_zh", None)
    return importlib.import_module("scripts.populate_names_zh")


class TestPopulateNamesZh(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()

        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_populate_players_uses_static_lookup_table(self):
        with self.SessionLocal() as session:
            session.add_all(
                [
                    self.models.Player(
                        player_id="2544",
                        full_name="LeBron James",
                        full_name_zh=None,
                        is_active=True,
                        is_team=False,
                    ),
                    self.models.Player(
                        player_id="999999",
                        full_name="Example Player",
                        full_name_zh=None,
                        is_active=True,
                        is_team=False,
                    ),
                ]
            )
            session.commit()

        with patch.object(self.module, "SessionLocal", self.SessionLocal):
            updated = self.module.populate_players()

        with self.SessionLocal() as session:
            lebron = session.get(self.models.Player, "2544")
            unknown = session.get(self.models.Player, "999999")

        self.assertEqual(updated, 1)
        self.assertEqual(lebron.full_name_zh, "勒布朗·詹姆斯")
        self.assertIsNone(unknown.full_name_zh)

    def test_populate_players_can_fill_from_remote_feed_without_overwriting_existing_non_static_name(self):
        with self.SessionLocal() as session:
            session.add_all(
                [
                    self.models.Player(
                        player_id="203932",
                        full_name="Aaron Gordon",
                        full_name_zh=None,
                        is_active=True,
                        is_team=False,
                    ),
                    self.models.Player(
                        player_id="999998",
                        full_name="Existing Player",
                        full_name_zh="已有译名",
                        is_active=True,
                        is_team=False,
                    ),
                ]
            )
            session.commit()

        with patch.object(self.module, "SessionLocal", self.SessionLocal), patch.object(
            self.module,
            "_fetch_nba_cn_player_names",
            return_value={
                "203932": "阿隆 戈登",
                "999998": "远端译名",
            },
        ):
            updated = self.module.populate_players(include_remote_feed=True)

        with self.SessionLocal() as session:
            aaron = session.get(self.models.Player, "203932")
            existing = session.get(self.models.Player, "999998")

        self.assertEqual(updated, 1)
        self.assertEqual(aaron.full_name_zh, "阿隆 戈登")
        self.assertEqual(existing.full_name_zh, "已有译名")


if __name__ == "__main__":
    unittest.main()
