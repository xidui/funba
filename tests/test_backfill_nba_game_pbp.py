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
    sys.modules.pop("db.backfill_nba_game_pbp", None)
    return importlib.import_module("db.backfill_nba_game_pbp")


class TestBackfillNbaGamePbp(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()

        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def test_normalize_pbp_preserves_person_id_as_player1(self):
        payload = {
            "game": {
                "actions": [
                    {
                        "actionNumber": 7,
                        "actionType": "Made Shot",
                        "clock": "PT11M34.00S",
                        "description": "Test bucket",
                        "location": "h",
                        "period": 1,
                        "personId": 203999,
                        "scoreHome": "2",
                        "scoreAway": "0",
                    }
                ]
            }
        }

        normalized = self.module._normalize_pbp(payload)

        self.assertEqual(len(normalized["PlayByPlay"]), 1)
        self.assertEqual(normalized["PlayByPlay"][0]["PLAYER1_ID"], 203999)

    def test_back_fill_pbp_nulls_non_player_person_ids(self):
        with self.SessionLocal() as session:
            session.add(
                self.models.Game(
                    game_id="0022400001",
                    season="22024",
                )
            )
            session.add(
                self.models.Player(
                    player_id="203999",
                    first_name="Test",
                    last_name="Player",
                    full_name="Test Player",
                    is_active=True,
                    is_team=False,
                )
            )
            session.commit()

            fake_pbp = {
                "PlayByPlay": [
                    {
                        "EVENTNUM": 1,
                        "EVENTMSGTYPE": 1,
                        "EVENTMSGACTIONTYPE": None,
                        "PERIOD": 1,
                        "WCTIMESTRING": "PT11M34.00S",
                        "PCTIMESTRING": "11:34",
                        "HOMEDESCRIPTION": "Test Player makes shot",
                        "NEUTRALDESCRIPTION": None,
                        "VISITORDESCRIPTION": None,
                        "SCORE": "2 - 0",
                        "SCOREMARGIN": "2",
                        "PLAYER1_ID": 203999,
                        "PLAYER2_ID": 0,
                        "PLAYER3_ID": 0,
                    },
                    {
                        "EVENTNUM": 2,
                        "EVENTMSGTYPE": 5,
                        "EVENTMSGACTIONTYPE": None,
                        "PERIOD": 1,
                        "WCTIMESTRING": "PT11M10.00S",
                        "PCTIMESTRING": "11:10",
                        "HOMEDESCRIPTION": "Shot Clock Turnover",
                        "NEUTRALDESCRIPTION": None,
                        "VISITORDESCRIPTION": None,
                        "SCORE": None,
                        "SCOREMARGIN": None,
                        "PLAYER1_ID": 1610612744,
                        "PLAYER2_ID": 0,
                        "PLAYER3_ID": 0,
                    },
                ]
            }

            with patch.object(self.module, "fetch_game_play_by_play", return_value=fake_pbp):
                self.module.back_fill_pbp("0022400001", session, True)

            rows = (
                session.query(self.models.GamePlayByPlay)
                .order_by(self.models.GamePlayByPlay.event_num.asc())
                .all()
            )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].player1_id, "203999")
        self.assertIsNone(rows[1].player1_id)


if __name__ == "__main__":
    unittest.main()
