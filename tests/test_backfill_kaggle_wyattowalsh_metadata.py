import importlib
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

import pandas as pd
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
    sys.modules.pop("db.backfill_kaggle_wyattowalsh", None)
    importlib.import_module("db.backfill_kaggle_wyattowalsh")
    sys.modules.pop("db.backfill_kaggle_wyattowalsh_metadata", None)
    return importlib.import_module("db.backfill_kaggle_wyattowalsh_metadata")


class TestBackfillKaggleWyattowalshMetadata(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()
        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _write_zip_dataset(self, path: Path) -> None:
        with zipfile.ZipFile(path, "w") as zipped:
            with zipped.open("csv/team.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "id": "1610612752",
                            "full_name": "New York Knicks",
                            "abbreviation": "NYK",
                            "nickname": "Knicks",
                            "city": "New York",
                            "state": "New York",
                            "year_founded": 1946,
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/team_details.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "team_id": "1610612752",
                            "abbreviation": "NYK",
                            "nickname": "Knicks",
                            "yearfounded": 1946,
                            "city": "New York",
                            "arena": "Madison Square Garden",
                            "arenacapacity": 19812,
                            "owner": "MSG Sports",
                            "generalmanager": "Leon Rose",
                            "headcoach": "Tom Thibodeau",
                            "dleagueaffiliation": "Westchester Knicks",
                            "facebook": "https://facebook.com/nyknicks",
                            "instagram": "https://instagram.com/nyknicks",
                            "twitter": "https://twitter.com/nyknicks",
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/player.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "id": "76001",
                            "full_name": "Alaa Abdelnaby",
                            "first_name": "Alaa",
                            "last_name": "Abdelnaby",
                            "is_active": 0,
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/common_player_info.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "person_id": "76001",
                            "first_name": "Alaa",
                            "last_name": "Abdelnaby",
                            "display_first_last": "Alaa Abdelnaby",
                            "player_slug": "alaa-abdelnaby",
                            "birthdate": "1968-06-24 00:00:00",
                            "school": "Duke",
                            "country": "USA",
                            "height": "6-10",
                            "weight": "240",
                            "season_exp": 5,
                            "jersey": "30",
                            "position": "Forward",
                            "rosterstatus": "Inactive",
                            "from_year": 1990,
                            "to_year": 1994,
                            "draft_year": "1990",
                            "draft_round": "1",
                            "draft_number": "25",
                            "greatest_75_flag": "N",
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/game_info.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "game_id": "0024600001",
                            "game_date": "1946-11-01 00:00:00",
                            "attendance": 18321,
                            "game_time": "8:30 PM",
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/game_summary.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "game_id": "0024600001",
                            "game_status_id": 3,
                            "game_status_text": "Final",
                            "gamecode": "19461101/NYKHUS",
                            "home_team_id": "1610610035",
                            "visitor_team_id": "1610612752",
                            "season": "1946",
                            "natl_tv_broadcaster_abbreviation": "ABC",
                        }
                    ]
                ).to_csv(handle, index=False)

    def test_backfills_player_team_and_game_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "archive.zip"
            self._write_zip_dataset(source)

            with self.SessionLocal() as session:
                session.add(
                    self.models.Team(
                        id=1,
                        team_id="1610612752",
                        canonical_team_id="1610612752",
                        full_name="New York Knicks",
                        abbr="NYK",
                        city="New York",
                    )
                )
                session.add(
                    self.models.Team(
                        id=2,
                        team_id="1610610035",
                        canonical_team_id="1610610035",
                        full_name="Toronto Huskies",
                        abbr="HUS",
                        city="Toronto",
                    )
                )
                session.add(
                    self.models.Game(
                        game_id="0024600001",
                        data_source="kaggle_box_scores",
                        season="21946",
                        game_status=None,
                        home_team_id="1610610035",
                        road_team_id="1610612752",
                        home_team_score=66,
                        road_team_score=68,
                    )
                )
                session.commit()

            with self.SessionLocal() as session:
                counts = self.module.backfill_kaggle_wyattowalsh_metadata(session, source)
                session.commit()

            self.assertEqual(counts.players_created, 1)
            self.assertEqual(counts.games_updated, 1)
            self.assertEqual(counts.teams_created, 0)

            with self.SessionLocal() as session:
                player = session.get(self.models.Player, "76001")
                team = session.query(self.models.Team).filter_by(team_id="1610612752").one()
                game = session.get(self.models.Game, "0024600001")

            self.assertEqual(player.full_name, "Alaa Abdelnaby")
            self.assertEqual(player.slug, "alaa-abdelnaby")
            self.assertEqual(player.school, "Duke")
            self.assertEqual(player.country, "USA")
            self.assertEqual(player.height, "6-10")
            self.assertEqual(player.weight, 240)
            self.assertEqual(player.draft_year, 1990)
            self.assertEqual(player.season_exp, 5)
            self.assertFalse(player.is_active)

            self.assertEqual(team.state, "New York")
            self.assertEqual(team.arena, "Madison Square Garden")
            self.assertEqual(team.arena_capacity, 19812)
            self.assertEqual(team.owner, "MSG Sports")
            self.assertEqual(team.general_manager, "Leon Rose")
            self.assertEqual(team.head_coach, "Tom Thibodeau")
            self.assertEqual(team.g_league_affiliation, "Westchester Knicks")
            self.assertEqual(team.twitter_url, "https://twitter.com/nyknicks")

            self.assertEqual(game.attendance, 18321)
            self.assertEqual(game.tipoff_time, "8:30 PM")
            self.assertEqual(game.external_game_code, "19461101/NYKHUS")
            self.assertEqual(game.national_tv_broadcaster, "ABC")
            self.assertEqual(game.game_status, "completed")


if __name__ == "__main__":
    unittest.main()
