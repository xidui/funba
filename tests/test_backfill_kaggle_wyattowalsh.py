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
    return importlib.import_module("db.backfill_kaggle_wyattowalsh")


class TestBackfillKaggleWyattowalsh(unittest.TestCase):
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
            with zipped.open("csv/game.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "season_id": 21946,
                            "team_id_home": 1610610035,
                            "team_abbreviation_home": "HUS",
                            "team_name_home": "Toronto Huskies",
                            "game_id": 24600001,
                            "game_date": "1946-11-01 00:00:00",
                            "wl_home": "L",
                            "min": 0,
                            "fgm_home": 25,
                            "fga_home": "",
                            "fg_pct_home": "",
                            "fg3m_home": "",
                            "fg3a_home": "",
                            "fg3_pct_home": "",
                            "ftm_home": 16,
                            "fta_home": 29,
                            "ft_pct_home": 0.552,
                            "oreb_home": "",
                            "dreb_home": "",
                            "reb_home": "",
                            "ast_home": "",
                            "stl_home": "",
                            "blk_home": "",
                            "tov_home": "",
                            "pf_home": "",
                            "pts_home": 66,
                            "plus_minus_home": -2,
                            "team_id_away": 1610612752,
                            "team_abbreviation_away": "NYK",
                            "team_name_away": "New York Knicks",
                            "wl_away": "W",
                            "fgm_away": 24,
                            "fga_away": "",
                            "fg_pct_away": "",
                            "fg3m_away": "",
                            "fg3a_away": "",
                            "fg3_pct_away": "",
                            "ftm_away": 20,
                            "fta_away": 26,
                            "ft_pct_away": 0.769,
                            "oreb_away": "",
                            "dreb_away": "",
                            "reb_away": "",
                            "ast_away": "",
                            "stl_away": "",
                            "blk_away": "",
                            "tov_away": "",
                            "pf_away": "",
                            "pts_away": 68,
                            "plus_minus_away": 2,
                            "season_type": "Regular Season",
                        },
                        {
                            "season_id": 21956,
                            "team_id_home": 1610612758,
                            "team_abbreviation_home": "ROC",
                            "team_name_home": "Rochester Royals",
                            "game_id": 25600099,
                            "game_date": "1956-12-01 00:00:00",
                            "wl_home": "W",
                            "min": 0,
                            "fgm_home": 31,
                            "fga_home": 70,
                            "fg_pct_home": 0.443,
                            "fg3m_home": "",
                            "fg3a_home": "",
                            "fg3_pct_home": "",
                            "ftm_home": 14,
                            "fta_home": 20,
                            "ft_pct_home": 0.7,
                            "oreb_home": "",
                            "dreb_home": "",
                            "reb_home": 41,
                            "ast_home": 19,
                            "stl_home": "",
                            "blk_home": "",
                            "tov_home": "",
                            "pf_home": 20,
                            "pts_home": 76,
                            "plus_minus_home": 8,
                            "team_id_away": 1610612755,
                            "team_abbreviation_away": "SYR",
                            "team_name_away": "Syracuse Nationals",
                            "wl_away": "L",
                            "fgm_away": 28,
                            "fga_away": 73,
                            "fg_pct_away": 0.384,
                            "fg3m_away": "",
                            "fg3a_away": "",
                            "fg3_pct_away": "",
                            "ftm_away": 12,
                            "fta_away": 18,
                            "ft_pct_away": 0.667,
                            "oreb_away": "",
                            "dreb_away": "",
                            "reb_away": 37,
                            "ast_away": 16,
                            "stl_away": "",
                            "blk_away": "",
                            "tov_away": "",
                            "pf_away": 22,
                            "pts_away": 68,
                            "plus_minus_away": -8,
                            "season_type": "Regular Season",
                        },
                        {
                            "season_id": 31955,
                            "team_id_home": 1610616833,
                            "team_abbreviation_home": "EST",
                            "team_name_home": "East NBA All Stars East",
                            "game_id": 35500001,
                            "game_date": "1956-01-24 00:00:00",
                            "wl_home": "W",
                            "min": 0,
                            "fgm_home": 40,
                            "fga_home": 88,
                            "fg_pct_home": 0.455,
                            "fg3m_home": "",
                            "fg3a_home": "",
                            "fg3_pct_home": "",
                            "ftm_home": 18,
                            "fta_home": 24,
                            "ft_pct_home": 0.75,
                            "oreb_home": "",
                            "dreb_home": "",
                            "reb_home": 50,
                            "ast_home": 22,
                            "stl_home": "",
                            "blk_home": "",
                            "tov_home": "",
                            "pf_home": 17,
                            "pts_home": 98,
                            "plus_minus_home": 3,
                            "team_id_away": 1610616834,
                            "team_abbreviation_away": "WST",
                            "team_name_away": "West NBA All Stars West",
                            "wl_away": "L",
                            "fgm_away": 39,
                            "fga_away": 86,
                            "fg_pct_away": 0.453,
                            "fg3m_away": "",
                            "fg3a_away": "",
                            "fg3_pct_away": "",
                            "ftm_away": 17,
                            "fta_away": 23,
                            "ft_pct_away": 0.739,
                            "oreb_away": "",
                            "dreb_away": "",
                            "reb_away": 47,
                            "ast_away": 21,
                            "stl_away": "",
                            "blk_away": "",
                            "tov_away": "",
                            "pf_away": 19,
                            "pts_away": 95,
                            "plus_minus_away": -3,
                            "season_type": "All Star",
                        },
                        {
                            "season_id": 31955,
                            "team_id_home": 1610616833,
                            "team_abbreviation_home": "EST",
                            "team_name_home": "East NBA All Stars East",
                            "game_id": 35500001,
                            "game_date": "1956-01-24 00:00:00",
                            "wl_home": "W",
                            "min": 0,
                            "fgm_home": 40,
                            "fga_home": 88,
                            "fg_pct_home": 0.455,
                            "fg3m_home": "",
                            "fg3a_home": "",
                            "fg3_pct_home": "",
                            "ftm_home": 18,
                            "fta_home": 24,
                            "ft_pct_home": 0.75,
                            "oreb_home": "",
                            "dreb_home": "",
                            "reb_home": 50,
                            "ast_home": 22,
                            "stl_home": "",
                            "blk_home": "",
                            "tov_home": "",
                            "pf_home": 17,
                            "pts_home": 98,
                            "plus_minus_home": 3,
                            "team_id_away": 1610616834,
                            "team_abbreviation_away": "WST",
                            "team_name_away": "West NBA All Stars West",
                            "wl_away": "L",
                            "fgm_away": 39,
                            "fga_away": 86,
                            "fg_pct_away": 0.453,
                            "fg3m_away": "",
                            "fg3a_away": "",
                            "fg3_pct_away": "",
                            "ftm_away": 17,
                            "fta_away": 23,
                            "ft_pct_away": 0.739,
                            "oreb_away": "",
                            "dreb_away": "",
                            "reb_away": 47,
                            "ast_away": 21,
                            "stl_away": "",
                            "blk_away": "",
                            "tov_away": "",
                            "pf_away": 19,
                            "pts_away": 95,
                            "plus_minus_away": -3,
                            "season_type": "All-Star",
                        },
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/line_score.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "game_date_est": "1946-11-01 00:00:00",
                            "game_id": 24600001,
                            "team_id_home": 1610610035,
                            "team_abbreviation_home": "HUS",
                            "team_city_name_home": "Toronto",
                            "team_nickname_home": "Huskies",
                            "pts_qtr1_home": "",
                            "pts_qtr2_home": "",
                            "pts_qtr3_home": "",
                            "pts_qtr4_home": "",
                            "pts_ot1_home": 18,
                            "pts_ot2_home": "",
                            "pts_ot3_home": "",
                            "pts_ot4_home": "",
                            "pts_ot5_home": "",
                            "pts_ot6_home": "",
                            "pts_ot7_home": "",
                            "pts_ot8_home": "",
                            "pts_ot9_home": "",
                            "pts_ot10_home": "",
                            "pts_home": 66,
                            "team_id_away": 1610612752,
                            "team_abbreviation_away": "NYK",
                            "team_city_name_away": "New York",
                            "team_nickname_away": "Knicks",
                            "pts_qtr1_away": "",
                            "pts_qtr2_away": "",
                            "pts_qtr3_away": "",
                            "pts_qtr4_away": "",
                            "pts_ot1_away": 24,
                            "pts_ot2_away": "",
                            "pts_ot3_away": "",
                            "pts_ot4_away": "",
                            "pts_ot5_away": "",
                            "pts_ot6_away": "",
                            "pts_ot7_away": "",
                            "pts_ot8_away": "",
                            "pts_ot9_away": "",
                            "pts_ot10_away": "",
                            "pts_away": 68,
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/team.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "id": 1610612752,
                            "full_name": "New York Knicks",
                            "abbreviation": "NYK",
                            "nickname": "Knicks",
                            "city": "New York",
                            "state": "New York",
                            "year_founded": 1946,
                        }
                    ]
                ).to_csv(handle, index=False)

            with zipped.open("csv/team_history.csv", "w") as handle:
                pd.DataFrame(
                    [
                        {
                            "team_id": 1610612758,
                            "city": "Rochester",
                            "nickname": "Royals",
                            "year_founded": 1948,
                            "year_active_till": 1956,
                        },
                        {
                            "team_id": 1610612755,
                            "city": "Syracuse",
                            "nickname": "Nationals",
                            "year_founded": 1949,
                            "year_active_till": 1963,
                        },
                    ]
                ).to_csv(handle, index=False)

    def test_imports_games_team_stats_and_line_scores_from_wide_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "archive.zip"
            self._write_zip_dataset(source)

            with self.SessionLocal() as session:
                session.add(
                    self.models.Team(
                        team_id="1610612758",
                        canonical_team_id="1610612758",
                        full_name="Sacramento Kings",
                        abbr="SAC",
                        is_legacy=False,
                    )
                )
                session.add(
                    self.models.Team(
                        team_id="1610612752",
                        canonical_team_id="1610612752",
                        full_name="New York Knicks",
                        abbr="NYK",
                        is_legacy=False,
                    )
                )
                session.commit()

            with self.SessionLocal() as session:
                counts = self.module.backfill_kaggle_wyattowalsh(session, source, season_end=1956)
                session.commit()

            self.assertEqual(counts.games_created, 3)
            self.assertEqual(counts.team_stats_created, 6)
            self.assertEqual(counts.line_scores_created, 2)
            self.assertEqual(counts.teams_created, 4)

            with self.SessionLocal() as session:
                first_game = session.get(self.models.Game, "0024600001")
                home_team_stat = session.query(self.models.TeamGameStats).filter_by(game_id="0024600001", team_id="1610610035").one()
                road_team_stat = session.query(self.models.TeamGameStats).filter_by(game_id="0025600099", team_id="1610612758").one()
                line_score = session.query(self.models.GameLineScore).filter_by(game_id="0024600001", team_id="1610610035").one()
                toronto = session.query(self.models.Team).filter_by(team_id="1610610035").one()
                kings = session.query(self.models.Team).filter_by(team_id="1610612758").one()

            self.assertIsNotNone(first_game)
            self.assertEqual(first_game.data_source, self.module.KAGGLE_BOX_SCORE_SOURCE)
            self.assertEqual(first_game.home_team_id, "1610610035")
            self.assertEqual(home_team_stat.data_source, self.module.KAGGLE_BOX_SCORE_SOURCE)
            self.assertIsNone(home_team_stat.fg3m)
            self.assertEqual(road_team_stat.team_id, "1610612758")
            self.assertEqual(line_score.ot1_pts, 18)
            self.assertEqual(toronto.full_name, "Toronto Huskies")
            self.assertEqual(toronto.abbr, "HUS")
            self.assertEqual(kings.full_name, "Sacramento Kings")
            self.assertEqual(kings.abbr, "SAC")

    def test_skips_existing_nba_api_games_and_is_idempotent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "archive.zip"
            self._write_zip_dataset(source)

            with self.SessionLocal() as session:
                session.add(
                    self.models.Team(
                        team_id="1610612752",
                        canonical_team_id="1610612752",
                        full_name="New York Knicks",
                        abbr="NYK",
                        is_legacy=False,
                    )
                )
                session.add(
                    self.models.Team(
                        team_id="1610612758",
                        canonical_team_id="1610612758",
                        full_name="Sacramento Kings",
                        abbr="SAC",
                        is_legacy=False,
                    )
                )
                session.add(
                    self.models.Game(
                        game_id="0025600099",
                        data_source="nba_api_box_scores",
                        season="21956",
                    )
                )
                session.commit()

            with self.SessionLocal() as session:
                first = self.module.backfill_kaggle_wyattowalsh(session, source, season_end=1956)
                session.commit()

            with self.SessionLocal() as session:
                second = self.module.backfill_kaggle_wyattowalsh(session, source, season_end=1946)
                session.commit()
                game_count = session.query(self.models.Game).count()
                team_stat_count = session.query(self.models.TeamGameStats).count()
                line_score_count = session.query(self.models.GameLineScore).count()

            self.assertEqual(first.skipped_existing_nba_api, 1)
            self.assertEqual(first.games_created, 2)
            self.assertEqual(second.games_created, 0)
            self.assertEqual(second.games_updated, 1)
            self.assertEqual(game_count, 3)
            self.assertEqual(team_stat_count, 4)
            self.assertEqual(line_score_count, 2)


if __name__ == "__main__":
    unittest.main()
