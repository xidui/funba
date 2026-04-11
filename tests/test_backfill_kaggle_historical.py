import importlib
import sys
import tempfile
import unittest
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
    sys.modules.pop("db.backfill_kaggle_historical", None)
    return importlib.import_module("db.backfill_kaggle_historical")


class TestBackfillKaggleHistorical(unittest.TestCase):
    def setUp(self):
        self.models = _load_real_db_models()
        self.module = _load_module()
        self.engine = create_engine("sqlite:///:memory:")
        self.models.Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def tearDown(self):
        self.engine.dispose()

    def _write_dataset(self, root: Path) -> None:
        pd.DataFrame(
            [
                {
                    "game_id": "HIST-0001",
                    "game_date": "1984-10-05",
                    "season_id": "21984",
                    "home_team_id": "1610612738",
                    "home_team_name": "Boston Celtics",
                    "home_team_abbr": "BOS",
                    "away_team_id": "1610612747",
                    "away_team_name": "Los Angeles Lakers",
                    "away_team_abbr": "LAL",
                    "home_team_score": 102,
                    "away_team_score": 97,
                }
            ]
        ).to_csv(root / "Games.csv", index=False)

        pd.DataFrame(
            [
                {
                    "game_id": "HIST-0001",
                    "team_id": "1610612738",
                    "team_name": "Boston Celtics",
                    "team_abbr": "BOS",
                    "minutes": "240:00",
                    "points": 102,
                    "field_goals_made": 42,
                    "field_goals_attempted": 89,
                    "field_goals_percentage": 0.472,
                    "three_pointers_made": "",
                    "three_pointers_attempted": "",
                    "three_pointers_percentage": "",
                    "free_throws_made": 18,
                    "free_throws_attempted": 21,
                    "free_throws_percentage": 0.857,
                    "rebounds_offensive": "",
                    "rebounds_defensive": "",
                    "rebounds_total": 44,
                    "assists": 21,
                    "steals": 9,
                    "blocks": 5,
                    "turnovers": 13,
                    "fouls_personal": 20,
                    "q1_pts": 24,
                    "q2_pts": 28,
                    "q3_pts": 26,
                    "q4_pts": 24,
                },
                {
                    "game_id": "HIST-0001",
                    "team_id": "1610612747",
                    "team_name": "Los Angeles Lakers",
                    "team_abbr": "LAL",
                    "minutes": "240:00",
                    "points": 97,
                    "field_goals_made": 39,
                    "field_goals_attempted": 88,
                    "field_goals_percentage": 0.443,
                    "three_pointers_made": "",
                    "three_pointers_attempted": "",
                    "three_pointers_percentage": "",
                    "free_throws_made": 19,
                    "free_throws_attempted": 24,
                    "free_throws_percentage": 0.792,
                    "rebounds_offensive": "",
                    "rebounds_defensive": "",
                    "rebounds_total": 40,
                    "assists": 19,
                    "steals": 7,
                    "blocks": 4,
                    "turnovers": 15,
                    "fouls_personal": 22,
                    "q1_pts": 23,
                    "q2_pts": 25,
                    "q3_pts": 23,
                    "q4_pts": 26,
                },
            ]
        ).to_csv(root / "TeamStatistics.csv", index=False)

        pd.DataFrame(
            [
                {
                    "game_id": "HIST-0001",
                    "team_id": "1610612738",
                    "player_id": "historic-1",
                    "player_name": "Larry Bird",
                    "minutes": "38:15",
                    "starter": 1,
                    "position": "F",
                    "points": 28,
                    "field_goals_made": 11,
                    "field_goals_attempted": 20,
                    "field_goals_percentage": 0.55,
                    "three_pointers_made": "",
                    "three_pointers_attempted": "",
                    "three_pointers_percentage": "",
                    "free_throws_made": 6,
                    "free_throws_attempted": 7,
                    "free_throws_percentage": 0.857,
                    "rebounds_offensive": "",
                    "rebounds_defensive": "",
                    "rebounds_total": 10,
                    "assists": 7,
                    "steals": 2,
                    "blocks": 1,
                    "turnovers": 3,
                    "fouls_personal": 2,
                    "plus_minus": "",
                },
                {
                    "game_id": "HIST-0001",
                    "team_id": "1610612747",
                    "player_id": "historic-2",
                    "player_name": "Magic Johnson",
                    "minutes": "39:02",
                    "starter": 1,
                    "position": "G",
                    "points": 24,
                    "field_goals_made": 9,
                    "field_goals_attempted": 18,
                    "field_goals_percentage": 0.5,
                    "three_pointers_made": "",
                    "three_pointers_attempted": "",
                    "three_pointers_percentage": "",
                    "free_throws_made": 6,
                    "free_throws_attempted": 9,
                    "free_throws_percentage": 0.667,
                    "rebounds_offensive": "",
                    "rebounds_defensive": "",
                    "rebounds_total": 8,
                    "assists": 11,
                    "steals": 3,
                    "blocks": 0,
                    "turnovers": 4,
                    "fouls_personal": 3,
                    "plus_minus": "",
                },
            ]
        ).to_csv(root / "PlayerStatistics.csv", index=False)

        pd.DataFrame(
            [
                {"player_id": "historic-1", "full_name": "Larry Bird", "first_name": "Larry", "last_name": "Bird", "position": "F"},
                {"player_id": "historic-2", "full_name": "Magic Johnson", "first_name": "Magic", "last_name": "Johnson", "position": "G"},
            ]
        ).to_csv(root / "Players.csv", index=False)

        pd.DataFrame(
            [
                {"team_id": "1610612738", "full_name": "Boston Celtics", "abbr": "BOS", "start_year": 1946},
                {"team_id": "1610612747", "full_name": "Los Angeles Lakers", "abbr": "LAL", "start_year": 1948},
            ]
        ).to_csv(root / "TeamHistories.csv", index=False)

    def test_backfill_imports_source_tagged_rows_and_preserves_null_legacy_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_dataset(root)

            with self.SessionLocal() as session:
                counts = self.module.backfill_kaggle_historical(session, root)
                session.commit()

            self.assertEqual(counts.games_created, 1)
            self.assertEqual(counts.team_stats_created, 2)
            self.assertEqual(counts.player_stats_created, 2)
            self.assertEqual(counts.line_scores_created, 2)

            with self.SessionLocal() as session:
                game = session.get(self.models.Game, "HIST-0001")
                team_stat = session.query(self.models.TeamGameStats).filter_by(game_id="HIST-0001", team_id="1610612738").one()
                player_stat = session.query(self.models.PlayerGameStats).filter_by(game_id="HIST-0001", player_id="historic-1").one()
                line_score = session.query(self.models.GameLineScore).filter_by(game_id="HIST-0001", team_id="1610612738").one()

            self.assertIsNotNone(game)
            self.assertEqual(game.data_source, self.module.KAGGLE_BOX_SCORE_SOURCE)
            self.assertEqual(game.season, "21984")
            self.assertEqual(team_stat.data_source, self.module.KAGGLE_BOX_SCORE_SOURCE)
            self.assertIsNone(team_stat.fg3m)
            self.assertIsNone(team_stat.oreb)
            self.assertEqual(player_stat.data_source, self.module.KAGGLE_BOX_SCORE_SOURCE)
            self.assertIsNone(player_stat.fg3a)
            self.assertIsNone(player_stat.plus)
            self.assertEqual(line_score.source, self.module.KAGGLE_BOX_SCORE_SOURCE)
            self.assertEqual(line_score.q1_pts, 24)

    def test_backfill_is_idempotent_for_existing_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_dataset(root)

            with self.SessionLocal() as session:
                first = self.module.backfill_kaggle_historical(session, root)
                session.commit()

            with self.SessionLocal() as session:
                second = self.module.backfill_kaggle_historical(session, root)
                session.commit()
                game_count = session.query(self.models.Game).count()
                team_stat_count = session.query(self.models.TeamGameStats).count()
                player_stat_count = session.query(self.models.PlayerGameStats).count()
                line_score_count = session.query(self.models.GameLineScore).count()

            self.assertEqual(first.games_created, 1)
            self.assertEqual(second.games_created, 0)
            self.assertEqual(second.games_updated, 1)
            self.assertEqual(second.team_stats_created, 0)
            self.assertEqual(second.player_stats_created, 0)
            self.assertEqual(game_count, 1)
            self.assertEqual(team_stat_count, 2)
            self.assertEqual(player_stat_count, 2)
            self.assertEqual(line_score_count, 2)

    def test_matches_existing_game_by_date_teams_and_score_when_source_id_differs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_dataset(root)

            games = pd.read_csv(root / "Games.csv")
            games.loc[:, "game_id"] = "ALT-0001"
            games.to_csv(root / "Games.csv", index=False)

            team_stats = pd.read_csv(root / "TeamStatistics.csv")
            team_stats.loc[:, "game_id"] = "ALT-0001"
            team_stats.to_csv(root / "TeamStatistics.csv", index=False)

            player_stats = pd.read_csv(root / "PlayerStatistics.csv")
            player_stats.loc[:, "game_id"] = "ALT-0001"
            player_stats.to_csv(root / "PlayerStatistics.csv", index=False)

            with self.SessionLocal() as session:
                session.add(
                    self.models.Team(
                        team_id="1610612738",
                        canonical_team_id="1610612738",
                        full_name="Boston Celtics",
                        abbr="BOS",
                        is_legacy=False,
                    )
                )
                session.add(
                    self.models.Team(
                        team_id="1610612747",
                        canonical_team_id="1610612747",
                        full_name="Los Angeles Lakers",
                        abbr="LAL",
                        is_legacy=False,
                    )
                )
                session.add(
                    self.models.Game(
                        game_id="HIST-EXISTING",
                        data_source="kaggle_box_scores",
                        season="21984",
                        game_date=pd.Timestamp("1984-10-05").date(),
                        home_team_id="1610612738",
                        road_team_id="1610612747",
                        home_team_score=102,
                        road_team_score=97,
                    )
                )
                session.commit()

            with self.SessionLocal() as session:
                counts = self.module.backfill_kaggle_historical(session, root)
                session.commit()

            self.assertEqual(counts.games_created, 0)
            self.assertEqual(counts.games_updated, 1)

            with self.SessionLocal() as session:
                game_count = session.query(self.models.Game).count()
                existing = session.get(self.models.Game, "HIST-EXISTING")
                player_stat = session.query(self.models.PlayerGameStats).filter_by(game_id="HIST-EXISTING", player_id="historic-1").one()

            self.assertEqual(game_count, 1)
            self.assertIsNotNone(existing)
            self.assertEqual(existing.home_team_score, 102)
            self.assertEqual(player_stat.team_id, "1610612738")


if __name__ == "__main__":
    unittest.main()
