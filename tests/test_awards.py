import importlib
import sys
import types
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _import_web_helpers():
    original_db = sys.modules.get("db")
    original_db_models = sys.modules.get("db.models")
    original_backfill = sys.modules.get("db.backfill_nba_player_shot_detail")
    original_line = sys.modules.get("db.backfill_nba_game_line_score")
    fake_engine = MagicMock()

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award",
        "Feedback",
        "Game",
        "GamePlayByPlay",
        "MetricComputeRun",
        "MetricJobClaim",
        "MetricDefinition",
        "MetricResult",
        "MetricRunLog",
        "PageView",
        "Player",
        "PlayerGameStats",
        "ShotRecord",
        "Team",
        "TeamGameStats",
        "GameLineScore",
        "MagicToken",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.User = MagicMock()
    fake_models.engine = fake_engine
    sys.modules["db.models"] = fake_models

    fake_db = types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record = MagicMock()
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    fake_backfill.is_game_shot_back_filled = MagicMock(return_value=False)
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    fake_line = types.ModuleType("db.backfill_nba_game_line_score")
    fake_line.back_fill_game_line_score = MagicMock()
    fake_line.has_game_line_score = MagicMock(return_value=False)
    fake_line.normalize_game_line_score_payload = MagicMock()
    sys.modules["db.backfill_nba_game_line_score"] = fake_line

    for key in list(sys.modules):
        if key == "web.app" or key.startswith("web.app."):
            del sys.modules[key]

    from web.app import _award_badge_label, _group_award_entries

    if original_db is not None:
        sys.modules["db"] = original_db
    else:
        sys.modules.pop("db", None)

    if original_db_models is not None:
        sys.modules["db.models"] = original_db_models
    else:
        sys.modules.pop("db.models", None)

    if original_backfill is not None:
        sys.modules["db.backfill_nba_player_shot_detail"] = original_backfill
    else:
        sys.modules.pop("db.backfill_nba_player_shot_detail", None)

    if original_line is not None:
        sys.modules["db.backfill_nba_game_line_score"] = original_line
    else:
        sys.modules.pop("db.backfill_nba_game_line_score", None)

    return _award_badge_label, _group_award_entries


def _load_real_db_models():
    sys.modules.pop("db.models", None)
    real_models = importlib.import_module("db.models")
    if "db" in sys.modules:
        sys.modules["db"].models = real_models
    return real_models


def _load_real_backfill_awards():
    _load_real_db_models()
    sys.modules.pop("db.backfill_awards", None)
    return importlib.import_module("db.backfill_awards")


class TestAwardsBackfillHelpers(unittest.TestCase):
    def _make_session(self):
        Base = _load_real_db_models().Base

        engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        SessionLocal = sessionmaker(bind=engine)
        return SessionLocal()

    def test_season_text_to_award_season_supports_api_and_db_formats(self):
        from db.backfill_awards import _season_text_to_award_season

        self.assertEqual(_season_text_to_award_season("2024-25"), 22024)
        self.assertEqual(_season_text_to_award_season("22024"), 22024)
        self.assertIsNone(_season_text_to_award_season("202425"))

    def test_player_award_row_maps_all_nba_and_mvp_variants(self):
        from db.backfill_awards import _player_award_seed_from_row

        team_lookup = {
            "goldenstatewarriors": [
                SimpleNamespace(
                    team_id="1610612744",
                    full_name="Golden State Warriors",
                    is_legacy=False,
                    start_season="1946",
                    end_season=None,
                )
            ]
        }

        all_nba_seed = _player_award_seed_from_row(
            {
                "PERSON_ID": 201939,
                "TEAM": "Golden State Warriors",
                "DESCRIPTION": "All-NBA",
                "ALL_NBA_TEAM_NUMBER": "1",
                "SEASON": "2020-21",
                "SUBTYPE2": "KIANT",
            },
            team_lookup,
        )
        mvp_seed = _player_award_seed_from_row(
            {
                "PERSON_ID": 201939,
                "TEAM": "Golden State Warriors",
                "DESCRIPTION": "NBA Most Valuable Player",
                "ALL_NBA_TEAM_NUMBER": None,
                "SEASON": "2015-16",
                "SUBTYPE2": "KIMVP",
            },
            team_lookup,
        )

        self.assertEqual(all_nba_seed.award_type, "all_nba_first")
        self.assertEqual(all_nba_seed.team_id, "1610612744")
        self.assertEqual(all_nba_seed.season, 22020)
        self.assertEqual(mvp_seed.award_type, "mvp")
        self.assertEqual(mvp_seed.season, 22015)

    def test_upsert_award_matches_player_awards_by_player_key(self):
        backfill_awards = _load_real_backfill_awards()
        AwardSeed = backfill_awards.AwardSeed
        _upsert_award = backfill_awards._upsert_award
        Award = _load_real_db_models().Award

        with self._make_session() as session:
            session.add(Award(award_type="mvp", season=22015, player_id="201939", team_id=None, notes=None))
            session.commit()

            action = _upsert_award(
                session,
                AwardSeed(
                    award_type="mvp",
                    season=22015,
                    player_id="201939",
                    team_id="1610612744",
                    notes="Unanimous MVP",
                ),
            )
            session.flush()

            stored = session.query(Award).filter_by(award_type="mvp", season=22015, player_id="201939").one()
            self.assertEqual(action, "updated")
            self.assertEqual(session.query(Award).count(), 1)
            self.assertEqual(stored.team_id, "1610612744")
            self.assertEqual(stored.notes, "Unanimous MVP")

    def test_upsert_award_matches_team_awards_by_team_key(self):
        backfill_awards = _load_real_backfill_awards()
        AwardSeed = backfill_awards.AwardSeed
        _upsert_award = backfill_awards._upsert_award
        Award = _load_real_db_models().Award

        with self._make_session() as session:
            session.add(
                Award(
                    award_type="champion",
                    season=22022,
                    player_id=None,
                    team_id="1610612744",
                    notes="Final game: 0042200610",
                )
            )
            session.commit()

            action = _upsert_award(
                session,
                AwardSeed(
                    award_type="champion",
                    season=22022,
                    player_id=None,
                    team_id="1610612744",
                    notes="Final game: 0042200611",
                ),
            )
            session.flush()

            stored = session.query(Award).filter_by(award_type="champion", season=22022, team_id="1610612744").one()
            self.assertEqual(action, "updated")
            self.assertEqual(session.query(Award).count(), 1)
            self.assertEqual(stored.notes, "Final game: 0042200611")


class TestAwardsDisplayHelpers(unittest.TestCase):
    def setUp(self):
        self.award_badge_label, self.group_award_entries = _import_web_helpers()

    def test_badge_labels_match_player_profile_copy(self):
        self.assertEqual(self.award_badge_label("all_nba_first"), "1st Team")
        self.assertEqual(self.award_badge_label("champion"), "Champion")

    def test_group_award_entries_marks_dynasties_and_repeat_streaks(self):
        entries = [
            {
                "award_type": "champion",
                "season": 22020,
                "season_label": "2020-21",
                "player_id": None,
                "player_name": None,
                "player_headshot_url": None,
                "team_id": "1610612747",
                "team_abbr": "LAL",
                "team_name": "Los Angeles Lakers",
                "notes": None,
                "winner_key": "1610612747",
                "streak": None,
            },
            {
                "award_type": "champion",
                "season": 22019,
                "season_label": "2019-20",
                "player_id": None,
                "player_name": None,
                "player_headshot_url": None,
                "team_id": "1610612747",
                "team_abbr": "LAL",
                "team_name": "Los Angeles Lakers",
                "notes": None,
                "winner_key": "1610612747",
                "streak": None,
            },
            {
                "award_type": "mvp",
                "season": 22015,
                "season_label": "2015-16",
                "player_id": "201939",
                "player_name": "Stephen Curry",
                "player_headshot_url": None,
                "team_id": "1610612744",
                "team_abbr": "GSW",
                "team_name": "Golden State Warriors",
                "notes": None,
                "winner_key": "201939",
                "streak": None,
            },
            {
                "award_type": "mvp",
                "season": 22014,
                "season_label": "2014-15",
                "player_id": "201939",
                "player_name": "Stephen Curry",
                "player_headshot_url": None,
                "team_id": "1610612744",
                "team_abbr": "GSW",
                "team_name": "Golden State Warriors",
                "notes": None,
                "winner_key": "201939",
                "streak": None,
            },
            {
                "award_type": "all_nba_first",
                "season": 22020,
                "season_label": "2020-21",
                "player_id": "201939",
                "player_name": "Stephen Curry",
                "player_headshot_url": None,
                "team_id": "1610612744",
                "team_abbr": "GSW",
                "team_name": "Golden State Warriors",
                "notes": None,
                "winner_key": "201939",
                "streak": None,
            },
            {
                "award_type": "all_nba_first",
                "season": 22019,
                "season_label": "2019-20",
                "player_id": "201939",
                "player_name": "Stephen Curry",
                "player_headshot_url": None,
                "team_id": "1610612744",
                "team_abbr": "GSW",
                "team_name": "Golden State Warriors",
                "notes": None,
                "winner_key": "201939",
                "streak": None,
            },
        ]

        sections = self.group_award_entries(entries)
        champion_section = next(section for section in sections if section["award_type"] == "champion")
        mvp_section = next(section for section in sections if section["award_type"] == "mvp")
        all_nba_section = next(section for section in sections if section["award_type"] == "all_nba_first")

        self.assertTrue(champion_section["groups"][0]["is_dynasty"])
        self.assertEqual(champion_section["groups"][0]["streak"], 2)
        self.assertEqual(mvp_section["groups"][0]["streak"], 2)
        self.assertEqual(all_nba_section["groups"][0]["entries"][0]["streak"], 2)


if __name__ == "__main__":
    unittest.main()
