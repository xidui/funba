import sys
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _make_app_module():
    fake_engine = MagicMock()
    fake_user_cls = MagicMock()
    fake_user_cls.__name__ = "User"

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award", "Feedback", "Game", "GameLineScore", "GamePlayByPlay", "MagicToken",
        "MetricComputeRun", "MetricDefinition", "MetricResult", "MetricRunLog", "PageView",
        "Player", "PlayerGameStats", "PlayerSalary", "ShotRecord", "Team", "TeamGameStats", "SocialPost", "SocialPostVariant", "SocialPostDelivery",
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


class TestPbpEventTypeLabels(unittest.TestCase):
    def setUp(self):
        self.web_app = _make_app_module()

    def test_known_event_type_uses_human_label(self):
        self.assertEqual(self.web_app._pbp_event_type_label(3), "Free Throw")

    def test_unknown_event_type_falls_back_to_raw_number(self):
        self.assertEqual(self.web_app._pbp_event_type_label(99), "99")

    def test_missing_event_type_uses_placeholder(self):
        self.assertEqual(self.web_app._pbp_event_type_label(None), "-")


if __name__ == "__main__":
    unittest.main()
