import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import scripts.funba_hupu_publish as hupu_publish  # noqa: E402
import scripts.funba_reddit_publish as reddit_publish  # noqa: E402
import scripts.funba_xiaohongshu_publish as xhs_publish  # noqa: E402


class TestPublishAgeGuards(unittest.TestCase):
    def test_hupu_date_only_source_date_uses_end_of_local_day(self):
        age = hupu_publish._source_date_age_hours(
            "2026-04-06",
            now_utc=datetime.fromisoformat("2026-04-07T08:51:00"),
        )
        self.assertIsNotNone(age)
        self.assertLess(age, 2.0)

    def test_xiaohongshu_date_only_source_date_uses_end_of_local_day(self):
        age = xhs_publish._source_date_age_hours(
            "2026-04-06",
            now_utc=datetime.fromisoformat("2026-04-07T08:51:00"),
        )
        self.assertIsNotNone(age)
        self.assertLess(age, 2.0)

    def test_reddit_date_only_source_date_uses_end_of_local_day(self):
        age = reddit_publish._source_date_age_hours(
            "2026-04-06",
            now_utc=datetime.fromisoformat("2026-04-07T08:51:00"),
        )
        self.assertIsNotNone(age)
        self.assertLess(age, 2.0)

    @patch("scripts.funba_hupu_publish._source_date_age_hours", return_value=1.85)
    def test_hupu_preflight_allows_fresh_publish(self, _age_mock):
        error = hupu_publish._preflight_publish_guard_error(
            {"id": 188, "status": "approved", "source_date": "2026-04-06"},
            {"id": 388},
        )
        self.assertIsNone(error)

    @patch("scripts.funba_reddit_publish._source_date_age_hours", return_value=1.85)
    def test_reddit_preflight_allows_fresh_publish(self, _age_mock):
        error = reddit_publish._preflight_publish_guard_error(
            {"id": 197, "status": "approved", "source_date": "2026-04-07"},
            {"id": 422},
        )
        self.assertIsNone(error)


if __name__ == "__main__":
    unittest.main()
