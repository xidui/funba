from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
import unittest

from db.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_LIVE,
    GAME_STATUS_UPCOMING,
    get_game_status,
    infer_game_status,
)


class TestGameStatus(unittest.TestCase):
    def test_infer_game_status_distinguishes_upcoming_live_and_completed(self):
        today = date(2026, 4, 10)

        self.assertEqual(
            infer_game_status(game_date=today + timedelta(days=1), wining_team_id=None, today=today),
            GAME_STATUS_UPCOMING,
        )
        self.assertEqual(
            infer_game_status(
                game_date=today,
                wining_team_id=None,
                home_team_score=88,
                road_team_score=84,
                today=today,
            ),
            GAME_STATUS_LIVE,
        )
        self.assertEqual(
            infer_game_status(game_date=today - timedelta(days=1), wining_team_id="1610612738", today=today),
            GAME_STATUS_COMPLETED,
        )

    def test_get_game_status_prefers_explicit_status(self):
        game = SimpleNamespace(
            game_status="upcoming",
            game_date=date(2026, 4, 8),
            wining_team_id="1610612738",
            home_team_score=120,
            road_team_score=110,
        )
        self.assertEqual(get_game_status(game), GAME_STATUS_UPCOMING)
