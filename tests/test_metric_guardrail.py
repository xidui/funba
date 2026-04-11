from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from metrics.framework import runner


class TestMetricGuardrail(unittest.TestCase):
    def test_run_delta_only_skips_non_completed_games(self):
        session = MagicMock()
        game = SimpleNamespace(
            game_id="0022500999",
            season="22025",
            game_status="live",
            wining_team_id=None,
            home_team_score=88,
            road_team_score=84,
            home_team_id="1610612738",
            road_team_id="1610612747",
        )
        query = MagicMock()
        query.filter.return_value.first.return_value = game
        session.query.return_value = query

        with patch("metrics.framework.runner.get_metric") as get_metric:
            produced = runner.run_delta_only(session, game.game_id, "custom_metric", commit=False)

        self.assertFalse(produced)
        get_metric.assert_not_called()
        session.commit.assert_not_called()
