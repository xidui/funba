import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.funba_capture import (  # noqa: E402
    _capture_page_error,
    _game_boxscore_adjustments,
    _game_boxscore_plan,
    _game_metrics_plan,
    _metric_page_plan,
    _player_metrics_plan,
    _player_summary_plan,
    capture_funba_url,
)


class _FakeLocator:
    def __init__(self, text: str):
        self._text = text

    def inner_text(self, timeout=None):
        return self._text


class _FakePage:
    def __init__(self, body_text: str = ""):
        self._body_text = body_text

    def locator(self, selector):
        if selector != "body":
            raise AssertionError(f"unexpected selector: {selector}")
        return _FakeLocator(self._body_text)


class _FakeResponse:
    def __init__(self, status: int | None = None):
        self.status = status


class TestFunbaCapturePlans(unittest.TestCase):
    def test_capture_page_error_detects_http_500(self):
        page = _FakePage("Something Went Wrong")
        self.assertEqual(
            _capture_page_error(page, _FakeResponse(status=500)),
            "Screenshot target returned HTTP 500",
        )

    def test_capture_page_error_detects_rendered_error_page(self):
        page = _FakePage("500\nSomething Went Wrong\nAn unexpected error occurred.\nBack to Home")
        self.assertEqual(
            _capture_page_error(page),
            "Screenshot target rendered a server error page",
        )

    def test_player_summary_plan_prefers_header(self):
        plan = _player_summary_plan()
        self.assertEqual(plan["selectors"], [".player-header"])
        self.assertEqual(plan["max_height"], 420)

    def test_game_boxscore_plan_keeps_player_rows(self):
        plan = _game_boxscore_plan()
        self.assertEqual(plan["selectors"], [".scoreboard", "#bs-team", "#bs-players .box-score-grid"])
        self.assertEqual(plan["selector_height_limits"]["#bs-players .box-score-grid"], 420)
        self.assertEqual(plan["max_height"], 920)

    def test_game_boxscore_adjustments_remove_async_panel_and_trim_tables(self):
        adjustments = _game_boxscore_adjustments()
        self.assertEqual(adjustments["remove_selectors"], ["#game-metrics-panel", "#bs-team .table-wrap:first-child"])
        self.assertEqual(adjustments["limit_table_rows"]["#bs-players tbody"], 4)
        self.assertEqual(adjustments["style_updates"][".sb-chart-wrap"]["height"], "160px")

    def test_metric_page_plan_targets_top_n_row(self):
        plan = _metric_page_plan(top_n=5)
        self.assertEqual(
            plan["selectors"],
            [".detail-title", ".season-select", ".rankings-table thead", ".rankings-table tbody tr:nth-child(5)"],
        )
        self.assertEqual(plan["min_height"], 700)
        self.assertEqual(plan["max_height"], 760)

    def test_game_metrics_plan_widens_for_four_cards(self):
        plan = _game_metrics_plan(cards=4)
        self.assertEqual(plan["selectors"][2], "#game-metrics-panel .game-metrics-grid .gmc:nth-child(4)")
        self.assertEqual(plan["max_width"], 1220)

    def test_player_metrics_plan_defaults_to_four_cards(self):
        plan = _player_metrics_plan()
        self.assertEqual(plan["selectors"][3], ".metrics-section .metrics-grid .metric-card:nth-child(4)")
        self.assertEqual(plan["max_width"], 1180)


class TestFunbaCaptureDispatch(unittest.TestCase):
    @patch("social_media.funba_capture.capture_player_profile")
    def test_dispatch_players_url_to_player_profile(self, capture_mock):
        capture_funba_url("https://funba.app/players/1642843", "/tmp/player.png", wait_ms=1200)
        capture_mock.assert_called_once()

    @patch("social_media.funba_capture.capture_game_boxscore")
    def test_dispatch_games_url_to_game_boxscore(self, capture_mock):
        capture_funba_url("https://funba.app/games/0022501127", "/tmp/game.png", wait_ms=1200)
        capture_mock.assert_called_once()

    @patch("social_media.funba_capture.capture_metric_page")
    def test_dispatch_metric_url_to_metric_page(self, capture_mock):
        capture_funba_url(
            "https://funba.app/metrics/fifty_point_games?season=22025",
            "/tmp/metric.png",
            wait_ms=1200,
        )
        capture_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
