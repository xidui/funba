import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from social_media.funba_capture import (  # noqa: E402
    DEFAULT_BASE_URL,
    _capture_page_error,
    _game_boxscore_adjustments,
    _game_boxscore_plan,
    _game_metrics_plan,
    _metric_page_adjustments,
    _metric_page_plan,
    _player_metrics_adjustments,
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
    def test_default_capture_base_url_is_public_funba(self):
        self.assertEqual(DEFAULT_BASE_URL, "https://funba.app")

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
        self.assertEqual(plan["max_width"], 1220)
        self.assertEqual(plan["max_height"], 920)

    def test_game_boxscore_adjustments_remove_async_panel_and_trim_tables(self):
        adjustments = _game_boxscore_adjustments()
        self.assertIn(".topbar", adjustments["remove_selectors"])
        self.assertIn("#game-metrics-panel", adjustments["remove_selectors"])
        self.assertIn(".card:has(.game-admin-panel)", adjustments["remove_selectors"])
        self.assertEqual(adjustments["limit_table_rows"]["#bs-players tbody"], 4)
        self.assertEqual(adjustments["style_updates"][".sb-chart-wrap"]["height"], "160px")

    def test_metric_page_plan_targets_top_n_row(self):
        plan = _metric_page_plan(top_n=5)
        self.assertEqual(
            plan["selectors"],
            [".detail-title", ".detail-desc", ".rankings-table thead", ".rankings-table tbody tr:nth-child(5)"],
        )
        self.assertEqual(plan["min_height"], 520)
        self.assertEqual(plan["max_height"], 680)

    def test_metric_page_adjustments_trim_switches_search_and_rows(self):
        adjustments = _metric_page_adjustments(top_n=5)
        self.assertIn(".metric-switch", adjustments["remove_selectors"])
        self.assertIn(".season-select", adjustments["remove_selectors"])
        self.assertIn("form[action]", adjustments["remove_selectors"])
        self.assertIn(".rankings-table tbody tr.drilldown-row", adjustments["remove_selectors"])
        self.assertEqual(adjustments["limit_table_rows"][".rankings-table tbody"], 5)

    def test_game_metrics_plan_widens_for_four_cards(self):
        plan = _game_metrics_plan(cards=4)
        self.assertEqual(plan["selectors"][2], "#game-metrics-panel .game-metrics-grid .gmc:nth-child(4)")
        self.assertEqual(plan["max_width"], 1220)

    def test_player_metrics_plan_defaults_to_four_cards(self):
        plan = _player_metrics_plan()
        self.assertEqual(
            plan["selectors"][3],
            ".metrics-section .metrics-columns .metrics-col:first-child .metric-card:nth-child(4)",
        )
        self.assertEqual(plan["max_width"], 980)

    def test_player_metrics_adjustments_keep_single_column_and_limit_cards(self):
        adjustments = _player_metrics_adjustments(scope="season", cards=4)
        self.assertIn(".player-section-nav", adjustments["remove_selectors"])
        self.assertIn(".metrics-section .metrics-columns .metrics-col:nth-child(n+2)", adjustments["remove_selectors"])
        self.assertEqual(
            adjustments["limit_grid_cards"][".metrics-section .metrics-columns .metrics-col:first-child .metrics-grid"],
            4,
        )


class TestFunbaCaptureDispatch(unittest.TestCase):
    @patch("social_media.funba_capture.capture_player_profile")
    def test_dispatch_players_url_to_player_profile(self, capture_mock):
        capture_funba_url("https://funba.app/players/1642843", "/tmp/player.png", wait_ms=1200)
        capture_mock.assert_called_once()

    @patch("social_media.funba_capture.capture_player_profile")
    def test_localhost_player_url_is_rewritten_to_public_by_default(self, capture_mock):
        capture_funba_url("http://localhost:5001/players/1642843", "/tmp/player.png", wait_ms=1200)
        capture_mock.assert_called_once_with(
            "1642843",
            "/tmp/player.png",
            base_url="https://funba.app",
            wait_ms=1200,
        )

    @patch("social_media.funba_capture.capture_player_profile")
    def test_localhost_player_url_can_be_explicitly_allowed(self, capture_mock):
        capture_funba_url(
            "http://localhost:5001/players/1642843",
            "/tmp/player.png",
            wait_ms=1200,
            allow_private_hosts=True,
        )
        capture_mock.assert_called_once_with(
            "1642843",
            "/tmp/player.png",
            base_url="http://localhost:5001",
            wait_ms=1200,
        )

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
