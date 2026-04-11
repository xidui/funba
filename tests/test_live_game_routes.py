from __future__ import annotations

import unittest
from unittest.mock import patch

import web.app as web_app


class TestLiveGameRoutes(unittest.TestCase):
    def setUp(self):
        web_app.app.config["TESTING"] = True
        self.client = web_app.app.test_client()

    def test_api_games_live_returns_scoreboard_payload(self):
        payload = {
            "0022500999": {
                "game_id": "0022500999",
                "status": "live",
                "summary": "Q3 4:22",
                "road_score": 101,
                "home_score": 99,
                "road_team_id": "1610612747",
                "home_team_id": "1610612738",
            }
        }

        with patch("web.public_routes.fetch_live_scoreboard_map", return_value=payload):
            response = self.client.get("/api/games/live")

        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertEqual(body["games"][0]["game_id"], "0022500999")
        self.assertEqual(body["games"][0]["summary"], "Q3 4:22")

    def test_api_game_live_returns_503_when_live_data_is_unavailable(self):
        with patch("web.public_routes.fetch_live_game_detail", return_value=None):
            response = self.client.get("/api/games/0022500999/live")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "live_data_unavailable")
