from __future__ import annotations

import sys
import types
import unittest
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from web.live_game_data import build_live_game_stub, fetch_live_game_detail


def _install_live_endpoint_stubs(*, box_payload, pbp_payload):
    fake_nba_api = types.ModuleType("nba_api")
    fake_live = types.ModuleType("nba_api.live")
    fake_live_nba = types.ModuleType("nba_api.live.nba")
    fake_endpoints = types.ModuleType("nba_api.live.nba.endpoints")
    fake_boxscore = types.ModuleType("nba_api.live.nba.endpoints.boxscore")
    fake_playbyplay = types.ModuleType("nba_api.live.nba.endpoints.playbyplay")

    class _FakeBoxScore:
        def __init__(self, game_id):
            self.game_id = game_id

        def get_dict(self):
            return box_payload

    class _FakePlayByPlay:
        def __init__(self, game_id):
            self.game_id = game_id

        def get_dict(self):
            return pbp_payload

    fake_boxscore.BoxScore = _FakeBoxScore
    fake_playbyplay.PlayByPlay = _FakePlayByPlay
    fake_endpoints.boxscore = fake_boxscore
    fake_endpoints.playbyplay = fake_playbyplay
    fake_live_nba.endpoints = fake_endpoints
    fake_live.nba = fake_live_nba
    fake_nba_api.live = fake_live

    sys.modules["nba_api"] = fake_nba_api
    sys.modules["nba_api.live"] = fake_live
    sys.modules["nba_api.live.nba"] = fake_live_nba
    sys.modules["nba_api.live.nba.endpoints"] = fake_endpoints
    sys.modules["nba_api.live.nba.endpoints.boxscore"] = fake_boxscore
    sys.modules["nba_api.live.nba.endpoints.playbyplay"] = fake_playbyplay


class TestLiveGameData(unittest.TestCase):
    def tearDown(self):
        for key in [
            "nba_api",
            "nba_api.live",
            "nba_api.live.nba",
            "nba_api.live.nba.endpoints",
            "nba_api.live.nba.endpoints.boxscore",
            "nba_api.live.nba.endpoints.playbyplay",
        ]:
            sys.modules.pop(key, None)

    def test_fetch_live_game_detail_includes_summary_game_date(self):
        _install_live_endpoint_stubs(
            box_payload={
                "game": {
                    "gameId": "0022501178",
                    "gameStatus": 2,
                    "gameStatusText": "Q2 4:26",
                    "period": 2,
                    "gameClock": "PT04M26.00S",
                    "gameEt": "2026-04-10T21:30:00-04:00",
                    "homeTeam": {
                        "teamId": "1610612745",
                        "score": "61",
                        "statistics": {},
                        "players": [],
                    },
                    "awayTeam": {
                        "teamId": "1610612750",
                        "score": "61",
                        "statistics": {},
                        "players": [],
                    },
                }
            },
            pbp_payload={"game": {"actions": []}},
        )

        payload = fetch_live_game_detail("0022501178")

        self.assertIsNotNone(payload)
        self.assertEqual(payload["summary"]["game_date"], "2026-04-10")
        self.assertEqual(payload["summary"]["status"], "live")

    def test_builds_non_null_stub_date_for_live_detail_summary(self):
        _install_live_endpoint_stubs(
            box_payload={
                "game": {
                    "gameId": "0022501178",
                    "gameStatus": 2,
                    "gameStatusText": "Q2 4:26",
                    "period": 2,
                    "gameClock": "PT04M26.00S",
                    "gameTimeUTC": "2026-04-11T01:30:00Z",
                    "homeTeam": {
                        "teamId": "1610612745",
                        "score": "61",
                        "statistics": {},
                        "players": [],
                    },
                    "awayTeam": {
                        "teamId": "1610612750",
                        "score": "61",
                        "statistics": {},
                        "players": [],
                    },
                }
            },
            pbp_payload={"game": {"actions": []}},
        )

        payload = fetch_live_game_detail("0022501178")
        stub = build_live_game_stub(payload["summary"])

        self.assertIsNotNone(stub)
        self.assertEqual(stub.game_date, date(2026, 4, 11))
