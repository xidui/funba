from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from metrics import helpers


class _FakeSession:
    def __init__(self):
        self.info = {}


def test_pbp_offensive_foul_events_normalizes_ids_and_text():
    rows = [
        SimpleNamespace(
            event_msg_type=6,
            event_msg_action_type=4,
            event_num=10,
            period=2,
            pc_time="2:31",
            home_description=None,
            visitor_description="Tucker OFF.Foul (P3) (N.Sago)",
            neutral_description=None,
            player1_id="200782",
            player2_id="200768",
        ),
        SimpleNamespace(
            event_msg_type=6,
            event_msg_action_type=26,
            event_num=11,
            period=2,
            pc_time="2:12",
            home_description="Ibaka Offensive Charge Foul (P2.T3) (P.Fraher)",
            visitor_description=None,
            neutral_description=None,
            player1_id="201586",
            player2_id="200782",
        ),
        SimpleNamespace(
            event_msg_type=6,
            event_msg_action_type=1,
            event_num=12,
            period=2,
            pc_time="1:58",
            home_description="Regular personal foul",
            visitor_description=None,
            neutral_description=None,
            player1_id="111",
            player2_id="222",
        ),
    ]
    session = _FakeSession()

    with patch("metrics.helpers.game_pbp_rows", return_value=rows) as game_pbp_rows:
        events = helpers.pbp_offensive_foul_events(session, "game-1")

    assert game_pbp_rows.call_count == 1
    assert events == [
        {
            "game_id": "game-1",
            "event_num": 10,
            "period": 2,
            "pc_time": "2:31",
            "description": "Tucker OFF.Foul (P3) (N.Sago)",
            "action_type": 4,
            "foul_player_id": "200782",
            "drawn_by_player_id": "200768",
            "is_charge": False,
        },
        {
            "game_id": "game-1",
            "event_num": 11,
            "period": 2,
            "pc_time": "2:12",
            "description": "Ibaka Offensive Charge Foul (P2.T3) (P.Fraher)",
            "action_type": 26,
            "foul_player_id": "201586",
            "drawn_by_player_id": "200782",
            "is_charge": True,
        },
    ]


def test_pbp_charge_events_filters_to_charge_subset_and_uses_cache():
    rows = [
        SimpleNamespace(
            event_msg_type=6,
            event_msg_action_type=4,
            event_num=1,
            period=1,
            pc_time="10:00",
            home_description=None,
            visitor_description="OFF.Foul",
            neutral_description=None,
            player1_id="1",
            player2_id="2",
        ),
        SimpleNamespace(
            event_msg_type=6,
            event_msg_action_type=26,
            event_num=2,
            period=1,
            pc_time="9:30",
            home_description="Offensive Charge Foul",
            visitor_description=None,
            neutral_description=None,
            player1_id="3",
            player2_id="4",
        ),
    ]
    session = _FakeSession()

    with patch("metrics.helpers.game_pbp_rows", return_value=rows) as game_pbp_rows:
        first = helpers.pbp_charge_events(session, "game-2")
        second = helpers.pbp_charge_events(session, "game-2")

    assert game_pbp_rows.call_count == 1
    assert first == second == [
        {
            "game_id": "game-2",
            "event_num": 2,
            "period": 1,
            "pc_time": "9:30",
            "description": "Offensive Charge Foul",
            "action_type": 26,
            "foul_player_id": "3",
            "drawn_by_player_id": "4",
            "is_charge": True,
        },
    ]
