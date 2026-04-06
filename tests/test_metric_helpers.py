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


class _FakeJoinQuery:
    def __init__(self, rows):
        self._rows = rows

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)


class _FakeSeasonSession(_FakeSession):
    def __init__(self, rows):
        super().__init__()
        self._rows = rows

    def query(self, *args, **kwargs):
        return _FakeJoinQuery(self._rows)


def test_season_pbp_offensive_foul_events_bulk_loads_and_normalizes():
    session = _FakeSeasonSession(
        [
            (
                SimpleNamespace(
                    event_msg_type=6,
                    event_msg_action_type=4,
                    event_num=8,
                    period=1,
                    pc_time="5:11",
                    home_description=None,
                    visitor_description="Westbrook OFF.Foul (P1) (P.Fraher)",
                    neutral_description=None,
                    player1_id="201566",
                    player2_id="200768",
                ),
                SimpleNamespace(game_id="g1", season="22025", game_date=SimpleNamespace(isoformat=lambda: "2025-01-01")),
            ),
            (
                SimpleNamespace(
                    event_msg_type=6,
                    event_msg_action_type=26,
                    event_num=9,
                    period=1,
                    pc_time="4:58",
                    home_description="Ibaka Offensive Charge Foul (P2.T3) (P.Fraher)",
                    visitor_description=None,
                    neutral_description=None,
                    player1_id="201586",
                    player2_id="200782",
                ),
                SimpleNamespace(game_id="g2", season="22025", game_date=SimpleNamespace(isoformat=lambda: "2025-01-02")),
            ),
            (
                SimpleNamespace(
                    event_msg_type=6,
                    event_msg_action_type=1,
                    event_num=10,
                    period=1,
                    pc_time="4:30",
                    home_description="Regular personal foul",
                    visitor_description=None,
                    neutral_description=None,
                    player1_id="1",
                    player2_id="2",
                ),
                SimpleNamespace(game_id="g3", season="22025", game_date=SimpleNamespace(isoformat=lambda: "2025-01-03")),
            ),
        ]
    )

    events = helpers.season_pbp_offensive_foul_events(session, "22025")

    assert events == [
        {
            "game_id": "g1",
            "season": "22025",
            "game_date": "2025-01-01",
            "event_num": 8,
            "period": 1,
            "pc_time": "5:11",
            "description": "Westbrook OFF.Foul (P1) (P.Fraher)",
            "action_type": 4,
            "foul_player_id": "201566",
            "drawn_by_player_id": "200768",
            "is_charge": False,
        },
        {
            "game_id": "g2",
            "season": "22025",
            "game_date": "2025-01-02",
            "event_num": 9,
            "period": 1,
            "pc_time": "4:58",
            "description": "Ibaka Offensive Charge Foul (P2.T3) (P.Fraher)",
            "action_type": 26,
            "foul_player_id": "201586",
            "drawn_by_player_id": "200782",
            "is_charge": True,
        },
    ]


def test_season_pbp_charge_events_uses_cached_bulk_events():
    session = _FakeSession()
    sample_events = [
        {"game_id": "g1", "is_charge": False},
        {"game_id": "g2", "is_charge": True},
        {"game_id": "g3", "is_charge": True},
    ]

    with patch("metrics.helpers.season_pbp_offensive_foul_events", return_value=sample_events) as season_events:
        first = helpers.season_pbp_charge_events(session, "22025")
        second = helpers.season_pbp_charge_events(session, "22025")

    assert season_events.call_count == 1
    assert first == second == [
        {"game_id": "g2", "is_charge": True},
        {"game_id": "g3", "is_charge": True},
    ]
