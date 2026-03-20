from pathlib import Path
import sys
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from db.backfill_nba_game_line_score import normalize_game_line_score_payload


def test_normalize_game_line_score_payload_regular_game():
    game = SimpleNamespace(game_id="0021700211", home_team_id="1610612740", road_team_id="1610612761")
    payload = {
        "awayTeam": {
            "teamId": 1610612761,
            "score": 125,
            "periods": [
                {"period": 1, "periodType": "REGULAR", "score": 29},
                {"period": 2, "periodType": "REGULAR", "score": 35},
                {"period": 3, "periodType": "REGULAR", "score": 34},
                {"period": 4, "periodType": "REGULAR", "score": 27},
            ],
        },
        "homeTeam": {
            "teamId": 1610612740,
            "score": 116,
            "periods": [
                {"period": 1, "periodType": "REGULAR", "score": 34},
                {"period": 2, "periodType": "REGULAR", "score": 29},
                {"period": 3, "periodType": "REGULAR", "score": 23},
                {"period": 4, "periodType": "REGULAR", "score": 30},
            ],
        },
    }

    rows = normalize_game_line_score_payload(game, payload)
    away = next(row for row in rows if row["team_id"] == "1610612761")
    home = next(row for row in rows if row["team_id"] == "1610612740")

    assert away["on_road"] is True
    assert away["q1_pts"] == 29
    assert away["q4_pts"] == 27
    assert away["first_half_pts"] == 64
    assert away["second_half_pts"] == 61
    assert away["regulation_total_pts"] == 125
    assert away["total_pts"] == 125

    assert home["on_road"] is False
    assert home["q1_pts"] == 34
    assert home["q4_pts"] == 30
    assert home["first_half_pts"] == 63
    assert home["second_half_pts"] == 53
    assert home["regulation_total_pts"] == 116
    assert home["total_pts"] == 116


def test_normalize_game_line_score_payload_ot_overflow_to_json():
    game = SimpleNamespace(game_id="game_ot", home_team_id="home", road_team_id="away")
    payload = {
        "awayTeam": {
            "teamId": "away",
            "score": 132,
            "periods": [
                {"period": 1, "periodType": "REGULAR", "score": 20},
                {"period": 2, "periodType": "REGULAR", "score": 25},
                {"period": 3, "periodType": "REGULAR", "score": 25},
                {"period": 4, "periodType": "REGULAR", "score": 20},
                {"period": 5, "periodType": "OVERTIME", "score": 10},
                {"period": 6, "periodType": "OVERTIME", "score": 12},
                {"period": 7, "periodType": "OVERTIME", "score": 8},
                {"period": 8, "periodType": "OVERTIME", "score": 12},
            ],
        },
        "homeTeam": {
            "teamId": "home",
            "score": 130,
            "periods": [
                {"period": 1, "periodType": "REGULAR", "score": 25},
                {"period": 2, "periodType": "REGULAR", "score": 20},
                {"period": 3, "periodType": "REGULAR", "score": 25},
                {"period": 4, "periodType": "REGULAR", "score": 20},
                {"period": 5, "periodType": "OVERTIME", "score": 10},
                {"period": 6, "periodType": "OVERTIME", "score": 10},
                {"period": 7, "periodType": "OVERTIME", "score": 10},
                {"period": 8, "periodType": "OVERTIME", "score": 10},
            ],
        },
    }

    rows = normalize_game_line_score_payload(game, payload)
    away = next(row for row in rows if row["team_id"] == "away")

    assert away["ot1_pts"] == 10
    assert away["ot2_pts"] == 12
    assert away["ot3_pts"] == 8
    assert away["ot_extra_json"] == "[12]"
