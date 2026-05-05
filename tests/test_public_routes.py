from datetime import date
from types import SimpleNamespace

from web.public_routes import (
    _filter_unneeded_playoff_series_placeholders,
    _home_cached_payload_needs_curated_refresh,
)


def _playoff_game(game_id, *, winner=None, status="completed"):
    return SimpleNamespace(
        game_id=game_id,
        season="42025",
        game_date=date(2026, 4, 20 + int(game_id[-1])),
        home_team_id="A",
        road_team_id="B",
        home_team_score=101 if winner else None,
        road_team_score=92 if winner else None,
        wining_team_id=winner,
        game_status=status,
    )


def test_filter_unneeded_playoff_placeholders_drops_games_after_series_decided():
    games = [
        _playoff_game("0042500111", winner="A"),
        _playoff_game("0042500112", winner="A"),
        _playoff_game("0042500113", winner="A"),
        _playoff_game("0042500114", winner="A"),
        _playoff_game("0042500115", status="upcoming"),
        _playoff_game("0042500116", status="upcoming"),
        _playoff_game("0042500117", status="upcoming"),
    ]

    filtered = _filter_unneeded_playoff_series_placeholders(games)

    assert [game.game_id for game in filtered] == [
        "0042500111",
        "0042500112",
        "0042500113",
        "0042500114",
    ]


def test_filter_unneeded_playoff_placeholders_keeps_needed_next_game():
    games = [
        _playoff_game("0042500111", winner="A"),
        _playoff_game("0042500112", winner="A"),
        _playoff_game("0042500113", winner="B"),
        _playoff_game("0042500114", winner="A"),
        _playoff_game("0042500115", winner="B"),
        _playoff_game("0042500116", status="upcoming"),
    ]

    filtered = _filter_unneeded_playoff_series_placeholders(games)

    assert [game.game_id for game in filtered] == [game.game_id for game in games]


def test_home_cached_payload_refreshes_after_curator_writes_json():
    game = SimpleNamespace(
        highlights_curated_json='{"hero":[]}',
        highlights_curated_player_json=None,
        highlights_curated_team_json=None,
    )
    payload = {"game_metrics": {"season": []}, "_curated_merged": False}

    assert _home_cached_payload_needs_curated_refresh(game, payload) is True


def test_home_cached_payload_does_not_refresh_already_merged_payload():
    game = SimpleNamespace(
        highlights_curated_json='{"hero":[]}',
        highlights_curated_player_json=None,
        highlights_curated_team_json=None,
    )
    payload = {"game_metrics": {"season": []}, "_curated_merged": True}

    assert _home_cached_payload_needs_curated_refresh(game, payload) is False


def test_home_cached_payload_does_not_refresh_without_curated_json():
    game = SimpleNamespace(
        highlights_curated_json=None,
        highlights_curated_player_json=None,
        highlights_curated_team_json=None,
    )
    payload = {"game_metrics": {"season": []}, "_curated_merged": False}

    assert _home_cached_payload_needs_curated_refresh(game, payload) is False
