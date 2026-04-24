from types import SimpleNamespace

from web.public_routes import _home_cached_payload_needs_curated_refresh


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
