from __future__ import annotations

from datetime import date
from types import SimpleNamespace

from metrics.highlights.curator import _scope_reference_context, _window_label, build_game_context
from metrics.highlights.prefilter import build_llm_input


def _game(season: str):
    return SimpleNamespace(
        game_id="0042500133",
        season=season,
        game_date=date(2026, 4, 23),
        home_team_id="1610612761",
        road_team_id="1610612739",
        wining_team_id="1610612761",
        home_team_score=126,
        road_team_score=104,
    )


def test_build_game_context_labels_playoff_season_for_llm_wording():
    ctx = build_game_context(
        _game("42025"),
        {
            "1610612761": "Toronto Raptors",
            "1610612739": "Cleveland Cavaliers",
        },
    )

    assert ctx["season_phase"] == "playoffs"
    assert ctx["season_phase_en"] == "playoffs"
    assert ctx["season_reference_en"] == "this postseason"
    assert ctx["season_reference_zh"] == "本届季后赛"


def test_build_game_context_keeps_regular_season_this_season_wording():
    ctx = build_game_context(
        _game("22025"),
        {
            "1610612761": "Toronto Raptors",
            "1610612739": "Cleveland Cavaliers",
        },
    )

    assert ctx["season_phase"] == "regular"
    assert ctx["season_phase_en"] == "regular season"
    assert ctx["season_reference_en"] == "this season"
    assert ctx["season_reference_zh"] == "本赛季"


def test_scope_reference_context_distinguishes_playoff_windows():
    assert _scope_reference_context("wins_by_10_plus", "42025") == {
        "scope_window": "season",
        "scope_phase": "playoffs",
        "scope_reference_zh": "本届季后赛",
        "scope_reference_en": "this postseason",
    }
    assert _scope_reference_context("wins_by_10_plus_career", "all_playoffs") == {
        "scope_window": "career",
        "scope_phase": "playoffs",
        "scope_reference_zh": "季后赛历史",
        "scope_reference_en": "playoff history",
    }
    assert _scope_reference_context("wins_by_10_plus_last5", "last5_playoffs") == {
        "scope_window": "last5",
        "scope_phase": "playoffs",
        "scope_reference_zh": "过去5届季后赛",
        "scope_reference_en": "past 5 playoff seasons",
    }


def test_scope_reference_context_distinguishes_regular_windows():
    assert _scope_reference_context("wins_by_10_plus", "22025")["scope_reference_en"] == "this season"
    assert _scope_reference_context("wins_by_10_plus_career", "all_regular")["scope_reference_en"] == (
        "regular-season history"
    )
    assert _scope_reference_context("wins_by_10_plus_last3", "last3_regular")["scope_reference_en"] == (
        "past 3 regular seasons"
    )


def test_window_label_reads_synthetic_last_season_tokens():
    assert _window_label("wins_by_10_plus", "last5_playoffs") == "last5"
    assert _window_label("wins_by_10_plus", "last3_regular") == "last3"


def test_game_llm_input_preserves_scope_reference_fields():
    payload = build_llm_input(
        [
            {
                "metric_key": "game_total_steals",
                "metric_name": "Game Total Steals",
                "metric_window": "last5",
                "scope_window": "last5",
                "scope_phase": "playoffs",
                "scope_reference_zh": "过去5届季后赛",
                "scope_reference_en": "past 5 playoff seasons",
                "season": "last5_playoffs",
                "entity_id": "0042500133",
                "value_num": 25,
                "rank": 2,
                "total": 353,
            }
        ]
    )

    assert payload[0]["scope_reference_zh"] == "过去5届季后赛"
    assert payload[0]["scope_reference_en"] == "past 5 playoff seasons"
