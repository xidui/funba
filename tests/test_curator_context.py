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


def test_dedupe_triggered_cards_keeps_one_card_per_window():
    """Pre-fix bug: _dedupe_triggered_cards collapsed every window of the
    same metric family into one winner, so an entity with milestones in
    season + career + last3 + last5 ended up as a single candidate. The
    other windows' stories never reached the LLM."""
    from web.app import _dedupe_triggered_cards

    cards = [
        {
            "metric_key": "wins_by_10_plus",
            "season": "42025",
            "entity_type": "team",
            "entity_id": "DEN",
            "source": "milestone",
            "severity": 0.5,
            "event_type": "approaching_target",
        },
        {
            "metric_key": "wins_by_10_plus_career",
            "season": "all_playoffs",
            "entity_type": "team",
            "entity_id": "DEN",
            "source": "milestone",
            "severity": 0.6,
            "event_type": "approaching_target",
        },
        {
            "metric_key": "wins_by_10_plus_last3",
            "season": "last3_playoffs",
            "entity_type": "team",
            "entity_id": "DEN",
            "source": "milestone",
            "severity": 0.6,
            "event_type": "approaching_target",
        },
        {
            "metric_key": "wins_by_10_plus_last5",
            "season": "last5_playoffs",
            "entity_type": "team",
            "entity_id": "DEN",
            "source": "milestone",
            "severity": 0.6,
            "event_type": "approaching_target",
        },
    ]

    result = _dedupe_triggered_cards(cards, game_id="0042500165")
    assert len(result) == 4
    assert sorted(c["metric_key"] for c in result) == [
        "wins_by_10_plus",
        "wins_by_10_plus_career",
        "wins_by_10_plus_last3",
        "wins_by_10_plus_last5",
    ]


def test_dedupe_triggered_cards_collapses_within_same_window():
    """Within one window, multiple events of the same family still collapse
    to a single winner (otherwise an approaching_target + approaching_absolute
    pair from one detection round would both surface as separate cards)."""
    from web.app import _dedupe_triggered_cards

    cards = [
        {
            "metric_key": "wins_by_10_plus_career",
            "season": "all_playoffs",
            "entity_type": "team",
            "entity_id": "DEN",
            "source": "milestone",
            "severity": 0.5,
            "event_type": "approaching_target",
            "rank": 7,
            "value_num": 51,
        },
        {
            "metric_key": "wins_by_10_plus_career",
            "season": "all_playoffs",
            "entity_type": "team",
            "entity_id": "DEN",
            "source": "milestone",
            "severity": 0.7,
            "event_type": "absolute_threshold",
            "rank": 7,
            "value_num": 51,
        },
    ]
    result = _dedupe_triggered_cards(cards, game_id="0042500165")
    assert len(result) == 1
    # Higher severity / event_priority wins.
    assert result[0]["event_type"] == "absolute_threshold"


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
