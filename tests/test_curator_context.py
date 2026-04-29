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


def test_hero_cooldown_factor_curve():
    """Decay is gentle: 0.3 floor on day 0, linear ramp to 1.0 on day 14."""
    from web.app import _hero_cooldown_factor

    # No prior airing → full strength.
    assert _hero_cooldown_factor(None) == 1.0
    # Day 0 / negative → floor.
    assert _hero_cooldown_factor(0) == 0.3
    assert _hero_cooldown_factor(-1) == 0.3
    # Day 14+ → fully restored.
    assert _hero_cooldown_factor(14) == 1.0
    assert _hero_cooldown_factor(30) == 1.0
    # Linear in the middle, half-ish at day 7.
    f7 = _hero_cooldown_factor(7)
    assert 0.6 < f7 < 0.7
    # Monotonic non-decreasing.
    seq = [_hero_cooldown_factor(d) for d in range(0, 15)]
    assert seq == sorted(seq)


def test_window_class_for_card_long_vs_season():
    from web.app import _window_class_for_card

    assert _window_class_for_card({"metric_key": "wins_by_10_plus_career", "season": "all_playoffs"}) == "long"
    assert _window_class_for_card({"metric_key": "wins_by_10_plus_last5", "season": "last5_playoffs"}) == "long"
    assert _window_class_for_card({"metric_key": "wins_by_10_plus_last3", "season": "last3_playoffs"}) == "long"
    # Concrete-season card → its own bucket so this-season-leader stories
    # aren't muted by an unrelated career-window airing.
    assert _window_class_for_card({"metric_key": "wins_by_10_plus", "season": "42025"}) == "season"
    # Base metric_key paired with a window season (curator quirk) still
    # gets classified by the season prefix.
    assert _window_class_for_card({"metric_key": "season_total_assists", "season": "all_playoffs"}) == "long"
    # Game-scope card with empty season → season bucket (cooldown won't
    # match across games anyway since entity_id == game_id changes).
    assert _window_class_for_card({"metric_key": "game_total_blocks", "season": None}) == "season"


def test_finalize_triggered_result_keeps_career_card_under_pressure():
    """Layer 1 (the 60-card cut) used to flat-sort by best_ratio, which let
    65 low-ratio this-season cards crowd out a single career milestone with
    a mediocre ratio. After the tier-aware sort the career card must
    survive into the top 60 even when this-season cards have lower
    best_ratio numbers."""
    from web.app import _finalize_triggered_result

    cards = []
    for i in range(65):
        cards.append({
            "metric_key": f"this_season_metric_{i}",
            "season": "42025",
            "entity_type": "player",
            "entity_id": f"p{i}",
            "best_ratio": 0.001,  # very strong percentile, would dominate flat sort
        })
    cards.append({
        "metric_key": "career_three_pm_games_career",
        "season": "all_playoffs",
        "entity_type": "player",
        "entity_id": "p_star",
        "best_ratio": 0.05,  # mediocre ratio but tier-0
    })

    result = _finalize_triggered_result({"player": cards, "team": []}, game_id="g1")
    keys = {c["metric_key"] for c in result["player"]}
    assert "career_three_pm_games_career" in keys
    # And it should be sorted to the front of the player list.
    assert result["player"][0]["metric_key"] == "career_three_pm_games_career"


def test_card_window_tier_orders_career_first():
    from web.app import _card_window_tier

    assert _card_window_tier({"metric_key": "x_career", "season": "all_playoffs"}) == 0
    assert _card_window_tier({"metric_key": "x_last10", "season": "last10_playoffs"}) == 1
    assert _card_window_tier({"metric_key": "x_last5", "season": "last5_playoffs"}) == 2
    assert _card_window_tier({"metric_key": "x_last3", "season": "last3_playoffs"}) == 3
    assert _card_window_tier({"metric_key": "x", "season": "42025"}) == 4
    assert _card_window_tier({"metric_key": "game_total_blocks", "season": None}) == 4
    # Base metric_key paired with window season → still pick up tier from season.
    assert _card_window_tier({"metric_key": "season_total_assists", "season": "all_playoffs"}) == 0


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
