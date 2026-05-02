"""Regression tests for the hero-poster prompt builder helpers.

Pre-fix bug: build_prompt_context pulled metric_key from one card field
and season from another with separate fallback chains, so a curator card
with metric_key=*_last5 + season=all_playoffs produced an empty leaderboard
(the *_last5 variant has no rows under all_*). The defensive coerce now
rewrites the variant suffix to match the season's family if the original
pair has no rows.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest


def test_strip_variant_suffix_handles_all_three_windows():
    from social_media.hero_poster import _strip_variant_suffix

    assert _strip_variant_suffix("wins_by_10_plus_career") == "wins_by_10_plus"
    assert _strip_variant_suffix("wins_by_10_plus_last3") == "wins_by_10_plus"
    assert _strip_variant_suffix("wins_by_10_plus_last5") == "wins_by_10_plus"
    # Already a base key — no-op.
    assert _strip_variant_suffix("wins_by_10_plus") == "wins_by_10_plus"
    # A metric whose name happens to contain the substring "_last3" but isn't
    # a variant. This isn't currently a real metric, but the helper should be
    # narrow enough to only strip a TRAILING variant token.
    assert _strip_variant_suffix("season_total_points") == "season_total_points"


def test_coerce_swaps_suffix_when_pair_has_no_rows():
    """The original (metric_key, season) has no MetricResult rows, but the
    season's family has rows under a sibling variant — swap to that variant."""
    from social_media.hero_poster import _coerce_metric_key_for_season

    def fake_has_any_result(session, key, season):
        if (key, season) == ("wins_by_10_plus_last5", "all_playoffs"):
            return False  # the mismatched pair the curator handed us
        if (key, season) == ("wins_by_10_plus_career", "all_playoffs"):
            return True   # the family member that *does* have rows
        return False

    with patch("social_media.hero_poster._has_any_result", side_effect=fake_has_any_result):
        out = _coerce_metric_key_for_season(None, "wins_by_10_plus_last5", "all_playoffs")

    assert out == "wins_by_10_plus_career"


def test_coerce_passes_through_when_pair_has_rows():
    """No mutation needed if the original pair already has rows."""
    from social_media.hero_poster import _coerce_metric_key_for_season

    with patch("social_media.hero_poster._has_any_result", return_value=True):
        out = _coerce_metric_key_for_season(None, "wins_by_10_plus_last5", "last5_playoffs")
    assert out == "wins_by_10_plus_last5"


def test_coerce_dispatches_each_window_family():
    """Each window prefix maps to its own variant suffix."""
    from social_media.hero_poster import _coerce_metric_key_for_season

    def make_fake(allowed_pair):
        def _fake(session, key, season):
            return (key, season) == allowed_pair
        return _fake

    cases = [
        # (original_key, season, expected_swap_target)
        ("wins_by_10_plus", "all_regular", "wins_by_10_plus_career"),
        ("wins_by_10_plus", "last3_regular", "wins_by_10_plus_last3"),
        ("wins_by_10_plus", "last5_playoffs", "wins_by_10_plus_last5"),
    ]
    for original_key, season, expected in cases:
        with patch(
            "social_media.hero_poster._has_any_result",
            side_effect=make_fake((expected, season)),
        ):
            assert _coerce_metric_key_for_season(None, original_key, season) == expected


def test_coerce_returns_original_when_no_swap_works():
    """If neither the original pair nor any sibling has rows, give up
    rather than silently picking a wrong variant."""
    from social_media.hero_poster import _coerce_metric_key_for_season

    with patch("social_media.hero_poster._has_any_result", return_value=False):
        out = _coerce_metric_key_for_season(None, "wins_by_10_plus_last5", "all_playoffs")
    # Original returned unchanged — caller's leaderboard query will be empty,
    # which is the honest signal that nothing fits.
    assert out == "wins_by_10_plus_last5"


def test_pick_rank_window_uses_explicit_field_first():
    """The curator now emits rank_window directly. New entries should
    bypass all inference layers and use the explicit value."""
    from social_media.hero_poster import _pick_rank_window

    assert _pick_rank_window({"rank_window": "alltime"}) == "alltime"
    assert _pick_rank_window({"rank_window": "last5"}) == "last5"
    # Bogus value falls through to inference (then to season default).
    assert _pick_rank_window({"rank_window": "garbage"}) == "season"


def test_pick_rank_window_uses_metric_key_suffix_for_siblings():
    """Sibling metric variants encode the window in the key — deterministic
    even without an explicit rank_window field. Mirrors curator's
    `_window_label`."""
    from social_media.hero_poster import _pick_rank_window

    assert _pick_rank_window({"metric_key": "wins_total_career"}) == "alltime"
    assert _pick_rank_window({"metric_key": "wins_by_10_plus_last3"}) == "last3"
    assert _pick_rank_window({"metric_key": "foo_last5"}) == "last5"
    assert _pick_rank_window({"metric_key": "foo_last10"}) == "last10"
    # Season-token form: base key + virtual season.
    assert _pick_rank_window({"metric_key": "foo", "season": "all_playoffs"}) == "alltime"
    assert _pick_rank_window({"metric_key": "foo", "season": "last3_regular"}) == "last3"


def test_pick_rank_window_falls_back_to_narrative_for_legacy_entries():
    """Legacy curated entries (generated before curator emitted
    rank_window) leave the field empty. Narrative regex covers them so
    yesterday's posts in production still resolve correctly."""
    from social_media.hero_poster import _pick_rank_window

    cases_alltime = [
        "Jonathan Kuminga had -44, 5th-worst in playoff history.",
        "Knicks ripped off a 57-10 run, 1st in playoff history",
        "Player X is now 12th all-time in blocks.",
    ]
    for narr in cases_alltime:
        assert _pick_rank_window({"narrative_en": narr}) == "alltime", narr

    cases_season = [
        "Boston shot 26.1% in the third, 3rd-lowest this postseason",
        "The teams combined for 20 blocks, No. 1 this postseason",
    ]
    for narr in cases_season:
        assert _pick_rank_window({"narrative_en": narr}) == "season", narr

    # Chinese narratives also signal alltime via "史" markers.
    assert _pick_rank_window({"narrative_zh": "库明加正负值-44，季后赛史单场第5差。"}) == "alltime"

    # last/past N — both phrasings.
    assert _pick_rank_window({"narrative_en": "PHI moved within 1 of 17th in wins by 10+ over the past 3 playoff seasons."}) == "last3"
    assert _pick_rank_window({"narrative_en": "3rd-lowest in last 5 playoff seasons"}) == "last5"


def test_pick_rank_window_priority_order():
    """When multiple signals are present, explicit > metric_key > narrative.
    A curator that emits rank_window=season for a `*_career` metric wins
    over the suffix — trust the explicit field, that's the whole point."""
    from social_media.hero_poster import _pick_rank_window

    # Explicit beats both the suffix and the narrative.
    card = {
        "rank_window": "season",
        "metric_key": "wins_total_career",
        "narrative_en": "playoff history",
    }
    assert _pick_rank_window(card) == "season"

    # Suffix beats narrative when explicit is absent.
    card = {
        "metric_key": "wins_total_career",
        "narrative_en": "this postseason",
    }
    assert _pick_rank_window(card) == "alltime"


def test_pick_rank_window_defaults_to_season_when_unknown():
    """Minimal cards (admin asset preview) carry no rank context — fall
    back to single-season behavior rather than guessing."""
    from social_media.hero_poster import _pick_rank_window

    assert _pick_rank_window({}) == "season"
    assert _pick_rank_window({"narrative_en": "Random sentence with no window markers."}) == "season"


def test_window_season_label_and_title_line_2():
    """Header labels track the leaderboard's actual scope, not the
    trigger's literal season — pre-fix, an alltime-triggered card said
    "2025-26 NBA Playoffs" while the headline read "playoff history"."""
    from social_media.hero_poster import _window_season_label, _window_title_line_2

    assert _window_season_label("season", "42025") == "2025-26 NBA Playoffs"
    assert _window_season_label("alltime", "42025") == "All-Time NBA Playoffs"
    assert _window_season_label("last5", "42025") == "Last 5 NBA Playoffs"
    assert _window_season_label("last10", "42025") == "Last 10 NBA Playoffs"
    # Regular season stage.
    assert _window_season_label("season", "22025") == "2025-26 NBA Regular Season"
    assert _window_season_label("alltime", "22025") == "All-Time NBA Regular Season"

    assert _window_title_line_2("season", "42025", 10, "PLAYOFFS") == "2025-26 NBA PLAYOFFS · TOP 10"
    assert _window_title_line_2("alltime", "42025", 10, "PLAYOFFS") == "ALL-TIME NBA PLAYOFFS · TOP 10"
    assert _window_title_line_2("last3", "42025", 10, "PLAYOFFS") == "LAST 3 NBA PLAYOFFS · TOP 10"


def test_apply_window_season_filter_routes_each_window():
    """Each window dispatches to the correct SQL clause."""
    from unittest.mock import MagicMock
    from social_media.hero_poster import _apply_window_season_filter

    for window, season, recent in [
        ("alltime", "42025", []),
        ("last5", "42025", ["42025", "42024", "42023", "42022", "42021"]),
        ("season", "42025", []),
    ]:
        query = MagicMock()
        query.filter.return_value = "FILTERED"
        result = _apply_window_season_filter(query, window=window, season=season, recent_seasons=recent)
        assert result == "FILTERED"
        assert query.filter.call_count == 1


def test_pick_rank_window_matches_hyphenated_regular_season_history():
    """Curator's scope_reference_en uses the hyphenated form
    `regular-season history`, so the legacy narrative-fallback regex must
    match both spelling variants — otherwise regular-season alltime
    narratives silently degrade to a single-season leaderboard."""
    from social_media.hero_poster import _pick_rank_window

    assert _pick_rank_window({"narrative_en": "5th in regular-season history"}) == "alltime"
    assert _pick_rank_window({"narrative_en": "5th in regular season history"}) == "alltime"


def test_resolve_rank_context_uses_explicit_field():
    """HeroHighlightCard construction must honor the curator's explicit
    rank_window for new entries — pre-fix, the social variant copy and the
    image leaderboard could disagree because the variant builder still
    used ratio-based _best_rank_context while the image generator used
    the explicit field."""
    from content_pipeline.hero_highlight_variants import _resolve_rank_context

    snapshot = {
        "season": 1, "season_total": 249,
        "alltime": 5, "alltime_total": 12926,
    }
    # Curator wrote rank_window="season" — honor it even though the alltime
    # ratio is much smaller.
    window, text = _resolve_rank_context({"rank_window": "season", "rank_snapshot": snapshot})
    assert window == "season"
    assert "Season" in text and "#1" in text and "249" in text

    # Curator wrote rank_window="alltime".
    window, text = _resolve_rank_context({"rank_window": "alltime", "rank_snapshot": snapshot})
    assert window == "alltime"
    assert "All-time" in text and "#5" in text and "12926" in text


def test_resolve_rank_context_falls_back_for_legacy_entries():
    """Legacy entries with no rank_window field still flow through ratio
    inference — yesterday's posts must keep working."""
    from content_pipeline.hero_highlight_variants import _resolve_rank_context

    snapshot = {
        "season": 1, "season_total": 249,
        "alltime": 5, "alltime_total": 12926,
    }
    window, text = _resolve_rank_context({"rank_snapshot": snapshot})
    # alltime has the smaller ratio (5/12926 vs 1/249).
    assert window == "alltime"


def test_curator_coerce_rank_window_validates():
    """Curator's _coerce_rank_window accepts valid inputs, falls back to
    metric_window (mapping career→alltime), returns None when nothing fits."""
    from metrics.highlights.curator import _coerce_rank_window

    # Valid LLM output passes through.
    for v in ("season", "alltime", "last3", "last5", "last10"):
        assert _coerce_rank_window(v) == v

    # Garbage input falls back to scope_window mapping.
    assert _coerce_rank_window("nonsense", scope_window="career") == "alltime"
    assert _coerce_rank_window("", scope_window="last5") == "last5"
    assert _coerce_rank_window(None, scope_window="season") == "season"

    # No usable input → None (caller decides default).
    assert _coerce_rank_window(None) is None
    assert _coerce_rank_window("garbage") is None


def test_generate_image_retries_moderation_block_with_safety_prompt(tmp_path):
    from social_media.hero_poster import _generate_image_with_safety_retry

    class FakeModerationError(Exception):
        body = {"error": {"code": "moderation_blocked", "message": "blocked by safety system"}}

    calls = []

    def fake_generate_image(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise FakeModerationError("moderation_blocked")
        return Path(kwargs["output_path"])

    with patch("social_media.funba_imagegen.generate_image", side_effect=fake_generate_image):
        out, prompt_used = _generate_image_with_safety_retry(
            prompt="Metric: steals\n- Render real, recognizable team logos and player likenesses where they\n  apply (the data is real, the people are real).",
            output_path=tmp_path / "poster.png",
            model="gpt-image-2",
            size="1024x1536",
            quality="high",
            output_format="png",
            background="opaque",
        )

    assert out == tmp_path / "poster.png"
    assert len(calls) == 2
    assert "defensive takeaways" in prompt_used
    assert "Avoid realistic faces" in prompt_used


def test_generate_image_raises_when_api_retries_fail(tmp_path):
    from social_media.hero_poster import _generate_image_with_safety_retry

    class FakeModerationError(Exception):
        body = {"error": {"code": "moderation_blocked", "message": "blocked by safety system"}}

    def fake_generate_image(**_kwargs):
        raise FakeModerationError("moderation_blocked")

    target = tmp_path / "poster.png"
    with patch("social_media.funba_imagegen.generate_image", side_effect=fake_generate_image):
        with pytest.raises(FakeModerationError):
            _generate_image_with_safety_retry(
                prompt="Metric: steals\nSeason frame: Playoffs\nTonight's game: HOU @ LAL\nTrigger: LeBron reached 500",
                output_path=target,
                model="gpt-image-2",
                size="1024x1024",
                quality="high",
                output_format="png",
                background="opaque",
            )

    assert not target.exists()
