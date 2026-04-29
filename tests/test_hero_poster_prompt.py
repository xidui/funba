"""Regression tests for the hero-poster prompt builder helpers.

Pre-fix bug: build_prompt_context pulled metric_key from one card field
and season from another with separate fallback chains, so a curator card
with metric_key=*_last5 + season=all_playoffs produced an empty leaderboard
(the *_last5 variant has no rows under all_*). The defensive coerce now
rewrites the variant suffix to match the season's family if the original
pair has no rows.
"""
from __future__ import annotations

from unittest.mock import patch


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
