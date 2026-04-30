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


def test_generate_image_writes_local_fallback_when_api_retries_fail(tmp_path):
    from social_media.hero_poster import _generate_image_with_safety_retry

    class FakeModerationError(Exception):
        body = {"error": {"code": "moderation_blocked", "message": "blocked by safety system"}}

    def fake_generate_image(**_kwargs):
        raise FakeModerationError("moderation_blocked")

    target = tmp_path / "poster.png"
    with patch("social_media.funba_imagegen.generate_image", side_effect=fake_generate_image):
        out, prompt_used = _generate_image_with_safety_retry(
            prompt="Metric: steals\nSeason frame: Playoffs\nTonight's game: HOU @ LAL\nTrigger: LeBron reached 500",
            output_path=target,
            model="gpt-image-2",
            size="1024x1024",
            quality="high",
            output_format="png",
            background="opaque",
        )

    assert out == target
    assert target.exists()
    assert target.stat().st_size > 0
    assert "defensive takeaways" in prompt_used
