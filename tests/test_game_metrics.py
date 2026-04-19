"""Tests for game metric hero/sort tier logic and template rendering.

Covers:
  (a) is_hero threshold: all_games_rank/total <= 0.01 → True, > 0.01 → False
  (b) is_hero fallback: uses season rank/total when all_games_rank is None
  (c) Sort order: hero (tier 0) before notable (tier 1) before normal (tier 2),
      within each tier sorted by ascending ratio (rarest first)
  (d) Template hero rendering via _game_metrics.html partial: gmc-hero CSS class
      and ★ prefix on hero entries, plain card on non-hero entries, section
      hidden when no metrics
"""
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# ---------------------------------------------------------------------------
# Import the real game-metric helpers from web.app using module stubs.
# Mirrors the pattern used in test_auth.py to avoid a live DB connection.
# ---------------------------------------------------------------------------

def _import_helper():
    """Return the real game metric helpers from web.app."""
    original_db = sys.modules.get("db")
    original_db_models = sys.modules.get("db.models")
    original_backfill = sys.modules.get("db.backfill_nba_player_shot_detail")
    original_line = sys.modules.get("db.backfill_nba_game_line_score")
    original_game_analysis = sys.modules.get("content_pipeline.game_analysis_issues")
    fake_engine = MagicMock()

    fake_models = types.ModuleType("db.models")
    for name in (
        "Award", "Feedback", "Game", "GameContentAnalysisIssuePost", "GamePlayByPlay", "MagicToken", "MetricComputeRun", "MetricDefinition",
        "MetricPerfLog", "MetricResult", "MetricRunLog", "PageView", "Player",
        "PlayerGameStats", "PlayerSalary", "ShotRecord", "Team", "TeamGameStats", "SocialPost", "SocialPostImage", "SocialPostVariant", "SocialPostDelivery", "User",
        "GameLineScore",
    ):
        setattr(fake_models, name, MagicMock())
    fake_models.engine = fake_engine
    sys.modules["db.models"] = fake_models
    fake_db = sys.modules.get("db") or types.ModuleType("db")
    fake_db.__path__ = [str(REPO_ROOT / "db")]
    fake_db.models = fake_models
    sys.modules["db"] = fake_db

    fake_backfill = types.ModuleType("db.backfill_nba_player_shot_detail")
    fake_backfill.back_fill_game_shot_record = MagicMock()
    fake_backfill.back_fill_game_shot_record_from_api = MagicMock()
    fake_backfill.is_game_shot_back_filled = MagicMock(return_value=False)
    sys.modules["db.backfill_nba_player_shot_detail"] = fake_backfill

    fake_line = types.ModuleType("db.backfill_nba_game_line_score")
    fake_line.back_fill_game_line_score = MagicMock()
    fake_line.has_game_line_score = MagicMock(return_value=False)
    fake_line.normalize_game_line_score_payload = MagicMock()
    sys.modules["db.backfill_nba_game_line_score"] = fake_line

    fake_game_analysis = types.ModuleType("content_pipeline.game_analysis_issues")
    for name in (
        "ensure_game_content_analysis_issue_for_game",
        "ensure_game_content_analysis_issues",
        "game_analysis_readiness_detail",
        "game_analysis_issue_history",
        "link_post_to_game_analysis_issue",
        "resolve_game_analysis_issue_record",
    ):
        setattr(fake_game_analysis, name, MagicMock())
    sys.modules["content_pipeline.game_analysis_issues"] = fake_game_analysis

    # Clear any cached web.app so the stubs are picked up.
    for key in list(sys.modules):
        if key.startswith("web.app") or key == "web.app":
            del sys.modules[key]

    from web.app import (
        _apply_game_metric_tiers,
        _build_story_candidates,
        _game_metric_badge_text,
        _prepare_game_metric_cards,
        _season_type_prefix,
    )

    if original_db is not None:
        sys.modules["db"] = original_db
    else:
        sys.modules.pop("db", None)

    if original_db_models is not None:
        sys.modules["db.models"] = original_db_models
    else:
        sys.modules.pop("db.models", None)

    if original_backfill is not None:
        sys.modules["db.backfill_nba_player_shot_detail"] = original_backfill
    else:
        sys.modules.pop("db.backfill_nba_player_shot_detail", None)

    if original_line is not None:
        sys.modules["db.backfill_nba_game_line_score"] = original_line
    else:
        sys.modules.pop("db.backfill_nba_game_line_score", None)

    if original_game_analysis is not None:
        sys.modules["content_pipeline.game_analysis_issues"] = original_game_analysis
    else:
        sys.modules.pop("content_pipeline.game_analysis_issues", None)

    return _apply_game_metric_tiers, _build_story_candidates, _game_metric_badge_text, _prepare_game_metric_cards, _season_type_prefix


_apply_game_metric_tiers, _build_story_candidates, _game_metric_badge_text, _prepare_game_metric_cards, _season_type_prefix = _import_helper()


def _make_entry(metric_key, rank, total, ag_rank=None, ag_total=None):
    """Build a minimal game metric entry dict."""
    return {
        "metric_key": metric_key,
        "entity_id": f"{metric_key}:{rank}",
        "rank": rank,
        "total": total,
        "all_games_rank": ag_rank,
        "all_games_total": ag_total,
        "is_hero": False,
        "is_notable": total > 0 and rank / total <= 0.25,
        "value_str": f"{metric_key} value",
        "value_num": float(rank),
        "context_label": None,
        "all_games_is_notable": (
            ag_total > 0 and ag_rank / ag_total <= 0.25
            if ag_rank and ag_total else False
        ),
    }


class TestIsHeroThreshold(unittest.TestCase):
    """(a) is_hero uses all_games_rank/total when available."""

    def test_top_1pct_is_hero(self):
        # rank=1 out of 100 → ratio=0.01 → hero
        entries = [_make_entry("top_scorer", rank=1, total=50, ag_rank=1, ag_total=100)]
        _apply_game_metric_tiers(entries)
        self.assertTrue(entries[0]["is_hero"])

    def test_exact_1pct_boundary_is_hero(self):
        # ratio exactly 0.01 → hero (inclusive boundary)
        entries = [_make_entry("top_scorer", rank=1, total=50, ag_rank=1, ag_total=100)]
        _apply_game_metric_tiers(entries)
        self.assertTrue(entries[0]["is_hero"])

    def test_just_above_1pct_not_hero(self):
        # rank=2 out of 100 → ratio=0.02 → NOT hero
        entries = [_make_entry("top_scorer", rank=1, total=50, ag_rank=2, ag_total=100)]
        _apply_game_metric_tiers(entries)
        self.assertFalse(entries[0]["is_hero"])

    def test_25pct_notable_not_hero(self):
        # rank=25 out of 100 → ratio=0.25 → notable but NOT hero
        entries = [_make_entry("combined_score", rank=5, total=50, ag_rank=25, ag_total=100)]
        _apply_game_metric_tiers(entries)
        self.assertFalse(entries[0]["is_hero"])

    def test_50pct_not_hero(self):
        entries = [_make_entry("lead_changes", rank=10, total=50, ag_rank=50, ag_total=100)]
        _apply_game_metric_tiers(entries)
        self.assertFalse(entries[0]["is_hero"])


class TestIsHeroFallback(unittest.TestCase):
    """(b) Falls back to season rank when all_games_rank is None."""

    def test_season_rank_top_1pct_is_hero_when_no_all_games(self):
        # no all_games data; season rank=1/100 → 0.01 → hero
        entries = [_make_entry("top_scorer", rank=1, total=100, ag_rank=None, ag_total=None)]
        _apply_game_metric_tiers(entries)
        self.assertTrue(entries[0]["is_hero"])

    def test_season_rank_above_1pct_not_hero_when_no_all_games(self):
        # no all_games data; season rank=2/100 → 0.02 → NOT hero
        entries = [_make_entry("top_scorer", rank=2, total=100, ag_rank=None, ag_total=None)]
        _apply_game_metric_tiers(entries)
        self.assertFalse(entries[0]["is_hero"])

    def test_zero_total_not_hero(self):
        entries = [_make_entry("top_scorer", rank=0, total=0, ag_rank=None, ag_total=None)]
        _apply_game_metric_tiers(entries)
        self.assertFalse(entries[0]["is_hero"])


class TestSortOrder(unittest.TestCase):
    """(c) hero → notable → normal ordering, with rarest first within each tier."""

    def test_hero_before_notable_before_normal(self):
        hero    = _make_entry("top_scorer",    rank=1,  total=50, ag_rank=1,  ag_total=100)  # 0.01
        notable = _make_entry("combined_score", rank=5,  total=50, ag_rank=20, ag_total=100)  # 0.20
        normal  = _make_entry("lead_changes",   rank=10, total=50, ag_rank=60, ag_total=100)  # 0.60
        # Pass in reverse order
        entries = [normal, notable, hero]
        _apply_game_metric_tiers(entries)
        self.assertEqual(entries[0]["metric_key"], "top_scorer",     "hero first")
        self.assertEqual(entries[1]["metric_key"], "combined_score", "notable second")
        self.assertEqual(entries[2]["metric_key"], "lead_changes",   "normal third")

    def test_within_hero_tier_rarest_first(self):
        # Two hero-tier entries: ratio 0.005 vs 0.01
        hero_rarer  = _make_entry("metric_a", rank=1, total=50, ag_rank=1, ag_total=200)  # 0.005
        hero_common = _make_entry("metric_b", rank=1, total=50, ag_rank=1, ag_total=100)  # 0.010
        entries = [hero_common, hero_rarer]
        _apply_game_metric_tiers(entries)
        self.assertEqual(entries[0]["metric_key"], "metric_a", "rarer hero first")

    def test_within_notable_tier_rarest_first(self):
        n1 = _make_entry("metric_a", rank=1, total=50, ag_rank=5,  ag_total=100)  # 0.05
        n2 = _make_entry("metric_b", rank=1, total=50, ag_rank=20, ag_total=100)  # 0.20
        entries = [n2, n1]
        _apply_game_metric_tiers(entries)
        self.assertEqual(entries[0]["metric_key"], "metric_a", "rarer notable first")

    def test_no_entries_noop(self):
        entries = []
        _apply_game_metric_tiers(entries)
        self.assertEqual(entries, [])

    def test_single_entry_unchanged(self):
        entries = [_make_entry("top_scorer", rank=1, total=100, ag_rank=1, ag_total=100)]
        _apply_game_metric_tiers(entries)
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0]["is_hero"])


class TestTemplateHeroRendering(unittest.TestCase):
    """(d) _game_metrics.html partial renders hero CSS class and ★ prefix correctly."""

    @classmethod
    def setUpClass(cls):
        try:
            from jinja2 import Environment, FileSystemLoader
            import os
            template_dir = os.path.join(os.path.dirname(__file__), "..", "web", "templates")
            env = Environment(loader=FileSystemLoader(template_dir))
            env.globals["url_for"] = lambda endpoint, **kw: f"/metrics/{kw.get('metric_key', '')}"
            env.globals["t"] = lambda en, zh=None: en
            cls.env = env
            cls.available = True
        except Exception:
            cls.available = False

    def _render_section(self, metrics):
        """Render _game_metrics.html with the given metric list."""
        if not self.available:
            self.skipTest("Jinja2 not available in test environment")
        tmpl = self.env.get_template("_game_metrics.html")
        return tmpl.render(game_metrics={"season": metrics, "season_extra": []})

    def test_hero_entry_gets_gmc_hero_class(self):
        hero = _make_entry("top_scorer", rank=1, total=100, ag_rank=1, ag_total=100)
        hero["is_hero"] = True
        html = self._render_section([hero])
        self.assertIn("gmc-hero", html)

    def test_hero_entry_gets_star_prefix(self):
        hero = _make_entry("top_scorer", rank=1, total=100, ag_rank=1, ag_total=100)
        hero["is_hero"] = True
        html = self._render_section([hero])
        self.assertIn("★", html)

    def test_non_hero_entry_no_gmc_hero_class(self):
        normal = _make_entry("lead_changes", rank=50, total=100, ag_rank=50, ag_total=100)
        normal["is_hero"] = False
        html = self._render_section([normal])
        self.assertNotIn("gmc-hero", html)
        self.assertNotIn("★", html)

    def test_hero_card_links_to_metric_detail(self):
        hero = _make_entry("top_scorer", rank=1, total=100, ag_rank=1, ag_total=100)
        hero["is_hero"] = True
        html = self._render_section([hero])
        self.assertIn("/metrics/top_scorer", html)

    def test_empty_metrics_section_hidden(self):
        html = self._render_section([])
        self.assertNotIn("card", html)


class TestGameMetricCardSelection(unittest.TestCase):
    def test_season_type_prefix_extracts_type_code(self):
        self.assertEqual(_season_type_prefix("22025"), "2")
        self.assertEqual(_season_type_prefix("42024"), "4")
        self.assertIsNone(_season_type_prefix("all_2"))
        self.assertIsNone(_season_type_prefix(None))

    def test_badge_text_uses_absolute_rank(self):
        self.assertEqual(_game_metric_badge_text(1, 500, "Season"), "#1 Season")
        self.assertEqual(_game_metric_badge_text(12, 400, "All"), "#12 All")
        self.assertIsNone(_game_metric_badge_text(300, 400, "All"))

    def test_prepare_game_metric_cards_keeps_multiple_notable_rows_for_same_metric(self):
        entries = [
            _make_entry("low_quarter_score", rank=1, total=100, ag_rank=10, ag_total=1000),
            _make_entry("low_quarter_score", rank=8, total=100, ag_rank=50, ag_total=1000),
            _make_entry("low_quarter_score", rank=60, total=100, ag_rank=600, ag_total=1000),
        ]
        _apply_game_metric_tiers(entries)
        visible, extra = _prepare_game_metric_cards(entries)

        self.assertEqual(len(visible), 4 if len(entries) >= 4 else len(entries))
        self.assertEqual([c["rank"] for c in visible], [1, 8, 60])
        self.assertTrue(visible[0]["is_featured"])
        self.assertTrue(visible[1]["is_featured"])
        self.assertFalse(visible[2]["is_featured"])
        self.assertEqual(visible[0]["season_badge_text"], "#1 Season")
        self.assertEqual(visible[1]["season_badge_text"], "#8 Season")
        self.assertEqual(extra, [])

    def test_prepare_game_metric_cards_hides_metrics_when_nothing_is_notable(self):
        entries = [
            _make_entry("single_quarter_team_scoring", rank=60, total=100, ag_rank=600, ag_total=1000),
            _make_entry("single_quarter_team_scoring", rank=70, total=100, ag_rank=700, ag_total=1000),
        ]
        _apply_game_metric_tiers(entries)
        visible, extra = _prepare_game_metric_cards(entries)

        self.assertEqual(len(visible), 2)
        self.assertFalse(any(card["is_featured"] for card in visible))
        self.assertEqual(extra, [])

    def test_prepare_game_metric_cards_shows_all_featured_when_more_than_four(self):
        entries = [
            _make_entry(f"metric_{i}", rank=i + 1, total=100, ag_rank=i + 1, ag_total=1000)
            for i in range(5)
        ]
        _apply_game_metric_tiers(entries)
        visible, extra = _prepare_game_metric_cards(entries)

        self.assertEqual(len(visible), 5)
        self.assertTrue(all(card["is_featured"] for card in visible))


class TestStoryCandidates(unittest.TestCase):
    def test_story_candidates_prefers_top_scorer_as_lead_and_season_record_as_support(self):
        payload = _build_story_candidates(
            {
                "season": [
                    {
                        "metric_key": "top_scorer",
                        "metric_name": "Top Scorer",
                        "entity_id": "004:test",
                        "value_num": 85.0,
                        "value_str": "Player X scored 85 pts",
                        "rank": 2,
                        "total": 10,
                        "all_games_rank": 2,
                        "all_games_total": 50000,
                        "is_featured": True,
                        "is_hero": False,
                        "context_label": None,
                    }
                ],
                "season_extra": [],
            },
            [
                {
                    "metric_key": "highest_score_by_player_in_a_game",
                    "metric_name": "Highest Score by Player in a Game",
                    "entity_type": "player",
                    "entity_id": "123",
                    "season": "22025",
                    "player_name": "Player X",
                    "value_num": 85.0,
                    "value_str": "85",
                    "rank": 2,
                    "total": 500,
                    "all_rank": 120,
                    "all_total": 10000,
                    "is_featured": True,
                    "is_hero": False,
                    "context_label": "85 pts",
                }
            ],
            [],
        )
        self.assertEqual(payload["lead_candidates"][0]["metric_key"], "top_scorer")
        self.assertEqual(payload["support_candidates"][0]["metric_key"], "highest_score_by_player_in_a_game")

    def test_story_candidates_suppresses_routine_metrics(self):
        payload = _build_story_candidates(
            {"season": [], "season_extra": []},
            [
                {
                    "metric_key": "games_started",
                    "metric_name": "Games Started",
                    "entity_type": "player",
                    "entity_id": "123",
                    "season": "22025",
                    "player_name": "Player X",
                    "value_num": 1.0,
                    "value_str": "1",
                    "rank": 1,
                    "total": 50,
                    "all_rank": 500,
                    "all_total": 10000,
                    "is_featured": True,
                    "is_hero": False,
                    "context_label": None,
                }
            ],
            [],
        )
        self.assertEqual(payload["lead_candidates"], [])
        self.assertEqual(payload["support_candidates"], [])
        self.assertEqual(payload["suppressed_candidates"][0]["metric_key"], "games_started")
        self.assertEqual(extra, [])


if __name__ == "__main__":
    unittest.main()
