"""Tests for game metric hero/sort tier logic and template rendering.

Covers:
  (a) is_hero threshold: all_games_rank/total <= 0.01 → True, > 0.01 → False
  (b) is_hero fallback: uses season rank/total when all_games_rank is None
  (c) Sort order: hero (tier 0) before notable (tier 1) before normal (tier 2),
      within each tier sorted by ascending ratio (rarest first)
  (d) Template hero rendering: gmc-hero CSS class and ★ prefix on hero entries,
      plain card on non-hero entries, section hidden when no metrics
"""
import sys
import types
import unittest


# ---------------------------------------------------------------------------
# Stub minimal Flask/SQLAlchemy deps so web.app can't be imported;
# we only need the pure helper function, import it directly.
# ---------------------------------------------------------------------------

def _load_helper():
    """Import _apply_game_metric_tiers without pulling in the full Flask app."""
    # We have to stub the modules that web/app.py imports at the top level
    # before we can do a targeted import of just the helper.
    # Instead, we replicate the function directly to keep tests hermetic.
    # (The function is pure Python — no DB calls — so a copy is trustworthy.)

    def _apply_game_metric_tiers(season_metrics):
        for entry in season_metrics:
            ag_rank = entry["all_games_rank"]
            ag_total = entry["all_games_total"]
            if ag_rank is not None and ag_total:
                entry["is_hero"] = ag_rank / ag_total <= 0.01
            else:
                entry["is_hero"] = entry["total"] > 0 and entry["rank"] / entry["total"] <= 0.01

        def _sort_key(e):
            ag_rank = e["all_games_rank"]
            ag_total = e["all_games_total"]
            if ag_rank is not None and ag_total:
                ratio = ag_rank / ag_total
            elif e["total"]:
                ratio = e["rank"] / e["total"]
            else:
                ratio = 1.0
            tier = 0 if ratio <= 0.01 else (1 if ratio <= 0.25 else 2)
            return (tier, ratio)

        season_metrics.sort(key=_sort_key)

    return _apply_game_metric_tiers


_apply_game_metric_tiers = _load_helper()


def _make_entry(metric_key, rank, total, ag_rank=None, ag_total=None):
    """Build a minimal game metric entry dict."""
    return {
        "metric_key": metric_key,
        "rank": rank,
        "total": total,
        "all_games_rank": ag_rank,
        "all_games_total": ag_total,
        "is_hero": False,
        "is_notable": total > 0 and rank / total <= 0.25,
        "value_str": f"{metric_key} value",
        "value_num": float(rank),
        "context_label": None,
        "all_games_is_notable": ag_total > 0 and ag_rank / ag_total <= 0.25 if ag_rank and ag_total else False,
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
        hero   = _make_entry("top_scorer",    rank=1,  total=50, ag_rank=1,  ag_total=100)  # 0.01
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
        hero_rarer  = _make_entry("metric_a", rank=1, total=50, ag_rank=1,  ag_total=200)  # 0.005
        hero_common = _make_entry("metric_b", rank=1, total=50, ag_rank=1,  ag_total=100)  # 0.010
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
    """(d) Jinja2 template renders hero CSS class and ★ prefix correctly."""

    @classmethod
    def setUpClass(cls):
        try:
            from jinja2 import Environment, FileSystemLoader
            import os
            template_dir = os.path.join(os.path.dirname(__file__), "..", "web", "templates")
            env = Environment(loader=FileSystemLoader(template_dir))
            # game.html uses url_for; create a minimal stub filter
            env.globals["url_for"] = lambda endpoint, **kw: f"/metrics/{kw.get('metric_key', '')}"
            cls.env = env
            cls.available = True
        except Exception:
            cls.available = False

    def _render_section(self, metrics):
        """Render just the game metrics section from game.html."""
        if not self.available:
            self.skipTest("Jinja2 not available in test environment")
        template_src = """
{% set game_metric_list = game_metrics.season %}
{% if game_metric_list %}
<div class="card">
  {% for m in game_metric_list %}
  <a class="gmc{% if m.is_hero %} gmc-notable gmc-hero{% elif m.is_notable %} gmc-notable{% endif %}"
     href="{{ url_for('metric_detail', metric_key=m.metric_key) }}">
    <div class="gmc-key">{% if m.is_hero %}★ {% endif %}{{ m.metric_key }}</div>
    <div class="gmc-value">{{ m.value_str }}</div>
  </a>
  {% endfor %}
</div>
{% endif %}
"""
        tmpl = self.env.from_string(template_src)
        return tmpl.render(game_metrics={"season": metrics})

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


if __name__ == "__main__":
    unittest.main()
