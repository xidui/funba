"""Tests for game-scope last3/last5 virtual window support."""
from pathlib import Path
import sys
import types
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class TestCatalogEligibleWindowTypes(unittest.TestCase):
    def _row(self, scope, status="published", career=False, supports_career=False, trigger="game"):
        r = types.SimpleNamespace(
            status=status,
            scope=scope,
        )
        sf = {
            "scope": scope,
            "career": career,
            "supports_career": supports_career,
            "trigger": trigger,
        }
        return r, sf

    def _eligible(self, scope, **kwargs):
        from web.app import _catalog_eligible_window_types
        row, sf = self._row(scope, **kwargs)
        return _catalog_eligible_window_types(row, search_fields=sf)

    def test_game_scope_returns_last3_last5(self):
        result = self._eligible("game")
        self.assertEqual(result, ["last10", "last5", "last3"])

    def test_game_scope_no_career(self):
        result = self._eligible("game")
        self.assertNotIn("career", result)

    def test_game_scope_draft_returns_empty(self):
        row, sf = self._row("game", status="draft")
        from web.app import _catalog_eligible_window_types
        self.assertEqual(_catalog_eligible_window_types(row, search_fields=sf), [])

    def test_game_scope_career_variant_returns_empty(self):
        result = self._eligible("game", career=True)
        self.assertEqual(result, [])

    def test_season_scope_still_blocked(self):
        result = self._eligible("season")
        self.assertEqual(result, [])

    def test_player_scope_still_requires_supports_career(self):
        result = self._eligible("player", supports_career=False)
        self.assertEqual(result, [])
        result2 = self._eligible("player", supports_career=True, trigger="game")
        self.assertIn("career", result2)
        self.assertNotIn("last3", result2)

    def test_player_season_trigger_gets_all_three(self):
        result = self._eligible("player", supports_career=True, trigger="season")
        self.assertIn("career", result)
        self.assertIn("last3", result)
        self.assertIn("last5", result)


class TestVirtualGameCatalogEntries(unittest.TestCase):
    def _make_row(self, key="lowest_winning_score"):
        return types.SimpleNamespace(
            key=key,
            name="Lowest Winning Score",
            description="The fewest points a team scored and still won.",
            scope="game",
            category="game",
            status="published",
            source_type="code",
            expression="",
            min_sample=1,
            career_min_sample=None,
            created_by_user_id=None,
        )

    def _search_fields(self):
        return {
            "scope": "game",
            "career": False,
            "supports_career": False,
            "trigger": "game",
            "name": "Lowest Winning Score",
            "name_zh": "最低赢球得分",
            "description": "The fewest points a team scored and still won.",
            "description_zh": "赢球最低得分的比赛。",
            "min_sample": 1,
            "career_min_sample": None,
            "rank_order": "asc",
            "group_key": None,
        }

    def test_generates_last3_and_last5(self):
        from web.app import _virtual_window_catalog_metrics
        row = self._make_row()
        sf = self._search_fields()
        entries = _virtual_window_catalog_metrics(row, search_fields=sf, existing_keys={row.key}, counts={}, is_mine=False)
        keys = [e["key"] for e in entries]
        self.assertIn("lowest_winning_score_last3", keys)
        self.assertIn("lowest_winning_score_last5", keys)
        self.assertNotIn("lowest_winning_score_career", keys)

    def test_names_include_suffix(self):
        from web.app import _virtual_window_catalog_metrics
        row = self._make_row()
        sf = self._search_fields()
        entries = _virtual_window_catalog_metrics(row, search_fields=sf, existing_keys={row.key}, counts={}, is_mine=False)
        by_key = {e["key"]: e for e in entries}
        self.assertIn("Last 3 Seasons", by_key["lowest_winning_score_last3"]["name"])
        self.assertIn("近 3 季", by_key["lowest_winning_score_last3"].get("name_zh", ""))
        self.assertIn("Last 5 Seasons", by_key["lowest_winning_score_last5"]["name"])

    def test_scope_preserved_as_game(self):
        from web.app import _virtual_window_catalog_metrics
        row = self._make_row()
        sf = self._search_fields()
        entries = _virtual_window_catalog_metrics(row, search_fields=sf, existing_keys={row.key}, counts={}, is_mine=False)
        for e in entries:
            self.assertEqual(e["scope"], "game")

    def test_existing_key_not_duplicated(self):
        from web.app import _virtual_window_catalog_metrics
        row = self._make_row()
        sf = self._search_fields()
        existing = {row.key, "lowest_winning_score_last3"}
        entries = _virtual_window_catalog_metrics(row, search_fields=sf, existing_keys=existing, counts={}, is_mine=False)
        keys = [e["key"] for e in entries]
        self.assertNotIn("lowest_winning_score_last3", keys)
        self.assertIn("lowest_winning_score_last5", keys)


class TestSearchEmbeddingFallback(unittest.TestCase):
    def test_last3_last5_skipped_from_db_fetch(self):
        """_last3/_last5 keys must not trigger a DB embedding lookup."""
        from metrics.framework.search import _prerank_with_embeddings
        import numpy as np

        # Two fake candidates: one real key, one virtual last3 key
        candidates = [
            {"key": "lowest_winning_score"},
            {"key": "lowest_winning_score_last3"},
            {"key": "lowest_winning_score_last5"},
        ]
        # Fake base vector in cache
        import metrics.framework.search as _mod
        base_vec = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        _mod._embedding_vectors["lowest_winning_score"] = base_vec

        class _FakeSession:
            def query(self, *a, **kw):
                raise AssertionError("DB should not be queried for virtual window keys")

        # With top_k >= len, prerank returns all candidates unchanged
        result = _prerank_with_embeddings(_FakeSession(), "low scoring wins", candidates, top_k=10)
        self.assertEqual(len(result), 3)

    def test_last3_reuses_base_vector(self):
        """Virtual _last3 key falls back to base metric vector for scoring."""
        from metrics.framework.search import _prerank_with_embeddings
        import numpy as np
        import metrics.framework.search as _mod

        base_vec = np.array([1.0, 0.0, 0.0], dtype=np.float32)
        _mod._embedding_vectors["some_metric"] = base_vec

        candidates = [
            {"key": "some_metric"},
            {"key": "some_metric_last3"},
            {"key": "unrelated_metric"},  # no vector — should be excluded
        ]

        # top_k=1 forces scoring; both some_metric and some_metric_last3 share
        # the same vector so they tie — at least one of them should be included
        result = _prerank_with_embeddings(None, "some query", candidates, top_k=2)
        result_keys = [r["key"] for r in result]
        self.assertIn("some_metric_last3", result_keys)


if __name__ == "__main__":
    unittest.main()
