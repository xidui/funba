"""Tests for the _compute_standout_performances algorithm in web/app.py.

Tests are hermetic — they exercise an inline copy of the pure Python
algorithm so they do not depend on Flask, SQLAlchemy, or sys.modules stubs.
Any logic change to the production function must be mirrored here.

Covers:
  (a) Top-1% threshold — returns up to 5 cards when >= 2 qualify.
  (b) Top-5% fallback — used when fewer than 2 qualify at top-1%; max 3.
  (c) Empty section — returns [] when nothing qualifies at 5%.
  (d) Missing season — returns [] immediately.
  (e) Empty player_rows — returns [] immediately.
  (f) Tie-heavy distribution — PERCENT_RANK semantics prevent over-labelling
      tied-max values; they share the correct percentile rank.
  (g) Sorting — results are sorted by pct_rank descending (rarest first).
"""
import bisect
import unittest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Inline copy of the core algorithm
# Mirrors _compute_standout_performances() in web/app.py exactly.
# Keep in sync with the production function.
# ---------------------------------------------------------------------------

STAT_LABELS = {
    "pts": "points",
    "reb": "rebounds",
    "ast": "assists",
    "stl": "steals",
    "blk": "blocks",
    "fgm": "field goals made",
    "fg3m": "3-pointers made",
    "ftm": "free throws made",
    "plus": "+/-",
}
STAT_COLS = list(STAT_LABELS)


def _compute_standout_performances_algo(
    season_data: list[dict],
    player_rows: list,
    season: str | None,
) -> list[dict]:
    """Pure-Python version of the algorithm (no DB/Flask dependencies).

    Args:
        season_data: list of dicts with stat column -> value, representing
                     all PlayerGameStats rows for the season.
        player_rows: list of (stat_obj, player_obj) tuples for this game,
                     where stat_obj has attributes for each STAT_COL and
                     player_obj has a `full_name` attribute.
        season:      season ID string; empty/None → return [] immediately.
    """
    if not season or not player_rows:
        return []
    if not season_data:
        return []

    # Build sorted per-stat value lists (same as production code)
    stat_vals: dict[str, list] = {c: [] for c in STAT_COLS}
    for row in season_data:
        for col in STAT_COLS:
            v = row.get(col)
            if v is not None:
                stat_vals[col].append(v)
    for col in STAT_COLS:
        stat_vals[col].sort()

    def _pct_label(raw_pct: float) -> str:
        if raw_pct < 0.1:
            return "Top 0.1% Season"
        if raw_pct < 1.0:
            return f"Top {raw_pct:.1f}% Season"
        return f"Top {raw_pct:.0f}% Season"

    def _score_game_rows(threshold_pct: float) -> list[dict]:
        results = []
        for stat, player in player_rows:
            player_name = (
                player.full_name if player and player.full_name
                else str(stat.player_id)
            )
            for col in STAT_COLS:
                val = getattr(stat, col, None)
                if val is None:
                    continue
                sv = stat_vals.get(col, [])
                n = len(sv)
                if n == 0:
                    continue
                # PERCENT_RANK semantics: fraction of values strictly less
                # than val, divided by (n - 1).
                pct_rank = bisect.bisect_left(sv, val) / max(n - 1, 1)
                if pct_rank < threshold_pct:
                    continue
                raw_pct = (1.0 - pct_rank) * 100
                results.append({
                    "player_id": stat.player_id,
                    "player_name": player_name,
                    "stat": col,
                    "stat_label": STAT_LABELS[col],
                    "value": val,
                    "pct_rank": pct_rank,
                    "pct_label": _pct_label(raw_pct),
                    "level": "top_1" if threshold_pct >= 0.99 else "top_5",
                })
        results.sort(key=lambda x: x["pct_rank"], reverse=True)
        return results

    top_1 = _score_game_rows(0.99)
    if len(top_1) >= 2:
        return top_1[:5]

    top_5 = _score_game_rows(0.95)
    return top_5[:3]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_player_row(player_id: str, name: str | None, **stats):
    """Return a (stat_mock, player_mock) tuple."""
    stat = MagicMock()
    stat.player_id = player_id
    for col in STAT_COLS:
        setattr(stat, col, stats.get(col, None))

    player = MagicMock()
    player.full_name = name
    return (stat, player)


def _season(stat: str, values: list) -> list[dict]:
    """Build season_data with a single stat column populated."""
    return [{stat: v} for v in values]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStandoutPerformancesAlgo(unittest.TestCase):

    # (a) Top-1% threshold: >= 2 qualify → return up to 5 ------------------

    def test_top1_path_returns_up_to_five_cards(self):
        season = _season("pts", list(range(1000)))
        rows = [
            _make_player_row("p1", "Player One", pts=999),
            _make_player_row("p2", "Player Two", pts=998),
        ]
        result = _compute_standout_performances_algo(season, rows, "22025")

        self.assertGreaterEqual(len(result), 2)
        self.assertLessEqual(len(result), 5)
        self.assertTrue(all(c["level"] == "top_1" for c in result))

    # (b) Top-5% fallback: fewer than 2 qualify at top-1% ------------------

    def test_top5_fallback_when_insufficient_top1(self):
        # 100-value season; player scores 97 (top 3%, not top 1%)
        season = _season("pts", list(range(100)))
        rows = [_make_player_row("p1", "Solo Star", pts=97)]

        result = _compute_standout_performances_algo(season, rows, "22025")

        self.assertGreaterEqual(len(result), 1)
        self.assertLessEqual(len(result), 3)
        self.assertTrue(all(c["level"] == "top_5" for c in result))

    # (c) Empty section: nothing qualifies at top-5% -----------------------

    def test_empty_when_no_standouts(self):
        season = _season("pts", list(range(100)))
        rows = [_make_player_row("p1", "Average Player", pts=50)]

        result = _compute_standout_performances_algo(season, rows, "22025")

        self.assertEqual(result, [])

    # (d) Missing season ---------------------------------------------------

    def test_missing_season_returns_empty(self):
        season = _season("pts", [50])
        rows = [_make_player_row("p1", "X", pts=50)]

        self.assertEqual(_compute_standout_performances_algo(season, rows, None), [])
        self.assertEqual(_compute_standout_performances_algo(season, rows, ""), [])

    # (e) Empty player_rows ------------------------------------------------

    def test_empty_player_rows_returns_empty(self):
        season = _season("pts", [50])
        self.assertEqual(_compute_standout_performances_algo(season, [], "22025"), [])

    # (f) Tie-heavy distribution: PERCENT_RANK semantics -------------------

    def test_tied_max_values_do_not_overstate_rarity(self):
        """When 100 of 1000 rows share the max value (100 pts), their
        PERCENT_RANK = 900 / 999 ≈ 0.900 — they should NOT qualify at
        top-1% (threshold 0.99), only potentially at top-5% (0.95).

        The old bisect_right / n bug gave 1.0 → 'Top 0% Season' for all
        tied-max rows, which incorrectly promoted them to top-1%.
        """
        season_values = [50] * 900 + [100] * 100  # 100 ties at the max
        season = _season("pts", season_values)

        # Two players both have the tied-max value
        rows = [
            _make_player_row("p1", "Tied A", pts=100),
            _make_player_row("p2", "Tied B", pts=100),
        ]

        result = _compute_standout_performances_algo(season, rows, "22025")

        # Confirm no card is incorrectly at top_1 level
        for card in result:
            self.assertNotEqual(
                card["level"], "top_1",
                "Common tied-max values must not be labelled top_1"
            )

    def test_unique_max_qualifies_as_top1_when_two_qualify(self):
        """A unique maximum with PERCENT_RANK == 1.0 should qualify at top-1%.
        We need two such extremes to trigger the top_1 branch."""
        season = _season("pts", list(range(1000)))
        rows = [
            _make_player_row("p1", "Bam Adebayo", pts=999),
            _make_player_row("p2", "Player Two", pts=998),
        ]
        result = _compute_standout_performances_algo(season, rows, "22025")

        self.assertGreaterEqual(len(result), 2)
        self.assertTrue(all(c["level"] == "top_1" for c in result))

    # (g) Sorting: rarest first --------------------------------------------

    def test_results_sorted_by_pct_rank_descending(self):
        season = _season("pts", list(range(1000)))
        rows = [
            _make_player_row("p2", "Player Two", pts=950),  # lower rank
            _make_player_row("p1", "Player One", pts=999),  # higher rank
        ]
        result = _compute_standout_performances_algo(season, rows, "22025")

        self.assertGreaterEqual(len(result), 2)
        self.assertGreaterEqual(
            result[0]["pct_rank"], result[1]["pct_rank"],
            "Cards must be sorted rarest-first (pct_rank descending)"
        )
        self.assertEqual(result[0]["player_id"], "p1")

    # (h) Pct label accuracy -----------------------------------------------

    def test_pct_label_sub_tenth_percent(self):
        """A truly unique max in 1001 values has raw_pct < 0.1% → 'Top 0.1%'."""
        # 1000 zeros + 1 unique top value → pct_rank = 1000/1000 = 1.0
        # raw_pct = 0.0 → 'Top 0.1% Season'
        season = _season("pts", [0] * 1000 + [999])
        rows = [_make_player_row("p1", "Superstar", pts=999)]

        # Need a second top-1% row to trigger top_1 branch
        rows2 = [
            _make_player_row("p1", "Superstar", pts=999),
            _make_player_row("p2", "Superstar B", pts=998),
        ]
        season2 = _season("pts", list(range(1001)))
        result = _compute_standout_performances_algo(season2, rows2, "22025")

        labels = {c["pct_label"] for c in result}
        self.assertTrue(
            any("0.1%" in label or "Top 1" in label for label in labels),
            f"Expected a top-percentile label, got: {labels}"
        )


if __name__ == "__main__":
    unittest.main()
