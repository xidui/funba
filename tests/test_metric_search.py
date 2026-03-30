from pathlib import Path
import sys
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from metrics.framework.search import _candidate_search_document, rank_metrics


def test_candidate_search_document_includes_rich_fields_and_truncates_long_text():
    doc = _candidate_search_document(
        {
            "key": "clutch_fg_pct",
            "name": "Clutch FG%",
            "description": "Field goal percentage in clutch time.",
            "scope": "player",
            "category": "efficiency",
            "min_sample": 5,
            "supports_career": True,
            "expression": "shooting in the final five minutes with score within five" * 80,
            "source_excerpt": "late game shot profile " * 300,
            "code_python": "class Demo:\n    pass\n" * 400,
        }
    )

    assert "min_sample: 5" in doc
    assert "supports_career: yes" in doc
    assert "expression:" in doc
    assert "source_excerpt:" in doc
    assert "code_python:" not in doc
    assert "[truncated]" in doc


def test_rank_metrics_requires_llm_key():
    with patch.dict("os.environ", {}, clear=True):
        try:
            rank_metrics("after a missed shot", [{"key": "cold_streak_recovery"}], limit=8)
        except ValueError as exc:
            assert "requires OPENAI_API_KEY or ANTHROPIC_API_KEY" in str(exc)
        else:
            raise AssertionError("Expected ValueError when no LLM API key is configured")
