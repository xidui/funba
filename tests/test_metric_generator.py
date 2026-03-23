from pathlib import Path
import json
import sys
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from metrics.framework import generator


def test_generate_edit_mode_includes_key_and_preserves_existing_metadata():
    existing = {
        "key": "lowest_quarter_score",
        "name": "Lowest Quarter Score",
        "description": "Lowest score in any quarter.",
        "scope": "game",
        "category": "record",
        "rank_order": "asc",
        "code": "class Demo:\n    key = 'lowest_quarter_score'\n",
    }

    with patch.object(
        generator,
        "_call_llm",
        return_value=json.dumps(
            {
                "name": "Different Name",
                "description": "Different description.",
                "scope": "team",
                "category": "scoring",
                "rank_order": "desc",
                "code": "class Demo:\n    key = 'lowest_regulation_quarter_score'\n",
            }
        ),
    ) as mock_call:
        spec = generator.generate("limit to regulation", existing=existing)

    prompt = mock_call.call_args.args[0][0]["content"]
    assert "key: lowest_quarter_score" in prompt
    assert "rank_order: asc" in prompt
    assert "Keep the key, name, description, scope, category, and rank_order exactly" in prompt
    assert spec["key"] == "lowest_quarter_score"
    assert spec["name"] == "Lowest Quarter Score"
    assert spec["description"] == "Lowest score in any quarter."
    assert spec["scope"] == "game"
    assert spec["category"] == "record"
    assert spec["rank_order"] == "asc"
    assert spec["responseType"] == "code"


def test_generate_returns_clarification_payload_without_code_validation():
    with patch.object(
        generator,
        "_call_llm",
        return_value=json.dumps(
            {
                "responseType": "clarification",
                "message": "min_sample is the minimum number of games required before the metric appears.",
            }
        ),
    ):
        response = generator.generate("What does min_sample do?")

    assert response == {
        "responseType": "clarification",
        "message": "min_sample is the minimum number of games required before the metric appears.",
    }


def test_generate_defaults_missing_response_type_to_code():
    with patch.object(
        generator,
        "_call_llm",
        return_value=json.dumps(
            {
                "name": "Demo Metric",
                "description": "Demo description.",
                "scope": "player",
                "category": "scoring",
                "rank_order": "desc",
                "code": "class Demo:\n    pass\n",
            }
        ),
    ):
        spec = generator.generate("Generate a demo metric")

    assert spec["responseType"] == "code"
    assert spec["name"] == "Demo Metric"
