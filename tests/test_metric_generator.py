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
                "code": "class Demo:\n    key = 'lowest_regulation_quarter_score'\n",
            }
        ),
    ) as mock_call:
        spec = generator.generate("limit to regulation", existing=existing)

    prompt = mock_call.call_args.args[0][0]["content"]
    assert "key: lowest_quarter_score" in prompt
    assert "Keep the key, name, description, scope, and category exactly" in prompt
    assert spec["key"] == "lowest_quarter_score"
    assert spec["name"] == "Lowest Quarter Score"
    assert spec["description"] == "Lowest score in any quarter."
    assert spec["scope"] == "game"
    assert spec["category"] == "record"
