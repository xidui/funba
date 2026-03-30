"""Natural-language ranking for metrics catalog search."""
from __future__ import annotations

import json
import re
from collections.abc import Callable

from db.ai_usage import extract_provider_usage
from db.llm_models import ensure_model_available, env_default_llm_model, provider_for_model

_FIELD_ORDER = (
    "key",
    "name",
    "description",
    "scope",
    "category",
    "status",
    "source_type",
    "group_key",
    "time_scope",
    "min_sample",
    "career_min_sample",
    "supports_career",
    "career",
    "incremental",
    "rank_order",
    "result_count",
    "expression",
    "module_doc",
    "definition_json",
    "source_excerpt",
)
_LONG_TEXT_FIELDS = {"expression", "module_doc", "definition_json", "code_python", "source_excerpt"}
_EXCLUDED_SEARCH_FIELDS = {"code_python"}
_MAX_FIELD_CHARS = 2400
_MAX_DOCUMENT_CHARS = 8000


def _truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 15].rstrip() + " ...[truncated]"


def _stringify_candidate_value(key: str, value) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=True, sort_keys=True)
    else:
        text = str(value)
    text = text.strip()
    if not text:
        return ""
    if key in _LONG_TEXT_FIELDS:
        text = _truncate_text(text, _MAX_FIELD_CHARS)
    return text


def _candidate_search_document(candidate: dict) -> str:
    lines: list[str] = []
    seen: set[str] = set()

    def _append(key: str) -> None:
        seen.add(key)
        if key in _EXCLUDED_SEARCH_FIELDS:
            return
        text = _stringify_candidate_value(key, candidate.get(key))
        if text:
            lines.append(f"{key}: {text}")

    for key in _FIELD_ORDER:
        _append(key)

    for key in sorted(candidate):
        if key in seen:
            continue
        _append(key)

    document = "\n".join(lines)
    return _truncate_text(document, _MAX_DOCUMENT_CHARS)


def rank_metrics(
    query: str,
    candidates: list[dict],
    limit: int = 8,
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
) -> list[dict]:
    """Return ranked metric keys + reasons using an LLM."""
    if not query.strip():
        return []

    selected_model = model or env_default_llm_model()
    if not selected_model:
        raise ValueError("Metric search requires OPENAI_API_KEY or ANTHROPIC_API_KEY.")

    candidate_docs = [
        {
            "key": candidate["key"],
            "document": _candidate_search_document(candidate),
        }
        for candidate in candidates
    ]

    prompt = (
        "You rank existing NBA metrics by relevance to a user's natural-language query.\n"
        f"User query: {query}\n\n"
        "Return JSON only as an array of objects: "
        '[{"key":"metric_key","reason":"short reason"}, ...]\n'
        f"Return at most {limit} results.\n"
        "Prefer semantic relevance over keyword overlap.\n"
        "A strong match may come from the metric description, expression, rule definition, "
        "implementation details, min sample, career support, or ranking direction.\n\n"
        "Metric candidates with detailed dossiers:\n"
        f"{json.dumps(candidate_docs, ensure_ascii=True)}"
    )

    selected_model = ensure_model_available(selected_model)

    provider = provider_for_model(selected_model)
    if provider == "openai":
        import openai

        client = openai.OpenAI()
        response = client.chat.completions.create(
            model=selected_model,
            max_completion_tokens=1200,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        if usage_recorder:
            usage_recorder(extract_provider_usage(provider, response, selected_model))
        raw = response.choices[0].message.content.strip()
    else:
        import anthropic

        client = anthropic.Anthropic()
        message = client.messages.create(
            model=selected_model,
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        if usage_recorder:
            usage_recorder(extract_provider_usage(provider, message, selected_model))
        raw = message.content[0].text.strip()

    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("Metric search returned invalid JSON.")

    if not isinstance(parsed, list):
        raise ValueError("Metric search returned a non-list response.")

    allowed_keys = {candidate["key"] for candidate in candidates}
    results = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        if not key or key not in allowed_keys:
            continue
        results.append({
            "key": key,
            "reason": str(item.get("reason", "")).strip() or "Relevant match.",
        })
        if len(results) >= limit:
            break

    if not results:
        raise ValueError("Metric search returned no valid matches.")

    return results
