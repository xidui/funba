"""Natural-language ranking for metrics catalog search."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
from collections.abc import Callable
from pathlib import Path

from db.ai_usage import extract_provider_usage
from db.llm_models import ensure_model_available, env_default_llm_model, provider_for_model

EMBEDDING_MODEL = "text-embedding-3-large"
EMBEDDING_PRERANK_TOP_K = 40
_EMBEDDING_BATCH = 256
_CACHE_DIR = Path(os.environ.get("FUNBA_CACHE_DIR", str(Path.home() / ".cache" / "funba")))
_EMBEDDINGS_PATH = _CACHE_DIR / "metric_embeddings.json"
_embeddings_cache: dict | None = None
_embeddings_lock = threading.Lock()

_FIELD_ORDER = (
    "key",
    "scope",
    "category",
    "career",
    "supports_career",
    "min_sample",
    "career_min_sample",
    "rank_order",
    "name",
    "name_zh",
    "description",
    "description_zh",
)
_LONG_TEXT_FIELDS = {"expression", "module_doc", "definition_json", "code_python", "source_excerpt"}
_EXCLUDED_SEARCH_FIELDS = {
    "code_python",
    "expression",
    "module_doc",
    "definition_json",
    "source_excerpt",
}
_MAX_FIELD_CHARS = 2400
_MAX_DOCUMENT_CHARS = 800


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
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
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

    def _append(key: str) -> None:
        if key in _EXCLUDED_SEARCH_FIELDS:
            return
        text = _stringify_candidate_value(key, candidate.get(key))
        if text:
            lines.append(f"{key}: {text}")

    for key in _FIELD_ORDER:
        _append(key)

    document = "\n".join(lines)
    return _truncate_text(document, _MAX_DOCUMENT_CHARS)


def _candidate_embedding_text(candidate: dict) -> str:
    parts: list[str] = []
    for k in ("name", "name_zh", "scope", "category", "description", "description_zh"):
        v = candidate.get(k)
        if v:
            parts.append(str(v).strip())
    return " | ".join(parts)


def _hash_embedding_text(text: str) -> str:
    return hashlib.sha256(f"{EMBEDDING_MODEL}::{text}".encode("utf-8")).hexdigest()[:32]


def _load_embeddings_cache() -> dict:
    global _embeddings_cache
    if _embeddings_cache is not None:
        return _embeddings_cache
    fresh = {"model": EMBEDDING_MODEL, "entries": {}}
    if _EMBEDDINGS_PATH.exists():
        try:
            data = json.loads(_EMBEDDINGS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("model") == EMBEDDING_MODEL and isinstance(data.get("entries"), dict):
                fresh = data
        except (OSError, json.JSONDecodeError):
            pass
    _embeddings_cache = fresh
    return _embeddings_cache


def _save_embeddings_cache(data: dict) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = _EMBEDDINGS_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    tmp.replace(_EMBEDDINGS_PATH)


def _ensure_candidate_embeddings(candidates: list[dict]) -> dict[str, list[float]]:
    with _embeddings_lock:
        cache = _load_embeddings_cache()
        entries = cache["entries"]

        result: dict[str, list[float]] = {}
        needed: list[tuple[str, str, str]] = []
        for cand in candidates:
            key = cand["key"]
            text = _candidate_embedding_text(cand)
            if not text:
                continue
            h = _hash_embedding_text(text)
            stored = entries.get(key)
            if isinstance(stored, dict) and stored.get("hash") == h and isinstance(stored.get("vector"), list):
                result[key] = stored["vector"]
            else:
                needed.append((key, text, h))

        dirty = False
        if needed:
            import openai

            client = openai.OpenAI()
            for i in range(0, len(needed), _EMBEDDING_BATCH):
                chunk = needed[i : i + _EMBEDDING_BATCH]
                resp = client.embeddings.create(
                    model=EMBEDDING_MODEL,
                    input=[text for _, text, _ in chunk],
                )
                for (key, _text, h), item in zip(chunk, resp.data):
                    vec = list(item.embedding)
                    entries[key] = {"hash": h, "vector": vec}
                    result[key] = vec
            dirty = True

        catalog_keys = {c["key"] for c in candidates}
        stale_keys = [k for k in entries if k not in catalog_keys]
        if stale_keys:
            for k in stale_keys:
                entries.pop(k, None)
            dirty = True

        if dirty:
            _save_embeddings_cache(cache)

    return result


def _embed_query(query: str) -> list[float]:
    import openai

    client = openai.OpenAI()
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=[query])
    return list(resp.data[0].embedding)


def _cosine(a: list[float], b: list[float]) -> float:
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / math.sqrt(na * nb)


def _prerank_with_embeddings(query: str, candidates: list[dict], top_k: int) -> list[dict]:
    if len(candidates) <= top_k:
        return candidates
    try:
        cand_vectors = _ensure_candidate_embeddings(candidates)
        q_vec = _embed_query(query)
    except Exception:
        return candidates
    scored: list[tuple[float, dict]] = []
    for cand in candidates:
        vec = cand_vectors.get(cand["key"])
        if vec is None:
            continue
        scored.append((_cosine(q_vec, vec), cand))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [cand for _, cand in scored[:top_k]]


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

    candidates = _prerank_with_embeddings(query, candidates, EMBEDDING_PRERANK_TOP_K)

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
        "Each metric key must appear AT MOST ONCE in the output. Never repeat a key "
        "even with a different reason.\n"
        "Prefer semantic relevance over keyword overlap.\n"
        "A strong match may come from the metric description, expression, rule definition, "
        "implementation details, min sample, career support, or ranking direction.\n\n"
        "Metric candidates with detailed dossiers:\n"
        f"{json.dumps(candidate_docs, ensure_ascii=False)}"
    )

    selected_model = ensure_model_available(selected_model)

    provider = provider_for_model(selected_model)
    if provider == "openai":
        import openai

        client = openai.OpenAI()
        response = client.chat.completions.create(
            model=selected_model,
            max_completion_tokens=400,
            temperature=0,
            reasoning_effort="none",
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
    seen_keys: set[str] = set()
    for item in parsed:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        if not key or key not in allowed_keys or key in seen_keys:
            continue
        seen_keys.add(key)
        results.append({
            "key": key,
            "reason": str(item.get("reason", "")).strip() or "Relevant match.",
        })
        if len(results) >= limit:
            break

    if not results:
        raise ValueError("Metric search returned no valid matches.")

    return results
