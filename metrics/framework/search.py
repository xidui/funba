"""Natural-language ranking for metrics catalog search."""
from __future__ import annotations

import json
import math
import re
import threading
from collections.abc import Callable

import numpy as np

from db.ai_usage import extract_provider_usage
from db.embeddings import (
    EMBEDDING_DIM,
    EMBEDDING_MODEL,
    blob_to_vector as _blob_to_vector,
    embed_query as _embed_query,
    embed_texts as _embed_texts,
    hash_embedding_text as _hash_embedding_text,
    vector_to_blob as _vector_to_blob,
)
from db.llm_models import ensure_model_available, env_default_llm_model, provider_for_model

EMBEDDING_PRERANK_TOP_K = 40
# Per-process cache of {metric_key: float32 ndarray}. Populated lazily from the
# DB; gunicorn preload warms this once so all workers inherit it via CoW.
_embedding_vectors: dict[str, np.ndarray] = {}
_embedding_lock = threading.Lock()

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


def _candidate_embedding_text(candidate: dict | object) -> str:
    """Build the text used to compute the embedding for a metric.

    Accepts either a catalog dict or a SQLAlchemy MetricDefinition row.
    Field set is intentionally narrow (name + descriptions + scope/category)
    so it stays stable across catalog dict shape changes.
    """
    parts: list[str] = []
    for k in ("name", "name_zh", "scope", "category", "description", "description_zh"):
        v = candidate.get(k) if isinstance(candidate, dict) else getattr(candidate, k, None)
        if v:
            parts.append(str(v).strip())
    return " | ".join(parts)


def update_metric_embedding(session, row) -> None:
    """Recompute and persist the embedding for a single MetricDefinition row.

    Call this from any write path that mutates name/scope/category/description
    (sync_metric_family is the central one). The caller is responsible for
    committing the session. No-op if the embedding text is empty.
    """
    text = _candidate_embedding_text(row)
    if not text:
        return
    text_hash = _hash_embedding_text(text)
    if (
        getattr(row, "embedding_model", None) == EMBEDDING_MODEL
        and getattr(row, "embedding_text_hash", None) == text_hash
        and getattr(row, "embedding", None)
    ):
        return  # unchanged
    vector = _embed_texts([text])[0]
    row.embedding = _vector_to_blob(vector)
    row.embedding_model = EMBEDDING_MODEL
    row.embedding_text_hash = text_hash
    # Refresh the in-memory cache for this process so the very next search
    # sees the new vector. Other workers will pick it up via DB on their next
    # missing-key fetch.
    _embedding_vectors[row.key] = _blob_to_vector(row.embedding)


def _load_embeddings_for_keys(session, keys: list[str]) -> None:
    """Fetch any missing embeddings from DB into the in-memory cache."""
    if not keys:
        return
    from db.models import MetricDefinition as _MD

    missing = [k for k in keys if k not in _embedding_vectors]
    if not missing:
        return
    rows = (
        session.query(_MD.key, _MD.embedding, _MD.embedding_model)
        .filter(_MD.key.in_(missing))
        .filter(_MD.embedding.isnot(None))
        .filter(_MD.embedding_model == EMBEDDING_MODEL)
        .all()
    )
    for row in rows:
        try:
            _embedding_vectors[row.key] = _blob_to_vector(row.embedding)
        except Exception:
            continue


def warm_embedding_cache(session) -> int:
    """Bulk-load every metric's embedding into the in-memory cache.

    Used by gunicorn's when_ready hook before workers fork so they all
    inherit the populated dict via copy-on-write. Returns count loaded.
    """
    from db.models import MetricDefinition as _MD

    rows = (
        session.query(_MD.key, _MD.embedding)
        .filter(_MD.embedding.isnot(None))
        .filter(_MD.embedding_model == EMBEDDING_MODEL)
        .all()
    )
    count = 0
    for row in rows:
        try:
            _embedding_vectors[row.key] = _blob_to_vector(row.embedding)
            count += 1
        except Exception:
            continue
    return count


def _prerank_with_embeddings(
    session,
    query: str,
    candidates: list[dict],
    top_k: int,
) -> list[dict]:
    if len(candidates) <= top_k:
        return candidates
    try:
        # Pull in any keys we haven't seen yet (new metrics created in another
        # worker). Skip virtual career siblings — they have no DB row and
        # fall back to the base metric's vector below.
        missing_real = [
            c["key"] for c in candidates
            if c["key"] not in _embedding_vectors and not c["key"].endswith("_career")
        ]
        if missing_real:
            _load_embeddings_for_keys(session, missing_real)
        q_vec = _embed_query(query)
    except Exception:
        return candidates
    scored: list[tuple[float, dict]] = []
    for cand in candidates:
        key = cand["key"]
        vec = _embedding_vectors.get(key)
        if vec is None and key.endswith("_career"):
            # Virtual career sibling — reuse the base metric's vector.
            # The career variant's name/description differ only by a "Career"
            # suffix, so the semantic meaning is essentially identical.
            vec = _embedding_vectors.get(key[: -len("_career")])
        if vec is None:
            continue
        # both vectors are unnormalized; cosine = dot / (|a| * |b|)
        denom = float(np.linalg.norm(q_vec)) * float(np.linalg.norm(vec))
        score = float(np.dot(q_vec, vec)) / denom if denom else 0.0
        scored.append((score, cand))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [cand for _, cand in scored[:top_k]]


def rank_metrics(
    query: str,
    candidates: list[dict],
    limit: int = 8,
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
    *,
    session=None,
    mode: str = "search",
) -> list[dict]:
    """Return ranked metric keys + reasons using an LLM.

    `session` is optional only because some legacy tests don't pass one;
    embedding prerank is skipped when it's missing, falling back to sending
    the full candidate list to the LLM.

    `mode`:
    - "search": broad relevance ranking for the search page. Raises
      ValueError if the LLM returns nothing parseable.
    - "similarity": strict duplicate-check for the create-metric flow.
      Empty list is a valid result (nothing is similar).
    """
    if not query.strip():
        return []
    if not candidates:
        return []
    if mode not in ("search", "similarity"):
        raise ValueError(f"Unknown rank_metrics mode: {mode}")

    selected_model = model or env_default_llm_model()
    if not selected_model:
        raise ValueError("Metric search requires OPENAI_API_KEY or ANTHROPIC_API_KEY.")

    if session is not None:
        candidates = _prerank_with_embeddings(session, query, candidates, EMBEDDING_PRERANK_TOP_K)

    candidate_docs = [
        {
            "key": candidate["key"],
            "document": _candidate_search_document(candidate),
        }
        for candidate in candidates
    ]

    if mode == "search":
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
    else:  # similarity
        prompt = (
            "You check whether any existing NBA metric already measures the same thing "
            "as what the user wants to create.\n"
            f"User's new metric description: {query}\n\n"
            "Return JSON only as an array of objects: "
            '[{"key":"metric_key","reason":"why it is a duplicate or near-duplicate"}, ...]\n'
            f"Return at most {limit} results.\n"
            "Be STRICT — only include metrics that genuinely measure the same or nearly "
            "the same thing. Minor keyword overlap is not enough; the intent must match.\n"
            "If nothing is similar, return an empty array: []\n"
            "Each metric key must appear AT MOST ONCE in the output.\n\n"
            "Existing metric candidates with detailed dossiers:\n"
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

    if not results and mode == "search":
        raise ValueError("Metric search returned no valid matches.")

    return results
