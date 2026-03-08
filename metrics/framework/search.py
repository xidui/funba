"""Natural-language ranking for metrics catalog search."""
from __future__ import annotations

import json
import os
import re


def _tokenize(text: str) -> set[str]:
    return {tok for tok in re.findall(r"[a-z0-9]+", text.lower()) if len(tok) > 1}


def _heuristic_rank(query: str, candidates: list[dict], limit: int) -> list[dict]:
    q_tokens = _tokenize(query)
    ranked: list[tuple[float, dict]] = []
    for candidate in candidates:
        hay = " ".join(
            str(candidate.get(k, "") or "")
            for k in ("name", "description", "category", "scope", "key")
        )
        c_tokens = _tokenize(hay)
        overlap = len(q_tokens & c_tokens)
        phrase_bonus = 2 if query.lower() in hay.lower() else 0
        score = overlap + phrase_bonus
        if score <= 0:
            continue
        ranked.append((score, candidate))

    ranked.sort(key=lambda item: item[0], reverse=True)
    results = []
    for score, candidate in ranked[:limit]:
        results.append({
            "key": candidate["key"],
            "reason": f"Keyword overlap score {score}.",
        })
    return results


def rank_metrics(query: str, candidates: list[dict], limit: int = 8) -> list[dict]:
    """Return ranked metric keys + reasons using an LLM, with heuristic fallback."""
    openai_key = os.getenv("OPENAI_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    if not query.strip():
        return []

    if not openai_key and not anthropic_key:
        return _heuristic_rank(query, candidates, limit)

    prompt = (
        "You rank existing NBA metrics by relevance to a user's natural-language query.\n"
        f"User query: {query}\n\n"
        "Return JSON only as an array of objects: "
        '[{"key":"metric_key","reason":"short reason"}, ...]\n'
        f"Return at most {limit} results.\n"
        "Prefer semantic relevance over keyword overlap.\n\n"
        "Metric candidates:\n"
        f"{json.dumps(candidates, ensure_ascii=True)}"
    )

    if openai_key:
        import openai

        client = openai.OpenAI(api_key=openai_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.choices[0].message.content.strip()
    else:
        import anthropic

        client = anthropic.Anthropic(api_key=anthropic_key)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return _heuristic_rank(query, candidates, limit)

    if not isinstance(parsed, list):
        return _heuristic_rank(query, candidates, limit)

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

    return results or _heuristic_rank(query, candidates, limit)
