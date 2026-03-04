"""AI-based noteworthiness scoring using the Anthropic API (Claude)."""
from __future__ import annotations

import json
import logging
import os

from metrics.framework.base import MetricDefinition, MetricResult

logger = logging.getLogger(__name__)

_SCORE_THRESHOLD = float(os.getenv("FUNBA_NOTEWORTHINESS_THRESHOLD", "0.75"))


def score(
    result: MetricResult,
    metric_def: MetricDefinition,
    entity_name: str,
) -> tuple[float, str]:
    """Return (score 0–1, one-sentence reason) via Claude.

    Falls back to (0.5, fallback_msg) if the API is unavailable or the key
    is not configured.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.debug("ANTHROPIC_API_KEY not set; skipping AI scoring for %s", result.metric_key)
        return 0.5, "AI scoring unavailable (no API key)."

    try:
        import anthropic  # lazy import so the package is optional at startup

        value_str = (
            f"{result.value_num:.4f}" if result.value_num is not None else str(result.value_str)
        )
        context_str = json.dumps(result.context, indent=2) if result.context else "{}"

        prompt = (
            f"You are scoring the interestingness of an NBA statistic for a social-media post "
            f"or analyst report.\n\n"
            f"Metric: {metric_def.name}\n"
            f"Description: {metric_def.description}\n"
            f"Entity: {entity_name} (season: {result.season or 'N/A'})\n"
            f"Value: {value_str}\n"
            f"Context: {context_str}\n\n"
            f"Scoring guide:\n"
            f"  0.0–0.3 → routine/average, not worth highlighting\n"
            f"  0.3–0.6 → somewhat interesting, worth a footnote\n"
            f"  0.6–0.75 → notable, worth showing in a stats panel\n"
            f"  0.75–1.0 → remarkable, worthy of a social post or highlight\n\n"
            f"Reply with JSON only, no markdown:\n"
            f'  {{"score": <0.0 to 1.0>, "reason": "<one punchy sentence>"}}'
        )

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=128,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        data = json.loads(raw)
        return float(data["score"]), str(data["reason"])

    except Exception as exc:
        logger.warning("AI scoring failed for %s: %s", result.metric_key, exc)
        return 0.5, "AI scoring failed."


def is_notable(noteworthiness: float | None) -> bool:
    return noteworthiness is not None and noteworthiness >= _SCORE_THRESHOLD
