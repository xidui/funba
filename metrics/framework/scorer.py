"""AI-based noteworthiness scoring using the Anthropic API (Claude)."""
from __future__ import annotations

import json
import logging
import os

from metrics.framework.base import MetricDefinition, MetricResult

logger = logging.getLogger(__name__)

_SCORE_THRESHOLD = float(os.getenv("FUNBA_NOTEWORTHINESS_THRESHOLD", "0.75"))


def score_batch(
    items: list[tuple[MetricResult, MetricDefinition, str]],
) -> list[tuple[float, str]]:
    """Score a batch of metric results in a single API call.

    Args:
        items: list of (result, metric_def, entity_name)

    Returns:
        list of (score, reason) parallel to items.
        Falls back to (0.5, fallback_msg) for any item that fails.
    """
    fallback = [(0.5, "AI scoring unavailable (no API key).")]

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.debug("ANTHROPIC_API_KEY not set; skipping AI scoring.")
        return [(0.5, "AI scoring unavailable (no API key).") for _ in items]

    if not items:
        return []

    try:
        import anthropic

        entries = []
        for i, (result, metric_def, entity_name) in enumerate(items):
            value_str = (
                f"{result.value_num:.4f}" if result.value_num is not None else str(result.value_str)
            )
            context_str = json.dumps(result.context, indent=2) if result.context else "{}"
            entries.append(
                f'[{i}] Metric: {metric_def.name} | '
                f'Entity: {entity_name} (season: {result.season or "N/A"}) | '
                f'Value: {value_str} | '
                f'Context: {context_str}'
            )

        prompt = (
            "You are scoring the interestingness of NBA statistics for analyst reports.\n\n"
            "Scoring guide:\n"
            "  0.0–0.3 → routine/average, not worth highlighting\n"
            "  0.3–0.6 → somewhat interesting, worth a footnote\n"
            "  0.6–0.75 → notable, worth showing in a stats panel\n"
            "  0.75–1.0 → remarkable, worthy of a social post or highlight\n\n"
            "Rate each of the following metrics and reply with a JSON array only, no markdown.\n"
            "Each element: {\"index\": <i>, \"score\": <0.0–1.0>, \"reason\": \"<one punchy sentence>\"}\n\n"
            + "\n".join(entries)
        )

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=100 * len(items),
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        data = json.loads(raw)

        # Build index-keyed lookup then return in order
        by_index = {d["index"]: (float(d["score"]), str(d["reason"])) for d in data}
        return [by_index.get(i, (0.5, "AI scoring missing for this item.")) for i in range(len(items))]

    except Exception as exc:
        logger.warning("Batch AI scoring failed: %s", exc)
        return [(0.5, "AI scoring failed.") for _ in items]


def is_notable(noteworthiness: float | None) -> bool:
    return noteworthiness is not None and noteworthiness >= _SCORE_THRESHOLD
