from __future__ import annotations

import re
from typing import Protocol


TWITTER_TCO_URL_LENGTH = 23
TWITTER_POST_LIMIT = 280
URL_RE = re.compile(r"https?://\S+")


class HeroHighlightCardLike(Protocol):
    metric_name: str
    value_text: str
    value_time_label: str | None
    rank_text: str
    top_results: tuple[str, ...]
    metric_url: str
    game_url: str
    matchup: str
    entity_label: str | None


def estimated_tweet_length(text: str) -> int:
    total = 0
    pos = 0
    for match in URL_RE.finditer(str(text or "")):
        total += len(text[pos : match.start()])
        total += TWITTER_TCO_URL_LENGTH
        pos = match.end()
    total += len(text[pos:])
    return total


def _clean(value: str) -> str:
    return " ".join(str(value or "").split())


def _truncate(value: str, limit: int) -> str:
    text = _clean(value)
    return text if len(text) <= limit else text[: max(limit - 1, 0)].rstrip() + "…"


def _compact_metric_value(value: str) -> str:
    text = _clean(value)
    text = re.sub(r"\s+run$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bscored\s+(\d+)\s+pts\b", r"\1 pts", text, flags=re.IGNORECASE)
    return text


def _build_title(card: HeroHighlightCardLike, *, label_limit: int, metric_limit: int) -> str:
    """One-line headline: '{entity} — {metric}: {value}'.

    Hero poster carries the full ranking + top-3 visually, so the tweet body
    only needs a single title line — readers scan the image for the data.
    """
    label = _truncate(card.entity_label or card.matchup, label_limit)
    metric_name = _truncate(card.metric_name, metric_limit)
    value_text = _compact_metric_value(card.value_text)
    if card.value_time_label:
        value_text = f"{value_text} ({card.value_time_label})"
    base = f"{label} — {metric_name}" if label else metric_name
    if value_text:
        return f"{base}: {value_text}"
    return base


def _twitter_lines(
    card: HeroHighlightCardLike,
    *,
    label_limit: int,
    metric_limit: int,
    include_game_link: bool = True,
) -> list[str]:
    lines = [_build_title(card, label_limit=label_limit, metric_limit=metric_limit)]
    lines.extend(["", f"Source: {card.metric_url}"])
    if include_game_link:
        lines.append(f"Game: {card.game_url}")
    return lines


def render_hero_highlight(card: HeroHighlightCardLike) -> str:
    for label_limit, metric_limit in ((48, 60), (32, 48), (24, 36), (20, 28)):
        candidate = "\n".join(
            _twitter_lines(card, label_limit=label_limit, metric_limit=metric_limit)
        ).strip()
        if estimated_tweet_length(candidate) <= TWITTER_POST_LIMIT:
            return candidate
    for label_limit, metric_limit in ((48, 60), (32, 48), (24, 36), (20, 28)):
        candidate = "\n".join(
            _twitter_lines(
                card,
                label_limit=label_limit,
                metric_limit=metric_limit,
                include_game_link=False,
            )
        ).strip()
        if estimated_tweet_length(candidate) <= TWITTER_POST_LIMIT:
            return candidate
    return "\n".join(
        _twitter_lines(
            card, label_limit=20, metric_limit=28, include_game_link=False
        )
    ).strip()
