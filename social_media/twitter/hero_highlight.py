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


def estimated_tweet_length(text: str) -> int:
    total = 0
    pos = 0
    for match in URL_RE.finditer(str(text or "")):
        total += len(text[pos : match.start()])
        total += TWITTER_TCO_URL_LENGTH
        pos = match.end()
    total += len(text[pos:])
    return total


def _truncate(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: max(limit - 1, 0)].rstrip() + "…"


def _compact_rank_text(value: str) -> str:
    text = " ".join(str(value or "").split())
    text = text.replace(" / ", "/")
    text = text.replace("(", "").replace(")", "")
    text = text.replace("All-time", "all-time")
    return text


def _compact_metric_value(value: str) -> str:
    text = " ".join(str(value or "").split())
    text = re.sub(r"\s+run$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bscored\s+(\d+)\s+pts\b", r"\1 pts", text, flags=re.IGNORECASE)
    return text


def _compact_top_result(value: str) -> str:
    text = " ".join(str(value or "").split())
    text = re.sub(r"\s+\([A-Z]{2,4}\)(?=\s+-)", "", text)
    text = re.sub(r"\s+@\s+", "@", text)
    text = re.sub(r"\bscored\s+(\d+)\s+pts\b", r"\1 pts", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+run$", "", text, flags=re.IGNORECASE)
    return text


def _truncate_top_result(value: str, limit: int) -> str:
    text = _compact_top_result(value)
    if len(text) <= limit:
        return text
    prefix, sep, suffix = text.partition(" - ")
    if not sep:
        return _truncate(text, limit)
    reserved = len(sep) + len(suffix)
    prefix_limit = max(limit - reserved, 8)
    return f"{_truncate(prefix, prefix_limit)}{sep}{suffix}"


def _twitter_lines(
    card: HeroHighlightCardLike,
    *,
    top_limit: int,
    include_game_link: bool = True,
) -> list[str]:
    value_text = _compact_metric_value(card.value_text)
    if card.value_time_label:
        value_text = f"{value_text} ({card.value_time_label})"
    lines = [
        f"Data: {_truncate(card.metric_name, 36)} = {value_text}",
        f"Ranking: {_compact_rank_text(card.rank_text)}",
    ]
    if card.top_results:
        lines.extend(["", "Top 3:"])
        lines.extend(_truncate_top_result(result, top_limit) for result in card.top_results[:3])
    lines.extend(["", f"Source: {card.metric_url}"])
    if include_game_link:
        lines.append(f"Game: {card.game_url}")
    return lines


def render_hero_highlight(card: HeroHighlightCardLike) -> str:
    for top_limit in (80, 56, 40, 34, 28, 22):
        candidate = "\n".join(_twitter_lines(card, top_limit=top_limit)).strip()
        if estimated_tweet_length(candidate) <= TWITTER_POST_LIMIT:
            return candidate
    for top_limit in (80, 56, 40, 34, 28, 22, 18):
        candidate = "\n".join(_twitter_lines(card, top_limit=top_limit, include_game_link=False)).strip()
        if estimated_tweet_length(candidate) <= TWITTER_POST_LIMIT:
            return candidate
    return "\n".join(_twitter_lines(card, top_limit=18, include_game_link=False)).strip()
