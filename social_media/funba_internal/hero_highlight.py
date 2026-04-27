"""Funba home-feed variant for hero card social posts.

Compared to the Twitter variant, this one is image-dominant: the rendered
poster (slot="poster") carries most of the message, so the body text is
short — title, value + ranking, top 3 list, deep links. Public template
splices [[IMAGE:slot=poster]] into the rendered card.
"""
from __future__ import annotations

import re
from typing import Protocol


POSTER_SLOT_TAG = "[[IMAGE:slot=poster]]"


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


def _clean(value: str) -> str:
    return " ".join(str(value or "").split())


def _compact_top_result(value: str) -> str:
    text = _clean(value)
    text = re.sub(r"\s+\([A-Z]{2,4}\)(?=\s+-)", "", text)
    text = re.sub(r"\s+@\s+", " @ ", text)
    return text


def render_hero_highlight(card: HeroHighlightCardLike) -> str:
    value_text = _clean(card.value_text)
    if card.value_time_label:
        value_text = f"{value_text} ({card.value_time_label})"

    lines: list[str] = [POSTER_SLOT_TAG, ""]
    headline_left = card.entity_label or card.matchup
    lines.append(f"{_clean(headline_left)} — {_clean(card.metric_name)}")
    lines.append(f"{value_text} · {_clean(card.rank_text)}")

    if card.top_results:
        lines.extend(["", "Top 3:"])
        lines.extend(_compact_top_result(result) for result in card.top_results[:3])

    lines.extend(["", f"Game: {card.game_url}"])
    lines.append(f"Source: {card.metric_url}")
    return "\n".join(lines).strip()
