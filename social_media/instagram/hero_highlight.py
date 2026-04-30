"""Instagram caption variant for hero card social posts.

IG renders the square hero poster (slot=poster_ig) as the visual; the
caption stays short because IG doesn't make URLs clickable inline. We
add a few hashtags for discoverability and skip the deep links the
twitter / funba variants carry.
"""
from __future__ import annotations

from typing import Protocol


POSTER_SLOT_TAG = "[[IMAGE:slot=poster_ig]]"


class HeroHighlightCardLike(Protocol):
    metric_name: str
    value_text: str
    value_time_label: str | None
    matchup: str
    entity_label: str | None


def _clean(value: str) -> str:
    return " ".join(str(value or "").split())


def _hashtag(value: str | None) -> str:
    cleaned = "".join(ch for ch in str(value or "") if ch.isalnum())
    return f"#{cleaned}" if cleaned else ""


def render_hero_highlight(card: HeroHighlightCardLike) -> str:
    value_text = _clean(card.value_text)
    if card.value_time_label:
        value_text = f"{value_text} ({card.value_time_label})"

    headline_left = _clean(card.entity_label or card.matchup)
    metric_name = _clean(card.metric_name)
    title = f"{headline_left} — {metric_name}" if headline_left else metric_name
    if value_text:
        title = f"{title}: {value_text}"

    matchup = _clean(card.matchup)

    tags: list[str] = ["#NBA"]
    entity_tag = _hashtag(card.entity_label)
    if entity_tag and entity_tag.lower() != "#nba":
        tags.append(entity_tag)
    tags.append("#funba")

    parts = [POSTER_SLOT_TAG, "", title]
    if matchup and matchup != headline_left:
        parts.extend(["", matchup])
    parts.extend(["", " ".join(tags)])
    return "\n".join(parts).strip()
