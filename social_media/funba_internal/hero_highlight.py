"""Funba home-feed variant for hero card social posts.

Image-dominant: the rendered poster (slot="poster") carries the value,
ranking and top leaderboard visually, so the body text is just a single
title line + Source / Game deep links. Public template splices
[[IMAGE:slot=poster]] into the rendered card.
"""
from __future__ import annotations

from typing import Protocol


POSTER_SLOT_TAG = "[[IMAGE:slot=poster]]"


class HeroHighlightCardLike(Protocol):
    metric_name: str
    value_text: str
    value_time_label: str | None
    metric_url: str
    game_url: str
    matchup: str
    entity_label: str | None


def _clean(value: str) -> str:
    return " ".join(str(value or "").split())


def render_hero_highlight(card: HeroHighlightCardLike) -> str:
    value_text = _clean(card.value_text)
    if card.value_time_label:
        value_text = f"{value_text} ({card.value_time_label})"

    headline_left = _clean(card.entity_label or card.matchup)
    metric_name = _clean(card.metric_name)
    title = f"{headline_left} — {metric_name}" if headline_left else metric_name
    if value_text:
        title = f"{title}: {value_text}"

    lines = [POSTER_SLOT_TAG, "", title, "", f"Source: {card.metric_url}", f"Game: {card.game_url}"]
    return "\n".join(lines).strip()
