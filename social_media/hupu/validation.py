"""Validators for Hupu posts."""

from __future__ import annotations

_HUPU_TITLE_MIN = 4
_HUPU_TITLE_MAX = 40


def validate_hupu_title(title: str | None) -> str | None:
    """Return an error message when the title can't be posted to Hupu, else None."""
    if not title:
        return f"Hupu title required (length {_HUPU_TITLE_MIN}-{_HUPU_TITLE_MAX})"
    length = len(title)
    if length < _HUPU_TITLE_MIN or length > _HUPU_TITLE_MAX:
        return (
            f"Hupu title length must be {_HUPU_TITLE_MIN}-{_HUPU_TITLE_MAX}; "
            f"current: {length}"
        )
    return None
