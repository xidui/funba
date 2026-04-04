"""Helpers for storing agent-prepared image assets in Funba-managed media.

Funba does not generate, search, or review image content here. Agents are
responsible for collecting or creating image assets first, then passing local
file paths into Funba's content API for storage.
"""
from __future__ import annotations

import shutil
from pathlib import Path

MEDIA_ROOT = Path(__file__).resolve().parent.parent / "media" / "social_posts"


def ensure_post_media_dir(post_id: int) -> Path:
    directory = MEDIA_ROOT / str(post_id)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _image_suffix(source: Path) -> str:
    suffix = source.suffix.lower()
    return suffix or ".png"


def _unique_destination(directory: Path, slot: str, suffix: str) -> Path:
    candidate = directory / f"{slot}{suffix}"
    if not candidate.exists():
        return candidate
    index = 1
    while True:
        candidate = directory / f"{slot}_{index}{suffix}"
        if not candidate.exists():
            return candidate
        index += 1


def store_prepared_image(source_path: str, *, post_id: int, slot: str) -> str:
    """Copy one agent-prepared local image into Funba-managed post media."""
    source = Path(source_path).expanduser()
    if not source.exists() or not source.is_file():
        raise FileNotFoundError(f"Prepared image file not found: {source}")

    out_dir = ensure_post_media_dir(post_id)
    destination = _unique_destination(out_dir, slot, _image_suffix(source))

    try:
        same_file = source.resolve() == destination.resolve()
    except FileNotFoundError:
        same_file = False

    if not same_file:
        shutil.copy2(source, destination)
    return str(destination)
