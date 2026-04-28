"""Thumbnail generation for Funba media assets.

Every image we save (AI-generated hero poster, agent-prepared social post image)
gets a sibling `.thumb.webp` that the home feed and other listing UIs serve
instead of the multi-MB original. The original PNG is kept for full-size views.

Naming: `foo.png` -> `foo.thumb.webp`. The `.thumb.webp` suffix lets the URL
builder swap formats with a single suffix replacement and lets the backfill
script tell originals from thumbs at a glance.
"""
from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

THUMB_SUFFIX = ".thumb.webp"
DEFAULT_MAX_SIZE = (600, 600)
DEFAULT_QUALITY = 80

# Source extensions we know how to thumbnail. Anything else (txt sidecars,
# already-webp thumbs, etc.) is silently skipped by walk-style callers.
SOURCE_EXTS = frozenset({".png", ".jpg", ".jpeg"})


def thumbnail_path_for(src_path: Path | str) -> Path:
    """Return the conventional thumbnail path for a source image."""
    src = Path(src_path)
    return src.with_suffix(THUMB_SUFFIX) if src.suffix.lower() != ".webp" else src.with_name(src.stem + THUMB_SUFFIX)


def is_thumbnail(path: Path | str) -> bool:
    return str(path).endswith(THUMB_SUFFIX)


def make_thumbnail(
    src_path: Path | str,
    *,
    force: bool = False,
    max_size: tuple[int, int] = DEFAULT_MAX_SIZE,
    quality: int = DEFAULT_QUALITY,
) -> Path | None:
    """Generate a `.thumb.webp` next to `src_path`.

    Returns the thumbnail path on success, None on skip/failure. Idempotent:
    if the thumb already exists and is newer than the source, returns it
    immediately. Pass `force=True` to regenerate.

    Errors are logged and swallowed — callers (image generation pipelines)
    must not break because of a thumbnail hiccup.
    """
    src = Path(src_path)
    if not src.exists() or not src.is_file():
        return None
    if is_thumbnail(src):
        return None
    if src.suffix.lower() not in SOURCE_EXTS:
        return None

    dest = thumbnail_path_for(src)
    if not force and dest.exists():
        try:
            if dest.stat().st_mtime >= src.stat().st_mtime and dest.stat().st_size > 0:
                return dest
        except OSError:
            pass

    try:
        from PIL import Image
    except ImportError:
        logger.warning("Pillow not installed; cannot generate thumbnail for %s", src)
        return None

    try:
        with Image.open(src) as img:
            img.load()
            if img.mode in ("RGBA", "LA", "P"):
                img = img.convert("RGB")
            img.thumbnail(max_size, Image.Resampling.LANCZOS)
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            img.save(tmp, "WEBP", quality=quality, method=6)
            tmp.replace(dest)
        return dest
    except Exception:
        logger.exception("thumbnail: failed to generate %s from %s", dest, src)
        try:
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return None
