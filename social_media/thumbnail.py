"""Thumbnail generation for Funba media assets.

Every image we save (AI-generated hero poster, agent-prepared social post image)
gets a sibling `.thumb.webp` that the home feed and other listing UIs serve
instead of the multi-MB original. The original PNG is kept for full-size views.

Naming: `foo.png` -> `foo.thumb.webp`. The `.thumb.webp` suffix lets the URL
builder swap formats with a single suffix replacement and lets the backfill
script tell originals from thumbs at a glance.
"""
from __future__ import annotations

import colorsys
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

THUMB_SUFFIX = ".thumb.webp"
DEFAULT_MAX_SIZE = (600, 600)
DEFAULT_QUALITY = 80


def dominant_color_hex(src_path: Path | str) -> str | None:
    """Extract a vibrant representative color from an image.

    Strategy: quantize the image to 8 palette colors, then pick the one
    with the best (count × saturation) score, skipping near-black/white
    extremes. Falls back to the highest-count color if every cluster is
    desaturated. Returns "#rrggbb" or None on failure.
    """
    try:
        from PIL import Image
    except ImportError:
        return None
    try:
        with Image.open(src_path) as img:
            img.load()
            small = img.convert("RGB").resize((128, 128))
            quantized = small.quantize(colors=8, method=Image.Quantize.MEDIANCUT)
            palette = quantized.getpalette() or []
            counts = quantized.getcolors() or []  # list of (count, palette_idx)
        if not counts or not palette:
            return None

        scored: list[tuple[float, int, tuple[int, int, int]]] = []
        for count, idx in counts:
            r = palette[idx * 3]
            g = palette[idx * 3 + 1]
            b = palette[idx * 3 + 2]
            h, l, s = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
            # Skip near-black / near-white / near-grey clusters as ambient
            # backdrop candidates — those make a lifeless gradient.
            if l < 0.12 or l > 0.88:
                continue
            scored.append((count * (s + 0.05), count, (r, g, b)))

        if not scored:
            # Fall back to the most-frequent non-extreme color, even if grey.
            for count, idx in counts:
                r = palette[idx * 3]
                g = palette[idx * 3 + 1]
                b = palette[idx * 3 + 2]
                _, l, _ = colorsys.rgb_to_hls(r / 255, g / 255, b / 255)
                if 0.1 < l < 0.9:
                    return f"#{r:02x}{g:02x}{b:02x}"
            return None

        scored.sort(reverse=True)
        r, g, b = scored[0][2]
        return f"#{r:02x}{g:02x}{b:02x}"
    except Exception:
        logger.exception("dominant_color: failed for %s", src_path)
        return None

# Source extensions we know how to thumbnail. Anything else (txt sidecars,
# already-webp thumbs, etc.) is silently skipped by walk-style callers.
SOURCE_EXTS = frozenset({".png", ".jpg", ".jpeg"})


def thumbnail_path_for(src_path: Path | str) -> Path:
    """Return the conventional thumbnail path for a source image."""
    src = Path(src_path)
    return src.with_suffix(THUMB_SUFFIX) if src.suffix.lower() != ".webp" else src.with_name(src.stem + THUMB_SUFFIX)


def color_sidecar_path_for(src_path: Path | str) -> Path:
    """Return the sidecar path that stores an image's dominant color hex."""
    return Path(src_path).with_suffix(".color.txt")


def ensure_dominant_color_sidecar(src_path: Path | str, *, force: bool = False) -> str | None:
    """Compute the image's dominant color (once) and cache to a sidecar txt.

    Returns the hex string or None on failure. Idempotent: subsequent calls
    just read the sidecar, so the page render path can call this freely.
    """
    src = Path(src_path)
    if not src.exists() or not src.is_file():
        return None
    if is_thumbnail(src) or src.suffix.lower() not in SOURCE_EXTS:
        return None

    sidecar = color_sidecar_path_for(src)
    if not force and sidecar.exists():
        try:
            text = sidecar.read_text(encoding="utf-8").strip()
            if text.startswith("#") and len(text) == 7:
                return text
        except OSError:
            pass

    color = dominant_color_hex(src)
    if not color:
        return None
    try:
        sidecar.write_text(color, encoding="utf-8")
    except OSError:
        logger.exception("color sidecar: failed to write %s", sidecar)
    return color


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
        # Cache the dominant color too, while we already touched the image.
        ensure_dominant_color_sidecar(src)
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
