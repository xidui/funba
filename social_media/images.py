"""Image resolution for SocialPost image pool.

Supports three generation types:
- ai_generated: OpenAI DALL-E image generation
- web_search: Bing Image Search API
- screenshot: Playwright page capture (delegates to hupu/post.py helper)
"""
from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

MEDIA_ROOT = Path(__file__).resolve().parent.parent / "media" / "social_posts"


def ensure_post_media_dir(post_id: int) -> Path:
    d = MEDIA_ROOT / str(post_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def resolve_image(spec: dict, *, post_id: int, slot: str) -> str:
    """Generate or download an image from *spec*, return local file path.

    Raises on failure so the caller can record error_message.
    """
    image_type = spec.get("type", "")
    out_dir = ensure_post_media_dir(post_id)
    out_path = out_dir / f"{slot}.png"

    if image_type == "ai_generated":
        _generate_openai(spec["prompt"], str(out_path), spec.get("style"))
    elif image_type == "web_search":
        _search_and_download(spec["query"], str(out_path))
    elif image_type == "screenshot":
        _capture_screenshot(spec["target"], str(out_path))
    else:
        raise ValueError(f"Unknown image type: {image_type}")

    return str(out_path)


# ---------------------------------------------------------------------------
# OpenAI DALL-E
# ---------------------------------------------------------------------------

def _generate_openai(prompt: str, output_path: str, style: str | None = None) -> None:
    """Call OpenAI Images API (DALL-E 3) and save result."""
    from openai import OpenAI

    client = OpenAI()  # uses OPENAI_API_KEY env var
    resp = client.images.generate(
        model="dall-e-3",
        prompt=prompt,
        n=1,
        size="1024x1024",
        quality="standard",
        style=style or "vivid",
    )
    image_url = resp.data[0].url
    _download_file(image_url, output_path)
    logger.info("AI image generated: %s -> %s", prompt[:60], output_path)


# ---------------------------------------------------------------------------
# Web image search (Bing Image Search API)
# ---------------------------------------------------------------------------

BING_SEARCH_ENDPOINT = "https://api.bing.microsoft.com/v7.0/images/search"


def _search_and_download(query: str, output_path: str) -> None:
    """Search Bing Images and download the first suitable result."""
    api_key = os.getenv("BING_IMAGE_SEARCH_KEY")
    if not api_key:
        raise RuntimeError("BING_IMAGE_SEARCH_KEY not set — cannot perform web image search")

    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {
        "q": query,
        "count": 5,
        "imageType": "Photo",
        "safeSearch": "Moderate",
        "minWidth": 600,
        "minHeight": 400,
    }
    resp = requests.get(BING_SEARCH_ENDPOINT, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    results = resp.json().get("value", [])
    if not results:
        raise RuntimeError(f"No images found for query: {query}")

    for img in results:
        content_url = img.get("contentUrl", "")
        try:
            _download_file(content_url, output_path)
            logger.info("Web image downloaded: %s -> %s", query[:60], output_path)
            return
        except Exception:
            continue

    raise RuntimeError(f"All image download attempts failed for query: {query}")


# ---------------------------------------------------------------------------
# Screenshot (Playwright)
# ---------------------------------------------------------------------------

def _capture_screenshot(target_url: str, output_path: str) -> None:
    """Capture screenshot of a funba page."""
    from social_media.hupu.post import _capture_compact_screenshot
    _capture_compact_screenshot(target_url, output_path)
    logger.info("Screenshot captured: %s -> %s", target_url, output_path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _download_file(url: str, output_path: str) -> None:
    resp = requests.get(url, timeout=30, stream=True)
    resp.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
