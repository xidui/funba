"""Image resolution for SocialPost image pool.

Supports three generation types:
- ai_generated: OpenAI DALL-E image generation
- web_search: Smart scrape — search NBA sites, crawl pages, pick best image
- screenshot: Playwright page capture (delegates to hupu/post.py helper)
"""
from __future__ import annotations

import logging
import os
import re
import struct
import tempfile
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

logger = logging.getLogger(__name__)

MEDIA_ROOT = Path(__file__).resolve().parent.parent / "media" / "social_posts"

# Sites to search for NBA content images
NBA_SITES = [
    "reddit.com/r/nba",
    "espn.com",
    "nba.com",
    "bleacherreport.com",
    "theathletic.com",
    "twitter.com",
    "x.com",
]

# Domains/patterns that are never useful images
BLOCKED_IMAGE_DOMAINS = {
    "google.com", "googleadservices.com", "doubleclick.net",
    "facebook.com", "fbcdn.net",
    "pixel.", "analytics.", "tracker.",
    "gravatar.com",
}

BLOCKED_IMAGE_PATTERNS = re.compile(
    r"(logo|icon|avatar|emoji|badge|sprite|placeholder|ads?[-_.]|banner[-_.]ad|"
    r"tracking|pixel|spacer|arrow|button|loading|spinner|\.gif$|\.svg$)",
    re.IGNORECASE,
)


def ensure_post_media_dir(post_id: int) -> Path:
    d = MEDIA_ROOT / str(post_id)
    d.mkdir(parents=True, exist_ok=True)
    return d


def resolve_image(spec: dict, *, post_id: int, slot: str) -> list[str]:
    """Generate or download image(s) from *spec*, return list of local file paths.

    Most types return a single image. web_search may return multiple good candidates.
    Raises on failure so the caller can record error_message.
    """
    image_type = spec.get("type", "")
    out_dir = ensure_post_media_dir(post_id)

    if image_type == "ai_generated":
        out_path = out_dir / f"{slot}.png"
        _generate_openai(spec["prompt"], str(out_path), spec.get("style"))
        return [str(out_path)]
    elif image_type == "web_search":
        max_images = int(spec.get("max", 3))
        return _smart_search_and_download(spec["query"], out_dir, slot, max_images=max_images)
    elif image_type == "screenshot":
        out_path = out_dir / f"{slot}.png"
        _capture_screenshot(spec["target"], str(out_path))
        return [str(out_path)]
    else:
        raise ValueError(f"Unknown image type: {image_type}")


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
# Smart web search — scrape NBA sites for the best image
# ---------------------------------------------------------------------------

def _smart_search_and_download(query: str, out_dir: Path, slot: str, *, max_images: int = 3) -> list[str]:
    """Search for NBA images via DuckDuckGo HTTP API, download multiple good ones."""
    saved: list[str] = []

    results = _ddg_image_api(query + " NBA")
    logger.info("DDG image API returned %d results for '%s'", len(results), query[:40])

    for r in results:
        if len(saved) >= max_images:
            break
        img_url = r.get("image", "")
        w = r.get("width", 0)
        h = r.get("height", 0)
        if not img_url or not _is_good_image_url(img_url):
            continue
        # DDG gives us dimensions — filter before downloading
        if w < 400 or h < 300:
            continue
        # Skip extreme aspect ratios (banners, strips)
        ratio = w / h if h > 0 else 0
        if ratio < 0.4 or ratio > 3.0:
            continue

        idx = len(saved)
        suffix = f"_{idx}" if idx > 0 else ""
        path = str(out_dir / f"{slot}{suffix}.png")
        try:
            _download_file(img_url, path)
            actual_w, actual_h = _image_dimensions(path)
            if actual_w >= 400 and actual_h >= 300:
                logger.info("DDG image [%d/%d]: %dx%d from %s", idx + 1, max_images, actual_w, actual_h, img_url[:80])
                saved.append(path)
            else:
                Path(path).unlink(missing_ok=True)
        except Exception:
            Path(path).unlink(missing_ok=True)

    if not saved:
        raise RuntimeError(f"No suitable images found for: {query}")
    return saved


def _ddg_image_api(query: str) -> list[dict]:
    """Call DuckDuckGo image search via its HTTP API (no API key needed)."""
    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
    )

    # Step 1: get vqd token
    resp = session.get("https://duckduckgo.com/", params={"q": query}, timeout=10)
    resp.raise_for_status()
    match = re.search(r'vqd="([^"]+)"', resp.text) or re.search(r"vqd=([^&\"']+)", resp.text)
    if not match:
        raise RuntimeError("Failed to obtain DuckDuckGo vqd token")
    vqd = match.group(1)

    # Step 2: image search
    img_resp = session.get("https://duckduckgo.com/i.js", params={
        "l": "us-en",
        "o": "json",
        "q": query,
        "vqd": vqd,
        "f": ",,,,,",
        "p": "1",
    }, timeout=10)
    img_resp.raise_for_status()
    return img_resp.json().get("results", [])


def _is_good_image_url(url: str) -> bool:
    """Quick heuristic: is this URL likely a useful content image?"""
    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    for blocked in BLOCKED_IMAGE_DOMAINS:
        if blocked in domain:
            return False

    if BLOCKED_IMAGE_PATTERNS.search(url):
        return False

    return True


def _image_dimensions(path: str) -> tuple[int, int]:
    """Read width/height from a downloaded image file (PNG or JPEG header)."""
    try:
        with open(path, "rb") as f:
            header = f.read(32)
            # PNG
            if header[:8] == b"\x89PNG\r\n\x1a\n":
                w, h = struct.unpack(">II", header[16:24])
                return w, h
            # JPEG
            if header[:2] == b"\xff\xd8":
                f.seek(0)
                data = f.read()
                i = 2
                while i < len(data) - 8:
                    if data[i] != 0xFF:
                        break
                    marker = data[i + 1]
                    if marker in (0xC0, 0xC1, 0xC2):
                        h, w = struct.unpack(">HH", data[i + 5 : i + 9])
                        return w, h
                    length = struct.unpack(">H", data[i + 2 : i + 4])[0]
                    i += 2 + length
    except Exception:
        pass
    return 0, 0


# ---------------------------------------------------------------------------
# Bing Image Search API (fallback)
# ---------------------------------------------------------------------------

BING_SEARCH_ENDPOINT = "https://api.bing.microsoft.com/v7.0/images/search"


def _bing_image_search(query: str, output_path: str) -> None:
    """Fallback: Bing Image Search API."""
    api_key = os.getenv("BING_IMAGE_SEARCH_KEY")
    if not api_key:
        raise RuntimeError("BING_IMAGE_SEARCH_KEY not set and smart scrape failed")

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
            logger.info("Bing fallback image downloaded: %s -> %s", query[:60], output_path)
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
    resp = requests.get(url, timeout=30, stream=True, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    })
    resp.raise_for_status()
    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
