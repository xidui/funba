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
    """Search NBA sites for relevant pages, scrape images, return multiple good ones."""
    try:
        paths = _scrape_best_images(query, out_dir, slot, max_images=max_images)
        if paths:
            return paths
    except Exception as exc:
        logger.info("Smart scrape failed for '%s': %s — trying Bing fallback", query[:40], exc)

    # Fallback: single image from Bing
    fallback_path = str(out_dir / f"{slot}.png")
    _bing_image_search(query, fallback_path)
    return [fallback_path]


def _scrape_best_images(query: str, out_dir: Path, slot: str, *, max_images: int = 3) -> list[str]:
    """Google search NBA sites, crawl top results, return up to max_images good images."""
    from social_media.hupu.post import _playwright, REAL_BROWSER_UA
    import time

    saved: list[str] = []
    seen_urls: set[str] = set()

    def _try_save(url: str) -> bool:
        if url in seen_urls or len(saved) >= max_images:
            return False
        seen_urls.add(url)
        idx = len(saved)
        suffix = f"_{idx}" if idx > 0 else ""
        path = str(out_dir / f"{slot}{suffix}.png")
        try:
            _download_file(url, path)
            w, h = _image_dimensions(path)
            if w >= 400 and h >= 300:
                logger.info("Smart search [%d/%d]: %dx%d from %s", idx + 1, max_images, w, h, url[:80])
                saved.append(path)
                return True
            else:
                Path(path).unlink(missing_ok=True)
        except Exception:
            Path(path).unlink(missing_ok=True)
        return False

    site_clause = " OR ".join(f"site:{s}" for s in NBA_SITES[:4])
    search_query = f"{query} ({site_clause})"
    search_url = f"https://www.google.com/search?q={requests.utils.quote(search_query)}&tbm=isch&udm=2"

    with _playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--headless=new"])
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            user_agent=REAL_BROWSER_UA,
        )
        page = context.new_page()

        # Step 1: Google Image Search
        page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)

        candidate_urls = _extract_google_image_results(page)
        logger.info("Google image search returned %d candidates for '%s'", len(candidate_urls), query[:40])

        for url in candidate_urls[:15]:
            if len(saved) >= max_images:
                break
            if _is_good_image_url(url):
                _try_save(url)

        # Step 2: If we need more, crawl NBA site pages
        if len(saved) < max_images:
            regular_search_url = f"https://www.google.com/search?q={requests.utils.quote(query + ' NBA')}"
            page.goto(regular_search_url, wait_until="domcontentloaded", timeout=15000)
            time.sleep(2)

            page_urls = []
            links = page.query_selector_all("a[href]")
            for link in links:
                href = link.get_attribute("href") or ""
                if "/url?q=" in href:
                    actual = href.split("/url?q=")[1].split("&")[0]
                    if any(site in actual for site in NBA_SITES):
                        page_urls.append(requests.utils.unquote(actual))
                elif any(site in href for site in NBA_SITES):
                    page_urls.append(href)

            logger.info("Found %d NBA site pages to scrape for '%s'", len(page_urls), query[:40])

            for page_url in page_urls[:3]:
                if len(saved) >= max_images:
                    break
                try:
                    images = _scrape_page_images(page, page_url)
                    for img_url in images:
                        if len(saved) >= max_images:
                            break
                        _try_save(img_url)
                except Exception as exc:
                    logger.debug("Failed to scrape %s: %s", page_url[:60], exc)

        context.close()
        browser.close()

    return saved


def _extract_google_image_results(page) -> list[str]:
    """Extract image URLs from a Google Image Search results page."""
    urls = []
    # Google image results store full-res URLs in data attributes or nested JSON
    imgs = page.query_selector_all("img[src]")
    for img in imgs:
        src = img.get_attribute("src") or ""
        # Skip Google's own tiny thumbnails (base64 or gstatic)
        if src.startswith("data:") or "gstatic.com" in src:
            continue
        if src.startswith("http") and _is_good_image_url(src):
            urls.append(src)

    # Also try to extract from links around images
    links = page.query_selector_all("a[href*='imgurl=']")
    for link in links:
        href = link.get_attribute("href") or ""
        match = re.search(r"imgurl=([^&]+)", href)
        if match:
            urls.append(requests.utils.unquote(match.group(1)))

    return urls


def _scrape_page_images(page, url: str) -> list[str]:
    """Visit a page and return image URLs sorted by likely relevance."""
    import time
    page.goto(url, wait_until="domcontentloaded", timeout=12000)
    time.sleep(2)

    # Extract all images with their metadata
    raw_images = page.evaluate("""() => {
        const imgs = Array.from(document.querySelectorAll('img'));
        return imgs.map(img => {
            const rect = img.getBoundingClientRect();
            return {
                src: img.src || img.getAttribute('data-src') || img.getAttribute('data-lazy-src') || '',
                width: img.naturalWidth || rect.width || 0,
                height: img.naturalHeight || rect.height || 0,
                alt: img.alt || '',
                inViewport: rect.top >= 0 && rect.top < 3000,
                area: rect.width * rect.height,
            };
        });
    }""")

    # Score and filter
    scored = []
    for img in raw_images:
        src = img.get("src", "")
        if not src or not src.startswith("http"):
            continue
        if not _is_good_image_url(src):
            continue

        w = img.get("width", 0)
        h = img.get("height", 0)
        area = img.get("area", 0)

        # Must be reasonably large
        if w < 200 and h < 200 and area < 40000:
            continue

        score = 0.0
        # Prefer larger images
        score += min(area / 100000, 5.0)
        # Prefer images in the main content area (top 3000px)
        if img.get("inViewport"):
            score += 2.0
        # Prefer images with descriptive alt text
        if len(img.get("alt", "")) > 10:
            score += 1.0
        # Penalize very wide/narrow aspect ratios (likely banners)
        if w > 0 and h > 0:
            ratio = w / h
            if 0.5 < ratio < 2.5:
                score += 1.0

        scored.append((score, src))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [url for _, url in scored]


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
