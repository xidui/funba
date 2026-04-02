"""Image resolution for SocialPost image pool.

Supports four generation types:
- ai_generated: OpenAI DALL-E image generation for stylized supporting art
- player_headshot: Official NBA player headshot download
- web_search: Smart search for real editorial photos
- screenshot: Playwright page capture (delegates to hupu/post.py helper)
"""
from __future__ import annotations

import base64
import json
import logging
import os
import re
import struct
from pathlib import Path
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

MEDIA_ROOT = Path(__file__).resolve().parent.parent / "media" / "social_posts"

# Prefer official/editorial sources for real photos.
PREFERRED_IMAGE_DOMAINS = {
    "nba.com",
    "cdn.nba.com",
    "stats.nba.com",
    "espn.com",
    "espncdn.com",
    "a.espncdn.com",
    "apnews.com",
    "apimages.com",
    "usatoday.com",
    "ftw.usatoday.com",
}

# Domains/patterns that are never useful images
BLOCKED_IMAGE_DOMAINS = {
    "google.com", "googleadservices.com", "doubleclick.net",
    "facebook.com", "fbcdn.net",
    "pixel.", "analytics.", "tracker.",
    "gravatar.com",
}

BLOCKED_WATERMARK_DOMAINS = {
    "gettyimages.com",
    "alamy.com",
    "shutterstock.com",
    "istockphoto.com",
    "dreamstime.com",
    "depositphotos.com",
    "123rf.com",
    "bigstockphoto.com",
}

BLOCKED_IMAGE_PATTERNS = re.compile(
    r"(logo|icon|avatar|emoji|badge|sprite|placeholder|ads?[-_.]|banner[-_.]ad|"
    r"tracking|pixel|spacer|arrow|button|loading|spinner|\.gif$|\.svg$)",
    re.IGNORECASE,
)

BLOCKED_WATERMARK_PATTERNS = re.compile(
    r"(watermark|getty|alamy|shutterstock|istock(photo)?|dreamstime|depositphotos|123rf|bigstock)",
    re.IGNORECASE,
)

IMAGE_REVIEW_TYPES = {"web_search", "ai_generated"}


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
    elif image_type == "player_headshot":
        out_path = out_dir / f"{slot}.png"
        _download_official_player_headshot(spec["player_id"], str(out_path), player_name=spec.get("player_name"))
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


def review_resolved_image(spec: dict, image_path: str) -> dict[str, object]:
    """Run a lightweight vision QA pass when configured.

    Returns a dict with:
    - checked: whether a model review was attempted
    - ok: whether the image passed review
    - reason: rejection reason when available
    - model: model name used for review, if any
    """
    image_type = str(spec.get("type") or "").strip()
    if image_type not in IMAGE_REVIEW_TYPES:
        return {"checked": False, "ok": True, "reason": None, "model": None}
    if not os.getenv("OPENAI_API_KEY"):
        return {"checked": False, "ok": True, "reason": None, "model": None}

    review_model = (os.getenv("FUNBA_IMAGE_REVIEW_MODEL") or "gpt-5.4-mini").strip()
    try:
        from openai import OpenAI

        client = OpenAI()
        response = client.responses.create(
            model=review_model,
            input=[
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": _build_image_review_prompt(spec)},
                        {"type": "input_image", "image_url": _image_data_url(image_path), "detail": "low"},
                    ],
                }
            ],
            max_output_tokens=220,
            temperature=0,
        )
        raw_text = (getattr(response, "output_text", None) or "").strip()
        accepted, reason = _parse_image_review_output(raw_text)
        if accepted is None:
            raise RuntimeError(f"Invalid review response: {raw_text[:200]}")
        return {
            "checked": True,
            "ok": accepted,
            "reason": reason,
            "model": review_model,
            "raw_text": raw_text,
        }
    except Exception as exc:
        logger.warning("Image auto-review skipped for %s: %s", image_path, exc)
        return {"checked": False, "ok": True, "reason": None, "model": review_model, "error": str(exc)}


# ---------------------------------------------------------------------------
# Official player media
# ---------------------------------------------------------------------------

def _download_official_player_headshot(player_id: str, output_path: str, *, player_name: str | None = None) -> None:
    """Download an official NBA player headshot for a known player_id."""
    player_id = str(player_id or "").strip()
    if not player_id:
        raise ValueError("player_headshot requires player_id")

    urls = [
        f"https://cdn.nba.com/headshots/nba/latest/1040x760/{player_id}.png",
        f"https://cdn.nba.com/headshots/nba/latest/260x190/{player_id}.png",
    ]
    last_error: Exception | None = None

    for url in urls:
        try:
            _download_file(url, output_path)
            width, height = _image_dimensions(output_path)
            if width > 0 and height > 0:
                logger.info("Official player headshot downloaded: %s (%s) -> %s", player_name or player_id, player_id, output_path)
                return
        except Exception as exc:
            last_error = exc
        Path(output_path).unlink(missing_ok=True)

    if last_error:
        raise RuntimeError(f"Official player headshot unavailable for {player_id}") from last_error
    raise RuntimeError(f"Official player headshot unavailable for {player_id}")


# ---------------------------------------------------------------------------
# Smart web search — prefer real editorial photos without watermarks
# ---------------------------------------------------------------------------

def _smart_search_and_download(query: str, out_dir: Path, slot: str, *, max_images: int = 3) -> list[str]:
    """Search for NBA images via DuckDuckGo HTTP API, download multiple good ones."""
    saved: list[str] = []
    seen_urls: set[str] = set()

    results = _ddg_image_api(_web_search_query(query))
    logger.info("DDG image API returned %d results for '%s'", len(results), query[:40])

    ranked_results = sorted(results, key=_search_result_sort_key)

    for r in ranked_results:
        if len(saved) >= max_images:
            break
        img_url = r.get("image", "")
        if not img_url or img_url in seen_urls:
            continue
        seen_urls.add(img_url)
        w = r.get("width", 0)
        h = r.get("height", 0)
        if not _is_good_search_result(r):
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


def _web_search_query(query: str) -> str:
    base = str(query or "").strip()
    if not base:
        raise ValueError("web_search requires query")
    if "nba" not in base.lower():
        base = f"{base} NBA"
    exclusions = " ".join(f"-site:{domain}" for domain in sorted(BLOCKED_WATERMARK_DOMAINS))
    return f"{base} {exclusions}".strip()


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


def _is_good_search_result(result: dict) -> bool:
    """Reject results from known watermark-heavy domains and low-signal assets."""
    image_url = str(result.get("image") or "")
    if not image_url or not _is_good_image_url(image_url):
        return False

    page_url = str(result.get("url") or "")
    source = str(result.get("source") or "")
    title = str(result.get("title") or "")
    thumbnail = str(result.get("thumbnail") or "")

    haystack = " ".join(part for part in (image_url, page_url, source, title, thumbnail) if part)
    if BLOCKED_WATERMARK_PATTERNS.search(haystack):
        return False

    for domain in _search_result_domains(result):
        if _domain_matches_any(domain, BLOCKED_WATERMARK_DOMAINS):
            return False

    return True


def _search_result_sort_key(result: dict) -> tuple[int, str, str]:
    """Prefer official/editorial sources before generic web results."""
    preferred_rank = 0 if _is_preferred_search_result(result) else 1
    page_url = str(result.get("url") or "")
    image_url = str(result.get("image") or "")
    return (preferred_rank, page_url, image_url)


def _is_preferred_search_result(result: dict) -> bool:
    return any(_domain_matches_any(domain, PREFERRED_IMAGE_DOMAINS) for domain in _search_result_domains(result))


def _search_result_domains(result: dict) -> set[str]:
    domains: set[str] = set()
    for key in ("image", "url", "thumbnail"):
        value = str(result.get(key) or "").strip()
        if not value:
            continue
        parsed = urlparse(value)
        domain = parsed.netloc.lower().strip()
        if domain:
            domains.add(domain)
    source = str(result.get("source") or "").strip().lower()
    if source:
        domains.add(source)
    return domains


def _domain_matches_any(domain: str, candidates: set[str]) -> bool:
    return any(candidate in domain for candidate in candidates)


def _build_image_review_prompt(spec: dict) -> str:
    image_type = str(spec.get("type") or "").strip() or "unknown"
    context_lines = [f"image_type: {image_type}"]
    for key in ("player_name", "player_id", "query", "prompt", "note"):
        value = str(spec.get(key) or "").strip()
        if value:
            context_lines.append(f"{key}: {value}")

    criteria = [
        "Reject if the image has a visible watermark, stock-photo overlay, or agency branding.",
        "Reject if the image is not clearly basketball related.",
        "Reject if the image is a collage, meme screenshot, UI screenshot, or otherwise poor as a post image.",
    ]
    if image_type == "web_search":
        criteria.extend(
            [
                "Reject if it looks AI-generated, synthetic, uncanny, or heavily illustrated instead of a real editorial photo.",
                "Reject if it obviously shows the wrong player, wrong team context, or a scene unrelated to the target description.",
            ]
        )
    elif image_type == "ai_generated":
        criteria.extend(
            [
                "Reject if the basketball scene is clearly unrelated to the target description in the prompt.",
                "Reject if faces, limbs, jerseys, or text artifacts are so broken that the image is not publishable.",
            ]
        )

    return (
        "Review this candidate image for an NBA social post.\n"
        "Be conservative: if there is an obvious quality or authenticity problem, reject it.\n\n"
        "Context:\n"
        + "\n".join(f"- {line}" for line in context_lines)
        + "\n\n"
        "Decision rules:\n"
        + "\n".join(f"- {line}" for line in criteria)
        + "\n\n"
        'Reply with JSON only: {"accepted": true|false, "reason": "short reason"}'
    )


def _image_data_url(image_path: str) -> str:
    suffix = Path(image_path).suffix.lower()
    mime = "image/jpeg" if suffix in {".jpg", ".jpeg"} else "image/png"
    encoded = base64.b64encode(Path(image_path).read_bytes()).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _parse_image_review_output(text: str) -> tuple[bool | None, str | None]:
    cleaned = str(text or "").strip()
    if not cleaned:
        return None, None
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()

    parsed = _parse_json_object(cleaned)
    if not isinstance(parsed, dict):
        return None, None

    accepted = parsed.get("accepted")
    if isinstance(accepted, str):
        normalized = accepted.strip().lower()
        if normalized in {"true", "accept", "accepted", "pass"}:
            accepted = True
        elif normalized in {"false", "reject", "rejected", "fail"}:
            accepted = False
    if not isinstance(accepted, bool):
        return None, None

    reason = str(parsed.get("reason") or "").strip() or None
    return accepted, reason


def _parse_json_object(text: str) -> dict | None:
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        pass

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


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
