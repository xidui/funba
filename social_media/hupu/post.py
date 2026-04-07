"""Auto-post to Hupu forums using Playwright with Chrome cookie injection.

Usage:
    # Import cookies from Chrome (after logging in via normal Chrome)
    python -m social_media.hupu.post login --chrome-profile "Profile 1"

    # Check login status
    python -m social_media.hupu.post check

    # Post with plain text (dry run by default)
    python -m social_media.hupu.post post --title "xxx" --content "xxx" --forum "nba"

    # Post with images and links
    python -m social_media.hupu.post post --title "xxx" --content "xxx" --forum "雷霆专区" \\
        --image /tmp/screenshot.png \\
        --link-text "funba.app" --link-url "https://funba.app" \\
        --submit
"""
from __future__ import annotations

import argparse
from datetime import datetime
import html
import json
import re
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    from playwright.sync_api import sync_playwright, Page, BrowserContext
except ModuleNotFoundError:
    sync_playwright = None
    Page = BrowserContext = Any
from ..funba_capture import capture_funba_url as _capture_funba_url
from .forums import normalize_hupu_forum

MODULE_DIR = Path(__file__).resolve().parent
COOKIE_FILE = MODULE_DIR / ".hupu_cookies.json"
BROWSER_DATA_DIR = MODULE_DIR / ".hupu_browser_data"

HUPU_HOME = "https://bbs.hupu.com"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_AUTH_COOKIE_NAMES = ("u", "us", "_CLT")
_LOGGED_OUT_TEXT_MARKERS = (
    "欢迎访问虎扑，请先",
    "登录后的世界更精彩",
)
_LOGGED_IN_TEXT_MARKERS = (
    "我的首页",
    "创作者中心",
    "退出",
    "私信",
)

_SHORT_SLEEP = 0.08
_MEDIUM_SLEEP = 0.2
_LONG_SLEEP = 0.5
_FORUM_SEARCH_SLEEP = 0.8
_POST_PAGE_LOAD_SLEEP = 1.5
_HOME_PAGE_LOAD_SLEEP = 1.0
_UPLOAD_SETTLE_SLEEP = 0.2
_UPLOAD_COUNT_TIMEOUT = 8.0
_UPLOAD_READY_TIMEOUT = 3.0

NBA_COMPOSER_FORUM_ID = 179
CBA_COMPOSER_FORUM_ID = 346

FORUMS = {
    "nba": {"composer_id": NBA_COMPOSER_FORUM_ID, "label": "湿乎乎的话题", "aliases": ("nba", "NBA版", "湿乎乎的话题", "篮球场")},
    "cba": {"composer_id": CBA_COMPOSER_FORUM_ID, "label": "CBA版", "aliases": ("cba", "CBA版")},
}


def _slugify_artifact_part(value: str | None) -> str:
    cleaned = re.sub(r"[^\w\u4e00-\u9fff-]+", "-", str(value or "").strip(), flags=re.UNICODE)
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "artifact"


def _resolve_artifact_dir(requested_path: str | None, *, post_id: int | None, forum_label: str) -> Path:
    if requested_path:
        path = Path(requested_path).expanduser()
    else:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        parts = [stamp]
        if post_id is not None:
            parts.append(f"post{post_id}")
        parts.append(_slugify_artifact_part(forum_label))
        path = Path.cwd() / "logs" / "hupu_post" / "-".join(parts)
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def _write_text_artifact(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(content or ""), encoding="utf-8")


def _write_json_artifact(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_page_url(page: Page | None) -> str | None:
    if page is None:
        return None
    try:
        return str(page.url)
    except Exception:
        return None


def _safe_page_html(page: Page | None) -> str:
    if page is None:
        return ""
    try:
        return page.content()
    except Exception as exc:
        return f"<failed to capture html: {exc}>"


def _safe_page_body_text(page: Page | None) -> str:
    if page is None:
        return ""
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception as exc:
        return f"<failed to capture body text: {exc}>"


def _safe_current_forum_label(page: Page | None) -> str | None:
    if page is None:
        return None
    try:
        return _current_forum_label(page)
    except Exception:
        return None


def _safe_page_screenshot(page: Page | None, path: Path) -> None:
    if page is None:
        return
    try:
        page.screenshot(path=str(path), full_page=True)
    except Exception as exc:
        _write_text_artifact(path.with_suffix(".error.txt"), str(exc))


def _persist_hupu_failure_artifacts(
    artifact_dir: Path,
    *,
    page: Page | None,
    stage: str,
    requested_forum_label: str,
    title: str,
    content: str,
    post_id: int | None,
    exception: BaseException,
    browser_events: dict[str, list[dict[str, str]]] | None = None,
) -> None:
    body_text = _safe_page_body_text(page)
    html_text = _safe_page_html(page)
    state = {
        "stage": stage,
        "post_id": post_id,
        "requested_forum_label": requested_forum_label,
        "current_forum_label": _safe_current_forum_label(page),
        "page_url": _safe_page_url(page),
        "title": title,
        "content_preview": content[:500],
        "exception_type": type(exception).__name__,
        "exception_message": str(exception),
        "captured_at": datetime.utcnow().isoformat() + "Z",
    }
    _write_json_artifact(artifact_dir / "failure_state.json", state)
    _write_text_artifact(artifact_dir / "failure_traceback.txt", traceback.format_exc())
    _write_text_artifact(artifact_dir / "failure_body.txt", body_text)
    _write_text_artifact(artifact_dir / "failure_page.html", html_text)
    if browser_events is not None:
        _write_json_artifact(artifact_dir / "browser_events.json", browser_events)
    _safe_page_screenshot(page, artifact_dir / "failure.png")


def _playwright():
    if sync_playwright is None:
        raise RuntimeError("Playwright is required for Hupu posting commands. Install it with `pip install playwright`.")
    return sync_playwright()


def _load_cookies() -> list[dict]:
    """Load saved cookies from file."""
    if not COOKIE_FILE.exists():
        return []
    with open(COOKIE_FILE) as f:
        raw = json.load(f)
    clean = []
    for c in raw:
        entry = {
            "name": c["name"],
            "value": c.get("value", ""),
            "domain": c["domain"],
            "path": c.get("path", "/"),
        }
        if c.get("secure"):
            entry["secure"] = True
        clean.append(entry)
    return clean


def _create_context(pw, headless: bool = True) -> BrowserContext:
    """Create browser context with saved cookies injected."""
    launch_args: list[str] = []
    if headless:
        # Use new headless mode which is harder for sites to detect
        launch_args.append("--headless=new")
    browser = pw.chromium.launch(headless=headless, args=launch_args)
    context = browser.new_context(
        viewport={"width": 1280, "height": 1200},
        locale="zh-CN",
        user_agent=REAL_BROWSER_UA,
    )
    cookies = _load_cookies()
    if cookies:
        context.add_cookies(cookies)
    return context


def _page_text(page: Page) -> str:
    """Return body text for lightweight login-state heuristics."""
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def _login_state(page: Page) -> tuple[bool, dict[str, object]]:
    """Return whether Hupu appears logged in based on cookies and rendered UI."""
    cookies = page.context.cookies()
    auth_cookie_names = [
        c["name"]
        for c in cookies
        if c.get("name") in _AUTH_COOKIE_NAMES and c.get("value")
    ]
    text = _page_text(page)
    has_logged_out_ui = any(marker in text for marker in _LOGGED_OUT_TEXT_MARKERS)
    has_logged_in_ui = any(marker in text for marker in _LOGGED_IN_TEXT_MARKERS)
    is_logged_in = bool(auth_cookie_names) and (has_logged_in_ui or not has_logged_out_ui)
    return is_logged_in, {
        "auth_cookie_names": auth_cookie_names,
        "has_logged_out_ui": has_logged_out_ui,
        "has_logged_in_ui": has_logged_in_ui,
        "page_text": text,
    }


def _is_logged_in(page: Page) -> bool:
    """Return whether Hupu appears logged in."""
    return _login_state(page)[0]


def _login_failure_reason(details: dict[str, object]) -> str:
    reasons: list[str] = []
    auth_cookie_names = details.get("auth_cookie_names") or []
    if not auth_cookie_names:
        reasons.append("missing auth cookies")
    elif details.get("has_logged_out_ui"):
        reasons.append("auth cookies exist but the page still shows logged-out UI")
    if not details.get("page_text"):
        reasons.append("could not read page body")
    return "; ".join(reasons) if reasons else "unknown login-state check failure"


def _fill_editor(page: Page, paragraphs: list[str], footer_html: str | None = None) -> None:
    """Fill ProseMirror editor with paragraphs and optional footer HTML."""
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")

    # Build paragraphs via innerHTML for proper line breaks
    js_lines = json.dumps(paragraphs)
    editor.evaluate(
        f"""el => {{
        el.innerHTML = "";
        const lines = {js_lines};
        for (const line of lines) {{
            const p = document.createElement("p");
            p.textContent = line || "\\u200b";
            el.appendChild(p);
        }}
        el.dispatchEvent(new Event("input", {{ bubbles: true }}));
    }}"""
    )
    time.sleep(0.5)

    if footer_html:
        editor.evaluate(
            f"""el => {{
            const footer = {json.dumps(footer_html)};
            const div = document.createElement("div");
            div.innerHTML = footer;
            while (div.firstChild) el.appendChild(div.firstChild);
            el.dispatchEvent(new Event("input", {{ bubbles: true }}));
        }}"""
        )
        time.sleep(0.5)


def _resolve_forum(forum: str) -> tuple[str, int, str]:
    """Resolve an input forum alias into (key, composer_page_id, label)."""
    raw = (forum or "").strip()
    normalized = normalize_hupu_forum(raw) or raw
    for key, meta in FORUMS.items():
        aliases = meta.get("aliases") or ()
        if raw == key or raw in aliases or normalized == key or normalized in aliases or normalized == meta["label"]:
            return key, int(meta["composer_id"]), str(meta["label"])
    if normalized.endswith("专区"):
        return normalized, NBA_COMPOSER_FORUM_ID, normalized
    raise KeyError(forum)


def _render_inline_html(text: str) -> str:
    """Render a limited markdown-like subset into editor-safe HTML."""
    pattern = re.compile(r"\*\*(.+?)\*\*|\[([^\]]+)\]\((https?://[^)]+)\)|(https?://[^\s<>()]+)")
    parts: list[str] = []
    cursor = 0
    for match in pattern.finditer(text):
        parts.append(html.escape(text[cursor:match.start()]))
        if match.group(1) is not None:
            parts.append(f"<strong>{html.escape(match.group(1))}</strong>")
        elif match.group(2) is not None:
            label = html.escape(match.group(2))
            url = html.escape(match.group(3), quote=True)
            parts.append(f'<a href="{url}" target="_blank">{label}</a>')
        else:
            url = html.escape(match.group(4), quote=True)
            parts.append(f'<a href="{url}" target="_blank">{url}</a>')
        cursor = match.end()
    parts.append(html.escape(text[cursor:]))
    return "".join(parts)


def _parse_image_placeholder(line: str) -> dict[str, str] | None:
    """Parse one placeholder line into a key/value mapping."""
    match = re.match(r"^\s*\[\[IMAGE:(.+?)\]\]\s*$", line)
    if not match:
        return None
    payload = match.group(1)
    parsed: dict[str, str] = {}
    for part in payload.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key and value:
            parsed[key] = value
    return parsed


def _load_post_image_pool(post_id: int) -> dict[str, str]:
    """Load enabled images from SocialPostImage pool, returning {slot: file_path}."""
    from sqlalchemy.orm import sessionmaker
    from db.models import SocialPostImage, engine
    Session = sessionmaker(bind=engine)
    with Session() as s:
        rows = (
            s.query(SocialPostImage)
            .filter(
                SocialPostImage.post_id == post_id,
                SocialPostImage.is_enabled == True,
                SocialPostImage.file_path.isnot(None),
            )
            .order_by(SocialPostImage.id)
            .all()
        )
        return {row.slot: row.file_path for row in rows}


def _prepare_placeholder_images(
    content: str,
    images: list[str],
    *,
    post_id: int | None = None,
) -> tuple[list[str], list[str]]:
    """Return resolved images plus any temporary screenshots created from placeholders.

    When *post_id* is given, slot-based placeholders ([[IMAGE:slot=img1]]) are
    resolved from the SocialPostImage pool in the database.  Placeholders
    without a slot (legacy target-based) fall back to auto-screenshot.
    """
    resolved_images = list(images)
    temp_paths: list[str] = []

    pool: dict[str, str] = {}
    if post_id is not None:
        pool = _load_post_image_pool(post_id)

    placeholder_specs = [
        spec
        for spec in (_parse_image_placeholder(line) for line in content.split("\n"))
        if spec is not None
    ]
    if len(resolved_images) >= len(placeholder_specs):
        # Even without placeholders, append pool images so they go at the end
        if not placeholder_specs and pool and len(resolved_images) == 0:
            resolved_images.extend(pool.values())
        return resolved_images, temp_paths

    for spec in placeholder_specs[len(resolved_images):]:
        # Slot-based: look up from image pool
        slot = spec.get("slot")
        if slot and slot in pool:
            resolved_images.append(pool[slot])
            continue

        # Legacy target-based: auto-screenshot
        target = spec.get("target")
        if not target:
            continue
        tmp = tempfile.NamedTemporaryFile(prefix="funba_hupu_", suffix=".png", delete=False)
        tmp.close()
        _capture_funba_url(target, tmp.name)
        temp_paths.append(tmp.name)
        resolved_images.append(tmp.name)
    return resolved_images, temp_paths


def _append_paragraph(page: Page, line: str) -> None:
    """Append one paragraph, preserving simple inline rich text markers."""
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")
    html_line = _render_inline_html(line) if line else ""
    editor.evaluate(
        """(el, payload) => {
        const p = document.createElement("p");
        if (payload && payload.length > 0) {
            p.innerHTML = payload;
        } else {
            p.textContent = "\\u200b";
        }
        el.appendChild(p);
        el.dispatchEvent(new Event("input", { bubbles: true }));
    }""",
        html_line,
    )
    time.sleep(_SHORT_SLEEP)


def _append_footer_html(page: Page, footer_html: str) -> None:
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")
    editor.evaluate(
        """(el, footer) => {
        const div = document.createElement("div");
        div.innerHTML = footer;
        while (div.firstChild) el.appendChild(div.firstChild);
        el.dispatchEvent(new Event("input", { bubbles: true }));
    }""",
        footer_html,
    )
    time.sleep(_SHORT_SLEEP)


def _forum_label_matches(current_label: str | None, target_label: str) -> bool:
    """Return whether the current composer forum should be treated as the target."""
    current = (current_label or "").strip()
    target = (target_label or "").strip()
    if not current or not target:
        return False
    return current == target


def _current_forum_label(page: Page) -> str | None:
    """Return the currently selected forum label shown in the composer."""
    chip = page.locator(".selectTagWrap .ant-tag").first
    if chip.count() == 0:
        return None
    text = chip.inner_text(timeout=1000).strip()
    return text or None


def _clear_selected_forum(page: Page) -> None:
    """Remove the currently selected forum tag so the picker can reopen."""
    close = page.locator(".selectTagWrap .anticon-close").first
    if close.count() == 0:
        return
    close.click()
    time.sleep(1)


def _ensure_forum_selected(page: Page, forum_label: str) -> None:
    """Select the target forum in the composer via the dynamic picker."""
    current_label = _current_forum_label(page)
    if _forum_label_matches(current_label, forum_label):
        return

    _clear_selected_forum(page)

    add_forum = page.locator("text=添加专区").first
    if add_forum.count() == 0:
        raise RuntimeError("Forum picker trigger not found after clearing current forum")
    add_forum.click()
    time.sleep(_MEDIUM_SLEEP)

    search_input = page.locator('input[placeholder="添加专区可以让更多JR和你一起讨论"]').first
    if search_input.count() == 0:
        raise RuntimeError("Forum search input not found in picker")
    search_input.fill(forum_label)

    search_button = page.locator(".ant-modal button.btnRed", has_text="搜").first
    if search_button.count() == 0:
        raise RuntimeError("Forum search button not found in picker")
    search_button.click()
    time.sleep(_FORUM_SEARCH_SLEEP)

    option = page.locator(".ant-modal .listItem", has_text=forum_label).first
    if option.count() == 0:
        raise RuntimeError(f"Forum option not found in selector: {forum_label}")
    option.click()
    time.sleep(_MEDIUM_SLEEP)

    confirm = page.locator(".ant-modal button", has_text="确").first
    if confirm.count() == 0:
        raise RuntimeError("Forum confirm button not found in picker")
    confirm.click()
    time.sleep(_FORUM_SEARCH_SLEEP)

    current_label = _current_forum_label(page)
    if not _forum_label_matches(current_label, forum_label):
        raise RuntimeError(
            f"Forum selection did not persist: expected {forum_label}, got {current_label or 'none'}"
        )


def _fill_editor_with_content_blocks(
    page: Page,
    content: str,
    *,
    images: list[str],
    footer_html: str | None = None,
) -> None:
    """Fill editor while keeping image placeholders at the intended positions."""
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")
    editor.evaluate(
        """el => {
        el.innerHTML = "";
        el.dispatchEvent(new Event("input", { bubbles: true }));
    }"""
    )
    time.sleep(_SHORT_SLEEP)

    placeholder_re = re.compile(r"^\s*\[\[IMAGE:(.+?)\]\]\s*$")
    image_index = 0
    for line in content.split("\n"):
        if placeholder_re.match(line):
            if image_index < len(images):
                _upload_image(page, images[image_index])
                image_index += 1
            else:
                print(f"WARNING: Image placeholder found but no image provided: {line}")
            continue
        _append_paragraph(page, line)

    while image_index < len(images):
        _upload_image(page, images[image_index])
        image_index += 1

    if footer_html:
        _append_footer_html(page, footer_html)


def _append_placeholder_paragraph(page: Page, marker: str) -> None:
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")
    editor.evaluate(
        """(el, marker) => {
        const p = document.createElement("p");
        p.setAttribute("data-funba-placeholder", marker);
        p.textContent = "\\u200b";
        el.appendChild(p);
        el.dispatchEvent(new Event("input", { bubbles: true }));
    }""",
        marker,
    )
    time.sleep(_SHORT_SLEEP)


def _focus_placeholder(page: Page, marker: str) -> None:
    page.evaluate(
        """marker => {
        const node = document.querySelector(`p[data-funba-placeholder="${marker}"]`);
        if (!node) return;
        const selection = window.getSelection();
        if (!selection) return;
        const range = document.createRange();
        range.selectNodeContents(node);
        range.collapse(false);
        selection.removeAllRanges();
        selection.addRange(range);
    }""",
        marker,
    )
    time.sleep(_SHORT_SLEEP)


def _cleanup_placeholder(page: Page, marker: str) -> None:
    page.evaluate(
        """marker => {
        const node = document.querySelector(`p[data-funba-placeholder="${marker}"]`);
        if (!node) return;
        const text = (node.textContent || "").replace(/\\u200b/g, "").trim();
        if (!text && node.childElementCount === 0) node.remove();
    }""",
        marker,
    )
    time.sleep(_SHORT_SLEEP)


def _editor_image_count(page: Page) -> int:
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")
    return int(
        editor.evaluate(
            """el => el.querySelectorAll('img').length"""
        )
    )


def _editor_images_ready(page: Page) -> bool:
    editor = page.query_selector(".ProseMirror")
    if not editor:
        raise RuntimeError("Content editor (.ProseMirror) not found")
    return bool(
        editor.evaluate(
            """el => {
            const imgs = Array.from(el.querySelectorAll('img'));
            if (imgs.length === 0) return true;
            return imgs.every(img => {
                const src = img.getAttribute('src');
                return !!src && src.trim().length > 0;
            });
        }"""
        )
    )


def _upload_image(page: Page, image_path: str, marker: str | None = None) -> None:
    """Upload an image into the editor via the hidden file input."""
    editor = page.query_selector(".ProseMirror")
    if editor:
        editor.click()
        if marker:
            _focus_placeholder(page, marker)
        else:
            page.keyboard.press("End")
            page.keyboard.press("Enter")

    file_input = page.query_selector('input[type=file][accept*="image"]')
    if not file_input:
        raise RuntimeError("Image upload input not found")

    before_count = _editor_image_count(page)
    # Brief pause between consecutive uploads to let the editor settle
    if before_count > 0:
        time.sleep(_UPLOAD_SETTLE_SLEEP)
    file_input.set_input_files(image_path)
    deadline = time.time() + _UPLOAD_COUNT_TIMEOUT
    while time.time() < deadline:
        try:
            if _editor_image_count(page) >= before_count + 1:
                break
        except Exception:
            pass
        time.sleep(_SHORT_SLEEP)
    after_count = _editor_image_count(page)
    if after_count < before_count + 1:
        print(f"WARNING: Image upload may have failed: {image_path} (before={before_count}, after={after_count}) — continuing anyway")
    ready_deadline = time.time() + _UPLOAD_READY_TIMEOUT
    while time.time() < ready_deadline:
        try:
            if _editor_images_ready(page):
                break
        except Exception:
            pass
        time.sleep(_SHORT_SLEEP)
    if marker:
        _cleanup_placeholder(page, marker)
    print(f"Image uploaded: {image_path} (editor images: {before_count} -> {after_count})")


def _click_submit(page: Page) -> str:
    """Click the submit button and return the final thread URL when detectable."""
    captured_url: dict[str, str | None] = {"value": None}

    def _handle_response(response: Any) -> None:
        if captured_url["value"]:
            return
        extracted = _extract_thread_url_from_response(response)
        if extracted:
            captured_url["value"] = extracted

    _add_page_listener(page, "response", _handle_response)
    try:
        # Hupu uses a div.submitVideo, not a <button>
        page.click(".submitVideo")
        deadline = time.time() + 3.0
        while time.time() < deadline and not captured_url["value"]:
            time.sleep(_SHORT_SLEEP)
        body_text = page.locator("body").inner_text(timeout=3000)
        if "请先选择专区" in body_text:
            raise RuntimeError("Submit blocked by Hupu: 请先选择专区")
        if captured_url["value"]:
            return captured_url["value"]
        final_url = _wait_for_final_post_url(page)
        if final_url:
            return final_url
        raise RuntimeError(
            f"Submit completed but Hupu thread URL was not detected; still on {page.url}"
        )
    finally:
        _remove_page_listener(page, "response", _handle_response)


def _extract_thread_url_from_html(html: str) -> str | None:
    """Best-effort extract of the current thread URL from a rendered Hupu post page."""
    patterns = [
        r'"url":"(/(\d+)\.html)"',
        r'href="(/(\d+)\.html)"',
        r'content="(/(\d+)\.html)"',
        r'(https://bbs\.hupu\.com/(\d{6,12})\.html)',
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return _normalize_thread_url(match.group(1))
    return None


def _normalize_thread_url(value: str | None) -> str | None:
    if not value:
        return None
    candidate = str(value).strip()
    if not candidate:
        return None
    absolute_match = re.search(r"https://bbs\.hupu\.com/\d{6,12}\.html(?:[?#][^\s\"'<]*)?", candidate)
    if absolute_match:
        return absolute_match.group(0)
    relative_match = re.search(r"/\d{6,12}\.html(?:[?#][^\s\"'<]*)?", candidate)
    if relative_match:
        return f"{HUPU_HOME}{relative_match.group(0)}"
    digit_match = re.fullmatch(r"\d{6,12}", candidate)
    if digit_match:
        return f"{HUPU_HOME}/{candidate}.html"
    return None


def _extract_thread_url_from_url_like(value: str | None) -> str | None:
    normalized = _normalize_thread_url(value)
    if normalized:
        return normalized
    if not value:
        return None
    try:
        parsed = urlparse(str(value))
        query = parse_qs(parsed.query or "")
    except Exception:
        return None
    for key in ("tid", "threadId", "thread_id", "postId", "post_id"):
        values = query.get(key) or []
        for candidate in values:
            normalized = _normalize_thread_url(candidate)
            if normalized:
                return normalized
    return None


def _extract_thread_url_from_response_body(body: str) -> str | None:
    extracted = _extract_thread_url_from_html(body)
    if extracted:
        return extracted
    tid_patterns = [
        r'"tid"\s*:\s*"?(?P<tid>\d{6,12})"?',
        r'"threadId"\s*:\s*"?(?P<tid>\d{6,12})"?',
        r'"thread_id"\s*:\s*"?(?P<tid>\d{6,12})"?',
        r'"postId"\s*:\s*"?(?P<tid>\d{6,12})"?',
        r'"post_id"\s*:\s*"?(?P<tid>\d{6,12})"?',
        r'(?:[?&](?:tid|threadId|thread_id|postId|post_id)=)(?P<tid>\d{6,12})',
    ]
    for pattern in tid_patterns:
        match = re.search(pattern, body)
        if match:
            return _normalize_thread_url(match.group("tid"))
    return None


def _extract_thread_url_from_response(response: Any) -> str | None:
    response_url = str(getattr(response, "url", "") or "")
    if "hupu.com" not in response_url:
        return None
    extracted = _extract_thread_url_from_url_like(response_url)
    if extracted:
        return extracted
    headers_reader = getattr(response, "headers", None)
    if callable(headers_reader):
        try:
            headers = headers_reader() or {}
        except Exception:
            headers = {}
        if isinstance(headers, dict):
            for key in ("location", "Location"):
                extracted = _extract_thread_url_from_url_like(headers.get(key))
                if extracted:
                    return extracted
    for attr in ("text", "body"):
        reader = getattr(response, attr, None)
        if not callable(reader):
            continue
        try:
            payload = reader()
        except Exception:
            continue
        if isinstance(payload, bytes):
            try:
                payload = payload.decode("utf-8", errors="ignore")
            except Exception:
                continue
        if not isinstance(payload, str):
            continue
        extracted = _extract_thread_url_from_response_body(payload)
        if extracted:
            return extracted
    return None


def _extract_thread_url_from_page_state(page: Page) -> str | None:
    extracted = _extract_thread_url_from_url_like(getattr(page, "url", None))
    if extracted:
        return extracted
    try:
        html = page.content()
    except Exception:
        html = ""
    extracted = _extract_thread_url_from_response_body(html)
    if extracted:
        return extracted
    state = {}
    evaluator = getattr(page, "evaluate", None)
    if callable(evaluator):
        try:
            state = evaluator(
                """() => {
                let historyState = "";
                let documentTitle = "";
                let bodyText = "";
                let anchorHrefs = [];
                let resourceUrls = [];
                try { historyState = JSON.stringify(history.state || null); } catch {}
                try { documentTitle = document.title || ""; } catch {}
                try { bodyText = (document.body && document.body.innerText ? document.body.innerText : "").slice(0, 5000); } catch {}
                try {
                    anchorHrefs = Array.from(document.querySelectorAll("a[href]"))
                        .map(node => node.href || "")
                        .slice(0, 200);
                } catch {}
                try {
                    resourceUrls = performance.getEntriesByType("resource")
                        .map(entry => entry.name || "")
                        .slice(-200);
                } catch {}
                return {
                    history_state: historyState,
                    document_title: documentTitle,
                    body_text: bodyText,
                    anchor_hrefs: anchorHrefs,
                    resource_urls: resourceUrls,
                };
            }"""
            ) or {}
        except Exception:
            state = {}
    if not isinstance(state, dict):
        return None

    blob_candidates = [
        state.get("history_state"),
        state.get("document_title"),
        state.get("body_text"),
    ]
    for candidate in blob_candidates:
        extracted = _extract_thread_url_from_response_body(str(candidate or ""))
        if extracted:
            return extracted

    for key in ("anchor_hrefs", "resource_urls"):
        values = state.get(key) or []
        if not isinstance(values, list):
            continue
        for candidate in values:
            extracted = _extract_thread_url_from_url_like(str(candidate or ""))
            if extracted:
                return extracted
            extracted = _extract_thread_url_from_response_body(str(candidate or ""))
            if extracted:
                return extracted
    return None


def _add_page_listener(page: Page, event: str, handler: Any) -> None:
    listener = getattr(page, "on", None)
    if callable(listener):
        listener(event, handler)


def _remove_page_listener(page: Page, event: str, handler: Any) -> None:
    for name in ("off", "remove_listener", "removeListener"):
        listener = getattr(page, name, None)
        if callable(listener):
            try:
                listener(event, handler)
            except Exception:
                pass
            return

def _wait_for_final_post_url(page: Page, timeout_seconds: float = 20.0) -> str | None:
    """Poll for a stable thread URL after clicking submit.

    Hupu sometimes leaves the browser on `/newpost/<forum_id>` even after the page
    content has transitioned into the thread view. In that case, detect the thread
    URL from the rendered HTML instead of trusting `page.url`.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        extracted = _extract_thread_url_from_page_state(page)
        if extracted:
            return extracted
        time.sleep(_SHORT_SLEEP)
    return None


# ── Commands ──────────────────────────────────────────────────────────────────


def cmd_login(args: argparse.Namespace) -> None:
    """Import cookies from a Chrome profile."""
    profile = args.chrome_profile or "Profile 1"

    try:
        import browser_cookie3
        import os

        chrome_dir = os.path.expanduser(
            f"~/Library/Application Support/Google/Chrome/{profile}"
        )
        cookie_file = os.path.join(chrome_dir, "Cookies")
        if not os.path.exists(cookie_file):
            print(f"Chrome cookie file not found: {cookie_file}")
            print("Available profiles:")
            chrome_base = os.path.expanduser(
                "~/Library/Application Support/Google/Chrome"
            )
            for entry in sorted(os.listdir(chrome_base)):
                if os.path.exists(os.path.join(chrome_base, entry, "Cookies")):
                    print(f"  {entry}")
            sys.exit(1)

        cj = browser_cookie3.chrome(
            domain_name="hupu.com", cookie_file=cookie_file
        )
        cookies = [
            {
                "name": c.name,
                "value": c.value,
                "domain": c.domain,
                "path": c.path,
                "secure": bool(c.secure),
            }
            for c in cj
        ]

        COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(COOKIE_FILE, "w") as f:
            json.dump(cookies, f, indent=2)
        print(f"Imported {len(cookies)} cookies from Chrome {profile}")
        print(f"Saved to {COOKIE_FILE}")

        auth_cookies = [c for c in cookies if c["name"] in ("u", "us", "_CLT")]
        if auth_cookies:
            print("Auth cookies found. Login should work.")
        else:
            print(
                "WARNING: No auth cookies found. Make sure you're logged in on Chrome first."
            )

    except ImportError:
        print("browser-cookie3 not installed. Run: pip install browser-cookie3")
        sys.exit(1)


def cmd_check(args: argparse.Namespace) -> None:
    """Check if saved cookies are still valid."""
    cookies = _load_cookies()
    if not cookies:
        print("No cookies found. Run: python -m social_media.hupu.post login")
        sys.exit(1)

    with _playwright() as pw:
        context = _create_context(pw, headless=True)
        page = context.new_page()
        page.goto(HUPU_HOME, wait_until="domcontentloaded", timeout=15000)
        time.sleep(_HOME_PAGE_LOAD_SLEEP)

        logged_in, details = _login_state(page)
        if logged_in:
            print("Logged in.")
        else:
            print("Not logged in. Cookies may have expired.")
            print(f"Reason: {_login_failure_reason(details)}")
            print("Log in via Chrome and run: python -m social_media.hupu.post login")
            sys.exit(1)

        context.close()


def cmd_post(args: argparse.Namespace) -> None:
    """Create a post on Hupu with optional images and links."""
    title = args.title
    content = args.content
    forum = args.forum
    submit = args.submit
    images = args.image or []
    link_text = args.link_text
    link_url = args.link_url
    post_id = getattr(args, "post_id", None)

    try:
        forum_key, composer_id, forum_label = _resolve_forum(forum)
    except KeyError:
        available = ", ".join(f"{key}({meta['label']})" for key, meta in FORUMS.items())
        print(f"Unknown forum: {forum!r}. Available aliases: {available} or any Chinese Hupu forum label like '勇士专区'")
        sys.exit(1)

    artifact_dir = _resolve_artifact_dir(getattr(args, "artifact_dir", None), post_id=post_id, forum_label=forum_label)
    resolved_images, temp_images = _prepare_placeholder_images(content, images, post_id=post_id)
    stage = "starting"
    context = None
    page = None
    browser_events: dict[str, list[dict[str, str]]] = {
        "console": [],
        "pageerror": [],
        "requestfailed": [],
    }

    print(f"Forum: {forum_key} / {forum_label} (composer page {composer_id})")
    print(f"Title: {title}")
    print(f"Content: {content[:100]}{'...' if len(content) > 100 else ''}")
    if resolved_images:
        print(f"Images: {len(resolved_images)}")
    if link_url:
        print(f"Link: {link_text or link_url} -> {link_url}")
    print(f"Submit: {'YES' if submit else 'NO (dry run)'}")
    print(f"Artifacts: {artifact_dir}")
    print()

    _write_json_artifact(
        artifact_dir / "request.json",
        {
            "post_id": post_id,
            "forum": forum,
            "forum_label": forum_label,
            "forum_key": forum_key,
            "composer_id": composer_id,
            "title": title,
            "submit": bool(submit),
            "image_count": len(resolved_images),
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )

    try:
        with _playwright() as pw:
            context = _create_context(pw, headless=False)
            page = context.new_page()

            _add_page_listener(
                page,
                "console",
                lambda msg: browser_events["console"].append(
                    {
                        "type": str(getattr(msg, "type", "unknown")),
                        "text": str(getattr(msg, "text", "")),
                    }
                ),
            )
            _add_page_listener(
                page,
                "pageerror",
                lambda exc: browser_events["pageerror"].append({"text": str(exc)}),
            )
            _add_page_listener(
                page,
                "requestfailed",
                lambda request: browser_events["requestfailed"].append(
                    {
                        "url": str(getattr(request, "url", "")),
                        "method": str(getattr(request, "method", "")),
                        "failure": str(request.failure) if getattr(request, "failure", None) else "",
                    }
                ),
            )

            try:
                stage = "check_login"
                page.goto(HUPU_HOME, wait_until="domcontentloaded", timeout=15000)
                time.sleep(_HOME_PAGE_LOAD_SLEEP)
                logged_in, details = _login_state(page)
                _write_json_artifact(artifact_dir / "login_state.json", details)
                if not logged_in:
                    raise RuntimeError(
                        "Not logged in. Run: python -m social_media.hupu.post login. "
                        f"Reason: {_login_failure_reason(details)}"
                    )
                print("Logged in.")

                stage = "open_composer"
                post_url = f"{HUPU_HOME}/newpost/{composer_id}"
                page.goto(post_url, wait_until="domcontentloaded", timeout=15000)
                time.sleep(_POST_PAGE_LOAD_SLEEP)
                print(f"Post page loaded: {page.url}")
                _safe_page_screenshot(page, artifact_dir / "composer_loaded.png")

                stage = "select_forum"
                _ensure_forum_selected(page, forum_label)
                print(f"Forum selected: {forum_label}")
                _safe_page_screenshot(page, artifact_dir / "forum_selected.png")

                stage = "fill_title"
                title_input = page.query_selector('input[placeholder*="标题"]')
                if not title_input:
                    raise RuntimeError("Title input not found")
                title_input.fill(title)
                print("Title filled.")

                footer_parts = []
                if link_url:
                    display = link_text or link_url
                    footer_parts.append(
                        f'<p><a href="{link_url}" target="_blank">{display}</a></p>'
                    )
                footer_html = "".join(footer_parts) if footer_parts else None

                stage = "fill_content"
                _fill_editor_with_content_blocks(page, content, images=resolved_images, footer_html=footer_html)
                print("Content filled.")

                time.sleep(_LONG_SLEEP)
                filled_screenshot = artifact_dir / "filled.png"
                _safe_page_screenshot(page, filled_screenshot)
                print(f"Screenshot: {filled_screenshot}")

                if submit:
                    stage = "submit"
                    final_url = _click_submit(page)
                    submitted_screenshot = artifact_dir / "submitted.png"
                    _safe_page_screenshot(page, submitted_screenshot)
                    _write_json_artifact(
                        artifact_dir / "result.json",
                        {
                            "status": "published",
                            "final_url": final_url,
                            "page_url": _safe_page_url(page),
                            "captured_at": datetime.utcnow().isoformat() + "Z",
                        },
                    )
                    print(f"Post submitted! URL: {final_url}")
                else:
                    _write_json_artifact(
                        artifact_dir / "result.json",
                        {
                            "status": "dry_run",
                            "page_url": _safe_page_url(page),
                            "captured_at": datetime.utcnow().isoformat() + "Z",
                        },
                    )
                    print("\n[DRY RUN] Content filled but not submitted.")
                    print("Pass --submit to actually post.")
            except Exception as exc:
                _persist_hupu_failure_artifacts(
                    artifact_dir,
                    page=page,
                    stage=stage,
                    requested_forum_label=forum_label,
                    title=title,
                    content=content,
                    post_id=post_id,
                    exception=exc,
                    browser_events=browser_events,
                )
                raise
    except Exception as exc:
        print(f"ERROR: {exc}")
        print(f"Artifacts saved to: {artifact_dir}")
        sys.exit(1)
    finally:
        if context is not None:
            try:
                context.close()
            except Exception:
                pass

    for path in temp_images:
        try:
            Path(path).unlink(missing_ok=True)
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m social_media.hupu.post",
        description="Auto-post to Hupu forums.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Import cookies from Chrome.")
    p_login.add_argument(
        "--chrome-profile",
        default="Profile 1",
        help="Chrome profile name (default: 'Profile 1')",
    )

    sub.add_parser("check", help="Check if login session is valid.")

    p_post = sub.add_parser("post", help="Create a post on Hupu.")
    p_post.add_argument("--title", required=True, help="Post title")
    p_post.add_argument("--content", required=True, help="Post content (newlines become paragraphs)")
    p_post.add_argument(
        "--forum",
        default="nba",
        help="Forum name or label (for example: nba, CBA版, 雷霆专区, 76人专区, hawks)",
    )
    p_post.add_argument("--image", action="append", help="Image file to upload (repeatable)")
    p_post.add_argument("--link-text", help="Display text for footer link")
    p_post.add_argument("--link-url", help="URL for footer link")
    p_post.add_argument("--post-id", type=int, dest="post_id", help="SocialPost ID — resolve slot-based images from the image pool")
    p_post.add_argument("--artifact-dir", help="Directory for debug screenshots/logs/artifacts")
    p_post.add_argument("--submit", action="store_true", help="Actually submit (default: dry run)")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
