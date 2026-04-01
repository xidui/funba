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
import html
import json
import re
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

try:
    from playwright.sync_api import sync_playwright, Page, BrowserContext
except ModuleNotFoundError:
    sync_playwright = None
    Page = BrowserContext = Any
from .forums import normalize_hupu_forum

MODULE_DIR = Path(__file__).resolve().parent
COOKIE_FILE = MODULE_DIR / ".hupu_cookies.json"
BROWSER_DATA_DIR = MODULE_DIR / ".hupu_browser_data"

HUPU_HOME = "https://bbs.hupu.com"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

NBA_COMPOSER_FORUM_ID = 179
CBA_COMPOSER_FORUM_ID = 346

FORUMS = {
    "nba": {"composer_id": NBA_COMPOSER_FORUM_ID, "label": "湿乎乎的话题", "aliases": ("nba", "NBA版", "湿乎乎的话题", "篮球场")},
    "cba": {"composer_id": CBA_COMPOSER_FORUM_ID, "label": "CBA版", "aliases": ("cba", "CBA版")},
}


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


def _is_logged_in(page: Page) -> bool:
    """Check login by looking for auth cookies."""
    cookies = page.context.cookies()
    return any(c["name"] in ("u", "us", "_CLT") and c["value"] for c in cookies)


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
        _capture_compact_screenshot(target, tmp.name)
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
    time.sleep(0.2)


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
    time.sleep(0.3)


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
    time.sleep(1)

    search_input = page.locator('input[placeholder="添加专区可以让更多JR和你一起讨论"]').first
    if search_input.count() == 0:
        raise RuntimeError("Forum search input not found in picker")
    search_input.fill(forum_label)

    search_button = page.locator(".ant-modal button.btnRed", has_text="搜").first
    if search_button.count() == 0:
        raise RuntimeError("Forum search button not found in picker")
    search_button.click()
    time.sleep(1.5)

    option = page.locator(".ant-modal .listItem", has_text=forum_label).first
    if option.count() == 0:
        raise RuntimeError(f"Forum option not found in selector: {forum_label}")
    option.click()
    time.sleep(0.8)

    confirm = page.locator(".ant-modal button", has_text="确").first
    if confirm.count() == 0:
        raise RuntimeError("Forum confirm button not found in picker")
    confirm.click()
    time.sleep(1.5)

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
    time.sleep(0.3)

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
    time.sleep(0.2)


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
    time.sleep(0.2)


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
    time.sleep(0.1)


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
    if before_count > 0:
        try:
            file_input.set_input_files([])
            time.sleep(0.5)
            file_input = page.query_selector('input[type=file][accept*="image"]')
            if not file_input:
                raise RuntimeError("Image upload input disappeared after reset")
        except Exception:
            pass
    file_input.set_input_files(image_path)
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            if _editor_image_count(page) >= before_count + 1:
                break
        except Exception:
            pass
        time.sleep(0.5)
    after_count = _editor_image_count(page)
    if after_count < before_count + 1:
        print(f"WARNING: Image upload may have failed: {image_path} (before={before_count}, after={after_count}) — continuing anyway")
    ready_deadline = time.time() + 8
    while time.time() < ready_deadline:
        try:
            if _editor_images_ready(page):
                break
        except Exception:
            pass
        time.sleep(0.3)
    if marker:
        _cleanup_placeholder(page, marker)
    print(f"Image uploaded: {image_path} (editor images: {before_count} -> {after_count})")


def _click_submit(page: Page) -> str:
    """Click the submit button and return the final thread URL when detectable."""
    # Hupu uses a div.submitVideo, not a <button>
    page.click(".submitVideo")
    time.sleep(1)
    body_text = page.locator("body").inner_text(timeout=3000)
    if "请先选择专区" in body_text:
        raise RuntimeError("Submit blocked by Hupu: 请先选择专区")
    final_url = _wait_for_final_post_url(page)
    return final_url or page.url


def _extract_thread_url_from_html(html: str) -> str | None:
    """Best-effort extract of the current thread URL from a rendered Hupu post page."""
    patterns = [
        r'"url":"(/(\d+)\.html)"',
        r'href="(/(\d+)\.html)"',
        r'content="(/(\d+)\.html)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            rel = match.group(1)
            if rel.startswith("/"):
                return f"{HUPU_HOME}{rel}"
            return rel
    return None


def _capture_compact_screenshot(url: str, output_path: str, *, wait_ms: int = 4000) -> None:
    """Capture a compact screenshot suited for inline forum posts."""
    target_url = url

    with _playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="zh-CN",
            user_agent=REAL_BROWSER_UA,
        )
        page = context.new_page()
        page.goto(target_url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(wait_ms / 1000)

        # Prefer a meaningful in-page section instead of full-page screenshots.
        selectors = [
            '.rankings-table-wrap',
            '.rankings-table',
            '[class*="rankings-table"]',
            '[class*="leaderboard"]',
            '[class*="ranking"]',
            '[class*="game-metrics"]',
            '[class*="metric-strip"]',
            '[class*="boxscore"]',
            '[class*="team-stats"]',
            '[class*="player-stats"]',
            '[class*="game"]',
            'main',
        ]
        captured = False
        for selector in selectors:
            locator = page.locator(selector).first
            try:
                if locator.count() == 0:
                    continue
                ranking_selector = selector in {'.rankings-table-wrap', '.rankings-table', '[class*="rankings-table"]'}
                if not ranking_selector:
                    locator.scroll_into_view_if_needed(timeout=1000)
                    time.sleep(0.3)
                box = locator.bounding_box()
                if not box:
                    continue
                clip_x = max(box["x"], 0)
                clip_y = max(box["y"], 0)
                width = min(max(box["width"], 720), 1100)
                height = min(max(box["height"], 320), 720)

                # For ranking pages, keep the page at the original scroll position so
                # the metric header/title remains visible above the table.
                if ranking_selector:
                    header = page.locator('[class*="header"]').first
                    if header.count() > 0:
                        header_box = header.bounding_box()
                        if header_box:
                            clip_x = max(min(box["x"], header_box["x"]) - 8, 0)
                            clip_y = max(header_box["y"] - 16, 0)
                            width = min(max(max(box["width"], header_box["width"]), 760), 1180)
                            desired_bottom = min(box["y"] + 260, clip_y + 840)
                            height = max(520, desired_bottom - clip_y)
                page.screenshot(
                    path=output_path,
                    clip={
                        "x": clip_x,
                        "y": clip_y,
                        "width": width,
                        "height": height,
                    },
                )
                captured = True
                break
            except Exception:
                continue
        if not captured:
            page.screenshot(path=output_path)

        context.close()
        browser.close()


def _wait_for_final_post_url(page: Page, timeout_seconds: float = 25.0) -> str | None:
    """Poll for a stable thread URL after clicking submit.

    Hupu sometimes leaves the browser on `/newpost/<forum_id>` even after the page
    content has transitioned into the thread view. In that case, detect the thread
    URL from the rendered HTML instead of trusting `page.url`.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current_url = page.url
        if re.search(r"/\d+\.html($|[?#])", current_url):
            return current_url
        try:
            html = page.content()
        except Exception:
            html = ""
        extracted = _extract_thread_url_from_html(html)
        if extracted:
            return extracted
        time.sleep(1)
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
        time.sleep(2)

        if _is_logged_in(page):
            print("Logged in.")
        else:
            print("Not logged in. Cookies may have expired.")
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

    resolved_images, temp_images = _prepare_placeholder_images(content, images, post_id=post_id)

    print(f"Forum: {forum_key} / {forum_label} (composer page {composer_id})")
    print(f"Title: {title}")
    print(f"Content: {content[:100]}{'...' if len(content) > 100 else ''}")
    if resolved_images:
        print(f"Images: {len(resolved_images)}")
    if link_url:
        print(f"Link: {link_text or link_url} -> {link_url}")
    print(f"Submit: {'YES' if submit else 'NO (dry run)'}")
    print()

    with _playwright() as pw:
        context = _create_context(pw, headless=False)
        page = context.new_page()

        # Check login
        page.goto(HUPU_HOME, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)
        if not _is_logged_in(page):
            print("ERROR: Not logged in. Run: python -m social_media.hupu.post login")
            context.close()
            sys.exit(1)
        print("Logged in.")

        # Navigate to post page
        post_url = f"{HUPU_HOME}/newpost/{composer_id}"
        page.goto(post_url, wait_until="domcontentloaded", timeout=15000)
        time.sleep(3)
        print(f"Post page loaded: {page.url}")
        _ensure_forum_selected(page, forum_label)
        print(f"Forum selected: {forum_label}")

        # Fill title
        title_input = page.query_selector('input[placeholder*="标题"]')
        if not title_input:
            print("ERROR: Title input not found")
            context.close()
            sys.exit(1)
        title_input.fill(title)
        print(f"Title filled.")

        # Build footer HTML (links)
        footer_parts = []
        if link_url:
            display = link_text or link_url
            footer_parts.append(
                f'<p><a href="{link_url}" target="_blank">{display}</a></p>'
            )
        footer_html = "".join(footer_parts) if footer_parts else None

        _fill_editor_with_content_blocks(page, content, images=resolved_images, footer_html=footer_html)
        print(f"Content filled.")

        time.sleep(1)
        page.screenshot(path="/tmp/hupu_post_filled.png")
        print("Screenshot: /tmp/hupu_post_filled.png")

        if submit:
            final_url = _click_submit(page)
            page.screenshot(path="/tmp/hupu_post_submitted.png")
            print(f"Post submitted! URL: {final_url}")
        else:
            print("\n[DRY RUN] Content filled but not submitted.")
            print("Pass --submit to actually post.")

        context.close()

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
    p_post.add_argument("--submit", action="store_true", help="Actually submit (default: dry run)")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
