"""Auto-post to Hupu forums using Playwright with Chrome cookie injection.

Usage:
    # Import cookies from Chrome (after logging in via normal Chrome)
    python -m tools.hupu_post login --chrome-profile "Profile 1"

    # Check login status
    python -m tools.hupu_post check

    # Post with plain text (dry run by default)
    python -m tools.hupu_post post --title "xxx" --content "xxx" --forum "nba"

    # Post with images and links
    python -m tools.hupu_post post --title "xxx" --content "xxx" --forum "thunder" \\
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
import time
from pathlib import Path

from playwright.sync_api import sync_playwright, Page, BrowserContext

COOKIE_FILE = Path(__file__).resolve().parent.parent / ".hupu_cookies.json"

HUPU_HOME = "https://bbs.hupu.com"
FUNBA_LOCAL_BASE = "http://127.0.0.1:5001"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

FORUMS = {
    "nba": {"id": 179, "label": "NBA版", "aliases": ("nba", "NBA版")},
    "cba": {"id": 346, "label": "CBA版", "aliases": ("cba", "CBA版")},
    "thunder": {"id": 129, "label": "雷霆专区", "aliases": ("thunder", "雷霆专区")},
    "lakers": {"id": 127, "label": "湖人专区", "aliases": ("lakers", "湖人专区")},
}


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
    browser = pw.chromium.launch(headless=headless)
    context = browser.new_context(
        viewport={"width": 1280, "height": 1200},
        locale="zh-CN",
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
    """Resolve an input forum alias into (key, id, label)."""
    normalized = (forum or "").strip()
    for key, meta in FORUMS.items():
        aliases = meta.get("aliases") or ()
        if normalized == key or normalized in aliases:
            return key, int(meta["id"]), str(meta["label"])
    raise KeyError(forum)


def _render_inline_html(text: str) -> str:
    """Render a limited markdown-like subset into editor-safe HTML."""
    pattern = re.compile(r"\*\*(.+?)\*\*|\[([^\]]+)\]\((https?://[^)]+)\)")
    parts: list[str] = []
    cursor = 0
    for match in pattern.finditer(text):
        parts.append(html.escape(text[cursor:match.start()]))
        if match.group(1) is not None:
            parts.append(f"<strong>{html.escape(match.group(1))}</strong>")
        else:
            label = html.escape(match.group(2))
            url = html.escape(match.group(3), quote=True)
            parts.append(f'<a href="{url}" target="_blank">{label}</a>')
        cursor = match.end()
    parts.append(html.escape(text[cursor:]))
    return "".join(parts)


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


def _ensure_forum_selected(page: Page, forum_label: str) -> None:
    """Select the target forum in the composer if Hupu leaves it blank."""
    body_text = page.locator("body").inner_text(timeout=3000)
    if (
        f"专区：\n{forum_label}" in body_text
        or f"专区： {forum_label}" in body_text
        or forum_label in body_text
    ):
        return

    add_forum = page.locator("text=添加专区").first
    if add_forum.count() == 0:
        # Some default forums (e.g. NBA版) may already be implicitly selected and
        # the explicit picker affordance is not rendered. In that case, tolerate
        # the missing trigger instead of hard-failing.
        return
    add_forum.click()
    time.sleep(1)

    option = page.locator(f"text={forum_label}").first
    if option.count() == 0:
        raise RuntimeError(f"Forum option not found in selector: {forum_label}")
    option.click()
    time.sleep(1)

    # Some forum pickers need an extra click to close/confirm.
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass
    time.sleep(0.5)


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
        raise RuntimeError(
            f"Image upload did not appear in editor: {image_path} (before={before_count}, after={after_count})"
        )
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
    if target_url.startswith("https://funba.app/"):
        target_url = target_url.replace("https://funba.app", FUNBA_LOCAL_BASE, 1)
    elif target_url.startswith("http://funba.app/"):
        target_url = target_url.replace("http://funba.app", FUNBA_LOCAL_BASE, 1)

    with sync_playwright() as pw:
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
                locator.scroll_into_view_if_needed(timeout=1000)
                time.sleep(0.3)
                box = locator.bounding_box()
                if not box:
                    continue
                width = min(max(box["width"], 720), 1100)
                height = min(max(box["height"], 320), 720)
                page.screenshot(
                    path=output_path,
                    clip={
                        "x": max(box["x"], 0),
                        "y": max(box["y"], 0),
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
        print("No cookies found. Run: python -m tools.hupu_post login")
        sys.exit(1)

    with sync_playwright() as pw:
        context = _create_context(pw, headless=True)
        page = context.new_page()
        page.goto(HUPU_HOME, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)

        if _is_logged_in(page):
            print("Logged in.")
        else:
            print("Not logged in. Cookies may have expired.")
            print("Log in via Chrome and run: python -m tools.hupu_post login")
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

    try:
        forum_key, forum_id, forum_label = _resolve_forum(forum)
    except KeyError:
        available = ", ".join(f"{key}({meta['label']})" for key, meta in FORUMS.items())
        print(f"Unknown forum: {forum!r}. Available: {available}")
        sys.exit(1)

    print(f"Forum: {forum_key} / {forum_label} (ID {forum_id})")
    print(f"Title: {title}")
    print(f"Content: {content[:100]}{'...' if len(content) > 100 else ''}")
    if images:
        print(f"Images: {len(images)}")
    if link_url:
        print(f"Link: {link_text or link_url} -> {link_url}")
    print(f"Submit: {'YES' if submit else 'NO (dry run)'}")
    print()

    with sync_playwright() as pw:
        context = _create_context(pw, headless=False)
        page = context.new_page()

        # Check login
        page.goto(HUPU_HOME, wait_until="domcontentloaded", timeout=15000)
        time.sleep(2)
        if not _is_logged_in(page):
            print("ERROR: Not logged in. Run: python -m tools.hupu_post login")
            context.close()
            sys.exit(1)
        print("Logged in.")

        # Navigate to post page
        post_url = f"{HUPU_HOME}/newpost/{forum_id}"
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

        _fill_editor_with_content_blocks(page, content, images=images, footer_html=footer_html)
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


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m tools.hupu_post",
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
    p_post.add_argument("--forum", default="nba", help=f"Forum name ({', '.join(FORUMS)})")
    p_post.add_argument("--image", action="append", help="Image file to upload (repeatable)")
    p_post.add_argument("--link-text", help="Display text for footer link")
    p_post.add_argument("--link-url", help="URL for footer link")
    p_post.add_argument("--submit", action="store_true", help="Actually submit (default: dry run)")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
