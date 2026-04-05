"""Auto-post to Xiaohongshu creator graph notes using Playwright with cookie injection.

Usage:
    # Import cookies from Chrome (after logging in via normal Chrome)
    python -m social_media.xiaohongshu.post login --chrome-profile Default

    # Check login status
    python -m social_media.xiaohongshu.post check

    # Fill the composer without submitting (dry run)
    python -m social_media.xiaohongshu.post post \
        --title "雷霆这场赢球，不只是亚历山大又拿高分" \
        --content "正文..." \
        --image /tmp/funba_asset.png

    # Save a draft
    python -m social_media.xiaohongshu.post post \
        --title "标题" --content "正文" --image /tmp/funba_asset.png --save-draft

    # Submit for real
    python -m social_media.xiaohongshu.post post \
        --title "标题" --content "正文" --image /tmp/funba_asset.png --submit
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
    from playwright.sync_api import Page
except ModuleNotFoundError:
    Page = Any

from ..funba_capture import capture_funba_url as _capture_funba_url
from . import auth as xhs_auth

TITLE_PLACEHOLDER = "填写标题会有更多赞哦"
GRAPHIC_TAB_TEXT = "上传图文"
PUBLISH_BUTTON_TEXT = "发布"
DRAFT_BUTTON_TEXT = "暂存离开"
TITLE_LIMIT = 20
BODY_LIMIT = 1000
TOPIC_BUTTON_TEXT = "话题"
TOPIC_CONTAINER_SELECTOR = "#creator-editor-topic-container"

REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

_LOGIN_TEXT_MARKERS = (
    "短信登录",
    "发送验证码",
    "手机号登录",
    "登录后体验更多功能",
)
_ACTION_ERROR_MARKERS = (
    "请输入标题",
    "标题不能为空",
    "请输入正文",
    "正文不能为空",
    "请上传图片",
    "网络异常",
)
_NOTE_URL_RE = re.compile(
    r"https://www\.xiaohongshu\.com/(?:(?:explore)|(?:discovery/item))/([A-Za-z0-9]+)(?:[?#][^\s\"'<]*)?"
)
_NOTE_ID_PATTERNS = (
    r'"noteId"\s*:\s*"(?P<note_id>[A-Za-z0-9]{8,40})"',
    r'"note_id"\s*:\s*"(?P<note_id>[A-Za-z0-9]{8,40})"',
    r'"itemId"\s*:\s*"(?P<note_id>[A-Za-z0-9]{8,40})"',
    r'"item_id"\s*:\s*"(?P<note_id>[A-Za-z0-9]{8,40})"',
)
_TAGS_PLACEHOLDER_RE = re.compile(r"^\s*\[\[TAGS:(.+?)\]\]\s*$")
_URL_RE = re.compile(r"https?://\S+")

_SHORT_SLEEP = 0.1
_MEDIUM_SLEEP = 0.4
_TAB_SETTLE_SLEEP = 1.0
_UPLOAD_SETTLE_SLEEP = 2.5
_EDITOR_SETTLE_SLEEP = 0.5
_ACTION_TIMEOUT_SECONDS = 30.0
_KEEP_OPEN_FALLBACK_SECONDS = 600


def _playwright():
    return xhs_auth._playwright()


def _load_cookies() -> list[dict[str, Any]]:
    return xhs_auth.load_cookies()


def _create_context(pw, *, headless: bool = True):
    return xhs_auth._create_context(pw, _load_cookies(), headless=headless)


def _page_text(page: Page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def _is_logged_in(page: Page) -> bool:
    compact = xhs_auth._compress_text(_page_text(page))
    return (
        page.url.startswith("https://creator.xiaohongshu.com/")
        and "/login" not in page.url
        and all(marker not in compact for marker in _LOGIN_TEXT_MARKERS)
        and ("发布笔记" in compact or "笔记管理" in compact or "创作服务平台" in compact)
    )


def _login_failure_reason(page: Page) -> str:
    compact = xhs_auth._compress_text(_page_text(page))
    if "/login" in page.url:
        return f"redirected to login: {page.url}"
    for marker in _LOGIN_TEXT_MARKERS:
        if marker in compact:
            return f"creator page still shows login UI marker: {marker}"
    if not compact:
        return "could not read page body"
    return f"unexpected creator page state: {page.url}"


def _first_visible(locator):
    count = locator.count()
    for index in range(count):
        item = locator.nth(index)
        try:
            if item.is_visible():
                return item
        except Exception:
            continue
    return None


def _graphic_upload_input(page: Page):
    inputs = page.locator("input[type=file]")
    count = inputs.count()
    for index in range(count):
        item = inputs.nth(index)
        try:
            accept = (item.get_attribute("accept") or "").lower()
        except Exception:
            accept = ""
        if any(token in accept for token in (".jpg", ".jpeg", ".png", ".webp", "image")):
            return item
    raise RuntimeError("Graphic upload input not found on Xiaohongshu publish page")


def _switch_to_graphic_mode(page: Page) -> None:
    tabs = page.locator(".header-tabs .creator-tab", has_text=GRAPHIC_TAB_TEXT)
    target = _first_visible(tabs)
    if not target:
        raise RuntimeError("Visible Xiaohongshu '上传图文' tab not found")
    try:
        target.click(timeout=5000)
    except Exception:
        # Visible mode can report the tab as outside the viewport even though the
        # DOM node is interactable, so fall back to a direct DOM click.
        target.evaluate(
            """el => {
            el.scrollIntoView({ block: "center", inline: "center" });
            el.click();
        }"""
        )
    time.sleep(_TAB_SETTLE_SLEEP)

    _graphic_upload_input(page)
    body_text = _page_text(page)
    if "上传图片" not in body_text:
        raise RuntimeError("Xiaohongshu did not switch into graphic-post mode")


def _wait_for_editor(page: Page) -> None:
    page.locator(f'input[placeholder="{TITLE_PLACEHOLDER}"]').first.wait_for(state="visible", timeout=20000)
    page.locator('.tiptap.ProseMirror[contenteditable="true"]').first.wait_for(state="visible", timeout=20000)
    time.sleep(_EDITOR_SETTLE_SLEEP)


def _parse_image_placeholder(line: str) -> dict[str, str] | None:
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


def _parse_tags_placeholder(line: str) -> list[str] | None:
    match = _TAGS_PLACEHOLDER_RE.match(line or "")
    if not match:
        return None
    payload = match.group(1)
    tags = re.findall(r"#([^\s#]+)", payload)
    normalized: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        candidate = f"#{tag.strip()}"
        if not candidate or candidate == "#" or candidate in seen:
            continue
        seen.add(candidate)
        normalized.append(candidate)
    return normalized


def _load_post_image_pool(post_id: int) -> dict[str, str]:
    from sqlalchemy.orm import sessionmaker

    from db.models import SocialPostImage, engine

    Session = sessionmaker(bind=engine)
    with Session() as session:
        rows = (
            session.query(SocialPostImage)
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
        if not placeholder_specs and pool and len(resolved_images) == 0:
            resolved_images.extend(pool.values())
        return resolved_images, temp_paths

    for spec in placeholder_specs[len(resolved_images):]:
        slot = spec.get("slot")
        if slot and slot in pool:
            resolved_images.append(pool[slot])
            continue

        target = spec.get("target")
        if not target:
            continue
        tmp = tempfile.NamedTemporaryFile(prefix="funba_xhs_", suffix=".png", delete=False)
        tmp.close()
        _capture_funba_url(target, tmp.name)
        temp_paths.append(tmp.name)
        resolved_images.append(tmp.name)
    return resolved_images, temp_paths


def _render_plain_text_line(line: str) -> str:
    rendered = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
    rendered = re.sub(
        r"\[([^\]]+)\]\((https?://[^)]+)\)",
        lambda match: f"{match.group(1)} {match.group(2)}",
        rendered,
    )
    return html.unescape(rendered)


def _content_lines_for_editor(content: str) -> list[str]:
    lines: list[str] = []
    for raw_line in content.split("\n"):
        if _parse_image_placeholder(raw_line):
            continue
        if _parse_tags_placeholder(raw_line):
            continue
        lines.append(_render_plain_text_line(raw_line.rstrip()))
    return lines


def _extract_tags_from_content(content: str) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw_line in content.split("\n"):
        tags = _parse_tags_placeholder(raw_line)
        if not tags:
            continue
        for tag in tags:
            if tag in seen:
                continue
            seen.add(tag)
            ordered.append(tag)
    return ordered


def _body_text_for_xiaohongshu(content: str) -> str:
    return "\n".join(_content_lines_for_editor(content)).strip()


def _estimated_body_length_for_xiaohongshu(content: str) -> int:
    body = _body_text_for_xiaohongshu(content)
    tags = _extract_tags_from_content(content)
    if not tags:
        return len(body)
    divider = 1 if body else 0
    tag_text = " ".join(tags)
    return len(body) + divider + len(tag_text)


def _upload_images(page: Page, images: list[str]) -> None:
    if not images:
        raise RuntimeError("Xiaohongshu graphic posts require at least one image")
    upload_input = _graphic_upload_input(page)
    upload_input.set_input_files(images)
    time.sleep(_UPLOAD_SETTLE_SLEEP)
    _wait_for_editor(page)


def _fill_title(page: Page, title: str) -> None:
    title_input = page.locator(f'input[placeholder="{TITLE_PLACEHOLDER}"]').first
    title_input.click()
    title_input.fill(title)
    time.sleep(_SHORT_SLEEP)


def _fill_body(page: Page, content: str) -> None:
    lines = _content_lines_for_editor(content)
    editor = page.locator('.tiptap.ProseMirror[contenteditable="true"]').first
    editor.click()
    editor.evaluate(
        """(el, lines) => {
        const nextLines = Array.isArray(lines) && lines.length > 0 ? lines : [""];
        el.innerHTML = "";
        for (const line of nextLines) {
            const p = document.createElement("p");
            if (line && line.length > 0) {
                p.textContent = line;
            } else {
                const br = document.createElement("br");
                br.className = "ProseMirror-trailingBreak";
                p.appendChild(br);
            }
            el.appendChild(p);
        }
        el.dispatchEvent(new Event("input", { bubbles: true }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
    }""",
        lines,
    )
    time.sleep(_EDITOR_SETTLE_SLEEP)


def _move_cursor_to_end(page: Page) -> None:
    editor = page.locator('.tiptap.ProseMirror[contenteditable="true"]').first
    editor.click()
    editor.evaluate(
        """el => {
        const selection = window.getSelection();
        if (!selection) return;
        const range = document.createRange();
        range.selectNodeContents(el);
        range.collapse(false);
        selection.removeAllRanges();
        selection.addRange(range);
    }"""
    )
    time.sleep(_SHORT_SLEEP)


def _append_editor_paragraph(page: Page) -> None:
    _move_cursor_to_end(page)
    page.keyboard.press("Enter")
    time.sleep(_SHORT_SLEEP)


def _select_native_topic_candidate(page: Page, tag: str) -> dict[str, object]:
    normalized = tag if tag.startswith("#") else f"#{tag}"
    page.wait_for_function(
        """() => {
        const container = document.querySelector('#creator-editor-topic-container');
        return !!container && container.querySelectorAll('.item').length > 0;
    }""",
        timeout=5000,
    )
    return page.evaluate(
        """tag => {
        const container = document.querySelector('#creator-editor-topic-container');
        if (!container) return { clicked: false, names: [] };
        const items = Array.from(container.querySelectorAll('.item'));
        const normalize = value => (value || '').trim().toLowerCase();
        const names = items.map(item => {
            const name = item.querySelector('.name');
            return (name && name.textContent ? name.textContent : '').trim();
        }).filter(Boolean);
        const target = items.find(item => {
            const name = item.querySelector('.name');
            const value = name && name.textContent ? name.textContent : '';
            return normalize(value) === normalize(tag) || normalize(value).startsWith(normalize(tag));
        });
        if (!target) return { clicked: false, names };
        target.click();
        return { clicked: true, names };
    }""",
        normalized,
    )


def _insert_topics(page: Page, tags: list[str]) -> None:
    if not tags:
        return

    _append_editor_paragraph(page)
    for index, tag in enumerate(tags):
        display_tag = tag if tag.startswith("#") else f"#{tag}"
        button = page.locator("button", has_text=TOPIC_BUTTON_TEXT).first
        button.click()
        time.sleep(_SHORT_SLEEP)
        page.keyboard.type(display_tag.lstrip("#"))
        page.locator(TOPIC_CONTAINER_SELECTOR).first.wait_for(state="visible", timeout=5000)
        result = _select_native_topic_candidate(page, display_tag)
        if not bool(result.get("clicked")):
            page.keyboard.press("Enter")
            time.sleep(_SHORT_SLEEP)
        else:
            time.sleep(_SHORT_SLEEP)
        suggestion_still_present = page.locator('.tiptap.ProseMirror[contenteditable="true"] .suggestion').count() > 0
        if not suggestion_still_present:
            continue
        raise RuntimeError(
            f"Could not select Xiaohongshu native topic candidate: {display_tag}; "
            f"available candidates={result.get('names') or []}"
        )
        if index < len(tags) - 1:
            page.keyboard.type(" ")
            time.sleep(_SHORT_SLEEP)


def _normalize_note_url(value: str | None) -> str | None:
    if not value:
        return None
    candidate = str(value).strip().replace("\\/", "/")
    if not candidate:
        return None
    url_match = _NOTE_URL_RE.search(candidate)
    if url_match:
        return url_match.group(0)
    if re.fullmatch(r"[A-Za-z0-9]{8,40}", candidate):
        return f"https://www.xiaohongshu.com/explore/{candidate}"
    return None


def _extract_note_url_from_text(payload: str) -> str | None:
    direct = _normalize_note_url(payload)
    if direct:
        return direct
    for pattern in _NOTE_ID_PATTERNS:
        match = re.search(pattern, payload)
        if match:
            return _normalize_note_url(match.group("note_id"))
    try:
        parsed = json.loads(payload)
    except Exception:
        return None
    return _extract_note_url_from_json(parsed)


def _extract_note_url_from_json(payload: Any) -> str | None:
    if isinstance(payload, str):
        return _normalize_note_url(payload)
    if isinstance(payload, list):
        for item in payload:
            found = _extract_note_url_from_json(item)
            if found:
                return found
        return None
    if not isinstance(payload, dict):
        return None

    for key in ("share_url", "shareUrl", "url", "note_url", "noteUrl"):
        found = _normalize_note_url(payload.get(key))
        if found:
            return found
    for key in ("noteId", "note_id", "itemId", "item_id"):
        found = _normalize_note_url(payload.get(key))
        if found:
            return found
    for value in payload.values():
        found = _extract_note_url_from_json(value)
        if found:
            return found
    return None


def _extract_note_url_from_response(response: Any) -> str | None:
    response_url = str(getattr(response, "url", "") or "")
    if "xiaohongshu.com" not in response_url:
        return None
    for reader_name in ("text", "body"):
        reader = getattr(response, reader_name, None)
        if not callable(reader):
            continue
        try:
            payload = reader()
        except Exception:
            continue
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="ignore")
        if not isinstance(payload, str):
            continue
        found = _extract_note_url_from_text(payload)
        if found:
            return found
    return None


def _action_error_from_text(body_text: str) -> str | None:
    compact = xhs_auth._compress_text(body_text)
    for marker in _ACTION_ERROR_MARKERS:
        if marker in compact:
            return marker
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


def _click_action(page: Page, *, button_text: str, timeout_seconds: float = _ACTION_TIMEOUT_SECONDS) -> str | None:
    captured_url: dict[str, str | None] = {"value": None}

    def _handle_response(response: Any) -> None:
        if captured_url["value"]:
            return
        extracted = _extract_note_url_from_response(response)
        if extracted:
            captured_url["value"] = extracted

    _add_page_listener(page, "response", _handle_response)
    try:
        button = page.locator("button", has_text=button_text).last
        button.click()

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            if captured_url["value"]:
                return captured_url["value"]

            current_url = _normalize_note_url(page.url)
            if current_url:
                return current_url

            body_text = _page_text(page)
            action_error = _action_error_from_text(body_text)
            if action_error:
                raise RuntimeError(f"{button_text} blocked by Xiaohongshu: {action_error}")

            compact = xhs_auth._compress_text(body_text)
            if button_text == DRAFT_BUTTON_TEXT:
                if page.url != xhs_auth.PUBLISH_URL and "publish/publish" not in page.url:
                    return page.url
                if "草稿已保存" in compact or "保存成功" in compact:
                    return page.url
            else:
                if "发布成功" in compact:
                    return captured_url["value"]
                if page.url != xhs_auth.PUBLISH_URL and "publish/publish" not in page.url:
                    return page.url

            time.sleep(_MEDIUM_SLEEP)

        raise RuntimeError(f"{button_text} may not have completed; still on {page.url}")
    finally:
        _remove_page_listener(page, "response", _handle_response)


def _pause_before_exit() -> None:
    print("Browser is being kept open for inspection. Press Enter in this terminal to close it.")
    try:
        input()
    except EOFError:
        print(f"stdin unavailable; keeping browser open for {_KEEP_OPEN_FALLBACK_SECONDS}s instead.")
        try:
            time.sleep(_KEEP_OPEN_FALLBACK_SECONDS)
        except KeyboardInterrupt:
            pass


def cmd_login(args: argparse.Namespace) -> None:
    xhs_auth.cmd_login(args)


def cmd_check(args: argparse.Namespace) -> None:
    xhs_auth.cmd_check(args)


def cmd_post(args: argparse.Namespace) -> None:
    title = args.title.strip()
    content = args.content
    images = args.image or []
    post_id = getattr(args, "post_id", None)
    headless = not bool(getattr(args, "show_browser", False))
    keep_open = bool(getattr(args, "keep_open", False))
    save_draft = bool(getattr(args, "save_draft", False))
    submit = bool(getattr(args, "submit", False))

    if not title:
        print("ERROR: title is required")
        sys.exit(1)

    resolved_images, temp_images = _prepare_placeholder_images(content, images, post_id=post_id)
    if not resolved_images:
        print("ERROR: Xiaohongshu graphic posts require at least one image. Pass --image or --post-id with enabled image slots.")
        sys.exit(1)

    final_body = _body_text_for_xiaohongshu(content)
    tags = _extract_tags_from_content(content)
    estimated_body_length = _estimated_body_length_for_xiaohongshu(content)
    title_error = (
        f"Xiaohongshu title too long: {len(title)} characters "
        f"(current creator title limit appears to be {TITLE_LIMIT})"
    )
    body_error = (
        f"Xiaohongshu body too long: {estimated_body_length} characters "
        f"(current creator graph-note limit appears to be {BODY_LIMIT})"
    )
    url_error = "Xiaohongshu body contains external URL text; normal graph-note delivery should not rely on links"

    for message, is_error in (
        (title_error, len(title) > TITLE_LIMIT),
        (body_error, estimated_body_length > BODY_LIMIT),
        (url_error, bool(_URL_RE.search(final_body))),
    ):
        if not is_error:
            continue
        if submit or save_draft:
            print(f"ERROR: {message}")
            sys.exit(1)
        print(f"WARNING: {message}")

    print(f"Title: {title}")
    print(f"Content: {content[:100]}{'...' if len(content) > 100 else ''}")
    print(f"Images: {len(resolved_images)}")
    print(f"Final body length: {estimated_body_length} / {BODY_LIMIT}")
    if tags:
        print(f"Tags: {' '.join(tags)}")
    print(f"Mode: {'submit' if submit else 'save-draft' if save_draft else 'dry-run'}")
    print(f"Browser: {'visible' if not headless else 'headless'}")
    if keep_open:
        print("Keep open: yes")
    print()

    page = None
    context = None
    try:
        with _playwright() as pw:
            context = _create_context(pw, headless=headless)
            page = context.new_page()

            page.goto(xhs_auth.PUBLISH_URL, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(5000)
            if not _is_logged_in(page):
                print("ERROR: Not logged in. Run: python -m social_media.xiaohongshu.post login --chrome-profile Default")
                print(f"Reason: {_login_failure_reason(page)}")
                sys.exit(1)
            print("Logged in.")

            _switch_to_graphic_mode(page)
            print("Graphic mode selected.")

            _upload_images(page, resolved_images)
            print("Images uploaded.")

            _fill_title(page, title)
            _fill_body(page, content)
            _insert_topics(page, tags)
            print("Title and body filled.")

            page.screenshot(path="/tmp/xiaohongshu_post_filled.png", full_page=True)
            print("Screenshot: /tmp/xiaohongshu_post_filled.png")

            if save_draft:
                result = _click_action(page, button_text=DRAFT_BUTTON_TEXT)
                page.screenshot(path="/tmp/xiaohongshu_post_draft.png", full_page=True)
                print(f"Draft save result: {result or page.url}")
                print("Screenshot: /tmp/xiaohongshu_post_draft.png")
            elif submit:
                result = _click_action(page, button_text=PUBLISH_BUTTON_TEXT)
                page.screenshot(path="/tmp/xiaohongshu_post_submitted.png", full_page=True)
                print(f"Post submitted! URL: {result or 'not detected'}")
                print("Screenshot: /tmp/xiaohongshu_post_submitted.png")
            else:
                print("[DRY RUN] Content filled but not submitted.")
                print("Pass --save-draft to save a draft, or --submit to publish for real.")

            if keep_open and not headless:
                _pause_before_exit()

            context.close()
    except Exception as exc:
        if page is not None:
            try:
                page.screenshot(path="/tmp/xiaohongshu_post_error.png", full_page=True)
                print("Screenshot: /tmp/xiaohongshu_post_error.png")
            except Exception:
                pass
        print(f"ERROR: {exc}")
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
        prog="python -m social_media.xiaohongshu.post",
        description="Auto-post to Xiaohongshu creator graph notes.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Import Xiaohongshu cookies from Chrome.")
    p_login.add_argument(
        "--chrome-profile",
        default="Default",
        help="Chrome profile name (default: Default)",
    )

    sub.add_parser("check", help="Check if the saved Xiaohongshu login session is valid.")

    p_post = sub.add_parser("post", help="Create a Xiaohongshu graphic note.")
    p_post.add_argument("--title", required=True, help="Post title")
    p_post.add_argument("--content", required=True, help="Post content (newlines become paragraphs)")
    p_post.add_argument("--image", action="append", help="Image file to upload (repeatable)")
    p_post.add_argument("--post-id", type=int, dest="post_id", help="SocialPost ID — resolve slot-based images from the image pool")
    p_post.add_argument("--show-browser", action="store_true", help="Show the browser window instead of running headless")
    p_post.add_argument("--keep-open", action="store_true", help="Keep the visible browser open after filling the composer")
    action_group = p_post.add_mutually_exclusive_group()
    action_group.add_argument("--save-draft", action="store_true", help="Save to Xiaohongshu drafts instead of publishing")
    action_group.add_argument("--submit", action="store_true", help="Actually publish the note (default: dry run)")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
