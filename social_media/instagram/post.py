"""Auto-post Instagram photo/carousel posts using Playwright credential login.

Usage:
    python -m social_media.instagram.post check
    python -m social_media.instagram.post login --show-browser
    python -m social_media.instagram.post post --content "..." --image hero.png
    python -m social_media.instagram.post post --content "..." --image hero.png --submit
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
from urllib.parse import urlparse

try:
    from playwright.sync_api import BrowserContext, Page
except ModuleNotFoundError:
    BrowserContext = Page = Any

from ..funba_capture import capture_funba_url as _capture_funba_url
from . import auth as instagram_auth


MODULE_DIR = Path(__file__).resolve().parent
POST_URL_RE = re.compile(
    r"https://(?:www\.)?instagram\.com/(?:[A-Za-z0-9_.]+/)?(?:p|reel)/[A-Za-z0-9_-]+/?(?:[?#][^\s\"'<]*)?",
    re.IGNORECASE,
)
INSTAGRAM_MAX_IMAGES = 10
INSTAGRAM_CAPTION_LIMIT = 2200
_IMAGE_SLOT_PRIORITY = ("poster_ig", "instagram", "poster", "img1", "img2", "img3")
_IMAGE_PLACEHOLDER_RE = re.compile(r"^\s*\[\[IMAGE:(.+?)\]\]\s*$")
_TAGS_PLACEHOLDER_RE = re.compile(r"^\s*\[\[TAGS:(.+?)\]\]\s*$")
_SHORT_SLEEP = 0.15
_MEDIUM_SLEEP = 0.5
_LONG_SLEEP = 1.0
_UPLOAD_SETTLE_SECONDS = 4.0
_POST_RESULT_TIMEOUT_SECONDS = 45.0
_KEEP_OPEN_FALLBACK_SECONDS = 600


def _slugify_artifact_part(value: str | None) -> str:
    cleaned = re.sub(r"[^\w-]+", "-", str(value or "").strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "artifact"


def _resolve_artifact_dir(requested_path: str | None, *, post_id: int | None) -> Path:
    if requested_path:
        path = Path(requested_path).expanduser()
    else:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        parts = [stamp]
        if post_id is not None:
            parts.append(f"post{post_id}")
        path = Path.cwd() / "logs" / "instagram_post" / "-".join(parts)
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


def _safe_page_screenshot(page: Page | None, path: Path) -> None:
    if page is None:
        return
    try:
        page.screenshot(path=str(path), full_page=True)
    except Exception as exc:
        _write_text_artifact(path.with_suffix(".error.txt"), str(exc))


def _persist_instagram_failure_artifacts(
    artifact_dir: Path,
    *,
    page: Page | None,
    stage: str,
    content: str,
    post_id: int | None,
    exception: BaseException,
    browser_events: dict[str, list[dict[str, str]]] | None = None,
) -> None:
    state = {
        "stage": stage,
        "post_id": post_id,
        "page_url": _safe_page_url(page),
        "content_preview": content[:500],
        "exception_type": type(exception).__name__,
        "exception_message": str(exception),
        "captured_at": datetime.utcnow().isoformat() + "Z",
    }
    _write_json_artifact(artifact_dir / "failure_state.json", state)
    _write_text_artifact(artifact_dir / "failure_traceback.txt", traceback.format_exc())
    _write_text_artifact(artifact_dir / "failure_body.txt", _safe_page_body_text(page))
    _write_text_artifact(artifact_dir / "failure_page.html", _safe_page_html(page))
    if browser_events is not None:
        _write_json_artifact(artifact_dir / "browser_events.json", browser_events)
    _safe_page_screenshot(page, artifact_dir / "failure.png")


def _parse_image_placeholder(line: str) -> dict[str, str] | None:
    match = _IMAGE_PLACEHOLDER_RE.match(line or "")
    if not match:
        return None
    parsed: dict[str, str] = {}
    for part in match.group(1).split(";"):
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
    seen: set[str] = set()
    ordered: list[str] = []
    for tag in re.findall(r"#([^\s#]+)", match.group(1)):
        candidate = f"#{tag.strip()}"
        if not candidate or candidate == "#" or candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


def _render_plain_text_line(line: str) -> str:
    rendered = re.sub(r"\*\*(.+?)\*\*", r"\1", line)
    rendered = re.sub(
        r"\[([^\]]+)\]\((https?://[^)]+)\)",
        lambda match: f"{match.group(1)} {match.group(2)}",
        rendered,
    )
    return html.unescape(rendered)


def _caption_lines_for_instagram(content: str) -> list[str]:
    lines: list[str] = []
    for raw_line in str(content or "").splitlines():
        if _parse_image_placeholder(raw_line):
            continue
        if _parse_tags_placeholder(raw_line) is not None:
            continue
        lines.append(_render_plain_text_line(raw_line.rstrip()))
    return lines


def _extract_tags_from_content(content: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw_line in str(content or "").splitlines():
        tags = _parse_tags_placeholder(raw_line)
        if not tags:
            continue
        for tag in tags:
            if tag in seen:
                continue
            seen.add(tag)
            ordered.append(tag)
    return ordered


def _caption_text_for_instagram(content: str) -> str:
    body = "\n".join(_caption_lines_for_instagram(content)).strip()
    tags = _extract_tags_from_content(content)
    if not tags:
        return body
    tag_text = " ".join(tags)
    if not body:
        return tag_text
    return f"{body}\n\n{tag_text}"


def _load_post_image_rows(post_id: int) -> list[tuple[str, str]]:
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
        return [(str(row.slot or ""), str(row.file_path or "")) for row in rows if row.file_path]


def _post_image_pool(post_id: int | None) -> list[tuple[str, str]]:
    if post_id is None:
        return []
    return _load_post_image_rows(int(post_id))


def _paths_by_priority(rows: list[tuple[str, str]]) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()

    def append_path(path: str) -> None:
        candidate = str(path or "").strip()
        if candidate and candidate not in seen:
            seen.add(candidate)
            paths.append(candidate)

    by_slot: dict[str, list[str]] = {}
    for slot, path in rows:
        by_slot.setdefault(slot, []).append(path)

    for slot in _IMAGE_SLOT_PRIORITY:
        for path in by_slot.get(slot, []):
            append_path(path)
    for _slot, path in rows:
        append_path(path)
    return paths


def _prepare_placeholder_images(
    content: str,
    images: list[str],
    *,
    post_id: int | None = None,
) -> tuple[list[str], list[str]]:
    resolved_images = [str(item) for item in (images or []) if str(item or "").strip()]
    temp_paths: list[str] = []
    pool_rows = _post_image_pool(post_id)
    pool_by_slot = {slot: path for slot, path in pool_rows if slot and path}

    placeholder_specs = [
        spec
        for spec in (_parse_image_placeholder(line) for line in str(content or "").splitlines())
        if spec is not None
    ]

    if placeholder_specs and len(resolved_images) < len(placeholder_specs):
        for spec in placeholder_specs[len(resolved_images):]:
            slot = spec.get("slot")
            if slot and slot in pool_by_slot:
                resolved_images.append(pool_by_slot[slot])
                continue

            target = spec.get("target")
            if not target:
                continue
            tmp = tempfile.NamedTemporaryFile(prefix="funba_instagram_", suffix=".png", delete=False)
            tmp.close()
            _capture_funba_url(target, tmp.name)
            temp_paths.append(tmp.name)
            resolved_images.append(tmp.name)

    if not resolved_images and pool_rows:
        resolved_images.extend(_paths_by_priority(pool_rows))

    return resolved_images[:INSTAGRAM_MAX_IMAGES], temp_paths


def _resolve_image_paths(values: list[str] | None) -> list[Path]:
    if not values:
        return []
    seen: set[str] = set()
    resolved: list[Path] = []
    for raw in values:
        path = Path(str(raw or "").strip()).expanduser()
        if not str(path):
            continue
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(f"Image not found: {path}")
        if path.stat().st_size <= 0:
            raise ValueError(f"Image is empty: {path}")
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        resolved.append(path)
    if len(resolved) > INSTAGRAM_MAX_IMAGES:
        raise ValueError(f"Instagram accepts at most {INSTAGRAM_MAX_IMAGES} images, got {len(resolved)}")
    return resolved


def _normalize_post_url(value: str | None) -> str | None:
    candidate = str(value or "").strip().replace("\\/", "/")
    if not candidate:
        return None
    match = POST_URL_RE.search(candidate)
    if match:
        return re.sub(r"[?#].*$", "", match.group(0)).rstrip("/")
    relative = re.search(r"/(?:[A-Za-z0-9_.]+/)?(?:p|reel)/[A-Za-z0-9_-]+/?", candidate)
    if relative:
        return f"https://www.instagram.com{relative.group(0).rstrip('/')}"
    return None


def _extract_post_url_from_text(text: str | None) -> str | None:
    return _normalize_post_url(text)


def _post_url_belongs_to_username(value: str | None, username: str | None) -> bool:
    handle = str(username or "").strip().strip("@").lower()
    if not handle:
        return True
    normalized = _normalize_post_url(value)
    if not normalized:
        return False
    path = urlparse(normalized).path.strip("/").lower().split("/")
    return len(path) >= 3 and path[0] == handle and path[1] in {"p", "reel"}


def _extract_post_url_from_page_state(page: Page, *, username: str | None = None) -> str | None:
    extracted = _normalize_post_url(_safe_page_url(page))
    if extracted:
        return extracted

    evaluator = getattr(page, "evaluate", None)
    if not callable(evaluator):
        return None
    try:
        state = evaluator(
            """() => ({
                historyState: (() => { try { return JSON.stringify(history.state || null); } catch { return ""; } })(),
                title: document.title || "",
                anchors: Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]'))
                    .map((a) => a.href || a.getAttribute("href") || ""),
            })"""
        ) or {}
    except Exception:
        return None
    if not isinstance(state, dict):
        return None
    for candidate in [state.get("historyState"), state.get("title"), *(state.get("anchors") or [])]:
        text = str(candidate or "")
        extracted = _extract_post_url_from_text(text)
        if username and extracted and not _post_url_belongs_to_username(text, username):
            continue
        if extracted:
            return extracted
    return None


def _button_by_text_script(labels: list[str]) -> str:
    return """
(labels) => {
  const wanted = labels.map(label => String(label).trim().toLowerCase());
  const candidates = Array.from(document.querySelectorAll('button, div[role="button"], a[role="button"]'));
  for (const el of candidates) {
    const text = (el.textContent || '').trim().toLowerCase();
    if (!wanted.includes(text)) continue;
    el.scrollIntoView({ block: 'center', inline: 'center' });
    el.click();
    return text;
  }
  return null;
}
"""


def _click_button_by_text(page: Page, labels: list[str], *, timeout_seconds: float = 12.0) -> str:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while time.time() < deadline:
        for label in labels:
            try:
                button = page.get_by_role("button", name=re.compile(rf"^\s*{re.escape(label)}\s*$", re.IGNORECASE)).last
                if button.count() and button.is_visible(timeout=500):
                    button.click(timeout=2000)
                    return label
            except Exception as exc:
                last_error = exc
        try:
            clicked = page.evaluate(_button_by_text_script(labels), labels)
            if clicked:
                return str(clicked)
        except Exception as exc:
            last_error = exc
        time.sleep(_SHORT_SLEEP)
    raise RuntimeError(f"Instagram button not found: {labels} ({last_error})")


def _find_file_input(page: Page):
    selectors = [
        'input[type="file"][accept*="image"]',
        'input[type="file"][accept*="jpeg"]',
        'input[type="file"]',
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="attached", timeout=5000)
            return locator
        except Exception:
            continue
    return None


def _open_create_dialog(page: Page) -> None:
    page.goto(instagram_auth.CREATE_URL, wait_until="domcontentloaded", timeout=30000)
    time.sleep(_LONG_SLEEP)
    if _find_file_input(page):
        return

    page.goto(instagram_auth.HOME_URL, wait_until="domcontentloaded", timeout=30000)
    time.sleep(_LONG_SLEEP)
    click_targets = [
        'a[href="/create/select/"]',
        'svg[aria-label="New post"]',
        'svg[aria-label="Create"]',
    ]
    for selector in click_targets:
        try:
            target = page.locator(selector).first
            if target.count():
                target.click(timeout=3000)
                time.sleep(_LONG_SLEEP)
                if _find_file_input(page):
                    return
        except Exception:
            continue
    _click_button_by_text(page, ["Create", "New post"], timeout_seconds=5.0)
    time.sleep(_LONG_SLEEP)


def _upload_images(page: Page, images: list[Path]) -> None:
    if not images:
        raise RuntimeError("Instagram posts require at least one image")
    _open_create_dialog(page)
    upload_input = _find_file_input(page)
    if upload_input is None:
        raise RuntimeError("Instagram image upload input not found")
    upload_input.set_input_files([str(path) for path in images], timeout=20000)
    time.sleep(_UPLOAD_SETTLE_SECONDS)


def _caption_editor_selector(page: Page) -> str | None:
    selectors = [
        'textarea[aria-label*="caption" i]',
        'textarea[placeholder*="caption" i]',
        'div[contenteditable="true"][aria-label*="caption" i]',
        'div[contenteditable="true"][role="textbox"]',
        'textarea',
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.count() and locator.is_visible(timeout=500):
                return selector
        except Exception:
            continue
    return None


def _wait_for_caption_editor(page: Page, *, timeout_seconds: float = 12.0) -> str | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        selector = _caption_editor_selector(page)
        if selector:
            return selector
        time.sleep(_SHORT_SLEEP)
    return None


def _advance_to_caption_step(page: Page) -> str:
    selector = _wait_for_caption_editor(page, timeout_seconds=2.0)
    if selector:
        return selector
    for _ in range(3):
        _click_button_by_text(page, ["Next"], timeout_seconds=12.0)
        time.sleep(_LONG_SLEEP)
        selector = _wait_for_caption_editor(page, timeout_seconds=6.0)
        if selector:
            return selector
    raise RuntimeError("Instagram caption editor not found after upload")


def _fill_caption(page: Page, caption: str) -> str:
    selector = _advance_to_caption_step(page)
    locator = page.locator(selector).first
    locator.click(timeout=3000)
    if selector.startswith("textarea"):
        locator.fill(caption, timeout=5000)
        return selector
    locator.evaluate(
        """(el, value) => {
        el.focus();
        el.innerText = value;
        el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: value }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
    }""",
        caption,
    )
    return selector


def _instagram_action_error(body_text: str) -> str | None:
    compact = re.sub(r"\s+", " ", str(body_text or "")).strip()
    markers = (
        "Something went wrong",
        "Couldn't create post",
        "Try again later",
        "Your post could not be shared",
        "We restrict certain activity",
    )
    for marker in markers:
        if marker in compact:
            return marker
    return None


def _latest_profile_post_url(page: Page, username: str | None) -> str | None:
    handle = str(username or "").strip().strip("@")
    if not handle:
        return None
    profile_page = page.context.new_page()
    try:
        profile_page.goto(f"https://www.instagram.com/{handle}/", wait_until="domcontentloaded", timeout=30000)
        profile_page.wait_for_timeout(5000)
        anchors = profile_page.evaluate(
            """() => Array.from(document.querySelectorAll('a[href*="/p/"], a[href*="/reel/"]'))
                .map((a) => a.href || a.getAttribute("href") || "")"""
        ) or []
        for candidate in anchors:
            if not _post_url_belongs_to_username(str(candidate), handle):
                continue
            extracted = _normalize_post_url(str(candidate))
            if extracted:
                return extracted
    except Exception:
        return None
    finally:
        try:
            profile_page.close()
        except Exception:
            pass
    return None


def _wait_for_post_result(
    page: Page,
    *,
    username: str | None = None,
    timeout_seconds: float = _POST_RESULT_TIMEOUT_SECONDS,
    previous_latest_url: str | None = None,
) -> str | None:
    deadline = time.time() + timeout_seconds
    saw_success = False
    while time.time() < deadline:
        found = _extract_post_url_from_page_state(page, username=username)
        if found:
            return found

        body_text = _safe_page_body_text(page)
        action_error = _instagram_action_error(body_text)
        if action_error:
            raise RuntimeError(f"Instagram share blocked: {action_error}")

        compact = re.sub(r"\s+", " ", body_text).strip()
        if "Your post has been shared" in compact or "Post shared" in compact:
            saw_success = True
            profile_url = _latest_profile_post_url(page, username)
            if profile_url:
                return profile_url

        time.sleep(_MEDIUM_SLEEP)

    profile_url = _latest_profile_post_url(page, username)
    if profile_url and profile_url != previous_latest_url:
        return profile_url
    if saw_success:
        return profile_url
    raise RuntimeError("Instagram share may not have completed before timeout")


def _click_share(page: Page, *, username: str | None = None) -> str | None:
    previous_latest_url = _latest_profile_post_url(page, username)
    _click_button_by_text(page, ["Share"], timeout_seconds=15.0)
    return _wait_for_post_result(page, username=username, previous_latest_url=previous_latest_url)


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
    instagram_auth.cmd_login(args)


def cmd_check(args: argparse.Namespace) -> None:
    instagram_auth.cmd_check(args)


def cmd_post(args: argparse.Namespace) -> None:
    content = str(args.content or "")
    post_id = getattr(args, "post_id", None)
    submit = bool(getattr(args, "submit", False))
    show_browser = bool(getattr(args, "show_browser", False))
    keep_open = bool(getattr(args, "keep_open", False))
    artifact_dir = _resolve_artifact_dir(getattr(args, "artifact_dir", None), post_id=post_id)
    stage = "starting"
    context: BrowserContext | None = None
    page: Page | None = None
    temp_images: list[str] = []
    browser_events: dict[str, list[dict[str, str]]] = {
        "console": [],
        "pageerror": [],
        "requestfailed": [],
    }

    resolved_image_values, temp_images = _prepare_placeholder_images(
        content,
        args.image or [],
        post_id=post_id,
    )
    try:
        image_paths = _resolve_image_paths(resolved_image_values)
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    caption = _caption_text_for_instagram(content)
    if not image_paths:
        print("ERROR: Instagram posts require at least one image. Pass --image or --post-id with enabled image slots.")
        sys.exit(1)
    if len(caption) > INSTAGRAM_CAPTION_LIMIT:
        print(f"ERROR: Instagram caption too long: {len(caption)}/{INSTAGRAM_CAPTION_LIMIT}.")
        sys.exit(1)

    print(f"Caption length: {len(caption)} / {INSTAGRAM_CAPTION_LIMIT}")
    print(f"Images: {len(image_paths)}")
    print(f"Submit: {'YES' if submit else 'NO (dry run)'}")
    print(f"Browser: {'visible' if show_browser else 'headless'}")
    print(f"Artifacts: {artifact_dir}")
    print()

    _write_json_artifact(
        artifact_dir / "request.json",
        {
            "post_id": post_id,
            "submit": submit,
            "caption_length": len(caption),
            "image_paths": [str(path) for path in image_paths],
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )

    try:
        with instagram_auth._playwright() as pw:
            context, page, status = instagram_auth.ensure_login_context(
                pw,
                headless=not show_browser,
                login_timeout_seconds=float(getattr(args, "login_timeout_seconds", 90.0)),
            )
            if not status.get("ok"):
                raise RuntimeError(status.get("reason") or "Instagram login failed")
            print(f"Logged in as: {status.get('username')}")

            page.on(
                "console",
                lambda msg: browser_events["console"].append(
                    {"type": str(getattr(msg, "type", "unknown")), "text": str(getattr(msg, "text", ""))}
                ),
            )
            page.on("pageerror", lambda exc: browser_events["pageerror"].append({"text": str(exc)}))
            page.on(
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
                stage = "upload_images"
                _upload_images(page, image_paths)
                _safe_page_screenshot(page, artifact_dir / "uploaded.png")
                print("Images uploaded.")

                stage = "fill_caption"
                caption_selector = _fill_caption(page, caption)
                _safe_page_screenshot(page, artifact_dir / "filled.png")
                print("Caption filled.")

                if not submit:
                    _write_json_artifact(
                        artifact_dir / "result.json",
                        {
                            "status": "dry_run",
                            "page_url": _safe_page_url(page),
                            "caption_selector": caption_selector,
                            "captured_at": datetime.utcnow().isoformat() + "Z",
                        },
                    )
                    print("[DRY RUN] Instagram draft filled but not submitted.")
                    print("Pass --submit to actually post.")
                    if keep_open and show_browser:
                        _pause_before_exit()
                    return

                stage = "submit"
                final_url = _click_share(page, username=str(status.get("username") or ""))
                time.sleep(_LONG_SLEEP)
                _safe_page_screenshot(page, artifact_dir / "submitted.png")
                _write_json_artifact(
                    artifact_dir / "result.json",
                    {
                        "status": "published",
                        "final_url": final_url,
                        "page_url": _safe_page_url(page),
                        "captured_at": datetime.utcnow().isoformat() + "Z",
                    },
                )
                print(f"Post submitted! URL: {final_url or 'not detected'}")
            except Exception as exc:
                _persist_instagram_failure_artifacts(
                    artifact_dir,
                    page=page,
                    stage=stage,
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
        prog="python -m social_media.instagram.post",
        description="Auto-post Instagram photo/carousel posts.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Log in and cache Instagram browser storage state.")
    p_login.add_argument("--show-browser", action="store_true", help="Show the browser window for checkpoint/2FA handling.")
    p_login.add_argument("--timeout-seconds", type=float, default=90.0)

    p_check = sub.add_parser("check", help="Check Instagram login state; logs in if needed by default.")
    p_check.add_argument("--no-login", action="store_true", help="Only check cached state; do not submit credentials.")
    p_check.add_argument("--show-browser", action="store_true", help="Show the browser window if login is needed.")
    p_check.add_argument("--timeout-seconds", type=float, default=90.0)

    p_post = sub.add_parser("post", help="Create an Instagram photo/carousel post.")
    p_post.add_argument("--content", required=True, help="Post caption/content")
    p_post.add_argument("--image", action="append", help="Image file to upload (repeatable, max 10)")
    p_post.add_argument("--post-id", type=int, dest="post_id", help="SocialPost ID — resolve enabled image slots")
    p_post.add_argument("--artifact-dir", help="Directory for debug screenshots/logs/artifacts")
    p_post.add_argument("--show-browser", action="store_true", help="Show the browser window instead of running headless")
    p_post.add_argument("--keep-open", action="store_true", help="Keep the visible browser open after filling the composer")
    p_post.add_argument("--login-timeout-seconds", type=float, default=90.0)
    p_post.add_argument("--submit", action="store_true", help="Actually publish the post (default: dry run)")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
