"""Auto-post Reddit text posts using Playwright with saved cookie injection.

Usage:
    python -m social_media.reddit.post check
    python -m social_media.reddit.post post --title "..." --content "..." --subreddit nba
    python -m social_media.reddit.post post --title "..." --content "..." --subreddit nba --submit
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from .auth import REAL_BROWSER_UA, REDDIT_HOME, check_login_state, load_cookies

try:
    from playwright.sync_api import sync_playwright, BrowserContext, Page
except ModuleNotFoundError:
    sync_playwright = None
    BrowserContext = Page = Any


MODULE_DIR = Path(__file__).resolve().parent
POST_URL_RE = re.compile(
    r"https://(?:www\.)?reddit\.com/r/[A-Za-z0-9_]+/comments/[A-Za-z0-9]+(?:/[^\s\"'<]*)?/?(?:\?[^\s\"'<]*)?(?:#[^\s\"'<]*)?$",
    re.IGNORECASE,
)
_SHORT_SLEEP = 0.1
_MEDIUM_SLEEP = 0.4
_LONG_SLEEP = 1.0


def _cookie_for_playwright(cookie: dict[str, Any]) -> dict[str, Any]:
    entry = {
        "name": str(cookie["name"]),
        "value": str(cookie.get("value", "")),
    }
    domain = str(cookie.get("domain", "") or "").strip()
    url = str(cookie.get("url", "") or "").strip()
    if domain:
        entry["domain"] = domain
        entry["path"] = str(cookie.get("path", "/") or "/")
    elif url:
        entry["url"] = url
    else:
        raise ValueError(f"Cookie {entry['name']!r} is missing both domain and url")
    if cookie.get("secure"):
        entry["secure"] = True
    if cookie.get("httpOnly"):
        entry["httpOnly"] = True
    expires = cookie.get("expires")
    if expires is not None:
        entry["expires"] = int(expires)
    same_site = str(cookie.get("sameSite", "") or "").strip()
    if same_site in {"Strict", "Lax", "None"}:
        entry["sameSite"] = same_site
    return entry


def _slugify_artifact_part(value: str | None) -> str:
    cleaned = re.sub(r"[^\w-]+", "-", str(value or "").strip())
    cleaned = re.sub(r"-{2,}", "-", cleaned).strip("-")
    return cleaned or "artifact"


def _normalize_subreddit(subreddit: str) -> str:
    normalized = re.sub(r"^/+", "", str(subreddit or "").strip())
    normalized = re.sub(r"^(?i:r/)", "", normalized)
    normalized = normalized.strip("/")
    if not normalized:
        raise ValueError("Subreddit is required.")
    if not re.fullmatch(r"[A-Za-z0-9_]+", normalized):
        raise ValueError(f"Invalid subreddit: {subreddit!r}")
    return normalized


def _build_submit_url(*, subreddit: str, title: str = "", content: str = "") -> str:
    params = urlencode({
        "type": "self",
        "sr": _normalize_subreddit(subreddit),
        "title": title,
        "text": content,
    })
    return f"{REDDIT_HOME}/submit?{params}"


def _resolve_artifact_dir(requested_path: str | None, *, post_id: int | None, subreddit: str) -> Path:
    if requested_path:
        path = Path(requested_path).expanduser()
    else:
        stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        parts = [stamp]
        if post_id is not None:
            parts.append(f"post{post_id}")
        parts.append(_slugify_artifact_part(subreddit))
        path = Path.cwd() / "logs" / "reddit_post" / "-".join(parts)
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


def _persist_reddit_failure_artifacts(
    artifact_dir: Path,
    *,
    page: Page | None,
    stage: str,
    subreddit: str,
    title: str,
    content: str,
    post_id: int | None,
    exception: BaseException,
    browser_events: dict[str, list[dict[str, str]]] | None = None,
) -> None:
    state = {
        "stage": stage,
        "post_id": post_id,
        "subreddit": subreddit,
        "page_url": _safe_page_url(page),
        "title": title,
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


def _playwright():
    if sync_playwright is None:
        raise RuntimeError("Playwright is required for Reddit posting commands. Install it with `pip install playwright`.")
    return sync_playwright()


def _create_context(pw, *, headless: bool) -> BrowserContext:
    launch_args: list[str] = []
    if headless:
        launch_args.append("--headless=new")
    browser = pw.chromium.launch(headless=headless, args=launch_args)
    context = browser.new_context(
        viewport={"width": 1440, "height": 1400},
        locale="en-US",
        user_agent=REAL_BROWSER_UA,
    )
    cookies = load_cookies()
    if cookies:
        context.add_cookies([_cookie_for_playwright(cookie) for cookie in cookies])
    return context


def _set_control_value(page: Page, selectors: list[str], value: str) -> str | None:
    script = """
({selectors, value}) => {
  const dispatch = (el) => {
    el.dispatchEvent(new Event("input", { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
  };
  for (const selector of selectors) {
    const el = document.querySelector(selector);
    if (!el) continue;
    el.focus();
    if (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement) {
      el.value = value;
      dispatch(el);
      return selector;
    }
    if (typeof el.value !== "undefined") {
      try {
        el.value = value;
      } catch (_) {}
      if (typeof el.setAttribute === "function") {
        el.setAttribute("value", value);
      }
      dispatch(el);
      return selector;
    }
    if (el.isContentEditable) {
      const lines = String(value).split("\\n");
      el.innerHTML = "";
      if (!lines.length) lines.push("");
      lines.forEach((line, idx) => {
        const block = document.createElement("p");
        block.textContent = line;
        if (!line) block.appendChild(document.createElement("br"));
        el.appendChild(block);
        if (idx !== lines.length - 1 && !line) {
          el.appendChild(document.createElement("br"));
        }
      });
      dispatch(el);
      return selector;
    }
  }
  return null;
}
"""
    try:
        return page.evaluate(script, {"selectors": selectors, "value": value})
    except Exception:
        return None


def _fill_locator_value(page: Page, selectors: list[str], value: str) -> str | None:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=2000)
            locator.click(timeout=2000)
            locator.fill(value, timeout=3000)
            return selector
        except Exception:
            continue
    return None


def _first_selector_present(page: Page, selectors: list[str]) -> str | None:
    for selector in selectors:
        try:
            if page.query_selector(selector):
                return selector
        except Exception:
            continue
    return None


def _selected_subreddit(page: Page) -> str | None:
    evaluator = getattr(page, "evaluate", None)
    if not callable(evaluator):
        return None
    try:
        state = evaluator(
            """() => ({
                subredditName: document.querySelector('input[name="subredditName"]')?.value || "",
                prefixedName: document.querySelector('input[name="prefixedName"]')?.value || "",
            })"""
        ) or {}
    except Exception:
        return None
    if not isinstance(state, dict):
        return None
    for raw in (state.get("prefixedName"), state.get("subredditName")):
        candidate = str(raw or "").strip()
        if not candidate:
            continue
        try:
            return _normalize_subreddit(candidate)
        except ValueError:
            continue
    return None


def _select_subreddit(page: Page, subreddit: str) -> dict[str, str]:
    target = _normalize_subreddit(subreddit)
    current = _selected_subreddit(page)
    if current == target:
        return {"subreddit_selector": "prefilled", "selected_subreddit": current}

    trigger = None
    try:
        candidate = page.get_by_role("button", name=re.compile(r"select community", re.IGNORECASE)).first
        if candidate.is_visible():
            candidate.click(timeout=5000)
            trigger = "role=button[name=/select community/i]"
    except Exception:
        trigger = None

    search_selector = None
    search_locators = [
        ("input[placeholder*=\"Search communities\" i]", lambda: page.locator('input[placeholder*="Search communities" i]').first),
        ("input[placeholder*=\"community\" i]", lambda: page.locator('input[placeholder*="community" i]').first),
        ("role=textbox[name=/search communities/i]", lambda: page.get_by_role("textbox", name=re.compile(r"search communities", re.IGNORECASE)).first),
    ]
    search_input = None
    for selector, factory in search_locators:
        try:
            locator = factory()
            locator.wait_for(state="visible", timeout=5000)
            search_input = locator
            search_selector = selector
            break
        except Exception:
            continue
    if search_input is None:
        raise RuntimeError("Reddit community search input not found")

    search_input.click(timeout=5000)
    try:
        search_input.fill(target, timeout=5000)
    except Exception:
        page.keyboard.press("Meta+A")
        page.keyboard.press("Control+A")
        page.keyboard.type(target, delay=20)
    time.sleep(_LONG_SLEEP)

    candidate_patterns = [
        re.compile(rf"^\s*r/{re.escape(target)}\s*$", re.IGNORECASE),
        re.compile(rf"(?:^|\s)r/{re.escape(target)}(?:\s|$)", re.IGNORECASE),
        re.compile(rf"^\s*{re.escape(target)}\s*$", re.IGNORECASE),
    ]
    clicked = False
    candidate_factories = [
        lambda pattern: page.get_by_role("button", name=pattern).first,
        lambda pattern: page.get_by_role("link", name=pattern).first,
        lambda pattern: page.get_by_text(pattern).first,
    ]
    for pattern in candidate_patterns:
        for factory in candidate_factories:
            try:
                locator = factory(pattern)
                if locator.is_visible():
                    locator.click(timeout=3000)
                    clicked = True
                    break
            except Exception:
                continue
        if clicked:
            break
    if not clicked:
        try:
            search_input.press("ArrowDown", timeout=2000)
            search_input.press("Enter", timeout=2000)
        except Exception:
            pass

    deadline = time.time() + 10.0
    while time.time() < deadline:
        current = _selected_subreddit(page)
        if current == target:
            return {
                "subreddit_selector": search_selector or trigger or "",
                "selected_subreddit": current,
            }
        time.sleep(_SHORT_SLEEP)
    raise RuntimeError(f"Reddit community selection failed for r/{target}")


def _fill_submission_form(page: Page, *, subreddit: str, title: str, content: str) -> dict[str, str]:
    subreddit_info = _select_subreddit(page, subreddit)
    title_selector = _set_control_value(
        page,
        [
            'faceplate-textarea-input[name="title"]',
            'textarea[name="title"]',
            'input[name="title"]',
            'textarea[placeholder*="title" i]',
            'input[placeholder*="title" i]',
        ],
        title,
    )
    body_selectors = [
        '[slot="rte"][aria-label*="Post body" i]',
        '[slot="rte"][aria-label*="Optional Body" i]',
        '[aria-label*="Post body" i] [contenteditable="true"][role="textbox"]',
        '[aria-label*="Optional Body" i] [contenteditable="true"][role="textbox"]',
        '[slot="rte"][aria-label*="body" i] [contenteditable="true"][role="textbox"]',
        '[slot="editor"][contenteditable="true"][role="textbox"]',
        '[slot="rte"][contenteditable="true"]',
        '[contenteditable="true"][aria-label*="body" i]',
        'div[aria-label*="body" i][contenteditable="true"]',
        'textarea[name="text"]',
        'textarea[data-testid="post-content-input"]',
        '[contenteditable="true"][role="textbox"]',
        'div[contenteditable="true"]',
    ]
    body_selector = _fill_locator_value(page, body_selectors, content) or _set_control_value(page, body_selectors, content)
    if not title_selector:
        raise RuntimeError("Reddit title input not found")
    if not body_selector:
        raise RuntimeError("Reddit content editor not found")
    return {
        "subreddit_selector": subreddit_info.get("subreddit_selector", ""),
        "selected_subreddit": subreddit_info.get("selected_subreddit", ""),
        "title_selector": title_selector,
        "body_selector": body_selector,
    }


def _normalize_post_url(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    match = POST_URL_RE.search(candidate)
    if match:
        return match.group(0).rstrip("/")
    relative = re.search(r"/r/[A-Za-z0-9_]+/comments/[A-Za-z0-9]+(?:/[^\s\"'<]*)?/?", candidate)
    if relative:
        return f"{REDDIT_HOME}{relative.group(0).rstrip('/')}"
    return None


def _extract_post_url_from_text(text: str | None) -> str | None:
    return _normalize_post_url(text)


def _extract_post_url_from_page_state(page: Page) -> str | None:
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
            })"""
        ) or {}
    except Exception:
        return None
    if not isinstance(state, dict):
        return None
    for candidate in (state.get("historyState"), state.get("title")):
        extracted = _extract_post_url_from_text(str(candidate or ""))
        if extracted:
            return extracted
    return None


def _wait_for_final_post_url(page: Page, timeout_seconds: float = 20.0) -> str | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        extracted = _extract_post_url_from_page_state(page)
        if extracted:
            return extracted
        time.sleep(_SHORT_SLEEP)
    return None


def _click_submit(page: Page) -> str:
    clicked = False
    button_selectors = [
        'button[type="submit"]',
        'shreddit-submit-post button',
        'button[data-testid="post-submit-button"]',
        'faceplate-tracker[noun="submit_post"] button',
    ]
    for selector in button_selectors:
        try:
            button = page.query_selector(selector)
        except Exception:
            button = None
        if not button:
            continue
        try:
            button.click()
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        try:
            page.get_by_role("button", name=re.compile(r"post", re.IGNORECASE)).first.click()
            clicked = True
        except Exception:
            pass
    if not clicked:
        raise RuntimeError("Reddit submit button not found")

    confirm_deadline = time.time() + 5.0
    while time.time() < confirm_deadline:
        try:
            confirm_button = page.get_by_role("button", name=re.compile(r"submit without editing", re.IGNORECASE)).first
            if confirm_button.is_visible():
                confirm_button.click(timeout=2000, force=True)
                time.sleep(_LONG_SLEEP)
                break
        except Exception:
            pass
        time.sleep(_SHORT_SLEEP)
    body_text = _safe_page_body_text(page)
    if re.search(r"please select a community before posting", body_text, flags=re.IGNORECASE):
        raise RuntimeError("Reddit community was not selected before submit")
    final_url = _wait_for_final_post_url(page)
    if final_url:
        return final_url
    if re.search(r"sorry,\s*this post has been removed by the moderators of r/", body_text, flags=re.IGNORECASE):
        raise RuntimeError("Reddit post was removed by moderators before final URL was detected")
    raise RuntimeError("Submit completed but Reddit post URL was not detected")


def cmd_check(args: argparse.Namespace) -> None:
    del args
    status = check_login_state()
    if status["ok"]:
        print("Logged in.")
        print(f"Username: {status['username']}")
        print(f"Submit URL: {status['submit_url']}")
        return
    print("ERROR: Not logged in.")
    print(status["reason"])
    sys.exit(1)


def cmd_post(args: argparse.Namespace) -> None:
    title = str(args.title or "").strip()
    content = str(args.content or "")
    subreddit = _normalize_subreddit(args.subreddit)
    submit = bool(args.submit)
    post_id = getattr(args, "post_id", None)
    artifact_dir = _resolve_artifact_dir(getattr(args, "artifact_dir", None), post_id=post_id, subreddit=subreddit)
    stage = "starting"
    context = None
    page = None
    browser_events: dict[str, list[dict[str, str]]] = {
        "console": [],
        "pageerror": [],
        "requestfailed": [],
    }

    print(f"Subreddit: r/{subreddit}")
    print(f"Title: {title}")
    print(f"Content: {content[:100]}{'...' if len(content) > 100 else ''}")
    print(f"Submit: {'YES' if submit else 'NO (dry run)'}")
    print(f"Artifacts: {artifact_dir}")
    print()

    _write_json_artifact(
        artifact_dir / "request.json",
        {
            "post_id": post_id,
            "subreddit": subreddit,
            "title": title,
            "submit": submit,
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )

    status = check_login_state()
    _write_json_artifact(artifact_dir / "login_state.json", status)
    if not status["ok"]:
        print("ERROR: Not logged in.")
        print(status["reason"])
        print(f"Artifacts saved to: {artifact_dir}")
        sys.exit(1)

    try:
        with _playwright() as pw:
            context = _create_context(pw, headless=False)
            page = context.new_page()

            page.on(
                "console",
                lambda msg: browser_events["console"].append(
                    {"type": str(getattr(msg, "type", "unknown")), "text": str(getattr(msg, "text", ""))}
                ),
            )
            page.on(
                "pageerror",
                lambda exc: browser_events["pageerror"].append({"text": str(exc)}),
            )
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
                stage = "open_submit"
                submit_url = _build_submit_url(subreddit=subreddit, title=title, content=content)
                page.goto(submit_url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(_LONG_SLEEP)
                _safe_page_screenshot(page, artifact_dir / "submit_loaded.png")
                print(f"Submit page loaded: {page.url}")

                if "/login" in str(page.url):
                    raise RuntimeError("Not logged in. Reddit redirected to login.")

                stage = "fill_form"
                selector_info = _fill_submission_form(page, subreddit=subreddit, title=title, content=content)
                _write_json_artifact(artifact_dir / "form_selectors.json", selector_info)
                time.sleep(_MEDIUM_SLEEP)
                _safe_page_screenshot(page, artifact_dir / "filled.png")
                print("Draft prepared.")

                if not submit:
                    _write_json_artifact(
                        artifact_dir / "result.json",
                        {
                            "status": "dry_run",
                            "page_url": _safe_page_url(page),
                            "submit_button_selector": _first_selector_present(
                                page,
                                [
                                    'button[type="submit"]',
                                    'shreddit-submit-post button',
                                    'button[data-testid="post-submit-button"]',
                                    'faceplate-tracker[noun="submit_post"] button',
                                ],
                            ),
                            "captured_at": datetime.utcnow().isoformat() + "Z",
                        },
                    )
                    print("[DRY RUN] Draft filled but not submitted.")
                    print("Pass --submit to actually post.")
                    return

                stage = "submit"
                final_url = _click_submit(page)
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
                print(f"Post submitted! URL: {final_url}")
            except Exception as exc:
                _persist_reddit_failure_artifacts(
                    artifact_dir,
                    page=page,
                    stage=stage,
                    subreddit=subreddit,
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


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m social_media.reddit.post",
        description="Auto-post Reddit text posts.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="Check if the saved Reddit session is valid.")

    p_post = sub.add_parser("post", help="Create a Reddit text post.")
    p_post.add_argument("--title", required=True, help="Post title")
    p_post.add_argument("--content", required=True, help="Post body")
    p_post.add_argument("--subreddit", required=True, help="Target subreddit, with or without r/ prefix")
    p_post.add_argument("--post-id", type=int, dest="post_id", help="SocialPost ID for artifact labeling")
    p_post.add_argument("--artifact-dir", help="Directory for debug screenshots/logs/artifacts")
    p_post.add_argument("--submit", action="store_true", help="Actually submit (default: dry run)")

    args = parser.parse_args()
    {"check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
