"""Prepare and optionally submit X/Twitter posts with saved cookie injection.

Usage:
    python -m social_media.twitter.post check
    python -m social_media.twitter.post post --content "..."
    python -m social_media.twitter.post post --content "..." --submit
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Any

from .auth import COMPOSE_URL, REAL_BROWSER_UA, check_login_state, load_cookies

try:
    from playwright.sync_api import sync_playwright, BrowserContext, Page
except ModuleNotFoundError:
    sync_playwright = None
    BrowserContext = Page = Any


MODULE_DIR = Path(__file__).resolve().parent
POST_URL_RE = re.compile(
    r"https://(?:x|twitter)\.com/[A-Za-z0-9_]{1,20}/status/\d+(?:[/?#][^\s\"'<]*)?",
    re.IGNORECASE,
)
URL_RE = re.compile(r"https?://\S+")
DEFAULT_TCO_URL_LENGTH = 23
DEFAULT_TWEET_LIMIT = 280
_SHORT_SLEEP = 0.1
_MEDIUM_SLEEP = 0.4
_LONG_SLEEP = 1.0


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


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


def _estimated_tweet_length(text: str, *, url_length: int = DEFAULT_TCO_URL_LENGTH) -> int:
    """Approximate X text length by replacing URLs with the t.co length."""
    total = 0
    pos = 0
    for match in URL_RE.finditer(str(text or "")):
        total += len(text[pos : match.start()])
        total += url_length
        pos = match.end()
    total += len(text[pos:])
    return total


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
        path = Path.cwd() / "logs" / "twitter_post" / "-".join(parts)
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


def _persist_twitter_failure_artifacts(
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


def _playwright():
    if sync_playwright is None:
        raise RuntimeError("Playwright is required for X/Twitter posting commands. Install it with `pip install playwright`.")
    return sync_playwright()


def _create_context(pw, *, headless: bool | None = None) -> BrowserContext:
    browser = pw.chromium.launch(
        headless=_env_bool("FUNBA_TWITTER_HEADLESS", True) if headless is None else bool(headless)
    )
    context = browser.new_context(
        viewport={"width": 1440, "height": 1200},
        locale="en-US",
        user_agent=REAL_BROWSER_UA,
    )
    cookies = load_cookies()
    if cookies:
        context.add_cookies([_cookie_for_playwright(cookie) for cookie in cookies])
    return context


def _set_composer_text(page: Page, content: str) -> str | None:
    selectors = [
        '[data-testid="tweetTextarea_0"]',
        '[aria-label*="Post text" i][contenteditable="true"]',
        '[aria-label*="Tweet text" i][contenteditable="true"]',
        'div[role="textbox"][contenteditable="true"]',
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            locator.wait_for(state="visible", timeout=3000)
            locator.click(timeout=3000)
            locator.fill(content, timeout=5000)
            return selector
        except Exception:
            continue

    script = """
({selectors, value}) => {
  const dispatch = (el) => {
    el.dispatchEvent(new InputEvent("input", { bubbles: true, inputType: "insertText", data: value }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
  };
  for (const selector of selectors) {
    const el = document.querySelector(selector);
    if (!el) continue;
    el.focus();
    if (el.isContentEditable) {
      const lines = String(value).split("\\n");
      el.innerHTML = "";
      lines.forEach((line, index) => {
        const block = document.createElement("div");
        block.textContent = line;
        if (!line) block.appendChild(document.createElement("br"));
        el.appendChild(block);
        if (index === lines.length - 1) return;
      });
      dispatch(el);
      return selector;
    }
    if (el instanceof HTMLTextAreaElement || el instanceof HTMLInputElement) {
      el.value = value;
      dispatch(el);
      return selector;
    }
  }
  return null;
}
"""
    try:
        selector = page.evaluate(script, {"selectors": selectors, "value": content})
        if selector:
            return str(selector)
    except Exception:
        pass
    return None


def _first_selector_present(page: Page, selectors: list[str]) -> str | None:
    for selector in selectors:
        try:
            if page.query_selector(selector):
                return selector
        except Exception:
            continue
    return None


def _normalize_status_url(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    match = POST_URL_RE.search(candidate)
    if match:
        url = match.group(0).rstrip("/")
        url = re.sub(r"^https://twitter\.com/", "https://x.com/", url, flags=re.IGNORECASE)
        return re.sub(r"[?#].*$", "", url)
    relative = re.search(r"/[A-Za-z0-9_]{1,20}/status/\d+", candidate)
    if relative:
        return f"https://x.com{relative.group(0)}"
    return None


def _extract_status_url_from_text(text: str | None) -> str | None:
    return _normalize_status_url(text)


def _extract_status_urls_from_page_state(page: Page) -> set[str]:
    urls: set[str] = set()
    extracted = _normalize_status_url(_safe_page_url(page))
    if extracted:
        urls.add(extracted)

    evaluator = getattr(page, "evaluate", None)
    if not callable(evaluator):
        return urls
    try:
        state = evaluator(
            """() => ({
                historyState: (() => { try { return JSON.stringify(history.state || null); } catch { return ""; } })(),
                title: document.title || "",
                anchors: Array.from(document.querySelectorAll('a[href*="/status/"]')).map((a) => a.href || a.getAttribute("href") || ""),
            })"""
        ) or {}
    except Exception:
        return urls
    if not isinstance(state, dict):
        return urls
    for candidate in [state.get("historyState"), state.get("title"), *(state.get("anchors") or [])]:
        extracted = _extract_status_url_from_text(str(candidate or ""))
        if extracted:
            urls.add(extracted)
    return urls


def _wait_for_new_status_url(page: Page, before_urls: set[str], timeout_seconds: float = 20.0) -> str | None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        urls = _extract_status_urls_from_page_state(page)
        new_urls = [url for url in sorted(urls) if url not in before_urls]
        if new_urls:
            return new_urls[0]
        time.sleep(_SHORT_SLEEP)
    return None


def _click_post(page: Page) -> str | None:
    before_urls = _extract_status_urls_from_page_state(page)
    clicked = False
    button_selectors = [
        '[data-testid="tweetButton"]',
        '[data-testid="tweetButtonInline"]',
        'button[data-testid*="tweetButton"]',
    ]
    for selector in button_selectors:
        try:
            button = page.locator(selector).last
            button.wait_for(state="visible", timeout=3000)
            if button.is_disabled(timeout=1000):
                continue
            button.click(timeout=5000)
            clicked = True
            break
        except Exception:
            continue
    if not clicked:
        try:
            button = page.get_by_role("button", name=re.compile(r"^(post|tweet)$", re.IGNORECASE)).last
            if not button.is_disabled(timeout=1000):
                button.click(timeout=5000)
                clicked = True
        except Exception:
            pass
    if not clicked:
        raise RuntimeError("X/Twitter Post button not found or disabled")

    final_url = _wait_for_new_status_url(page, before_urls)
    return final_url


def cmd_check(args: argparse.Namespace) -> None:
    del args
    status = check_login_state()
    if status["ok"]:
        print("Logged in.")
        label = status.get("handle") or status.get("display_name")
        if label:
            print(f"Account: {label}")
        print(f"Compose URL: {status['compose_url']}")
        return
    print("ERROR: Not logged in.")
    print(status["reason"])
    sys.exit(1)


def cmd_post(args: argparse.Namespace) -> None:
    content = str(args.content or "")
    submit = bool(args.submit)
    post_id = getattr(args, "post_id", None)
    artifact_dir = _resolve_artifact_dir(getattr(args, "artifact_dir", None), post_id=post_id)
    keep_open_seconds = max(float(getattr(args, "keep_open_seconds", 0) or 0), 0.0)
    headed = bool(getattr(args, "headed", False)) or keep_open_seconds > 0
    tweet_limit = max(int(getattr(args, "tweet_limit", DEFAULT_TWEET_LIMIT) or DEFAULT_TWEET_LIMIT), 1)
    estimated_length = _estimated_tweet_length(content)
    stage = "starting"
    context = None
    page = None
    browser_events: dict[str, list[dict[str, str]]] = {
        "console": [],
        "pageerror": [],
        "requestfailed": [],
    }

    print(f"Estimated X length: {estimated_length}/{tweet_limit}")
    print(f"Submit: {'YES' if submit else 'NO (draft only)'}")
    print(f"Artifacts: {artifact_dir}")
    print()

    _write_json_artifact(
        artifact_dir / "request.json",
        {
            "post_id": post_id,
            "submit": submit,
            "estimated_length": estimated_length,
            "tweet_limit": tweet_limit,
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
    )

    if not content.strip():
        print("ERROR: X/Twitter content is required.")
        sys.exit(1)
    if estimated_length > tweet_limit:
        print(f"ERROR: X/Twitter content too long: estimated {estimated_length}/{tweet_limit}.")
        sys.exit(1)

    try:
        with _playwright() as pw:
            context = _create_context(pw, headless=not headed)
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
                stage = "open_compose"
                page.goto(COMPOSE_URL, wait_until="domcontentloaded", timeout=30000)
                time.sleep(_LONG_SLEEP)
                _safe_page_screenshot(page, artifact_dir / "compose_loaded.png")
                print(f"Compose page loaded: {page.url}")

                if "/login" in str(page.url):
                    raise RuntimeError("Not logged in. X/Twitter redirected to login.")

                stage = "fill_compose"
                selector = _set_composer_text(page, content)
                if not selector:
                    raise RuntimeError("X/Twitter composer text box not found")
                time.sleep(_MEDIUM_SLEEP)
                _safe_page_screenshot(page, artifact_dir / "filled.png")
                print("Draft prepared.")

                if not submit:
                    _write_json_artifact(
                        artifact_dir / "result.json",
                        {
                            "status": "dry_run",
                            "page_url": _safe_page_url(page),
                            "text_selector": selector,
                            "post_button_selector": _first_selector_present(
                                page,
                                [
                                    '[data-testid="tweetButton"]',
                                    '[data-testid="tweetButtonInline"]',
                                    'button[data-testid*="tweetButton"]',
                                ],
                            ),
                            "captured_at": datetime.utcnow().isoformat() + "Z",
                        },
                    )
                    print("[DRY RUN] Draft filled but not submitted.")
                    if keep_open_seconds > 0:
                        print(f"Keeping browser open for {keep_open_seconds:g}s for review.")
                        time.sleep(keep_open_seconds)
                    print("Pass --submit to actually post.")
                    return

                stage = "submit"
                final_url = _click_post(page)
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
                if final_url:
                    print(f"Post submitted! URL: {final_url}")
                else:
                    print("Post submitted! URL: not detected")
            except Exception as exc:
                _persist_twitter_failure_artifacts(
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


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m social_media.twitter.post",
        description="Prepare and optionally submit X/Twitter posts.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="Check if the saved X/Twitter session is valid.")

    p_post = sub.add_parser("post", help="Create an X/Twitter post.")
    p_post.add_argument("--content", required=True, help="Post body")
    p_post.add_argument("--post-id", type=int, dest="post_id", help="SocialPost ID for artifact labeling")
    p_post.add_argument("--artifact-dir", help="Directory for debug screenshots/logs/artifacts")
    p_post.add_argument("--tweet-limit", type=int, default=DEFAULT_TWEET_LIMIT)
    p_post.add_argument(
        "--keep-open-seconds",
        type=float,
        default=0,
        help="In dry-run mode, keep the visible browser open for manual review.",
    )
    p_post.add_argument(
        "--headed",
        action="store_true",
        help="Show the browser window for manual debugging. Default is headless.",
    )
    p_post.add_argument("--submit", action="store_true", help="Actually submit (default: draft only)")

    args = parser.parse_args()
    {"check": cmd_check, "post": cmd_post}[args.command](args)


if __name__ == "__main__":
    main()
