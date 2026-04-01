"""X/Twitter cookie import and login status checks.

Usage:
    .venv/bin/python -m social_media.twitter.auth login --chrome-profile Default
    .venv/bin/python -m social_media.twitter.auth check
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import browser_cookie3
except ModuleNotFoundError:
    browser_cookie3 = None

try:
    from playwright.sync_api import sync_playwright
except ModuleNotFoundError:
    sync_playwright = None

MODULE_DIR = Path(__file__).resolve().parent
COOKIE_FILE = MODULE_DIR / ".twitter_cookies.json"
SESSION_META_FILE = MODULE_DIR / ".twitter_session_meta.json"
BROWSER_DATA_DIR = MODULE_DIR / ".twitter_browser_data"

X_HOME = "https://x.com/home"
COMPOSE_URL = "https://x.com/compose/post"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def _playwright():
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for Twitter/X login checks. Install it with `pip install playwright`."
        )
    return sync_playwright()


def _chrome_cookie_file(profile: str) -> Path:
    return Path.home() / "Library/Application Support/Google/Chrome" / profile / "Cookies"


def _cookie_url(domain: str) -> str:
    normalized = (domain or "").lstrip(".")
    return f"https://{normalized or 'x.com'}"


def _serialize_cookie(cookie: Any) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "name": cookie.name,
        "value": cookie.value,
        "domain": cookie.domain,
        "path": cookie.path or "/",
        "secure": bool(cookie.secure),
        "url": _cookie_url(cookie.domain),
    }
    if cookie.expires is not None:
        entry["expires"] = int(cookie.expires)
    if getattr(cookie, "_rest", None):
        if "HttpOnly" in cookie._rest:
            entry["httpOnly"] = True
    return entry


def _import_domain_cookies(cookie_path: Path, domain_name: str) -> list[dict[str, Any]]:
    jar = browser_cookie3.chrome(cookie_file=str(cookie_path), domain_name=domain_name)
    return [_serialize_cookie(cookie) for cookie in jar]


def _import_chrome_cookies(profile: str) -> list[dict[str, Any]]:
    if browser_cookie3 is None:
        raise RuntimeError(
            "browser-cookie3 is required for Twitter/X login import. Install it with `pip install browser-cookie3`."
        )
    cookie_path = _chrome_cookie_file(profile)
    if not cookie_path.exists():
        raise FileNotFoundError(f"Chrome cookie DB not found: {cookie_path}")

    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for domain_name in ("x.com", "twitter.com"):
        for cookie in _import_domain_cookies(cookie_path, domain_name):
            domain = str(cookie.get("domain", ""))
            if "x.com" not in domain and "twitter.com" not in domain:
                continue
            key = (domain, str(cookie["name"]), str(cookie.get("path", "/")))
            merged[key] = cookie

    cookies = sorted(merged.values(), key=lambda item: (item["domain"], item["name"], item["path"]))
    return cookies


def load_cookies() -> list[dict[str, Any]]:
    if not COOKIE_FILE.exists():
        return []
    return json.loads(COOKIE_FILE.read_text())


def _cookies_for_playwright(cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for cookie in cookies:
        entry: dict[str, Any] = {
            "name": str(cookie["name"]),
            "value": str(cookie.get("value", "")),
            "domain": str(cookie["domain"]),
            "path": str(cookie.get("path", "/")),
            "secure": bool(cookie.get("secure", False)),
        }
        if cookie.get("expires") is not None:
            entry["expires"] = float(cookie["expires"])
        if cookie.get("httpOnly") is not None:
            entry["httpOnly"] = bool(cookie["httpOnly"])
        prepared.append(entry)
    return prepared


def _create_context(pw, cookies: list[dict[str, Any]], *, headless: bool = True):
    browser = pw.chromium.launch(headless=headless)
    context = browser.new_context(
        user_agent=REAL_BROWSER_UA,
        viewport={"width": 1440, "height": 1200},
        locale="en-US",
    )
    if cookies:
        context.add_cookies(_cookies_for_playwright(cookies))
    return context


def _extract_identity(account_text: str | None) -> tuple[str | None, str | None]:
    raw = (account_text or "").strip()
    if not raw:
        return None, None
    handle_match = re.search(r"@([A-Za-z0-9_]+)", raw)
    handle = handle_match.group(1) if handle_match else None
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    display_name = None
    for line in lines:
        if not line.startswith("@"):
            display_name = line
            break
    return display_name, handle


def _format_identity(display_name: str | None, handle: str | None) -> str | None:
    if display_name and handle:
        return f"{display_name} (@{handle})"
    if handle:
        return f"@{handle}"
    if display_name:
        return display_name
    return None


def check_login_state(cookies: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    active_cookies = cookies or load_cookies()
    if not active_cookies:
        return {
            "ok": False,
            "reason": "No cookies found.",
            "handle": None,
            "display_name": None,
            "final_url": None,
            "compose_url": None,
            "cookie_count": 0,
        }

    with _playwright() as pw:
        context = _create_context(pw, active_cookies, headless=True)
        page = context.new_page()
        page.goto(X_HOME, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(8000)

        final_url = page.url
        title = page.title()
        account_switcher = page.locator('[data-testid="SideNav_AccountSwitcher_Button"]').first
        account_switcher_count = account_switcher.count()
        tweet_button = page.locator('[data-testid="SideNav_NewTweet_Button"]').count()
        compose_textarea = page.locator('[data-testid="tweetTextarea_0"]').count()
        login_link = page.locator('a[href*="/login"]').count()
        account_text = account_switcher.inner_text(timeout=1000).strip() if account_switcher_count else None
        display_name, handle = _extract_identity(account_text)

        compose_page = context.new_page()
        compose_page.goto(COMPOSE_URL, wait_until="domcontentloaded", timeout=30000)
        compose_page.wait_for_timeout(5000)
        compose_url = compose_page.url
        compose_has_box = compose_page.locator('[data-testid="tweetTextarea_0"]').count()
        compose_login_link = compose_page.locator('a[href*="/login"]').count()

        context.close()

    ok = (
        final_url.startswith("https://x.com/")
        and title.endswith(" / X")
        and account_switcher_count > 0
        and tweet_button > 0
        and compose_textarea > 0
        and login_link == 0
        and compose_url.startswith("https://x.com/")
        and compose_has_box > 0
        and compose_login_link == 0
    )
    reason = "Logged in." if ok else "X/Twitter did not expose an authenticated UI."
    return {
        "ok": ok,
        "reason": reason,
        "handle": handle,
        "display_name": display_name,
        "final_url": final_url,
        "compose_url": compose_url,
        "cookie_count": len(active_cookies),
    }


def _write_cookie_cache(cookies: list[dict[str, Any]]) -> None:
    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)
    COOKIE_FILE.write_text(json.dumps(cookies, indent=2, ensure_ascii=False))


def _write_session_meta(profile: str, status: dict[str, Any]) -> None:
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "source_browser": "chrome",
        "source_profile": profile,
        "cookie_file": str(COOKIE_FILE.name),
        "cookie_count": status["cookie_count"],
        "display_name": status["display_name"],
        "handle": status["handle"],
        "ok": status["ok"],
        "final_url": status["final_url"],
        "compose_url": status["compose_url"],
    }
    SESSION_META_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def cmd_login(args: argparse.Namespace) -> None:
    cookies = _import_chrome_cookies(args.chrome_profile)
    if not cookies:
        print(f"No X/Twitter cookies found in Chrome profile {args.chrome_profile}.")
        sys.exit(1)

    _write_cookie_cache(cookies)
    status = check_login_state(cookies)
    _write_session_meta(args.chrome_profile, status)

    print(f"Imported {len(cookies)} X/Twitter cookies from Chrome {args.chrome_profile}")
    print(f"Saved to {COOKIE_FILE}")
    print(f"Metadata saved to {SESSION_META_FILE}")
    if status["ok"]:
        label = _format_identity(status["display_name"], status["handle"]) or "unknown"
        print(f"Logged in as: {label}")
        print(f"Home URL: {status['final_url']}")
        print(f"Compose URL: {status['compose_url']}")
    else:
        print("WARNING: Cookies were saved, but X/Twitter did not accept the session.")
        print(f"Home URL: {status['final_url']}")
        print(f"Compose URL: {status['compose_url']}")
        sys.exit(1)


def cmd_check(args: argparse.Namespace) -> None:
    del args
    status = check_login_state()
    if status["ok"]:
        print("Logged in.")
        label = _format_identity(status["display_name"], status["handle"])
        if label:
            print(f"Account: {label}")
        print(f"Home URL: {status['final_url']}")
        print(f"Compose URL: {status['compose_url']}")
        return

    print("Not logged in.")
    print(status["reason"])
    print("Run: .venv/bin/python -m social_media.twitter.auth login --chrome-profile Default")
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog=".venv/bin/python -m social_media.twitter.auth",
        description="Import and verify X/Twitter login cookies.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Import X/Twitter cookies from local Chrome.")
    p_login.add_argument(
        "--chrome-profile",
        default="Default",
        help="Chrome profile name (default: Default)",
    )

    sub.add_parser("check", help="Check if the saved X/Twitter cookie cache is still valid.")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check}[args.command](args)


if __name__ == "__main__":
    main()
