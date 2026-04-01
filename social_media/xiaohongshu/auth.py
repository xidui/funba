"""Xiaohongshu cookie import and creator login status checks.

Usage:
    .venv/bin/python -m social_media.xiaohongshu.auth login --chrome-profile Default
    .venv/bin/python -m social_media.xiaohongshu.auth check
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
COOKIE_FILE = MODULE_DIR / ".xiaohongshu_cookies.json"
SESSION_META_FILE = MODULE_DIR / ".xiaohongshu_session_meta.json"
BROWSER_DATA_DIR = MODULE_DIR / ".xiaohongshu_browser_data"

HOME_URL = "https://creator.xiaohongshu.com/new/home"
PUBLISH_URL = "https://creator.xiaohongshu.com/publish/publish"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def _playwright():
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for Xiaohongshu login checks. Install it with `pip install playwright`."
        )
    return sync_playwright()


def _chrome_cookie_file(profile: str) -> Path:
    return Path.home() / "Library/Application Support/Google/Chrome" / profile / "Cookies"


def _cookie_url(domain: str) -> str:
    normalized = (domain or "").lstrip(".")
    return f"https://{normalized or 'creator.xiaohongshu.com'}"


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


def _import_chrome_cookies(profile: str) -> list[dict[str, Any]]:
    if browser_cookie3 is None:
        raise RuntimeError(
            "browser-cookie3 is required for Xiaohongshu login import. Install it with `pip install browser-cookie3`."
        )
    cookie_path = _chrome_cookie_file(profile)
    if not cookie_path.exists():
        raise FileNotFoundError(f"Chrome cookie DB not found: {cookie_path}")

    jar = browser_cookie3.chrome(cookie_file=str(cookie_path), domain_name="xiaohongshu.com")
    cookies = [_serialize_cookie(cookie) for cookie in jar if "xiaohongshu.com" in cookie.domain]
    cookies.sort(key=lambda item: (item["domain"], item["name"], item["path"]))
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
        locale="zh-CN",
    )
    if cookies:
        context.add_cookies(_cookies_for_playwright(cookies))
    return context


def _compress_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _extract_identity(body_text: str | None) -> tuple[str | None, str | None]:
    compact = _compress_text(body_text)
    display_name = None
    account_id = None

    name_match = re.search(r"创作服务平台\s+(.+?)\s+发布笔记", compact)
    if name_match:
        candidate = name_match.group(1).strip()
        if candidate and candidate not in {"创作百科", "加入我们"}:
            display_name = candidate

    id_match = re.search(r"小红书账号[:：]\s*(\d+)", compact)
    if id_match:
        account_id = id_match.group(1)

    return display_name, account_id


def check_login_state(cookies: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    active_cookies = cookies or load_cookies()
    if not active_cookies:
        return {
            "ok": False,
            "reason": "No cookies found.",
            "display_name": None,
            "account_id": None,
            "home_url": None,
            "publish_url": None,
            "cookie_count": 0,
        }

    with _playwright() as pw:
        context = _create_context(pw, active_cookies, headless=True)

        home_page = context.new_page()
        home_page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
        home_page.wait_for_timeout(5000)
        home_url = home_page.url
        home_title = home_page.title()
        home_body = home_page.locator("body").inner_text()

        publish_page = context.new_page()
        publish_page.goto(PUBLISH_URL, wait_until="domcontentloaded", timeout=30000)
        publish_page.wait_for_timeout(5000)
        publish_url = publish_page.url
        publish_title = publish_page.title()
        publish_body = publish_page.locator("body").inner_text()

        context.close()

    display_name, account_id = _extract_identity(home_body)
    compact_home = _compress_text(home_body)
    compact_publish = _compress_text(publish_body)
    login_markers = ("短信登录", "发送验证码", "解锁创作者专属功能")

    home_ok = (
        home_url.startswith("https://creator.xiaohongshu.com/")
        and "/login" not in home_url
        and all(marker not in compact_home for marker in login_markers)
        and ("发布笔记" in compact_home or "小红书账号" in compact_home)
    )
    publish_ok = (
        publish_url.startswith("https://creator.xiaohongshu.com/")
        and "/login" not in publish_url
        and all(marker not in compact_publish for marker in login_markers)
        and ("上传图文" in compact_publish or "上传视频" in compact_publish)
    )
    ok = home_ok and publish_ok

    reason = "Logged in." if ok else "Xiaohongshu redirected to creator login."
    return {
        "ok": ok,
        "reason": reason,
        "display_name": display_name,
        "account_id": account_id,
        "home_url": home_url,
        "publish_url": publish_url,
        "cookie_count": len(active_cookies),
        "home_title": home_title,
        "publish_title": publish_title,
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
        "account_id": status["account_id"],
        "ok": status["ok"],
        "home_url": status["home_url"],
        "publish_url": status["publish_url"],
    }
    SESSION_META_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def cmd_login(args: argparse.Namespace) -> None:
    cookies = _import_chrome_cookies(args.chrome_profile)
    if not cookies:
        print(f"No Xiaohongshu cookies found in Chrome profile {args.chrome_profile}.")
        sys.exit(1)

    _write_cookie_cache(cookies)
    status = check_login_state(cookies)
    _write_session_meta(args.chrome_profile, status)

    print(f"Imported {len(cookies)} Xiaohongshu cookies from Chrome {args.chrome_profile}")
    print(f"Saved to {COOKIE_FILE}")
    print(f"Metadata saved to {SESSION_META_FILE}")
    if status["ok"]:
        if status["display_name"]:
            print(f"Logged in as: {status['display_name']}")
        if status["account_id"]:
            print(f"Account ID: {status['account_id']}")
        print(f"Home URL: {status['home_url']}")
        print(f"Publish URL: {status['publish_url']}")
    else:
        print("WARNING: Cookies were saved, but Xiaohongshu did not accept the session.")
        print(f"Home URL: {status['home_url']}")
        print(f"Publish URL: {status['publish_url']}")
        sys.exit(1)


def cmd_check(args: argparse.Namespace) -> None:
    del args
    status = check_login_state()
    if status["ok"]:
        print("Logged in.")
        if status["display_name"]:
            print(f"Account: {status['display_name']}")
        if status["account_id"]:
            print(f"Account ID: {status['account_id']}")
        print(f"Home URL: {status['home_url']}")
        print(f"Publish URL: {status['publish_url']}")
        return

    print("Not logged in.")
    print(status["reason"])
    print("Run: .venv/bin/python -m social_media.xiaohongshu.auth login --chrome-profile Default")
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog=".venv/bin/python -m social_media.xiaohongshu.auth",
        description="Import and verify Xiaohongshu creator cookies.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Import Xiaohongshu cookies from local Chrome.")
    p_login.add_argument(
        "--chrome-profile",
        default="Default",
        help="Chrome profile name (default: Default)",
    )

    sub.add_parser("check", help="Check if the saved Xiaohongshu cookie cache is still valid.")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check}[args.command](args)


if __name__ == "__main__":
    main()
