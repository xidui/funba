"""Reddit cookie import and login status checks.

Usage:
    .venv/bin/python -m social_media.reddit.auth login --chrome-profile Default
    .venv/bin/python -m social_media.reddit.auth check
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    import browser_cookie3
except ModuleNotFoundError:
    browser_cookie3 = None

MODULE_DIR = Path(__file__).resolve().parent
COOKIE_FILE = MODULE_DIR / ".reddit_cookies.json"
SESSION_META_FILE = MODULE_DIR / ".reddit_session_meta.json"
BROWSER_DATA_DIR = MODULE_DIR / ".reddit_browser_data"

REDDIT_HOME = "https://www.reddit.com"
ME_URL = f"{REDDIT_HOME}/user/me/"
SUBMIT_URL = f"{REDDIT_HOME}/submit"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)


def _chrome_cookie_file(profile: str) -> Path:
    return Path.home() / "Library/Application Support/Google/Chrome" / profile / "Cookies"


def _cookie_url(domain: str) -> str:
    normalized = (domain or "").lstrip(".")
    return f"https://{normalized or 'www.reddit.com'}"


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
            "browser-cookie3 is required for Reddit login import. Install it with `pip install browser-cookie3`."
        )
    cookie_path = _chrome_cookie_file(profile)
    if not cookie_path.exists():
        raise FileNotFoundError(f"Chrome cookie DB not found: {cookie_path}")

    jar = browser_cookie3.chrome(cookie_file=str(cookie_path), domain_name="reddit.com")
    cookies = [_serialize_cookie(cookie) for cookie in jar if "reddit.com" in cookie.domain]
    cookies.sort(key=lambda item: (item["domain"], item["name"], item["path"]))
    return cookies


def load_cookies() -> list[dict[str, Any]]:
    if not COOKIE_FILE.exists():
        return []
    return json.loads(COOKIE_FILE.read_text())


def build_requests_session(cookies: list[dict[str, Any]] | None = None) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": REAL_BROWSER_UA})
    for cookie in cookies or load_cookies():
        domain = str(cookie.get("domain", "")).lstrip(".")
        session.cookies.set(
            str(cookie["name"]),
            str(cookie.get("value", "")),
            domain=domain,
            path=str(cookie.get("path", "/")),
        )
    return session


def check_login_state(cookies: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    active_cookies = cookies or load_cookies()
    if not active_cookies:
        return {
            "ok": False,
            "reason": "No cookies found.",
            "username": None,
            "me_url": None,
            "submit_url": None,
            "cookie_count": 0,
        }

    session = build_requests_session(active_cookies)
    me_response = session.get(ME_URL, timeout=20, allow_redirects=True)
    submit_response = session.get(SUBMIT_URL, timeout=20, allow_redirects=True)

    me_url = me_response.url
    submit_url = submit_response.url
    username = None
    match = re.search(r"/user/([^/]+)/?$", me_url)
    if match:
        username = match.group(1)

    me_ok = me_response.status_code == 200 and "/login" not in me_url
    submit_ok = submit_response.status_code == 200 and "/login" not in submit_url
    ok = me_ok and submit_ok and bool(username)

    reason = "Logged in." if ok else "Reddit redirected to login."
    return {
        "ok": ok,
        "reason": reason,
        "username": username,
        "me_url": me_url,
        "submit_url": submit_url,
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
        "username": status["username"],
        "ok": status["ok"],
        "me_url": status["me_url"],
        "submit_url": status["submit_url"],
    }
    SESSION_META_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False))


def cmd_login(args: argparse.Namespace) -> None:
    cookies = _import_chrome_cookies(args.chrome_profile)
    if not cookies:
        print(f"No Reddit cookies found in Chrome profile {args.chrome_profile}.")
        sys.exit(1)

    _write_cookie_cache(cookies)
    status = check_login_state(cookies)
    _write_session_meta(args.chrome_profile, status)

    print(f"Imported {len(cookies)} Reddit cookies from Chrome {args.chrome_profile}")
    print(f"Saved to {COOKIE_FILE}")
    print(f"Metadata saved to {SESSION_META_FILE}")
    if status["ok"]:
        print(f"Logged in as: {status['username']}")
        print(f"Me URL: {status['me_url']}")
        print(f"Submit URL: {status['submit_url']}")
    else:
        print("WARNING: Cookies were saved, but Reddit did not accept the session.")
        print(f"Me URL: {status['me_url']}")
        print(f"Submit URL: {status['submit_url']}")
        sys.exit(1)


def cmd_check(args: argparse.Namespace) -> None:
    del args
    status = check_login_state()
    if status["ok"]:
        print("Logged in.")
        print(f"Username: {status['username']}")
        print(f"Me URL: {status['me_url']}")
        print(f"Submit URL: {status['submit_url']}")
        return

    print("Not logged in.")
    print(status["reason"])
    print("Run: .venv/bin/python -m social_media.reddit.auth login --chrome-profile Default")
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog=".venv/bin/python -m social_media.reddit.auth",
        description="Import and verify Reddit login cookies.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Import Reddit cookies from local Chrome.")
    p_login.add_argument(
        "--chrome-profile",
        default="Default",
        help="Chrome profile name (default: Default)",
    )

    sub.add_parser("check", help="Check if the saved Reddit cookie cache is still valid.")

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check}[args.command](args)


if __name__ == "__main__":
    main()
