"""Instagram credential login and session checks.

Usage:
    python -m social_media.instagram.auth login
    python -m social_media.instagram.auth check

Credentials are read from environment first, then from repo-local SECRETS.md:
    INSTAGRAM_USER
    INSTAGRAM_PASSWORD
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from playwright.sync_api import BrowserContext, Page, sync_playwright
except ModuleNotFoundError:
    sync_playwright = None
    BrowserContext = Page = Any


MODULE_DIR = Path(__file__).resolve().parent
REPO_ROOT = MODULE_DIR.parents[1]
STORAGE_STATE_FILE = MODULE_DIR / ".instagram_storage_state.json"
SESSION_META_FILE = MODULE_DIR / ".instagram_session_meta.json"

HOME_URL = "https://www.instagram.com/"
LOGIN_URL = "https://www.instagram.com/accounts/login/"
CREATE_URL = "https://www.instagram.com/create/select/"
REAL_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)

_AUTH_COOKIE_NAMES = ("sessionid", "ds_user_id")
_LOGIN_URL_MARKERS = ("/accounts/login", "/accounts/signup")
_CHECKPOINT_URL_MARKERS = ("/challenge/", "/accounts/two_factor", "/accounts/onetap")
_LOGGED_OUT_TEXT_MARKERS = (
    "Phone number, username, or email",
    "Mobile number, username or email",
    "Log in with Facebook",
    "Sign up",
)
_OPTIONAL_DIALOG_BUTTONS = (
    re.compile(r"^Not Now$", re.IGNORECASE),
    re.compile(r"^Not now$", re.IGNORECASE),
    re.compile(r"^Cancel$", re.IGNORECASE),
)


def _playwright():
    if sync_playwright is None:
        raise RuntimeError(
            "Playwright is required for Instagram automation. Install it with `pip install playwright`."
        )
    return sync_playwright()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _default_secrets_path() -> Path:
    return Path(os.getenv("FUNBA_SECRETS_PATH") or (REPO_ROOT / "SECRETS.md")).expanduser()


def _strip_secret_value(value: str) -> str:
    cleaned = str(value or "").strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        return cleaned[1:-1]
    return cleaned


def _read_secrets_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line = re.sub(r"^[-*]\s*", "", line)
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)\s*(?:=|:)\s*(.*)$", line)
        if not match:
            continue
        values[match.group(1)] = _strip_secret_value(match.group(2))
    return values


def load_credentials(
    *,
    environ: dict[str, str] | None = None,
    secrets_path: str | Path | None = None,
) -> tuple[str, str]:
    env = environ if environ is not None else os.environ
    username = (env.get("INSTAGRAM_USER") or env.get("INSTAGRAM_USERNAME") or "").strip()
    password = (env.get("INSTAGRAM_PASSWORD") or "").strip()

    if not username or not password:
        secrets = _read_secrets_file(Path(secrets_path).expanduser() if secrets_path else _default_secrets_path())
        username = username or secrets.get("INSTAGRAM_USER", "") or secrets.get("INSTAGRAM_USERNAME", "")
        password = password or secrets.get("INSTAGRAM_PASSWORD", "")

    username = username.strip()
    password = password.strip()
    if not username or not password:
        raise RuntimeError(
            "Instagram credentials missing. Set INSTAGRAM_USER and INSTAGRAM_PASSWORD in env or SECRETS.md."
        )
    return username, password


def _create_context(
    pw,
    *,
    headless: bool | None = None,
    use_storage_state: bool = True,
) -> BrowserContext:
    resolved_headless = _env_bool("FUNBA_INSTAGRAM_HEADLESS", True) if headless is None else bool(headless)
    launch_args: list[str] = []
    if resolved_headless:
        launch_args.append("--headless=new")
    browser = pw.chromium.launch(headless=resolved_headless, args=launch_args)
    context_kwargs: dict[str, Any] = {
        "viewport": {"width": 1440, "height": 1200},
        "locale": "en-US",
        "user_agent": REAL_BROWSER_UA,
    }
    if use_storage_state and STORAGE_STATE_FILE.exists():
        context_kwargs["storage_state"] = str(STORAGE_STATE_FILE)
    return browser.new_context(**context_kwargs)


def _compress_text(text: str | None) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _page_text(page: Page) -> str:
    try:
        return page.locator("body").inner_text(timeout=5000)
    except Exception:
        return ""


def _cookie_names(context: BrowserContext) -> set[str]:
    try:
        cookies = context.cookies([HOME_URL])
    except Exception:
        try:
            cookies = context.cookies()
        except Exception:
            cookies = []
    return {str(cookie.get("name") or "") for cookie in cookies if cookie.get("value")}


def _has_auth_cookies(context: BrowserContext) -> bool:
    names = _cookie_names(context)
    return all(name in names for name in _AUTH_COOKIE_NAMES)


def _login_form_count(page: Page) -> int:
    try:
        return page.locator('input[name="username"], input[name="password"], input[name="email"], input[name="pass"]').count()
    except Exception:
        return 0


def _page_looks_logged_in(page: Page) -> bool:
    url = str(getattr(page, "url", "") or "")
    if any(marker in url for marker in _LOGIN_URL_MARKERS):
        return False
    if _login_form_count(page) > 0:
        return False
    if not _has_auth_cookies(page.context):
        return False
    compact = _compress_text(_page_text(page))
    if any(marker in compact for marker in _LOGGED_OUT_TEXT_MARKERS):
        return False
    return url.startswith("https://www.instagram.com/")


def _checkpoint_reason(page: Page) -> str | None:
    url = str(getattr(page, "url", "") or "")
    if any(marker in url for marker in _CHECKPOINT_URL_MARKERS):
        return f"Instagram is waiting for a checkpoint/2FA flow at {url}"
    return None


def _click_optional_dialogs(page: Page) -> None:
    for pattern in _OPTIONAL_DIALOG_BUTTONS:
        try:
            button = page.get_by_role("button", name=pattern).first
            if button.count() and button.is_visible(timeout=500):
                button.click(timeout=1000)
                time.sleep(0.5)
        except Exception:
            continue


def _login_failure_reason(page: Page) -> str:
    checkpoint = _checkpoint_reason(page)
    if checkpoint:
        return checkpoint
    if _login_form_count(page) > 0:
        return "Instagram still shows the login form."
    if not _has_auth_cookies(page.context):
        return "Instagram auth cookies were not set."
    body = _compress_text(_page_text(page))
    if not body:
        return "Could not read Instagram page body."
    return f"Unexpected Instagram login state at {getattr(page, 'url', '')}"


def _write_session_meta(status: dict[str, Any]) -> None:
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "username": status.get("username"),
        "ok": status.get("ok"),
        "home_url": status.get("home_url"),
        "cookie_count": status.get("cookie_count"),
        "storage_state_file": STORAGE_STATE_FILE.name,
    }
    SESSION_META_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _status_from_page(page: Page, *, username: str | None, reason: str | None = None) -> dict[str, Any]:
    cookie_count = len(_cookie_names(page.context))
    ok = _page_looks_logged_in(page)
    return {
        "ok": ok,
        "reason": "Logged in." if ok else (reason or _login_failure_reason(page)),
        "username": username,
        "home_url": str(getattr(page, "url", "") or ""),
        "cookie_count": cookie_count,
        "storage_state_file": str(STORAGE_STATE_FILE),
    }


def _fill_login_form(page: Page, username: str, password: str) -> None:
    username_input = page.locator('input[name="username"], input[name="email"]').first
    password_input = page.locator('input[name="password"], input[name="pass"]').first
    username_input.wait_for(state="visible", timeout=30000)
    password_input.wait_for(state="visible", timeout=30000)
    username_input.fill(username, timeout=5000)
    password_input.fill(password, timeout=5000)
    try:
        page.locator('button[type="submit"], input[type="submit"]').first.click(timeout=5000)
    except Exception:
        password_input.press("Enter", timeout=3000)


def ensure_login_context(
    pw,
    *,
    headless: bool | None = None,
    login_timeout_seconds: float = 90.0,
) -> tuple[BrowserContext, Page, dict[str, Any]]:
    username, password = load_credentials()

    context = _create_context(pw, headless=headless, use_storage_state=True)
    page = context.new_page()
    page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(3000)
    _click_optional_dialogs(page)
    if _page_looks_logged_in(page):
        status = _status_from_page(page, username=username)
        return context, page, status

    try:
        context.close()
    except Exception:
        pass

    context = _create_context(pw, headless=headless, use_storage_state=False)
    page = context.new_page()
    page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
    _fill_login_form(page, username, password)

    deadline = time.time() + max(float(login_timeout_seconds), 15.0)
    last_reason = None
    while time.time() < deadline:
        page.wait_for_timeout(1000)
        _click_optional_dialogs(page)
        if _page_looks_logged_in(page):
            STORAGE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            context.storage_state(path=str(STORAGE_STATE_FILE))
            status = _status_from_page(page, username=username)
            _write_session_meta(status)
            return context, page, status
        last_reason = _checkpoint_reason(page) or last_reason

    status = _status_from_page(page, username=username, reason=last_reason)
    raise RuntimeError(status["reason"])


def check_login_state(
    *,
    auto_login: bool = True,
    headless: bool | None = None,
    login_timeout_seconds: float = 90.0,
) -> dict[str, Any]:
    username = None
    try:
        username, _password = load_credentials()
    except Exception as exc:
        return {
            "ok": False,
            "reason": str(exc),
            "username": None,
            "home_url": None,
            "cookie_count": 0,
            "storage_state_file": str(STORAGE_STATE_FILE),
        }

    with _playwright() as pw:
        context = _create_context(pw, headless=headless, use_storage_state=True)
        page = context.new_page()
        page.goto(HOME_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)
        _click_optional_dialogs(page)
        status = _status_from_page(page, username=username)
        context.close()
        if status["ok"] or not auto_login:
            return status

        context, page, status = ensure_login_context(
            pw,
            headless=headless,
            login_timeout_seconds=login_timeout_seconds,
        )
        context.close()
        return status


def cmd_login(args: argparse.Namespace) -> None:
    try:
        with _playwright() as pw:
            context, _page, status = ensure_login_context(
                pw,
                headless=not bool(args.show_browser),
                login_timeout_seconds=float(args.timeout_seconds),
            )
            context.close()
    except Exception as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print("Logged in.")
    print(f"Username: {status['username']}")
    print(f"Home URL: {status['home_url']}")
    print(f"Storage state: {STORAGE_STATE_FILE}")


def cmd_check(args: argparse.Namespace) -> None:
    status = check_login_state(
        auto_login=not bool(args.no_login),
        headless=not bool(args.show_browser),
        login_timeout_seconds=float(args.timeout_seconds),
    )
    if status["ok"]:
        print("Logged in.")
        print(f"Username: {status['username']}")
        print(f"Home URL: {status['home_url']}")
        return

    print("ERROR: Not logged in.")
    print(status["reason"])
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m social_media.instagram.auth",
        description="Log in to Instagram with credentials from env or SECRETS.md.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_login = sub.add_parser("login", help="Log in and cache Instagram browser storage state.")
    p_login.add_argument("--show-browser", action="store_true", help="Show the browser window for checkpoint/2FA handling.")
    p_login.add_argument("--timeout-seconds", type=float, default=90.0)

    p_check = sub.add_parser("check", help="Check the cached Instagram session; logs in if needed by default.")
    p_check.add_argument("--no-login", action="store_true", help="Only check cached state; do not submit credentials.")
    p_check.add_argument("--show-browser", action="store_true", help="Show the browser window if login is needed.")
    p_check.add_argument("--timeout-seconds", type=float, default=90.0)

    args = parser.parse_args()
    {"login": cmd_login, "check": cmd_check}[args.command](args)


if __name__ == "__main__":
    main()
