#!/usr/bin/env python3
"""Single-shot watchdog for the Funba web LaunchAgent.

launchd runs this periodically. It checks the local Flask health endpoint and,
if the app is alive but unresponsive, removes stale Funba processes that still
hold the web port before asking launchd to start a fresh service instance.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


DEFAULT_HEALTH_URL = "http://127.0.0.1:5001/api/health"
DEFAULT_FALLBACK_URL = "http://127.0.0.1:5001/robots.txt"
DEFAULT_PORT = 5001
DEFAULT_SERVICE = "app.funba.web"
DEFAULT_TIMEOUT_SECONDS = 3.0
DEFAULT_TERM_GRACE_SECONDS = 5.0


def _log(message: str) -> None:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[{now}] {message}", flush=True)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _run(args: list[str], *, timeout: float = 10.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _health_ok(url: str, timeout_seconds: float, *, require_json: bool) -> tuple[bool, str]:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "funba-web-watchdog/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read(4096)
            status = getattr(response, "status", response.getcode())
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"

    if status != 200:
        return False, f"HTTP {status}"

    if not require_json:
        return True, "ok"

    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return False, "health endpoint returned non-JSON"

    if payload.get("ok") is True:
        return True, "ok"
    return False, f"unexpected payload: {payload!r}"


def _app_ok(health_url: str, fallback_url: str, timeout_seconds: float) -> tuple[bool, str]:
    ok, detail = _health_ok(health_url, timeout_seconds, require_json=True)
    if ok:
        return True, detail
    if detail == "HTTP 404" and fallback_url:
        fallback_ok, fallback_detail = _health_ok(fallback_url, timeout_seconds, require_json=False)
        if fallback_ok:
            return True, f"{health_url} missing; fallback healthy"
        return False, f"{detail}; fallback {fallback_url} ({fallback_detail})"
    return False, detail


def _listener_pids(port: int) -> set[int]:
    result = _run(
        ["/usr/sbin/lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
        timeout=5.0,
    )
    if result.returncode not in (0, 1):
        _log(f"lsof failed: {result.stderr.strip() or result.stdout.strip()}")
        return set()
    pids: set[int] = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            pids.add(int(line))
        except ValueError:
            continue
    return pids


def _pid_command(pid: int) -> str:
    result = _run(["/bin/ps", "-p", str(pid), "-o", "command="], timeout=5.0)
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _is_funba_web_process(command: str) -> bool:
    lower = command.lower()
    if "funba" not in lower:
        return False
    return "gunicorn" in lower or "web.app" in lower or "web/dev_server.py" in lower


def _terminate_stale_listeners(port: int, grace_seconds: float) -> list[int]:
    current_pid = os.getpid()
    candidates: list[int] = []
    for pid in sorted(_listener_pids(port)):
        if pid == current_pid:
            continue
        command = _pid_command(pid)
        if _is_funba_web_process(command):
            candidates.append(pid)
        else:
            _log(f"not killing pid {pid}; command does not look like Funba web: {command}")

    if not candidates:
        return []

    _log(f"terminating stale Funba listener pid(s): {', '.join(map(str, candidates))}")
    for pid in candidates:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        remaining = _listener_pids(port).intersection(candidates)
        if not remaining:
            return candidates
        time.sleep(0.25)

    remaining = _listener_pids(port).intersection(candidates)
    if remaining:
        _log(f"force-killing stale Funba listener pid(s): {', '.join(map(str, sorted(remaining)))}")
    for pid in remaining:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return candidates


def _kickstart(service: str) -> bool:
    target = f"gui/{os.getuid()}/{service}"
    result = _run(["/bin/launchctl", "kickstart", "-k", target], timeout=15.0)
    if result.returncode == 0:
        _log(f"kickstarted {target}")
        return True
    detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
    _log(f"launchctl kickstart failed for {target}: {detail}")
    return False


def main() -> int:
    health_url = os.getenv("FUNBA_WEB_WATCHDOG_URL", DEFAULT_HEALTH_URL)
    fallback_url = os.getenv("FUNBA_WEB_WATCHDOG_FALLBACK_URL", DEFAULT_FALLBACK_URL)
    port = _env_int("FUNBA_WEB_WATCHDOG_PORT", DEFAULT_PORT)
    service = os.getenv("FUNBA_WEB_WATCHDOG_SERVICE", DEFAULT_SERVICE)
    timeout_seconds = _env_float("FUNBA_WEB_WATCHDOG_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    grace_seconds = _env_float("FUNBA_WEB_WATCHDOG_TERM_GRACE_SECONDS", DEFAULT_TERM_GRACE_SECONDS)
    verbose = os.getenv("FUNBA_WEB_WATCHDOG_VERBOSE") == "1"

    ok, detail = _app_ok(health_url, fallback_url, timeout_seconds)
    if ok:
        if verbose:
            _log(f"healthy: {health_url}")
        return 0

    _log(f"unhealthy: {health_url} ({detail})")
    _terminate_stale_listeners(port, grace_seconds)
    _kickstart(service)

    time.sleep(2.0)
    ok, detail = _app_ok(health_url, fallback_url, timeout_seconds)
    if ok:
        _log("recovered")
        return 0

    _log(f"still unhealthy after restart attempt: {detail}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
