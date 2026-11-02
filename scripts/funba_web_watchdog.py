#!/usr/bin/env python3
"""Check web health and ask launchd to recover a persistently unhealthy service.

Failures and restart attempts survive each scheduled invocation. A busy worker
must not cause a restart storm; recovery targets only the launchd-managed service.
"""

from __future__ import annotations

import fcntl
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_HEALTH_URL = "http://127.0.0.1:5001/api/health"
DEFAULT_FALLBACK_URL = "http://127.0.0.1:5001/robots.txt"
DEFAULT_SERVICE = "app.funba.web"
DEFAULT_TIMEOUT_SECONDS = 3.0
DEFAULT_FAILURE_THRESHOLD = 3
DEFAULT_RESTART_COOLDOWN_SECONDS = 300.0


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

    if isinstance(payload, dict) and payload.get("ok") is True:
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


def _kickstart(target: str) -> bool:
    try:
        result = _run(["/bin/launchctl", "kickstart", "-k", target], timeout=15.0)
    except (OSError, subprocess.TimeoutExpired) as exc:
        _log(f"launchctl kickstart failed for {target}: {exc}")
        return False
    if result.returncode == 0:
        _log(f"kickstarted {target}")
        return True
    detail = result.stderr.strip() or result.stdout.strip() or f"exit {result.returncode}"
    _log(f"launchctl kickstart failed for {target}: {detail}")
    return False


def _managed_keepalive_pid(target: str) -> int | None:
    result = _run(["/bin/launchctl", "print", target], timeout=5.0)
    if result.returncode != 0:
        return None
    properties = re.search(r"^\s*properties = (.+)$", result.stdout, re.MULTILINE)
    pid = re.search(r"^\s*pid = (\d+)\s*$", result.stdout, re.MULTILINE)
    if not properties or "keepalive" not in properties.group(1).split(" | ") or not pid:
        return None
    return int(pid.group(1))


def _restart_service(target: str) -> bool:
    if not target.startswith("system/") or os.geteuid() == 0:
        return _kickstart(target)

    # An unprivileged system daemon cannot kickstart another system service.
    # It can gracefully stop its own launchd-managed master, with KeepAlive
    # responsible for restarting it. Never signal workers or arbitrary listeners.
    try:
        pid = _managed_keepalive_pid(target)
        if pid is None:
            _log(f"recovery skipped: {target} has no running KeepAlive master")
            return False
        process = _run(["/bin/ps", "-p", str(pid), "-o", "uid=,ppid=,command="], timeout=5.0)
        fields = process.stdout.strip().split(None, 2)
        if process.returncode != 0 or len(fields) != 3:
            _log(f"recovery skipped: cannot verify master pid {pid}")
            return False
        uid, parent_pid, command = fields
        if (int(uid) != os.geteuid() or int(parent_pid) != 1
                or "funba" not in command.lower() or "gunicorn" not in command
                or "web.app:app" not in command):
            _log(f"recovery skipped: pid {pid} is not our launchd-managed Funba gunicorn master")
            return False
        if _managed_keepalive_pid(target) != pid:
            _log("recovery skipped: launchd master changed during verification")
            return False
        os.kill(pid, signal.SIGTERM)
        _log(f"sent SIGTERM to {target} master pid {pid}; launchd KeepAlive will restart it")
        return True
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        _log(f"recovery failed for {target}: {exc}")
        return False


def _save_state(path: Path, state: dict) -> None:
    # Keep the lock in a separate file so atomic replacement does not release it.
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(state))
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _check_service(state_path: Path) -> int:
    health_url = os.getenv("FUNBA_WEB_WATCHDOG_URL", DEFAULT_HEALTH_URL)
    fallback_url = os.getenv("FUNBA_WEB_WATCHDOG_FALLBACK_URL", DEFAULT_FALLBACK_URL)
    service = os.getenv("FUNBA_WEB_WATCHDOG_SERVICE", DEFAULT_SERVICE)
    target = os.getenv("FUNBA_WEB_WATCHDOG_LAUNCHCTL_TARGET") or f"gui/{os.getuid()}/{service}"
    timeout_seconds = _env_float("FUNBA_WEB_WATCHDOG_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    failure_threshold = max(2, _env_int("FUNBA_WEB_WATCHDOG_FAILURE_THRESHOLD", DEFAULT_FAILURE_THRESHOLD))
    cooldown_seconds = max(30.0, _env_float(
        "FUNBA_WEB_WATCHDOG_RESTART_COOLDOWN_SECONDS", DEFAULT_RESTART_COOLDOWN_SECONDS,
    ))
    verbose = os.getenv("FUNBA_WEB_WATCHDOG_VERBOSE") == "1"

    try:
        state = json.loads(state_path.read_text())
    except FileNotFoundError:
        state = {}
    if not isinstance(state, dict):
        raise ValueError("watchdog state must be an object")
    if state.get("target") != target:
        state = {"target": target, "failures": 0, "last_restart_at": None}

    ok, detail = _app_ok(health_url, fallback_url, timeout_seconds)
    if ok:
        state["failures"] = 0
        _save_state(state_path, state)
        if verbose:
            _log(f"healthy: {health_url}")
        return 0

    state["failures"] = min(int(state.get("failures", 0)) + 1, failure_threshold)
    _save_state(state_path, state)
    _log(f"unhealthy: {health_url} ({detail}); consecutive failures {state['failures']}/{failure_threshold}")
    if state["failures"] < failure_threshold:
        return 1

    now = time.time()
    last_restart = state.get("last_restart_at")
    if last_restart is not None and now - float(last_restart) < cooldown_seconds:
        _log("recovery skipped: restart cooldown is active")
        return 1

    # Persist before calling launchctl: failed or timed-out restarts also back off.
    state.update(failures=0, last_restart_at=now)
    _save_state(state_path, state)
    if not _restart_service(target):
        return 1

    time.sleep(2.0)
    ok, detail = _app_ok(health_url, fallback_url, timeout_seconds)
    if ok:
        _log("recovered")
        return 0

    _log(f"still unhealthy after restart attempt: {detail}")
    return 1


def main() -> int:
    state_path = Path(os.getenv("FUNBA_WEB_WATCHDOG_STATE_PATH") or (
        Path.home() / "Library" / "Caches" / "funba" / "web-watchdog.json"
    ))
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with state_path.with_name(f"{state_path.name}.lock").open("a") as lock:
            try:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 0
            return _check_service(state_path)
    except (OSError, ValueError, TypeError) as exc:
        # Never restart when durable cooldown/failure tracking is unavailable.
        _log(f"watchdog state unavailable; recovery skipped: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
