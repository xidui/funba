#!/usr/bin/env python3
"""Wait for local service ports, then exec the target command.

Used by launchd during machine boot so Funba workers do not start before
MySQL/Redis are accepting connections.
"""

from __future__ import annotations

import os
import socket
import sys
import time
from datetime import datetime, timezone


DEFAULT_TIMEOUT_SECONDS = 180.0
DEFAULT_INTERVAL_SECONDS = 2.0
DEFAULT_STABLE_SECONDS = 3.0


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


def _parse_targets(raw: str) -> list[tuple[str, int]]:
    targets: list[tuple[str, int]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        host, sep, port = item.rpartition(":")
        if not sep or not host:
            raise ValueError(f"invalid FUNBA_WAIT_TCP entry: {item!r}")
        targets.append((host, int(port)))
    return targets


def _can_connect(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _wait_for_targets(
    targets: list[tuple[str, int]],
    timeout_seconds: float,
    interval_seconds: float,
    stable_seconds: float,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    stable_since: float | None = None
    pending = list(targets)
    while time.monotonic() < deadline:
        next_pending: list[tuple[str, int]] = []
        for host, port in targets:
            if _can_connect(host, port):
                if (host, port) in pending:
                    _log(f"ready: {host}:{port}")
            else:
                next_pending.append((host, port))
        pending = next_pending
        if not pending:
            if stable_since is None:
                stable_since = time.monotonic()
                if stable_seconds > 0:
                    _log(f"all targets ready; waiting {stable_seconds:.1f}s for stability")
            elif time.monotonic() - stable_since >= stable_seconds:
                return True
            time.sleep(min(interval_seconds, max(stable_seconds, 0.1)))
        else:
            stable_since = None
            time.sleep(interval_seconds)
            continue

        if pending:
            time.sleep(interval_seconds)
    if pending:
        _log("timed out waiting for: " + ", ".join(f"{host}:{port}" for host, port in pending))
    else:
        _log("timed out waiting for targets to remain stable")
    return False


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: funba_wait_exec.py COMMAND [ARG...]", file=sys.stderr)
        return 64

    raw_targets = os.getenv("FUNBA_WAIT_TCP", "")
    targets = _parse_targets(raw_targets) if raw_targets else []
    if targets:
        timeout_seconds = _env_float("FUNBA_WAIT_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
        interval_seconds = _env_float("FUNBA_WAIT_INTERVAL_SECONDS", DEFAULT_INTERVAL_SECONDS)
        stable_seconds = _env_float("FUNBA_WAIT_STABLE_SECONDS", DEFAULT_STABLE_SECONDS)
        if not _wait_for_targets(targets, timeout_seconds, interval_seconds, stable_seconds):
            return 75

    os.execv(argv[0], argv)
    return 127


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
