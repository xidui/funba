from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WATCH_DIRS = [ROOT / "web", ROOT / "db"]
WATCH_SUFFIXES = {".py", ".html", ".css", ".js"}


def _snapshot() -> dict[str, int]:
    state: dict[str, int] = {}
    for watch_dir in WATCH_DIRS:
        if not watch_dir.exists():
            continue
        for path in watch_dir.rglob("*"):
            if not path.is_file() or path.suffix not in WATCH_SUFFIXES:
                continue
            try:
                state[str(path)] = path.stat().st_mtime_ns
            except FileNotFoundError:
                continue
    return state


def _spawn_server() -> subprocess.Popen:
    env = os.environ.copy()
    env.setdefault("FUNBA_WEB_HOST", "127.0.0.1")
    env.setdefault("FUNBA_WEB_PORT", "5001")
    env["FUNBA_WEB_DEBUG"] = "0"
    return subprocess.Popen([sys.executable, "-m", "web.app"], cwd=str(ROOT), env=env)


def _stop_server(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def main() -> None:
    running = True
    proc: subprocess.Popen | None = None

    def _handle_signal(signum, _frame):
        nonlocal running
        running = False
        _stop_server(proc)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    prev = _snapshot()
    proc = _spawn_server()

    while running:
        time.sleep(1)
        if proc.poll() is not None:
            proc = _spawn_server()
            prev = _snapshot()
            continue

        curr = _snapshot()
        if curr != prev:
            _stop_server(proc)
            proc = _spawn_server()
            prev = curr


if __name__ == "__main__":
    main()

