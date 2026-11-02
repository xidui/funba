import sys
import json
import signal
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = REPO_ROOT / "tests"
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

from test_auth import _make_app  # noqa: E402
from scripts import funba_web_watchdog as watchdog  # noqa: E402


def test_health_endpoint_is_lightweight_and_public():
    app, _, _ = _make_app()
    app.config["TESTING"] = True
    resp = app.test_client().get("/api/health", environ_overrides={"REMOTE_ADDR": "8.8.8.8"})
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True, "service": "funba"}


def test_search_referrer_deep_page_throttle_is_early_and_switchable(monkeypatch):
    app, _, _ = _make_app()
    app.config["TESTING"] = False
    monkeypatch.setenv("FUNBA_SEARCH_DEEP_THROTTLE", "1")

    resp = app.test_client().get(
        "/metrics/player_total_pts_vs_opponent?entity=2544&season=22025",
        headers={"Referer": "https://www.google.com/"},
        environ_overrides={"REMOTE_ADDR": "8.8.8.8"},
    )

    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "300"


def _configure_watchdog(monkeypatch, tmp_path, health_results):
    state_path = tmp_path / "watchdog.json"
    monkeypatch.setenv("FUNBA_WEB_WATCHDOG_STATE_PATH", str(state_path))
    monkeypatch.setenv("FUNBA_WEB_WATCHDOG_LAUNCHCTL_TARGET", "gui/501/app.funba.web")
    monkeypatch.setenv("FUNBA_WEB_WATCHDOG_FAILURE_THRESHOLD", "3")
    monkeypatch.setenv("FUNBA_WEB_WATCHDOG_RESTART_COOLDOWN_SECONDS", "300")
    monkeypatch.setattr(watchdog, "_app_ok", Mock(side_effect=health_results))
    monkeypatch.setattr(watchdog.time, "time", Mock(return_value=1000.0))
    monkeypatch.setattr(watchdog.time, "sleep", Mock())
    restart = Mock(return_value=True)
    monkeypatch.setattr(watchdog, "_restart_service", restart)
    return state_path, restart


def test_watchdog_requires_consecutive_failures_and_resets_after_health(monkeypatch, tmp_path):
    unhealthy = (False, "timed out")
    state_path, restart = _configure_watchdog(
        monkeypatch, tmp_path, [unhealthy, unhealthy, (True, "ok"), unhealthy],
    )

    assert [watchdog.main() for _ in range(4)] == [1, 1, 0, 1]
    restart.assert_not_called()
    assert json.loads(state_path.read_text())["failures"] == 1


def test_watchdog_restart_cooldown_survives_invocations_and_success(monkeypatch, tmp_path):
    unhealthy = (False, "timed out")
    state_path, restart = _configure_watchdog(
        monkeypatch, tmp_path,
        [unhealthy] * 3 + [(True, "ok"), (True, "ok")] + [unhealthy] * 4 + [(True, "ok")],
    )

    assert [watchdog.main() for _ in range(4)] == [1, 1, 0, 0]
    assert [watchdog.main() for _ in range(3)] == [1, 1, 1]
    restart.assert_called_once_with("gui/501/app.funba.web")
    assert json.loads(state_path.read_text())["last_restart_at"] == 1000.0

    watchdog.time.time.return_value = 1301.0
    assert watchdog.main() == 0
    assert restart.call_count == 2


def test_watchdog_failed_restart_also_backs_off(monkeypatch, tmp_path):
    _, restart = _configure_watchdog(monkeypatch, tmp_path, [(False, "timed out")] * 6)
    restart.return_value = False

    assert [watchdog.main() for _ in range(6)] == [1] * 6
    restart.assert_called_once()


@pytest.mark.parametrize("contents", ["not json", "[]", '{"target": "gui/501/app.funba.web", "failures": []}'])
def test_watchdog_never_restarts_with_invalid_durable_state(monkeypatch, tmp_path, contents):
    state_path, restart = _configure_watchdog(monkeypatch, tmp_path, [(False, "timed out")])
    state_path.write_text(contents)

    assert watchdog.main() == 1
    restart.assert_not_called()


def test_watchdog_does_not_overlap_locked_invocation(monkeypatch, tmp_path):
    state_path, restart = _configure_watchdog(monkeypatch, tmp_path, [(False, "timed out")])
    with state_path.with_name(f"{state_path.name}.lock").open("a") as lock:
        watchdog.fcntl.flock(lock.fileno(), watchdog.fcntl.LOCK_EX | watchdog.fcntl.LOCK_NB)
        assert watchdog.main() == 0

    watchdog._app_ok.assert_not_called()
    restart.assert_not_called()


def _watchdog_managed_restart_mocks(
    monkeypatch, *, uid=501, parent=1, keepalive=True, changed_pid=False,
    command="/Users/test/Library/funba/venv/bin/python -m gunicorn web.app:app",
):
    def launchd_output(pid):
        return f"\tpid = {pid}\n\tproperties = {'keepalive | ' if keepalive else ''}runatload\n"

    responses = [
        subprocess.CompletedProcess([], 0, launchd_output(123)),
        subprocess.CompletedProcess([], 0, f"{uid} {parent} {command}"),
        subprocess.CompletedProcess([], 0, launchd_output(456 if changed_pid else 123)),
    ]
    run = Mock(side_effect=responses)
    kill = Mock()
    monkeypatch.setattr(watchdog, "_run", run)
    monkeypatch.setattr(watchdog.os, "geteuid", lambda: 501)
    monkeypatch.setattr(watchdog.os, "kill", kill)
    return run, kill


def test_watchdog_unprivileged_recovery_signals_only_verified_managed_master(monkeypatch):
    run, kill = _watchdog_managed_restart_mocks(monkeypatch)

    assert watchdog._restart_service("system/app.funba.web") is True
    kill.assert_called_once_with(123, signal.SIGTERM)
    assert [call.args[0][1] for call in run.call_args_list] == ["print", "-p", "print"]


@pytest.mark.parametrize("overrides", [
    {"uid": 502},
    {"parent": 123},
    {"keepalive": False},
    {"changed_pid": True},
    {"command": "/Users/test/Library/funba/venv/bin/python -m web.app"},
    {"command": "/other/venv/bin/python -m gunicorn other.app:app"},
])
def test_watchdog_never_signals_unowned_worker_unmanaged_or_changed_process(monkeypatch, overrides):
    run, kill = _watchdog_managed_restart_mocks(monkeypatch, **overrides)

    assert watchdog._restart_service("system/app.funba.web") is False
    kill.assert_not_called()
    assert all(call.args[0][1] != "kickstart" for call in run.call_args_list)


def test_watchdog_handles_signal_permission_failure_without_kickstart(monkeypatch):
    run, kill = _watchdog_managed_restart_mocks(monkeypatch)
    kill.side_effect = PermissionError("not permitted")

    assert watchdog._restart_service("system/app.funba.web") is False
    kill.assert_called_once_with(123, signal.SIGTERM)
    assert all(call.args[0][1] != "kickstart" for call in run.call_args_list)


def test_watchdog_privileged_recovery_uses_launchd_instead_of_signals(monkeypatch):
    run = Mock(return_value=subprocess.CompletedProcess([], 0, "", ""))
    kill = Mock()
    monkeypatch.setattr(watchdog, "_run", run)
    monkeypatch.setattr(watchdog.os, "geteuid", lambda: 0)
    monkeypatch.setattr(watchdog.os, "kill", kill)

    assert watchdog._restart_service("system/app.funba.web") is True
    run.assert_called_once_with(["/bin/launchctl", "kickstart", "-k", "system/app.funba.web"], timeout=15.0)
    kill.assert_not_called()


def test_watchdog_handles_launchctl_timeout(monkeypatch):
    monkeypatch.setattr(watchdog, "_run", Mock(side_effect=subprocess.TimeoutExpired("launchctl", 15)))

    assert watchdog._kickstart("gui/501/app.funba.web") is False
