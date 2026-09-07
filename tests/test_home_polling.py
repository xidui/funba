"""Exercise the rendered homepage poller with a deterministic browser clock."""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

from jinja2 import Environment
import pytest


NODE = shutil.which("node")
pytestmark = pytest.mark.skipif(NODE is None, reason="Node.js is required for browser-script tests")
TEMPLATE = Path(__file__).resolve().parents[1] / "web/templates/home.html"

HARNESS = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
const input = JSON.parse(require('node:fs').readFileSync(0, 'utf8'));
let now = 1_800_000_000_000;
let serial = 0;
let reloads = 0;
let calls = 0;
const timers = new Map();
const listeners = new Map();
const response = (games = [], status = 200, retryAfter = null) => ({
  ok: status === 200, status,
  headers: { get: () => retryAfter },
  json: async () => ({ games }),
});
let fetchResult = async () => response();
const document = {
  hidden: !!input.hidden,
  addEventListener: (name, callback) => listeners.set(name, callback),
  querySelector: () => null,
};
const window = {
  setTimeout(callback, delay) {
    const id = ++serial;
    timers.set(id, { callback, due: now + delay });
    return id;
  },
  clearTimeout: id => timers.delete(id),
  location: { reload: () => reloads++ },
};
class FakeDate extends Date { static now() { return now; } }
function nextDelay() {
  assert.equal(timers.size, 1);
  return [...timers.values()][0].due - now;
}
function fireNext() {
  assert.equal(timers.size, 1);
  const [id, timer] = [...timers.entries()][0];
  timers.delete(id);
  now = Math.max(now, timer.due);
  return timer.callback();
}
function show(visible) {
  document.hidden = !visible;
  listeners.get('visibilitychange')?.();
}
vm.runInNewContext(input.source, {
  window, document, Date: FakeDate,
  fetch: () => { calls++; return fetchResult(); },
});
(async () => { await eval('(async () => {\n' + input.scenario + '\n})()'); })()
  .catch(error => { console.error(error); process.exitCode = 1; });
"""


def run_poller(games, scenario, *, hidden=False):
    source = TEMPLATE.read_text().split('<script id="home-live-scores">', 1)[1].split("</script>", 1)[0]
    source = Environment().from_string(source).render(
        today_games=games,
        url_for=lambda endpoint: "/api/games/live",
    )
    result = subprocess.run(
        [NODE, "-e", HARNESS],
        input=json.dumps({"source": source, "scenario": scenario, "hidden": hidden}),
        text=True,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("games", [[], [{"game_id": "finished", "status": "completed"}]])
def test_idle_homepage_never_polls(games):
    run_poller(games, """
      assert.equal(timers.size, 0);
      show(false); show(true);
      assert.equal(timers.size, 0);
      assert.equal(calls, 0);
    """)


@pytest.mark.parametrize("status,delay", [("live", 30000), ("upcoming", 60000)])
def test_polling_cadence_matches_game_status(status, delay):
    run_poller([{"game_id": "game", "status": status}], f"""
      assert.equal(nextDelay(), {delay});
      await fireNext();
      assert.equal(calls, 1);
      assert.equal(nextDelay(), {delay});
    """)


def test_hidden_tabs_pause_and_resume_without_focus_request_bursts():
    run_poller([{"game_id": "game", "status": "live"}], """
      assert.equal(timers.size, 0);
      show(true);
      assert.equal(nextDelay(), 30000);
      now += 10000;
      show(false);
      assert.equal(timers.size, 0);
      show(true);
      assert.equal(nextDelay(), 20000);
      show(false);
      now += 60000;
      show(true);
      assert.equal(nextDelay(), 0);
      await fireNext();
      show(false); show(true);
      assert.equal(nextDelay(), 30000);
      assert.equal(calls, 1);
    """, hidden=True)


def test_visibility_changes_do_not_overlap_an_inflight_request():
    run_poller([{"game_id": "game", "status": "live"}], """
      let finish;
      fetchResult = () => new Promise(resolve => { finish = resolve; });
      const pending = fireNext();
      assert.equal(calls, 1);
      show(false); now += 60000; show(true);
      assert.equal(timers.size, 0);
      assert.equal(calls, 1);
      finish(response());
      await pending;
      assert.equal(nextDelay(), 30000);
    """)


@pytest.mark.parametrize("status,next_status", [("upcoming", "live"), ("live", "completed")])
def test_status_transitions_reload_once_even_without_a_visible_card(status, next_status):
    run_poller([{"game_id": "game", "status": status}], f"""
      fetchResult = async () => response([{{game_id: 'game', status: '{next_status}'}}]);
      await fireNext();
      assert.equal(reloads, 1);
      assert.equal(timers.size, 0);
      show(false); show(true);
      assert.equal(timers.size, 0);
    """)


def test_a_request_finishing_in_background_does_not_reload_the_hidden_page():
    run_poller([{"game_id": "game", "status": "live"}], """
      let finish;
      fetchResult = () => new Promise(resolve => { finish = resolve; });
      const pending = fireNext();
      show(false);
      finish(response([{game_id: 'game', status: 'completed'}]));
      await pending;
      assert.equal(reloads, 0);
      assert.equal(timers.size, 0);
      now += 60000;
      show(true);
      fetchResult = async () => response([{game_id: 'game', status: 'completed'}]);
      await fireNext();
      assert.equal(reloads, 1);
    """)


def test_untracked_game_statuses_do_not_reload_the_page():
    run_poller([{"game_id": "tracked", "status": "live"}], """
      fetchResult = async () => response([
        {game_id: 'other', status: 'completed'},
        {game_id: 'tracked', status: 'live'},
      ]);
      await fireNext();
      assert.equal(reloads, 0);
      assert.equal(nextDelay(), 30000);
    """)


def test_game_ids_match_across_numeric_and_string_representations():
    run_poller([{"game_id": 123, "status": "upcoming"}], """
      fetchResult = async () => response([{game_id: '123', status: 'live'}]);
      await fireNext();
      assert.equal(reloads, 1);
      assert.equal(timers.size, 0);
    """)


def test_rate_limit_retry_after_is_honored_and_success_restores_normal_cadence():
    run_poller([{"game_id": "game", "status": "live"}], """
      fetchResult = async () => response([], 429, '600');
      await fireNext();
      assert.equal(nextDelay(), 600000);
      show(false); show(true);
      assert.equal(nextDelay(), 600000);
      fetchResult = async () => response();
      await fireNext();
      assert.equal(nextDelay(), 30000);
    """)


def test_retry_after_accepts_an_http_date():
    run_poller([{"game_id": "game", "status": "live"}], """
      fetchResult = async () => response([], 503, new Date(now + 180000).toUTCString());
      await fireNext();
      assert.equal(nextDelay(), 180000);
    """)


def test_network_and_json_errors_back_off_without_stopping_recovery():
    run_poller([{"game_id": "game", "status": "live"}], """
      fetchResult = async () => { throw new Error('offline'); };
      await fireNext();
      assert.equal(nextDelay(), 60000);
      fetchResult = async () => ({ ...response(), json: async () => { throw new Error('invalid JSON'); } });
      await fireNext();
      assert.equal(nextDelay(), 120000);
      await fireNext();
      assert.equal(nextDelay(), 240000);
      await fireNext();
      assert.equal(nextDelay(), 300000);
    """)
