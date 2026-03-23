from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import tasks.dispatch as dispatch
import tasks.metrics as metrics_tasks


def _ctx(session):
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


def test_cmd_metric_backfill_creates_runs_and_skips_active_metrics():
    args = SimpleNamespace(
        metric="clutch_fg_pct",
        season="22025",
        date_from=None,
        date_to=None,
        force=False,
    )
    created_run = SimpleNamespace(id="run-1")
    active_run = SimpleNamespace(id="run-2")

    with patch.object(dispatch, "_query_games", return_value=["g1", "g2"]), \
         patch("metrics.framework.runtime.get_all_metrics", return_value=[SimpleNamespace(key="clutch_fg_pct")]), \
         patch("metrics.framework.runtime.expand_metric_keys", return_value=["clutch_fg_pct", "clutch_fg_pct_career"]), \
         patch.object(dispatch, "_create_metric_compute_run", side_effect=[(created_run, True), (active_run, False)]), \
         patch.object(metrics_tasks.compute_game_delta, "apply_async") as apply_async, \
         patch("builtins.print") as print_mock:
        dispatch.cmd_metric_backfill(args)

    assert apply_async.call_count == 2
    apply_async.assert_any_call(args=["g1", "clutch_fg_pct"], queue="metrics")
    apply_async.assert_any_call(args=["g2", "clutch_fg_pct"], queue="metrics")
    printed = "\n".join(" ".join(str(a) for a in call.args) for call in print_mock.call_args_list)
    assert "for 1 compute run(s)" in printed
    assert "clutch_fg_pct_career (run-2)" in printed


def test_compute_game_delta_skips_inline_reduce_when_mapping_run_exists():
    session_a = _ctx(MagicMock())
    session_b = _ctx(MagicMock())
    session_c = _ctx(MagicMock())

    metrics_tasks.compute_game_delta.push_request(id="worker-1")
    try:
        with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session_a, session_b, session_c])), \
             patch.object(metrics_tasks, "_try_claim", return_value=(True, None)), \
             patch.object(metrics_tasks, "run_delta_only", return_value=True), \
             patch.object(metrics_tasks, "_mark_done") as mark_done, \
             patch.object(metrics_tasks, "_has_mapping_compute_run", return_value=True), \
             patch.object(metrics_tasks.compute_game_delta, "retry", side_effect=AssertionError("retry not expected")), \
             patch.object(metrics_tasks, "_maybe_trigger_reduce") as maybe_trigger:
            result = metrics_tasks.compute_game_delta.run("g1", "metric_a")
    finally:
        metrics_tasks.compute_game_delta.pop_request()

    mark_done.assert_called_once_with(session_c, "g1", "metric_a")
    maybe_trigger.assert_not_called()
    assert result["reduce_triggered"] == []


def test_sweeper_promotes_completed_mapping_run_once():
    run = SimpleNamespace(id="run-1", target_game_count=3)
    query = MagicMock()
    query.filter.return_value.order_by.return_value.all.return_value = [run]
    session = _ctx(MagicMock())
    session.query.return_value = query

    with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
         patch.object(metrics_tasks, "_done_claim_count_for_run", return_value=3), \
         patch.object(metrics_tasks, "_promote_run_to_reducing", return_value=True) as promote, \
         patch.object(metrics_tasks.sweep_metric_compute_runs_task, "retry", side_effect=AssertionError("retry not expected")), \
         patch.object(metrics_tasks.reduce_metric_compute_run_task, "delay") as delay:
        result = metrics_tasks.sweep_metric_compute_runs_task.run()

    promote.assert_called_once_with(session, "run-1")
    delay.assert_called_once_with("run-1")
    assert result == {"checked_runs": 1, "promoted_runs": ["run-1"]}
