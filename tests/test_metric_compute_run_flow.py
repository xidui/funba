from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import OperationalError as SAOperationalError


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import tasks.dispatch as dispatch
import tasks.metrics as metrics_tasks


def _ctx(session):
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


def _deadlock_error():
    orig = Exception("Deadlock found when trying to get lock; try restarting transaction")
    orig.args = (1213, "Deadlock found when trying to get lock; try restarting transaction")
    return SAOperationalError("SELECT 1", None, orig)


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
         patch("tasks.metrics.create_metric_compute_run", side_effect=[(created_run, True), (active_run, False)]), \
         patch("celery.chord") as chord_mock, \
         patch("builtins.print") as print_mock:
        dispatch.cmd_metric_backfill(args)

    # chord should be called once (for clutch_fg_pct; career skipped as active)
    assert chord_mock.call_count == 1
    map_tasks = chord_mock.call_args[0][0]
    assert len(map_tasks) == 2  # 2 games
    printed = "\n".join(" ".join(str(a) for a in call.args) for call in print_mock.call_args_list)
    assert "for 1 compute run(s)" in printed
    assert "clutch_fg_pct_career (run-2)" in printed


def test_compute_game_delta_computes_when_not_already_done():
    session_a = _ctx(MagicMock())
    session_b = _ctx(MagicMock())
    # _is_already_computed returns False
    session_a.query.return_value.filter.return_value.first.return_value = None

    metrics_tasks.compute_game_delta.push_request(id="worker-1")
    try:
        with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session_a, session_b])), \
             patch.object(metrics_tasks, "run_delta_only", return_value=True), \
             patch.object(metrics_tasks.compute_game_delta, "retry", side_effect=AssertionError("retry not expected")):
            result = metrics_tasks.compute_game_delta.run("g1", "metric_a")
    finally:
        metrics_tasks.compute_game_delta.pop_request()

    assert result["produced"] is True


def test_sweeper_promotes_stuck_mapping_run_after_timeout():
    from datetime import datetime, timedelta

    # Run created 3 hours ago — beyond _CHORD_FALLBACK_SECONDS (2h)
    run = SimpleNamespace(id="run-1", metric_key="metric-a", target_game_count=3,
                          created_at=datetime.utcnow() - timedelta(hours=3))
    mapping_query = MagicMock()
    mapping_query.filter.return_value.order_by.return_value.all.return_value = [run]
    reducing_query = MagicMock()
    reducing_query.filter.return_value.order_by.return_value.all.return_value = []
    active_reducing_keys_query = MagicMock()
    active_reducing_keys_query.filter.return_value.all.return_value = []
    # MetricRunLog count query — return 3 (all games done)
    log_count_query = MagicMock()
    log_count_query.filter.return_value.scalar.return_value = 3
    session = _ctx(MagicMock())
    session.query.side_effect = [reducing_query, active_reducing_keys_query, mapping_query, log_count_query]

    with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
         patch.object(metrics_tasks, "_promote_run_to_reducing", return_value=True) as promote, \
         patch.object(metrics_tasks, "_finalize_reducing_run_if_complete", return_value=False) as finalize, \
         patch.object(metrics_tasks, "_requeue_stale_reducing_run", return_value=False) as requeue, \
         patch.object(metrics_tasks.sweep_metric_compute_runs_task, "retry", side_effect=AssertionError("retry not expected")), \
         patch.object(metrics_tasks.reduce_metric_compute_run_task, "delay") as delay:
        result = metrics_tasks.sweep_metric_compute_runs_task.run()

    promote.assert_called_once_with(session, "run-1")
    finalize.assert_not_called()
    requeue.assert_not_called()
    delay.assert_called_once_with("run-1")
    assert result == {
        "checked_runs": 1,
        "promoted_runs": ["run-1"],
        "finalized_runs": [],
        "requeued_runs": [],
    }


def test_sweeper_finalizes_completed_reducing_run():
    run = SimpleNamespace(id="run-1", metric_key="metric-a", target_game_count=3)
    mapping_query = MagicMock()
    mapping_query.filter.return_value.order_by.return_value.all.return_value = []
    reducing_query = MagicMock()
    reducing_query.filter.return_value.order_by.return_value.all.return_value = [run]
    active_reducing_keys_query = MagicMock()
    active_reducing_keys_query.filter.return_value.all.return_value = []
    session = _ctx(MagicMock())
    session.query.side_effect = [reducing_query, active_reducing_keys_query, mapping_query]

    with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
         patch.object(metrics_tasks, "_finalize_reducing_run_if_complete", return_value=True) as finalize, \
         patch.object(metrics_tasks, "_requeue_stale_reducing_run", return_value=False) as requeue, \
         patch.object(metrics_tasks.sweep_metric_compute_runs_task, "retry", side_effect=AssertionError("retry not expected")), \
         patch.object(metrics_tasks.reduce_metric_compute_run_task, "delay") as delay:
        result = metrics_tasks.sweep_metric_compute_runs_task.run()

    finalize.assert_called_once_with(session, run)
    requeue.assert_not_called()
    delay.assert_not_called()
    assert result == {
        "checked_runs": 0,
        "promoted_runs": [],
        "finalized_runs": ["run-1"],
        "requeued_runs": [],
    }


def test_sweeper_requeues_stale_reducing_run():
    run = SimpleNamespace(id="run-1", metric_key="metric-a", target_game_count=3)
    reducing_query = MagicMock()
    reducing_query.filter.return_value.order_by.return_value.all.return_value = [run]
    active_reducing_keys_query = MagicMock()
    active_reducing_keys_query.filter.return_value.all.return_value = []
    mapping_query = MagicMock()
    mapping_query.filter.return_value.order_by.return_value.all.return_value = []
    session = _ctx(MagicMock())
    session.query.side_effect = [reducing_query, active_reducing_keys_query, mapping_query]

    with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
         patch.object(metrics_tasks, "_finalize_reducing_run_if_complete", return_value=False) as finalize, \
         patch.object(metrics_tasks, "_requeue_stale_reducing_run", return_value=True) as requeue, \
         patch.object(metrics_tasks.sweep_metric_compute_runs_task, "retry", side_effect=AssertionError("retry not expected")), \
         patch.object(metrics_tasks.reduce_metric_compute_run_task, "delay") as delay:
        result = metrics_tasks.sweep_metric_compute_runs_task.run()

    finalize.assert_called_once_with(session, run)
    requeue.assert_called_once()
    delay.assert_not_called()
    assert result == {
        "checked_runs": 0,
        "promoted_runs": [],
        "finalized_runs": [],
        "requeued_runs": ["run-1"],
    }


def test_sweeper_does_not_promote_mapping_runs_while_reducing_backlog_exists():
    reducing_run = SimpleNamespace(id="run-r", metric_key="metric-a", target_game_count=3)
    mapping_run = SimpleNamespace(id="run-m", metric_key="metric-a", target_game_count=3)
    reducing_query = MagicMock()
    reducing_query.filter.return_value.order_by.return_value.all.return_value = [reducing_run]
    active_reducing_keys_query = MagicMock()
    active_reducing_keys_query.filter.return_value.all.return_value = [("metric-a",)]
    mapping_query = MagicMock()
    mapping_query.filter.return_value.order_by.return_value.all.return_value = [mapping_run]
    session = _ctx(MagicMock())
    session.query.side_effect = [reducing_query, active_reducing_keys_query, mapping_query]

    with patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
         patch.object(metrics_tasks, "_finalize_reducing_run_if_complete", return_value=False) as finalize, \
         patch.object(metrics_tasks, "_requeue_stale_reducing_run", return_value=False) as requeue, \
         patch.object(metrics_tasks.sweep_metric_compute_runs_task, "retry", side_effect=AssertionError("retry not expected")), \
         patch.object(metrics_tasks.reduce_metric_compute_run_task, "delay") as delay:
        result = metrics_tasks.sweep_metric_compute_runs_task.run()

    finalize.assert_called_once_with(session, reducing_run)
    requeue.assert_called_once()
    delay.assert_not_called()
    assert result == {
        "checked_runs": 1,
        "promoted_runs": [],
        "finalized_runs": [],
        "requeued_runs": [],
    }


def test_compute_season_metric_retries_deadlock_with_extended_budget():
    deadlock_exc = _deadlock_error()

    metrics_tasks.compute_season_metric_task.push_request(id="worker-1", retries=0)
    try:
        with patch.object(metrics_tasks, "_reduce_locked_session_factory", side_effect=deadlock_exc), \
             patch.object(metrics_tasks.compute_season_metric_task, "retry", side_effect=RuntimeError("retrying")) as retry_mock:
            try:
                metrics_tasks.compute_season_metric_task.run("bench_high_score", "22025", run_id="run-1")
            except RuntimeError as exc:
                assert str(exc) == "retrying"
    finally:
        metrics_tasks.compute_season_metric_task.pop_request()

    assert retry_mock.call_count == 1
    assert retry_mock.call_args.kwargs["max_retries"] == 5
    assert retry_mock.call_args.kwargs["countdown"] >= 15


def test_compute_season_metric_marks_run_failed_after_deadlock_retries_exhausted():
    deadlock_exc = _deadlock_error()
    session = _ctx(MagicMock())

    metrics_tasks.compute_season_metric_task.push_request(id="worker-1", retries=5)
    try:
        with patch.object(metrics_tasks, "_reduce_locked_session_factory", side_effect=deadlock_exc), \
             patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
             patch.object(metrics_tasks, "_mark_run_failed") as mark_failed, \
             patch.object(metrics_tasks.compute_season_metric_task, "retry", side_effect=AssertionError("retry not expected")):
            try:
                metrics_tasks.compute_season_metric_task.run("bench_high_score", "22025", run_id="run-1")
            except SAOperationalError:
                pass
    finally:
        metrics_tasks.compute_season_metric_task.pop_request()

    mark_failed.assert_called_once()
    assert mark_failed.call_args.args[1] == "run-1"
    assert "22025" in mark_failed.call_args.args[2]
    assert "deadlock retries" in mark_failed.call_args.args[2]


def test_compute_season_metric_marks_run_failed_on_terminal_non_deadlock_error():
    session = _ctx(MagicMock())

    metrics_tasks.compute_season_metric_task.push_request(id="worker-1", retries=2)
    try:
        with patch.object(metrics_tasks, "_reduce_locked_session_factory", side_effect=ValueError("boom")), \
             patch.object(metrics_tasks, "_session_factory", return_value=MagicMock(side_effect=[session])), \
             patch.object(metrics_tasks, "_mark_run_failed") as mark_failed, \
             patch.object(metrics_tasks.compute_season_metric_task, "retry", side_effect=AssertionError("retry not expected")):
            try:
                metrics_tasks.compute_season_metric_task.run("bench_high_score", "22025", run_id="run-1")
            except ValueError:
                pass
    finally:
        metrics_tasks.compute_season_metric_task.pop_request()

    mark_failed.assert_called_once()
    assert mark_failed.call_args.args[1] == "run-1"
    assert "season 22025 failed" in mark_failed.call_args.args[2]
