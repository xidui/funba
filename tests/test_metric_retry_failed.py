from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import tasks.dispatch as dispatch


def test_cmd_metric_retry_failed_requeues_runs_and_resets_status():
    args = SimpleNamespace(metric=None)
    run1 = SimpleNamespace(id="run-1")
    run2 = SimpleNamespace(id="run-2")

    session = MagicMock()
    query = MagicMock()
    session.query.return_value = query
    query.filter.return_value = query
    query.order_by.return_value.all.return_value = [run1, run2]
    query.update.return_value = 2

    with patch.object(dispatch, "_session", return_value=session), \
         patch("tasks.metrics.reduce_metric_compute_run_task.delay") as delay, \
         patch("builtins.print") as print_mock:
        dispatch.cmd_metric_retry_failed(args)

    query.update.assert_called_once()
    session.commit.assert_called_once()
    delay.assert_any_call("run-1")
    delay.assert_any_call("run-2")
    printed = "\n".join(" ".join(str(a) for a in call.args) for call in print_mock.call_args_list)
    assert "Re-enqueued 2 failed compute run(s)" in printed
