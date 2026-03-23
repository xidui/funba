from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


from metrics.framework.runner import _flush_run_logs, _log_run


def test_log_run_builds_serializable_row():
    row = _log_run(
        "game-1",
        "metric_a",
        "player",
        "player-1",
        "22025",
        {"ast": 5, "tov": 2},
        True,
        qualified=True,
    )

    assert row["game_id"] == "game-1"
    assert row["metric_key"] == "metric_a"
    assert row["entity_type"] == "player"
    assert row["entity_id"] == "player-1"
    assert row["season"] == "22025"
    assert row["produced_result"] is True
    assert row["qualified"] is True
    assert row["delta_json"] == '{"ast": 5, "tov": 2}'
    assert row["computed_at"] is not None


def test_flush_run_logs_noops_for_empty_batch():
    class DummySession:
        def __init__(self):
            self.calls = 0

        def execute(self, stmt):
            self.calls += 1

    sess = DummySession()
    _flush_run_logs(sess, [])
    assert sess.calls == 0
