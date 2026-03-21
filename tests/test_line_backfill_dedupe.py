from pathlib import Path
import sys
from types import SimpleNamespace
from unittest.mock import call, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import tasks.dispatch as dispatch


def test_cmd_line_backfill_enqueues_only_missing_games():
    args = SimpleNamespace(season="22025", date_from=None, date_to=None)
    line_score_q = object()

    with patch.object(dispatch, "_query_games_missing_line_score", return_value=["g1", "g2"]), \
         patch.object(dispatch, "_queue", return_value=line_score_q), \
         patch.object(dispatch.backfill_game_line_score, "apply_async") as apply_async:
        dispatch.cmd_line_backfill(args)

    assert apply_async.call_args_list == [
        call(args=["g1"], queue="line_score", declare=[line_score_q]),
        call(args=["g2"], queue="line_score", declare=[line_score_q]),
    ]
