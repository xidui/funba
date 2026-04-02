from datetime import date
from pathlib import Path
import sys
from unittest.mock import MagicMock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


import tasks.content as content_tasks
import tasks.dispatch as dispatch_tasks
import tasks.ingest as ingest_tasks
import tasks.metrics as metrics_tasks


def _ctx(session: MagicMock) -> MagicMock:
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    return session


def test_ensure_daily_content_analysis_issue_waits_for_artifacts():
    with patch.object(
        content_tasks,
        "_pipeline_status_for_date",
        return_value={
            "game_ids": ["0021", "0022"],
            "artifacts_ready": False,
            "pending_game_ids": ["0022"],
        },
    ), patch.object(content_tasks, "_all_games_have_metrics") as metrics_mock, patch.object(
        content_tasks,
        "load_paperclip_bridge_config",
    ) as cfg_mock:
        result = content_tasks.ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

    assert result == {
        "ok": False,
        "status": "waiting_for_pipeline",
        "pipeline_stage": "artifacts",
        "source_date": "2026-03-29",
        "game_ids": ["0021", "0022"],
        "pending_game_ids": ["0022"],
    }
    metrics_mock.assert_not_called()
    cfg_mock.assert_not_called()


def test_ingest_recent_games_enqueues_only_incomplete_games():
    with patch.object(
        ingest_tasks,
        "_recent_target_dates",
        return_value=[date.fromisoformat("2026-04-01"), date.fromisoformat("2026-03-31")],
    ), patch.object(
        ingest_tasks,
        "_discover_game_ids_for_date",
        side_effect=[["g1", "g2"], ["g2", "g3"]],
    ), patch.object(
        ingest_tasks,
        "_list_incomplete_game_ids",
        return_value=["g2", "g3"],
    ), patch("celery.chord") as chord_mock, patch(
        "tasks.metrics.refresh_current_season_metrics",
    ):
        result = ingest_tasks.ingest_recent_games.run(lookback_days=2)

    assert chord_mock.call_count == 1
    map_tasks = chord_mock.call_args.args[0]
    assert len(map_tasks) == 2
    assert result == {
        "lookback_days": 2,
        "discovered": 3,
        "enqueued": 2,
        "dates": ["2026-03-31", "2026-04-01"],
        "game_ids": ["g2", "g3"],
    }


def test_ingest_game_retries_when_artifacts_still_incomplete():
    before_status = {
        "exists_game": True,
        "artifacts_supported": True,
        "has_detail": False,
        "has_pbp": False,
        "has_shot": True,
        "season": "22025",
    }
    after_status = {
        "exists_game": True,
        "artifacts_supported": True,
        "has_detail": False,
        "has_pbp": False,
        "has_shot": True,
        "season": "22025",
    }

    status_session = _ctx(MagicMock())
    process_session = _ctx(MagicMock())
    final_status_session = _ctx(MagicMock())

    ingest_tasks.ingest_game.push_request(id="worker-1", retries=0)
    try:
        with patch.object(
            ingest_tasks,
            "_session_factory",
            return_value=MagicMock(side_effect=[status_session, process_session, final_status_session]),
        ), patch.object(
            ingest_tasks,
            "_load_game_artifact_status",
            side_effect=[before_status, after_status],
        ), patch.object(
            ingest_tasks,
            "_fetch_api_row",
            return_value={"GAME_ID": "g1", "SEASON_ID": "22025", "GAME_DATE": "2026-03-29", "MATCHUP": "LAL vs. BOS"},
        ), patch.object(
            ingest_tasks,
            "process_and_store_game",
        ) as process_mock, patch.object(
            ingest_tasks.ingest_game,
            "retry",
            side_effect=RuntimeError("retrying"),
        ) as retry_mock:
            try:
                ingest_tasks.ingest_game.run("g1")
            except RuntimeError as exc:
                assert str(exc) == "retrying"
    finally:
        ingest_tasks.ingest_game.pop_request()

    process_mock.assert_called_once()
    retry_mock.assert_called_once()
    assert "Artifacts not ready" in str(retry_mock.call_args.kwargs["exc"])


def test_compute_season_metric_queues_recent_content_check():
    session = _ctx(MagicMock())
    session_factory = MagicMock(return_value=session)
    outer_ctx = _ctx(session_factory)

    with patch.object(
        metrics_tasks,
        "_reduce_locked_session_factory",
        return_value=outer_ctx,
    ), patch.object(
        metrics_tasks,
        "run_season_metric",
        return_value=7,
    ), patch.object(
        metrics_tasks,
        "_increment_compute_run_progress",
    ) as progress_mock, patch(
        "tasks.content.ensure_recent_content_analysis_for_season_task.delay",
    ) as delay_mock:
        result = metrics_tasks.compute_season_metric_task.run("metric_a", "22025", run_id="run-1")

    progress_mock.assert_called_once_with("run-1")
    delay_mock.assert_called_once_with("22025")
    assert result == {"metric_key": "metric_a", "season": "22025", "results_written": 7}


def test_sync_schedule_games_enables_unplayed_upsert_mode():
    with patch.object(dispatch_tasks, "discover_and_insert_games", return_value={"g1"}) as discover_mock:
        result = dispatch_tasks.sync_schedule_games(date_from="04/01/2026", date_to="05/01/2026")

    discover_mock.assert_called_once_with(
        season=None,
        season_types=None,
        date_from="04/01/2026",
        date_to="05/01/2026",
        include_unplayed=True,
        upsert_existing=True,
    )
    assert result == {"g1"}


def test_sync_schedule_window_syncs_expected_date_range():
    class _FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 1)

    with patch.object(ingest_tasks, "date", _FakeDate), patch(
        "tasks.dispatch.sync_schedule_games",
        return_value={"g1", "g2"},
    ) as sync_mock:
        result = ingest_tasks.sync_schedule_window.run(
            lookback_days=2,
            lookahead_days=5,
            season_types=["PlayIn", "Playoffs"],
        )

    sync_mock.assert_called_once_with(
        date_from="03/30/2026",
        date_to="04/06/2026",
        season_types=["PlayIn", "Playoffs"],
    )
    assert result == {
        "date_from": "03/30/2026",
        "date_to": "04/06/2026",
        "season_types": ["PlayIn", "Playoffs"],
        "synced_games": 2,
    }
