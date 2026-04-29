from datetime import date
from pathlib import Path
import sys
from types import SimpleNamespace
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
    with patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0021", "0022"],
            "ready_game_ids": [],
            "pending_artifact_game_ids": ["0021", "0022"],
            "pending_metric_game_ids": [],
        },
    ), patch(
        "content_pipeline.game_analysis_issues.covered_game_ids_for_date",
        return_value=set(),
    ), patch(
        "content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock",
        return_value=_ctx(MagicMock()),
    ), patch(
        "content_pipeline.game_analysis_issues.load_paperclip_bridge_config",
        return_value=object(),
    ) as cfg_mock, patch("content_pipeline.game_analysis_issues.PaperclipClient") as client_cls:
        client = client_cls.return_value
        client.discover_defaults.return_value = MagicMock(
            project_id="project-1",
            content_analyst_agent_id="agent-1",
        )
        client.list_issues.return_value = []
        result = content_tasks.ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

    assert result == {
        "ok": False,
        "status": "waiting_for_pipeline",
        "source_date": "2026-03-29",
        "game_ids": ["0021", "0022"],
        "results": [
            {
                "ok": False,
                "status": "waiting_for_pipeline",
                "pipeline_stage": "artifacts",
                "source_date": "2026-03-29",
                "game_id": "0021",
            },
            {
                "ok": False,
                "status": "waiting_for_pipeline",
                "pipeline_stage": "artifacts",
                "source_date": "2026-03-29",
                "game_id": "0022",
            },
        ],
        "created_count": 0,
        "existing_count": 0,
        "covered_count": 0,
        "waiting_count": 2,
        "issue_id": None,
        "issue_identifier": None,
    }
    cfg_mock.assert_called_once()
    client.create_issue.assert_not_called()


def test_ensure_daily_content_analysis_issue_waits_for_curator():
    with patch(
        "content_pipeline.game_analysis_issues.game_pipeline_status_for_date",
        return_value={
            "game_ids": ["0021"],
            "ready_game_ids": [],
            "pending_artifact_game_ids": [],
            "pending_metric_game_ids": [],
            "pending_curator_game_ids": ["0021"],
        },
    ), patch(
        "content_pipeline.game_analysis_issues.covered_game_ids_for_date",
        return_value=set(),
    ), patch(
        "content_pipeline.game_analysis_issues._game_analysis_issue_creation_lock",
        return_value=_ctx(MagicMock()),
    ), patch(
        "content_pipeline.game_analysis_issues.load_paperclip_bridge_config",
        return_value=object(),
    ), patch("content_pipeline.game_analysis_issues.PaperclipClient") as client_cls:
        client = client_cls.return_value
        client.discover_defaults.return_value = MagicMock(
            project_id="project-1",
            content_analyst_agent_id="agent-1",
        )
        client.list_issues.return_value = []
        result = content_tasks.ensure_daily_content_analysis_issue(date.fromisoformat("2026-03-29"))

    assert result["ok"] is False
    assert result["status"] == "waiting_for_pipeline"
    assert result["waiting_count"] == 1
    assert result["results"] == [
        {
            "ok": False,
            "status": "waiting_for_pipeline",
            "pipeline_stage": "curator",
            "source_date": "2026-03-29",
            "game_id": "0021",
        }
    ]
    client.create_issue.assert_not_called()


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
    ), patch.object(ingest_tasks.ingest_game, "apply_async") as apply_async_mock:
        result = ingest_tasks.ingest_recent_games.run(lookback_days=2)

    assert apply_async_mock.call_count == 2
    apply_async_mock.assert_any_call(args=["g2"], kwargs={"force": True})
    apply_async_mock.assert_any_call(args=["g3"], kwargs={"force": True})
    assert result == {
        "lookback_days": 2,
        "discovered": 3,
        "enqueued": 2,
        "dates": ["2026-03-31", "2026-04-01"],
        "game_ids": ["g2", "g3"],
    }


def test_ingest_yesterday_enqueues_each_game_without_chord():
    class _FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 6)

    with patch.object(ingest_tasks, "date", _FakeDate), patch.object(
        ingest_tasks,
        "_discover_game_ids_for_date",
        return_value=["g1", "g2"],
    ), patch.object(ingest_tasks.ingest_game, "apply_async") as apply_async_mock:
        result = ingest_tasks.ingest_yesterday.run()

    assert apply_async_mock.call_count == 2
    apply_async_mock.assert_any_call(args=["g1"])
    apply_async_mock.assert_any_call(args=["g2"])
    assert result == {"date": "2026-04-05", "enqueued": 2}


def test_ingest_game_does_not_enqueue_season_refresh_for_shot_only_backfill():
    before_status = {
        "exists_game": True,
        "game_status": "completed",
        "artifacts_supported": True,
        "has_detail": True,
        "has_pbp": True,
        "has_shot": False,
        "season": "22025",
    }
    after_status = {
        "exists_game": True,
        "game_status": "completed",
        "artifacts_supported": True,
        "has_detail": True,
        "has_pbp": True,
        "has_shot": True,
        "season": "22025",
    }

    status_session = _ctx(MagicMock())
    final_status_session = _ctx(MagicMock())
    zero_score_session = _ctx(MagicMock())
    zero_score_session.query.return_value.filter.return_value.first.return_value = None
    line_score_session = _ctx(MagicMock())
    period_check_session = _ctx(MagicMock())
    shot_backfill_session = _ctx(MagicMock())
    metric_log_session = _ctx(MagicMock())
    metric_log_session.query.return_value.filter.return_value.first.return_value = object()

    ingest_tasks.ingest_game.push_request(id="worker-1", retries=0)
    try:
        with patch.object(
            ingest_tasks,
            "_session_factory",
            return_value=MagicMock(
                side_effect=[
                    status_session,
                    final_status_session,
                    zero_score_session,
                    line_score_session,
                    metric_log_session,
                    period_check_session,
                    shot_backfill_session,
                ]
            ),
        ), patch.object(
            ingest_tasks,
            "_load_game_artifact_status",
            side_effect=[before_status, after_status],
        ), patch.object(
            ingest_tasks,
            "back_fill_game_shot_record",
        ) as shot_mock, patch.object(
            ingest_tasks,
            "has_game_line_score",
            return_value=True,
        ), patch.object(
            ingest_tasks,
            "has_game_period_stats",
            return_value=True,
        ), patch(
            "tasks.metrics.refresh_current_season_metrics.delay",
        ) as refresh_delay_mock:
            result = ingest_tasks.ingest_game.run("g1")
    finally:
        ingest_tasks.ingest_game.pop_request()

    shot_mock.assert_called_once()
    refresh_delay_mock.assert_not_called()
    assert result["game_id"] == "g1"
    assert result["shot_refreshed"] is True


def test_ingest_game_forces_season_refresh_when_metric_logs_are_missing():
    before_status = {
        "exists_game": True,
        "game_status": "completed",
        "artifacts_supported": True,
        "has_detail": True,
        "has_pbp": True,
        "has_shot": False,
        "season": "52025",
    }
    after_status = {
        "exists_game": True,
        "game_status": "completed",
        "artifacts_supported": True,
        "has_detail": True,
        "has_pbp": True,
        "has_shot": True,
        "season": "52025",
    }

    status_session = _ctx(MagicMock())
    final_status_session = _ctx(MagicMock())
    zero_score_session = _ctx(MagicMock())
    zero_score_session.query.return_value.filter.return_value.first.return_value = None
    line_score_session = _ctx(MagicMock())
    period_check_session = _ctx(MagicMock())
    shot_backfill_session = _ctx(MagicMock())
    metric_log_session = _ctx(MagicMock())
    metric_log_session.query.return_value.filter.return_value.first.return_value = None

    ingest_tasks.ingest_game.push_request(id="worker-1", retries=0)
    try:
        with patch.object(
            ingest_tasks,
            "_session_factory",
            return_value=MagicMock(
                side_effect=[
                    status_session,
                    final_status_session,
                    zero_score_session,
                    line_score_session,
                    metric_log_session,
                    period_check_session,
                    shot_backfill_session,
                ]
            ),
        ), patch.object(
            ingest_tasks,
            "_load_game_artifact_status",
            side_effect=[before_status, after_status],
        ), patch.object(
            ingest_tasks,
            "back_fill_game_shot_record",
        ) as shot_mock, patch.object(
            ingest_tasks,
            "has_game_line_score",
            return_value=True,
        ), patch.object(
            ingest_tasks,
            "has_game_period_stats",
            return_value=True,
        ), patch(
            "tasks.metrics.refresh_current_season_metrics.delay",
        ) as refresh_delay_mock:
            result = ingest_tasks.ingest_game.run("g1")
    finally:
        ingest_tasks.ingest_game.pop_request()

    shot_mock.assert_called_once()
    refresh_delay_mock.assert_called_once_with(
        [
            {
                "game_id": "g1",
                "status": "ok",
                "new_game": False,
                "needed_detail_pbp_refresh": False,
                "shot_refreshed": False,
                "line_score_rows": 0,
                "metric_tasks_enqueued": 0,
                "metric_refresh_reason": "missing_metric_run_logs",
            }
        ]
    )
    assert result["game_id"] == "g1"
    assert result["shot_refreshed"] is True


def test_metric_refresh_reason_ignores_non_entity_metric_logs():
    session = MagicMock()

    with patch.object(ingest_tasks, "_has_entity_metric_run_logs", return_value=False), patch.object(
        ingest_tasks,
        "_has_metric_results",
        return_value=True,
    ) as results_mock:
        reason = ingest_tasks._metric_refresh_reason_for_game(
            session,
            "g1",
            needed_detail_pbp_refresh=False,
        )

    assert reason == "missing_metric_run_logs"
    results_mock.assert_not_called()


def test_metric_refresh_reason_requires_metric_results():
    session = MagicMock()

    with patch.object(ingest_tasks, "_has_entity_metric_run_logs", return_value=True), patch.object(
        ingest_tasks,
        "_has_metric_results",
        return_value=False,
    ):
        reason = ingest_tasks._metric_refresh_reason_for_game(
            session,
            "g1",
            needed_detail_pbp_refresh=False,
        )

    assert reason == "missing_metric_results"


def test_ingest_game_retries_when_artifacts_still_incomplete():
    before_status = {
        "exists_game": True,
        "game_status": "completed",
        "artifacts_supported": True,
        "has_detail": False,
        "has_pbp": False,
        "has_shot": True,
        "season": "22025",
    }
    after_status = {
        "exists_game": True,
        "game_status": "completed",
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
    assert "Core artifacts not ready" in str(retry_mock.call_args.kwargs["exc"])


def test_compute_season_metric_waits_for_milestone_chain_before_content_check():
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
    delay_mock.assert_not_called()
    assert result == {"metric_key": "metric_a", "season": "22025", "results_written": 7}


def test_compute_season_metric_waits_for_milestone_chain_before_playin_content_check():
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
        result = metrics_tasks.compute_season_metric_task.run("metric_a", "52025", run_id="run-1")

    progress_mock.assert_called_once_with("run-1")
    delay_mock.assert_not_called()
    assert result == {"metric_key": "metric_a", "season": "52025", "results_written": 7}


def test_compute_season_metric_marks_run_failed_when_runner_raises():
    session = _ctx(MagicMock())
    session_factory = MagicMock(return_value=session)
    outer_ctx = _ctx(session_factory)
    failure_session = _ctx(MagicMock())

    metrics_tasks.compute_season_metric_task.push_request(id="worker-1", retries=2)
    try:
        with patch.object(
            metrics_tasks,
            "_reduce_locked_session_factory",
            return_value=outer_ctx,
        ), patch.object(
            metrics_tasks,
            "run_season_metric",
            side_effect=LookupError("Metric 'east_vs_west_total_record' not found."),
        ), patch.object(
            metrics_tasks,
            "_session_factory",
            return_value=MagicMock(side_effect=[failure_session]),
        ), patch.object(
            metrics_tasks,
            "_mark_run_failed",
        ) as mark_failed, patch.object(
            metrics_tasks,
            "_increment_compute_run_progress",
        ) as progress_mock, patch.object(
            metrics_tasks.compute_season_metric_task,
            "retry",
            side_effect=AssertionError("retry not expected"),
        ):
            try:
                metrics_tasks.compute_season_metric_task.run(
                    "east_vs_west_total_record",
                    "22025",
                    run_id="run-1",
                )
            except LookupError as exc:
                assert "east_vs_west_total_record" in str(exc)
    finally:
        metrics_tasks.compute_season_metric_task.pop_request()

    mark_failed.assert_called_once()
    assert mark_failed.call_args.args[1] == "run-1"
    assert "22025" in mark_failed.call_args.args[2]
    progress_mock.assert_not_called()


def test_detect_milestones_for_games_passes_results_to_completion_callback():
    session = _ctx(MagicMock())

    with patch.object(
        metrics_tasks,
        "_session_factory",
        return_value=MagicMock(return_value=session),
    ), patch(
        "metrics.framework.milestones.detect_batch_incremental",
        return_value=[{"event": "milestone"}],
    ) as detect_mock, patch.object(
        metrics_tasks.milestone_detection_complete_task,
        "delay",
    ) as completion_delay_mock:
        result = metrics_tasks.detect_milestones_for_games_task.run(["g2", "g1", "g1"], metric_keys=["metric_a"])

    detect_mock.assert_called_once_with(session, ["g1", "g2"], metric_keys=["metric_a"])
    session.commit.assert_called_once()
    completion_delay_mock.assert_called_once_with([], ["g1", "g2"])
    assert result == {"games": 2, "events": 1}


def test_refresh_current_season_metrics_respects_metric_season_types():
    session = _ctx(MagicMock())
    season_query = MagicMock()
    season_query.filter.return_value.distinct.return_value.all.return_value = [("22025",), ("42025",), ("52025",)]
    session.query.return_value = season_query
    SessionFactory = MagicMock(return_value=session)

    regular_only = MagicMock(key="salary_per_point", trigger="season", career=False, supports_career=False, season_types=("regular",))
    playoffs_only = MagicMock(key="playoff_points", trigger="season", career=False, supports_career=True, season_types=("playoffs",))

    with patch.object(metrics_tasks, "sessionmaker", return_value=SessionFactory), patch(
        "metrics.framework.runtime.get_all_metrics",
        return_value=[regular_only, playoffs_only],
    ), patch.object(
        metrics_tasks,
        "create_metric_compute_run",
        side_effect=[(SimpleNamespace(id="run-regular"), True), (SimpleNamespace(id="run-playoffs"), True)],
    ), patch.object(metrics_tasks.compute_season_metric_task, "delay") as delay_mock, patch(
        "tasks.metrics.chord",
    ) as chord_mock:
        result = metrics_tasks.refresh_current_season_metrics.run(
            [{"game_id": "g1", "metric_refresh_reason": "needed_detail_pbp_refresh"}]
        )

    assert result == {
        "seasons": ["22025", "42025", "52025"],
        "game_ids": ["g1"],
        "career_buckets": [
            "all_regular",
            "all_playoffs",
            "all_playin",
            "last10_regular",
            "last10_playoffs",
            "last10_playin",
            "last5_regular",
            "last5_playoffs",
            "last5_playin",
            "last3_regular",
            "last3_playoffs",
            "last3_playin",
        ],
        "metrics": 2,
        "enqueued": 2,
        "callbacks": 1,
    }
    delay_mock.assert_any_call("salary_per_point", "22025", run_id="run-regular")
    assert delay_mock.call_count == 1
    chord_mock.assert_called_once()
    season_tasks = chord_mock.call_args.args[0]
    assert len(season_tasks) == 1
    assert season_tasks[0].args == ("playoff_points", "42025")
    assert season_tasks[0].kwargs == {"run_id": "run-playoffs"}
    callback_sig = chord_mock.return_value.call_args.args[0]
    assert callback_sig.kwargs["metric_key"] == "playoff_points"
    assert callback_sig.kwargs["run_id"] == "run-playoffs"
    assert callback_sig.kwargs["buckets"] == ["all_playoffs", "last10_playoffs", "last5_playoffs", "last3_playoffs"]
    assert callback_sig.kwargs["game_ids"] == ["g1"]


def test_sync_schedule_games_enables_unplayed_upsert_mode():
    assert dispatch_tasks._schedule_seasons("22025") == ["2025-26"]
    assert dispatch_tasks._schedule_seasons(date_from="04/01/2026", date_to="05/01/2026") == ["2025-26"]


def test_schedule_game_rows_from_frame_marks_future_games_upcoming_without_scores():
    import pandas as pd

    frame = pd.DataFrame(
        [
            {
                "gameId": "0022501186",
                "gameDate": "04/12/2026 00:00:00",
                "gameStatus": 1,
                "homeTeam_teamId": "1610612738",
                "awayTeam_teamId": "1610612753",
                "homeTeam_score": 0,
                "awayTeam_score": 0,
            },
            {
                "gameId": "0052500101",
                "gameDate": "04/14/2026 00:00:00",
                "gameStatus": 1,
                "homeTeam_teamId": None,
                "awayTeam_teamId": None,
                "homeTeam_score": 0,
                "awayTeam_score": 0,
            },
        ]
    )

    rows = dispatch_tasks._schedule_game_rows_from_frame(
        frame,
        date_from="04/11/2026",
        date_to="04/12/2026",
        season_types=["Regular Season"],
    )

    assert list(rows) == ["0022501186"]
    assert rows["0022501186"] == {
        "game_id": "0022501186",
        "season": "22025",
        "game_date": date.fromisoformat("2026-04-12"),
        "home_team_id": "1610612738",
        "road_team_id": "1610612753",
        "home_team_score": None,
        "road_team_score": None,
        "wining_team_id": None,
        "game_status": "upcoming",
        "backfill_mismatch": False,
        "data_source": "nba_api_scheduleleaguev2",
    }


def test_sync_schedule_window_syncs_expected_date_range():
    class _FakeDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 4, 1)

    with patch.object(ingest_tasks, "date", _FakeDate), patch(
        "tasks.dispatch.sync_schedule_games",
        return_value={"g1", "g2"},
    ) as sync_mock, patch.object(
        ingest_tasks,
        "_patch_today_meta_from_live",
        return_value=(0, set()),
    ), patch.object(
        ingest_tasks,
        "_sweep_game_slugs",
        return_value=0,
    ):
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
        "live_patched": 0,
        "slug_patched": 0,
    }


def test_ingest_game_skips_existing_upcoming_game():
    before_status = {
        "exists_game": True,
        "game_status": "upcoming",
        "artifacts_supported": True,
        "has_detail": False,
        "has_pbp": False,
        "has_shot": False,
        "season": "22025",
    }
    status_session = _ctx(MagicMock())

    ingest_tasks.ingest_game.push_request(id="worker-1", retries=0)
    try:
        with patch.object(
            ingest_tasks,
            "_session_factory",
            return_value=MagicMock(side_effect=[status_session]),
        ), patch.object(
            ingest_tasks,
            "_load_game_artifact_status",
            return_value=before_status,
        ), patch.object(
            ingest_tasks,
            "_fetch_api_row",
        ) as fetch_mock, patch(
            "tasks.metrics.refresh_current_season_metrics.delay",
        ) as refresh_delay_mock:
            result = ingest_tasks.ingest_game.run("future-game")
    finally:
        ingest_tasks.ingest_game.pop_request()

    fetch_mock.assert_not_called()
    refresh_delay_mock.assert_not_called()
    assert result == {
        "game_id": "future-game",
        "status": "skipped",
        "skip_reason": "upcoming",
        "new_game": False,
        "needed_detail_pbp_refresh": False,
        "shot_refreshed": False,
        "line_score_rows": 0,
        "metric_tasks_enqueued": 0,
    }


def test_ingest_game_force_refreshes_existing_upcoming_game():
    before_status = {
        "exists_game": True,
        "game_status": "upcoming",
        "artifacts_supported": True,
        "has_detail": False,
        "has_pbp": False,
        "has_shot": True,
        "season": "22025",
    }
    after_status = {
        "exists_game": True,
        "game_status": "completed",
        "artifacts_supported": True,
        "has_detail": True,
        "has_pbp": True,
        "has_shot": True,
        "season": "22025",
    }

    status_session = _ctx(MagicMock())
    process_session = _ctx(MagicMock())
    final_status_session = _ctx(MagicMock())
    zero_score_session = _ctx(MagicMock())
    zero_score_session.query.return_value.filter.return_value.first.return_value = None
    line_score_session = _ctx(MagicMock())
    metric_refresh_session = _ctx(MagicMock())
    period_check_session = _ctx(MagicMock())

    ingest_tasks.ingest_game.push_request(id="worker-1", retries=0)
    try:
        with patch.object(
            ingest_tasks,
            "_session_factory",
            return_value=MagicMock(
                side_effect=[
                    status_session,
                    process_session,
                    final_status_session,
                    zero_score_session,
                    line_score_session,
                    metric_refresh_session,
                    period_check_session,
                ]
            ),
        ), patch.object(
            ingest_tasks,
            "_load_game_artifact_status",
            side_effect=[before_status, after_status],
        ), patch.object(
            ingest_tasks,
            "_build_existing_game_row",
            return_value={
                "GAME_ID": "g1",
                "SEASON_ID": "22025",
                "GAME_DATE": "2026-04-12",
                "MATCHUP": "LAL vs. BOS",
            },
        ), patch.object(
            ingest_tasks,
            "_fetch_api_row",
        ) as fetch_row_mock, patch.object(
            ingest_tasks,
            "process_and_store_game",
        ) as process_mock, patch.object(
            ingest_tasks,
            "has_game_line_score",
            return_value=True,
        ), patch.object(
            ingest_tasks,
            "has_game_period_stats",
            return_value=True,
        ), patch(
            "tasks.metrics.refresh_current_season_metrics.delay",
        ) as refresh_delay_mock:
            result = ingest_tasks.ingest_game.run("g1", force=True)
    finally:
        ingest_tasks.ingest_game.pop_request()

    process_mock.assert_called_once()
    fetch_row_mock.assert_not_called()
    refresh_delay_mock.assert_called_once_with(
        [
            {
                "game_id": "g1",
                "status": "ok",
                "new_game": False,
                "needed_detail_pbp_refresh": True,
                "shot_refreshed": False,
                "line_score_rows": 0,
                "metric_tasks_enqueued": 0,
                "metric_refresh_reason": "needed_detail_pbp_refresh",
            }
        ]
    )
    assert result == {
        "game_id": "g1",
        "status": "ok",
        "new_game": False,
        "needed_detail_pbp_refresh": True,
        "shot_refreshed": False,
        "line_score_rows": 0,
        "metric_tasks_enqueued": 0,
    }


def test_matchup_team_role_accepts_vs_without_period():
    assert dispatch_tasks._matchup_team_role("LAL vs BOS") == "home"
    assert dispatch_tasks._matchup_team_role("LAL vs. BOS") == "home"
    assert dispatch_tasks._matchup_team_role("LAL @ BOS") == "road"
    assert dispatch_tasks._matchup_team_role("DAL @ DET", team_abbr="DET") == "home"
    assert dispatch_tasks._matchup_team_role("DAL @ DET", team_abbr="DAL") == "road"


def test_cmd_season_metrics_with_explicit_season_enqueues_matching_career_bucket():
    base_metric = MagicMock(
        key="metric_a",
        trigger="season",
        career=False,
        supports_career=True,
        season_types=("regular",),
    )
    args = SimpleNamespace(metric=None, season="22025")

    with patch("metrics.framework.runtime.get_all_metrics", return_value=[base_metric]), patch(
        "tasks.metrics.enqueue_season_metric_refresh",
        return_value={
            "seasons": ["22025"],
            "career_buckets": ["all_regular", "last5_regular", "last3_regular"],
            "metrics": 1,
            "enqueued": 1,
            "callbacks": 1,
        },
    ) as enqueue_refresh_mock:
        dispatch_tasks.cmd_season_metrics(args)

    enqueue_refresh_mock.assert_called_once()
    call_args, call_kwargs = enqueue_refresh_mock.call_args
    assert call_args == (["22025"],)
    assert call_kwargs["metrics"] == [base_metric]
