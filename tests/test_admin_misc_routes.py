from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from celery.schedules import crontab

from web.admin_misc_routes import _format_schedule_interval, register_admin_misc_routes


def test_format_schedule_interval_formats_second_intervals():
    assert _format_schedule_interval(120) == "2m"
    assert _format_schedule_interval(7200) == "2h"
    assert _format_schedule_interval(45) == "45s"


def test_format_schedule_interval_formats_daily_crontab():
    assert _format_schedule_interval(crontab(hour=6, minute=0)) == "daily 06:00"


def test_format_schedule_interval_formats_generic_crontab():
    assert _format_schedule_interval(crontab(minute="*/5", hour="*")) == "cron */5 * * * *"


def _make_misc_app():
    app = Flask(__name__)
    register_admin_misc_routes(
        app,
        SimpleNamespace(
            require_admin_json=lambda: (lambda: None),
            require_admin_page=lambda: (lambda: None),
            build_game_metrics_payload=lambda: (lambda game_id: {
                "game_id": game_id,
                "game_metrics": {"season": [], "season_extra": []},
                "triggered_player_metrics": [],
                "triggered_team_metrics": [],
                "story_candidates": {
                    "lead_candidates": [],
                    "support_candidates": [],
                    "suppressed_candidates": [],
                },
            }),
        ),
    )
    return app


def test_api_data_game_metrics_returns_shared_payload():
    app = _make_misc_app()

    with app.test_client() as client:
        resp = client.get("/api/data/games/0022501066/metrics")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["game_id"] == "0022501066"
    assert payload["game_metrics"] == {"season": [], "season_extra": []}
    assert payload["triggered_player_metrics"] == []
    assert payload["triggered_team_metrics"] == []
    assert payload["story_candidates"] == {
        "lead_candidates": [],
        "support_candidates": [],
        "suppressed_candidates": [],
    }
