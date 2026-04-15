"""Celery application configuration for Funba event-driven pipeline."""
import os
import sys

# Ensure the project root is on sys.path so tasks can import sibling
# packages (e.g. `web.live_game_data`) when the worker is launched via the
# bare `celery` CLI from outside an explicit module path.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from celery import Celery
from celery.worker.autoscale import Autoscaler
from kombu import Queue


class CooldownAutoscaler(Autoscaler):
    """Autoscaler with 60s cooldown before shrinking."""
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("keepalive", 60)
        super().__init__(*args, **kwargs)

BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
VISIBILITY_TIMEOUT = int(os.getenv("CELERY_VISIBILITY_TIMEOUT", "7200"))

app = Celery("funba", broker=BROKER_URL, backend=RESULT_BACKEND)

app.conf.update(
    # --- Queue routing ---
    task_queues=(
        Queue("ingest"),
        Queue("metrics"),
        Queue("reduce"),
        Queue("news"),
    ),
    task_default_queue="ingest",
    task_routes={
        "tasks.ingest.ingest_game": {"queue": "ingest"},
        "tasks.ingest.ingest_recent_games": {"queue": "ingest"},
        "tasks.ingest.sync_schedule_window": {"queue": "ingest"},
        "tasks.metrics.compute_game_delta": {"queue": "metrics"},
        "tasks.metrics.sweep_metric_compute_runs": {"queue": "reduce"},
        "tasks.metrics.reduce_metric_compute_run": {"queue": "reduce"},
        "tasks.metrics.reduce_metric_season": {"queue": "reduce"},
        "tasks.metrics.chord_reduce_callback": {"queue": "reduce"},
        "tasks.metrics.reduce_after_ingest": {"queue": "reduce"},
        "tasks.metrics.compute_season_metric": {"queue": "metrics"},
        "tasks.metrics.enqueue_career_metric_family": {"queue": "metrics"},
        "tasks.content.ensure_daily_content_analysis": {"queue": "ingest"},
        "tasks.content.ensure_recent_content_analysis": {"queue": "ingest"},
        "tasks.content.ensure_recent_content_analysis_for_season": {"queue": "ingest"},
        "tasks.ingest.scrape_nba_news": {"queue": "news"},
        "tasks.ingest.refresh_news_scores": {"queue": "news"},
        "tasks.ingest.refresh_current_team_logos": {"queue": "ingest"},
        "tasks.ingest.sync_current_team_rosters": {"queue": "ingest"},
    },

    # --- Serialization ---
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # --- Results (needed for chord) ---
    result_expires=3600,

    # --- Celery Beat schedule ---
    beat_schedule={
        "ingest-recent-games": {
            "task": "tasks.ingest.ingest_recent_games",
            "schedule": 600,
        },
        "sweep-metric-compute-runs": {
            "task": "tasks.metrics.sweep_metric_compute_runs",
            "schedule": 120,
        },
        "ensure-recent-content-analysis": {
            "task": "tasks.content.ensure_recent_content_analysis",
            "schedule": 600,
        },
        "sync-schedule-window": {
            "task": "tasks.ingest.sync_schedule_window",
            "schedule": 3600,
            "kwargs": {
                "lookahead_days": 365,
                "season_types": ["Regular Season", "PlayIn", "Playoffs"],
            },
        },
        "scrape-nba-news": {
            "task": "tasks.ingest.scrape_nba_news",
            "schedule": 3600,
        },
        "refresh-news-scores": {
            "task": "tasks.ingest.refresh_news_scores",
            "schedule": 300,
        },
        "refresh-current-team-logos": {
            "task": "tasks.ingest.refresh_current_team_logos",
            "schedule": 60 * 60 * 24 * 30,  # ~monthly
        },
        "sync-current-team-rosters": {
            "task": "tasks.ingest.sync_current_team_rosters",
            "schedule": 60 * 60 * 24,  # daily
        },
    },

    # --- Broker ---
    broker_connection_retry_on_startup=True,
    broker_transport_options={"visibility_timeout": VISIBILITY_TIMEOUT},

    # --- Worker ---
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=5000,
    worker_autoscaler="tasks.celery_app:CooldownAutoscaler",
)

# Explicitly import task modules so workers register them
import tasks.ingest  # noqa: F401, E402
import tasks.metrics  # noqa: F401, E402
import tasks.topics  # noqa: F401, E402
import tasks.content  # noqa: F401, E402
