"""Celery application configuration for Funba event-driven pipeline."""
import os

from celery import Celery
from celery.schedules import crontab
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
    ),
    task_default_queue="ingest",
    task_routes={
        "tasks.ingest.ingest_game": {"queue": "ingest"},
        "tasks.metrics.compute_game_delta": {"queue": "metrics"},
        "tasks.metrics.sweep_metric_compute_runs": {"queue": "reduce"},
        "tasks.metrics.reduce_metric_compute_run": {"queue": "reduce"},
        "tasks.metrics.reduce_metric_season": {"queue": "reduce"},
        "tasks.metrics.chord_reduce_callback": {"queue": "reduce"},
        "tasks.metrics.reduce_after_ingest": {"queue": "reduce"},
        "tasks.metrics.compute_season_metric": {"queue": "metrics"},
        "tasks.metrics.enqueue_career_metric_family": {"queue": "metrics"},
        "tasks.topics.generate_daily_topics": {"queue": "reduce"},
    },

    # --- Serialization ---
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # --- Results (needed for chord) ---
    result_expires=3600,

    # --- Celery Beat schedule ---
    beat_schedule={
        "ingest-yesterday-games": {
            "task": "tasks.ingest.ingest_yesterday",
            "schedule": 60 * 60,
        },
        "sweep-metric-compute-runs": {
            "task": "tasks.metrics.sweep_metric_compute_runs",
            "schedule": 120,
        },
        "generate-daily-topics": {
            "task": "tasks.topics.generate_daily_topics",
            "schedule": crontab(hour=12, minute=0),
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
