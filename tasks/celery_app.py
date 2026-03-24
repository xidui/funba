"""Celery application configuration for Funba event-driven pipeline."""
import os

from celery import Celery
from celery.worker.autoscale import Autoscaler
from kombu import Queue


class CooldownAutoscaler(Autoscaler):
    """Autoscaler with 60s cooldown before shrinking."""
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("keepalive", 60)
        super().__init__(*args, **kwargs)

BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND") or None

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
    },

    # --- Serialization ---
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_ignore_result=True,

    # --- Celery Beat schedule ---
    beat_schedule={
        "ingest-yesterday-games": {
            "task": "tasks.ingest.ingest_yesterday",
            "schedule": 60 * 60,
        },
        "sweep-metric-compute-runs": {
            "task": "tasks.metrics.sweep_metric_compute_runs",
            "schedule": 10,
        },
    },

    # --- Broker ---
    broker_connection_retry_on_startup=True,

    # --- Worker ---
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=5000,
    worker_autoscaler="tasks.celery_app:CooldownAutoscaler",
)

# Explicitly import task modules so workers register them
import tasks.ingest  # noqa: F401, E402
import tasks.metrics  # noqa: F401, E402
