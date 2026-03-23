"""Celery application configuration for Funba event-driven pipeline."""
import os

from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.getenv("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND") or None

app = Celery("funba", broker=BROKER_URL, backend=RESULT_BACKEND)

# Dead-letter exchanges — must be declared before the main queues that reference them
_dlx_ingest = Exchange("ingest.dlx", type="fanout", durable=True)
_dlx_metrics = Exchange("metrics.dlx", type="fanout", durable=True)
_dlx_reduce = Exchange("reduce.dlx", type="fanout", durable=True)

app.conf.update(
    # --- Queue routing ---
    # Kombu Queue objects: declares exchanges, queues, and DLX bindings in RabbitMQ
    task_queues=(
        Queue(
            "ingest",
            Exchange("ingest", type="direct", durable=True),
            routing_key="ingest",
            queue_arguments={"x-dead-letter-exchange": "ingest.dlx"},
            durable=True,
        ),
        Queue(
            "metrics",
            Exchange("metrics", type="direct", durable=True),
            routing_key="metrics",
            queue_arguments={"x-dead-letter-exchange": "metrics.dlx"},
            durable=True,
        ),
        Queue(
            "reduce",
            Exchange("reduce", type="direct", durable=True),
            routing_key="reduce",
            queue_arguments={"x-dead-letter-exchange": "reduce.dlx"},
            durable=True,
        ),
        # Dead-letter queues — fanout-bound so all rejected messages land here
        Queue("ingest.dlq", _dlx_ingest, routing_key="#", durable=True),
        Queue("metrics.dlq", _dlx_metrics, routing_key="#", durable=True),
        Queue("reduce.dlq", _dlx_reduce, routing_key="#", durable=True),
    ),
    task_default_queue="ingest",
    task_routes={
        "tasks.ingest.ingest_game": {"queue": "ingest"},
        "tasks.metrics.compute_game_delta": {"queue": "metrics"},
        "tasks.metrics.reduce_metric_season": {"queue": "reduce"},
    },

    # --- Serialization ---
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_ignore_result=True,      # fire-and-forget pipeline; do not create/store task results

    # --- Retry policies (per task in task definitions) ---
    # ingest: max 3 retries, exponential backoff 30s/90s/270s
    # metrics: max 3 retries, flat 10s

    # --- Celery Beat schedule (daily cron) ---
    beat_schedule={
        "ingest-yesterday-games": {
            "task": "tasks.ingest.ingest_yesterday",
            "schedule": 60 * 60,  # every hour — task itself skips already-ingested games
        },
    },

    # --- Broker ---
    broker_connection_retry_on_startup=True,

    # --- Worker ---
    worker_prefetch_multiplier=1,  # one task at a time per worker slot (fair dispatch)
    task_acks_late=True,           # ack only after task completes (safe re-queue on crash)
)

# Explicitly import task modules so workers register them
import tasks.ingest  # noqa: F401, E402
import tasks.metrics  # noqa: F401, E402


@app.on_after_finalize.connect
def declare_dlx_topology(sender, **kwargs):
    """Declare DLX exchanges and DLQ queues on worker startup.

    Celery only auto-declares queues it actively consumes from. The DLQ queues
    (ingest.dlq / metrics.dlq) are for dead-letter inspection — no worker
    consumes them — so we declare them explicitly here.
    """
    with sender.connection_for_write() as conn:
        channel = conn.channel()
        for dlx, dlq in [
            (_dlx_ingest, "ingest.dlq"),
            (_dlx_metrics, "metrics.dlq"),
            (_dlx_reduce, "reduce.dlq"),
        ]:
            dlx.declare(channel=channel)
            from kombu import Queue as KombuQueue
            KombuQueue(dlq, dlx, routing_key="#", durable=True).declare(channel=channel)
