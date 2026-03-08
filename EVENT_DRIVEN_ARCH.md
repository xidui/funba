# Event-Driven Metrics Pipeline

## Architecture Overview

```
[Trigger: cron / CLI dispatch]
         │
         ▼
   Queue 1: "ingest"          (4 workers — rate-limited by NBA API)
   Task: ingest_game(game_id)
     • check what's missing: game detail, PBP, shot records
     • fetch from NBA API only what's needed
     • on success → fan-out: one compute task per metric key
         │
         └──▶ Queue 2: "metrics"    (8 workers — pure DB compute, no rate limits)
              Task: compute_game_metrics(game_id, metric_key)
                • run run_for_game_single_metric(session, game_id, metric_key)
                • write MetricResult + MetricRunLog

Dead-letter exchange:
  ingest failures  → ingest.dlq
  metrics failures → metrics.dlq
```

## Two-Queue Rationale

- **No redundant API fetches.** Without queue separation, N metrics × M games would
  trigger concurrent PBP/shot fetches for the same game. Queue 1 fetches once per game;
  Queue 2 only reads from the DB.
- **Independent scaling.** Queue 2 workers have no API rate limit — scale to 20+ safely.
- **Different retry semantics.** Ingest retries with exponential backoff (API throttling);
  metrics retries fast (DB transient errors).

## Unified Entry Points

| Trigger | Command | Effect |
|---|---|---|
| Daily cron | Celery Beat schedule | All games from yesterday → Queue 1 |
| Date range (new games) | `python -m tasks.dispatch discover --date-from 2026-03-02 --date-to 2026-03-07` | Discover via NBA API → Queue 1 |
| Season backfill (DB games) | `python -m tasks.dispatch backfill --season 22025` | Games already in DB → Queue 1 |
| Single game | `python -m tasks.dispatch game 0022400909` | One game → Queue 1 |
| New metric | `python -m tasks.dispatch metric-backfill --metric clutch_fg_pct` | All games → Queue 1 (artifact check) → Queue 2 |
| All metrics, all games | `python -m tasks.dispatch metric-backfill` | All games → Queue 1 (artifact check) → Queue 2 |
| Force recompute | `python -m tasks.dispatch metric-backfill --metric clutch_fg_pct --force` | Clears claims, undo-redo running totals |

### discover vs backfill

`discover` queries the NBA API (LeagueGameFinder) for games in a date range and is the right
choice when games may not yet be in the DB — e.g. ingesting a new season or catching up after
downtime. `backfill` queries the local DB for games that are already stored and re-runs the
ingest + metric pipeline on them.

## Local Quickstart (Docker Compose)

### Prerequisites
- Docker + Docker Compose
- MySQL running on the host (or update `NBA_DB_URL` in `.env`)
- Celery task results are intentionally disabled; RabbitMQ is used only as the broker

### Steps

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env if needed (DB URL, broker URL)

# 2. Start RabbitMQ
docker-compose up -d rabbitmq
# Open http://localhost:15672 — login guest / guest

# 3. Start workers
docker-compose up -d worker-ingest worker-metrics

# 4. (Optional) Start Celery Beat for daily cron
docker-compose up -d scheduler

# 5. Dispatch a task
python -m tasks.dispatch game 0022400909

# 6. Watch RabbitMQ UI: "ingest" queue gets 1 message → consumed →
#    "metrics" queue gets N messages (one per metric key)
```

### Verify

```sql
-- Check metric run logs after workers finish
SELECT COUNT(*) FROM MetricRunLog WHERE game_id = '0022400909';
-- Should equal the number of registered metric keys
```

## AWS Migration Path

Change two env vars, redeploy the same Docker image to ECS Fargate:

```bash
CELERY_BROKER_URL=amqps://user:pass@b-xxx.mq.us-east-1.amazonaws.com:5671//
NBA_DB_URL=mysql+pymysql://user:pass@rds-host.us-east-1.rds.amazonaws.com/nba_data
```

**Auto-scaling:** Create a CloudWatch alarm on RabbitMQ `MessagesReady` metric
→ ECS scale-out policy for `worker-metrics` service.

ECS task definition commands:
- `worker-ingest`: `celery -A tasks.celery_app worker -Q ingest -c 4 --loglevel=info`
- `worker-metrics`: `celery -A tasks.celery_app worker -Q metrics -c 8 --loglevel=info`
- `scheduler`: `celery -A tasks.celery_app beat --loglevel=info`

## Adding a New Metric

1. Define the metric in `metrics/definitions/` and register it in `metrics/framework/registry.py`.
2. Run a metric backfill to compute it for all historical games:

```bash
python -m tasks.dispatch metric-backfill --metric your_new_metric_key
```

This routes through Queue 1 (ingest) first, which verifies artifact presence
(game detail, PBP, shot records) and fetches anything missing before fanning
out metric compute tasks to Queue 2.

## Operational Runbook

### RabbitMQ Management UI
- URL: http://localhost:15672 (or your Amazon MQ endpoint)
- Credentials: guest/guest (local) or your MQ user
- Queue depths visible on the Queues tab

### Scaling Workers
```bash
# Add more metrics workers without rebuild
docker-compose up -d --scale worker-metrics=3
```

### Dead-Letter Inspection + Replay

Failed tasks land in `ingest.dlq` or `metrics.dlq`. To inspect:

1. In the RabbitMQ Management UI → Queues → `ingest.dlq` or `metrics.dlq`
2. Click "Get messages" to view the failed task body
3. To replay, move messages back to the source queue:

```bash
# Using rabbitmqadmin (install from http://localhost:15672/cli)
rabbitmqadmin move-messages \
  --source=metrics.dlq \
  --destination=metrics \
  --count=100
```

Or use Celery's `flower` UI for task monitoring:
```bash
pip install flower
celery -A tasks.celery_app flower
# Open http://localhost:5555
```

### Reranking Metrics

After a bulk backfill, recompute noteworthiness percentile ranks:

```bash
python -m metrics.framework.daily_job --season 22025 --force
# or use the existing rerank CLI if implemented
```

## Files Reference

| File | Purpose |
|---|---|
| `tasks/celery_app.py` | Celery app + queue config + Beat schedule |
| `tasks/ingest.py` | `ingest_game`, `ingest_yesterday` tasks |
| `tasks/metrics.py` | `compute_game_metrics` task |
| `tasks/dispatch.py` | CLI to enqueue tasks without starting a worker |
| `Dockerfile` | Single image for all worker types |
| `docker-compose.yml` | Local dev: RabbitMQ + ingest/metrics workers + scheduler |
| `.env.example` | Environment variable template |
| `metrics/framework/runner.py` | Added `run_for_game_single_metric()` |
| `metrics/framework/daily_job.py` | Unchanged — local fallback (no Docker) |
| `db/backfill_nba_games_targeted.py` | **Deprecated** — use `dispatch discover` instead |

## Notes

- Celery is configured with `task_ignore_result=True`, so workers do not publish task results to an `rpc://` reply queue.
- This pipeline is fire-and-forget: correctness is tracked in MySQL via `MetricJobClaim` / `MetricRunLog`, not via Celery task results.
