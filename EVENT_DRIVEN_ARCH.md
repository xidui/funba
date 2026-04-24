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

Broker: Redis (`redis://localhost:6379/0` in production launchd plists).
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
| Force recompute | `python -m tasks.dispatch metric-backfill --metric clutch_fg_pct --force` | Clears run logs, recomputes all deltas |

### discover vs backfill

`discover` queries the NBA API (LeagueGameFinder) for games in a date range and is the right
choice when games may not yet be in the DB — e.g. ingesting a new season or catching up after
downtime. `backfill` queries the local DB for games that are already stored and re-runs the
ingest + metric pipeline on them.

## Local Quickstart (Docker Compose)

### Prerequisites
- Docker + Docker Compose
- MySQL running on the host (or update `NBA_DB_URL` in `.env`)
- Redis is used as the Celery broker and result backend

### Steps

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env if needed (DB URL, broker URL)

# 2. Start Redis
docker-compose up -d redis

# 3. Start workers
docker-compose up -d worker-ingest worker-metrics

# 4. (Optional) Start Celery Beat for daily cron
docker-compose up -d scheduler

# 5. Dispatch a task
python -m tasks.dispatch game 0022400909

# 6. Watch Redis queue depth:
redis-cli llen ingest
redis-cli llen metrics
```

### Verify

```sql
-- Check metric run logs after workers finish
SELECT COUNT(*) FROM MetricRunLog WHERE game_id = '0022400909';
-- Should equal the number of registered metric keys
```

## AWS Migration Path

Use managed Redis/Valkey for the Celery broker and result backend, then redeploy
the same Docker image to ECS Fargate:

```bash
CELERY_BROKER_URL=redis://redis-host:6379/0
CELERY_RESULT_BACKEND=redis://redis-host:6379/0
NBA_DB_URL=mysql+pymysql://user:pass@rds-host.us-east-1.rds.amazonaws.com/nba_data
```

**Auto-scaling:** Create an alarm from Redis queue depth / worker lag and connect
it to the ECS scale-out policy for worker services.

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

### Redis Queue Inspection

```bash
redis-cli llen ingest
redis-cli llen metrics
redis-cli llen reduce
```

### Scaling Workers
```bash
# Add more metrics workers without rebuild
docker-compose up -d --scale worker-metrics=3
```

### Failed Task Inspection

Workers log failures to `logs/worker-*-stderr.log`. Redis queue depth can be
checked with `redis-cli llen <queue>`. Stuck queue entries can be purged with
`redis-cli del <queue>` after confirming they are safe to drop.

Or use Celery's `flower` UI for task monitoring:
```bash
pip install flower
celery -A tasks.celery_app flower
# Open http://localhost:5555
```

### Reranking Metrics

After a bulk backfill, recompute noteworthiness percentile ranks:

```bash
python -m tasks.dispatch metric-backfill --season 22025 --force
```

## Files Reference

| File | Purpose |
|---|---|
| `tasks/celery_app.py` | Celery app + queue config + Beat schedule |
| `tasks/ingest.py` | `ingest_game`, `ingest_yesterday` tasks |
| `tasks/metrics.py` | `compute_game_metrics` task |
| `tasks/dispatch.py` | CLI to enqueue tasks without starting a worker |
| `Dockerfile` | Single image for all worker types |
| `docker-compose.yml` | Local dev: Redis + ingest/metrics workers + scheduler |
| `.env.example` | Environment variable template |
| `metrics/framework/runner.py` | Added `run_for_game_single_metric()` |
| `metrics/framework/daily_job.py` | **Deprecated** — use `tasks.dispatch` instead |
| `db/backfill_nba_games_targeted.py` | **Deprecated** — use `dispatch discover` instead |

## Notes

- Celery chord is used for completion detection (both backfill and daily ingest). Redis result backend stores chord counters (results expire after 1 hour).
- Correctness is tracked in MySQL via `MetricRunLog`. Idempotency is via MetricRunLog existence check.
