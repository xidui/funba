# Funba Deployment Runbook

## Git Workflow

Two flows exist. Pick the one that matches who is doing the work.

### Paperclip-managed tickets (human reviewers)

Use the company delivery workflow:

- Implement on a ticket feature branch, not on `origin/main`.
- Keep exactly one GitHub PR per ticket.
- If the work does not fit cleanly in one PR, split it into child tickets before continuing.
- After review approval, squash-merge the ticket PR into `origin/main` (no cherry-pick).
- DevOps deploys only the latest `origin/main`, never a feature branch.

### Agent-driven work (Claude Code, Codex, etc.)

When the owner is driving an AI agent interactively — no human PR reviewers involved — the PR ceremony is pure overhead. Skip it:

- Commit directly to `main` (`git commit` + `git push origin main`).
- Immediately run the deploy steps below (update `.paperclip/deploy-main` worktree, run alembic if schema changed, restart the affected launchd services).
- Real-time review happens in the agent conversation, not on GitHub.
- Fall back to the feature-branch + PR flow only when the owner explicitly asks for one, the change is large enough that they want a GitHub diff view before merging, or an external reviewer (e.g. Codex review of a Claude Code PR) benefits from a reviewable URL.

Agents must still: run migrations/tests before restarting services, flag destructive actions before executing them, and never force-push `main`. The rule relaxation is about process overhead, not about safety.

The remote is `https://github.com/xidui/funba.git` (private repo). Every completed code change must be pushed — for ticket flow, branches stay on the PR until mainline integration; for agent flow, they land on `main` immediately.

**History note:** Commit `9920d56` introduced a 2.98 GB SQL dump (`funba_nba_data_20260225_174416.sql`) that was subsequently removed in `d5e3dc0`. When the push backlog was first cleared (2026-03-15), `git filter-repo` was used to excise the blob from history before force-pushing. Future large data files (> 50 MB) must be gitignored and never committed.

---

## Architecture Overview (Option B — Cloudflare Tunnel)

```
User → funba.app (Cloudflare DNS, proxied)
     → Cloudflare Edge (global PoPs, auto-TLS)
     → cloudflared tunnel (runs on Mac Studio, launchd LaunchDaemon)
     → localhost:5001 on Mac Studio
     → gunicorn web app (4 workers, launchd LaunchDaemon)
```

No reverse SSH tunnel or public droplet required for the app traffic. The DigitalOcean
droplet continues to serve `*.babyrasier.com` but is removed from the `funba.app` path.

---

## Machines

| Machine     | Role                                     | Access                    |
|-------------|------------------------------------------|---------------------------|
| Mac Studio  | App server, DB, compute, cloudflared     | Local / SSH if needed     |
| Droplet     | Other domains (babyrasier.com), optional | `ssh root@209.38.71.231`  |

---

## Host Context — AI agents read this first

Every `launchctl`, `git -C .../deploy-main`, and `curl localhost:5001` command in this runbook only does the right thing **on the Mac Studio itself**. `launchctl` always operates on the local launchd domain — running it on the laptop will list the laptop's agents, not Mac Studio's, and silently do nothing useful.

So before executing any deploy step, detect where you are:

```bash
# Prints "deploy-host" or "remote"
if mount | grep -q 'xixihaha:.*github/funba'; then echo remote; else echo deploy-host; fi
```

- **deploy-host (Mac Studio)** — run commands directly as written below.
- **remote (e.g. laptop where the repo is sshfs-mounted at `/Users/yuewang/mnt/xixihaha-funba`)** — wrap every deploy command in `ssh xixihaha '...'`. The `xixihaha` alias in `~/.ssh/config` resolves to the Mac Studio. Editing files locally is fine (the mount writes through), but **service management and worktree updates must run via SSH**, not on the local mount.

Quick sanity check that proves you're remote:
- Your `pwd` is under `/Users/yuewang/mnt/xixihaha-funba`
- `launchctl list | grep app.funba` returns nothing
- `mount | grep xixihaha` shows an `macfuse` line

When in doubt: `ssh xixihaha 'launchctl list | grep app.funba'` — if that returns the `app.funba.*` entries, you needed the SSH wrapper.

Code edits and `git push` work from either side (the mount writes through, and `git push` only needs network). It's just the *applying-the-deploy* steps that must run on Mac Studio.

---

## Boot And Session Behavior

The production web stack runs as system `LaunchDaemon`s so it can start without
a user GUI login:

- `homebrew.mxcl.mysql`
- `homebrew.mxcl.redis`
- `app.funba.web`
- `app.funba.cloudflared`
- `app.funba.scheduler`
- `app.funba.worker-ingest`
- `app.funba.worker-metrics`
- `app.funba.worker-reduce`
- `app.funba.worker-news`
- `app.funba.web-watchdog`

Runtime files for those daemons live outside `~/Documents` to avoid macOS TCC
protection issues:

```bash
/Users/yuewang/Library/funba/deploy-main
/Users/yuewang/Library/funba/venv
/Users/yuewang/Library/funba/LaunchDaemons
/Users/yuewang/Library/Logs/funba
```

Prepare or refresh the runtime/staged plist files from the current deploy
worktree and local source plist environment. These one-time migration helpers
are intentionally stored outside the repo:

```bash
python3 /Users/yuewang/Library/funba/migration/prepare_launchdaemon_runtime.py
```

Install and switch the services into the system launchd domain:

```bash
sudo bash /Users/yuewang/Library/funba/migration/install_launch_daemons.sh
```

`sudo` is required because `/Library/LaunchDaemons` must be owned by
`root:wheel`. The install script moves the old user LaunchAgent plist files into
a timestamped backup directory under `~/Library/LaunchAgents/` and also keeps a
stable copy at `~/Library/funba/LaunchAgents-source/` so future runtime refreshes
can regenerate daemon plists without committing secrets.

Power-loss behavior:

- `pmset -g | grep autorestart` should show `autorestart 1`; set it with
  `sudo pmset -a autorestart 1`.
- If FileVault is enabled, a full power-loss restart still stops at the
  FileVault unlock screen. LaunchDaemons can start before GUI login only after
  the encrypted volume is unlocked. Fully unattended power-loss recovery
  requires disabling FileVault or preventing hard power loss with a UPS.

---

## Initial Setup (completed — for reference / disaster recovery)

Setup was completed on 2026-03-15. Credentials live in `~/.cloudflared/`:
- `cert.pem` — Cloudflare origin cert (re-run `cloudflared tunnel login` if expired)
- `d9c59fa5-f7e1-43a3-ae8e-7e66618275a3.json` — tunnel credentials
- `config.yml` — tunnel config (hostname + ingress rules)

**To redo from scratch** (e.g. after re-imaging Mac Studio):

```bash
# 1 — Login (browser opens, authorize funba.app on Cloudflare)
cloudflared tunnel login

# 2 — Recreate tunnel
cloudflared tunnel create funba

# 3 — Update ~/.cloudflared/config.yml with new tunnel ID + credentials path

# 4 — Route DNS
cloudflared tunnel route dns funba funba.app

# 5 — Refresh staged daemons and start service
python3 /Users/yuewang/Library/funba/migration/prepare_launchdaemon_runtime.py
sudo bash /Users/yuewang/Library/funba/migration/install_launch_daemons.sh
```

---

## Droplet: funba.app removed from Caddy (done 2026-03-15)

The `funba.app` block was removed from `/etc/caddy/Caddyfile` on the droplet and Caddy
was reloaded. The autossh tunnel was disabled: `launchctl unload app.funba.tunnel.plist`.
The droplet now only serves `*.babyrasier.com`.

---

## Mac Studio: Web App (gunicorn, launchd)

The app runs under launchd as service `app.funba.web`, supervised by gunicorn (4 workers).

### Runtime source of truth:

The live launchd service does **not** run directly from the repo root checkout.
The source deploy worktree remains:

```bash
/Users/yuewang/Documents/github/funba/.paperclip/deploy-main
```

System LaunchDaemons run from a synced runtime copy outside `~/Documents`:

```bash
/Users/yuewang/Library/funba/deploy-main
```

This means normal pushes to `origin/main` do **not** update the running app by itself.
Deploying the latest `main` requires three explicit steps:

```bash
# 1. Update the deploy worktree to the target commit
git -C /Users/yuewang/Documents/github/funba/.paperclip/deploy-main checkout --detach <commit-or-origin/main>

# 2. Sync daemon runtime + regenerate staged daemon plists
python3 /Users/yuewang/Library/funba/migration/prepare_launchdaemon_runtime.py

# 3. Restart the system web service
sudo launchctl kickstart -k system/app.funba.web
```

If the deploy worktree is not updated first, the restarted service will continue
running the old code even when `origin/main` is newer.

### Service management:

```bash
# Check status
sudo launchctl print system/app.funba.web

# Start
sudo launchctl bootstrap system /Library/LaunchDaemons/app.funba.web.plist

# Stop
sudo launchctl bootout system/app.funba.web

# Restart
sudo launchctl kickstart -k system/app.funba.web
```

### Logs:
- Access log: `~/Library/Logs/funba/web-app-5001.log`
- Error log: `~/Library/Logs/funba/web-app-5001-error.log`
- Stdout: `~/Library/Logs/funba/web-app-5001-stdout.log`
- Stderr: `~/Library/Logs/funba/web-app-5001-stderr.log`

### Web watchdog:

`app.funba.web-watchdog` runs every 30 seconds and checks
`http://127.0.0.1:5001/api/health`. It requires three consecutive failed checks
before recovery and waits at least five minutes between recovery attempts.
Failure counts and cooldown survive invocations in
`~/Library/Caches/funba/web-watchdog.json`; overlapping checks are skipped.

For a system daemon running as the app user, recovery verifies the launchd
KeepAlive master PID, ownership, and command, then gracefully terminates only
that master so launchd restarts it. Root or GUI-domain watchdogs use kickstart.
The watchdog never force-kills all processes listening on the web port.

Optional settings are `FUNBA_WEB_WATCHDOG_FAILURE_THRESHOLD`,
`FUNBA_WEB_WATCHDOG_RESTART_COOLDOWN_SECONDS`, and
`FUNBA_WEB_WATCHDOG_STATE_PATH`. Keep the state file outside the synced runtime.

Install or update it after deploy changes that touch `scripts/funba_web_watchdog.py`
by refreshing the daemon runtime:

```bash
python3 /Users/yuewang/Library/funba/migration/prepare_launchdaemon_runtime.py
sudo launchctl kickstart -k system/app.funba.web-watchdog
```

Logs:
- Stdout: `~/Library/Logs/funba/web-watchdog-stdout.log`
- Stderr: `~/Library/Logs/funba/web-watchdog-stderr.log`

### Environment variables (set in plist):

**Web app** (`app.funba.web`):

| Variable | Purpose |
|----------|---------|
| `NBA_DB_URL` | MySQL connection string |
| `CELERY_BROKER_URL` | Redis broker URL |
| `FLASK_SECRET_KEY` | Session signing |
| `GOOGLE_CLIENT_ID` | OAuth login |
| `GOOGLE_CLIENT_SECRET` | OAuth login |
| `FUNBA_CURL_ALLOWED_IPS` | Comma/space-separated IPs or CIDRs allowed to use `curl/` through Cloudflare |
| `FUNBA_GAME_METRICS_CACHE_REDIS_URL` | Optional Redis URL for cached single-game highlight payloads; defaults to `CELERY_BROKER_URL` |
| `FUNBA_GAME_METRICS_CACHE_TTL_SECONDS` | Optional TTL for cached single-game highlight payloads; defaults to 7 days |
| `OPENAI_API_KEY` | Metric code generation |
| `STRIPE_SECRET_KEY` | Subscription billing |
| `STRIPE_PUBLISHABLE_KEY` | Stripe frontend |
| `STRIPE_PRO_PRICE_ID` | Pro tier price |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook verification |
| `RESEND_API_KEY` | Transactional email |

**Workers** (`app.funba.worker-*`):

| Variable | Purpose |
|----------|---------|
| `NBA_DB_URL` | MySQL connection string |
| `CELERY_BROKER_URL` | Redis broker URL |
| `DB_POOL_SIZE` | SQLAlchemy pool (1 for ingest/metrics, 4 for reduce) |
| `DB_MAX_OVERFLOW` | SQLAlchemy pool overflow |
| `OBJC_DISABLE_INITIALIZE_FORK_SAFETY` | macOS fork safety workaround |
| `RESEND_API_KEY` | Email notifications (worker-reduce only) |

To override, edit the source plist under `~/Library/funba/LaunchAgents-source/`
after migration, or `~/Library/LaunchAgents/app.funba.<service>.plist` before
migration, then rerun:

```bash
python3 /Users/yuewang/Library/funba/migration/prepare_launchdaemon_runtime.py
sudo bash /Users/yuewang/Library/funba/migration/install_launch_daemons.sh
```

Secrets (API keys, etc.) must NOT be committed to git. See local `SECRETS.md` (gitignored).

---

## Mac Studio: Cloudflare Tunnel

### Service management:

```bash
# Check status
sudo launchctl print system/app.funba.cloudflared

# Start
sudo launchctl bootstrap system /Library/LaunchDaemons/app.funba.cloudflared.plist

# Stop
sudo launchctl bootout system/app.funba.cloudflared

# Restart
sudo launchctl kickstart -k system/app.funba.cloudflared
```

### Logs:
- `~/Library/Logs/funba/cloudflared-stdout.log`
- `~/Library/Logs/funba/cloudflared-stderr.log`

---

## Mac Studio: Celery Workers + Redis (native, launchd)

The async pipeline (game ingestion, metric computation, backfill) runs via Celery
workers as native processes managed by launchd, with Redis as the message broker.

### Architecture:

```
Web app (publish metric / daily scheduler)
     → Redis (redis://localhost:6379/0, Homebrew service)
     → worker-ingest (Queue: ingest, autoscale 2–10)
     → worker-metrics (Queue: metrics, autoscale 8–50)
     → worker-reduce (Queue: reduce, autoscale 2–16)
     → scheduler (Celery Beat)
     → MySQL (shared with web app)
```

### launchd services:

| Service | Label | Queue(s) | Autoscale (min–max) | Notes |
|---------|-------|----------|---------------------|-------|
| Redis | `homebrew.mxcl.redis` | — | — | Broker (system LaunchDaemon) |
| Ingest worker | `app.funba.worker-ingest` | `ingest` | 2–10 | DB_POOL_SIZE=1 |
| Metrics worker | `app.funba.worker-metrics` | `metrics` | 8–50 | DB_POOL_SIZE=1, fd limit 4096 |
| Reduce worker | `app.funba.worker-reduce` | `reduce` | 2–16 | DB_POOL_SIZE=4 |
| Scheduler | `app.funba.scheduler` | — | — | Celery Beat (hourly ingest + 2-min sweep + daily topics at 12:00) |

All workers use the runtime deploy copy as WorkingDirectory and set
`CELERY_BROKER_URL=redis://localhost:6379/0` in their plist EnvironmentVariables.

`CELERY_RESULT_BACKEND` defaults to `redis://localhost:6379/0` in code (same as
broker). No plist change needed unless a separate Redis instance is desired.

### Worker tuning (celery_app.py):

| Setting | Value | Purpose |
|---------|-------|---------|
| `worker_max_tasks_per_child` | 5000 | Recycle prefork processes to prevent memory bloat |
| `worker_autoscaler` | `CooldownAutoscaler` | 60s keepalive before shrinking idle workers |
| `worker_prefetch_multiplier` | 1 | Fair task distribution across workers |
| `visibility_timeout` | 120s | Unacked tasks redelivered after 2 min (all tasks idempotent) |
| `result_expires` | 3600s | Chord result keys auto-expire after 1 hour |

### Metric pipeline completion:

All metrics use `trigger=season` (whole-season recompute). Two flows:

- **Daily ingest**: `ingest_yesterday` uses `chord(ingest_tasks)(refresh_current_season_metrics)`.
  After all games finish ingesting, the callback detects which seasons were affected
  and enqueues `compute_season_metric_task` for each (metric, season) pair plus
  corresponding career buckets (`all_regular`, `all_playoffs`, etc.).
- **Bulk backfill** (rebackfill from UI): `cmd_season_metrics` creates a
  `MetricComputeRun` and dispatches tasks via chord. Each task atomically increments
  `done_game_count`, providing real-time progress in the UI.

The sweep task (every 120s) acts as a fallback for:
- Mapping runs stuck > 2 hours (chord counter lost, e.g. Redis restart)
- Reducing runs stuck > 30 minutes (worker killed mid-reduce)

One `MetricComputeRun` row per metric (old runs auto-deleted on new backfill).

Workers use `--autoscale=MAX,MIN` (not `-c`) in their launchd plists.
Idle memory footprint is ~900 MB (12 processes); under full backfill load it
scales to ~2–3 GB (76 processes). Without `max_tasks_per_child`, prefork workers
leak ~100 MB/min during sustained backfill due to Python malloc fragmentation.

### Service management:

```bash
# Check all funba services
sudo launchctl print system | grep app.funba

# Check Redis
sudo launchctl print system/homebrew.mxcl.redis
redis-cli ping

# Start a worker
sudo launchctl bootstrap system /Library/LaunchDaemons/app.funba.worker-metrics.plist

# Stop a worker
sudo launchctl bootout system/app.funba.worker-metrics

# Restart a worker
sudo launchctl kickstart -k system/app.funba.worker-metrics

# View logs
tail -f ~/Library/Logs/funba/worker-metrics-stderr.log
tail -f ~/Library/Logs/funba/worker-ingest-stderr.log
tail -f ~/Library/Logs/funba/worker-reduce-stderr.log
tail -f ~/Library/Logs/funba/scheduler-stderr.log
```

### After code changes:

Workers run from the deploy worktree. After pushing code that affects
`metrics/`, `tasks/`, or `db/`:

```bash
# 1. Update deploy worktree
git -C /Users/yuewang/Documents/github/funba/.paperclip/deploy-main checkout --detach origin/main

# 2. Restart all workers + scheduler (chord changes affect all of them)
for svc in worker-ingest worker-metrics worker-reduce scheduler; do
  launchctl kickstart -k gui/$(id -u)/app.funba.$svc
done
```

### Metric backfill via CLI:

```bash
# Backfill a single metric across all games
.venv/bin/python -m tasks.dispatch metric-backfill --metric single_quarter_team_scoring

# Backfill a single game
.venv/bin/python -m tasks.dispatch game 0022500826
```

### Troubleshooting:

```bash
# Redis healthy?
redis-cli ping

# Check queue depths
redis-cli llen ingest
redis-cli llen metrics
redis-cli llen reduce

# Worker consuming tasks?
tail -50 ~/Library/Logs/funba/worker-metrics-stderr.log

# Purge stuck tasks from a queue
redis-cli del metrics
```

### Docker Compose (test environment only):

`docker-compose.yml` is kept for local testing with Redis, matching production
broker semantics. It is NOT used in production. To run: copy `.env.example` to
`.env`, adjust `NBA_DB_URL` if needed, and use `docker compose up -d`.

---

## End-to-End Verification

```bash
# 1. Confirm gunicorn (not dev_server) owns port 5001
lsof -nP -iTCP:5001 -sTCP:LISTEN
# Expected: COMMAND=Python, PID=<gunicorn master>, LISTEN on 127.0.0.1:5001
# Verify process: ps -p <PID> -o args= | grep gunicorn

# 2. Web app is up
curl -s -o /dev/null -w "Health: %{http_code}\n" http://localhost:5001/api/health

# 3. Tunnel is connected
cloudflared tunnel info funba

# 4. Public HTTPS
curl -A 'Codex' -s -o /dev/null -w "HTTPS: %{http_code}\n" https://funba.app/
```

The app allows the `Codex` diagnostic User-Agent; a generic external curl can
be blocked by bot rules or Cloudflare. `/api/health` is public in the app, but
Cloudflare rules still apply. Verify ordinary browser access as well.

### PageView query performance

Run migration `l9m0n1o2p3q4` (or a later head) to add the IP/time, user-agent/time,
and path/crawler/visitor indexes. MySQL builds them online with concurrent
reads and writes; a bounded metadata-lock timeout fails the migration instead
of leaving requests queued indefinitely. Check and resolve long-running
PageView transactions before retrying if that timeout occurs.

Request-time crawler history checks remain enabled by default. Each MySQL
history SELECT has a 75 ms execution limit and a request stops starting history
checks after a shared 250 ms budget. These limits exclude connection-pool and
network waits. `FUNBA_CRAWLER_HISTORY_LOOKUPS=0` disables only the historical
heuristics during an incident; static bot rules remain active.

---

## Mac Studio: MySQL Backup (daily, launchd)

The backup job runs daily at 02:00 via launchd service `app.funba.backup`.
- Script: `db/backup_mysql.sh` (installed to `~/Library/Scripts/funba/` — see below)
- Output: `backups/nba_data_YYYYMMDD_HHMMSS.sql.gz` (gitignored symlink → `~/Library/Application Support/funba/backups/`)
- Retention: 7 days (auto-pruned by the script)
- Logs: `~/Library/Logs/funba/` (backup-stdout.log, backup-stderr.log, backup_mysql.log)

### macOS TCC note

On macOS Ventura+, launchd agents cannot read shell script files or list directories
in `~/Documents` without Full Disk Access (TCC restriction). Two paths are used to
work around this:

| Asset | Path | Why |
|---|---|---|
| Backup script | `~/Library/Scripts/funba/backup_mysql.sh` | Library paths are TCC-free; launchd can read and exec here |
| Backup files | `~/Library/Application Support/funba/backups/` | TCC-free; launchd can list for retention pruning |
| Project symlink | `funba/backups -> ~/Library/Application Support/funba/backups/` | Makes backups visible under the project dir |
| Logs | `~/Library/Logs/funba/` | TCC-free |

### Install / update (first time and after any change to backup_mysql.sh):

```bash
bash ~/Documents/github/funba/db/install_backup_agent.sh
```

This script:
1. Copies `db/backup_mysql.sh` → `~/Library/Scripts/funba/backup_mysql.sh`
2. Creates/migrates `~/Library/Application Support/funba/backups/`
3. Creates the `funba/backups` symlink
4. Writes the plist to `~/Library/LaunchAgents/app.funba.backup.plist`
5. Reloads the launchd job

### Service management:

```bash
# Check status
launchctl list app.funba.backup

# Run immediately (manual backup)
launchctl kickstart gui/$(id -u)/app.funba.backup

# Stop / disable
launchctl unload ~/Library/LaunchAgents/app.funba.backup.plist

# Re-enable
launchctl load ~/Library/LaunchAgents/app.funba.backup.plist
```

### Verify a successful backup run:

```bash
# Wait a few minutes after kickstart, then:
tail -20 ~/Library/Logs/funba/backup-stdout.log
# Expected last lines:
#   Backup complete: .../nba_data_YYYYMMDD_HHMMSS.sql.gz (NNNm)
#   Pruning backups older than 7 days...
#   Pruned N old backup(s)
#   === Backup finished ===

launchctl list app.funba.backup
# Expected: "LastExitStatus" = 0

ls -lh ~/Documents/github/funba/backups/
# Expected: symlink shows backup files
```

### Manual backup:

```bash
bash ~/Documents/github/funba/db/backup_mysql.sh
ls -lh ~/Documents/github/funba/backups/
```

---

## Startup Checklist (after Mac Studio reboot)

1. Power recovery: `pmset -g | grep autorestart` — should show `autorestart 1`
2. FileVault: `fdesetup status` — if enabled, the machine must still be unlocked after power loss
3. MySQL: `sudo launchctl print system/homebrew.mxcl.mysql` — should show `state = running`
4. Redis: `sudo launchctl print system/homebrew.mxcl.redis` — should show `state = running`
5. Web app: `sudo launchctl print system/app.funba.web` — should show `state = running`
6. Web watchdog: `sudo launchctl print system/app.funba.web-watchdog` — should show recent successful runs
7. Tunnel: `sudo launchctl print system/app.funba.cloudflared` — should show `state = running`
8. Backup job: `launchctl list app.funba.backup` — should show `"LastExitStatus" = 0` (no PID between runs; it exits after each dump)
9. Workers: `sudo launchctl print system | grep app.funba.worker` — all four workers should be present
10. Scheduler: `sudo launchctl print system/app.funba.scheduler` — should show `state = running`

`app.funba.web`, `app.funba.web-watchdog`, `app.funba.cloudflared`, Redis,
MySQL, scheduler, and workers have `RunAtLoad`/`KeepAlive` in the system launchd
domain. They are not tied to the user GUI session, but FileVault can still block
unattended boot before disk unlock.

---

## Troubleshooting

### Site returns 502 / 503
```bash
lsof -nP -iTCP:5001 -sTCP:LISTEN          # What's on port 5001?
curl http://localhost:5001/api/health       # Is gunicorn responding?
sudo launchctl print system/app.funba.web               # state?
sudo launchctl print system/app.funba.web-watchdog      # last run?
sudo launchctl print system/app.funba.cloudflared       # state?
```
If a stale process (dev_server, web.app) is holding port 5001, kill it and kickstart gunicorn:
```bash
kill <stale-pid>
sudo launchctl kickstart -k system/app.funba.web
```

### Tunnel disconnected
```bash
cloudflared tunnel info funba              # Check connections
sudo launchctl kickstart -k system/app.funba.cloudflared
```

### cert.pem missing / tunnel login expired
```bash
cloudflared tunnel login                   # Re-auth in browser
```

---

## Rollback

If Cloudflare Tunnel needs to be disabled:
1. `sudo launchctl bootout system/app.funba.cloudflared`
2. Re-add `funba.app` block to droplet Caddyfile (see Option A below)
3. Re-load autossh: `launchctl load ~/Library/LaunchAgents/app.funba.tunnel.plist`
4. Update DNS at Porkbun: `funba.app A 209.38.71.231`

---

## Option A: Reverse SSH Tunnel + Caddy (Retired — kept for rollback reference)

The autossh tunnel and Caddy `funba.app` block were disabled on 2026-03-15 after
Cloudflare Tunnel went live. To re-enable:

```
User → funba.app (A record → 209.38.71.231)
     → Caddy on droplet (TLS via Let's Encrypt)
     → localhost:19001 on droplet
     → autossh tunnel → localhost:5001 on Mac Studio
```

### Tunnel service:
```bash
launchctl list app.funba.tunnel
launchctl kickstart -k gui/$(id -u)/app.funba.tunnel   # restart
```

### Caddy config on droplet:
```
funba.app {
    reverse_proxy 127.0.0.1:19001
}
```

### DNS for Option A:
```
funba.app   A   209.38.71.231   TTL 300
```
