# Funba Deployment Runbook

## Git Workflow (Required)

Use the company delivery workflow:

- Implement on a ticket feature branch, not on `origin/main`.
- Keep exactly one GitHub PR per ticket.
- If the work does not fit cleanly in one PR, split it into child tickets before continuing.
- After review approval, merge or cherry-pick into `origin/main`.
- DevOps deploys only the latest `origin/main`, never a feature branch.

The remote is `https://github.com/xidui/funba.git` (private repo). Every completed code change must be pushed, but feature branches stay on the PR until mainline integration.

**History note:** Commit `9920d56` introduced a 2.98 GB SQL dump (`funba_nba_data_20260225_174416.sql`) that was subsequently removed in `d5e3dc0`. When the push backlog was first cleared (2026-03-15), `git filter-repo` was used to excise the blob from history before force-pushing. Future large data files (> 50 MB) must be gitignored and never committed.

---

## Architecture Overview (Option B — Cloudflare Tunnel)

```
User → funba.app (Cloudflare DNS, proxied)
     → Cloudflare Edge (global PoPs, auto-TLS)
     → cloudflared tunnel (runs on Mac Studio, launchd LaunchAgent)
     → localhost:5001 on Mac Studio
     → gunicorn web app (4 workers, launchd LaunchAgent)
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

## Session Behavior

The current Mac Studio app services are user `LaunchAgent`s under `~/Library/LaunchAgents/`.

- `app.funba.web`
- `app.funba.cloudflared`
- `app.funba.backup`

This means:

- They keep running while the screen is locked.
- They stop on logout because the `gui/$(id -u)` launchd domain is torn down.
- Promoting them to true system `LaunchDaemon`s requires `sudo` access to `/Library/LaunchDaemons` and is safer only after runtime assets are moved out of TCC-protected paths such as `~/Documents`.

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

# 5 — Start service
launchctl load ~/Library/LaunchAgents/app.funba.cloudflared.plist
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
It runs from the deploy worktree:

```bash
/Users/yuewang/Documents/github/funba/.paperclip/deploy-main
```

That path is the `WorkingDirectory` in `~/Library/LaunchAgents/app.funba.web.plist`.

This means a normal `git push origin main` does **not** update the running app by itself.
Deploying the latest `main` requires two explicit steps:

```bash
# 1. Update the deploy worktree to the target commit
git -C /Users/yuewang/Documents/github/funba/.paperclip/deploy-main checkout --detach <commit-or-origin/main>

# 2. Restart the launchd web service
launchctl kickstart -k gui/$(id -u)/app.funba.web
```

If the deploy worktree is not updated first, the restarted service will continue
running the old code even when `origin/main` is newer.

### Service management:

```bash
# Check status
launchctl list app.funba.web

# Start
launchctl load ~/Library/LaunchAgents/app.funba.web.plist

# Stop
launchctl unload ~/Library/LaunchAgents/app.funba.web.plist

# Restart
launchctl unload ~/Library/LaunchAgents/app.funba.web.plist
launchctl load ~/Library/LaunchAgents/app.funba.web.plist
```

### Logs:
- Access log: `logs/web-app-5001.log`
- Error log: `logs/web-app-5001-error.log`
- Stdout: `logs/web-app-5001-stdout.log`
- Stderr: `logs/web-app-5001-stderr.log`

### Environment variables (set in plist):
| Variable     | Value                                            |
|--------------|--------------------------------------------------|
| `NBA_DB_URL` | `mysql+pymysql://root@localhost/nba_data`        |

To override, edit `~/Library/LaunchAgents/app.funba.web.plist` → `EnvironmentVariables`.

Secrets (API keys, etc.) must NOT be committed to git. See local `SECRETS.md` (gitignored).

---

## Mac Studio: Cloudflare Tunnel

### Service management:

```bash
# Check status
launchctl list app.funba.cloudflared

# Start
launchctl load ~/Library/LaunchAgents/app.funba.cloudflared.plist

# Stop
launchctl unload ~/Library/LaunchAgents/app.funba.cloudflared.plist

# Restart
launchctl unload ~/Library/LaunchAgents/app.funba.cloudflared.plist
launchctl load ~/Library/LaunchAgents/app.funba.cloudflared.plist
```

### Logs:
- `logs/cloudflared-stdout.log`
- `logs/cloudflared-stderr.log`

---

## Mac Studio: Celery Workers + RabbitMQ (Docker Compose)

The async pipeline (game ingestion, metric computation, backfill) runs via Celery
workers inside Docker containers, with RabbitMQ as the message broker.

### Architecture:

```
Web app (publish metric / daily scheduler)
     → RabbitMQ (amqp://guest:guest@localhost:5672)
     → worker-ingest (Queue: ingest, 4 concurrency)
     → worker-line-score (Queue: line_score, 4 concurrency)
     → worker-metrics (Queue: metrics, 50 concurrency)
     → MySQL (shared with web app)
```

### Docker Compose file: `docker-compose.yml`

| Service | Image | Queue | Purpose |
|---------|-------|-------|---------|
| `rabbitmq` | `rabbitmq:3-management` | — | Message broker (management UI at `localhost:15672`, guest/guest) |
| `worker-ingest` | Built from `Dockerfile` | `ingest` | Game data ingestion (box scores, PBP, shots) |
| `worker-line-score` | Built from `Dockerfile` | `line_score` | Official line-score backfill |
| `worker-metrics` | Built from `Dockerfile` | `metrics` | Metric computation and backfill |
| `scheduler` | Built from `Dockerfile` | — | Celery Beat (daily cron for new games) |

Workers use `.env` for environment variables (NBA_DB_URL, API keys, etc.).
Each worker sets `DB_POOL_SIZE=1` to limit MySQL connections per forked process.

### Service management:

```bash
# Check status
docker compose ps

# Start all services
docker compose up -d

# Restart workers after code changes (rebuild images first)
docker compose build worker-ingest worker-line-score worker-metrics
docker compose up -d worker-ingest worker-line-score worker-metrics

# Restart just one service
docker compose restart worker-metrics

# View logs
docker compose logs -f worker-metrics
docker compose logs -f worker-ingest
docker compose logs -f worker-line-score

# Stop all
docker compose down
```

### After code changes:

Workers run from the Docker image, not the live repo. After pushing code that
affects `metrics/`, `tasks/`, or `db/`, rebuild and restart:

```bash
docker compose build worker-ingest worker-line-score worker-metrics scheduler
docker compose up -d worker-ingest worker-line-score worker-metrics scheduler
```

### Metric backfill via CLI (bypasses Celery):

For one-off backfills without Celery, use the dispatch CLI directly:

```bash
# Backfill a single metric across all games
.venv/bin/python -m tasks.dispatch --metric single_quarter_team_scoring

# Backfill a single game
.venv/bin/python -m tasks.dispatch --game 0022500826
```

### Troubleshooting:

```bash
# RabbitMQ healthy?
docker compose exec rabbitmq rabbitmq-diagnostics ping

# Worker consuming tasks?
docker compose logs --tail 50 worker-metrics

# Purge stuck tasks from a queue
docker compose exec rabbitmq rabbitmqctl purge_queue metrics
```

---

## End-to-End Verification

```bash
# 1. Confirm gunicorn (not dev_server) owns port 5001
lsof -nP -iTCP:5001 -sTCP:LISTEN
# Expected: COMMAND=Python, PID=<gunicorn master>, LISTEN on 127.0.0.1:5001
# Verify process: ps -p <PID> -o args= | grep gunicorn

# 2. Web app is up
curl -s -o /dev/null -w "Flask: %{http_code}\n" http://localhost:5001/

# 3. Tunnel is connected
cloudflared tunnel info funba

# 4. Public HTTPS
curl -s -o /dev/null -w "HTTPS: %{http_code}\n" https://funba.app/
```

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

1. MySQL: `brew services list | grep mysql` — should show `started`
2. Web app: `launchctl list app.funba.web` — should show PID
3. Tunnel: `launchctl list app.funba.cloudflared` — should show PID
4. Backup job: `launchctl list app.funba.backup` — should show `"LastExitStatus" = 0` (no PID between runs; it exits after each dump)
5. Docker / Celery: `docker compose ps` — rabbitmq (healthy), worker-ingest, worker-metrics, scheduler should all be Up

Both `app.funba.web` and `app.funba.cloudflared` have `RunAtLoad + KeepAlive` so they
start automatically after login, restart on crash, and stay up while the screen is
locked. They do not survive a full logout because they are `LaunchAgent`s, not system
`LaunchDaemon`s.

---

## Troubleshooting

### Site returns 502 / 503
```bash
lsof -nP -iTCP:5001 -sTCP:LISTEN          # What's on port 5001?
curl http://localhost:5001/                  # Is gunicorn up?
launchctl list app.funba.web               # PID?
launchctl list app.funba.cloudflared       # PID?
```
If a stale process (dev_server, web.app) is holding port 5001, kill it and kickstart gunicorn:
```bash
kill <stale-pid>
launchctl kickstart -k gui/$(id -u)/app.funba.web
```

### Tunnel disconnected
```bash
cloudflared tunnel info funba              # Check connections
launchctl kickstart -k gui/$(id -u)/app.funba.cloudflared
```

### cert.pem missing / tunnel login expired
```bash
cloudflared tunnel login                   # Re-auth in browser
```

---

## Rollback

If Cloudflare Tunnel needs to be disabled:
1. `launchctl unload ~/Library/LaunchAgents/app.funba.cloudflared.plist`
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
