# Funba Deployment Runbook

## Architecture Overview (Option B — Cloudflare Tunnel)

```
User → funba.app (Cloudflare DNS, proxied)
     → Cloudflare Edge (global PoPs, auto-TLS)
     → cloudflared tunnel (runs on Mac Studio, launchd)
     → localhost:5001 on Mac Studio
     → gunicorn web app (4 workers, launchd)
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

Secrets (API keys, etc.) must NOT be committed to git. See `SECRETS.md` if it exists.

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

## Startup Checklist (after Mac Studio reboot)

1. MySQL: `brew services list | grep mysql` — should show `started`
2. Web app: `launchctl list app.funba.web` — should show PID
3. Tunnel: `launchctl list app.funba.cloudflared` — should show PID

Both `app.funba.web` and `app.funba.cloudflared` have `RunAtLoad + KeepAlive` so they
start automatically at login and restart on crash.

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
