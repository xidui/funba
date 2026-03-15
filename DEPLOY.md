# Funba Deployment Runbook

## Architecture Overview

```
User → funba.app (Porkbun DNS → 209.38.71.231)
     → Caddy (DigitalOcean droplet, auto TLS)
     → localhost:19001 on droplet
     → reverse SSH tunnel (autossh, persistent)
     → localhost:5001 on Mac Studio
     → Flask/gunicorn web app
```

The Mac Studio hosts all data (MySQL) and compute. The DigitalOcean droplet acts
as a public entry point with TLS termination via Caddy.

---

## Machines

| Machine     | Role                        | Access                    |
|-------------|-----------------------------|--------------------------  |
| Mac Studio  | App server, DB, compute     | Local / SSH tunnel        |
| Droplet     | Public proxy, TLS endpoint  | `ssh root@209.38.71.231`  |

---

## DNS

Domain `funba.app` is registered at **Porkbun**.

Required DNS record (set at Porkbun):
```
funba.app   A   209.38.71.231   TTL 300
```

To update: log into [porkbun.com](https://porkbun.com) → Domain Management → DNS → edit the A record.

---

## Droplet: Caddy Configuration

File: `/etc/caddy/Caddyfile`

```
funba.app {
    reverse_proxy 127.0.0.1:19001
}
```

Caddy handles HTTPS automatically via Let's Encrypt (no manual cert management needed).

### Reload Caddy after config changes:
```bash
ssh root@209.38.71.231
caddy validate --config /etc/caddy/Caddyfile
caddy reload --config /etc/caddy/Caddyfile
```

---

## Mac Studio: Reverse SSH Tunnel

An `autossh` service maintains a persistent reverse tunnel so the droplet can reach
the Flask app on Mac Studio.

### Tunnel details:
- Tunnel command: `autossh -M 0 -N -R 127.0.0.1:19001:localhost:5001 root@209.38.71.231`
- Forwarding: `droplet:localhost:19001` → `mac:localhost:5001`
- Managed by: launchd service `app.funba.tunnel`
- Plist location: `~/Library/LaunchAgents/app.funba.tunnel.plist`
- Log: `logs/tunnel.log`

### Service management:
```bash
# Check status
launchctl list app.funba.tunnel

# Start / restart
launchctl kickstart -k gui/$(id -u)/app.funba.tunnel

# Stop
launchctl stop app.funba.tunnel

# Re-enable after editing plist
launchctl unload ~/Library/LaunchAgents/app.funba.tunnel.plist
launchctl load ~/Library/LaunchAgents/app.funba.tunnel.plist
```

The service starts automatically at user login (`RunAtLoad = true`, `KeepAlive = true`).

### If the tunnel drops:
autossh will reconnect automatically. To force reconnect:
```bash
launchctl kickstart -k gui/$(id -u)/app.funba.tunnel
```

---

## Mac Studio: Web App (Flask)

### Running the app

The app runs via a `screen` session managed manually.

**Start (production mode):**
```bash
cd /Users/yuewang/Documents/github/funba
screen -dmS funba_web zsh -lc "env FUNBA_WEB_PORT=5001 FUNBA_WEB_HOST=127.0.0.1 FUNBA_WEB_DEBUG=0 \
  ./.venv/bin/python -m web.dev_server >> logs/web-app-5001.log 2>&1"
```

**Check if running:**
```bash
screen -ls | grep funba_web
curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/
```

**Attach to session:**
```bash
screen -r funba_web
```
Detach: `Ctrl-A D`

**Stop:**
```bash
screen -S funba_web -X quit
```

### Environment variables
| Variable           | Default       | Description                  |
|--------------------|---------------|------------------------------|
| `FUNBA_WEB_PORT`   | `5000`        | Flask listen port (use 5001) |
| `FUNBA_WEB_HOST`   | `127.0.0.1`   | Flask listen host            |
| `FUNBA_WEB_DEBUG`  | (not set)     | Set `0` for production       |
| `NBA_DB_URL`       | mysql+pymysql://root@localhost/nba_data | MySQL connection |

Secrets (API keys, etc.) must NOT be committed to git. See `SECRETS.md` if it exists.

---

## End-to-End Verification

### From Mac Studio:
```bash
# 1. Check Flask is up
curl -s -o /dev/null -w "Flask: %{http_code}\n" http://localhost:5001/

# 2. Check tunnel is up
launchctl list app.funba.tunnel | grep PID

# 3. Check droplet can reach Flask via tunnel
ssh root@209.38.71.231 "curl -s -o /dev/null -w 'Tunnel: %{http_code}\n' http://127.0.0.1:19001/"
```

### After DNS propagation:
```bash
curl -s -o /dev/null -w "HTTPS: %{http_code}\n" https://funba.app/
```

---

## Startup Checklist (after Mac Studio reboot)

1. MySQL service running: `brew services list | grep mysql`
2. Flask app started: `screen -r funba_web` or start it (see above)
3. SSH tunnel: `launchctl list app.funba.tunnel` — should show PID
4. Test via droplet: `ssh root@209.38.71.231 "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:19001/"`

---

## Troubleshooting

### Site returns 502 Bad Gateway
Caddy is up but can't reach `localhost:19001` on the droplet. Tunnel is down.
→ Reconnect: `launchctl kickstart -k gui/$(id -u)/app.funba.tunnel`
→ Check Flask is running: `curl -s -o /dev/null -w "%{http_code}" http://localhost:5001/`

### Site returns 503 / connection refused
Flask app is down on Mac Studio.
→ Restart Flask screen session (see above).

### HTTPS cert errors
Caddy handles Let's Encrypt automatically. If `funba.app` DNS doesn't point to droplet,
cert issuance will fail. Verify A record at Porkbun.

### Tunnel won't connect (key issues)
SSH key used: `~/.ssh/id_ed25519`
→ Verify: `ssh root@209.38.71.231 echo ok`
→ If key changes, update `~/.ssh/known_hosts`: `ssh-keyscan -H 209.38.71.231 >> ~/.ssh/known_hosts`

---

## Future: Production Hardening

- Replace Flask dev server with **gunicorn**: `gunicorn -w 4 -b 127.0.0.1:5001 web.app:app`
- Replace screen session with a launchd service for the web app
- Consider Cloudflare Tunnel (Option B) as zero-ops alternative to autossh:
  1. `brew install cloudflared`
  2. `cloudflared tunnel login` (browser auth to Cloudflare account)
  3. `cloudflared tunnel create funba`
  4. Configure `~/.cloudflared/config.yml` with `url: http://localhost:5001`
  5. Update `funba.app` DNS at Porkbun: ALIAS → `<tunnel-id>.cfargotunnel.com`
  6. `cloudflared service install` (launchd auto-start)
  7. Remove autossh plist + Caddy funba.app block from droplet
