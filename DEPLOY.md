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

## Initial Setup (one-time, interactive — requires your terminal)

These steps require a browser and your Cloudflare account. Run them once on Mac Studio:

### Step 1 — Login to Cloudflare

```bash
cloudflared tunnel login
```

This opens a browser. Authorize the domain `funba.app` on your Cloudflare account.
On success, `~/.cloudflared/cert.pem` is created.

### Step 2 — Create the named tunnel

```bash
cloudflared tunnel create funba
```

Note the Tunnel ID printed (UUID format, e.g. `a1b2c3d4-...`). Then:

```bash
# Edit config to add tunnel ID and credentials file
nano ~/.cloudflared/config.yml
```

Uncomment and fill in these two lines:
```yaml
tunnel: <TUNNEL_ID>
credentials-file: /Users/yuewang/.cloudflared/<TUNNEL_ID>.json
```

### Step 3 — Configure DNS (via Cloudflare)

```bash
cloudflared tunnel route dns funba funba.app
```

This creates a CNAME at Cloudflare: `funba.app → <TUNNEL_ID>.cfargotunnel.com`.
Make sure `funba.app` is managed by Cloudflare (nameservers pointing to Cloudflare).

If Porkbun still manages DNS, either:
- Transfer nameservers to Cloudflare (recommended), or
- Manually add CNAME `funba.app → <TUNNEL_ID>.cfargotunnel.com` in Porkbun (disable proxy in Porkbun, Cloudflare handles TLS on its edge)

### Step 4 — Start cloudflared as a service

```bash
launchctl load ~/Library/LaunchAgents/app.funba.cloudflared.plist
```

### Step 5 — Verify

```bash
# Check tunnel is connected
cloudflared tunnel info funba

# Check app is reachable via tunnel
curl -s -o /dev/null -w "HTTPS: %{http_code}\n" https://funba.app/
```

---

## Droplet: Remove funba.app from Caddy (after tunnel is live)

Once Cloudflare Tunnel is active, remove the funba.app block from the droplet:

```bash
ssh root@209.38.71.231
# Edit /etc/caddy/Caddyfile — remove the funba.app { ... } block
caddy reload --config /etc/caddy/Caddyfile
```

Also stop and disable the autossh tunnel on Mac Studio:

```bash
launchctl unload ~/Library/LaunchAgents/app.funba.tunnel.plist
```

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
# 1. Web app is up
curl -s -o /dev/null -w "Flask: %{http_code}\n" http://localhost:5001/

# 2. Tunnel is connected
cloudflared tunnel info funba

# 3. Public HTTPS
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
curl http://localhost:5001/                  # Is gunicorn up?
launchctl list app.funba.web               # PID?
launchctl list app.funba.cloudflared       # PID?
```
Restart whichever is down.

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

## Option A: Reverse SSH Tunnel + Caddy (Fallback / Current interim)

If Cloudflare Tunnel is not yet set up, the reverse SSH tunnel is in place as fallback:

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
