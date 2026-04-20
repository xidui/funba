# Funba

NBA data ingestion, metrics generation, and lightweight web analytics for NBA games and players.

## Repo Layout

- `web/`: Flask app, routes, templates, and local dev server entrypoints
- `db/`: SQLAlchemy models, config, migrations helpers, backfill scripts, and backup tooling
- `metrics/`: metric framework plus game/team/player metric definitions
- `tasks/`: Celery task wiring and async job entrypoints
- `tests/`: pytest coverage for auth and metrics
- `alembic/`: schema migrations
- `legacy_files/`: historical scripts; do not extend unless the ticket explicitly targets them

## Setup And Run

- Create a virtualenv and install `requirements.txt`
- Set `NBA_DB_URL`
- Run `alembic upgrade head` before using the app against a fresh database
- Start the web app with `python -m web.app`
- Use `DEPLOY.md` for launchd, tunnel, and production deploy procedures

## Runtime And Secrets

Common env vars used in this repo:

- `NBA_DB_URL`
- `DB_POOL_SIZE`, `DB_MAX_OVERFLOW`
- `FLASK_SECRET_KEY`
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`
- `FUNBA_WEB_HOST`, `FUNBA_WEB_PORT`, `FUNBA_WEB_DEBUG`
- `FUNBA_CURL_ALLOWED_IPS`
- `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND`
- `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`
- `STRIPE_SECRET_KEY`, `STRIPE_PUBLISHABLE_KEY`, `STRIPE_PRO_PRICE_ID`, `STRIPE_WEBHOOK_SECRET`
- `RESEND_API_KEY`
- `PAPERCLIP_API_URL`, `PAPERCLIP_API_KEY`, `PAPERCLIP_COMPANY_ID`
- `PAPERCLIP_FUNBA_PROJECT_ID`
- `PAPERCLIP_CONTENT_ANALYST_AGENT_ID`, `PAPERCLIP_CONTENT_ANALYST_NAME`
- `PAPERCLIP_CONTENT_REVIEWER_AGENT_ID`, `PAPERCLIP_CONTENT_REVIEWER_NAME`
- `PAPERCLIP_DELIVERY_PUBLISHER_AGENT_ID`, `PAPERCLIP_DELIVERY_PUBLISHER_NAME`
- `PAPERCLIP_CONTENT_REVIEW_USER_ID`, `PAPERCLIP_CONTENT_REVIEW_USER_NAME`
- `PAPERCLIP_TIMEOUT_SECONDS`

Keep actual values only in local machine config and local `SECRETS.md`. Never commit secrets.

## Delivery

Follow the company delivery workflow. See [DEPLOY.md](./DEPLOY.md) for deployment targets and instructions.

## Verification Strategy

- **Verification type**: web app (Flask)
- **Verification timing**: post-deploy
- **How to verify**: After DevOps deploys to production, verify the live app responds correctly:
  ```bash
  curl -s -o /dev/null -w "HTTPS: %{http_code}\n" https://funba.app/
  # expect 200
  curl -s -o /dev/null -w "Health: %{http_code}\n" https://funba.app/api/health
  # expect 200 if /api/health endpoint exists, otherwise verify a known page loads
  ```
  Also spot-check: player pages load, team pages load, metrics catalog renders without 500 errors.
- **Environment requirements**: Network access to `https://funba.app`. No device or simulator required.
