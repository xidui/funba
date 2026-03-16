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
- `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND`
- `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

Keep actual values only in local machine config and local `SECRETS.md`. Never commit secrets.

## Delivery Rules

- Follow the company ticket branch/worktree workflow. Do not implement directly on `origin/main`.
- Keep exactly one GitHub PR per code-change ticket.
- If the task does not fit in one PR, split it into child tickets before implementation.
- Deploy only the latest `origin/main`, never a feature branch.
