# funba

NBA data ingestion + lightweight analytics project.

This repo was half-finished; this README reflects a revived runnable baseline.

## What this project does

- Ingests NBA metadata and game data using `nba_api`
- Stores raw/derived stats in MySQL via SQLAlchemy models
- Computes simple behavioral shooting metrics (after make / after miss)
- Computes a "pity loss" label from play-by-play end-game lead changes

## Current pipeline (today)

1. **Schema/models** in `db/models.py`
2. **Backfill dimensions**
   - Teams: `db/backfill_nba_teams.py`
   - Players: `db/backfill_nba_player.py`
3. **Backfill game facts**
   - Games + box score + PBP: `db/backfill_nba_games.py`
4. **Backfill shot detail**
   - Seasonal shot detail: `db/backfill_nba_player_shot_detail.py`
5. **Build metrics**
   - `metrics/shot_pct_after_made.py`
   - `metrics/shot_pct_after_miss.py`
   - `metrics/pity_loss.py`

## Quick start

### 1) Setup Python env

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Configure database URL

Set `NBA_DB_URL` (required for all DB access + Alembic):

```bash
export NBA_DB_URL='mysql+pymysql://<user>:<password>@<host>/<db_name>'
```

> Backward-compatible default exists in code for local legacy setups, but env var is recommended.

### 3) Initialize schema

Preferred (migrations):

```bash
alembic upgrade head
```

Fallback (create-all bootstrap):

```bash
python -c "from db.models import init_db; init_db()"
```

### 4) Run ingestion

```bash
python -m db.backfill_nba_teams
python -m db.backfill_nba_player
python -m db.backfill_nba_games 2023-24
```

Targeted backfill (day/season/team/player):

```bash
# Warriors, 2025-26, regular + playoffs
# Default behavior: only process games not fully backfilled yet
# (Game row + game detail + play-by-play)
python -m db.backfill_nba_games_targeted --team-abbr GSW --season 2025-26

# Single day
python -m db.backfill_nba_games_targeted --day 2026-02-10

# Reprocess existing games too
python -m db.backfill_nba_games_targeted --team-abbr GSW --season 2025-26 --include-existing
```

### 5) Run metrics

```bash
python -m metrics.shot_pct_after_made
python -m metrics.shot_pct_after_miss
python -m metrics.pity_loss
```

### 6) Run web pages

```bash
python -m web.app
```

Open `http://127.0.0.1:5000` and browse:
- Player page: `/players/<player_id>` (includes season switch + per-game status table)
- Team page: `/teams/<team_id>` (season win/loss table + current season game status table)
- Game page: `/games/<game_id>` (detailed game/team/player stats)

## Immediate fixes applied in this revival pass

- Added env-based DB config via `NBA_DB_URL` (`db/config.py`)
- Wired Alembic to use same env config (`alembic/env.py`)
- Fixed broken tuple assignments in `backfill_nba_game_detail.py` (Game fields were not being set correctly)
- Fixed team resolution logic to fall back from `canonical_team_id` to `team_id`
- Replaced invalid `raise "..."` patterns with real exceptions
- Fixed pity-loss SQL bug (losing team selection logic)
- Normalized module imports (`from db.models ...`) so `python -m ...` commands work reliably
- Added this runnable root README with end-to-end instructions

## Concrete phased implementation plan

### Phase 0 — Stabilize (done in this pass)
- Shared env-based DB config
- Basic ingestion path runnable from clean env
- Obvious correctness bugs fixed
- README with setup + runbook

### Phase 1 — Data integrity + reproducibility
- Add non-destructive idempotency checks to all backfill scripts
- Add structured logging (JSON/log levels) and progress summaries
- Add minimal smoke tests against a dedicated test DB
- Add row-count/constraint sanity report script

### Phase 2 — Quality + performance
- Batch commits in loaders (reduce per-row commits)
- Add retry/jitter and rate-limit handling for all nba_api endpoints
- Add indexes aligned to query patterns in metrics scripts
- Remove remaining duplicate-prone pathways in shot backfill

### Phase 3 — Productize analytics
- Turn SQL snippets into reusable query/report functions
- Add CLI entrypoints (e.g., `python -m funba.cli ...`)
- Add notebook/report examples with season outputs
- Package as installable project (`pyproject.toml`, lint, CI)

### Phase 4 — Reliability/ops
- Scheduled incremental ingestion job (daily)
- Data freshness + failure alerting
- Optional cache layer for external API responses

## Notes / caveats

- `nba_api` endpoints may be slow or intermittently rate-limited; retries are present in some scripts but not uniformly.
- Existing Alembic history may not fully represent all model evolution from scratch; validate in a fresh DB before production use.
- Some historical seasons may have incomplete shot/PBP data depending on source availability.
