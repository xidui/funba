# Open Issues

## [METRICS-1] Migrate builtin code metrics to MetricDefinition DB rows

Once the `MetricDefinition` table is introduced for user-defined metrics, builtin metrics should
also be represented as DB rows (`source_type = 'builtin'`) so the full catalog lives in one place.

**Metrics and migratability:**
| Metric | Migratable to rule? |
|--------|-------------------|
| multi_20pt_game | ✅ |
| scoring_consistency | ✅ |
| double_double_rate | ✅ |
| bench_scoring_share | ✅ |
| close_game_record | ✅ |
| franchise_scoring_rank | ✅ |
| clutch_fg_pct | ✅ |
| win_pct_leading_at_half | ⚠️ needs PBP join |
| hot_hand | ❌ streak detection, keep in Python |
| cold_streak_recovery | ❌ streak detection, keep in Python |

**Acceptance criteria:**
- [ ] `MetricDefinition` table with `source_type` field
- [ ] All 10 metrics have a corresponding DB row
- [ ] `/metrics` catalog reads from DB, not in-memory registry
- [ ] Runner handles both `rule` and `builtin` source types
- [ ] No change in computed results

**Depends on:** User-defined metrics milestone

---

## [INFRA-1] On-demand AWS backfill cluster

Run backfills on a disposable AWS cluster (N workers) that spins up on demand and destroys itself when done. Eliminates the need to run overnight locally and enables parallelism across multiple IPs (useful for NBA API rate limits).

**Architecture:**
- SQS queue of game_ids as the work unit
- ECS Fargate Spot tasks as workers (pull from SQS, process, exit)
- RDS MySQL as shared DB (replaces local MySQL)
- ECR for Docker image
- Launcher script: `python launch_backfill.py --season 2024-25 --workers 10`

**Estimated cost per run:** ~$0.50 (Fargate Spot) + RDS ongoing ~$15-30/month

**Work breakdown:**
- [ ] Dockerize backfill script + push to ECR
- [ ] SQS queue + worker pull loop
- [ ] Migrate local MySQL → RDS (mysqldump + restore)
- [ ] ECS task definition + IAM roles + VPC/security groups
- [ ] Launcher CLI script (push game_ids, start tasks, tail logs, teardown)

**Key decision:** Requires migrating DB to RDS — without that, AWS workers can't reach local MySQL.

**Note:** For now, run backfills locally overnight with `--workers 20`. Multiple seasons can be queued sequentially.

---

## [METRICS-2] Cross-season ranking

Currently noteworthiness is a percentile rank within a single season (e.g. "top 5% of players in TS% this season"). A useful extension would be ranking each (entity, season) pair across all seasons — e.g. "LeBron's 2012-13 TS% was the 3rd best single-season mark in franchise history".

**How it would work:**
- After all historical seasons are backfilled, run a cross-season rank pass per metric
- For each metric + entity, sort all (season, value) pairs by value and assign rank
- Store as a separate field or a new `MetricRank` table: `(metric_key, entity_id, season, cross_season_rank, cross_season_total)`
- Surface on player/team pages: "This season ranks #2 all-time for this player"

**Use cases:**
- "Is this Jokic's best double-double season ever?"
- "Is this the Warriors' best home-court advantage season?"
- Highlights truly historic seasons vs. just good ones

**Depends on:** Multiple seasons backfilled (`22024`, `22023`, etc.)

---

## [METRICS-3] User-defined metric MVP follow-ups

The current UI-to-backfill MVP is working, but a few implementation choices are intentionally conservative and can be tightened later.

**Follow-ups:**
- [ ] Avoid per-ingest metric catalog DB reads. `ingest_game()` currently calls the runtime metric loader without reusing an existing session, which adds one small extra query per ingest task.
- [ ] Unify metric catalog assembly on one runtime path. `/metrics` currently combines builtins from the in-memory registry with DB-defined metrics from `MetricDefinition`; behavior is correct, but the implementation is asymmetric.
- [ ] Evaluate whether some DB-backed rule metrics can support an incremental execution mode. They currently run as full recomputes (`incremental = False`) for correctness and simplicity.
- [ ] Revisit new-metric backfill dispatch speed. Publish-triggered backfill currently routes through the ingest queue for every game to preserve artifact checks and pipeline consistency, but it is slower than a direct metrics-queue dispatch for fully hydrated games.

**Non-goal for now:** Do not change behavior until there is a measured need; these are optimization/cleanup items, not correctness blockers.

---

## [METRICS-4] Plan for MetricResult / MetricRunLog growth under user-defined metrics

If end users can create and publish arbitrary metrics with full historical backfills, table growth will become a real concern.

**Why this matters:**
- `MetricResult` grows roughly with `metrics × entities × seasons`
- `MetricRunLog` grows much faster with `metrics × games × touched entities`
- A single player-scoped metric can write ~1M `MetricRunLog` rows across full history
- Today `MetricRunLog` is not just observability; it also supports incremental `force=True` undo-redo via `delta_json`

**Current system constraint:**
- `MetricJobClaim` is the primary per-`(game_id, metric_key)` idempotency gate
- `MetricRunLog` is still part of correctness for incremental recompute
- That means `MetricRunLog` cannot be treated as disposable operational noise without replacing its correctness role

**Questions to settle:**
- [ ] What retention target is acceptable for `MetricRunLog`?
- [ ] Should published user metrics always backfill full history, or should some default to recent seasons only?
- [ ] Do we need per-metric quotas / approval for global historical backfills?
- [ ] At what table size or query latency do we treat this as urgent?

**Options:**
- [ ] Keep the current model, but add retention / archiving for old `MetricRunLog` rows once a safer recompute strategy exists.
- [ ] Partition `MetricRunLog` / `MetricResult` by metric key, season, or time to keep indexes and deletes manageable.
- [ ] Add scoped backfill policies for user metrics (for example: draft = no backfill, published = limited backfill, admin-approved = full history).
- [ ] Move correctness away from `MetricRunLog.delta_json` into a per-game fact table, so run logs can become optional operational history instead of correctness state.
- [ ] Add lightweight monitoring on row counts and growth rate so the issue is driven by actual data, not guesswork.

**Recommended long-term direction:**
- Keep `MetricResult` as the product-facing aggregate table
- Reduce reliance on `MetricRunLog` for correctness
- Introduce per-game canonical metric facts if user-created metric volume starts to grow meaningfully

---

## [METRICS-5] Decide backfill dispatch fairness vs throughput

Current `metric-backfill --metric ...` dispatch enqueues one full-game pass per metric. When multiple metric backfills are launched back-to-back, the queue tends to process them in metric order:

- all games for metric A
- then all games for metric B
- then all games for metric C

This is simple and keeps each task small, but it can make later metrics appear "stuck" even though they are only waiting behind earlier batches.

**Observed behavior:**
- dispatching four scoring metrics enqueued ~52k ingest jobs for each metric separately
- the first metric started progressing immediately
- the later three stayed at `0 done` until their own ingest tasks reached the front of the queue

**Competing options:**
- [ ] Keep metric-major ordering:
  - simpler dispatch model
  - easy to reason about per-metric backfill progress
  - worse fairness when multiple metrics are launched together
- [ ] Switch to game-major interleaving:
  - enqueue `ingest_game(game_id, metric_keys=[...])` with multiple metrics per game
  - better fairness and more visible progress across all selected metrics
  - larger per-task fanout and different queue shape
- [ ] Add a hybrid mode:
  - use metric-major for one-off backfills
  - use game-major when multiple metrics are launched together

**Questions to settle:**
- [ ] Is it more important for all selected metrics to make visible progress together, or for one metric to complete as fast as possible?
- [ ] Does game-major batching improve total wall-clock time in practice, or only improve perceived fairness?
- [ ] Should artifact-light DB rule metrics bypass ingest entirely when artifacts are already known to exist?

**Suggested evaluation:**
- Run the same 4-metric backfill both ways on a representative subset of games
- Compare:
  - first-result latency per metric
  - total completion time
  - queue depth behavior
  - DB load / worker stability

---

## [INFRA-2] Historical season backfill (pre-1983-84)

We have data from 1983-84 (`21983`) onward. The NBA API (`LeagueGameLog` + `BoxScoreTraditionalV3`) has player stats all the way back to **1946-47** (first BAA season) — confirmed working for 1982-83, 1979-80, 1970-71, 1960-61, and 1946-47. That's ~37 missing seasons.

The pipeline already handles pre-1996 seasons correctly (PBP/shot skipped via `_artifacts_available_from_nba_api`). The only blocker is game discovery.

**Root cause:** `_fetch_api_row` in `tasks/ingest.py` and `tasks.dispatch discover` use `LeagueGameFinder`, which returns 0 rows for pre-modern seasons regardless of filters. `LeagueGameLog` works for all eras (confirmed 1946-47 through 2024-25). These are not era-specific endpoints — `LeagueGameLog` is simply more reliable and should replace `LeagueGameFinder` in the discovery path entirely.

**What works today:**
- `BoxScoreTraditionalV3` — full player stats confirmed for all tested eras back to 1946-47 ✅
- `PLUS_MINUS` is null for pre-modern seasons (expected) ✅
- PBP and shot detail correctly skipped for pre-1996 seasons ✅

**What needs fixing:**
- [ ] Replace `LeagueGameFinder` with `LeagueGameLog` in the discovery path (`_fetch_api_row`, `tasks.dispatch discover`)
- [ ] Verify season format mapping: `LeagueGameLog` uses `1982-83`, DB season ID is `21982`

---

## [UI-1] Replace `color-mix()` if older browser support becomes necessary

The metric search/detail UI uses `color-mix()` in CSS for badges and status surfaces. This is fine for modern Chrome/Safari/Firefox, but older browsers may not render those styles correctly.

**Acceptance criteria:**
- [ ] Decide whether older-browser support matters for this project
- [ ] If yes, replace `color-mix()` usage with static color values or a compatible fallback pattern
