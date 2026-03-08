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

## [UI-1] Replace `color-mix()` if older browser support becomes necessary

The metric search/detail UI uses `color-mix()` in CSS for badges and status surfaces. This is fine for modern Chrome/Safari/Firefox, but older browsers may not render those styles correctly.

**Acceptance criteria:**
- [ ] Decide whether older-browser support matters for this project
- [ ] If yes, replace `color-mix()` usage with static color values or a compatible fallback pattern
