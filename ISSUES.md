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
