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
