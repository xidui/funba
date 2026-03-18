# FUNBA Metrics Platform — Requirements

## Vision

Transform FUNBA from a data browser into an NBA analytics platform where
**metrics are first-class citizens**. Every game is an event that triggers
computation across a library of metrics. Outstanding results get surfaced
automatically so analysts, fans, or social posts can discover what's
genuinely interesting — not just what's expected.

---

## Core Concepts

### Metric
A named, reproducible computation that runs against stored game data and
produces a typed result for a player, team, or game. A metric always has:

- A **value** (number, percentage, rank, boolean)
- **Context** (sample size, date range, comparison baseline)
- A **noteworthiness score** (0–1, AI-generated)
- A **noteworthiness reason** (one sentence, AI-generated, post-ready)

### Noteworthiness
AI-scored (Claude). After a metric is computed, the value + context are sent
to Claude with a prompt asking: *"Is this statistically or historically
remarkable? Score 0–1 and explain in one sentence why."*

Scores above a configurable threshold (default 0.75) are considered
"highlight-worthy" and surfaced prominently in the UI and daily reports.

### Event Loop
Every time a new game is backfilled into the DB, the metric runner triggers
for all players and the two teams in that game. Results are stored as
snapshots. This runs as an automated daily job after the nightly backfill.

---

## Metric Scopes

| Scope    | Computed per             | Shown on              |
|----------|--------------------------|-----------------------|
| `player` | player × season          | Player page           |
| `team`   | team × season            | Team page             |
| `game`   | player or team × game_id | Game page             |
| `league` | season-wide              | Browse / report page  |

---

## Initial Metric Library (10 metrics)

### Player Metrics

| # | Key | Name | Description | Data needed |
|---|-----|------|-------------|-------------|
| 1 | `hot_hand` | Hot Hand | FG% after 3+ consecutive makes vs. baseline FG% | ShotRecord sequences |
| 2 | `cold_streak_recovery` | Cold Streak Recovery | FG% after 3+ consecutive misses vs. baseline | ShotRecord sequences |
| 3 | `clutch_fg_pct` | Clutch FG% | FG% in final 2 min of Q4 when score margin ≤ 5 | ShotRecord + GamePlayByPlay |
| 4 | `scoring_consistency` | Scoring Consistency | % of games with 20+ points this season | PlayerGameStats |
| 5 | `double_double_rate` | Double-Double Rate | % of games with a double-double this season | PlayerGameStats |
| 6 | `franchise_scoring_rank_regular` | Franchise Scoring Rank (Regular) | Player's all-time rank in regular-season points for their current team | PlayerGameStats |

### Team Metrics

| # | Key | Name | Description | Data needed |
|---|-----|------|-------------|-------------|
| 7 | `win_pct_leading_at_half` | Leads-at-Half Win% | Win % when leading at halftime | GamePlayByPlay |
| 8 | `close_game_record` | Close Game Record | W-L record in games decided by ≤ 5 points | Game + TeamGameStats |
| 9 | `bench_scoring_share` | Bench Scoring Share | % of team points from non-starters | PlayerGameStats |

### League / Game Metric

| # | Key | Name | Description | Data needed |
|---|-----|------|-------------|-------------|
| 10 | `multi_20pt_game` | 20+ Point Contributors | Number of players scoring 20+ in this game | PlayerGameStats |

---

## Data Model

### New DB Tables

```
MetricResult
  id               INT PK AUTO_INCREMENT
  metric_key       VARCHAR(64)   -- e.g. "hot_hand"
  entity_type      ENUM('player','team','game','league')
  entity_id        VARCHAR(50)   -- player_id or team_id
  season           VARCHAR(10)   -- nullable for game-scoped
  game_id          VARCHAR(20)   -- nullable, which game triggered
  value_num        FLOAT         -- primary numeric value (nullable)
  value_str        VARCHAR(255)  -- for text/rank values (nullable)
  context_json     TEXT          -- JSON: sample_size, baseline, etc.
  noteworthiness   FLOAT         -- 0.0–1.0, AI-scored
  notable_reason   TEXT          -- one-sentence AI explanation
  computed_at      DATETIME
  INDEX (metric_key, entity_type, entity_id, season)
  INDEX (noteworthiness, computed_at)
```

MetricDefinitions live in Python code (registered in a central registry).
Future user-defined metrics will add a `MetricDefinition` DB table.

---

## Code Architecture

```
metrics/
  framework/
    base.py          # MetricDefinition ABC, MetricResult dataclass
    registry.py      # central registry of all active metrics
    runner.py        # MetricRunner: runs all metrics for a game trigger
    scorer.py        # AI noteworthiness scoring via Claude API
    daily_job.py     # entry point: find yesterday's games → run metrics
  definitions/
    player/
      hot_hand.py
      cold_streak_recovery.py
      clutch_fg_pct.py
      scoring_consistency.py
      double_double_rate.py
    team/
      win_pct_leading_at_half.py
      close_game_record.py
      bench_scoring_share.py
    game/
      multi_20pt_game.py
```

Each `MetricDefinition` subclass implements:
```python
class HotHand(MetricDefinition):
    key = "hot_hand"
    name = "Hot Hand"
    scope = "player"
    category = "conditional"

    def compute(self, session, entity_id, season) -> MetricResult | None:
        # query ShotRecord sequences, compute conditional FG%
        # return MetricResult or None if insufficient data
```

---

## Noteworthiness Scoring

The `scorer.py` module calls Claude with:
- The metric name + description
- The computed value and context (sample size, baseline comparison)
- Brief entity context (player name, team, season)

Prompt template:
> *"You are scoring the interestingness of an NBA statistic for a social post
> or analyst report. Metric: {name}. Value: {value}. Context: {context}.
> Reply with JSON: {"score": 0.0–1.0, "reason": "one sentence"}."*

Results are stored in `MetricResult.noteworthiness` and `notable_reason`.
Threshold for "highlight-worthy": **≥ 0.75** (configurable env var).

---

## UI Integration

### Player Page
- New "Metrics" section below career summary
- Shows all `scope=player` MetricResults for that player, current season
- Highlights (orange border) any with noteworthiness ≥ 0.75

### Team Page
- New "Metrics" section
- Shows `scope=team` MetricResults for that team, current season

### Game Page
- Inline metric badges on the scoreboard card
- e.g. "3 players scored 20+" shown as a chip

### `/metrics` Browse Page (new route)
- Searchable/filterable table of all MetricResults
- Filters: scope, category, season, noteworthiness threshold
- Sort by noteworthiness descending by default

---

## Daily Report

Run via the Celery event-driven pipeline (see `EVENT_DRIVEN_ARCH.md`).
Celery Beat triggers `ingest_yesterday` hourly, which ingests new games and
fans out metric computation tasks automatically.

Manual trigger for a specific date:
```bash
python -m tasks.dispatch discover --date-from 2026-03-04 --date-to 2026-03-04
```

> **Note:** `metrics/framework/daily_job.py` is **deprecated**. Use `tasks.dispatch` instead.

---

## Future: User-Defined Metrics

- User describes a metric in natural language via the UI
- Backend sends it to Claude: *"Generate a Python MetricDefinition
  subclass that computes: {description}. Available models: {schema}."*
- System validates the result runs without error and returns sensible values
- User sees a preview; if satisfied, saves it
- Admin can promote user metrics to "official" status
- Stored in `MetricDefinition` DB table as serialized code + metadata

---

## Implementation Phases

### Phase 1 — Framework + 10 metrics (MVP)
- DB migration for `MetricResult`
- `base.py`, `registry.py`, `runner.py`
- 10 metric definitions
- `daily_job.py` (**deprecated** — replaced by `tasks.dispatch`)
- AI scorer (Claude API, with graceful fallback if unavailable)

### Phase 2 — UI integration
- Metrics sections on player, team, game pages
- `/metrics` browse page

### Phase 3 — Daily report + automation
- Cron-friendly daily job
- Notable results log / summary

### Phase 4 — User-defined metrics
- Natural language → MetricDefinition via Claude
- Preview + save flow
- Admin promotion workflow
