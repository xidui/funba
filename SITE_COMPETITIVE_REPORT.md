# FUNBA Site Positioning Report

Prepared: March 8, 2026

## Executive Summary

FUNBA is strongest where most NBA sites are weakest: it is already built around a **metric engine**, not just a stats table. The best product angle is not to out-NBA.com NBA.com, or out-Basketball-Reference Basketball-Reference. It is to become the place where people can **discover, create, rank, and track interesting basketball metrics** with enough game context to trust the result.

Right now, FUNBA has a clearer "analytics product" direction than most public NBA sites, but it still lacks some baseline tools users expect before they will fully trust or rely on it. The best path is:

1. Add a few high-value parity features that improve trust and usability.
2. Double down on user-defined metrics, metric rankings, and story-like insight discovery.
3. Build a few signature tools competitors do not combine in one place.

## What FUNBA Already Does Well

Based on the current repo, FUNBA already has a more opinionated product shape than a typical stats hobby site:

- **Metric-first architecture**
  - There is a real metric framework, metric registry, runtime, ranking, backfill status, and metric detail UI.
  - The repo currently includes 20 shipped metric definitions across player, team, and game scopes.
  - Evidence: `metrics/framework/*`, `web/app.py`, `web/templates/metrics.html`, `web/templates/metric_detail.html`
- **User-defined metrics**
  - Users can search metrics in natural language, generate a rule from plain English, preview results, save drafts, and publish.
  - Evidence: `/api/metrics/search`, `/api/metrics/generate`, `/api/metrics/preview`, `/api/metrics`, `/api/metrics/<metric_key>/publish` in `web/app.py`; `web/templates/metric_new.html`
- **Data trust / ops visibility**
  - The admin page exposes coverage, backfill progress, active claims, and missing data status. Most fan-facing NBA sites do not expose this at all.
  - Evidence: `/admin`, `/admin/backfill/<season>` in `web/app.py`; `web/templates/admin.html`
- **Good game-context primitives**
  - Game pages include scoreboard context, quarter scoring, play-by-play, score progression, team/player box data, and shot charts.
  - Evidence: `/games/<game_id>` in `web/app.py`; `web/templates/game.html`
- **Recovery for incomplete data**
  - If shot chart data is missing, the app can fetch and backfill it directly from the game page.
  - Evidence: `/games/<game_id>/shotchart/backfill`, `/api/games/<game_id>/shotchart/backfill` in `web/app.py`
- **Useful player and team summary views**
  - Player pages already have career summaries, regular/playoff switching, season logs, shot heatmaps, and analytics insight cards.
  - Team pages already have season record views, game logs, and analytics insight cards.
  - Evidence: `web/templates/player.html`, `web/templates/team.html`

## Current Product Advantage

If you had to describe FUNBA in one line, it should be:

> "A metric creation and insight discovery layer on top of NBA game data."

That is a better position than "another NBA stats site."

### Why this is a real advantage

- **NBA.com Stats** is broad and official, but it is dashboard-heavy and not centered on custom metric creation.
- **Basketball-Reference** is great for historical reference and dense stat lookup, but it does not feel like a metric lab.
- **StatMuse** is great for asking questions, but it is not a transparent metric-building workspace.
- **Cleaning the Glass / PBP Stats / DataBallr** have stronger niche analytics tools, but FUNBA can combine custom metrics, explainability, rankings, and backfill visibility in one product.

FUNBA should lean into:

- discover interesting stats
- create new metrics quickly
- compare entities through ranked metrics
- explain why a result matters
- show the data quality behind the result

## Competitor Comparison

### 1. NBA.com Stats

What it does well:

- Official league data and very broad coverage
- Large filter surface across players, teams, lineups, clutch, tracking, shooting, playtype, defense, hustle, and box score views
- Familiar tables for mainstream users

What FUNBA does better:

- Custom metric creation workflow
- Metric-specific ranking pages and backfill visibility
- More explicit product identity around "interesting findings" rather than raw table navigation

What FUNBA should borrow:

- Better filter controls everywhere
- More split types: clutch, opponent, date ranges, home/away, last X, role/starter, regular season/playoffs side by side
- Lineup and on/off views
- League-average and percentile context on every major stat page

### 2. Basketball-Reference

What it does well:

- Historical depth and trust
- Dense reference pages with per-game, totals, advanced stats, shooting, game logs, schedules, standings, box scores, and play-by-play
- Extremely fast "lookup" workflow for serious fans

What FUNBA does better:

- More modern product opportunity around interactive insight discovery
- Native metric objects instead of static stat pages
- Better foundation for user-generated analytics

What FUNBA should borrow:

- Faster comparison workflow
- More "reference mode" pages: side-by-side player compare, season finder, franchise history, opponent splits
- More exhaustive links between player, team, season, and game pages

### 3. StatMuse

What it does well:

- Natural-language query UX
- Fast answer-oriented experience
- Good habit loop for casual users who want one question answered immediately

What FUNBA does better:

- FUNBA can make the answer inspectable: definition, ranking logic, sample size, backfill status, and related metrics
- FUNBA can go from question -> metric -> publish -> leaderboard, which is more powerful than simple Q&A

What FUNBA should borrow:

- A site-wide ask bar that works from every page
- Suggested follow-up questions
- Better empty states and auto-suggestions
- Shareable answer cards

### 4. Cleaning the Glass

What it does well:

- Strong interpretation layer: percentiles, possession-based context, better signal-to-noise
- Lineup analysis and cleaner contextual stats
- Better explanation of what numbers mean

What FUNBA does better:

- More flexible path to user-defined metrics
- More transparent compute/backfill workflow

What FUNBA should borrow:

- Percentiles and league context
- Better possession-based metrics
- Stronger lineup filters
- More explanation around methodology and metric reliability

### 5. DataBallr / PBP Stats

What they do well:

- Tool-style NBA analytics products
- Matchup tools, WOWY/on-off, lineup combinations, shot-quality views, and detailed tracking filters
- In DataBallr's case, some features connect analysis with visuals and even film

What FUNBA does better:

- Stronger foundation for a unified "metric platform" concept
- Better opportunity to tie custom metrics directly to rankings, pages, and data pipelines

What FUNBA should borrow:

- Matchup explorer
- WOWY / teammate impact explorer
- Last-X-games and trend tooling
- Shot chart interactions that link to clips or at least to play-by-play events

## What Is Nice and Valuable to Add

## Tier 1: Add These First

These are the best near-term additions because they improve trust and make FUNBA feel complete.

### 1. Universal filtering and split controls

Add reusable filters across player, team, game, and metric pages:

- season type
- last X games
- home vs away
- wins vs losses
- starter vs bench
- opponent
- date range
- clutch only
- playoffs vs regular season compare

Why:

- NBA.com users expect filters.
- It makes every existing page more powerful without changing your product identity.

### 2. League-average, percentile, and rank context

For every stat card or metric result, show:

- league average
- percentile
- sample size
- trend vs previous season / previous 10 games

Why:

- Raw values are weak without reference context.
- Cleaning the Glass is strong here.
- This makes FUNBA's metric cards feel more credible immediately.

### 3. Side-by-side comparison pages

Add:

- player vs player
- team vs team
- metric vs metric comparisons

Why:

- This is a common user intent.
- It also makes custom metrics more useful because people can compare outcomes directly.

### 4. Last-X and trend views

Add trend modules for:

- last 5 / 10 / 20 games
- rolling averages
- streaks
- metric movement over time

Why:

- This closes a major gap versus DataBallr and mainstream fan expectations.
- It also gives your insights a more "alive" feel.

### 5. Better homepage framing

Instead of mostly map + standings, add a "What stood out today?" layer:

- most notable metric results
- biggest risers / fallers
- weirdest game
- best custom metric result of the day
- newly published metrics

Why:

- This reinforces your actual product identity.
- Users should understand the site within 10 seconds.

## Tier 2: The Best Differentiation Bets

These are the features most likely to make FUNBA feel unique instead of derivative.

### 1. Daily insight feed

Create a page that automatically surfaces the top notable findings after each ingest:

- "Best bench scoring share tonight"
- "Most extreme clutch drop-off in the last 10 games"
- "Player with the biggest hot-hand gap this month"

Why:

- This is where your metric engine becomes a product, not just infrastructure.
- It creates a repeat-visit loop.

### 2. Explain-this-metric cards

For every metric result, show:

- what it measures
- why the entity ranks there
- what inputs drove the result
- whether the sample is strong or weak
- which comparable players/teams/games are nearby

Why:

- StatMuse wins on convenience; FUNBA can win on understanding.

### 3. Metric collections / watchlists

Let users follow:

- favorite players
- favorite teams
- favorite metrics
- saved custom dashboards

Why:

- This creates retention and personalization.
- It turns metric publishing into an ongoing workflow.

### 4. Matchup intelligence

Build a page for:

- player vs defender
- team vs scheme-style proxy
- opponent-specific metric history
- shot zones vs a specific opponent

Why:

- DataBallr-style matchup exploration is compelling.
- It would pair well with your existing shot chart and play-by-play foundation.

### 5. Metric graph / relationship explorer

Show relationships like:

- high bench scoring share teams vs win rate
- hot hand score vs true shooting
- pace vs comeback win percentage

Why:

- This makes the site exploratory instead of only lookup-driven.
- It is also a strong visual differentiator.

## Tier 3: Nice Additions for Breadth

- Contracts / cap context
- Injury and availability context
- Schedule strength / rest disadvantage
- Franchise timelines and era splits
- Export to CSV / image / share card
- Embeddable cards for social posts
- Draft and prospect pages if you want to broaden the brand later

## What To Avoid

These would dilute the product:

- Trying to replicate every table from NBA.com
- Turning the homepage into a generic scoreboard-first portal
- Adding too many raw stats before adding interpretation
- Hiding metric definitions behind magic

FUNBA should be opinionated. The win is not "more columns." The win is "more signal."

## Best Product Positioning

Recommended positioning:

> FUNBA helps fans and analysts discover what was actually interesting in NBA games, build custom metrics in plain English, and track those insights across players, teams, and games.

That is distinct, credible, and supported by the current codebase.

## Suggested Roadmap

### Next 2-4 weeks

- Add league average + percentile + sample size to metric and stat cards
- Add last-X and rolling trend views
- Redesign homepage around notable metric discoveries
- Add player-vs-player and team-vs-team compare pages

### Next 1-2 months

- Add reusable filters and splits across all pages
- Add saved watchlists and followed metrics
- Add daily insight feed and "best findings today" page
- Add matchup explorer for player/team contexts

### Later

- Add lineup / WOWY tools
- Add film-linked shot or event exploration
- Add contracts / injuries / schedule context if you want broader utility

## Bottom Line

Your site's biggest advantage is not raw data breadth. It is that FUNBA already has the foundation of an **NBA insight engine**:

- ingest data
- compute metrics
- rank outcomes
- let users create new metrics
- expose compute status and backfill confidence

That combination is uncommon. The opportunity is to make that advantage obvious in the UI and support it with a few must-have comparison and filtering tools.

## External Benchmark Sources

Accessed March 8, 2026.

- NBA.com Stats: https://www.nba.com/stats
- NBA.com Players Shooting: https://www.nba.com/stats/players/shooting
- NBA.com Players Clutch Usage: https://www.nba.com/stats/players/clutch-usage
- NBA.com Teams Advanced Leaders: https://www.nba.com/stats/teams/advanced-leaders
- Basketball-Reference player/team/game search results:
  - https://www.google.com/search?q=site%3Abasketball-reference.com+Basketball+Reference+player+pages+game+logs+splits+advanced+stats+shot+charts
  - https://www.google.com/search?q=site%3Abasketball-reference.com%2Fteams%2F+basketball-reference+team+page+roster+schedule+game+logs
  - https://www.google.com/search?q=site%3Abasketball-reference.com%2Fboxscores%2Fpbp+basketball-reference+play-by-play+boxscores
- StatMuse: https://www.statmuse.com/
- Cleaning the Glass lineups update: https://cleaningtheglass.com/site-update-lineups/
- Cleaning the Glass guide pages:
  - https://www.cleaningtheglass.com/stats/guide/player_positions
  - https://www.cleaningtheglass.com/stats/guide/games
- DataBallr: https://databallr.com/
- DataBallr matchup example: https://databallr.com/matchups/203507/giannis-antetokounmpo/1628384/og-anunoby/2024/2026
- PBP Stats Tracking: https://tracking.pbpstats.com/
