# Metric Series — Reddit Writing Playbook

Use this playbook when generating Funba **metric series** content for Reddit.

## Scope

This document is for the **metric data series** only. Do not mix with game-analysis Reddit rules (`funba-reddit-writing.md`).

## Post Format

Metric series Reddit posts are **image posts** (not text posts):

- ONE screenshot image of the metric ranking page
- Short factual title in English
- No body text
- No links
- No `[OC]` tag
- No source footer

This is the opposite of game-series Reddit posts (which are long-form text analysis). Metric series posts let the data screenshot speak for itself.

## Title

Write a short, factual English title (under 150 characters). State the interesting finding directly.

Good examples:
- `10 140-point games for Miami Heat this season`
- `The 2024-25 season already has more 30-point triple-doubles than any season in NBA history`
- `LeBron James has 47 career 40-point games after age 35 — more than anyone else has after 30`

Bad examples:
- `[OC] An analysis of 140-point games across NBA history` (too formal, has [OC])
- `Check out this crazy stat about the Heat` (clickbait, no data)
- `Source: funba.app/metrics/...` (no links in title)

## Image

Use the metric ranking page screenshot as the post image. The screenshot should clearly show:
- The metric name
- The ranked list (top entries visible)
- The season or time scope

Capture command:
```
python -m social_media.funba_capture metric-page --metric-key <key> --season <season> --top-n 5 --output <file>
```

## Subreddit Targeting

- `r/nba` — always create a variant for general NBA audience
- Team subreddit — create an additional variant when a specific team dominates the metric or the finding is clearly team-relevant. Use canonical names from `social_media/reddit/forums.py`.

Title should be adjusted per subreddit:
- `r/nba`: neutral framing, league-wide context
- Team sub: fan-friendly framing, emphasis on why this matters to that fanbase

## Safety

- Write in English only
- Do not fabricate stats
- Do not include any links (URLs will get the post flagged or removed)
- Do not include body text — image posts with added text look spammy
