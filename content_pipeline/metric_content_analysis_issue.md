TITLE: Metric content analysis — funba — {metric_key} — {metric_name}

Run the metric data series content pass for the specified Funba metric.

**Work mode: Metric Analysis (数据系列)**

Metric key: {metric_key}
Metric name: {metric_name}
Metric name (Chinese): {metric_name_zh}
Metric description: {metric_description}
Metric scope: {metric_scope}
Metric page: {metric_page_url}
Has career variant: {has_career}

## Pre-Computed Highlights

The system has pre-computed the top results across different views. Use these as starting material to find the strongest angle.

{highlights_text}

## Required Work

1. Review the highlights above. Identify the single strongest angle — a record, a surprising leader, a historical trend, or a current-season standout.
2. Capture metric ranking screenshots using the Funba capture CLI. Only use `metric-page` captures:
   ```
   python -m social_media.funba_capture metric-page --metric-key {metric_key} --season <season> --top-n 5 --output <local_file>
   ```
   Capture 2–5 screenshots across different season views relevant to the chosen angle.
3. Create exactly 1 `SocialPost` for this metric. Do not split into multiple posts.
4. Inside that `SocialPost`, create variants for all enabled platforms: **{enabled_platforms}**. Skip any platform not in this list.
5. When calling `POST /api/content/posts`, include `analysis_issue_identifier` set to this issue's identifier.
6. Leave the resulting post in `ai_review` status.
7. Do not publish to external platforms from this issue.

## Platform Writing Rules

Read the metric-series-specific playbooks for each platform:

- Reddit: `agents/social-media/metric-reddit-writing.md`
- Hupu: `agents/social-media/metric-hupu-writing.md`
- Xiaohongshu: `agents/social-media/metric-xiaohongshu-writing.md`

**Important:** These are the metric data series playbooks. Do NOT read the game-analysis playbooks (`funba-*-writing.md`) — those are for a different content series with different rules.

## Image Rules

- Only use metric ranking page screenshots from the capture CLI above
- No game action photos
- No AI-generated images
- No web-sourced images
- Min 2, max 5 screenshots
- Save screenshots locally, then pass them through `images[].file_path`

## Variant Targeting

Look at the top-ranked entities in the highlights:
- If a specific team dominates the ranking, create a team-specific variant for that team's forum/subreddit
- If the finding is league-wide (e.g., season-scope metrics), target general audience channels only
- For player-scope metrics: identify which team(s) the top players belong to

## Angle Selection

Pick the single strongest angle. Prefer:
- Historical records or near-records ("most ever", "first time since")
- Current season surprises (unexpected team/player in top spots)
- Career milestones for well-known players
- Cross-season trends visible in the data

Avoid:
- Routine stats with no surprise factor
- Angles that require game-by-game context (that's the game series)
- Speculation or predictions
