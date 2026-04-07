---
name: funba-capture
description: Use when an agent needs compact Funba screenshots for social content. Provides stable CLI commands for player profile, player metrics, game boxscore, async game metrics, and metric detail leaderboard captures. Use this instead of arbitrary full-page screenshots or Hupu-specific tooling.
---

# Funba Capture

Run these commands from the `funba` repo root.

Use the most specific command that matches the panel you want:

- Player summary / bio header:
  - `python -m social_media.funba_capture player-profile --player-id <player_id> --output <local_file>`
- Player metrics cards:
  - `python -m social_media.funba_capture player-metrics --player-id <player_id> --scope season --season <season> --output <local_file>`
  - `python -m social_media.funba_capture player-metrics --player-id <player_id> --scope career --career-type all_regular --output <local_file>`
- Game scoreboard + box score:
  - `python -m social_media.funba_capture game-boxscore --game-id <game_id> --output <local_file>`
- Game triggered metrics panel:
  - `python -m social_media.funba_capture game-metrics --game-id <game_id> --output <local_file>`
- Metric detail page with top leaderboard rows:
  - `python -m social_media.funba_capture metric-page --metric-key <metric_key> --season <season> --top-n 5 --output <local_file>`

Fallback only when there is no better specific command:

- Raw URL:
  - `python -m social_media.funba_capture url --url <funba_url> --output <local_file>`

Rules:

- Prefer the panel-specific commands over the raw URL fallback.
- Do not take arbitrary full-page screenshots when one of these commands fits.
- Save the resulting file locally, then pass it into Funba through `images[].file_path`.
- Keep screenshot types honest in metadata:
  - player header or player metrics: `type: screenshot`
  - game box score or game metrics: `type: screenshot`
  - metric detail leaderboard: `type: screenshot`
