TITLE: Game content analysis — funba — {source_date} — {game_id} — {matchup}

Run the per-game Funba content analysis pass once NBA ingest and metric computation are stable.

Source date: {source_date}
Game ID: {game_id}
Matchup: {matchup}
Season: {season_label}

Required work:
1. Read this game's boxscore, shared game-metrics payload (`/api/data/games/{id}/metrics`), and game detail from Funba localhost APIs.
   - use `game_metrics` for page-equivalent game-scope metrics
   - use `triggered_player_metrics` / `triggered_team_metrics` for season-aggregate signals this game advanced
2. Before drafting, triage the game's candidate signals into `P1` / `P2` / `P3` using the contract in `agents/content-analyst/AGENTS.md`.
   - `P1`: most important, must be consciously handled
   - `P2`: useful support, use when it sharpens the main story
   - `P3`: secondary, omit unless it helps explain a stronger point
   - post this triage into the issue comments / ticket notes so the reviewer and later agents can see it
3. Create exactly 1 strong `SocialPost` for this single game only. Do not broaden into unrelated same-date games and do not split this game into multiple `SocialPost` records.
4. Inside that single `SocialPost`, create variants for all platforms listed in the default target set in your role instructions (`agents/content-analyst/AGENTS.md`). Read the per-platform writing playbooks under `agents/social-media/` for tone, length, and formatting rules. Only create variants for currently enabled platforms: **{enabled_platforms}**. Skip any platform not in this list.
5. When calling `POST /api/content/posts`, include `analysis_issue_identifier` set to this Paperclip issue's identifier so Funba can link the created posts back to this game-analysis ticket.
6. Avoid duplicate angles against existing posts for the same game via `GET /api/content/posts?date=YYYY-MM-DD`.
7. End each post with 6-8 metric / page links. Every metric or page mentioned in the body should appear in that ending section.
8. Leave the resulting posts in Funba in `ai_review` so the Content Reviewer agent can audit them before human review.
9. Do not publish to external platforms from this issue.

## Topic Selection Rules

- This ticket is for one game and should produce one `SocialPost`. Pick the single strongest angle for that game.
- If the game supports multiple useful sub-angles, combine them into one coherent post and express audience differences through variants instead of creating multiple posts.
- Avoid duplicate same-game coverage. If another post already covers the same game with a very similar angle, skip it or choose a materially different angle.
- Do not keep using always-on metrics like common double-doubles / 20+5+5 style triggers as the title hook for the same stars every game.
- Use those routine metrics only when there is a real milestone, streak, leaderboard movement, unusual efficiency, or broader context.
- Prefer titles built around what changed, what is rare, what is newly meaningful, or what reshapes the season narrative.
- If the game triggers a current-season / current-playoff `#1` or tied `#1`, treat it as a `P1` candidate by default.
- `P1` does not automatically mean `title hook`. The analyst must decide whether it is the main story or only an important early support point.
- If a `P1` signal is passed over in favor of a weaker angle, there should be a clear reason in the issue comment / ticket note: weak basketball meaning, redundant with stronger framing, or low fan relevance.
- Apply a freshness test only when freshness is actually observable from the issue, notes, or available APIs.
  - if first hit / newly tied / moved up is explicitly evidenced, it is a strong candidate to write up
  - if freshness is not observable, mark it `unknown` in the note and avoid movement language
  - do not invent rank movement from a post-game `rank` field alone
- Keep source discipline:
  - current-game numbers come from boxscore / game page / play-by-play facts
  - season / playoff significance comes from triggered metrics or metric-top context
  - do not rewrite season-context rankings into fake `今天` / `本场` stat lines

## Image Pool

- Each post must include at least 8 prepared image assets.
- Prepare the image files yourself before calling Funba, then pass them through `images[].file_path`.
- Keep all images tied to the same game.
- Use zero player headshots. Do not use `player_headshot` for this workflow.
- At least 3 images must be real game or arena-action photos from this specific game context.
- At least 4 images must be Funba data screenshots (game page, player page, metric page, ranking page, or other relevant Funba data views).
- At least 1 image must be an AI-generated supporting visual that still matches the same game story.
- For Funba screenshots, use the dedicated capture CLI instead of taking arbitrary full-page screenshots:
  `python -m social_media.funba_capture game-boxscore --game-id <game-id> --output <local-file>`
  `python -m social_media.funba_capture game-metrics --game-id <game-id> --output <local-file>`
  `python -m social_media.funba_capture player-metrics --player-id <player-id> --scope season --season <season> --output <local-file>`
  `python -m social_media.funba_capture metric-page --metric-key <metric-key> --season <season> --top-n 5 --output <local-file>`
- For the required AI-generated supporting visual, use the dedicated image generation CLI instead of ad-hoc SDK code:
  `python -m social_media.funba_imagegen generate --prompt <prompt> --reference-image <real-game-photo> --output <local-file>`
- When possible, pass 1-2 real game photos as `--reference-image` so the generated player pose and scene stay grounded in the actual matchup.
- Reference image assets with slot placeholders like `[[IMAGE:slot=img1]]`.
- Do not submit a post whose writing depends on images unless those image assets are already prepared and included.

Do not publish externally from this issue.
