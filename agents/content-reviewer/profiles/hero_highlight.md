# Hero Highlight Review Profile

Use this profile for `Hero Highlight` SocialPosts.

## Scope

Hero Highlights are deterministic short social cards generated from one curated game hero snapshot. They are poster-first posts, not long-form articles.

Do not apply the normal long-form requirement for a 10+ image pool. A valid Hero Highlight usually has:

- `poster` for Funba and Twitter
- `poster_ig` only when Instagram generation is enabled

## Required Inputs

Read these before moving the post forward:

- `GET /api/admin/content/{post_id}`
- `GET /api/admin/content/{post_id}/image-review-payload?include_disabled=1`
- `GET /api/admin/content/{post_id}/poster-prompt` when the poster identity, team, jersey, logo, arena, or prompt anchor looks suspicious

## Checks

1. Decode the topic:
   - Format: `Hero Highlight - {game_id} - {scope} - {metric_key} - {entity_id}`.
   - The real topic uses em dash separators. Split on that delimiter from the API payload, not on spaces.
   - For compound player game entity IDs, verify the player belongs to the team shown in the game context. Do not infer team from the opponent or from leaderboard rows.

2. Verify poster identity:
   - player identity
   - current team, jersey colors, logos, and visual anchor
   - opponent context and scoreboard direction
   - `poster` vs `poster_ig` dimensions/slot usage

3. Verify copy:
   - metric value and rank basis
   - rank season/window, including playoff vs regular-season wording
   - game date, teams, score, and winner
   - source metric URL and game URL
   - do not rewrite a `stocks` rank as a blocks rank unless the rank basis is actually blocks

4. Variant status decision:
   - If any high-severity issue remains, leave the post in `ai_review` and comment with the concrete fix needed.
   - If the issue is directly fixable in copy, patch the variant first.
   - If the poster is wrong, do not move the post forward unless a correct replacement already exists and is enabled.
   - If the post passes, move the post to `in_review` through `/api/admin/content/{post_id}/update`.

## Publishing Boundary

Your job still ends by moving the post to `in_review`. Funba may automatically approve and publish Hero variants whose platform is enabled for Hero Card autopublish in the publishing matrix.
