# Funba API Reference

Internal APIs for the content pipeline. All endpoints are **localhost-only** — requests must originate from `127.0.0.1` (not via Cloudflare tunnel). No authentication token needed.

Base URL: `http://localhost:5001`

---

## Data API

Read-only endpoints for NBA game data and metrics. Used by Paperclip to gather context for content generation.

Entity page URLs returned by Funba data/admin APIs should be treated as canonical public URLs.
Do not hand-compose `/games/<game_id>`, `/players/<player_id>`, or `/teams/<team_id>` links in
generated content; use canonical URLs, or rewrite them to the localized `/cn/...` equivalent while
keeping the same slug path.

### GET /api/data/games

Get all games for a date.

**Query parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `date` | string | yes | ISO date, e.g. `2026-03-28` |

**Response:**

```json
{
  "date": "2026-03-28",
  "games": [
    {
      "game_id": "0022501066",
      "season": "22025",
      "home_team": "IND",
      "road_team": "LAC",
      "home_team_id": "1610612754",
      "road_team_id": "1610612746",
      "home_score": 113,
      "road_score": 114,
      "winner": "LAC",
      "overtime": false,
      "url": "https://funba.app/games/20260327-lac-ind"
    }
  ]
}
```

---

### GET /api/data/games/{game_id}/boxscore

Get full box score for a game (team totals + player lines).

**Response:**

```json
{
  "game_id": "0022501066",
  "game_date": "2026-03-27",
  "home_team": "IND",
  "road_team": "LAC",
  "home_score": 113,
  "road_score": 114,
  "teams": [
    {
      "team": "IND", "team_id": "1610612754",
      "pts": 113, "fgm": 42, "fga": 88,
      "fg3m": 12, "fg3a": 35,
      "ftm": 17, "fta": 22,
      "reb": 44, "ast": 25, "tov": 13
    }
  ],
  "players": [
    {
      "name": "Tyrese Haliburton", "player_id": "1630169",
      "team": "IND",
      "pts": 28, "reb": 4, "ast": 12,
      "min": 36, "starter": true
    }
  ]
}
```

---

### GET /api/data/games/{game_id}/pbp

Get play-by-play for a specific period (last 30 plays).

**Query parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `period` | int | no | 4 | Period number (1-4 regulation, 5+ OT) |

**Response:**

```json
{
  "game_id": "0022501066",
  "period": 4,
  "plays": [
    {
      "time": "2:30",
      "score": "108 - 110",
      "margin": "2",
      "description": "Haliburton 25' 3PT Jump Shot (28 PTS)"
    }
  ]
}
```

---

### GET /api/data/games/{game_id}/metrics

Get the shared game-metrics payload used by both the game page and the content pipeline.

The response includes:

- `game_metrics`: game-scope metric rows for this exact game (same data family shown in the game page metrics section)
- `triggered_player_metrics`: player season-aggregate metrics this game advanced
- `triggered_team_metrics`: team season-aggregate metrics this game advanced

**Response:**

```json
{
  "game_id": "0022501066",
  "game_metrics": {
    "season": [...],
    "season_extra": [...]
  },
  "triggered_player_metrics": [...],
  "triggered_team_metrics": [...]
}
```

---

### GET /api/data/metrics/{metric_key}/top

Get top N entities for a metric, ranked by value.

**Query parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `season` | string | no | all | Season ID, e.g. `22025` |
| `limit` | int | no | 10 | Max results (capped at 100) |

**Response:**

```json
{
  "metric_key": "blowout_rate",
  "results": [
    {
      "rank": 1,
      "entity": "OKC",
      "entity_type": "team",
      "entity_id": "1610612760",
      "value": 0.52,
      "value_str": "52.0%",
      "season": "22025"
    }
  ]
}
```

---

### GET /api/data/metrics/triggered

Get metrics triggered by games on a date, ranked by noteworthiness (best percentile first).

**Query parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `date` | string | yes | ISO date, e.g. `2026-03-28` |

**Response:**

```json
{
  "date": "2026-03-28",
  "metrics": [
    {
      "metric_key": "ot_winner_max_deficit",
      "metric_name": "OT Winner Max Deficit",
      "scope": "game",
      "entity": "Tyrese Haliburton",
      "entity_id": "1630169",
      "entity_type": "player",
      "game_id": "0022501066",
      "value": 15.0,
      "value_str": "15",
      "rank": 1,
      "total": 42,
      "rank_pct": 0.024,
      "notable": true,
      "metric_url": "https://funba.app/metrics/ot_winner_max_deficit"
    }
  ]
}
```

`notable` is true when `rank_pct <= 0.25` (top 25%).

---

## Content API

CRUD for the SocialPost content pipeline. Used by Paperclip to create posts after generation and report delivery status after publishing.

### POST /api/content/posts

Create a SocialPost with variants and delivery destinations.

**Request body:**

```json
{
  "topic": "本赛季大胜率排行分析",
  "source_date": "2026-03-28",
  "source_metrics": ["blowout_rate"],
  "source_game_ids": ["0022501066"],
  "analysis_issue_identifier": "XIX-659",
  "priority": 30,
  "status": "draft",
  "llm_model": "claude-sonnet-4-6",
  "images": [
    {
      "slot": "img1",
      "type": "screenshot",
      "file_path": "/tmp/funba_assets/flagg_player_page.png",
      "target": "https://funba.app/players/1642843",
      "note": "弗拉格球员页截图"
    },
    {
      "slot": "img2",
      "type": "web_search",
      "file_path": "/tmp/funba_assets/flagg_game_photo.jpg",
      "query": "Luka Doncic postgame celebration Mavericks",
      "note": "东契奇庆祝照"
    }
  ],
  "variants": [
    {
      "title": "智趣NBA: 雷霆大胜率联盟第二！",
      "content_raw": "帖子正文...",
      "audience_hint": "thunder fans",
      "destinations": [
        {"platform": "hupu", "forum": "thunder"},
        {"platform": "reddit", "forum": "r/thunder"}
      ]
    },
    {
      "title": "智趣NBA: 本赛季哪支球队最能碾压对手？",
      "content_raw": "中立角度正文...",
      "audience_hint": "general nba",
      "destinations": [
        {"platform": "hupu", "forum": "湿乎乎的话题"}
      ]
    },
    {
      "title": "雷霆这场赢球，真正夸张的是攻防压制力",
      "content_raw": "小红书图文正文...",
      "audience_hint": "xiaohongshu nba note",
      "destinations": [
        {"platform": "xiaohongshu", "forum": "graph_note"}
      ]
    }
  ]
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `topic` | string | yes | | Post topic description |
| `source_date` | string | yes | | ISO date of the games this post is about |
| `source_metrics` | string[] | no | [] | Metric keys used as source material |
| `source_game_ids` | string[] | no | [] | Game IDs referenced |
| `analysis_issue_id` | string | no | null | Optional Paperclip game-analysis issue ID to link this post back to the triggering game-analysis ticket |
| `analysis_issue_identifier` | string | no | null | Optional Paperclip game-analysis issue identifier such as `XIX-659`; recommended for agent-created posts spawned from a game-analysis ticket |
| `priority` | int | no | 50 | 0-20 historic, 20-50 notable, 50-80 interesting |
| `status` | string | no | "draft" | Initial status: `draft`, `ai_review`, `in_review`, `approved`, `archived` |
| `llm_model` | string | no | null | Which model generated the content |
| `images` | object[] | no | [] | Agent-prepared image assets referenced by `[[IMAGE:slot=...]]` placeholders |
| `images[].slot` | string | yes | | Slot name such as `img1` |
| `images[].type` | string | yes | | Provenance label such as `web_search`, `screenshot`, `ai_generated`, `player_headshot` |
| `images[].file_path` | string | yes | | Local path to an already-prepared image file on the Funba machine |
| `images[].query` | string | no | null | Optional provenance metadata when the agent sourced the image via web search |
| `images[].target` | string | no | null | Optional provenance metadata when the agent captured a screenshot |
| `images[].prompt` | string | no | null | Optional provenance metadata when the agent created an AI image |
| `images[].player_id` | string | no | null | Optional provenance metadata for an official headshot source |
| `images[].player_name` | string | no | null | Reviewer-facing provenance context |
| `images[].note` | string | no | null | Chinese note shown in admin review |
| `images[].is_enabled` | bool | no | true | Whether the image should start enabled in the pool |
| `variants` | object[] | no | [] | Audience-specific content variants |
| `variants[].title` | string | yes | | Post title |
| `variants[].content_raw` | string | yes | | Post body (markdown) |
| `variants[].audience_hint` | string | no | null | e.g. "thunder fans", "general nba" |
| `variants[].destinations` | object[] | no | [] | Suggested delivery targets |
| `variants[].destinations[].platform` | string | yes | | `hupu`, `reddit`, `discord`, `twitter`, `xiaohongshu` |
| `variants[].destinations[].forum` | string | no | null | Platform-specific target, e.g. `雷霆专区`, `湿乎乎的话题`, `r/nba` |

Image ownership notes:

- The calling agent is responsible for generating, collecting, searching, downloading, or screenshotting image assets before calling `POST /api/content/posts`.
- Funba stores the provided files into its managed post media directory and records the metadata.
- Semantic keep/disable decisions for still-enabled images belong to the `Content Reviewer` workflow through the admin image-review endpoints.

**Response:**

```json
{
  "ok": true,
  "post_id": 42,
  "variant_ids": [101, 102],
  "workflow": {
    "enabled": true,
    "issue_id": "9f4d...",
    "issue_identifier": "XIX-123",
    "issue_status": "in_review",
    "owner_label": "Reviewer",
    "last_synced_at": "2026-03-28T12:35:00",
    "sync_error": null
  }
}
```

---

### GET /api/content/posts

List social posts.

**Query parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `status` | string | no | all | Filter by status |
| `date` | string | no | all | Filter by source_date |
| `limit` | int | no | 50 | Max results (capped at 200) |
| `offset` | int | no | 0 | Pagination offset |

**Response:**

```json
{
  "total": 15,
  "posts": [
    {
      "id": 42,
      "topic": "本赛季大胜率排行分析",
      "source_date": "2026-03-28",
      "status": "draft",
      "priority": 30,
      "created_at": "2026-03-28T12:30:00"
    }
  ]
}
```

---

### GET /api/admin/content/{post_id}

Get full post detail with all variants and deliveries. (Uses admin session auth — works from localhost.)

**Response:**

```json
{
  "id": 42,
  "topic": "本赛季大胜率排行分析",
  "source_date": "2026-03-28",
  "source_metrics": ["blowout_rate"],
  "source_game_ids": ["0022501066"],
  "status": "draft",
  "priority": 30,
  "admin_comments": [],
  "llm_model": "claude-sonnet-4-6",
  "created_at": "2026-03-28T12:30:00",
  "workflow": {
    "enabled": true,
    "issue_id": "9f4d...",
    "issue_identifier": "XIX-123",
    "issue_status": "in_review",
    "owner_label": "Reviewer",
    "last_synced_at": "2026-03-28T12:35:00",
    "sync_error": null
  },
  "variants": [
    {
      "id": 101,
      "title": "智趣NBA: 雷霆大胜率联盟第二！",
      "content_raw": "帖子正文...",
      "audience_hint": "thunder fans",
      "deliveries": [
        {
          "id": 201,
          "platform": "hupu",
          "forum": "thunder",
          "status": "pending",
          "content_final": null,
          "published_url": null,
          "published_at": null,
          "error_message": null
        }
      ]
    }
  ]
}
```

Some posts may be metric-page placeholder workflows. You can identify them from the Funba admin comments / linked issue brief:

- treat the existing variant as a placeholder draft to replace, not final copy
- read the issue thread plus Funba admin comments for the LLM brief and digging direction
- keep season/view framing in the generated content only; it is not a persistent SocialPost metadata field
- add any needed destinations through the admin destination API before moving the post to `in_review`

---

### POST /api/content/deliveries/{delivery_id}/status

Update a delivery's status after publishing (or on failure). Called by Paperclip after attempting to publish.

**Request body:**

```json
{
  "status": "published",
  "published_url": "https://bbs.hupu.com/12345678.html",
  "content_final": "最终发布的内容（可选）",
  "error_message": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `status` | string | yes | `pending`, `publishing`, `published`, `failed` |
| `published_url` | string | no | Final URL of the published post |
| `content_final` | string | no | Platform-rendered content (if different from raw) |
| `error_message` | string | no | Error details if failed |

**Response:**

```json
{"ok": true}
```

---

## Admin CRUD API

These endpoints are also available from localhost. Used by the admin kanban UI and can be called programmatically.

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/admin/content/{post_id}/update` | Update topic, status, priority |
| POST | `/api/admin/content/{post_id}/comment` | Add admin comment `{text}` |
| POST | `/api/admin/content/{post_id}/delete` | Delete post + variants + deliveries |
| POST | `/api/admin/content/{post_id}/variants/{variant_id}/update` | Update variant title/content/audience |
| POST | `/api/admin/content/{post_id}/variants/{variant_id}/destinations` | Add delivery destination `{platform, forum}` |
| POST | `/api/admin/content/{post_id}/paperclip/sync` | Pull latest Paperclip issue status + comments into Funba |
| POST | `/api/admin/content/{post_id}/images` | Add one prepared image asset `{slot, type, file_path, ...}` to an existing post |
| POST | `/api/admin/content/{post_id}/images/{image_id}/replace` | Replace one existing image asset with a new prepared file |
| GET | `/api/admin/content/{post_id}/image-review-payload` | Get variants plus image-pool payload for reviewer/agent image review |
| POST | `/api/admin/content/{post_id}/image-review/apply` | Apply structured image review decisions `{review_source, summary?, image_decisions[]}` |
| POST | `/api/admin/metrics/{metric_key}/deep-dive-post` | Admin-only trigger for a metric-page placeholder post + Paperclip handoff |

Metric deep-dive trigger body:

```json
{
  "selected_view_label": "2024-25 Regular Season",
  "current_season_label": "2025-26 Regular Season",
  "metric_page_url": "/metrics/blowout_rate?season=22024"
}
```

This endpoint is used by the Funba admin metric page. It creates a placeholder `SocialPost`, mirrors a brief to Paperclip, and assigns the issue to `Content Analyst`.

### Paperclip Workflow Bridge

When the Paperclip env vars are configured, Funba mirrors workflow signals into Paperclip:

- comments added in `/admin/content` are mirrored to the linked Paperclip issue
- `draft -> ai_review` hands the post to the configured Content Reviewer agent
- `ai_review -> draft` requests revision from the configured Content Analyst
- `ai_review -> in_review` hands the post to the configured human review user
- `in_review -> draft` requests revision from the configured content analyst
- `approved` hands the post to the configured delivery publisher
- Paperclip issue comments can be synced back into Funba via `POST /api/admin/content/{post_id}/paperclip/sync`

---

## Typical Paperclip Workflow

```
1. GET  /api/data/games?date=2026-03-28           → get today's games
2. GET  /api/data/metrics/triggered?date=2026-03-28 → get noteworthy metrics
3. GET  /api/data/games/{id}/boxscore              → get detailed stats
4. GET  /api/data/metrics/{key}/top                → get rankings
5. [Paperclip generates content with LLM]
6. POST /api/content/posts                         → create SocialPost + variants + deliveries
7. [Paperclip publishes to Hupu via social_media/hupu/post.py]
8. POST /api/content/deliveries/{id}/status        → report published_url or error
```

---

## Hupu Publishing Tool

Paperclip uses `social_media/hupu/post.py` for Hupu browser automation:

```bash
# Check login status
python -m social_media.hupu.post check

# Post (dry run)
python -m social_media.hupu.post post --title "标题" --content "正文" --forum "雷霆专区"

# Post (submit for real)
python -m social_media.hupu.post post --title "标题" --content "正文" --forum "76人专区" --submit

# Examples:
#   `nba` / `NBA版` -> `湿乎乎的话题`
#   CBA版 -> cba
#   any NBA team forum can be passed as its Chinese label, e.g. 雷霆专区 / 76人专区 / 老鹰专区
#   common English team aliases may also work and are normalized by the tool
```

Cookie file: `social_media/hupu/.hupu_cookies.json`.

Refresh flow:

```bash
python -m social_media.hupu.post login --chrome-profile "Profile 1"
python -m social_media.hupu.post check
```

Notes:

- `check` validates the live Hupu page, not just the presence of `u/us/_CLT` cookie names.
- On macOS, `login` may fail if the current process cannot read `Chrome Safe Storage` from Keychain. In that case the operator must grant Keychain access or use a dedicated interactive browser session.

---

## Xiaohongshu Publishing Tool

Funba can also publish Xiaohongshu graph notes through `social_media/xiaohongshu/post.py`:

```bash
# Check login status
python -m social_media.xiaohongshu.post check

# Dry run
python -m social_media.xiaohongshu.post post \
  --title "标题" \
  --content "正文" \
  --image /tmp/funba_asset.png

# Save to drafts
python -m social_media.xiaohongshu.post post \
  --title "标题" \
  --content "正文" \
  --image /tmp/funba_asset.png \
  --save-draft

# Publish for real
python -m social_media.xiaohongshu.post post \
  --title "标题" \
  --content "正文" \
  --image /tmp/funba_asset.png \
  --submit
```

Cookie file: `social_media/xiaohongshu/.xiaohongshu_cookies.json`.

Notes:

- The publisher currently targets the creator web graph-note flow (`上传图文`) and requires at least one image.
- Pass `--post-id <id>` when publishing from Funba so slot-based image placeholders can resolve from the DB image pool.

---

## Error Responses

All endpoints return JSON errors:

```json
{"error": "not_found"}     // 404
{"error": "admin_only"}    // 403 (not from localhost)
{"error": "date required"} // 400
```
