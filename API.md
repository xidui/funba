# Funba API Reference

Internal APIs for the content pipeline. All endpoints are **localhost-only** — requests must originate from `127.0.0.1` (not via Cloudflare tunnel). No authentication token needed.

Base URL: `http://localhost:5001`

---

## Data API

Read-only endpoints for NBA game data and metrics. Used by Paperclip to gather context for content generation.

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
      "url": "https://funba.app/games/0022501066"
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

Get all metrics triggered by a single game, ranked by noteworthiness. Unlike `/api/data/metrics/triggered` (which deduplicates across all games on a date), this returns every triggered metric for the specific game.

**Response:**

```json
{
  "game_id": "0022501066",
  "metrics": [
    {
      "metric_key": "ot_winner_max_deficit",
      "metric_name": "OT Winner Max Deficit",
      "scope": "game",
      "entity": "Tyrese Haliburton",
      "entity_id": "1630169",
      "entity_type": "player",
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
  "priority": 30,
  "status": "draft",
  "llm_model": "claude-sonnet-4-6",
  "variants": [
    {
      "title": "[funba] 雷霆大胜率联盟第二！",
      "content_raw": "帖子正文...",
      "audience_hint": "thunder fans",
      "destinations": [
        {"platform": "hupu", "forum": "thunder"},
        {"platform": "reddit", "forum": "r/thunder"}
      ]
    },
    {
      "title": "[funba] 本赛季哪支球队最能碾压对手？",
      "content_raw": "中立角度正文...",
      "audience_hint": "general nba",
      "destinations": [
        {"platform": "hupu", "forum": "nba"}
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
| `priority` | int | no | 50 | 0-20 historic, 20-50 notable, 50-80 interesting |
| `status` | string | no | "draft" | Initial status: `draft`, `in_review`, `approved`, `archived` |
| `llm_model` | string | no | null | Which model generated the content |
| `variants` | object[] | no | [] | Audience-specific content variants |
| `variants[].title` | string | yes | | Post title |
| `variants[].content_raw` | string | yes | | Post body (markdown) |
| `variants[].audience_hint` | string | no | null | e.g. "thunder fans", "general nba" |
| `variants[].destinations` | object[] | no | [] | Suggested delivery targets |
| `variants[].destinations[].platform` | string | yes | | `hupu`, `reddit`, `discord`, `twitter` |
| `variants[].destinations[].forum` | string | no | null | Platform-specific target, e.g. `thunder`, `nba`, `r/nba` |

**Response:**

```json
{"ok": true, "post_id": 42, "variant_ids": [101, 102]}
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
  "variants": [
    {
      "id": 101,
      "title": "[funba] 雷霆大胜率联盟第二！",
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

---

## Typical Paperclip Workflow

```
1. GET  /api/data/games?date=2026-03-28           → get today's games
2. GET  /api/data/metrics/triggered?date=2026-03-28 → get noteworthy metrics
3. GET  /api/data/games/{id}/boxscore              → get detailed stats
4. GET  /api/data/metrics/{key}/top                → get rankings
5. [Paperclip generates content with LLM]
6. POST /api/content/posts                         → create SocialPost + variants + deliveries
7. [Paperclip publishes to Hupu via tools/hupu_post.py]
8. POST /api/content/deliveries/{id}/status        → report published_url or error
```

---

## Hupu Publishing Tool

Paperclip uses `tools/hupu_post.py` (in the funba repo) for Hupu browser automation:

```bash
# Check login status
python -m tools.hupu_post check

# Post (dry run)
python -m tools.hupu_post post --title "标题" --content "正文" --forum thunder

# Post (submit for real)
python -m tools.hupu_post post --title "标题" --content "正文" --forum thunder --submit

# Available forums: nba (179), cba (346), thunder (129)
```

Cookie file: `.hupu_cookies.json` (refresh with `python -m tools.hupu_post login --chrome-profile "Profile 1"`).

---

## Error Responses

All endpoints return JSON errors:

```json
{"error": "not_found"}     // 404
{"error": "admin_only"}    // 403 (not from localhost)
{"error": "date required"} // 400
```
