"""Daily topic generation — LLM-powered social media post candidates."""
from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime, timedelta

from celery import shared_task
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import sessionmaker

from db.models import (
    Game, GameLineScore, GamePlayByPlay, MetricDefinition, MetricResult,
    MetricRunLog, Player, PlayerGameStats, SocialPost, SocialPostDelivery,
    SocialPostVariant, Team, TeamGameStats, TopicPost, engine,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://funba.app"

Session = sessionmaker(bind=engine)

# ---------------------------------------------------------------------------
# Tool definitions for LLM function calling
# ---------------------------------------------------------------------------

_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_metric_top_results",
            "description": "Get the top N results for a metric, with entity names and values.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric_key": {"type": "string", "description": "The metric key, e.g. 'ot_winner_max_deficit'"},
                    "season": {"type": "string", "description": "Season ID like '22025', or omit for current season"},
                    "limit": {"type": "integer", "description": "Number of top results to return (default 10)"},
                },
                "required": ["metric_key"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_game_box_score",
            "description": "Get full box score for a game: team totals and player stats.",
            "parameters": {
                "type": "object",
                "properties": {
                    "game_id": {"type": "string", "description": "Game ID, e.g. '0022501058'"},
                },
                "required": ["game_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_game_play_by_play",
            "description": "Get play-by-play for a specific period of a game.",
            "parameters": {
                "type": "object",
                "properties": {
                    "game_id": {"type": "string", "description": "Game ID"},
                    "period": {"type": "integer", "description": "Period number (1-4 for regulation, 5+ for OT)"},
                },
                "required": ["game_id", "period"],
            },
        },
    },
]

# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


def _exec_get_metric_top_results(session, metric_key: str, season: str | None = None, limit: int = 10) -> str:
    """Return top N results for a metric as JSON string."""
    q = session.query(MetricResult).filter(
        MetricResult.metric_key == metric_key,
        MetricResult.value_num.isnot(None),
    )
    if season:
        q = q.filter(MetricResult.season == season)

    # Determine sort order from MetricDefinition
    md = session.query(MetricDefinition.code_python, MetricDefinition.definition_json).filter(
        MetricDefinition.key == metric_key,
    ).first()
    descending = True  # default
    if md and md.code_python and "rank_order = \"asc\"" in md.code_python:
        descending = False

    if descending:
        q = q.order_by(MetricResult.value_num.desc())
    else:
        q = q.order_by(MetricResult.value_num.asc())

    rows = q.limit(limit).all()

    # Resolve entity names
    player_ids = {r.entity_id for r in rows if r.entity_type == "player"}
    team_ids = {r.entity_id for r in rows if r.entity_type == "team"}
    player_names = {
        p.player_id: p.full_name
        for p in session.query(Player.player_id, Player.full_name).filter(Player.player_id.in_(player_ids)).all()
    } if player_ids else {}
    team_names = {
        t.team_id: t.abbr
        for t in session.query(Team.team_id, Team.abbr).filter(Team.team_id.in_(team_ids)).all()
    } if team_ids else {}

    results = []
    for i, r in enumerate(rows, 1):
        name = player_names.get(r.entity_id) or team_names.get(r.entity_id) or r.entity_id
        results.append({
            "rank": i,
            "entity": name,
            "entity_type": r.entity_type,
            "value": r.value_num,
            "value_str": r.value_str,
            "season": r.season,
        })
    return json.dumps(results, ensure_ascii=False)


def _exec_get_game_box_score(session, game_id: str) -> str:
    """Return box score for a game as JSON string."""
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if not game:
        return json.dumps({"error": "Game not found"})

    team_map = {t.team_id: t.abbr for t in session.query(Team.team_id, Team.abbr).all()}

    team_stats = session.query(TeamGameStats).filter(TeamGameStats.game_id == game_id).all()
    player_stats = session.query(PlayerGameStats, Player.full_name).join(
        Player, Player.player_id == PlayerGameStats.player_id,
    ).filter(PlayerGameStats.game_id == game_id).order_by(
        PlayerGameStats.team_id, PlayerGameStats.pts.desc(),
    ).all()

    teams = []
    for ts in team_stats:
        teams.append({
            "team": team_map.get(ts.team_id, ts.team_id),
            "pts": ts.pts, "fgm": ts.fgm, "fga": ts.fga,
            "fg3m": ts.fg3m, "fg3a": ts.fg3a,
            "ftm": ts.ftm, "fta": ts.fta,
            "reb": ts.reb, "ast": ts.ast, "tov": ts.tov,
        })

    players = []
    for ps, name in player_stats:
        players.append({
            "name": name, "team": team_map.get(ps.team_id, ps.team_id),
            "pts": ps.pts, "reb": ps.reb, "ast": ps.ast,
            "min": ps.min, "starter": bool(ps.starter),
        })

    return json.dumps({"teams": teams, "players": players}, ensure_ascii=False)


def _exec_get_game_play_by_play(session, game_id: str, period: int) -> str:
    """Return PBP for a specific period as JSON string."""
    rows = session.query(GamePlayByPlay).filter(
        GamePlayByPlay.game_id == game_id,
        GamePlayByPlay.period == period,
    ).order_by(GamePlayByPlay.event_num).all()

    plays = []
    for r in rows:
        desc = r.home_description or r.neutral_description or r.visitor_description or ""
        if not desc:
            continue
        plays.append({
            "time": r.pc_time,
            "score": r.score,
            "margin": r.score_margin,
            "description": desc[:120],
        })
    return json.dumps(plays[-30:], ensure_ascii=False)  # last 30 plays to limit tokens


def _dispatch_tool_call(session, name: str, args: dict) -> str:
    """Route a tool call to the appropriate function."""
    if name == "get_metric_top_results":
        return _exec_get_metric_top_results(session, **args)
    elif name == "get_game_box_score":
        return _exec_get_game_box_score(session, **args)
    elif name == "get_game_play_by_play":
        return _exec_get_game_play_by_play(session, **args)
    return json.dumps({"error": f"Unknown tool: {name}"})


# ---------------------------------------------------------------------------
# Context building: games + triggered metrics
# ---------------------------------------------------------------------------


def _build_game_context(session, target_date: date) -> str:
    """Build a text summary of all games and their triggered metrics for a date."""
    games = (
        session.query(Game)
        .filter(Game.game_date == target_date)
        .order_by(Game.game_id)
        .all()
    )
    if not games:
        return ""

    team_map = {t.team_id: t for t in session.query(Team).all()}

    def abbr(tid):
        t = team_map.get(tid)
        return t.abbr if t else str(tid)

    # Check OT games
    ot_game_ids = set()
    ot_rows = (
        session.query(GameLineScore.game_id)
        .filter(
            GameLineScore.game_id.in_([g.game_id for g in games]),
            GameLineScore.ot1_pts.isnot(None),
        )
        .distinct()
        .all()
    )
    ot_game_ids = {r[0] for r in ot_rows}

    # Get all metric run logs for these games (distinct metric_keys per game)
    game_ids = [g.game_id for g in games]
    run_logs = (
        session.query(
            MetricRunLog.game_id,
            MetricRunLog.metric_key,
            MetricRunLog.entity_type,
            MetricRunLog.entity_id,
        )
        .filter(
            MetricRunLog.game_id.in_(game_ids),
            MetricRunLog.produced_result == True,
        )
        .all()
    )

    # Group by game_id → set of (metric_key, entity_type, entity_id)
    game_metrics: dict[str, list[tuple]] = {}
    for rl in run_logs:
        game_metrics.setdefault(rl.game_id, []).append(
            (rl.metric_key, rl.entity_type, rl.entity_id)
        )

    # Get MetricResult rank info for all triggered (metric_key, entity_id, season) combos
    # We need rank within their season — use a subquery approach
    # For simplicity, gather all relevant MetricResult rows and compute rank from value_num
    triggered_keys = {(rl.metric_key, rl.entity_type, rl.entity_id) for rl in run_logs}

    # Bulk fetch MetricResult for these entities
    result_map: dict[tuple, MetricResult] = {}
    if triggered_keys:
        # Fetch in batches by metric_key to avoid huge IN clause
        metric_keys = {k for k, _, _ in triggered_keys}
        entity_ids = {eid for _, _, eid in triggered_keys}
        mr_rows = (
            session.query(MetricResult)
            .filter(
                MetricResult.metric_key.in_(metric_keys),
                MetricResult.entity_id.in_(entity_ids),
                MetricResult.value_num.isnot(None),
            )
            .all()
        )
        for mr in mr_rows:
            result_map[(mr.metric_key, mr.entity_type, mr.entity_id, mr.season)] = mr

    # Get rank/total for each result via counting
    # For each MetricResult, count how many have higher value_num in same (metric_key, season)
    rank_cache: dict[int, tuple[int, int]] = {}  # mr.id -> (rank, total)

    def _get_rank(mr: MetricResult) -> tuple[int, int]:
        if mr.id in rank_cache:
            return rank_cache[mr.id]
        total = session.query(func.count(MetricResult.id)).filter(
            MetricResult.metric_key == mr.metric_key,
            MetricResult.season == mr.season,
            MetricResult.value_num.isnot(None),
        ).scalar() or 0
        better = session.query(func.count(MetricResult.id)).filter(
            MetricResult.metric_key == mr.metric_key,
            MetricResult.season == mr.season,
            MetricResult.value_num > mr.value_num,
        ).scalar() or 0
        rank = better + 1
        rank_cache[mr.id] = (rank, total)
        return rank, total

    # MetricDefinition name lookup
    md_map = {
        md.key: md
        for md in session.query(MetricDefinition).filter(
            MetricDefinition.key.in_({k for k, _, _ in triggered_keys}),
            MetricDefinition.status == "published",
        ).all()
    }

    # Player/team name lookup
    player_ids = {eid for _, et, eid in triggered_keys if et == "player"}
    player_names = {
        p.player_id: p.full_name
        for p in session.query(Player.player_id, Player.full_name).filter(Player.player_id.in_(player_ids)).all()
    } if player_ids else {}

    # Build text
    lines = []
    lines.append(f"# NBA Games on {target_date.isoformat()} ({len(games)} games)\n")

    for g in games:
        road = abbr(g.road_team_id)
        home = abbr(g.home_team_id)
        winner = abbr(g.wining_team_id) if g.wining_team_id else "?"
        rs = g.road_team_score or 0
        hs = g.home_team_score or 0
        margin = abs(hs - rs)
        ot_tag = " (OT)" if g.game_id in ot_game_ids else ""

        lines.append(f"## {road} {rs} - {hs} {home}{ot_tag}")
        lines.append(f"Winner: {winner} | Margin: {margin}")
        lines.append(f"Game URL: {_BASE_URL}/games/{g.game_id}")

        # Metrics triggered by this game, sorted by noteworthiness
        gm = game_metrics.get(g.game_id, [])

        # Deduplicate by metric_key (take most notable entity per metric)
        metric_entries: dict[str, dict] = {}
        for mk, et, eid in gm:
            md = md_map.get(mk)
            if not md:
                continue
            # Skip career variants for brevity
            if mk.endswith("_career"):
                continue

            # Find the MetricResult for this entity
            # Try current season first, then any season
            mr = None
            for season_candidate in [g.season, None]:
                if season_candidate:
                    mr = result_map.get((mk, et, eid, season_candidate))
                    if mr:
                        break
                else:
                    # Find any matching result
                    for key, val in result_map.items():
                        if key[0] == mk and key[1] == et and key[2] == eid:
                            mr = val
                            break

            if not mr or mr.value_num is None:
                continue

            rank, total = _get_rank(mr)
            pct = rank / total if total > 0 else 1.0
            notable = pct <= 0.25

            entity_name = player_names.get(eid) or abbr(eid) or eid

            # Keep only the best entry per metric_key for this game
            existing = metric_entries.get(mk)
            if existing and existing["pct"] <= pct:
                continue

            metric_entries[mk] = {
                "key": mk,
                "name": md.name,
                "entity": entity_name,
                "value_str": mr.value_str or str(mr.value_num),
                "rank": rank,
                "total": total,
                "pct": pct,
                "notable": notable,
                "scope": md.scope,
            }

        # Sort by rank percentile (most notable first)
        sorted_metrics = sorted(metric_entries.values(), key=lambda x: x["pct"])

        if sorted_metrics:
            lines.append("Triggered metrics:")
            for m in sorted_metrics[:15]:  # cap per game
                star = "★" if m["notable"] else "·"
                entity_part = f" for {m['entity']}" if m["scope"] != "game" else ""
                lines.append(
                    f"  {star} {m['name']} = {m['value_str']}{entity_part}"
                    f" — #{m['rank']} of {m['total']}"
                    f" ({_BASE_URL}/metrics/{m['key']})"
                )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# LLM call with tool use loop
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a professional NBA analyst and long-form content writer for funba.app.
You receive a summary of today's NBA games with triggered metric highlights.
Your job: identify the most compelling stories and write detailed social media posts in Chinese.

## Writing Style
- Write like a seasoned NBA analyst on 虎扑 or 小红书 — passionate, data-backed, with real basketball insight
- Use Chinese (中文), keep English player/team names as-is
- Each post should be **800-1500 characters** — a proper article, not a news brief
- Structure each post with:
  1. A hook that grabs attention (why should the reader care?)
  2. The core story with specific stats and context
  3. Historical comparison or broader significance (use tools to look up rankings if needed)
  4. A takeaway or opinion that sparks discussion
- Use markdown formatting: **bold** for key numbers, line breaks for readability
- Include funba.app links naturally in the text (not dumped at the end)
- Add image placeholders where visuals enhance the story: <!-- image: 描述 -->
  Available types: 球员投篮热图, 比赛比分走势, 球队战绩表, 赛季数据对比图

## Content Guidelines
- ONLY use facts from the provided data or tool results — NEVER fabricate statistics
- Use the tools proactively: call get_game_box_score to get player stat lines for storytelling, call get_metric_top_results to build historical context and comparisons
- Generate 3-6 posts, ranked by how interesting they are
- Assign priority: 0-20 (record-breaking/historic), 20-50 (notable/surprising), 50-80 (interesting)
- Focus on stories that would genuinely interest basketball fans, not just stat dumps

Output your final answer as a JSON array (no markdown fences):
[
  {
    "title": "帖子标题",
    "body": "帖子正文 (markdown格式，800-1500字)",
    "priority": 10,
    "metric_keys": ["metric_key_1"],
    "game_ids": ["0022501058"],
    "entity_ids": ["player_id_or_team_id"]
  }
]
"""


def _call_llm_with_tools(session, context: str, model: str) -> list[dict]:
    """Call OpenAI with tool use, loop until final answer."""
    import openai
    client = openai.OpenAI()

    messages = [
        {"role": "system", "content": _SYSTEM_PROMPT},
        {"role": "user", "content": context},
    ]

    max_rounds = 5
    for _ in range(max_rounds):
        response = client.chat.completions.create(
            model=model,
            max_completion_tokens=16384,
            temperature=0.7,
            messages=messages,
            tools=_TOOL_DEFINITIONS,
        )
        choice = response.choices[0]

        # If model wants to call tools
        if choice.finish_reason == "tool_calls" or (choice.message.tool_calls and len(choice.message.tool_calls) > 0):
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)
                logger.info("LLM tool call: %s(%s)", fn_name, fn_args)
                result = _dispatch_tool_call(session, fn_name, fn_args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
            continue

        # Final text response
        text = (choice.message.content or "").strip()
        return _parse_topics_json(text)

    logger.warning("LLM tool use loop exhausted after %d rounds", max_rounds)
    return []


def _parse_topics_json(text: str) -> list[dict]:
    """Parse JSON array from LLM output, stripping markdown fences if present."""
    # Strip markdown code fences
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            stripped = part.strip()
            if stripped.startswith("json"):
                stripped = stripped[4:].strip()
            if stripped.startswith("["):
                text = stripped
                break

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.error("Failed to parse LLM output as JSON: %s", text[:500])
        return []

    if not isinstance(data, list):
        return []

    return data


# ---------------------------------------------------------------------------
# Main generation function
# ---------------------------------------------------------------------------


def generate_daily_topics(target_date_str: str | None = None, force: bool = False) -> dict:
    """Generate topic post candidates for a given date.

    Can be called directly (from admin UI) or as a Celery task.

    Args:
        target_date_str: ISO date string (YYYY-MM-DD). Defaults to yesterday.
        force: If True, delete existing drafts and regenerate.

    Returns:
        dict with generation results.
    """
    from db.llm_models import resolve_llm_model

    if target_date_str:
        target = date.fromisoformat(target_date_str)
    else:
        target = date.today() - timedelta(days=1)

    session = Session()
    try:
        # Idempotency check
        existing = (
            session.query(func.count(TopicPost.id))
            .filter(TopicPost.date == target)
            .scalar() or 0
        )
        if existing > 0 and not force:
            logger.info("Topics already exist for %s (%d posts), skipping.", target, existing)
            return {"date": str(target), "skipped": True, "existing": existing}

        if force and existing > 0:
            session.query(TopicPost).filter(
                TopicPost.date == target,
                TopicPost.status == "draft",
            ).delete()
            session.commit()
            logger.info("Deleted %d draft topics for %s", existing, target)

        # Build context
        context = _build_game_context(session, target)
        if not context:
            logger.info("No games found for %s, skipping topic generation.", target)
            return {"date": str(target), "skipped": True, "reason": "no_games"}

        # Resolve model
        model = resolve_llm_model(session, purpose="generate")
        logger.info("Generating topics for %s using model %s", target, model)

        # Call LLM
        topics = _call_llm_with_tools(session, context, model)
        if not topics:
            logger.warning("LLM returned no topics for %s", target)
            return {"date": str(target), "generated": 0}

        # Save to DB
        now = datetime.utcnow()
        count = 0
        for t in topics:
            title = (t.get("title") or "").strip()
            body = (t.get("body") or "").strip()
            if not title or not body:
                continue
            post = TopicPost(
                date=target,
                title=title,
                body=body,
                priority=int(t.get("priority", 50)),
                status="draft",
                source_metric_keys=json.dumps(t.get("metric_keys", []), ensure_ascii=False),
                source_game_ids=json.dumps(t.get("game_ids", []), ensure_ascii=False),
                source_entity_ids=json.dumps(t.get("entity_ids", []), ensure_ascii=False),
                llm_model=model,
                created_at=now,
                updated_at=now,
            )
            session.add(post)
            count += 1

        session.commit()
        logger.info("Generated %d topics for %s", count, target)
        return {"date": str(target), "generated": count}

    except Exception:
        session.rollback()
        logger.exception("Failed to generate topics for %s", target_date_str)
        raise
    finally:
        session.close()


@shared_task(
    bind=True,
    name="tasks.topics.generate_daily_topics",
    max_retries=2,
    default_retry_delay=300,
)
def generate_daily_topics_task(self, target_date_str: str | None = None) -> dict:
    """Celery wrapper for generate_daily_topics."""
    return generate_daily_topics(target_date_str)


# ---------------------------------------------------------------------------
# Content pipeline: multi-variant SocialPost generation
# ---------------------------------------------------------------------------

_SOCIAL_POST_SYSTEM_PROMPT = """\
You are a professional NBA analyst and content strategist for funba.app.
You receive a summary of today's NBA games with triggered metric highlights.
Your job: identify the most compelling stories and write social media posts with **multiple audience-targeted variants**.

## Writing Style
- Write like a seasoned NBA analyst on 虎扑 or 小红书 — passionate, data-backed, with real basketball insight
- Use Chinese (中文), keep English player/team names as-is
- Each variant should be **800-1500 characters** — a proper article, not a news brief
- Structure each variant with:
  1. A hook that grabs attention (why should the reader care?)
  2. The core story with specific stats and context
  3. Historical comparison or broader significance (use tools to look up rankings if needed)
  4. A takeaway or opinion that sparks discussion
- Use markdown formatting: **bold** for key numbers, line breaks for readability
- Include funba.app links naturally in the text (not dumped at the end)

## Multi-Variant Strategy
For each topic/story, generate **2-4 variants** targeting different audiences:
- **Team-specific fans**: Focus on their team's angle, use "我们"/"我霆" etc.
- **General NBA fans**: Neutral perspective, compare across teams
- **Casual fans**: Simpler language, focus on the wow factor
Each variant should suggest which platforms/forums it's best suited for.

## Content Guidelines
- ONLY use facts from the provided data or tool results — NEVER fabricate statistics
- Use the tools proactively: call get_game_box_score for player stats, get_metric_top_results for rankings
- Generate 3-6 posts (topics), each with 2-4 variants
- Assign priority: 0-20 (record-breaking/historic), 20-50 (notable/surprising), 50-80 (interesting)

Output your final answer as a JSON array (no markdown fences):
[
  {
    "topic": "话题描述 (e.g. 本赛季大胜率排行分析)",
    "priority": 10,
    "metric_keys": ["metric_key_1"],
    "game_ids": ["0022501058"],
    "variants": [
      {
        "title": "[funba] 帖子标题",
        "audience_hint": "thunder fans",
        "content": "���子正文 (markdown, 800-1500字)",
        "suggested_destinations": [
          {"platform": "hupu", "forum": "thunder"},
          {"platform": "reddit", "forum": "r/thunder"}
        ]
      },
      {
        "title": "[funba] 中立角度标题",
        "audience_hint": "general nba",
        "content": "中立角度正文...",
        "suggested_destinations": [
          {"platform": "hupu", "forum": "nba"}
        ]
      }
    ]
  }
]
"""


def _call_llm_for_social_posts(session, context: str, model: str) -> list[dict]:
    """Call LLM with tool use to generate multi-variant social posts."""
    import openai
    client = openai.OpenAI()

    messages = [
        {"role": "system", "content": _SOCIAL_POST_SYSTEM_PROMPT},
        {"role": "user", "content": context},
    ]

    max_rounds = 5
    for _ in range(max_rounds):
        response = client.chat.completions.create(
            model=model,
            max_completion_tokens=16384,
            temperature=0.7,
            messages=messages,
            tools=_TOOL_DEFINITIONS,
        )
        choice = response.choices[0]

        if choice.finish_reason == "tool_calls" or (choice.message.tool_calls and len(choice.message.tool_calls) > 0):
            messages.append(choice.message)
            for tc in choice.message.tool_calls:
                fn_name = tc.function.name
                fn_args = json.loads(tc.function.arguments)
                logger.info("LLM tool call: %s(%s)", fn_name, fn_args)
                result = _dispatch_tool_call(session, fn_name, fn_args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
            continue

        text = (choice.message.content or "").strip()
        return _parse_topics_json(text)

    logger.warning("LLM tool use loop exhausted after %d rounds", max_rounds)
    return []


def generate_social_posts(target_date_str: str | None = None, force: bool = False) -> dict:
    """Generate SocialPost records with variants for a given date.

    Args:
        target_date_str: ISO date string (YYYY-MM-DD). Defaults to yesterday.
        force: If True, delete existing drafts and regenerate.

    Returns:
        dict with generation results.
    """
    from db.llm_models import resolve_llm_model

    if target_date_str:
        target = date.fromisoformat(target_date_str)
    else:
        target = date.today() - timedelta(days=1)

    session = Session()
    try:
        # Idempotency check
        existing = (
            session.query(func.count(SocialPost.id))
            .filter(SocialPost.source_date == target)
            .scalar() or 0
        )
        if existing > 0 and not force:
            logger.info("Social posts already exist for %s (%d posts), skipping.", target, existing)
            return {"date": str(target), "skipped": True, "existing": existing}

        if force and existing > 0:
            # Delete draft social posts and their children
            draft_ids = [
                r[0] for r in session.query(SocialPost.id).filter(
                    SocialPost.source_date == target,
                    SocialPost.status == "draft",
                ).all()
            ]
            if draft_ids:
                variant_ids = [
                    r[0] for r in session.query(SocialPostVariant.id).filter(
                        SocialPostVariant.post_id.in_(draft_ids)
                    ).all()
                ]
                if variant_ids:
                    session.query(SocialPostDelivery).filter(
                        SocialPostDelivery.variant_id.in_(variant_ids)
                    ).delete(synchronize_session=False)
                session.query(SocialPostVariant).filter(
                    SocialPostVariant.post_id.in_(draft_ids)
                ).delete(synchronize_session=False)
                session.query(SocialPost).filter(
                    SocialPost.id.in_(draft_ids)
                ).delete(synchronize_session=False)
                session.commit()
                logger.info("Deleted %d draft social posts for %s", len(draft_ids), target)

        # Build context
        context = _build_game_context(session, target)
        if not context:
            logger.info("No games found for %s, skipping.", target)
            return {"date": str(target), "skipped": True, "reason": "no_games"}

        # Resolve model
        model = resolve_llm_model(session, purpose="generate")
        logger.info("Generating social posts for %s using model %s", target, model)

        # Call LLM
        posts_data = _call_llm_for_social_posts(session, context, model)
        if not posts_data:
            logger.warning("LLM returned no posts for %s", target)
            return {"date": str(target), "generated": 0}

        # Save to DB
        now = datetime.utcnow()
        post_count = 0
        for pd in posts_data:
            topic = (pd.get("topic") or "").strip()
            variants_data = pd.get("variants", [])
            if not topic or not variants_data:
                continue

            sp = SocialPost(
                topic=topic,
                source_date=target,
                source_metrics=json.dumps(pd.get("metric_keys", []), ensure_ascii=False),
                source_game_ids=json.dumps(pd.get("game_ids", []), ensure_ascii=False),
                status="draft",
                priority=int(pd.get("priority", 50)),
                llm_model=model,
                created_at=now,
                updated_at=now,
            )
            session.add(sp)
            session.flush()  # get sp.id

            for vd in variants_data:
                vtitle = (vd.get("title") or "").strip()
                vcontent = (vd.get("content") or "").strip()
                if not vtitle or not vcontent:
                    continue

                sv = SocialPostVariant(
                    post_id=sp.id,
                    title=vtitle,
                    content_raw=vcontent,
                    audience_hint=(vd.get("audience_hint") or "").strip() or None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(sv)
                session.flush()  # get sv.id

                # Create suggested deliveries
                for dest in vd.get("suggested_destinations", []):
                    platform = (dest.get("platform") or "").strip()
                    forum = (dest.get("forum") or "").strip() or None
                    if platform:
                        sd = SocialPostDelivery(
                            variant_id=sv.id,
                            platform=platform,
                            forum=forum,
                            status="pending",
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(sd)

            post_count += 1

        session.commit()
        logger.info("Generated %d social posts for %s", post_count, target)
        return {"date": str(target), "generated": post_count}

    except Exception:
        session.rollback()
        logger.exception("Failed to generate social posts for %s", target_date_str)
        raise
    finally:
        session.close()


@shared_task(
    bind=True,
    name="tasks.topics.generate_social_posts_task",
    max_retries=2,
    default_retry_delay=300,
)
def generate_social_posts_task(self, target_date_str: str | None = None) -> dict:
    """Celery wrapper for generate_social_posts."""
    return generate_social_posts(target_date_str)
