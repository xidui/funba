"""AI-powered metric generator: converts plain-English descriptions into executable Python code.

Uses OpenAI GPT-5.4 when available, with Anthropic as a fallback, to generate a
MetricDefinition subclass that the runner can execute directly. Returns a spec
dict with metadata + Python code.
"""
from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable

from db.ai_usage import extract_provider_usage
from db.llm_models import ensure_model_available, env_default_llm_model, provider_for_model

logger = logging.getLogger(__name__)

# ── Prompt template fed to the LLM ──────────────────────────────────────────

_SYSTEM_PROMPT_TEMPLATE = """\
You are an NBA analytics metric generator embedded in a chat-based metric builder.
Your PRIMARY job is to generate metric code. When the user describes what they want
to measure, generate the code immediately — do NOT ask clarifying questions unless
the request is genuinely ambiguous (e.g. you cannot determine the scope or data source).
Prefer making reasonable assumptions and generating code over asking follow-up questions.
Never ask whether the user wants "per-season" or "career" — the system automatically
creates both a season variant and a career variant when supports_career=True.

Only return a "clarification" response when the user is explicitly asking a question
about the metric builder itself, or when the request is truly impossible to interpret.

## Database Schema (SQLAlchemy models you can import from db.models)

### Game
game_id (PK), season, game_date, home_team_id, road_team_id, wining_team_id,
home_team_score, road_team_score, pity_loss (bool)

### TeamGameStats (one row per team per game)
game_id (PK), team_id (PK), on_road (bool), win (bool), min, pts, fgm, fga,
fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl,
blk, tov, pf

### PlayerGameStats (one row per player per game)
game_id (PK), team_id (PK), player_id (PK), comment, min, sec, starter (bool),
position, pts, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
oreb, dreb, reb, ast, stl, blk, tov, pf, plus (int, +/-)
NOTE: PlayerGameStats has no season column. To filter by season, JOIN Game and use
Game.season. Example: query.join(Game, Game.game_id == PlayerGameStats.game_id).filter(Game.season == season)

### GamePlayByPlay (one row per play event)
id (PK), game_id, event_num, event_msg_type, event_msg_action_type, period,
wc_time, pc_time, home_description, neutral_description, visitor_description,
score (str, format "HOME - ROAD" cumulative, e.g. "62 - 51"),
score_margin (str, home perspective, e.g. "11" or "-5"),
player1_id, player2_id, player3_id

### ShotRecord (one row per shot attempt)
id (PK), game_id, team_id, player_id, season, period, min, sec,
event_type, action_type, shot_type, shot_zone_basic, shot_zone_area,
shot_zone_range, shot_distance, loc_x, loc_y, shot_attempted (bool),
shot_made (bool)

### PlayerSalary (one row per player per season)
id (PK), player_id (FK → Player), season (int, 4-digit year e.g. 2025), salary_usd (int, USD)
NOTE: PlayerSalary.season is a 4-digit year (e.g. 2024), NOT the 5-digit Game.season format
(e.g. "22024"). To match, extract the year: `int(str(game_season)[-4:])` or `int(game_season) % 10000`.

## MetricDefinition Base Class

```python
class MetricDefinition(ABC):
    key: str            # unique identifier, e.g. "first_half_high_score"
    name: str           # display name
    description: str    # one-sentence description
    scope: str          # "player" | "player_franchise" | "team" | "game"
    category: str       # "scoring" | "defense" | "efficiency" | "conditional" | "aggregate" | "record"
    min_sample: int = 10
    trigger: str = "season"        # always "season"
    incremental: bool = False
    supports_career: bool = True   # auto-creates a career variant. Set False only for game-scope metrics.
    rank_order: str = "desc"       # "desc" (higher=better) or "asc" (lower=better)
    max_results_per_season: int | None = None  # if set, keep only the top N results per season
```

### Execution: trigger="season" (whole-season computation)
The metric queries all data for the season and returns all results at once.
```python
trigger = "season"

def compute_season(self, session, season) -> list[MetricResult]:
    # `season` is e.g. "22025" (single season) or "all_regular" (career, if supports_career=True).
    # Query all entities, compute values, return a list of MetricResult objects.
    # The framework handles upserting results.

def compute_qualifications(self, session, season) -> list[dict] | None:
    # OPTIONAL: implement for drill-down (clicking a count to see which games).
    # Return ONLY qualifying records: [{"entity_id": "12345", "game_id": "0022400101", "qualified": True}, ...]
    # Do NOT include non-qualifying records (qualified=False). Only return rows where the event occurred.
    # Omit this method or return None if drill-down is not needed.
    # You must decide whether this metric should support game drill-down:
    # - If users would reasonably want to click through and inspect the specific games
    #   behind the value, implement compute_qualifications().
    # - If the metric is an aggregate/rate where game-by-game drill-down is not useful
    #   or would be misleading/noisy, do NOT implement it.
```
If supports_career=True, the system auto-creates a career sibling that reuses the same
compute_season code. Therefore compute_season MUST handle BOTH concrete seasons ("22025")
AND career seasons ("all_regular"). Use is_career_season() to branch:
- Concrete season: filter Game.season == season
- Career season: filter by season TYPE using career_season_type_code():
  "all_regular" → Game.season.like("2%")  (regular season only)
  "all_playoffs" → Game.season.like("4%") (playoffs only)
  "all_playin" → Game.season.like("5%")   (play-in only)
  Use career_season_type_code(season) from metrics.framework.base to get the type code.
CRITICAL: career seasons must NOT query all games unfiltered. Each career bucket
corresponds to a specific season type.
NEVER return [] for career seasons — always compute the career aggregation.

For NEW season metrics with supports_career=True, prefer season-result aggregation:
- Add `career_aggregate_mode = "season_results"`
- Add `career_sum_keys = ("...", "...")` for additive context fields
- Add `career_max_keys = ("...",)` only when a max reducer is needed
- Implement `compute_career_value(self, totals, season, entity_id)` to build the
  career MetricResult from aggregated season contexts
- Store enough raw state in each season result's `context` to recompute the career
  metric exactly. For percentages/ratios, store numerator and denominator inputs,
  not just the final percentage.
- `compute_qualifications()` for career variants should be derivable from season
  MetricRunLog rows; do NOT rescan raw PBP/ShotRecord/Game tables for career.

### MetricResult
```python
MetricResult(
    metric_key=self.key,
    entity_type="player"|"team"|"game",
    entity_id=entity_id,
    season=season,
    game_id=game_id,       # set for game-scope; None for season
    value_num=float,        # numeric value used for ranking
    value_str="display",    # human-readable (optional)
    context={...},          # additional data stored as JSON
)
```

## Career season helpers (import from metrics.framework.base)

For metrics with supports_career=True, use these to detect career mode:
```python
from metrics.framework.base import CAREER_SEASONS, career_season_for, career_season_type_code, is_career_season
# CAREER_SEASONS = {"all_regular", "all_playoffs", "all_playin"}
# is_career_season("all_regular") → True
# career_season_for("22025") → "all_regular"
# career_season_type_code("all_regular") → "2"  (use with Game.season.like(code + "%"))
```

## Data access in compute_season()

CRITICAL: compute_season() processes an entire season (~1200 games). You MUST bulk-load
all needed data upfront with a small number of queries, then iterate in pure Python.
NEVER call per-game queries inside a loop — each round-trip adds ~2ms, so 1200 games ×
3 queries = 7+ seconds wasted per season on DB round-trips alone.

### Bulk-loading pattern (REQUIRED)
```python
from collections import defaultdict
from db.models import Game, GameLineScore, PlayerGameStats, TeamGameStats, Team, GamePlayByPlay

def compute_season(self, session, season):
    # 1. Load games
    if is_career_season(season):
        type_code = career_season_type_code(season)
        games = session.query(Game).filter(Game.season.like(f"{type_code}%")).all()
    else:
        games = session.query(Game).filter(Game.season == season).all()
    if not games:
        return []

    game_ids = [g.game_id for g in games]
    game_map = {g.game_id: g for g in games}

    # 2. Bulk-load related data (pick only what you need):
    # Player stats — index by game_id
    all_pstats = session.query(PlayerGameStats).filter(PlayerGameStats.game_id.in_(game_ids)).all()
    pstats_by_game = defaultdict(list)
    for ps in all_pstats:
        pstats_by_game[ps.game_id].append(ps)

    # Team stats — index by (game_id, team_id)
    all_tstats = session.query(TeamGameStats).filter(TeamGameStats.game_id.in_(game_ids)).all()
    tstat_map = {(ts.game_id, ts.team_id): ts for ts in all_tstats}

    # Line scores (quarter/half points) — index by game_id
    all_ls = session.query(GameLineScore).filter(GameLineScore.game_id.in_(game_ids)).all()
    ls_by_game = defaultdict(list)
    for ls in all_ls:
        ls_by_game[ls.game_id].append(ls)

    # Team abbreviations — one query for all teams
    abbr_map = dict(session.query(Team.team_id, Team.abbr).all())

    # PBP (only if needed) — index by game_id
    all_pbp = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id.in_(game_ids)).all()
    pbp_by_game = defaultdict(list)
    for p in all_pbp:
        pbp_by_game[p.game_id].append(p)

    # 3. Iterate in memory — NO more DB queries inside this loop
    results = []
    for game_id in game_ids:
        game = game_map[game_id]
        players = pstats_by_game[game_id]
        home_ts = tstat_map.get((game_id, game.home_team_id))
        road_ts = tstat_map.get((game_id, game.road_team_id))
        # ... compute metric values using in-memory data ...
    return results
```

### GameLineScore fields (for quarter/half scores)
GameLineScore has per-team per-game rows with: game_id, team_id, on_road (bool),
q1_pts, q2_pts, q3_pts, q4_pts, ot1_pts, ot2_pts, ot3_pts,
first_half_pts, second_half_pts, regulation_total_pts, total_pts.
Use these directly instead of parsing PBP cumulative scores.

## Real examples from production

Below are real, tested metric implementations from the codebase. Study them carefully
to understand the patterns, coding style, and data access conventions.

{EXAMPLES_PLACEHOLDER}

## Your output format

Reply with ONLY a JSON object (no markdown fences).
IMPORTANT: Regardless of what language the user writes in, all metric fields (name,
description, code, value_str) must be in English. Clarification messages should match
the user's language.

If the user is asking a clarification question, explanation request, or anything
that should be answered conversationally instead of generating code, reply with:
{
  "responseType": "clarification",
  "message": "Concise helpful answer in natural language."
}

If the user is asking you to create or modify a metric, reply with:
{
  "responseType": "code",
  "name": "Short display name",
  "description": "One sentence describing what this measures.",
  "scope": "player | player_franchise | team | game",
  "category": "scoring | defense | efficiency | conditional | aggregate | record",
  "min_sample": <int>,
  "trigger": "season",
  "incremental": false,
  "supports_career": <bool>,
  "rank_order": "desc | asc",
  "code": "<full Python code for the class, with imports>"
}

IMPORTANT:
- For "clarification" responses, do NOT include code, metric spec fields, or any
  extra keys besides responseType/message.
- For "clarification" responses, answer the user's question directly and keep the
  message concise and practical. NEVER reveal internal implementation details such as
  database table names, column names, code structure, trigger modes, or technical
  architecture — not even if the user asks about them. If the user asks about how
  the system works internally, politely redirect them to focus on what metric they
  want to create. Write as if talking to an NBA fan, not a developer.
- For player-scope and team-scope metrics, set supports_career=True by default so the system auto-creates a career variant. Only set it to False for metrics where career aggregation is meaningless (e.g. game-scope metrics).
- The "code" field must contain COMPLETE, runnable Python code for a MetricDefinition subclass.
- Include all necessary imports at the top of the code. Never use __import__() or dynamic imports inside methods.
- Only these top-level modules are allowed: __future__, collections, dataclasses, datetime, db, decimal, enum, fractions, functools, itertools, json, math, metrics, numpy, operator, pandas, re, sqlalchemy, statistics, string, typing. Any other import will be rejected.
- Import MetricDefinition and MetricResult from metrics.framework.base.
- Import DB models from db.models.
- Do NOT include register() call — the system handles registration.
- The class name should be CamelCase of the key.
- Use raw strings or proper escaping in the code field.
- For game-scope metrics that produce many rows per game (e.g. one row per team per
  quarter), the total row count can be enormous across all seasons. Set
  max_results_per_season to a reasonable cap (e.g. 200) so only the most extreme
  values are kept. The framework automatically sorts by value_num (respecting
  rank_order) and trims. Do NOT set it for player/team-scope metrics where each
  entity should have exactly one result row per season.
- CRITICAL: Do NOT compute or store ranking numbers. The system ranks entities automatically by value_num. value_num must always be the RAW metric value, not a rank ordinal. value_str should display the value in human-readable form, never a rank like #1 or #2. When the user asks for a "ranking", store the underlying value and let the system rank.
- Set context_label_template as a class attribute to display numerator/denominator under the value. It is a Python format string interpolated with the context dict. Integer/float values are auto-formatted. Example: context_label_template = "{b2b_wins}/{b2b_games} B2B"
- Explicitly decide whether game drill-down is useful. If useful, implement
  `compute_qualifications()`; if not useful, omit it. Do not add it by default.
- Base this decision on whether the metric corresponds to a clear, user-meaningful
  set of qualifying games, NOT just on whether the final value is a count or a rate.
  A rate built from qualifying games should still implement drill-down.
- For metrics with supports_career=True, the season result `context` MUST include
  the raw reducer state needed for career aggregation. Examples:
  `made/attempts`, `wins/games`, `pts/fga/fta`, `salary_usd/minutes_played`.
- For metrics with supports_career=True, define:
  `career_aggregate_mode = "season_results"`, `career_sum_keys`, optional
  `career_max_keys`, and `compute_career_value()`.

CRITICAL — PBP score parsing:
- GamePlayByPlay.score is a CUMULATIVE score string like "62 - 51" (home - road).
- It is NOT the score for that play or that quarter. It is the running total.
- To get single-quarter points, you MUST subtract the previous quarter's end score.
- The score field is the SAME regardless of which team scored — do NOT filter by home_description or visitor_description to get per-team scores.
- Always parse ALL periods' last score row, then compute per-period deltas.

CRITICAL — performance:
- Follow the bulk-loading pattern described in "Data access in compute_season()" above.
  NEVER call per-game queries inside a loop. Load all data upfront, iterate in memory.
- NEVER implement career by scanning all historical raw rows for `all_regular` /
  `all_playoffs` / `all_playin` unless absolutely necessary.
  Prefer season-result aggregation via `compute_career_value()`.
- If you use PBP data, do NOT loop over every game twice. Build qualifications during
  the same pass as the season computation.
- If exact career aggregation cannot be expressed from season result context, return
  a "clarification" response instead of generating unsafe code.
"""


def _load_example_metrics() -> str:
    """Load curated DB-backed metric source examples for the prompt."""
    from sqlalchemy.orm import sessionmaker

    from db.models import MetricDefinition as MetricDefinitionModel, engine

    SessionLocal = sessionmaker(bind=engine)

    curated_keys = [
        "combined_score",
        "lead_changes",
        "top_scorer",
        "win_pct_leading_at_half",
        "road_win_pct",
        "bench_scoring_share",
        "comeback_win_pct",
        "hot_hand",
        "clutch_fg_pct",
        "double_double_rate",
        "true_shooting_pct",
        "scoring_consistency",
    ]

    examples = []
    with SessionLocal() as session:
        rows = (
            session.query(MetricDefinitionModel)
            .filter(MetricDefinitionModel.key.in_(curated_keys))
            .all()
        )
        rows_by_key = {row.key: row for row in rows}
        for key in curated_keys:
            row = rows_by_key.get(key)
            if row is None or not row.code_python:
                continue
            cleaned = "\n".join(
                line
                for line in row.code_python.rstrip().split("\n")
                if not line.strip().startswith("register(")
            )
            examples.append(f"### {key}\n```python\n{cleaned.strip()}\n```")

    return "\n\n".join(examples) if examples else "(no examples found)"


def _build_system_prompt() -> str:
    """Build the full system prompt with dynamically loaded examples."""
    examples = _load_example_metrics()
    return _SYSTEM_PROMPT_TEMPLATE.replace("{EXAMPLES_PLACEHOLDER}", examples)


def _initial_user_message(expression: str) -> str:
    return (
        "Handle this metric-builder chat message. "
        "If it is a metric creation/modification request, return the metric-spec JSON. "
        "If it is a clarification question, return the clarification JSON.\n\n"
        f"{expression}"
    )


def _call_llm_with_system(
    system_prompt: str,
    messages: list[dict],
    model: str | None = None,
    max_tokens: int = 4096,
    usage_recorder: Callable[[dict], None] | None = None,
) -> str:
    """Call OpenAI or Anthropic with an explicit system prompt."""
    selected_model = model or env_default_llm_model()
    if not selected_model:
        raise ValueError("No AI API key set — set OPENAI_API_KEY.")

    selected_model = ensure_model_available(selected_model)
    provider = provider_for_model(selected_model)

    if provider == "openai":
        import openai
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model=selected_model,
            max_completion_tokens=max_tokens,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                *messages,
            ],
        )
        if usage_recorder:
            usage_recorder(extract_provider_usage(provider, response, selected_model))
        return response.choices[0].message.content.strip()
    elif provider == "anthropic":
        import anthropic
        client = anthropic.Anthropic()
        message = client.messages.create(
            model=selected_model,
            max_tokens=max_tokens,
            system=system_prompt,
            messages=messages,
        )
        if usage_recorder:
            usage_recorder(extract_provider_usage(provider, message, selected_model))
        return message.content[0].text.strip()
    else:
        raise ValueError(f"Unsupported provider: {provider}")


def _call_llm(
    messages: list[dict],
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
) -> str:
    """Call LLM with the metric-generator system prompt."""
    return _call_llm_with_system(
        _build_system_prompt(),
        messages,
        model=model,
        usage_recorder=usage_recorder,
    )


def generate(
    expression: str,
    history: list[dict] | None = None,
    existing: dict | None = None,
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
) -> dict:
    """Convert a plain-English expression into a metric spec with Python code.

    Args:
        expression: The user's current message (initial description or followup).
        history: Previous conversation turns as [{"role": "user"|"assistant", "content": "..."}].
                 None for first-time generation.
        existing: Current metric info (key, name, description, scope, category, rank_order, code) for edit mode.
                  When provided, the AI should only modify the code and keep metadata unchanged.

    Returns either:
    - {"responseType": "code", ...metric spec fields...}
    - {"responseType": "clarification", "message": "..."}

    Raises ValueError if generation fails or output is unparseable.
    """
    edit_prefix = ""
    if existing:
        edit_prefix = (
            "You are EDITING an existing metric. Keep the key, name, description, scope, "
            "category, and rank_order exactly as provided below — only modify the code.\n"
            "The MetricDefinition subclass in the code must keep the same key value and rank_order.\n\n"
            f"Current metric:\n"
            f"  key: {existing.get('key', '')}\n"
            f"  name: {existing.get('name', '')}\n"
            f"  description: {existing.get('description', '')}\n"
            f"  scope: {existing.get('scope', '')}\n"
            f"  category: {existing.get('category', '')}\n"
            f"  rank_order: {existing.get('rank_order', '')}\n"
            f"\nCurrent code:\n```python\n{existing.get('code', '')}\n```\n\n"
            "User's requested change:\n"
        )

    if history:
        # Multi-turn: append the new user message to existing conversation
        messages = list(history) + [{"role": "user", "content": edit_prefix + expression}]
    else:
        # First turn
        if existing:
            messages = [{"role": "user", "content": edit_prefix + expression}]
        else:
            messages = [{"role": "user", "content": _initial_user_message(expression)}]

    raw = _call_llm(messages, model=model, usage_recorder=usage_recorder)

    # Strip markdown code fences if the model wrapped the response
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        spec = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Generator returned invalid JSON: %s\nRaw: %s", exc, raw)
        raise ValueError(f"AI returned invalid JSON: {exc}") from exc

    response_type = str(spec.get("responseType") or "code").strip().lower()

    if response_type == "clarification":
        message = str(spec.get("message") or "").strip()
        if not message:
            raise ValueError("AI clarification response missing 'message'")
        return {
            "responseType": "clarification",
            "message": message,
        }

    if response_type != "code":
        raise ValueError(f"AI returned unsupported responseType: {response_type!r}")

    spec["responseType"] = "code"

    # Validate required keys
    for key in ("name", "description", "scope", "code"):
        if key not in spec:
            raise ValueError(f"AI response missing required key: {key!r}")

    if not str(spec["code"]).strip():
        raise ValueError("AI returned empty code")

    # In edit mode, override metadata with the existing values
    if existing:
        for field in ("key", "name", "description", "scope", "category", "rank_order"):
            if field in existing:
                spec[field] = existing[field]

    return spec


def check_similar(
    expression: str,
    catalog: list[dict],
    model: str | None = None,
    usage_recorder: Callable[[dict], None] | None = None,
) -> list[dict]:
    """Check if any existing metrics are similar to the user's description.

    Args:
        expression: The user's metric description.
        catalog: List of dicts with at least 'key', 'name', 'description' for each existing metric.
        model: Optional LLM model override.

    Returns:
        List of similar metrics: [{"key", "name", "description", "reason"}, ...].
        Empty list if nothing is similar.
    """
    if not expression.strip() or not catalog:
        return []

    catalog_lines = "\n".join(
        f"- key={m['key']}  name={m.get('name', '')}  description={m.get('description', '')}"
        for m in catalog
    )

    system_prompt = (
        "You are an NBA analytics assistant. The user wants to create a new metric.\n"
        "Your job is to check whether any existing metrics already measure the same thing "
        "or are very similar to what the user described.\n\n"
        "Existing metrics:\n"
        f"{catalog_lines}\n\n"
        "Reply with ONLY a JSON array (no markdown fences). Each element:\n"
        '{"key": "metric_key", "reason": "short explanation of why it is similar"}\n\n'
        "Rules:\n"
        "- Only include metrics that genuinely measure the same or very similar thing.\n"
        "- If nothing is similar, return an empty array: []\n"
        "- Return at most 3 results.\n"
        "- Be strict — minor keyword overlap is NOT enough. The intent must match.\n"
    )

    raw = _call_llm_with_system(
        system_prompt,
        [{"role": "user", "content": expression}],
        model=model,
        max_tokens=512,
        usage_recorder=usage_recorder,
    )

    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("check_similar returned invalid JSON: %s", raw)
        return []

    if not isinstance(parsed, list):
        return []

    catalog_by_key = {m["key"]: m for m in catalog}
    results = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key", "")).strip()
        if key not in catalog_by_key:
            continue
        m = catalog_by_key[key]
        results.append({
            "key": key,
            "name": m.get("name", ""),
            "description": m.get("description", ""),
            "reason": str(item.get("reason", "")).strip() or "Similar metric.",
        })
        if len(results) >= 3:
            break

    return results


def generate_rule(expression: str) -> dict:
    """Legacy: generate a JSON rule definition. Kept for backwards compatibility."""
    from metrics.framework._generator_rule import generate as _gen_rule
    return _gen_rule(expression)
