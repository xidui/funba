"""AI-powered metric generator: converts plain-English descriptions into executable Python code.

Uses OpenAI GPT-5.4 when available, with Anthropic as a fallback, to generate a
MetricDefinition subclass that the runner can execute directly. Returns a spec
dict with metadata + Python code.
"""
from __future__ import annotations

import json
import logging
import os
import re

logger = logging.getLogger(__name__)

# ── Prompt template fed to the LLM ──────────────────────────────────────────

_SYSTEM_PROMPT_TEMPLATE = """\
You are an NBA analytics metric generator. Given a plain-English description,
you produce a Python class that extends MetricDefinition and a JSON metadata block.

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

## MetricDefinition Base Class

```python
class MetricDefinition(ABC):
    key: str            # unique identifier, e.g. "first_half_high_score"
    name: str           # display name
    description: str    # one-sentence description
    scope: str          # "player" | "team" | "game"
    category: str       # "scoring" | "defense" | "efficiency" | "conditional" | "aggregate" | "record"
    min_sample: int = 10
    incremental: bool = True       # True: use compute_delta+compute_value; False: use compute()
    supports_career: bool = False  # auto-register career sibling
    career: bool = False
    rank_order: str = "desc"       # "desc" (higher=better) or "asc" (lower=better)
```

### Two execution modes:

**Mode 1: incremental=True (for season/career aggregation)**
Used when you accumulate stats across games (e.g., win rate, FG%).
```python
def compute_delta(self, session, entity_id, game_id) -> dict | None:
    # Return per-game additive data. Numeric values are SUMMED across games.
    # Return None if entity didn't participate.

def compute_value(self, totals, season, entity_id) -> MetricResult | None:
    # Derive final value from accumulated totals. Return None if below min_sample.
```

**Mode 2: incremental=False (for per-game metrics)**
Used when each game produces an independent value (e.g., combined score).
```python
def compute(self, session, entity_id, season, game_id=None) -> MetricResult | list[MetricResult] | None:
    # For game-scope: entity_id IS the game_id
    # Compute and return result for this single game.
    # Can return a LIST of MetricResults to produce multiple rows per game.
    # When returning multiple rows, each must have a unique entity_id
    # (e.g., "game_id:team_id:period" for per-quarter-per-team data).
```

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

## Helper functions (import from metrics.helpers)

These are available for use — especially recommended for PBP score parsing:

```python
from metrics.helpers import get_quarter_scores, get_half_scores, team_abbr

# get_quarter_scores(session, game_id) -> list[dict]
# Returns per-quarter per-team points:
# [{"period": 1, "home_pts": 28, "road_pts": 31, "home_team_id": "...", "road_team_id": "..."}, ...]
# Handles all PBP cumulative score parsing internally.

# get_half_scores(session, game_id) -> dict | None
# Returns: {"home_team_id", "road_team_id", "home_first_half", "road_first_half",
#           "home_second_half", "road_second_half"}

# team_abbr(session, team_id) -> str
# Returns team abbreviation like "GSW", "LAL", etc.
```

## Real examples from production

Below are real, tested metric implementations from the codebase. Study them carefully
to understand the patterns, coding style, and data access conventions.

{EXAMPLES_PLACEHOLDER}

## Your output format

Reply with ONLY a JSON object (no markdown fences):
{
  "name": "Short display name",
  "description": "One sentence describing what this measures.",
  "scope": "player | team | game",
  "category": "scoring | defense | efficiency | conditional | aggregate | record",
  "min_sample": <int>,
  "incremental": <bool>,
  "supports_career": <bool>,
  "rank_order": "desc | asc",
  "code": "<full Python code for the class, with imports>"
}

IMPORTANT:
- The "code" field must contain COMPLETE, runnable Python code for a MetricDefinition subclass.
- Include all necessary imports at the top of the code.
- Import MetricDefinition and MetricResult from metrics.framework.base.
- Import DB models from db.models.
- Do NOT include register() call — the system handles registration.
- The class name should be CamelCase of the key.
- Use raw strings or proper escaping in the code field.
- Do NOT put ranking numbers (like #1, #2) in value_str. Ranking is handled by the system at query time, not inside compute(). value_str should only contain the descriptive value (e.g. "ATL Q1: 44 pts").

CRITICAL — PBP score parsing:
- GamePlayByPlay.score is a CUMULATIVE score string like "62 - 51" (home - road).
- It is NOT the score for that play or that quarter. It is the running total.
- To get single-quarter points, you MUST subtract the previous quarter's end score.
- The score field is the SAME regardless of which team scored — do NOT filter by home_description or visitor_description to get per-team scores.
- Always parse ALL periods' last score row, then compute per-period deltas.
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

    # Also include helpers source so LLM can see how they work
    from pathlib import Path
    helpers_path = Path(__file__).parent.parent / "helpers.py"
    if helpers_path.exists():
        helpers_code = helpers_path.read_text()
        examples.append(f"### metrics/helpers.py (available utility functions)\n```python\n{helpers_code.strip()}\n```")

    return "\n\n".join(examples) if examples else "(no examples found)"


def _build_system_prompt() -> str:
    """Build the full system prompt with dynamically loaded examples."""
    examples = _load_example_metrics()
    return _SYSTEM_PROMPT_TEMPLATE.replace("{EXAMPLES_PLACEHOLDER}", examples)


def _call_llm(messages: list[dict]) -> str:
    """Call OpenAI (preferred) or Anthropic and return the raw text response.

    messages: list of {"role": "user"|"assistant", "content": "..."}
    """
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")

    if not anthropic_key and not openai_key:
        raise ValueError("No AI API key set — set ANTHROPIC_API_KEY or OPENAI_API_KEY.")

    system_prompt = _build_system_prompt()

    if openai_key:
        import openai
        client = openai.OpenAI(api_key=openai_key)
        response = client.chat.completions.create(
            model="gpt-5.4",
            max_completion_tokens=4096,
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                *messages,
            ],
        )
        return response.choices[0].message.content.strip()
    else:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=system_prompt,
            messages=messages,
        )
        return message.content[0].text.strip()


def generate(expression: str, history: list[dict] | None = None,
             existing: dict | None = None) -> dict:
    """Convert a plain-English expression into a metric spec with Python code.

    Args:
        expression: The user's current message (initial description or followup).
        history: Previous conversation turns as [{"role": "user"|"assistant", "content": "..."}].
                 None for first-time generation.
        existing: Current metric info (key, name, description, scope, category, rank_order, code) for edit mode.
                  When provided, the AI should only modify the code and keep metadata unchanged.

    Returns a dict with keys: name, description, scope, category, min_sample,
    incremental, supports_career, rank_order, code.

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
            messages = [{"role": "user", "content": (
                f"Convert this NBA metric description into a MetricDefinition Python class:\n\n"
                f"\"{expression}\""
            )}]

    raw = _call_llm(messages)

    # Strip markdown code fences if the model wrapped the response
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        spec = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Generator returned invalid JSON: %s\nRaw: %s", exc, raw)
        raise ValueError(f"AI returned invalid JSON: {exc}") from exc

    # Validate required keys
    for key in ("name", "description", "scope", "code"):
        if key not in spec:
            raise ValueError(f"AI response missing required key: {key!r}")

    if not spec["code"].strip():
        raise ValueError("AI returned empty code")

    # In edit mode, override metadata with the existing values
    if existing:
        for field in ("key", "name", "description", "scope", "category", "rank_order"):
            if field in existing:
                spec[field] = existing[field]

    return spec


def generate_rule(expression: str) -> dict:
    """Legacy: generate a JSON rule definition. Kept for backwards compatibility."""
    from metrics.framework._generator_rule import generate as _gen_rule
    return _gen_rule(expression)
