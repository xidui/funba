"""AI-powered metric generator: converts plain-English descriptions into executable Python code.

Uses Anthropic Claude (preferred) or OpenAI to generate a MetricDefinition subclass
that the runner can execute directly. Returns a spec dict with metadata + Python code.
"""
from __future__ import annotations

import json
import logging
import os
import re

logger = logging.getLogger(__name__)

# ── Prompt template fed to the LLM ──────────────────────────────────────────

_SYSTEM_PROMPT = """\
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

## Examples

### Example 1: Game-scope, non-incremental
```python
from metrics.framework.base import MetricDefinition, MetricResult
from db.models import Game

class CombinedScore(MetricDefinition):
    key = "combined_score"
    name = "Combined Score"
    description = "Total points scored by both teams."
    scope = "game"
    category = "scoring"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        game = session.query(Game.home_team_score, Game.road_team_score) \\
            .filter(Game.game_id == entity_id).one_or_none()
        if game is None or game.home_team_score is None:
            return None
        total = game.home_team_score + game.road_team_score
        return MetricResult(
            metric_key=self.key, entity_type="game", entity_id=entity_id,
            season=season, game_id=entity_id,
            value_num=float(total), value_str=f"{total} pts",
            context={"combined_score": total},
        )
```

### Example 2: Team-scope, incremental with career
```python
from metrics.framework.base import MetricDefinition, MetricResult
from db.models import Game, GamePlayByPlay, TeamGameStats

class WinPctLeadingAtHalf(MetricDefinition):
    key = "win_pct_leading_at_half"
    name = "Leads-at-Half Win%"
    description = "Win % in games where the team was leading at halftime."
    scope = "team"
    category = "conditional"
    min_sample = 5
    incremental = True
    supports_career = True

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = session.query(TeamGameStats).filter(
            TeamGameStats.team_id == entity_id, TeamGameStats.game_id == game_id
        ).first()
        if tgs is None or tgs.win is None:
            return None
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if not game:
            return None
        is_home = game.home_team_id == entity_id
        pbp_row = session.query(GamePlayByPlay.score_margin).filter(
            GamePlayByPlay.game_id == game_id, GamePlayByPlay.period == 2,
            GamePlayByPlay.score_margin.isnot(None),
        ).order_by(GamePlayByPlay.event_num.desc()).first()
        if pbp_row is None or pbp_row.score_margin in (None, "null", ""):
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}
        try:
            margin = int(pbp_row.score_margin)
        except (ValueError, TypeError):
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}
        team_leading = margin > 0 if is_home else margin < 0
        if not team_leading:
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}
        return {"total_games": 1, "leading_total": 1, "leading_wins": 1 if tgs.win else 0}

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        leading_total = totals.get("leading_total", 0)
        if leading_total < self.min_sample:
            return None
        win_pct = totals.get("leading_wins", 0) / leading_total
        return MetricResult(
            metric_key=self.key, entity_type="team", entity_id=entity_id,
            season=season, game_id=None,
            value_num=round(win_pct, 4),
            context={"wins": totals.get("leading_wins", 0),
                     "games_leading_at_half": leading_total,
                     "total_games": totals.get("total_games", 0)},
        )
```

### Example 3: Game-scope, parsing PBP score for quarter data
```python
from metrics.framework.base import MetricDefinition, MetricResult
from db.models import Game, GamePlayByPlay, Team

class FirstHalfHighScore(MetricDefinition):
    key = "first_half_high_score"
    name = "First Half High Score"
    description = "Highest first-half score by either team in a game."
    scope = "game"
    category = "scoring"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        target = entity_id
        # Get cumulative score at end of Q2 (halftime)
        row = session.query(GamePlayByPlay.score).filter(
            GamePlayByPlay.game_id == target,
            GamePlayByPlay.period == 2,
            GamePlayByPlay.score.isnot(None),
        ).order_by(GamePlayByPlay.event_num.desc()).first()
        if not row or not row.score:
            return None
        parts = row.score.split("-")
        if len(parts) != 2:
            return None
        try:
            home_half = int(parts[0].strip())
            road_half = int(parts[1].strip())
        except (ValueError, TypeError):
            return None
        game = session.query(Game).filter(Game.game_id == target).one_or_none()
        if not game:
            return None
        # Resolve team abbreviations for display
        high = max(home_half, road_half)
        if home_half >= road_half:
            high_team_id = game.home_team_id
        else:
            high_team_id = game.road_team_id
        team = session.query(Team.abbr).filter(Team.team_id == high_team_id).first()
        abbr = team.abbr if team else high_team_id
        return MetricResult(
            metric_key=self.key, entity_type="game", entity_id=target,
            season=season, game_id=target,
            value_num=float(high), value_str=f"{abbr} {high}",
            context={"home_half": home_half, "road_half": road_half,
                     "high_team_id": high_team_id},
        )
```

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

CRITICAL — PBP score parsing:
- GamePlayByPlay.score is a CUMULATIVE score string like "62 - 51" (home - road).
- It is NOT the score for that play or that quarter. It is the running total.
- To get single-quarter points, you MUST subtract the previous quarter's end score.
- The score field is the SAME regardless of which team scored — do NOT filter by home_description or visitor_description to get per-team scores.
- Always parse ALL periods' last score row, then compute per-period deltas.
- See Example 3 above for the correct pattern.
"""


def _call_llm(prompt: str) -> str:
    """Call Anthropic (preferred) or OpenAI and return the raw text response."""
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")

    if not anthropic_key and not openai_key:
        raise ValueError("No AI API key set — set ANTHROPIC_API_KEY or OPENAI_API_KEY.")

    if anthropic_key:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text.strip()
    else:
        import openai
        client = openai.OpenAI(api_key=openai_key)
        response = client.chat.completions.create(
            model="gpt-5.4",
            max_completion_tokens=4096,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        return response.choices[0].message.content.strip()


def generate(expression: str) -> dict:
    """Convert a plain-English expression into a metric spec with Python code.

    Returns a dict with keys: name, description, scope, category, min_sample,
    incremental, supports_career, rank_order, code.

    Raises ValueError if generation fails or output is unparseable.
    """
    prompt = (
        f"Convert this NBA metric description into a MetricDefinition Python class:\n\n"
        f"\"{expression}\""
    )

    raw = _call_llm(prompt)

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

    return spec


def generate_rule(expression: str) -> dict:
    """Legacy: generate a JSON rule definition. Kept for backwards compatibility."""
    from metrics.framework._generator_rule import generate as _gen_rule
    return _gen_rule(expression)
