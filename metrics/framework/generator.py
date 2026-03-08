"""AI-powered metric generator: converts plain-English expressions into definition_json.

Uses OpenAI by default (falls back to Anthropic) to parse the user's description into a
structured rule that the RuleEngine can execute. Returns a full metric spec including
name, description, scope, and the rule.
"""
from __future__ import annotations

import json
import logging
import os
import re

logger = logging.getLogger(__name__)

# ── Schema documentation fed to Claude ───────────────────────────────────────

_SCHEMA_DOCS = """
You convert plain-English NBA metric descriptions into structured JSON rule definitions.

## Available data sources

### player_game_stats  (one row per player per game)
Fields: pts, reb, ast, stl, blk, tov, fgm, fga, fg3m, fg3a, ftm, fta, plus_minus, min, starter (bool)

### shot_records  (one row per shot attempt)
Fields: shot_made (bool), shot_attempted (bool), shot_distance (int, feet),
        shot_zone_basic (str: "Restricted Area","In The Paint (Non-RA)","Mid-Range","Left Corner 3","Right Corner 3","Above the Break 3","Backcourt"),
        shot_zone_area (str), period (int), min (int, minutes remaining), sec (int)

### game_pbp  (one row per play-by-play event)
Fields: period (int), score_margin (int, home perspective), event_type (str)

### team_game_stats  (one row per team per game)
Fields: pts, reb, ast, stl, blk, tov, fgm, fga, fg3m, fg3a, ftm, fta, min, win (bool), on_road (bool)

## Aggregations

- avg        — average of a stat field  {"aggregation":"avg","stat":"pts"}
- max        — max of a stat field      {"aggregation":"max","stat":"pts"}
- sum        — total of a stat field    {"aggregation":"sum","stat":"pts"}
- count      — count of matching rows   {"aggregation":"count"}
- pct_rows   — % of rows matching filters vs total  {"aggregation":"pct_rows"}
- ratio      — SUM(numerator)/SUM(denominator)  {"aggregation":"ratio","numerator":"fgm","denominator":"fga"}
- pct_of_total — entity's SUM(stat) as % of all entities' SUM(stat)  {"aggregation":"pct_of_total","stat":"pts"}

## Filter operators
=, !=, >, >=, <, <=, in, not_in

## Scopes
- player  (one result per player per season)
- team    (one result per team per season)
- game    (one result per game)

## Output format (JSON only, no markdown)
{
  "name": "Short display name",
  "description": "One sentence describing what this measures.",
  "scope": "player | team | game",
  "category": "scoring | defense | efficiency | conditional | aggregate | record",
  "group_key": null,
  "min_sample": <int — minimum rows before result is meaningful>,
  "definition": {
    "source": "<source>",
    "filters": [{"field":"<f>","op":"<op>","value":<v>}, ...],
    "aggregation": "<agg>",
    "supports_career": <bool>,  // optional
    "career_name_suffix": "<suffix>",  // optional
    "career_min_sample": <int>,  // optional
    // ratio only:
    "numerator": "<field>",
    "denominator": "<field>",
    // avg/sum/count/pct_rows/pct_of_total:
    "stat": "<field>",
    // optional baseline for comparison:
    "baseline": {
      "aggregation": "<agg>",
      "numerator": "<field>",   // if ratio
      "denominator": "<field>", // if ratio
      "stat": "<field>"         // if avg/sum
    }
  }
}
"""


def generate(expression: str) -> dict:
    """Convert a plain-English expression into a metric spec using OpenAI or Anthropic.

    Returns a dict with keys: name, description, scope, category, group_key,
    min_sample, definition (the rule dict).

    Raises ValueError if generation fails or output is unparseable.
    """
    openai_key = os.getenv("OPENAI_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    if not openai_key and not anthropic_key:
        raise ValueError("No AI API key set — set OPENAI_API_KEY or ANTHROPIC_API_KEY.")

    prompt = (
        f"{_SCHEMA_DOCS}\n\n"
        f"Convert this metric description into the JSON format above:\n\n"
        f"\"{expression}\"\n\n"
        f"Reply with valid JSON only."
    )

    if openai_key:
        import openai
        client = openai.OpenAI(api_key=openai_key)
        response = client.chat.completions.create(
            model="gpt-4o",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.choices[0].message.content.strip()
    else:
        import anthropic
        client = anthropic.Anthropic(api_key=anthropic_key)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()

    # Strip markdown code fences if Claude wrapped the response
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    try:
        spec = json.loads(raw)
    except json.JSONDecodeError as exc:
        logger.error("Generator returned invalid JSON: %s\nRaw: %s", exc, raw)
        raise ValueError(f"AI returned invalid JSON: {exc}") from exc

    # Validate required keys
    for key in ("name", "description", "scope", "definition"):
        if key not in spec:
            raise ValueError(f"AI response missing required key: {key!r}")

    return spec
