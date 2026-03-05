"""RuleEngine: interprets a MetricDefinition's definition_json into a live query.

Supported sources:
  player_game_stats  — PlayerGameStats joined to Game
  shot_records       — ShotRecord joined to Game
  game_pbp           — GamePlayByPlay joined to Game

Supported aggregations:
  avg          — AVG(stat)
  sum          — SUM(stat)
  count        — COUNT(*) of rows matching filters
  pct_rows     — COUNT(matching) / COUNT(total)  e.g. % of games scoring 20+
  ratio        — SUM(numerator) / SUM(denominator)  e.g. FG%
  pct_of_total — entity SUM(stat) / group SUM(stat)  e.g. bench scoring share

Supported filter operators: =, !=, >, >=, <, <=, in
"""
from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy import func, case

from db.models import Game, GamePlayByPlay, PlayerGameStats, ShotRecord, TeamGameStats

logger = logging.getLogger(__name__)

# ── Field maps ────────────────────────────────────────────────────────────────

_PGS_FIELDS: dict[str, Any] = {
    "pts": PlayerGameStats.pts,
    "reb": PlayerGameStats.reb,
    "ast": PlayerGameStats.ast,
    "stl": PlayerGameStats.stl,
    "blk": PlayerGameStats.blk,
    "tov": PlayerGameStats.tov,
    "fgm": PlayerGameStats.fgm,
    "fga": PlayerGameStats.fga,
    "fg3m": PlayerGameStats.fg3m,
    "fg3a": PlayerGameStats.fg3a,
    "ftm": PlayerGameStats.ftm,
    "fta": PlayerGameStats.fta,
    "plus_minus": PlayerGameStats.plus_minus,
    "starter": PlayerGameStats.starter,
    "min": PlayerGameStats.min,
}

_SHOT_FIELDS: dict[str, Any] = {
    "shot_made": ShotRecord.shot_made,
    "shot_attempted": ShotRecord.shot_attempted,
    "shot_distance": ShotRecord.shot_distance,
    "shot_zone_basic": ShotRecord.shot_zone_basic,
    "shot_zone_area": ShotRecord.shot_zone_area,
    "period": ShotRecord.period,
    "min": ShotRecord.min,
    "sec": ShotRecord.sec,
}

_PBP_FIELDS: dict[str, Any] = {
    "period": GamePlayByPlay.period,
    "score_margin": GamePlayByPlay.score_margin,
    "event_type": GamePlayByPlay.event_type,
}

_SOURCE_MAP = {
    "player_game_stats": ("player", PlayerGameStats, _PGS_FIELDS, PlayerGameStats.player_id),
    "shot_records":      ("player", ShotRecord,       _SHOT_FIELDS, ShotRecord.player_id),
    "game_pbp":          ("game",   GamePlayByPlay,   _PBP_FIELDS,  GamePlayByPlay.game_id),
}


# ── Filter building ───────────────────────────────────────────────────────────

def _build_filter(col: Any, op: str, value: Any):
    if op == "=":
        return col == value
    if op == "!=":
        return col != value
    if op == ">":
        return col > value
    if op == ">=":
        return col >= value
    if op == "<":
        return col < value
    if op == "<=":
        return col <= value
    if op == "in":
        return col.in_(value if isinstance(value, list) else [value])
    if op == "not_in":
        return col.notin_(value if isinstance(value, list) else [value])
    raise ValueError(f"Unsupported filter operator: {op!r}")


def _apply_filters(q, field_map: dict, filters: list[dict]):
    for f in filters:
        field = f["field"]
        col = field_map.get(field)
        if col is None:
            raise ValueError(f"Unknown field {field!r} for this source")
        q = q.filter(_build_filter(col, f["op"], f["value"]))
    return q


# ── Core compute ──────────────────────────────────────────────────────────────

def compute(
    session: Session,
    definition: dict,
    entity_id: str,
    season: str,
    scope: str,
) -> float | None:
    """Run a rule definition for one entity/season. Returns value_num or None."""
    source = definition.get("source", "player_game_stats")
    filters = definition.get("filters", [])
    aggregation = definition["aggregation"]

    if source not in _SOURCE_MAP:
        raise ValueError(f"Unknown source: {source!r}")

    _, model, field_map, id_col = _SOURCE_MAP[source]

    # Base query scoped to this entity + season
    base_q = (
        session.query(model)
        .join(Game, model.game_id == Game.game_id)
        .filter(id_col == entity_id, Game.season == season)
    )

    if aggregation == "count":
        q = _apply_filters(base_q, field_map, filters)
        result = q.count()
        return float(result)

    if aggregation == "pct_rows":
        # % of rows matching the filter vs total
        stat_col = field_map.get(definition.get("stat", "pts"))
        total = base_q.count()
        if not total:
            return None
        matched = _apply_filters(base_q, field_map, filters).count()
        return matched / total

    if aggregation in ("avg", "sum"):
        stat_col = field_map.get(definition["stat"])
        if stat_col is None:
            raise ValueError(f"Unknown stat: {definition['stat']!r}")
        agg_fn = func.avg if aggregation == "avg" else func.sum
        q = _apply_filters(base_q, field_map, filters)
        val = q.with_entities(agg_fn(func.coalesce(stat_col, 0))).scalar()
        return float(val) if val is not None else None

    if aggregation == "ratio":
        num_col = field_map.get(definition["numerator"])
        den_col = field_map.get(definition["denominator"])
        if num_col is None or den_col is None:
            raise ValueError("ratio requires numerator and denominator fields")
        q = _apply_filters(base_q, field_map, filters)
        row = q.with_entities(
            func.sum(func.coalesce(num_col, 0)),
            func.sum(func.coalesce(den_col, 0)),
        ).one()
        num, den = row
        if not den:
            return None
        return float(num) / float(den)

    if aggregation == "pct_of_total":
        # entity's sum as % of all entities' sum for this season
        stat_col = field_map.get(definition["stat"])
        if stat_col is None:
            raise ValueError(f"Unknown stat: {definition['stat']!r}")
        entity_q = _apply_filters(base_q, field_map, filters)
        entity_sum = entity_q.with_entities(func.sum(func.coalesce(stat_col, 0))).scalar() or 0

        total_q = (
            session.query(func.sum(func.coalesce(stat_col, 0)))
            .select_from(model)
            .join(Game, model.game_id == Game.game_id)
            .filter(Game.season == season)
        )
        total_q = _apply_filters(total_q, field_map, filters)
        total_sum = total_q.scalar() or 0
        if not total_sum:
            return None
        return float(entity_sum) / float(total_sum)

    raise ValueError(f"Unsupported aggregation: {aggregation!r}")


def compute_baseline(
    session: Session,
    definition: dict,
    entity_id: str,
    season: str,
    scope: str,
) -> float | None:
    """Compute the baseline value (same aggregation, no filters) for comparison."""
    baseline_def = definition.get("baseline")
    if not baseline_def:
        return None
    # Baseline uses same source/entity but its own aggregation/filters (usually empty)
    merged = {**definition, **baseline_def, "filters": baseline_def.get("filters", [])}
    return compute(session, merged, entity_id, season, scope)


# ── Preview: top N results across all entities ────────────────────────────────

def preview(
    session: Session,
    definition: dict,
    scope: str,
    season: str,
    limit: int = 25,
) -> list[dict]:
    """Run a rule definition against all entities for a season, return ranked rows."""
    source = definition.get("source", "player_game_stats")
    if source not in _SOURCE_MAP:
        raise ValueError(f"Unknown source: {source!r}")

    _, model, field_map, id_col = _SOURCE_MAP[source]

    # Get distinct entity_ids for this season
    entity_ids = [
        row[0]
        for row in (
            session.query(id_col)
            .join(Game, model.game_id == Game.game_id)
            .filter(Game.season == season, id_col.isnot(None))
            .distinct()
            .all()
        )
    ]

    rows = []
    for eid in entity_ids:
        try:
            val = compute(session, definition, eid, season, scope)
        except Exception as exc:
            logger.debug("preview compute failed for %s: %s", eid, exc)
            continue
        if val is None:
            continue
        baseline = compute_baseline(session, definition, eid, season, scope)
        rows.append({
            "entity_id": eid,
            "value_num": round(val, 4),
            "baseline": round(baseline, 4) if baseline is not None else None,
        })

    rows.sort(key=lambda r: r["value_num"], reverse=True)
    return rows[:limit]
