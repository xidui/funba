"""RuleEngine: interprets a MetricDefinition's definition_json into a live query.

Supported sources:
  player_game_stats  — PlayerGameStats joined to Game
  shot_records       — ShotRecord joined to Game
  game_pbp           — GamePlayByPlay joined to Game

Supported aggregations:
  avg          — AVG(stat)
  max          — MAX(stat)
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

from db.models import Game, GamePlayByPlay, PlayerGameStats, ShotRecord, Team, TeamGameStats
from metrics.framework.base import CAREER_SEASON

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
    "plus_minus": PlayerGameStats.plus,
    "plus": PlayerGameStats.plus,
    "starter": PlayerGameStats.starter,
    "min": PlayerGameStats.min,
    "team_id": PlayerGameStats.team_id,
    "franchise_id": func.coalesce(Team.canonical_team_id, Team.team_id),
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
    "team_id": ShotRecord.team_id,
    "franchise_id": func.coalesce(Team.canonical_team_id, Team.team_id),
}

_PBP_FIELDS: dict[str, Any] = {
    "period": GamePlayByPlay.period,
    "score_margin": GamePlayByPlay.score_margin,
    "event_type": GamePlayByPlay.event_msg_type,
    "event_msg_type": GamePlayByPlay.event_msg_type,
    "event_msg_action_type": GamePlayByPlay.event_msg_action_type,
}

_TGS_FIELDS: dict[str, Any] = {
    "pts": TeamGameStats.pts,
    "reb": TeamGameStats.reb,
    "ast": TeamGameStats.ast,
    "stl": TeamGameStats.stl,
    "blk": TeamGameStats.blk,
    "tov": TeamGameStats.tov,
    "fgm": TeamGameStats.fgm,
    "fga": TeamGameStats.fga,
    "fg3m": TeamGameStats.fg3m,
    "fg3a": TeamGameStats.fg3a,
    "ftm": TeamGameStats.ftm,
    "fta": TeamGameStats.fta,
    "min": TeamGameStats.min,
    "win": TeamGameStats.win,
    "on_road": TeamGameStats.on_road,
    "team_id": TeamGameStats.team_id,
    "franchise_id": func.coalesce(Team.canonical_team_id, Team.team_id),
}

_SOURCE_MAP = {
    "player_game_stats": {
        "model": PlayerGameStats,
        "field_map": _PGS_FIELDS,
        "id_cols": {
            "player": PlayerGameStats.player_id,
            "team": PlayerGameStats.team_id,
            "game": PlayerGameStats.game_id,
        },
        "team_join_col": PlayerGameStats.team_id,
    },
    "shot_records": {
        "model": ShotRecord,
        "field_map": _SHOT_FIELDS,
        "id_cols": {
            "player": ShotRecord.player_id,
            "team": ShotRecord.team_id,
            "game": ShotRecord.game_id,
        },
        "team_join_col": ShotRecord.team_id,
    },
    "game_pbp": {
        "model": GamePlayByPlay,
        "field_map": _PBP_FIELDS,
        "id_cols": {
            "game": GamePlayByPlay.game_id,
        },
    },
    "team_game_stats": {
        "model": TeamGameStats,
        "field_map": _TGS_FIELDS,
        "id_cols": {
            "team": TeamGameStats.team_id,
            "game": TeamGameStats.game_id,
        },
        "team_join_col": TeamGameStats.team_id,
    },
}


def _player_franchise_entity_id(player_id: str | None, franchise_id: str | None) -> str | None:
    if not player_id or not franchise_id:
        return None
    return f"{player_id}:{franchise_id}"


def _parse_player_franchise_entity_id(entity_id: str) -> tuple[str | None, str | None]:
    if not entity_id or ":" not in entity_id:
        return None, None
    player_id, franchise_id = entity_id.split(":", 1)
    return player_id or None, franchise_id or None


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


def _definition_source(definition: dict) -> str:
    return str(definition.get("dataset") or definition.get("source") or "player_game_stats")


def _normalize_definition(definition: dict) -> dict:
    normalized = dict(definition)
    if "source" not in normalized and "dataset" in normalized:
        normalized["source"] = normalized["dataset"]
    if "dataset" not in normalized and "source" in normalized:
        normalized["dataset"] = normalized["source"]
    return normalized


def _team_franchise_id(session: Session, team_id: str | None) -> str | None:
    if not team_id:
        return None
    row = (
        session.query(Team.team_id, Team.canonical_team_id)
        .filter(Team.team_id == team_id)
        .first()
    )
    if row is None:
        return team_id
    return row.canonical_team_id or row.team_id


def _resolve_entity_context(
    session: Session,
    scope: str,
    entity_id: str,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "id": entity_id,
        "entity_id": entity_id,
    }
    if scope == "player":
        latest = (
            session.query(PlayerGameStats.team_id)
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, PlayerGameStats.team_id.isnot(None))
            .order_by(Game.game_date.desc(), Game.game_id.desc())
            .first()
        )
        current_team_id = latest.team_id if latest is not None else None
        context["player_id"] = entity_id
        context["current_team_id"] = current_team_id
        context["current_franchise_id"] = _team_franchise_id(session, current_team_id)
        context["franchise_id"] = context["current_franchise_id"]
    elif scope == "player_franchise":
        player_id, franchise_id = _parse_player_franchise_entity_id(entity_id)
        context["player_id"] = player_id
        context["franchise_id"] = franchise_id
        context["current_team_id"] = None
        context["current_franchise_id"] = franchise_id
    elif scope == "team":
        context["current_team_id"] = entity_id
        context["current_franchise_id"] = _team_franchise_id(session, entity_id)
        context["franchise_id"] = context["current_franchise_id"]
    else:
        context["current_team_id"] = None
        context["current_franchise_id"] = None
        context["franchise_id"] = None
    return context


def _resolve_dynamic_value(value: Any, entity_context: dict[str, Any]) -> Any:
    if isinstance(value, str) and value.startswith("entity."):
        return entity_context.get(value.split(".", 1)[1])
    return value


def _season_type_codes(season_types: list[str] | None) -> set[str] | None:
    if not season_types:
        return None
    codes: set[str] = set()
    for raw in season_types:
        kind = str(raw).strip().lower()
        if kind in {"combined", "all"}:
            return None
        if kind in {"regular", "regular_season"}:
            codes.add("002")
        elif kind in {"playoffs", "playoff"}:
            codes.add("004")
        elif kind in {"play_in", "playin"}:
            codes.add("005")
        else:
            raise ValueError(f"Unsupported season type: {raw!r}")
    return codes or None


def _apply_season_scope(q, season: str, definition: dict):
    if season != CAREER_SEASON:
        q = q.filter(Game.season == season)
    season_codes = _season_type_codes(definition.get("season_types"))
    if season_codes:
        q = q.filter(func.substr(Game.game_id, 1, 3).in_(sorted(season_codes)))
    return q


def _resolve_partition_group(partition_by: list[Any], entity_context: dict[str, Any]) -> str | None:
    if not partition_by:
        return None
    parts: list[str] = []
    for item in partition_by:
        value = _resolve_dynamic_value(item, entity_context)
        if value in (None, ""):
            continue
        parts.append(str(value))
    return "|".join(parts) if parts else None


def _context_with_counts(
    source: str,
    scope: str,
    count_value: int | None,
    rank_group: str | None,
) -> dict[str, Any]:
    context: dict[str, Any] = {}
    if count_value is not None:
        context["rows_counted"] = int(count_value)
        if source in {"player_game_stats", "team_game_stats"} and scope in {"player", "team"}:
            context["games"] = int(count_value)
    if rank_group is not None:
        context["rank_group"] = rank_group
    return context


def _format_value(definition: dict, value: float | None) -> str | None:
    if value is None:
        return None
    display = definition.get("display") or {}
    fmt = str(display.get("value_format") or "").strip().lower()
    unit = str(display.get("unit") or "").strip()
    if fmt in {"integer", "number"} or float(value).is_integer():
        rendered = f"{int(round(value)):,}"
    else:
        rendered = f"{value:.4f}".rstrip("0").rstrip(".")
    if unit:
        rendered = f"{rendered} {unit}"
    return rendered


def _apply_filters(q, field_map: dict, filters: list[dict], entity_context: dict[str, Any]):
    for f in filters:
        field = f["field"]
        col = field_map.get(field)
        if col is None:
            raise ValueError(f"Unknown field {field!r} for this source")
        raw_value = f.get("value_from", f.get("value"))
        value = _resolve_dynamic_value(raw_value, entity_context)
        q = q.filter(_build_filter(col, f["op"], value))
    return q


# ── Core compute ──────────────────────────────────────────────────────────────

def compute_result(
    session: Session,
    definition: dict,
    entity_id: str,
    season: str,
    scope: str,
) -> dict[str, Any] | None:
    """Run a rule definition for one entity/season. Returns value + metadata."""
    definition = _normalize_definition(definition)
    source = _definition_source(definition)
    filters = definition.get("filters", [])
    aggregation = definition["aggregation"]

    if source not in _SOURCE_MAP:
        raise ValueError(f"Unknown source: {source!r}")

    source_spec = _SOURCE_MAP[source]
    model = source_spec["model"]
    field_map = source_spec["field_map"]
    entity_context = _resolve_entity_context(session, scope, entity_id)
    ranking = definition.get("ranking") or {}
    rank_group = _resolve_partition_group(ranking.get("partition_by") or [], entity_context)

    # Base query scoped to this entity + season. season="all" is the cross-season
    # bucket used by career metrics.
    base_q = (
        session.query(model)
        .join(Game, model.game_id == Game.game_id)
    )
    team_join_col = source_spec.get("team_join_col")
    if team_join_col is not None:
        base_q = base_q.outerjoin(Team, team_join_col == Team.team_id)
    if scope == "player_franchise":
        player_col = source_spec["id_cols"].get("player")
        franchise_col = field_map.get("franchise_id")
        player_id = entity_context.get("player_id")
        franchise_id = entity_context.get("franchise_id")
        if player_col is None or franchise_col is None:
            raise ValueError(f"Source {source!r} does not support scope {scope!r}")
        if not player_id or not franchise_id:
            return None
        base_q = base_q.filter(player_col == player_id, franchise_col == franchise_id)
    else:
        id_col = source_spec["id_cols"].get(scope)
        if id_col is None:
            raise ValueError(f"Source {source!r} does not support scope {scope!r}")
        base_q = base_q.filter(id_col == entity_id)
    base_q = _apply_season_scope(base_q, season, definition)

    if aggregation == "count":
        q = _apply_filters(base_q, field_map, filters, entity_context)
        result = q.count()
        value = float(result)
        return {
            "value_num": value,
            "value_str": _format_value(definition, value),
            "context": _context_with_counts(source, scope, int(result), rank_group),
            "rank_group": rank_group,
        }

    if aggregation == "pct_rows":
        # % of rows matching the filter vs total
        total = base_q.count()
        if not total:
            return None
        matched = _apply_filters(base_q, field_map, filters, entity_context).count()
        value = matched / total
        context = _context_with_counts(source, scope, int(total), rank_group)
        context["matched_rows"] = int(matched)
        return {
            "value_num": value,
            "value_str": _format_value(definition, value),
            "context": context,
            "rank_group": rank_group,
        }

    if aggregation in ("avg", "sum", "max"):
        stat_col = field_map.get(definition["stat"])
        if stat_col is None:
            raise ValueError(f"Unknown stat: {definition['stat']!r}")
        agg_fn = {
            "avg": func.avg,
            "sum": func.sum,
            "max": func.max,
        }[aggregation]
        q = _apply_filters(base_q, field_map, filters, entity_context)
        val = q.with_entities(agg_fn(func.coalesce(stat_col, 0))).scalar()
        if val is None:
            return None
        value = float(val)
        row_count = q.count()
        return {
            "value_num": value,
            "value_str": _format_value(definition, value),
            "context": _context_with_counts(source, scope, row_count, rank_group),
            "rank_group": rank_group,
        }

    if aggregation == "ratio":
        num_col = field_map.get(definition["numerator"])
        den_col = field_map.get(definition["denominator"])
        if num_col is None or den_col is None:
            raise ValueError("ratio requires numerator and denominator fields")
        q = _apply_filters(base_q, field_map, filters, entity_context)
        row = q.with_entities(
            func.sum(func.coalesce(num_col, 0)),
            func.sum(func.coalesce(den_col, 0)),
        ).one()
        num, den = row
        if not den:
            return None
        value = float(num) / float(den)
        context = _context_with_counts(source, scope, q.count(), rank_group)
        context["numerator_sum"] = float(num or 0)
        context["denominator_sum"] = float(den or 0)
        return {
            "value_num": value,
            "value_str": _format_value(definition, value),
            "context": context,
            "rank_group": rank_group,
        }

    if aggregation == "pct_of_total":
        # entity's sum as % of all entities' sum for this season
        stat_col = field_map.get(definition["stat"])
        if stat_col is None:
            raise ValueError(f"Unknown stat: {definition['stat']!r}")
        entity_q = _apply_filters(base_q, field_map, filters, entity_context)
        entity_sum = entity_q.with_entities(func.sum(func.coalesce(stat_col, 0))).scalar() or 0

        total_q = (
            session.query(func.sum(func.coalesce(stat_col, 0)))
            .select_from(model)
            .join(Game, model.game_id == Game.game_id)
        )
        if team_join_col is not None:
            total_q = total_q.outerjoin(Team, team_join_col == Team.team_id)
        total_q = _apply_season_scope(total_q, season, definition)
        total_q = _apply_filters(total_q, field_map, filters, entity_context)
        total_sum = total_q.scalar() or 0
        if not total_sum:
            return None
        value = float(entity_sum) / float(total_sum)
        context = _context_with_counts(source, scope, entity_q.count(), rank_group)
        context["entity_sum"] = float(entity_sum)
        context["group_sum"] = float(total_sum)
        return {
            "value_num": value,
            "value_str": _format_value(definition, value),
            "context": context,
            "rank_group": rank_group,
        }

    raise ValueError(f"Unsupported aggregation: {aggregation!r}")


def compute(
    session: Session,
    definition: dict,
    entity_id: str,
    season: str,
    scope: str,
) -> float | None:
    result = compute_result(session, definition, entity_id, season, scope)
    if result is None:
        return None
    return result["value_num"]


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
    definition = _normalize_definition(definition)
    source = _definition_source(definition)
    if source not in _SOURCE_MAP:
        raise ValueError(f"Unknown source: {source!r}")

    source_spec = _SOURCE_MAP[source]
    model = source_spec["model"]
    field_map = source_spec["field_map"]
    # Get distinct entity_ids for this season
    if scope == "player_franchise":
        player_col = source_spec["id_cols"].get("player")
        franchise_col = field_map.get("franchise_id")
        if player_col is None or franchise_col is None:
            raise ValueError(f"Source {source!r} does not support scope {scope!r}")
        entity_q = (
            session.query(player_col, franchise_col)
            .join(Game, model.game_id == Game.game_id)
            .filter(player_col.isnot(None), franchise_col.isnot(None))
        )
    else:
        id_col = source_spec["id_cols"].get(scope)
        if id_col is None:
            raise ValueError(f"Source {source!r} does not support scope {scope!r}")
        entity_q = (
            session.query(id_col)
            .join(Game, model.game_id == Game.game_id)
            .filter(id_col.isnot(None))
        )
    team_join_col = source_spec.get("team_join_col")
    if team_join_col is not None:
        entity_q = entity_q.outerjoin(Team, team_join_col == Team.team_id)
    preview_season = CAREER_SEASON if str(definition.get("time_scope") or "").lower() == "career" else season
    entity_q = _apply_season_scope(entity_q, preview_season, definition)
    if scope == "player_franchise":
        entity_ids = [
            _player_franchise_entity_id(row[0], row[1])
            for row in entity_q.distinct().all()
            if _player_franchise_entity_id(row[0], row[1]) is not None
        ]
    else:
        entity_ids = [row[0] for row in entity_q.distinct().all()]

    rows = []
    for eid in entity_ids:
        try:
            result = compute_result(session, definition, eid, preview_season, scope)
        except Exception as exc:
            logger.debug("preview compute failed for %s: %s", eid, exc)
            continue
        if result is None:
            continue
        baseline = compute_baseline(session, definition, eid, preview_season, scope)
        rows.append({
            "entity_id": eid,
            "value_num": round(result["value_num"], 4),
            "baseline": round(baseline, 4) if baseline is not None else None,
            "rank_group": result.get("rank_group"),
        })

    rows.sort(key=lambda r: r["value_num"], reverse=True)
    return rows[:limit]
