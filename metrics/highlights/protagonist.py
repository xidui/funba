"""Resolve which team is the visual/narrative protagonist of a game-scope metric.

Game metrics like ``most_threes_made_in_a_loss`` are about the LOSING team, not
the winner. Other game metrics (largest margin of victory, comeback win, …)
are about the winner. The metric author signals this via
``MetricDefinition.protagonist``; downstream consumers (hero posters,
narratives) call this resolver to convert the per-metric signal plus the game
context into a concrete ``team_id``.
"""
from __future__ import annotations

import json
from typing import Any

from sqlalchemy.orm import Session

from db.models import Game, MetricDefinition, MetricResult


def resolve_team_id(
    protagonist: str | None, ctx: dict[str, Any] | None, game: Game | None
) -> str | None:
    """Map a (protagonist, context, game) triple to the team_id to anchor on.

    - ``context_team_id`` (or NULL when ``ctx['team_id']`` is set): use the
      team_id the metric stamped into its result context.
    - ``winner`` / ``loser`` / ``home`` / ``road``: pick from the game.
    - Otherwise (NULL, no context team_id): return None — caller falls back
      to its own default (typically the winner).
    """
    if protagonist == "context_team_id" or not protagonist:
        if isinstance(ctx, dict):
            tid = ctx.get("team_id")
            if tid:
                return str(tid)
        if not protagonist:
            return None
    if not game:
        return None
    if protagonist == "winner":
        return str(game.wining_team_id) if game.wining_team_id else None
    if protagonist == "loser":
        if not game.wining_team_id:
            return None
        if str(game.home_team_id) == str(game.wining_team_id):
            return str(game.road_team_id) if game.road_team_id else None
        return str(game.home_team_id) if game.home_team_id else None
    if protagonist == "home":
        return str(game.home_team_id) if game.home_team_id else None
    if protagonist == "road":
        return str(game.road_team_id) if game.road_team_id else None
    return None


def lookup_team_id(
    session: Session, metric_key: str, game_id: str, season: str | None, game: Game | None
) -> str | None:
    """Best-effort resolver when the caller only has metric_key + game.

    Reads ``MetricDefinition.protagonist`` and a matching ``MetricResult``'s
    ``context_json`` and feeds them into :func:`resolve_team_id`. Returns
    None when nothing useful can be inferred.
    """
    if not metric_key or not game_id:
        return None
    md = (
        session.query(MetricDefinition.protagonist)
        .filter(MetricDefinition.key == metric_key)
        .first()
    )
    protagonist = md[0] if md else None
    ctx: dict[str, Any] = {}
    q = session.query(MetricResult.context_json).filter(
        MetricResult.metric_key == metric_key,
        MetricResult.entity_type == "game",
        MetricResult.entity_id == str(game_id),
    )
    if season:
        q = q.filter(MetricResult.season == str(season))
    row = q.first()
    if row and row[0]:
        try:
            parsed = json.loads(row[0])
            if isinstance(parsed, dict):
                ctx = parsed
        except Exception:
            ctx = {}
    return resolve_team_id(protagonist, ctx, game)
