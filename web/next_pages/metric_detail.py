"""Next UI — metric detail page plugin.

``enrich`` adds:
* ``row_team``: {row index -> team_id} used for team-color accents. Players are
  resolved to the team they played for in that row's season (or their latest
  team for career / multi-season rows); team rows use themselves; game rows
  use the subject team (``game_id:team_id``) or the home team.
* ``bar_max``: largest |value| on the page, for the inline value bars.
* ``show_podium``: whether to render the top-3 podium.
"""
from __future__ import annotations

from collections import defaultdict

from sqlalchemy import func


def _player_teams(api, wanted: dict[str, set[str]]) -> dict[tuple[str, str], str]:
    """wanted: {season_or_'*': {player_id}} -> {(player_id, season_or_'*'): team_id}."""
    Game = api.models.Game
    PGS = api.models.PlayerGameStats
    out: dict[tuple[str, str], str] = {}
    with api.session_local() as session:
        concrete = {s: ids for s, ids in wanted.items() if s != "*" and ids}
        if concrete:
            all_ids = sorted({pid for ids in concrete.values() for pid in ids})
            seasons = sorted(concrete.keys())
            rows = (
                session.query(PGS.player_id, Game.season, PGS.team_id, Game.game_date)
                .join(Game, Game.game_id == PGS.game_id)
                .filter(PGS.player_id.in_(all_ids), Game.season.in_(seasons))
                .order_by(Game.game_date.asc())
                .all()
            )
            for pid, season, team_id, _d in rows:
                out[(str(pid), str(season))] = team_id
        latest_ids = sorted(wanted.get("*", set()))
        if latest_ids:
            last_date = (
                session.query(PGS.player_id.label("pid"), func.max(Game.game_date).label("d"))
                .join(Game, Game.game_id == PGS.game_id)
                .filter(PGS.player_id.in_(latest_ids))
                .group_by(PGS.player_id)
                .subquery()
            )
            rows = (
                session.query(PGS.player_id, PGS.team_id)
                .join(Game, Game.game_id == PGS.game_id)
                .join(last_date, (last_date.c.pid == PGS.player_id) & (last_date.c.d == Game.game_date))
                .all()
            )
            for pid, team_id in rows:
                out[(str(pid), "*")] = team_id
    return out


def enrich(ctx: dict, api) -> dict:
    rows = ctx.get("result_rows") or []
    metric_def = ctx.get("metric_def")
    selected = str(ctx.get("selected_season") or "")

    values = [abs(float(r.get("value_num"))) for r in rows if isinstance(r, dict) and r.get("value_num") is not None]
    bar_max = max(values) if values else 0.0

    def season_key(row: dict) -> str:
        raw = str(row.get("season_raw") or selected or "")
        return raw if (len(raw) == 5 and raw.isdigit()) else "*"

    wanted: dict[str, set[str]] = defaultdict(set)
    for r in rows:
        if r.get("entity_type") in ("player", "player_franchise") and r.get("entity_id"):
            wanted[season_key(r)].add(str(r["entity_id"]).split(":")[0])

    key = (getattr(metric_def, "key", ""), selected, ctx.get("page"), ctx.get("search_q"), ctx.get("entity_filter_id"),
           ctx.get("sub_key_filter"), ctx.get("active_only"), ctx.get("expand"), ctx.get("sort_by"))
    try:
        team_lookup = api.cached(f"nx_md_teams:{key}", 600, lambda: _player_teams(api, wanted)) if wanted else {}
    except Exception:
        team_lookup = {}

    row_team: dict[int, str] = {}
    for idx, r in enumerate(rows):
        et = r.get("entity_type")
        eid = str(r.get("entity_id") or "")
        team_id = None
        if et in ("player", "player_franchise"):
            pid = eid.split(":")[0]
            team_id = team_lookup.get((pid, season_key(r))) or team_lookup.get((pid, "*"))
            if not team_id and ":" in eid:
                team_id = eid.split(":")[1]
        elif et == "team":
            team_id = eid
        elif et == "game":
            parts = eid.split(":")
            team_id = parts[1] if len(parts) > 1 else None
        if team_id:
            row_team[idx] = str(team_id)

    show_podium = (
        ctx.get("page", 1) == 1
        and not ctx.get("search_q")
        and not ctx.get("entity_filter_id")
        and not ctx.get("sub_key_filter")
        and not ctx.get("sort_by")
        and len(rows) >= 3
        and all(r.get("value_num") is not None for r in rows[:3])
    )
    return {"row_team": row_team, "bar_max": bar_max, "show_podium": show_podium}
