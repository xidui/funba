"""Next UI — games list enrichment.

* ``recent``  — when the default landing page (no ``page`` param) shows few
  finished games (the classic paginator lands on "today", which is mostly
  scheduled games), append the most recent two game-days of results that
  match the current year/phase/team filters.
* ``spark[game_id]`` — 49-point home-minus-road margin series (from PBP)
* ``top[game_id]``   — top scorer per side ({road: {...}, home: {...}})
Everything is computed for visible games only and cached for 10 minutes.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from types import SimpleNamespace

from flask import request
from sqlalchemy import or_


def _page_entries(ctx: dict) -> list:
    out = []
    for _dt, entries in (ctx.get("date_groups") or []):
        out.extend(entries or [])
    return out


def _season_ids(year: str | None, phase: str | None) -> list[str]:
    if not year:
        return []
    if phase in ("1", "2", "3"):
        prefixes = [phase]
    elif phase == "4":
        prefixes = ["4", "5"]
    else:
        prefixes = ["1", "2", "3", "4", "5"]
    return [p + str(year) for p in prefixes]


def _recent_groups(ctx: dict, api) -> list:
    if "page" in request.args:
        return []
    entries = _page_entries(ctx)
    done = [e for e in entries if getattr(e, "status", None) == "completed"]
    if len(done) >= 8:
        return []
    today = ctx.get("today") or date.today()
    page_dates = [e.game_date for e in entries if getattr(e, "game_date", None)]
    before = min([d for d in page_dates if d <= today] or [today])
    seasons = _season_ids(ctx.get("selected_year"), ctx.get("selected_phase"))
    team = ctx.get("selected_team")
    seen_ids = {e.game_id for e in entries}

    def build():
        Game = api.models.Game
        with api.session_local() as session:
            q = session.query(Game).filter(
                Game.game_status == "completed",
                Game.game_date.isnot(None),
                Game.game_date <= before,
            )
            if seasons:
                q = q.filter(Game.season.in_(seasons))
            if team:
                q = q.filter(or_(Game.home_team_id == team, Game.road_team_id == team))
            rows = q.order_by(Game.game_date.desc(), Game.game_id.desc()).limit(40).all()
            dates = []
            for g in rows:
                if g.game_date not in dates:
                    dates.append(g.game_date)
            keep = set(dates[:2])
            groups: dict = defaultdict(list)
            for g in rows:
                if g.game_date not in keep or g.game_id in seen_ids:
                    continue
                groups[g.game_date].append(SimpleNamespace(
                    game=g,
                    game_id=g.game_id,
                    game_date=g.game_date,
                    season=g.season,
                    road_team_id=g.road_team_id,
                    home_team_id=g.home_team_id,
                    road_score=g.road_team_score,
                    home_score=g.home_team_score,
                    road_won=bool(g.wining_team_id) and g.wining_team_id == g.road_team_id,
                    home_won=bool(g.wining_team_id) and g.wining_team_id == g.home_team_id,
                    status="completed",
                    status_summary="",
                    link_enabled=True,
                ))
                session.expunge(g)
            return sorted(groups.items(), key=lambda kv: kv[0], reverse=True)

    key = f"next_games_recent:{ctx.get('selected_year')}:{ctx.get('selected_phase')}:{team}:{before}"
    return api.cached(key, 300, build)


def _spark_and_tops(entries: list, api) -> dict:
    done = [e for e in entries if getattr(e, "status", None) == "completed" and getattr(e, "game_id", None)][:90]
    if not done:
        return {"spark": {}, "top": {}}
    ids = tuple(sorted({e.game_id for e in done}))
    sides = {e.game_id: (e.road_team_id, e.home_team_id) for e in done}

    def build():
        m = api.models
        with api.session_local() as session:
            pbp = (
                session.query(m.GamePlayByPlay.game_id, m.GamePlayByPlay.period, m.GamePlayByPlay.pc_time, m.GamePlayByPlay.score)
                .filter(m.GamePlayByPlay.game_id.in_(ids), m.GamePlayByPlay.score.isnot(None))
                .order_by(m.GamePlayByPlay.game_id, m.GamePlayByPlay.period, m.GamePlayByPlay.event_num)
                .all()
            )
            stats = (
                session.query(
                    m.PlayerGameStats.game_id,
                    m.PlayerGameStats.team_id,
                    m.PlayerGameStats.player_id,
                    m.PlayerGameStats.pts,
                    m.PlayerGameStats.reb,
                    m.PlayerGameStats.ast,
                    m.Player.full_name,
                    m.Player.full_name_zh,
                )
                .join(m.Player, m.Player.player_id == m.PlayerGameStats.player_id)
                .filter(m.PlayerGameStats.game_id.in_(ids))
                .all()
            )
        by_game = defaultdict(list)
        for row in pbp:
            by_game[row.game_id].append(row)
        spark = {}
        for gid in ids:
            series = api.margin_series(by_game.get(gid, []))
            if series:
                spark[gid] = series
        best: dict = {}
        for row in stats:
            key = (row.game_id, row.team_id)
            pts = row.pts or 0
            cur = best.get(key)
            if cur is None or (pts, (row.reb or 0) + (row.ast or 0)) > (cur["pts"], cur["reb"] + cur["ast"]):
                best[key] = {
                    "player_id": row.player_id,
                    "name": row.full_name,
                    "name_zh": row.full_name_zh,
                    "pts": pts,
                    "reb": row.reb or 0,
                    "ast": row.ast or 0,
                }
        top = {gid: {"road": best.get((gid, r)), "home": best.get((gid, h))} for gid, (r, h) in sides.items()}
        return {"spark": spark, "top": top}

    return api.cached("next_games_list:" + ",".join(ids), 600, build)


def enrich(ctx: dict, api) -> dict:
    out: dict = {"spark": {}, "top": {}, "recent": []}
    try:
        out["recent"] = _recent_groups(ctx, api)
    except Exception:
        api.app.logger.warning("next-ui games_list recent failed", exc_info=True)
    try:
        entries = _page_entries(ctx)
        for _dt, extra in out["recent"]:
            entries.extend(extra)
        out.update(_spark_and_tops(entries, api))
    except Exception:
        api.app.logger.warning("next-ui games_list enrich failed", exc_info=True)
    return out
