"""NBA data queries for content pipeline — used by data API endpoints."""
from __future__ import annotations

import json
import logging
from datetime import date

from sqlalchemy import func
from sqlalchemy.orm import sessionmaker

from db.models import (
    Game, GameLineScore, GamePlayByPlay, MetricDefinition, MetricResult,
    MetricRunLog, Player, PlayerGameStats, Team, TeamGameStats, engine,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://funba.app"

Session = sessionmaker(bind=engine)


# ---------------------------------------------------------------------------
# Data query functions (exposed via /api/data/ endpoints)
# ---------------------------------------------------------------------------


def get_metric_top_results(metric_key: str, season: str | None = None, limit: int = 10) -> list[dict]:
    """Return top N results for a metric with entity names and values."""
    session = Session()
    try:
        q = session.query(MetricResult).filter(
            MetricResult.metric_key == metric_key,
            MetricResult.value_num.isnot(None),
        )
        if season:
            q = q.filter(MetricResult.season == season)

        md = session.query(MetricDefinition.code_python).filter(
            MetricDefinition.key == metric_key,
        ).first()
        descending = True
        if md and md.code_python and "rank_order = \"asc\"" in md.code_python:
            descending = False

        q = q.order_by(MetricResult.value_num.desc() if descending else MetricResult.value_num.asc())
        rows = q.limit(limit).all()

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
                "entity_id": r.entity_id,
                "value": r.value_num,
                "value_str": r.value_str,
                "season": r.season,
            })
        return results
    finally:
        session.close()


def get_game_box_score(game_id: str) -> dict:
    """Return box score for a game: team totals and player stats."""
    session = Session()
    try:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if not game:
            return {"error": "Game not found"}

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
                "team_id": ts.team_id,
                "pts": ts.pts, "fgm": ts.fgm, "fga": ts.fga,
                "fg3m": ts.fg3m, "fg3a": ts.fg3a,
                "ftm": ts.ftm, "fta": ts.fta,
                "reb": ts.reb, "ast": ts.ast, "tov": ts.tov,
            })

        players = []
        for ps, name in player_stats:
            players.append({
                "name": name, "player_id": ps.player_id,
                "team": team_map.get(ps.team_id, ps.team_id),
                "pts": ps.pts, "reb": ps.reb, "ast": ps.ast,
                "min": ps.min, "starter": bool(ps.starter),
            })

        return {
            "game_id": game_id,
            "game_date": game.game_date.isoformat() if game.game_date else None,
            "home_team": team_map.get(game.home_team_id, game.home_team_id),
            "road_team": team_map.get(game.road_team_id, game.road_team_id),
            "home_score": game.home_team_score,
            "road_score": game.road_team_score,
            "teams": teams,
            "players": players,
        }
    finally:
        session.close()


def get_game_play_by_play(game_id: str, period: int) -> list[dict]:
    """Return PBP for a specific period."""
    session = Session()
    try:
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
        return plays[-30:]
    finally:
        session.close()


def get_games_by_date(target_date: date) -> list[dict]:
    """Return all games for a date with scores and basic info."""
    session = Session()
    try:
        games = session.query(Game).filter(
            Game.game_date == target_date,
        ).order_by(Game.game_id).all()

        if not games:
            return []

        team_map = {t.team_id: t.abbr for t in session.query(Team.team_id, Team.abbr).all()}

        # Check OT
        ot_game_ids = set()
        game_ids = [g.game_id for g in games]
        ot_rows = session.query(GameLineScore.game_id).filter(
            GameLineScore.game_id.in_(game_ids),
            GameLineScore.ot1_pts.isnot(None),
        ).distinct().all()
        ot_game_ids = {r[0] for r in ot_rows}

        results = []
        for g in games:
            results.append({
                "game_id": g.game_id,
                "season": g.season,
                "home_team": team_map.get(g.home_team_id, g.home_team_id),
                "road_team": team_map.get(g.road_team_id, g.road_team_id),
                "home_team_id": g.home_team_id,
                "road_team_id": g.road_team_id,
                "home_score": g.home_team_score,
                "road_score": g.road_team_score,
                "winner": team_map.get(g.wining_team_id, g.wining_team_id) if g.wining_team_id else None,
                "overtime": g.game_id in ot_game_ids,
                "url": f"{_BASE_URL}/games/{g.game_id}",
            })
        return results
    finally:
        session.close()


def get_triggered_metrics(target_date: date) -> list[dict]:
    """Return metrics triggered by games on a given date, ranked by noteworthiness."""
    session = Session()
    try:
        games = session.query(Game).filter(Game.game_date == target_date).all()
        if not games:
            return []

        team_map = {t.team_id: t.abbr for t in session.query(Team.team_id, Team.abbr).all()}
        game_ids = [g.game_id for g in games]
        game_season = {g.game_id: g.season for g in games}

        run_logs = session.query(
            MetricRunLog.game_id, MetricRunLog.metric_key,
            MetricRunLog.entity_type, MetricRunLog.entity_id,
        ).filter(
            MetricRunLog.game_id.in_(game_ids),
            MetricRunLog.produced_result == True,
        ).all()

        triggered_keys = {(rl.metric_key, rl.entity_type, rl.entity_id) for rl in run_logs}
        if not triggered_keys:
            return []

        # Bulk fetch MetricResult
        metric_keys = {k for k, _, _ in triggered_keys}
        entity_ids = {eid for _, _, eid in triggered_keys}
        mr_rows = session.query(MetricResult).filter(
            MetricResult.metric_key.in_(metric_keys),
            MetricResult.entity_id.in_(entity_ids),
            MetricResult.value_num.isnot(None),
        ).all()
        result_map: dict[tuple, MetricResult] = {}
        for mr in mr_rows:
            result_map[(mr.metric_key, mr.entity_type, mr.entity_id, mr.season)] = mr

        # MetricDefinition lookup
        md_map = {
            md.key: md
            for md in session.query(MetricDefinition).filter(
                MetricDefinition.key.in_(metric_keys),
                MetricDefinition.status == "published",
            ).all()
        }

        # Player name lookup
        player_ids = {eid for _, et, eid in triggered_keys if et == "player"}
        player_names = {
            p.player_id: p.full_name
            for p in session.query(Player.player_id, Player.full_name).filter(
                Player.player_id.in_(player_ids)
            ).all()
        } if player_ids else {}

        # Rank cache
        rank_cache: dict[int, tuple[int, int]] = {}

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

        # Build results grouped by game
        seen: dict[str, dict] = {}  # metric_key -> best entry
        for rl in run_logs:
            mk, et, eid = rl.metric_key, rl.entity_type, rl.entity_id
            md = md_map.get(mk)
            if not md or mk.endswith("_career"):
                continue

            season = game_season.get(rl.game_id)
            mr = result_map.get((mk, et, eid, season))
            if not mr:
                for key, val in result_map.items():
                    if key[0] == mk and key[1] == et and key[2] == eid:
                        mr = val
                        break
            if not mr or mr.value_num is None:
                continue

            rank, total = _get_rank(mr)
            pct = rank / total if total > 0 else 1.0
            entity_name = player_names.get(eid) or team_map.get(eid) or eid

            existing = seen.get(mk)
            if existing and existing["rank_pct"] <= pct:
                continue

            seen[mk] = {
                "metric_key": mk,
                "metric_name": md.name,
                "scope": md.scope,
                "entity": entity_name,
                "entity_id": eid,
                "entity_type": et,
                "game_id": rl.game_id,
                "value": mr.value_num,
                "value_str": mr.value_str or str(mr.value_num),
                "rank": rank,
                "total": total,
                "rank_pct": pct,
                "notable": pct <= 0.25,
                "metric_url": f"{_BASE_URL}/metrics/{mk}",
            }

        return sorted(seen.values(), key=lambda x: x["rank_pct"])
    finally:
        session.close()


def get_game_metrics(game_id: str) -> list[dict]:
    """Return all metrics triggered by a single game, ranked by noteworthiness."""
    session = Session()
    try:
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if not game:
            return []

        team_map = {t.team_id: t.abbr for t in session.query(Team.team_id, Team.abbr).all()}

        run_logs = session.query(
            MetricRunLog.metric_key, MetricRunLog.entity_type, MetricRunLog.entity_id,
        ).filter(
            MetricRunLog.game_id == game_id,
            MetricRunLog.produced_result == True,
        ).all()
        if not run_logs:
            return []

        triggered = [(rl.metric_key, rl.entity_type, rl.entity_id) for rl in run_logs]
        metric_keys = {k for k, _, _ in triggered}
        entity_ids = {eid for _, _, eid in triggered}

        mr_rows = session.query(MetricResult).filter(
            MetricResult.metric_key.in_(metric_keys),
            MetricResult.entity_id.in_(entity_ids),
            MetricResult.value_num.isnot(None),
        ).all()
        result_map: dict[tuple, MetricResult] = {}
        for mr in mr_rows:
            result_map[(mr.metric_key, mr.entity_type, mr.entity_id, mr.season)] = mr

        md_map = {
            md.key: md
            for md in session.query(MetricDefinition).filter(
                MetricDefinition.key.in_(metric_keys),
                MetricDefinition.status == "published",
            ).all()
        }

        player_ids = {eid for _, et, eid in triggered if et == "player"}
        player_names = {
            p.player_id: p.full_name
            for p in session.query(Player.player_id, Player.full_name).filter(
                Player.player_id.in_(player_ids)
            ).all()
        } if player_ids else {}

        rank_cache: dict[int, tuple[int, int]] = {}

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

        results = []
        for mk, et, eid in triggered:
            md = md_map.get(mk)
            if not md or mk.endswith("_career"):
                continue

            mr = result_map.get((mk, et, eid, game.season))
            if not mr:
                for key, val in result_map.items():
                    if key[0] == mk and key[1] == et and key[2] == eid:
                        mr = val
                        break
            if not mr or mr.value_num is None:
                continue

            rank, total = _get_rank(mr)
            pct = rank / total if total > 0 else 1.0
            entity_name = player_names.get(eid) or team_map.get(eid) or eid

            results.append({
                "metric_key": mk,
                "metric_name": md.name,
                "scope": md.scope,
                "entity": entity_name,
                "entity_id": eid,
                "entity_type": et,
                "value": mr.value_num,
                "value_str": mr.value_str or str(mr.value_num),
                "rank": rank,
                "total": total,
                "rank_pct": pct,
                "notable": pct <= 0.25,
                "metric_url": f"{_BASE_URL}/metrics/{mk}",
            })

        return sorted(results, key=lambda x: x["rank_pct"])
    finally:
        session.close()
