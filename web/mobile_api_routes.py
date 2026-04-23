"""JSON API consumed by the React Native mobile app.

All endpoints live under ``/api/v1/mobile/`` and return clean JSON payloads
derived from the SQLAlchemy models. Language selection is via the
``?lang=en|zh`` query parameter (default ``en``). Auth is via a bearer token
issued when a magic link is verified through this API.

The module is deliberately self-contained: it talks to the DB directly instead
of reusing the server-rendered route machinery, which is tightly coupled to
Jinja templates.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from flask import Flask, abort, g, jsonify, request
from itsdangerous import BadSignature, URLSafeTimedSerializer
from sqlalchemy import and_, case, desc, func, or_
from sqlalchemy.orm import Session

from db.models import (
    Award,
    Feedback,
    Game,
    GameLineScore,
    GamePlayByPlay,
    MagicToken,
    MetricDefinition,
    MetricResult,
    NewsArticle,
    NewsArticlePlayer,
    NewsArticleTeam,
    NewsCluster,
    Player,
    PlayerGameStats,
    ShotRecord,
    Team,
    TeamCoachStint,
    TeamGameStats,
    TeamRosterStint,
    User,
)

logger = logging.getLogger(__name__)

_TOKEN_SALT = "funba-mobile-auth-v1"
_TOKEN_MAX_AGE_SECONDS = 90 * 24 * 3600  # 90 days
_MAGIC_TOKEN_TTL_MINUTES = 15


def _serializer(app: Flask) -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(app.secret_key, salt=_TOKEN_SALT)


def _issue_token(app: Flask, user_id: str) -> str:
    return _serializer(app).dumps({"uid": user_id})


def _load_user(app: Flask, session: Session) -> User | None:
    header = request.headers.get("Authorization", "")
    if not header.lower().startswith("bearer "):
        return None
    token = header.split(None, 1)[1].strip()
    try:
        payload = _serializer(app).loads(token, max_age=_TOKEN_MAX_AGE_SECONDS)
    except BadSignature:
        return None
    user_id = (payload or {}).get("uid")
    if not user_id:
        return None
    return session.query(User).filter(User.id == user_id).first()


def _is_zh() -> bool:
    return (request.args.get("lang") or "").lower() == "zh"


def _player_name(player: Player | None) -> str:
    if player is None:
        return "-"
    if _is_zh() and player.full_name_zh:
        return player.full_name_zh
    return player.full_name or player.player_id or "-"


def _team_name(team: Team | None) -> str:
    if team is None:
        return "-"
    if _is_zh() and team.full_name_zh:
        return team.full_name_zh
    return team.full_name or team.abbr or team.team_id or "-"


def _season_label(season: str | int | None) -> str:
    """22025 → '2025-26 Regular Season' / '2025-26 常规赛'."""
    if season is None:
        return ""
    s = str(season)
    if len(s) != 5 or not s.isdigit():
        return s
    kind_code, year = s[0], int(s[1:])
    y2 = str((year + 1) % 100).zfill(2)
    base = f"{year}-{y2}"
    kind_en = {"0": "Pre-Season", "2": "Regular Season", "3": "All-Star", "4": "Playoffs", "5": "Play-In"}
    kind_zh = {"0": "季前赛", "2": "常规赛", "3": "全明星", "4": "季后赛", "5": "附加赛"}
    if _is_zh():
        return f"{base} {kind_zh.get(kind_code, '')}".strip()
    return f"{base} {kind_en.get(kind_code, '')}".strip()


def _serialize_team(team: Team | None) -> dict[str, Any] | None:
    if team is None:
        return None
    return {
        "team_id": team.team_id,
        "slug": team.slug or team.team_id,
        "abbr": team.abbr,
        "full_name": team.full_name,
        "full_name_zh": team.full_name_zh,
        "display_name": _team_name(team),
        "nick_name": team.nick_name,
        "city": team.city,
        "is_active": bool(team.active),
    }


def _serialize_player(player: Player | None) -> dict[str, Any] | None:
    if player is None:
        return None
    return {
        "player_id": player.player_id,
        "slug": player.slug or player.player_id,
        "full_name": player.full_name,
        "full_name_zh": player.full_name_zh,
        "display_name": _player_name(player),
        "is_active": bool(player.is_active),
        "position": player.position,
        "jersey": player.jersey,
        "height": player.height,
        "weight": player.weight,
        "birth_date": player.birth_date.isoformat() if player.birth_date else None,
        "country": player.country,
        "school": player.school,
        "draft_year": player.draft_year,
        "draft_round": player.draft_round,
        "draft_number": player.draft_number,
        "from_year": player.from_year,
        "to_year": player.to_year,
    }


def _zero_stats() -> dict[str, int]:
    return {
        "games": 0, "min": 0, "pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0,
        "tov": 0, "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0,
        "oreb": 0, "dreb": 0, "pf": 0, "plus": 0,
    }


def _aggregate_player_stats(rows: list[PlayerGameStats]) -> dict[str, Any]:
    total = _zero_stats()
    for r in rows:
        total["games"] += 1
        total["min"] += (r.min or 0)
        total["pts"] += (r.pts or 0)
        total["reb"] += (r.reb or 0)
        total["ast"] += (r.ast or 0)
        total["stl"] += (r.stl or 0)
        total["blk"] += (r.blk or 0)
        total["tov"] += (r.tov or 0)
        total["fgm"] += (r.fgm or 0)
        total["fga"] += (r.fga or 0)
        total["fg3m"] += (r.fg3m or 0)
        total["fg3a"] += (r.fg3a or 0)
        total["ftm"] += (r.ftm or 0)
        total["fta"] += (r.fta or 0)
        total["oreb"] += (r.oreb or 0)
        total["dreb"] += (r.dreb or 0)
        total["pf"] += (r.pf or 0)
        total["plus"] += (r.plus or 0)
    total["fg_pct"] = (total["fgm"] / total["fga"]) if total["fga"] else None
    total["fg3_pct"] = (total["fg3m"] / total["fg3a"]) if total["fg3a"] else None
    total["ft_pct"] = (total["ftm"] / total["fta"]) if total["fta"] else None
    if total["games"]:
        total["ppg"] = round(total["pts"] / total["games"], 1)
        total["rpg"] = round(total["reb"] / total["games"], 1)
        total["apg"] = round(total["ast"] / total["games"], 1)
        total["mpg"] = round(total["min"] / total["games"], 1)
    else:
        total["ppg"] = total["rpg"] = total["apg"] = total["mpg"] = 0.0
    return total


def _game_status(g: Game) -> str:
    if g.home_team_score is not None and g.road_team_score is not None:
        return "completed"
    if (g.game_status or "").lower() in ("live", "in progress"):
        return "live"
    return "upcoming"


def _format_game_date(d) -> str:
    if d is None:
        return ""
    return d.isoformat() if hasattr(d, "isoformat") else str(d)


def _team_lookup(session: Session, team_ids: list[str]) -> dict[str, Team]:
    if not team_ids:
        return {}
    rows = session.query(Team).filter(Team.team_id.in_(list(set(team_ids)))).all()
    return {t.team_id: t for t in rows}


def _player_lookup(session: Session, ids: list[str]) -> dict[str, Player]:
    if not ids:
        return {}
    rows = session.query(Player).filter(Player.player_id.in_(list(set(ids)))).all()
    return {p.player_id: p for p in rows}


# ---------------------------------------------------------------------------
# Route factory
# ---------------------------------------------------------------------------

def register_mobile_api_routes(app: Flask, *, session_factory: Callable[[], Session], send_magic_link: Callable[[str, str, str | None], None] | None = None) -> None:
    """Register ``/api/v1/mobile/*`` JSON routes on the given Flask app.

    ``send_magic_link(email, token, deep_link)`` is the hook used to deliver the
    magic-link email; if omitted we log a warning and no email is sent (useful
    for local dev).
    """

    @app.after_request
    def _mobile_cors(response):
        if not (request.path or "").startswith("/api/v1/mobile/"):
            return response
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        return response

    @app.route("/api/v1/mobile/<path:_any>", methods=["OPTIONS"])
    def mobile_options(_any):
        return ("", 204)

    def _current_user(session: Session) -> User | None:
        return _load_user(app, session)

    # ------------------------------------------------------------------
    # Health / meta
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/health")
    def mobile_health():
        return jsonify({"ok": True, "version": "1"})

    @app.route("/api/v1/mobile/me")
    def mobile_me():
        with session_factory() as session:
            user = _current_user(session)
            if user is None:
                return jsonify({"user": None})
            return jsonify({
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "display_name": user.display_name,
                    "avatar_url": user.avatar_url,
                    "is_admin": bool(user.is_admin),
                    "subscription_tier": user.subscription_tier,
                    "subscription_status": user.subscription_status,
                },
            })

    # ------------------------------------------------------------------
    # Home: standings + team grid
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/home")
    def mobile_home():
        season = request.args.get("season") or _current_regular_season()
        with session_factory() as session:
            teams = (
                session.query(Team)
                .filter(Team.active == True, Team.canonical_team_id.is_(None))
                .order_by(Team.full_name)
                .all()
            )
            standings = _compute_standings(session, season)
            return jsonify({
                "season": season,
                "season_label": _season_label(season),
                "season_options": _available_seasons(session, kind="2"),
                "teams": [_serialize_team(t) for t in teams],
                "east_standings": standings["east"],
                "west_standings": standings["west"],
                "recent_games": _recent_games_payload(session, limit=8),
            })

    # ------------------------------------------------------------------
    # Games list
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/games")
    def mobile_games():
        year = request.args.get("year") or str(_current_year())
        phase = request.args.get("phase") or "2"
        team_id = request.args.get("team") or None
        page = max(1, int(request.args.get("page") or "1"))
        page_size = 30

        season_id = f"{phase}{year}" if phase != "all" else None
        with session_factory() as session:
            q = session.query(Game)
            if season_id:
                q = q.filter(Game.season == season_id)
            else:
                q = q.filter(Game.season.like(f"%{year}"))
            if team_id:
                q = q.filter(or_(Game.home_team_id == team_id, Game.road_team_id == team_id))
            q = q.order_by(desc(Game.game_date), desc(Game.game_id))
            total = q.count()
            games = q.offset((page - 1) * page_size).limit(page_size).all()
            team_ids = []
            for g in games:
                team_ids.extend([g.home_team_id, g.road_team_id])
            tl = _team_lookup(session, team_ids)
            return jsonify({
                "year": year,
                "phase": phase,
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": max(1, (total + page_size - 1) // page_size),
                "available_years": _available_years(session),
                "available_phases": [
                    {"code": "2", "label": _season_label("22025").split(" ", 1)[-1] if False else ("常规赛" if _is_zh() else "Regular Season")},
                    {"code": "4", "label": "季后赛" if _is_zh() else "Playoffs"},
                    {"code": "5", "label": "附加赛" if _is_zh() else "Play-In"},
                    {"code": "0", "label": "季前赛" if _is_zh() else "Pre-Season"},
                ],
                "games": [_serialize_game_row(g, tl) for g in games],
            })

    # ------------------------------------------------------------------
    # Game detail
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/games/<game_id>")
    def mobile_game_detail(game_id: str):
        with session_factory() as session:
            game = session.query(Game).filter(or_(Game.game_id == game_id, Game.slug == game_id)).first()
            if game is None:
                return jsonify({"error": "game_not_found"}), 404
            tl = _team_lookup(session, [game.home_team_id, game.road_team_id])

            team_stats_rows = session.query(TeamGameStats).filter(TeamGameStats.game_id == game.game_id).all()
            team_stats = [_serialize_team_stats(t, tl) for t in team_stats_rows]

            player_rows = session.query(PlayerGameStats).filter(PlayerGameStats.game_id == game.game_id).all()
            pl = _player_lookup(session, [p.player_id for p in player_rows])
            players_by_team: dict[str, list[dict[str, Any]]] = {}
            for r in player_rows:
                entry = _serialize_player_box(r, pl.get(r.player_id))
                players_by_team.setdefault(r.team_id, []).append(entry)
            for tid in players_by_team:
                players_by_team[tid].sort(key=lambda p: (0 if p["is_starter"] else 1, -(p.get("minutes_total_sec") or 0)))

            pbp_rows = (
                session.query(GamePlayByPlay)
                .filter(GamePlayByPlay.game_id == game.game_id)
                .order_by(GamePlayByPlay.period, desc(GamePlayByPlay.pc_time), GamePlayByPlay.event_num)
                .all()
            )

            line_scores = session.query(GameLineScore).filter(GameLineScore.game_id == game.game_id).all()
            quarter_scores = _serialize_quarter_scores(line_scores, game)

            return jsonify({
                "game": _serialize_game_row(game, tl, full=True),
                "team_stats": team_stats,
                "players_by_team": players_by_team,
                "ordered_team_ids": [game.road_team_id, game.home_team_id],
                "pbp": [_serialize_pbp(e) for e in pbp_rows[:400]],
                "quarter_scores": quarter_scores,
                "metrics": _game_metrics_payload(session, game.game_id),
            })

    # ------------------------------------------------------------------
    # Teams
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/teams")
    def mobile_teams_list():
        with session_factory() as session:
            teams = (
                session.query(Team)
                .filter(Team.active == True, Team.canonical_team_id.is_(None))
                .order_by(Team.full_name)
                .all()
            )
            return jsonify({"teams": [_serialize_team(t) for t in teams]})

    @app.route("/api/v1/mobile/teams/<team_id>")
    def mobile_team_detail(team_id: str):
        season = request.args.get("season") or _current_regular_season()
        with session_factory() as session:
            team = session.query(Team).filter(or_(Team.team_id == team_id, Team.slug == team_id)).first()
            if team is None:
                return jsonify({"error": "team_not_found"}), 404
            games = (
                session.query(Game)
                .filter(
                    Game.season == season,
                    or_(Game.home_team_id == team.team_id, Game.road_team_id == team.team_id),
                )
                .order_by(Game.game_date)
                .all()
            )
            tl = _team_lookup(session, [g.home_team_id for g in games] + [g.road_team_id for g in games])
            game_rows = [_serialize_team_game_row(g, team.team_id, tl) for g in games]
            wins = sum(1 for row in game_rows if row["result"] == "W")
            losses = sum(1 for row in game_rows if row["result"] == "L")

            stint_rows = (
                session.query(TeamRosterStint, Player)
                .join(Player, TeamRosterStint.player_id == Player.player_id)
                .filter(TeamRosterStint.team_id == team.team_id, TeamRosterStint.left_at.is_(None))
                .order_by(Player.full_name)
                .all()
            )
            roster = [
                {
                    **_serialize_player(p),
                    "jersey": s.jersey,
                    "position": s.position,
                    "joined_at": s.joined_at.isoformat() if s.joined_at else None,
                }
                for s, p in stint_rows
            ]

            coaches = (
                session.query(TeamCoachStint)
                .filter(TeamCoachStint.team_id == team.team_id, TeamCoachStint.left_at.is_(None))
                .order_by(TeamCoachStint.is_assistant, TeamCoachStint.coach_name)
                .all()
            )

            team_stats_totals = (
                session.query(
                    func.sum(TeamGameStats.pts).label("pts"),
                    func.sum(TeamGameStats.fgm).label("fgm"),
                    func.sum(TeamGameStats.fga).label("fga"),
                    func.sum(TeamGameStats.fg3m).label("fg3m"),
                    func.sum(TeamGameStats.fg3a).label("fg3a"),
                    func.sum(TeamGameStats.ftm).label("ftm"),
                    func.sum(TeamGameStats.fta).label("fta"),
                    func.sum(TeamGameStats.reb).label("reb"),
                    func.sum(TeamGameStats.ast).label("ast"),
                    func.sum(TeamGameStats.stl).label("stl"),
                    func.sum(TeamGameStats.blk).label("blk"),
                )
                .join(Game, TeamGameStats.game_id == Game.game_id)
                .filter(Game.season == season, TeamGameStats.team_id == team.team_id)
                .first()
            )

            return jsonify({
                "team": _serialize_team(team),
                "selected_season": season,
                "season_label": _season_label(season),
                "season_options": _available_seasons(session, kind="2"),
                "record": {
                    "wins": wins,
                    "losses": losses,
                    "win_pct": round(wins / (wins + losses), 3) if (wins + losses) else 0.0,
                },
                "totals": _serialize_team_totals(team_stats_totals, wins + losses),
                "games": game_rows,
                "roster": roster,
                "coaches": [
                    {"coach_name": c.coach_name, "coach_type": c.coach_type, "is_assistant": bool(c.is_assistant)}
                    for c in coaches
                ],
            })

    # ------------------------------------------------------------------
    # Players
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/players")
    def mobile_players_browse():
        season = request.args.get("season") or _current_regular_season()
        team_id = request.args.get("team")
        with session_factory() as session:
            base = session.query(PlayerGameStats).join(Game, PlayerGameStats.game_id == Game.game_id).filter(Game.season == season)
            if team_id:
                base = base.filter(PlayerGameStats.team_id == team_id)
            rows = base.all()

            per_player: dict[str, list[PlayerGameStats]] = {}
            for r in rows:
                per_player.setdefault(r.player_id, []).append(r)
            players = _player_lookup(session, list(per_player.keys()))
            teams = _team_lookup(session, [r.team_id for r in rows])

            out = []
            for pid, stats in per_player.items():
                p = players.get(pid)
                if p is None:
                    continue
                primary_team_id = max(
                    set(s.team_id for s in stats),
                    key=lambda tid: sum(1 for s in stats if s.team_id == tid),
                )
                summary = _aggregate_player_stats(stats)
                out.append({
                    **_serialize_player(p),
                    "team": _serialize_team(teams.get(primary_team_id)),
                    "summary": summary,
                })
            out.sort(key=lambda x: -(x["summary"]["pts"] or 0))
            return jsonify({
                "season": season,
                "season_label": _season_label(season),
                "season_options": _available_seasons(session, kind="2"),
                "players": out,
            })

    @app.route("/api/v1/mobile/players/hints")
    def mobile_player_hints():
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"players": []})
        with session_factory() as session:
            rows = (
                session.query(Player)
                .filter(or_(
                    Player.full_name.ilike(f"%{q}%"),
                    Player.full_name_zh.ilike(f"%{q}%"),
                ))
                .limit(12)
                .all()
            )
            return jsonify({
                "players": [
                    {
                        "player_id": p.player_id,
                        "slug": p.slug or p.player_id,
                        "display_name": _player_name(p),
                        "full_name": p.full_name,
                        "full_name_zh": p.full_name_zh,
                        "is_active": bool(p.is_active),
                    }
                    for p in rows
                ],
            })

    @app.route("/api/v1/mobile/players/<player_id>")
    def mobile_player_detail(player_id: str):
        season = request.args.get("season") or _current_regular_season()
        heatmap_season = request.args.get("heatmap_season") or season
        with session_factory() as session:
            player = session.query(Player).filter(or_(Player.player_id == player_id, Player.slug == player_id)).first()
            if player is None:
                return jsonify({"error": "player_not_found"}), 404

            all_games = (
                session.query(PlayerGameStats, Game)
                .join(Game, PlayerGameStats.game_id == Game.game_id)
                .filter(PlayerGameStats.player_id == player.player_id)
                .order_by(desc(Game.game_date))
                .all()
            )
            career_overall = _aggregate_player_stats([pgs for pgs, _ in all_games])

            by_season: dict[str, list[PlayerGameStats]] = {}
            for pgs, g in all_games:
                by_season.setdefault(g.season or "unknown", []).append(pgs)
            season_rows = [
                {
                    "season": s,
                    "season_label": _season_label(s),
                    **_aggregate_player_stats(stats),
                }
                for s, stats in sorted(by_season.items(), key=lambda kv: kv[0], reverse=True)
            ]

            current_season_games = [(pgs, g) for pgs, g in all_games if g.season == season]
            team_ids = set()
            for pgs, g in current_season_games:
                team_ids.update([pgs.team_id, g.home_team_id, g.road_team_id])
            tl = _team_lookup(session, list(team_ids))
            game_log = []
            for pgs, g in current_season_games[:80]:
                opp_id = g.road_team_id if pgs.team_id == g.home_team_id else g.home_team_id
                my_score = g.home_team_score if pgs.team_id == g.home_team_id else g.road_team_score
                opp_score = g.road_team_score if pgs.team_id == g.home_team_id else g.home_team_score
                result = "-"
                if my_score is not None and opp_score is not None:
                    result = "W" if my_score > opp_score else "L"
                game_log.append({
                    "game_id": g.game_id,
                    "slug": g.slug or g.game_id,
                    "game_date": _format_game_date(g.game_date),
                    "team": _serialize_team(tl.get(pgs.team_id)),
                    "opponent": _serialize_team(tl.get(opp_id)),
                    "is_home": pgs.team_id == g.home_team_id,
                    "result": result,
                    "my_score": my_score,
                    "opp_score": opp_score,
                    "minutes": pgs.min or 0,
                    "pts": pgs.pts or 0,
                    "reb": pgs.reb or 0,
                    "ast": pgs.ast or 0,
                    "stl": pgs.stl or 0,
                    "blk": pgs.blk or 0,
                    "fgm": pgs.fgm or 0,
                    "fga": pgs.fga or 0,
                    "fg3m": pgs.fg3m or 0,
                    "fg3a": pgs.fg3a or 0,
                    "ftm": pgs.ftm or 0,
                    "fta": pgs.fta or 0,
                    "plus": pgs.plus or 0,
                    "is_starter": bool(pgs.starter),
                })

            awards = (
                session.query(Award)
                .filter(Award.player_id == player.player_id)
                .order_by(Award.season.desc())
                .all()
            )
            award_summary: dict[str, dict[str, Any]] = {}
            for a in awards:
                entry = award_summary.setdefault(a.award_type, {"award_type": a.award_type, "count": 0, "seasons": []})
                entry["count"] += 1
                entry["seasons"].append(a.season)

            shots_q = session.query(ShotRecord).filter(ShotRecord.player_id == player.player_id)
            if heatmap_season and heatmap_season != "overall":
                shots_q = shots_q.filter(ShotRecord.season == heatmap_season)
            shots = shots_q.all()
            zones: dict[str, dict[str, int]] = {}
            dots = []
            for s in shots:
                key = s.shot_zone_basic or "Unknown"
                z = zones.setdefault(key, {"made": 0, "attempts": 0})
                z["attempts"] += 1
                if s.shot_made:
                    z["made"] += 1
                dots.append({"x": s.loc_x, "y": s.loc_y, "made": bool(s.shot_made)})
            heatmap_zones = {k: {**v, "pct": round(v["made"] / v["attempts"], 3) if v["attempts"] else None} for k, v in zones.items()}

            return jsonify({
                "player": _serialize_player(player),
                "selected_season": season,
                "season_label": _season_label(season),
                "season_options": sorted(by_season.keys(), reverse=True),
                "career_overall": career_overall,
                "career_season_rows": season_rows,
                "game_log": game_log,
                "awards": list(award_summary.values()),
                "heatmap": {
                    "selected_season": heatmap_season,
                    "zones": heatmap_zones,
                    "dots": dots[:2000],
                },
            })

    @app.route("/api/v1/mobile/players/compare")
    def mobile_player_compare():
        ids_raw = request.args.get("ids") or ""
        ids = [s.strip() for s in ids_raw.split(",") if s.strip()][:4]
        if len(ids) < 2:
            return jsonify({"error": "need_at_least_2_ids", "players": []}), 400
        with session_factory() as session:
            players = _player_lookup(session, ids)
            result = []
            for pid in ids:
                p = players.get(pid)
                if p is None:
                    continue
                rows = (
                    session.query(PlayerGameStats)
                    .filter(PlayerGameStats.player_id == pid)
                    .all()
                )
                result.append({
                    **_serialize_player(p),
                    "career": _aggregate_player_stats(rows),
                })
            return jsonify({"players": result})

    # ------------------------------------------------------------------
    # News
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/news")
    def mobile_news():
        with session_factory() as session:
            clusters = (
                session.query(NewsCluster)
                .order_by(desc(NewsCluster.score), desc(NewsCluster.last_seen_at))
                .limit(60)
                .all()
            )
            rep_ids = [c.representative_article_id for c in clusters if c.representative_article_id]
            articles = {a.id: a for a in session.query(NewsArticle).filter(NewsArticle.id.in_(rep_ids)).all()}
            return jsonify({
                "news": [_serialize_news_cluster(c, articles.get(c.representative_article_id)) for c in clusters],
            })

    @app.route("/api/v1/mobile/news/<int:cluster_id>")
    def mobile_news_detail(cluster_id: int):
        with session_factory() as session:
            cluster = session.query(NewsCluster).filter(NewsCluster.id == cluster_id).first()
            if cluster is None:
                return jsonify({"error": "cluster_not_found"}), 404
            articles = (
                session.query(NewsArticle)
                .filter(NewsArticle.cluster_id == cluster_id)
                .order_by(desc(NewsArticle.published_at))
                .all()
            )
            main = next((a for a in articles if a.id == cluster.representative_article_id), articles[0] if articles else None)
            siblings = [a for a in articles if a.id != (main.id if main else None)]

            player_tags = (
                session.query(NewsArticlePlayer, Player)
                .join(Player, NewsArticlePlayer.player_id == Player.player_id)
                .filter(NewsArticlePlayer.article_id == (main.id if main else -1))
                .all()
            )
            team_tags = (
                session.query(NewsArticleTeam, Team)
                .join(Team, NewsArticleTeam.team_id == Team.team_id)
                .filter(NewsArticleTeam.article_id == (main.id if main else -1))
                .all()
            )

            return jsonify({
                "cluster": {"id": cluster.id, "article_count": cluster.article_count, "unique_view_count": cluster.unique_view_count},
                "article": _serialize_news_article(main) if main else None,
                "siblings": [_serialize_news_article(a) for a in siblings],
                "players": [_serialize_player(p) for _, p in player_tags],
                "teams": [_serialize_team(t) for _, t in team_tags],
            })

    # ------------------------------------------------------------------
    # Draft
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/draft/<int:year>")
    def mobile_draft(year: int):
        with session_factory() as session:
            rows = (
                session.query(Player)
                .filter(Player.draft_year == year, Player.draft_number.isnot(None))
                .order_by(Player.draft_round, Player.draft_number)
                .all()
            )
            year_min = session.query(func.min(Player.draft_year)).filter(Player.draft_year > 1900).scalar() or 1947
            year_max = session.query(func.max(Player.draft_year)).scalar() or datetime.now().year
            return jsonify({
                "year": year,
                "min_year": int(year_min),
                "max_year": int(year_max),
                "players": [_serialize_player(p) for p in rows],
            })

    # ------------------------------------------------------------------
    # Awards
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/awards")
    def mobile_awards():
        award_type = request.args.get("type") or "mvp"
        with session_factory() as session:
            rows = (
                session.query(Award)
                .filter(Award.award_type == award_type)
                .order_by(desc(Award.season))
                .all()
            )
            player_ids = [a.player_id for a in rows if a.player_id]
            team_ids = [a.team_id for a in rows if a.team_id]
            pl = _player_lookup(session, player_ids)
            tl = _team_lookup(session, team_ids)
            types = [
                ("champion", "总冠军", "Champion"),
                ("mvp", "常规赛MVP", "MVP"),
                ("finals_mvp", "总决赛MVP", "Finals MVP"),
                ("dpoy", "最佳防守球员", "Defensive POY"),
                ("roy", "最佳新秀", "Rookie of the Year"),
                ("smoy", "最佳第六人", "Sixth Man"),
                ("mip", "进步最快球员", "Most Improved"),
                ("coy", "最佳教练", "Coach of the Year"),
            ]
            return jsonify({
                "award_type": award_type,
                "available_types": [
                    {"code": t[0], "label": t[1] if _is_zh() else t[2]}
                    for t in types
                ],
                "results": [
                    {
                        "id": a.id,
                        "award_type": a.award_type,
                        "season": a.season,
                        "season_label": _season_label(f"2{a.season}") if a.season else None,
                        "player": _serialize_player(pl.get(a.player_id)) if a.player_id else None,
                        "team": _serialize_team(tl.get(a.team_id)) if a.team_id else None,
                        "notes": a.notes,
                    }
                    for a in rows
                ],
            })

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/metrics")
    def mobile_metrics_browse():
        scope = request.args.get("scope")
        q = (request.args.get("q") or "").strip()
        with session_factory() as session:
            query = session.query(MetricDefinition).filter(MetricDefinition.status == "published")
            if scope:
                query = query.filter(MetricDefinition.scope == scope)
            if q:
                query = query.filter(or_(
                    MetricDefinition.name.ilike(f"%{q}%"),
                    MetricDefinition.name_zh.ilike(f"%{q}%"),
                    MetricDefinition.description.ilike(f"%{q}%"),
                ))
            rows = query.order_by(MetricDefinition.name).limit(200).all()
            return jsonify({
                "metrics": [_serialize_metric_def(m) for m in rows],
            })

    @app.route("/api/v1/mobile/metrics/<metric_key>")
    def mobile_metric_detail(metric_key: str):
        season = request.args.get("season")
        page = max(1, int(request.args.get("page") or "1"))
        page_size = 30
        with session_factory() as session:
            m = session.query(MetricDefinition).filter(MetricDefinition.key == metric_key).first()
            if m is None:
                return jsonify({"error": "metric_not_found"}), 404
            season_query = (
                session.query(MetricResult.season, func.count(MetricResult.id))
                .filter(MetricResult.metric_key == metric_key)
                .group_by(MetricResult.season)
                .all()
            )
            season_options = sorted([s for s, _ in season_query if s], reverse=True)
            if season is None and season_options:
                season = season_options[0]

            q = session.query(MetricResult).filter(MetricResult.metric_key == metric_key)
            if season:
                q = q.filter(MetricResult.season == season)
            q = q.filter(MetricResult.value_num.isnot(None))
            q = q.order_by(desc(MetricResult.value_num))
            total = q.count()
            results = q.offset((page - 1) * page_size).limit(page_size).all()

            player_ids = [r.entity_id for r in results if r.entity_type == "player"]
            team_ids = [r.entity_id for r in results if r.entity_type == "team"]
            pl = _player_lookup(session, player_ids)
            tl = _team_lookup(session, team_ids)

            rows = []
            for idx, r in enumerate(results):
                entity = None
                entity_label = r.entity_id or ""
                if r.entity_type == "player":
                    p = pl.get(r.entity_id)
                    entity = _serialize_player(p)
                    entity_label = _player_name(p) if p else entity_label
                elif r.entity_type == "team":
                    t = tl.get(r.entity_id)
                    entity = _serialize_team(t)
                    entity_label = _team_name(t) if t else entity_label

                rows.append({
                    "rank": (page - 1) * page_size + idx + 1,
                    "entity_type": r.entity_type,
                    "entity_id": r.entity_id,
                    "entity_label": entity_label,
                    "entity": entity,
                    "season": r.season,
                    "season_label": _season_label(r.season) if r.season else None,
                    "value_num": r.value_num,
                    "value_str": r.value_str,
                    "is_notable": (r.noteworthiness or 0) >= 0.75,
                    "notable_reason": r.notable_reason,
                    "context": _parse_json(r.context_json),
                })

            return jsonify({
                "metric": _serialize_metric_def(m),
                "selected_season": season,
                "season_label": _season_label(season) if season else None,
                "season_options": season_options,
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": max(1, (total + page_size - 1) // page_size),
                "results": rows,
            })

    @app.route("/api/v1/mobile/metrics/mine")
    def mobile_my_metrics():
        with session_factory() as session:
            user = _current_user(session)
            if user is None:
                return jsonify({"error": "auth_required"}), 401
            rows = (
                session.query(MetricDefinition)
                .filter(MetricDefinition.created_by_user_id == user.id)
                .order_by(desc(MetricDefinition.updated_at))
                .all()
            )
            drafts = [_serialize_metric_def(m) for m in rows if m.status in ("draft", "disabled")]
            published = [_serialize_metric_def(m) for m in rows if m.status == "published"]
            return jsonify({"drafts": drafts, "published": published})

    # ------------------------------------------------------------------
    # Auth: magic link
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/auth/magic/request", methods=["POST"])
    def mobile_auth_magic_request():
        data = request.get_json(silent=True) or {}
        email = (data.get("email") or "").strip().lower()
        if not email or "@" not in email:
            return jsonify({"error": "invalid_email"}), 400
        token = secrets.token_urlsafe(32)
        expires = datetime.utcnow() + timedelta(minutes=_MAGIC_TOKEN_TTL_MINUTES)
        with session_factory() as session:
            session.add(MagicToken(token=token, email=email, expires_at=expires, used=False, created_at=datetime.utcnow()))
            session.commit()
        deep_link = f"funba://auth?token={token}"
        try:
            if send_magic_link is not None:
                send_magic_link(email, token, deep_link)
            else:
                logger.warning("mobile magic link for %s: %s (no send_magic_link hook)", email, deep_link)
        except Exception:
            logger.exception("mobile magic link send failed")
        return jsonify({"ok": True})

    @app.route("/api/v1/mobile/auth/magic/verify", methods=["POST"])
    def mobile_auth_magic_verify():
        data = request.get_json(silent=True) or {}
        token = (data.get("token") or request.args.get("token") or "").strip()
        if not token:
            return jsonify({"error": "invalid_token"}), 400
        with session_factory() as session:
            row = session.query(MagicToken).filter(MagicToken.token == token).first()
            if row is None or row.used or row.expires_at < datetime.utcnow():
                return jsonify({"error": "invalid_token"}), 400
            row.used = True
            user = session.query(User).filter(User.email == row.email).first()
            now = datetime.utcnow()
            if user is None:
                user = User(
                    id=str(uuid.uuid4()),
                    email=row.email,
                    display_name=row.email.split("@", 1)[0],
                    is_admin=False,
                    subscription_tier="free",
                    created_at=now,
                    last_login_at=now,
                )
                session.add(user)
            else:
                user.last_login_at = now
            session.commit()
            bearer = _issue_token(app, user.id)
            return jsonify({
                "token": bearer,
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "display_name": user.display_name,
                    "avatar_url": user.avatar_url,
                    "subscription_tier": user.subscription_tier,
                },
            })

    # ------------------------------------------------------------------
    # Feedback
    # ------------------------------------------------------------------

    @app.route("/api/v1/mobile/feedback", methods=["POST"])
    def mobile_feedback():
        with session_factory() as session:
            user = _current_user(session)
            if user is None:
                return jsonify({"error": "auth_required"}), 401
            data = request.get_json(silent=True) or {}
            content = (data.get("content") or "").strip()
            if not content:
                return jsonify({"error": "empty_content"}), 400
            session.add(Feedback(user_id=user.id, content=content[:5000], page_url=data.get("page_url"), created_at=datetime.utcnow()))
            session.commit()
            return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Serializer helpers
# ---------------------------------------------------------------------------


def _serialize_game_row(game: Game, teams: dict[str, Team], *, full: bool = False) -> dict[str, Any]:
    home = teams.get(game.home_team_id)
    road = teams.get(game.road_team_id)
    status = _game_status(game)
    payload = {
        "game_id": game.game_id,
        "slug": game.slug or game.game_id,
        "season": game.season,
        "season_label": _season_label(game.season),
        "game_date": _format_game_date(game.game_date),
        "status": status,
        "home_team": _serialize_team(home),
        "road_team": _serialize_team(road),
        "home_team_score": game.home_team_score,
        "road_team_score": game.road_team_score,
        "wining_team_id": game.wining_team_id,
        "summary_text": "Final" if status == "completed" else ("" if status == "upcoming" else (game.game_status or "Live")),
    }
    if full:
        payload["tipoff_time"] = game.tipoff_time
        payload["attendance"] = game.attendance
        payload["national_tv_broadcaster"] = game.national_tv_broadcaster
    return payload


def _serialize_team_stats(t: TeamGameStats, teams: dict[str, Team]) -> dict[str, Any]:
    return {
        "team": _serialize_team(teams.get(t.team_id)),
        "team_id": t.team_id,
        "pts": t.pts,
        "fgm": t.fgm, "fga": t.fga, "fg_pct": t.fg_pct,
        "fg3m": t.fg3m, "fg3a": t.fg3a, "fg3_pct": t.fg3_pct,
        "ftm": t.ftm, "fta": t.fta, "ft_pct": t.ft_pct,
        "reb": t.reb, "oreb": t.oreb, "dreb": t.dreb,
        "ast": t.ast, "stl": t.stl, "blk": t.blk, "tov": t.tov, "pf": t.pf,
        "win": bool(t.win),
        "on_road": bool(t.on_road),
    }


def _serialize_player_box(r: PlayerGameStats, player: Player | None) -> dict[str, Any]:
    minutes_total = (r.min or 0) * 60 + (r.sec or 0)
    is_dnp = not (r.min or r.sec or r.pts or r.reb or r.ast)
    return {
        "player_id": r.player_id,
        "team_id": r.team_id,
        "player": _serialize_player(player),
        "is_starter": bool(r.starter),
        "is_dnp": is_dnp,
        "status": "DNP" if is_dnp else "Active",
        "minutes_display": f"{r.min or 0}:{(r.sec or 0):02d}",
        "minutes_total_sec": minutes_total,
        "pts": r.pts or 0,
        "reb": r.reb or 0,
        "ast": r.ast or 0,
        "stl": r.stl or 0,
        "blk": r.blk or 0,
        "tov": r.tov or 0,
        "fgm": r.fgm or 0, "fga": r.fga or 0,
        "fg3m": r.fg3m or 0, "fg3a": r.fg3a or 0,
        "ftm": r.ftm or 0, "fta": r.fta or 0,
        "oreb": r.oreb or 0, "dreb": r.dreb or 0, "pf": r.pf or 0,
        "plus": r.plus or 0,
    }


def _serialize_pbp(e: GamePlayByPlay) -> dict[str, Any]:
    description = e.home_description or e.visitor_description or e.neutral_description or ""
    return {
        "period": e.period,
        "clock": e.pc_time or "",
        "description": description,
        "score": e.score,
        "score_margin": e.score_margin,
        "event_msg_type": e.event_msg_type,
        "player1_id": e.player1_id,
    }


def _serialize_quarter_scores(line_scores: list[GameLineScore], game: Game) -> list[dict[str, Any]]:
    mapping: dict[str, GameLineScore] = {ls.team_id: ls for ls in line_scores}
    home = mapping.get(game.home_team_id)
    road = mapping.get(game.road_team_id)
    if home is None or road is None:
        return []
    periods = []
    for idx, (attr, label) in enumerate([("q1_pts", "Q1"), ("q2_pts", "Q2"), ("q3_pts", "Q3"), ("q4_pts", "Q4"), ("ot1_pts", "OT"), ("ot2_pts", "OT2"), ("ot3_pts", "OT3")]):
        h = getattr(home, attr)
        r = getattr(road, attr)
        if h is None and r is None:
            continue
        periods.append({"period": idx + 1, "label": label, "home": h or 0, "road": r or 0})
    return periods


def _serialize_news_cluster(cluster: NewsCluster, rep: NewsArticle | None) -> dict[str, Any]:
    return {
        "cluster_id": cluster.id,
        "article_count": cluster.article_count,
        "unique_view_count": cluster.unique_view_count,
        "score": cluster.score,
        "first_seen_at": cluster.first_seen_at.isoformat() if cluster.first_seen_at else None,
        "last_seen_at": cluster.last_seen_at.isoformat() if cluster.last_seen_at else None,
        "article": _serialize_news_article(rep) if rep else None,
    }


def _serialize_news_article(a: NewsArticle | None) -> dict[str, Any] | None:
    if a is None:
        return None
    return {
        "id": a.id,
        "source": a.source,
        "title": a.title,
        "summary": a.summary,
        "url": a.url,
        "thumbnail_url": a.thumbnail_url,
        "published_at": a.published_at.isoformat() if a.published_at else None,
    }


def _serialize_metric_def(m: MetricDefinition) -> dict[str, Any]:
    return {
        "key": m.key,
        "family_key": m.family_key,
        "variant": m.variant,
        "name": m.name_zh if _is_zh() and m.name_zh else m.name,
        "name_en": m.name,
        "name_zh": m.name_zh,
        "description": m.description_zh if _is_zh() and m.description_zh else m.description,
        "scope": m.scope,
        "category": m.category,
        "status": m.status,
    }


def _serialize_team_game_row(game: Game, team_id: str, teams: dict[str, Team]) -> dict[str, Any]:
    is_home = team_id == game.home_team_id
    opp_id = game.road_team_id if is_home else game.home_team_id
    my_score = game.home_team_score if is_home else game.road_team_score
    opp_score = game.road_team_score if is_home else game.home_team_score
    result = "-"
    if my_score is not None and opp_score is not None:
        result = "W" if my_score > opp_score else "L"
    return {
        "game_id": game.game_id,
        "slug": game.slug or game.game_id,
        "game_date": _format_game_date(game.game_date),
        "opponent": _serialize_team(teams.get(opp_id)),
        "is_home": is_home,
        "my_score": my_score,
        "opp_score": opp_score,
        "result": result,
        "status": _game_status(game),
    }


def _serialize_team_totals(row, games: int) -> dict[str, Any]:
    if row is None or games <= 0:
        return {}
    pts = row.pts or 0
    return {
        "games": games,
        "ppg": round(pts / games, 1) if games else 0.0,
        "fg_pct": round((row.fgm or 0) / (row.fga or 1), 3) if row.fga else None,
        "fg3_pct": round((row.fg3m or 0) / (row.fg3a or 1), 3) if row.fg3a else None,
        "ft_pct": round((row.ftm or 0) / (row.fta or 1), 3) if row.fta else None,
        "reb_pg": round((row.reb or 0) / games, 1) if games else 0.0,
        "ast_pg": round((row.ast or 0) / games, 1) if games else 0.0,
        "stl_pg": round((row.stl or 0) / games, 1) if games else 0.0,
        "blk_pg": round((row.blk or 0) / games, 1) if games else 0.0,
    }


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _current_year() -> int:
    now = datetime.now()
    return now.year if now.month >= 10 else now.year - 1


def _current_regular_season() -> str:
    return f"2{_current_year()}"


def _available_years(session: Session) -> list[str]:
    rows = session.query(Game.season).distinct().all()
    years = sorted({s[0][-4:] for s in rows if s[0] and len(s[0]) >= 5}, reverse=True)
    return years[:25]


def _available_seasons(session: Session, kind: str = "2") -> list[dict[str, str]]:
    rows = session.query(Game.season).distinct().all()
    seasons = sorted({s[0] for s in rows if s[0] and s[0].startswith(kind)}, reverse=True)
    return [{"season": s, "label": _season_label(s)} for s in seasons[:25]]


def _parse_json(raw: str | None) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def _compute_standings(session: Session, season: str) -> dict[str, list[dict[str, Any]]]:
    """Return east/west standings for a regular-season id."""
    rows = (
        session.query(
            TeamGameStats.team_id,
            func.sum(case((TeamGameStats.win == True, 1), else_=0)).label("wins"),
            func.sum(case((TeamGameStats.win == False, 1), else_=0)).label("losses"),
        )
        .join(Game, TeamGameStats.game_id == Game.game_id)
        .filter(Game.season == season)
        .group_by(TeamGameStats.team_id)
        .all()
    )
    team_ids = [r[0] for r in rows]
    tl = _team_lookup(session, team_ids)
    records = []
    for tid, wins, losses in rows:
        wins = int(wins or 0)
        losses = int(losses or 0)
        team = tl.get(tid)
        if team is None:
            continue
        total = wins + losses
        records.append({
            "team_id": tid,
            "team": _serialize_team(team),
            "wins": wins,
            "losses": losses,
            "win_pct": round(wins / total, 3) if total else 0.0,
        })
    records.sort(key=lambda x: -x["win_pct"])
    # Simple east/west split via city heuristic is complex; divide by half for now.
    split = len(records) // 2 or 1
    return {"east": records[:split], "west": records[split:]}


def _recent_games_payload(session: Session, limit: int = 8) -> list[dict[str, Any]]:
    games = (
        session.query(Game)
        .filter(Game.home_team_score.isnot(None))
        .order_by(desc(Game.game_date), desc(Game.game_id))
        .limit(limit)
        .all()
    )
    tl = _team_lookup(session, [g.home_team_id for g in games] + [g.road_team_id for g in games])
    return [_serialize_game_row(g, tl) for g in games]


def _game_metrics_payload(session: Session, game_id: str) -> list[dict[str, Any]]:
    rows = (
        session.query(MetricResult, MetricDefinition)
        .join(MetricDefinition, MetricResult.metric_key == MetricDefinition.key)
        .filter(MetricResult.game_id == game_id)
        .filter(MetricDefinition.status == "published")
        .order_by(desc(MetricResult.noteworthiness))
        .limit(30)
        .all()
    )
    out = []
    for r, m in rows:
        out.append({
            "metric_key": m.key,
            "metric_name": m.name_zh if _is_zh() and m.name_zh else m.name,
            "scope": m.scope,
            "entity_type": r.entity_type,
            "entity_id": r.entity_id,
            "value_num": r.value_num,
            "value_str": r.value_str,
            "is_notable": (r.noteworthiness or 0) >= 0.75,
            "notable_reason": r.notable_reason,
        })
    return out


