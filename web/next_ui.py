"""Funba "Next" UI — an opt-in redesign that lives alongside the classic templates.

How it works
------------
* Every page render goes through ``render`` (installed in ``web/app.py``). When
  the request opted into the new UI and ``templates/next/<name>`` exists, the
  next template is rendered with the *same* context the classic view built —
  optionally enriched with extra data (see ``_ENRICHERS``).
* Opt in/out: ``?ui=next`` / ``?ui=classic`` on any URL (sticky via cookie), or
  ``/ui/next`` / ``/ui/classic``. ``FUNBA_UI_DEFAULT=next`` flips the default.
* New JSON endpoints used by the redesign live under ``/api/next/``.
"""
from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from urllib.parse import parse_qsl, quote, urlencode, urlparse

from flask import g, jsonify, make_response, redirect, request
from sqlalchemy import and_, case, func, or_

NEXT_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates" / "next"
UI_COOKIE = "funba_ui"
UI_VARIANTS = ("next", "classic")

# ── Team colors ──────────────────────────────────────────────────────────────
# (primary, secondary). Primary is the brand color used for hero fields and
# charts; secondary is used when two teams in a matchup collide visually.
TEAM_COLORS: dict[str, tuple[str, str]] = {
    "1610612737": ("#E03A3E", "#C1D32F"),  # ATL
    "1610612738": ("#007A33", "#BA9653"),  # BOS
    "1610612751": ("#1A1A1A", "#8F8F8F"),  # BKN
    "1610612766": ("#1D1160", "#00788C"),  # CHA
    "1610612741": ("#CE1141", "#1A1A1A"),  # CHI
    "1610612739": ("#860038", "#FDBB30"),  # CLE
    "1610612742": ("#00538C", "#B8C4CA"),  # DAL
    "1610612743": ("#0E2240", "#FEC524"),  # DEN
    "1610612765": ("#C8102E", "#1D42BA"),  # DET
    "1610612744": ("#1D428A", "#FFC72C"),  # GSW
    "1610612745": ("#CE1141", "#1A1A1A"),  # HOU
    "1610612754": ("#002D62", "#FDBB30"),  # IND
    "1610612746": ("#C8102E", "#1D428A"),  # LAC
    "1610612747": ("#552583", "#FDB927"),  # LAL
    "1610612763": ("#5D76A9", "#12173F"),  # MEM
    "1610612748": ("#98002E", "#F9A01B"),  # MIA
    "1610612749": ("#00471B", "#D6C9A8"),  # MIL
    "1610612750": ("#0C2340", "#78BE20"),  # MIN
    "1610612740": ("#0C2340", "#B4975A"),  # NOP
    "1610612752": ("#006BB6", "#F58426"),  # NYK
    "1610612760": ("#007AC1", "#EF3B24"),  # OKC
    "1610612753": ("#0077C0", "#9AA6AD"),  # ORL
    "1610612755": ("#006BB6", "#ED174C"),  # PHI
    "1610612756": ("#1D1160", "#E56020"),  # PHX
    "1610612757": ("#E03A3E", "#1A1A1A"),  # POR
    "1610612758": ("#5A2D81", "#63727A"),  # SAC
    "1610612759": ("#1A1A1A", "#A7B1B8"),  # SAS
    "1610612761": ("#CE1141", "#1A1A1A"),  # TOR
    "1610612762": ("#4E2A84", "#1A1A1A"),  # UTA
    "1610612764": ("#002B5C", "#E31837"),  # WAS
}
NEUTRAL_COLOR = "#6B6862"


def _hex_to_rgb(value: str) -> tuple[int, int, int]:
    value = value.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def _color_distance(a: str, b: str) -> float:
    """Perceptual-ish RGB distance ("redmean")."""
    r1, g1, b1 = _hex_to_rgb(a)
    r2, g2, b2 = _hex_to_rgb(b)
    rmean = (r1 + r2) / 2
    dr, dg, db = r1 - r2, g1 - g2, b1 - b2
    return math.sqrt((2 + rmean / 256) * dr * dr + 4 * dg * dg + (2 + (255 - rmean) / 256) * db * db)


def team_color(team_id: str | None, which: int = 0) -> str:
    pair = TEAM_COLORS.get(str(team_id or ""))
    if not pair:
        return NEUTRAL_COLOR
    return pair[1 if which else 0]


def matchup_colors(road_id: str | None, home_id: str | None) -> tuple[str, str]:
    """Return (road, home) colors that are visually distinguishable."""
    home = team_color(home_id)
    road = team_color(road_id)
    if _color_distance(road, home) < 170:
        alt = team_color(road_id, 1)
        if _color_distance(alt, home) >= 170:
            road = alt
        else:
            alt_home = team_color(home_id, 1)
            if _color_distance(road, alt_home) >= 170:
                home = alt_home
            else:
                road = NEUTRAL_COLOR
    return road, home


def ink_on(hex_color: str | None) -> str:
    """Pick a legible text color (near-black or white) for a solid background."""
    try:
        r, g_, b = _hex_to_rgb(hex_color or NEUTRAL_COLOR)
    except ValueError:
        return "#fff"
    def lin(c: int) -> float:
        c = c / 255
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    lum = 0.2126 * lin(r) + 0.7152 * lin(g_) + 0.0722 * lin(b)
    return "#141414" if lum > 0.42 else "#ffffff"


def headshot_url(player_id: str | None, size: str = "260x190") -> str:
    if not player_id:
        return ""
    return f"https://cdn.nba.com/headshots/nba/latest/{size}/{player_id}.png"


# ── Small in-process TTL cache ───────────────────────────────────────────────
_cache: dict[str, tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _cached(key: str, ttl: float, builder: Callable[[], Any]) -> Any:
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    value = builder()
    with _cache_lock:
        _cache[key] = (now, value)
    return value


_NO_FALLBACK = object()
_refreshing: set[str] = set()


def _refresh_async(key: str, builder: Callable[[], Any], wrap: Callable[[Callable[[], Any]], Any] | None = None) -> None:
    with _cache_lock:
        if key in _refreshing:
            return
        _refreshing.add(key)

    def run():
        try:
            value = wrap(builder) if wrap else builder()
            with _cache_lock:
                _cache[key] = (time.monotonic(), value)
        except Exception:
            pass
        finally:
            with _cache_lock:
                _refreshing.discard(key)

    threading.Thread(target=run, name=f"next-ui-refresh:{key}", daemon=True).start()


def _cached_swr(
    key: str,
    ttl: float,
    builder: Callable[[], Any],
    *,
    fallback: Any = _NO_FALLBACK,
    wrap: Callable[[Callable[[], Any]], Any] | None = None,
) -> Any:
    """Stale-while-revalidate: never block a request on a rebuild once a value exists.

    With ``fallback`` given, even the very first call returns immediately and the
    value is computed in the background.
    """
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    if hit:
        _refresh_async(key, builder, wrap)
        return hit[1]
    if fallback is not _NO_FALLBACK:
        _refresh_async(key, builder, wrap)
        return fallback
    value = builder()
    with _cache_lock:
        _cache[key] = (time.monotonic(), value)
    return value


# ── Variant resolution ───────────────────────────────────────────────────────

def _default_variant() -> str:
    value = (os.getenv("FUNBA_UI_DEFAULT") or "classic").strip().lower()
    return value if value in UI_VARIANTS else "classic"


def current_variant() -> str:
    return getattr(g, "ui_variant", None) or _default_variant()


def _resolve_variant() -> str:
    arg = (request.args.get("ui") or "").strip().lower()
    if arg in UI_VARIANTS:
        return arg
    cookie = (request.cookies.get(UI_COOKIE) or "").strip().lower()
    if cookie in UI_VARIANTS:
        return cookie
    return _default_variant()


def _has_next_template(name: str) -> bool:
    if not name or ".." in name or name.startswith("/"):
        return False
    return (NEXT_TEMPLATE_DIR / name).is_file()


# ── Formatting helpers exposed to templates ──────────────────────────────────

def _fmt_num(value: Any, digits: int = 1) -> str:
    if value is None or value == "":
        return "—"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if digits == 0:
        return f"{num:.0f}"
    return f"{num:.{digits}f}"


def _fmt_pct(value: Any, digits: int = 1) -> str:
    if value is None or value == "":
        return "—"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if num <= 1.0:
        num *= 100
    return f"{num:.{digits}f}"


def _fmt_money(value: Any) -> str:
    if value is None:
        return "—"
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if abs(num) >= 1_000_000:
        return f"${num / 1_000_000:.1f}M"
    if abs(num) >= 1_000:
        return f"${num / 1_000:.0f}K"
    return f"${num:.0f}"


def _ordinal(n: Any) -> str:
    try:
        n = int(n)
    except (TypeError, ValueError):
        return str(n)
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _age_from(birth: Any) -> int | None:
    if not birth:
        return None
    if isinstance(birth, datetime):
        birth = birth.date()
    if not isinstance(birth, date):
        try:
            birth = date.fromisoformat(str(birth)[:10])
        except ValueError:
            return None
    today = date.today()
    return today.year - birth.year - ((today.month, today.day) < (birth.month, birth.day))


# ── Score-flow analysis ──────────────────────────────────────────────────────

def analyze_flow(progression: list[dict]) -> dict:
    """Derive narrative stats from a [{t, road, home}] score progression."""
    lead_changes = 0
    times_tied = 0
    largest = {"home": 0, "road": 0}
    largest_at = {"home": None, "road": None}
    prev_leader = None
    best_run = {"team": None, "pts": 0, "start": None, "end": None, "opp": 0}
    run_team = None
    run_pts = 0
    run_start = None
    prev_h = prev_r = 0
    for point in progression[1:]:
        h, r = int(point.get("home") or 0), int(point.get("road") or 0)
        margin = h - r
        leader = "home" if margin > 0 else "road" if margin < 0 else None
        if leader is None and (prev_h != prev_r):
            times_tied += 1
        if leader and prev_leader and leader != prev_leader:
            lead_changes += 1
        if leader:
            prev_leader = leader
        if margin > largest["home"]:
            largest["home"] = margin
            largest_at["home"] = point.get("t")
        if -margin > largest["road"]:
            largest["road"] = -margin
            largest_at["road"] = point.get("t")
        dh, dr = h - prev_h, r - prev_r
        scorer = "home" if dh > 0 else "road" if dr > 0 else None
        if scorer:
            if scorer == run_team:
                run_pts += dh or dr
            else:
                run_team, run_pts, run_start = scorer, (dh or dr), point.get("t")
            if run_pts > best_run["pts"]:
                best_run = {"team": run_team, "pts": run_pts, "start": run_start, "end": point.get("t")}
        prev_h, prev_r = h, r
    return {
        "lead_changes": lead_changes,
        "times_tied": times_tied,
        "largest_lead": largest,
        "largest_lead_at": largest_at,
        "best_run": best_run,
    }


def _margin_series(pbp_rows: list, buckets: int = 48) -> list[int]:
    """Downsample PBP scores into a fixed-length home-minus-road margin series."""
    points: list[tuple[float, int]] = [(0.0, 0)]
    for row in pbp_rows:
        if not row.score or row.period is None:
            continue
        parts = str(row.score).split("-")
        if len(parts) != 2:
            continue
        try:
            home_score, road_score = int(parts[0].strip()), int(parts[1].strip())
        except ValueError:
            continue
        period = int(row.period)
        try:
            mins, secs = str(row.pc_time or "0:00").split(":")
            remaining = int(mins) * 60 + int(float(secs))
        except ValueError:
            remaining = 0
        if period <= 4:
            elapsed = (period - 1) * 720 + (720 - remaining)
        else:
            elapsed = 2880 + (period - 5) * 300 + (300 - remaining)
        points.append((elapsed / 60.0, home_score - road_score))
    if len(points) < 2:
        return []
    end_t = max(48.0, points[-1][0])
    series = []
    idx = 0
    current = 0
    for b in range(buckets + 1):
        t = end_t * b / buckets
        while idx < len(points) and points[idx][0] <= t:
            current = points[idx][1]
            idx += 1
        series.append(current)
    series[-1] = points[-1][1]
    return series



# ── Milestone sentences (home page "record book") ────────────────────────────
_TRIVIAL_MILESTONE_FAMILIES = ("games_played", "games_started", "total_games", "team_wins", "wins_count")


def _window_label(season: Any) -> tuple[str, str] | None:
    value = str(season or "")
    if value.startswith("all_"):
        return ("career", "生涯")
    for n in ("10", "5", "3"):
        if value.startswith(f"last{n}_"):
            return (f"last {n} seasons", f"近{n}季")
    return None


def _milestone_sentence(card: dict, team_lookup: Callable[[Any], dict]) -> tuple[str | None, str | None]:
    ctx = card.get("context_json") or {}
    metric_en = (card.get("metric_name") or ctx.get("stat_label_en") or "").strip()
    metric_zh = (card.get("metric_name_zh") or ctx.get("stat_label") or metric_en).strip()
    player = ctx.get("player") if isinstance(ctx.get("player"), dict) else None
    if player:
        actor_en = player.get("name_en") or player.get("player_id")
        actor_zh = player.get("name_zh") or actor_en
    else:
        info = team_lookup(card.get("entity_id"))
        actor_en = info.get("name") or info.get("abbr")
        actor_zh = info.get("name_zh") or actor_en
    if not actor_en or not metric_en:
        return None, None
    window = _window_label(card.get("season"))
    scope_en = f" ({window[0]})" if window else " this season"
    scope_zh = f"（{window[1]}）" if window else "本季"
    rank = card.get("rank")
    value = card.get("value_num")
    value_txt = (str(int(value)) if value is not None and float(value).is_integer() else (f"{value:.1f}" if value is not None else "?"))
    if card.get("event_type") == "rank_crossing":
        passed = (card.get("passed") or [None])[0] or {}
        passed_en = passed.get("name_en") or passed.get("abbr")
        passed_zh = passed.get("name_zh") or passed_en
        en = f"{actor_en} climbs to No. {rank} in {metric_en}{scope_en if window else ''}"
        zh = f"{actor_zh}{scope_zh if window else ''}{metric_zh}升至第{rank}"
        if passed_en:
            en += f", passing {passed_en}"
            zh += f"，超越{passed_zh}"
        return en, zh
    if card.get("event_type") == "absolute_threshold":
        before = ctx.get("count_reached_before_this_game")
        en = f"{actor_en} reaches {value_txt} {metric_en}{scope_en}"
        zh = f"{actor_zh}{scope_zh}{metric_zh}达到{value_txt}"
        if before == 0 and not window:
            en += " — first in the league"
            zh += "，联盟第一人"
        elif rank:
            en += f" (No. {rank})"
            zh += f"（第{rank}）"
        return en, zh
    return None, None

# ── Registration ─────────────────────────────────────────────────────────────

def register_next_ui(app, *, base_render: Callable[..., Any], deps: SimpleNamespace) -> Callable[..., Any]:
    """Install hooks/routes/APIs and return the variant-aware render function."""

    SessionLocal = deps.session_local

    def _in_app_context(fn: Callable[[], Any]) -> Any:
        with app.app_context():
            return fn()

    Game = deps.Game
    Team = deps.Team
    Player = deps.Player
    PlayerGameStats = deps.PlayerGameStats
    TeamGameStats = deps.TeamGameStats
    GamePlayByPlay = deps.GamePlayByPlay
    MetricDefinition = deps.MetricDefinition
    t = deps.t

    # ── request hooks ────────────────────────────────────────────────────
    @app.before_request
    def _next_ui_resolve_variant():
        g.ui_variant = _resolve_variant()

    @app.after_request
    def _next_ui_persist_variant(response):
        arg = (request.args.get("ui") or "").strip().lower()
        if arg in UI_VARIANTS and request.cookies.get(UI_COOKIE) != arg:
            response.set_cookie(UI_COOKIE, arg, max_age=60 * 60 * 24 * 365, samesite="Lax")
        return response

    def ui_switch(variant: str):
        variant = (variant or "").lower()
        if variant not in UI_VARIANTS:
            variant = "next"
        target = request.args.get("next") or request.referrer or "/"
        if not target.startswith("/") or target.startswith("//"):
            # Only allow same-site relative redirects (or our own referrer path).
            try:
                parsed = urlparse(target)
                target = parsed.path or "/"
                if parsed.query:
                    target += "?" + parsed.query
            except Exception:
                target = "/"
        # Drop any explicit ?ui=… from the target so the cookie wins.
        if "ui=" in target:
            parsed = urlparse(target)
            query = [(k, v) for k, v in parse_qsl(parsed.query) if k != "ui"]
            target = parsed.path + (("?" + urlencode(query)) if query else "")
        response = make_response(redirect(target or "/", code=302))
        response.set_cookie(UI_COOKIE, variant, max_age=60 * 60 * 24 * 365, samesite="Lax")
        return response

    app.add_url_rule("/ui/<variant>", endpoint="ui_switch", view_func=ui_switch)

    # ── ticker (today's slate, shown on every next page) ─────────────────
    def _build_ticker() -> list[dict]:
        with SessionLocal() as session:
            today = date.today()
            rows = (
                session.query(Game)
                .filter(Game.game_date == today, Game.game_status.in_(["live", "completed", "upcoming"]))
                .order_by(Game.tipoff_time.asc(), Game.game_id.asc())
                .all()
            )
            last_done_date = (
                session.query(func.max(Game.game_date))
                .filter(Game.game_status == "completed", Game.game_date < today)
                .scalar()
            )
            prev_rows = []
            if last_done_date:
                prev_rows = (
                    session.query(Game)
                    .filter(Game.game_date == last_done_date, Game.game_status == "completed")
                    .order_by(Game.game_id.asc())
                    .all()
                )
            out = []
            for game in prev_rows + rows:
                out.append({
                    "game_id": game.game_id,
                    "slug": game.slug,
                    "date": game.game_date.isoformat() if game.game_date else None,
                    "is_today": game.game_date == today,
                    "status": game.game_status,
                    "home_id": game.home_team_id,
                    "road_id": game.road_team_id,
                    "home_score": game.home_team_score,
                    "road_score": game.road_team_score,
                    "tipoff": game.tipoff_time,
                    "winner": game.wining_team_id,
                })
            return out

    def ticker_games() -> list[dict]:
        try:
            return _cached("ticker", 60, _build_ticker)
        except Exception:
            app.logger.warning("next-ui ticker failed", exc_info=True)
            return []

    # ── team lookups ─────────────────────────────────────────────────────
    def _team_index() -> dict[str, dict]:
        def build():
            with SessionLocal() as session:
                return {
                    tm.team_id: {
                        "team_id": tm.team_id,
                        "abbr": tm.abbr,
                        "name": tm.full_name,
                        "name_zh": tm.full_name_zh,
                        "nick": tm.nick_name,
                        "slug": tm.slug,
                        "active": bool(tm.active) and not bool(tm.is_legacy),
                    }
                    for tm in session.query(Team).all()
                }
        return _cached("team_index", 600, build)

    def team_info(team_id: str | None) -> dict:
        return _team_index().get(str(team_id or ""), {"team_id": team_id, "abbr": "—", "name": "—"})

    # ── current season helpers ───────────────────────────────────────────
    def _current_regular_season(session) -> str | None:
        return (
            session.query(func.max(Game.season))
            .filter(Game.season.like("2%"), Game.game_status == "completed")
            .scalar()
        )

    # ── player season aggregates (leaders / percentiles) ────────────────
    def _season_player_table(season: str) -> dict:
        def build():
            with SessionLocal() as session:
                played = or_(PlayerGameStats.min > 0, PlayerGameStats.sec > 0)
                rows = (
                    session.query(
                        PlayerGameStats.player_id,
                        func.count(PlayerGameStats.game_id).label("gp"),
                        func.sum(PlayerGameStats.min * 60 + func.coalesce(PlayerGameStats.sec, 0)).label("secs"),
                        func.sum(PlayerGameStats.pts).label("pts"),
                        func.sum(PlayerGameStats.reb).label("reb"),
                        func.sum(PlayerGameStats.ast).label("ast"),
                        func.sum(PlayerGameStats.stl).label("stl"),
                        func.sum(PlayerGameStats.blk).label("blk"),
                        func.sum(PlayerGameStats.tov).label("tov"),
                        func.sum(PlayerGameStats.fgm).label("fgm"),
                        func.sum(PlayerGameStats.fga).label("fga"),
                        func.sum(PlayerGameStats.fg3m).label("fg3m"),
                        func.sum(PlayerGameStats.fg3a).label("fg3a"),
                        func.sum(PlayerGameStats.ftm).label("ftm"),
                        func.sum(PlayerGameStats.fta).label("fta"),
                        func.sum(PlayerGameStats.plus).label("pm"),
                        func.max(PlayerGameStats.team_id).label("team_id"),
                    )
                    .join(Game, Game.game_id == PlayerGameStats.game_id)
                    .filter(Game.season == season, played)
                    .group_by(PlayerGameStats.player_id)
                    .all()
                )
                team_games = (
                    session.query(func.count(TeamGameStats.game_id))
                    .join(Game, Game.game_id == TeamGameStats.game_id)
                    .filter(Game.season == season)
                    .group_by(TeamGameStats.team_id)
                    .all()
                )
                max_team_games = max((r[0] for r in team_games), default=0)
                min_gp = max(3, int(round(max_team_games * 0.55)))

                # latest team per player (by most recent game)
                latest_team = dict(
                    session.query(PlayerGameStats.player_id, PlayerGameStats.team_id)
                    .join(Game, Game.game_id == PlayerGameStats.game_id)
                    .filter(Game.season == season)
                    .order_by(Game.game_date.asc())
                    .all()
                )
                player_ids = [r.player_id for r in rows]
                players = {}
                if player_ids:
                    for p in session.query(Player.player_id, Player.full_name, Player.full_name_zh, Player.slug, Player.position, Player.jersey).filter(Player.player_id.in_(player_ids)).all():
                        players[p.player_id] = p

            table = {}
            for r in rows:
                gp = int(r.gp or 0)
                if gp <= 0:
                    continue
                fga, fta = float(r.fga or 0), float(r.fta or 0)
                tsa = 2 * (fga + 0.44 * fta)
                p = players.get(r.player_id)
                table[r.player_id] = {
                    "player_id": r.player_id,
                    "name": p.full_name if p else r.player_id,
                    "name_zh": p.full_name_zh if p else None,
                    "slug": p.slug if p else None,
                    "team_id": latest_team.get(r.player_id) or r.team_id,
                    "gp": gp,
                    "mpg": (float(r.secs or 0) / 60.0) / gp,
                    "ppg": float(r.pts or 0) / gp,
                    "rpg": float(r.reb or 0) / gp,
                    "apg": float(r.ast or 0) / gp,
                    "spg": float(r.stl or 0) / gp,
                    "bpg": float(r.blk or 0) / gp,
                    "tpg": float(r.tov or 0) / gp,
                    "fg3pg": float(r.fg3m or 0) / gp,
                    "fg_pct": (float(r.fgm or 0) / fga) if fga >= 1 else None,
                    "fg3_pct": (float(r.fg3m or 0) / float(r.fg3a)) if (r.fg3a or 0) >= 1 else None,
                    "ft_pct": (float(r.ftm or 0) / fta) if fta >= 1 else None,
                    "ts_pct": (float(r.pts or 0) / tsa) if tsa > 0 else None,
                    "fga_pg": fga / gp,
                    "fg3a_pg": float(r.fg3a or 0) / gp,
                    "fta_pg": fta / gp,
                    "pm_pg": float(r.pm or 0) / gp,
                }
            return {"season": season, "min_gp": min_gp, "players": table}

        return _cached(f"season_players:{season}", 900, build)

    LEADER_CATS = [
        ("ppg", "Points", "得分", "PPG"),
        ("rpg", "Rebounds", "篮板", "RPG"),
        ("apg", "Assists", "助攻", "APG"),
        ("fg3pg", "Threes", "三分", "3PM"),
        ("spg", "Steals", "抢断", "SPG"),
        ("bpg", "Blocks", "盖帽", "BPG"),
    ]

    def league_leaders(season: str, per_cat: int = 5) -> list[dict]:
        data = _season_player_table(season)
        qualified = [p for p in data["players"].values() if p["gp"] >= data["min_gp"]]
        out = []
        for key, en, zh, abbr in LEADER_CATS:
            ranked = sorted(qualified, key=lambda p: p[key], reverse=True)[:per_cat]
            out.append({"key": key, "label_en": en, "label_zh": zh, "abbr": abbr, "rows": ranked})
        return out

    PERCENTILE_STATS = [
        ("ppg", "Points", "得分", 1, False),
        ("rpg", "Rebounds", "篮板", 1, False),
        ("apg", "Assists", "助攻", 1, False),
        ("spg", "Steals", "抢断", 1, False),
        ("bpg", "Blocks", "盖帽", 1, False),
        ("fg3pg", "3PM", "三分命中", 1, False),
        ("ts_pct", "True Shooting", "真实命中率", 1, False),
        ("fg3_pct", "3P%", "三分命中率", 1, False),
        ("tpg", "Turnovers", "失误", 1, True),
        ("mpg", "Minutes", "上场时间", 1, False),
    ]

    def player_percentiles(season: str, player_id: str) -> dict | None:
        data = _season_player_table(season)
        me = data["players"].get(str(player_id))
        if not me:
            return None
        pool = [p for p in data["players"].values() if p["gp"] >= data["min_gp"]]
        if len(pool) < 20:
            return None
        out = []
        for key, en, zh, _digits, lower_is_better in PERCENTILE_STATS:
            mine = me.get(key)
            if mine is None:
                continue
            values = [p[key] for p in pool if p.get(key) is not None]
            if key in ("fg3_pct",):
                values = [p[key] for p in pool if p.get(key) is not None and p["fg3a_pg"] >= 1.0]
                if me["fg3a_pg"] < 1.0:
                    continue
            if not values:
                continue
            below = sum(1 for v in values if v < mine)
            equal = sum(1 for v in values if v == mine)
            pct = (below + 0.5 * equal) / len(values) * 100
            if lower_is_better:
                pct = 100 - pct
            is_pct = key.endswith("_pct")
            out.append({
                "key": key,
                "label_en": en,
                "label_zh": zh,
                "value": (mine * 100) if is_pct else mine,
                "is_pct": is_pct,
                "percentile": max(0, min(100, round(pct))),
                "lower_is_better": lower_is_better,
            })
        return {
            "season": season,
            "qualified": me["gp"] >= data["min_gp"],
            "min_gp": data["min_gp"],
            "pool_size": len(pool),
            "gp": me["gp"],
            "stats": out,
            "line": me,
        }

    # ── team season aggregates (ranks) ───────────────────────────────────
    def _season_team_table(season: str) -> dict:
        def build():
            with SessionLocal() as session:
                rows = (
                    session.query(TeamGameStats, Game.game_date)
                    .join(Game, Game.game_id == TeamGameStats.game_id)
                    .filter(Game.season == season, Game.game_status == "completed")
                    .all()
                )
            by_game: dict[str, list] = defaultdict(list)
            for tgs, _gd in rows:
                by_game[tgs.game_id].append(tgs)
            agg: dict[str, dict] = defaultdict(lambda: defaultdict(float))
            for game_id, pair in by_game.items():
                if len(pair) != 2:
                    continue
                for me, opp in ((pair[0], pair[1]), (pair[1], pair[0])):
                    a = agg[me.team_id]
                    a["gp"] += 1
                    a["w"] += 1 if me.win else 0
                    for col in ("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb"):
                        a[col] += float(getattr(me, col) or 0)
                    a["opp_pts"] += float(opp.pts or 0)
                    a["opp_fga"] += float(opp.fga or 0)
                    a["opp_fta"] += float(opp.fta or 0)
                    a["opp_oreb"] += float(opp.oreb or 0)
                    a["opp_tov"] += float(opp.tov or 0)
                    a["opp_fg3m"] += float(opp.fg3m or 0)
            table = {}
            for team_id, a in agg.items():
                gp = a["gp"] or 1
                poss = (a["fga"] + 0.44 * a["fta"] - a["oreb"] + a["tov"] + a["opp_fga"] + 0.44 * a["opp_fta"] - a["opp_oreb"] + a["opp_tov"]) / 2
                table[team_id] = {
                    "gp": int(a["gp"]),
                    "w": int(a["w"]),
                    "ppg": a["pts"] / gp,
                    "opp_ppg": a["opp_pts"] / gp,
                    "net": (a["pts"] - a["opp_pts"]) / gp,
                    "ortg": (a["pts"] / poss * 100) if poss else None,
                    "drtg": (a["opp_pts"] / poss * 100) if poss else None,
                    "pace": (poss / gp) if gp else None,
                    "fg_pct": a["fgm"] / a["fga"] if a["fga"] else None,
                    "fg3_pct": a["fg3m"] / a["fg3a"] if a["fg3a"] else None,
                    "fg3pg": a["fg3m"] / gp,
                    "rpg": a["reb"] / gp,
                    "apg": a["ast"] / gp,
                    "tpg": a["tov"] / gp,
                    "spg": a["stl"] / gp,
                    "bpg": a["blk"] / gp,
                }
            return table

        return _cached(f"season_teams:{season}", 900, build)

    TEAM_RANK_STATS = [
        ("ortg", "Offensive Rating", "进攻效率", False, False),
        ("drtg", "Defensive Rating", "防守效率", True, False),
        ("net", "Net Margin", "净胜分", False, False),
        ("pace", "Pace", "回合数", False, False),
        ("ppg", "Points", "得分", False, False),
        ("opp_ppg", "Opp. Points", "失分", True, False),
        ("fg3_pct", "3P%", "三分命中率", False, True),
        ("fg3pg", "3PM", "三分命中", False, False),
        ("rpg", "Rebounds", "篮板", False, False),
        ("apg", "Assists", "助攻", False, False),
        ("tpg", "Turnovers", "失误", True, False),
        ("spg", "Steals", "抢断", False, False),
    ]

    def team_ranks(season: str, team_id: str) -> dict | None:
        table = _season_team_table(season)
        me = table.get(str(team_id))
        if not me or len(table) < 10:
            return None
        n = len(table)
        out = []
        for key, en, zh, lower_better, is_pct in TEAM_RANK_STATS:
            mine = me.get(key)
            if mine is None:
                continue
            values = sorted((row[key] for row in table.values() if row.get(key) is not None), reverse=not lower_better)
            rank = values.index(mine) + 1
            out.append({
                "key": key,
                "label_en": en,
                "label_zh": zh,
                "value": mine * 100 if is_pct else mine,
                "is_pct": is_pct,
                "rank": rank,
                "of": n,
                "score": round((n - rank) / max(1, n - 1) * 100),
            })
        return {"season": season, "stats": out, "line": me}

    # ── home page data ───────────────────────────────────────────────────
    def _drama_score(g_: dict) -> float:
        margin = abs((g_["home_score"] or 0) - (g_["road_score"] or 0))
        score = 30 - min(margin, 30)
        score += g_["flow"].get("lead_changes", 0) * 1.5
        score += (g_.get("ot") or 0) * 12
        top = max((pl.get("pts") or 0) for pl in g_["top"]) if g_["top"] else 0
        if top >= 40:
            score += (top - 35) * 1.5
        return score

    def _build_home_extra() -> dict:
        with SessionLocal() as session:
            today = date.today()
            slate_date = (
                session.query(func.max(Game.game_date))
                .filter(Game.game_status == "completed", Game.game_date <= today)
                .scalar()
            )
            games_out: list[dict] = []
            if slate_date:
                games = (
                    session.query(Game)
                    .filter(Game.game_date == slate_date, Game.game_status == "completed")
                    .all()
                )
                game_ids = [gm.game_id for gm in games]
                stat_rows = (
                    session.query(PlayerGameStats, Player.full_name, Player.full_name_zh, Player.slug)
                    .join(Player, Player.player_id == PlayerGameStats.player_id)
                    .filter(PlayerGameStats.game_id.in_(game_ids))
                    .all()
                ) if game_ids else []
                by_game: dict[str, list] = defaultdict(list)
                for st, name, name_zh, slug in stat_rows:
                    by_game[st.game_id].append({
                        "player_id": st.player_id,
                        "team_id": st.team_id,
                        "name": name,
                        "name_zh": name_zh,
                        "slug": slug,
                        "pts": st.pts or 0,
                        "reb": st.reb or 0,
                        "ast": st.ast or 0,
                        "stl": st.stl or 0,
                        "blk": st.blk or 0,
                        "fgm": st.fgm or 0,
                        "fga": st.fga or 0,
                        "fg3m": st.fg3m or 0,
                    })
                pbp = (
                    session.query(GamePlayByPlay.game_id, GamePlayByPlay.period, GamePlayByPlay.pc_time, GamePlayByPlay.score, GamePlayByPlay.event_num)
                    .filter(GamePlayByPlay.game_id.in_(game_ids), GamePlayByPlay.score.isnot(None))
                    .order_by(GamePlayByPlay.game_id, GamePlayByPlay.period, GamePlayByPlay.event_num)
                    .all()
                ) if game_ids else []
                pbp_by_game: dict[str, list] = defaultdict(list)
                for row in pbp:
                    pbp_by_game[row.game_id].append(row)
                max_period = dict(
                    session.query(GamePlayByPlay.game_id, func.max(GamePlayByPlay.period))
                    .filter(GamePlayByPlay.game_id.in_(game_ids))
                    .group_by(GamePlayByPlay.game_id)
                    .all()
                ) if game_ids else {}
                for gm in games:
                    players = by_game.get(gm.game_id, [])
                    top_home = sorted((p for p in players if p["team_id"] == gm.home_team_id), key=lambda p: (p["pts"], p["reb"] + p["ast"]), reverse=True)[:1]
                    top_road = sorted((p for p in players if p["team_id"] == gm.road_team_id), key=lambda p: (p["pts"], p["reb"] + p["ast"]), reverse=True)[:1]
                    series = _margin_series(pbp_by_game.get(gm.game_id, []))
                    prog = [{"t": i, "home": max(0, m), "road": max(0, -m)} for i, m in enumerate(series)]
                    flow = analyze_flow(prog) if prog else {}
                    road_c, home_c = matchup_colors(gm.road_team_id, gm.home_team_id)
                    entry = {
                        "game_id": gm.game_id,
                        "slug": gm.slug,
                        "home_id": gm.home_team_id,
                        "road_id": gm.road_team_id,
                        "home_score": gm.home_team_score,
                        "road_score": gm.road_team_score,
                        "winner": gm.wining_team_id,
                        "home_color": home_c,
                        "road_color": road_c,
                        "series": series,
                        "flow": flow,
                        "ot": max(0, int(max_period.get(gm.game_id) or 4) - 4),
                        "top": top_road + top_home,
                    }
                    entry["drama"] = _drama_score(entry)
                    games_out.append(entry)
                games_out.sort(key=lambda x: x["drama"], reverse=True)

            season = _current_regular_season(session)

            # milestones (narratives) from the slate
            milestones = []
            if games_out and getattr(deps, "milestone_cards_for_game", None):
                for gm in games_out:
                    try:
                        cards = deps.milestone_cards_for_game(session, gm["game_id"])
                    except Exception:
                        continue
                    for card in cards.get("player", []) + cards.get("team", []):
                        if card.get("event_type") not in ("rank_crossing", "absolute_threshold"):
                            continue
                        family = (card.get("metric_key") or "")
                        for suffix in ("_career", "_last10", "_last5", "_last3"):
                            if family.endswith(suffix):
                                family = family[: -len(suffix)]
                        if any(x in family for x in _TRIVIAL_MILESTONE_FAMILIES):
                            continue
                        en, zh = _milestone_sentence(card, team_info)
                        if not en:
                            continue
                        milestones.append({
                            "game_id": gm["game_id"],
                            "game_slug": gm["slug"],
                            "metric_key": card.get("metric_key"),
                            "metric_family": family,
                            "metric_name": card.get("metric_name"),
                            "metric_name_zh": card.get("metric_name_zh"),
                            "entity_type": card.get("entity_type"),
                            "entity_id": card.get("entity_id"),
                            "rank": card.get("rank"),
                            "value_str": card.get("value_str"),
                            "severity": card.get("severity") or 0,
                            "event_type": card.get("event_type"),
                            "narrative_en": en,
                            "narrative_zh": zh,
                            "season": card.get("season"),
                            "window": _window_label(card.get("season")),
                        })
                seen = set()
                deduped = []
                for m in sorted(milestones, key=lambda m: (-(m["severity"] or 0), m["rank"] or 999)):
                    key = (m["entity_id"], m["metric_family"])
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(m)
                milestones = deduped[:10]

        return {
            "slate_date": slate_date.isoformat() if slate_date else None,
            "slate_games": games_out,
            "milestones": milestones,
            "season": season,
        }

    def _build_slate_standouts(slate_games: list[dict]) -> list[dict]:
        with SessionLocal() as session:
            ids = [gm["game_id"] for gm in slate_games]
            if not ids:
                return []
            rows = (
                session.query(PlayerGameStats, Player.full_name, Player.full_name_zh, Player.slug)
                .join(Player, Player.player_id == PlayerGameStats.player_id)
                .filter(PlayerGameStats.game_id.in_(ids))
                .all()
            )
        lines = []
        for st, name, name_zh, slug in rows:
            pts, reb, ast = st.pts or 0, st.reb or 0, st.ast or 0
            fga, fgm, fta, ftm = st.fga or 0, st.fgm or 0, st.fta or 0, st.ftm or 0
            gmsc = pts + 0.4 * fgm - 0.7 * fga - 0.4 * (fta - ftm) + 0.7 * (st.oreb or 0) + 0.3 * (st.dreb or 0) + (st.stl or 0) + 0.7 * ast + 0.7 * (st.blk or 0) - 0.4 * (st.pf or 0) - (st.tov or 0)
            lines.append({
                "player_id": st.player_id,
                "team_id": st.team_id,
                "game_id": st.game_id,
                "name": name,
                "name_zh": name_zh,
                "slug": slug,
                "pts": pts, "reb": reb, "ast": ast,
                "stl": st.stl or 0, "blk": st.blk or 0,
                "fgm": fgm, "fga": fga, "fg3m": st.fg3m or 0,
                "plus": st.plus,
                "gmsc": round(gmsc, 1),
            })
        lines.sort(key=lambda x: x["gmsc"], reverse=True)
        return lines[:8]

    def _cover_story(data: dict) -> dict | None:
        standouts = data.get("standouts") or []
        if not standouts:
            return None
        star = standouts[0]
        game = next((gm for gm in data["slate_games"] if gm["game_id"] == star["game_id"]), None)
        if not game:
            return None
        is_home = star["team_id"] == game["home_id"]
        opp_id = game["road_id"] if is_home else game["home_id"]
        team, opp = team_info(star["team_id"]), team_info(opp_id)
        own_score = game["home_score"] if is_home else game["road_score"]
        opp_score = game["road_score"] if is_home else game["home_score"]
        won = game["winner"] == star["team_id"]
        parts_en = [f"{star['pts']} points"]
        parts_zh = [f"{star['pts']}分"]
        if star["reb"] >= 8:
            parts_en.append(f"{star['reb']} rebounds"); parts_zh.append(f"{star['reb']}篮板")
        if star["ast"] >= 6:
            parts_en.append(f"{star['ast']} assists"); parts_zh.append(f"{star['ast']}助攻")
        if star["blk"] >= 4:
            parts_en.append(f"{star['blk']} blocks"); parts_zh.append(f"{star['blk']}盖帽")
        line_en = parts_en[0] if len(parts_en) == 1 else ", ".join(parts_en[:-1]) + " and " + parts_en[-1]
        verb_en = "beat" if won else "fall to"
        sentence_en = (
            f"{star['name']} puts up {line_en} as the {team.get('nick') or team.get('abbr')} "
            f"{verb_en} the {opp.get('nick') or opp.get('abbr')}, {own_score}–{opp_score}."
        )
        sentence_zh = (
            f"{star.get('name_zh') or star['name']}砍下{''.join(parts_zh)}，"
            f"{team.get('name_zh') or team.get('abbr')}以{own_score}–{opp_score}"
            f"{'击败' if won else '不敌'}{opp.get('name_zh') or opp.get('abbr')}。"
        )
        last_name = (star["name"] or "").split(" ")[-1] if star.get("name") else ""
        headline_nums = [str(star["pts"])]
        if star["reb"] >= 10:
            headline_nums.append(str(star["reb"]))
        elif star["ast"] >= 10:
            headline_nums.append(str(star["ast"]))
        return {
            "player": star,
            "game": game,
            "team_id": star["team_id"],
            "opp_id": opp_id,
            "won": won,
            "own_score": own_score,
            "opp_score": opp_score,
            "last_name": last_name,
            "headline_nums": headline_nums,
            "sentence_en": sentence_en,
            "sentence_zh": sentence_zh,
        }

    def home_extra() -> dict:
        def build():
            data = _build_home_extra()
            data["standouts"] = _build_slate_standouts(data["slate_games"])
            data["cover"] = _cover_story(data)
            if data.get("season"):
                data["leaders"] = league_leaders(data["season"])
            else:
                data["leaders"] = []
            return data
        return _cached_swr("home_extra", 120, build, wrap=_in_app_context)

    # ── metrics count (for nav / home) ───────────────────────────────────
    def published_metric_count() -> int:
        def build():
            with SessionLocal() as session:
                counter = getattr(deps, "catalog_metrics_total", None)
                if callable(counter):
                    return int(counter(session) or 0)
                return int(
                    session.query(func.count(MetricDefinition.id))
                    .filter(MetricDefinition.status == "published")
                    .scalar() or 0
                )
        try:
            # Counting the catalog compiles every code metric (~5s cold), so
            # never block a page on it: serve the last value, refresh in the
            # background, and show no number until the first build lands.
            return _cached_swr("metric_count", 3600, build, fallback=None, wrap=_in_app_context)
        except Exception:
            return None

    # ── enrichers per template ───────────────────────────────────────────
    def enrich_home(ctx: dict) -> dict:
        try:
            return {"nx": home_extra()}
        except Exception:
            app.logger.warning("next-ui home enrichment failed", exc_info=True)
            return {"nx": {"slate_games": [], "milestones": [], "leaders": [], "standouts": []}}

    def enrich_game(ctx: dict) -> dict:
        game = ctx.get("game")
        if game is None:
            return {}
        road_c, home_c = matchup_colors(game.road_team_id, game.home_team_id)
        try:
            progression = json.loads(ctx.get("score_progression_json") or "[]")
        except ValueError:
            progression = []
        flow = analyze_flow(progression) if len(progression) > 1 else None
        team_stats = {}
        for ts in ctx.get("team_stats") or []:
            tid = ts.get("team_id") if isinstance(ts, dict) else getattr(ts, "team_id", None)
            if tid:
                team_stats[str(tid)] = ts
        season_line = {}
        try:
            if game.season:
                tbl = _season_team_table(game.season)
                season_line = {tid: tbl.get(tid) for tid in (game.road_team_id, game.home_team_id)}
        except Exception:
            season_line = {}
        return {
            "nx": {
                "road_color": road_c,
                "home_color": home_c,
                "flow": flow,
                "team_stats_by_id": team_stats,
                "road": team_info(game.road_team_id),
                "home": team_info(game.home_team_id),
                "season_line": season_line,
                "ot_periods": max(0, len(ctx.get("quarter_scores") or []) - 4),
            }
        }

    def enrich_player(ctx: dict) -> dict:
        player = ctx.get("player")
        if player is None:
            return {}
        season = ctx.get("selected_season")
        percentiles = None
        try:
            if season and str(season).startswith("2"):
                percentiles = player_percentiles(str(season), player.player_id)
        except Exception:
            app.logger.warning("next-ui percentiles failed", exc_info=True)
        team_id = ctx.get("player_current_team_id")
        return {
            "nx": {
                "color": team_color(team_id),
                "color2": team_color(team_id, 1),
                "age": _age_from(getattr(player, "birth_date", None)),
                "percentiles": percentiles,
            }
        }

    def enrich_team(ctx: dict) -> dict:
        team = ctx.get("team")
        if team is None:
            return {}
        season = ctx.get("selected_season")
        ranks = None
        try:
            if season and str(season).startswith("2"):
                ranks = team_ranks(str(season), team.team_id)
        except Exception:
            app.logger.warning("next-ui team ranks failed", exc_info=True)
        return {
            "nx": {
                "color": team_color(team.team_id),
                "color2": team_color(team.team_id, 1),
                "ranks": ranks,
            }
        }

    def enrich_metrics(ctx: dict) -> dict:
        return {"nx": {"metric_count": published_metric_count()}}

    _ENRICHERS: dict[str, Callable[[dict], dict]] = {
        "home.html": enrich_home,
        "game.html": enrich_game,
        "player.html": enrich_player,
        "team.html": enrich_team,
        "metrics.html": enrich_metrics,
    }

    # ── template filters ─────────────────────────────────────────────────
    _WINDOW_SUFFIXES = ("_career", "_last10", "_last5", "_last3")

    def _family(metric_key: str | None) -> str:
        key = metric_key or ""
        for suffix in _WINDOW_SUFFIXES:
            if key.endswith(suffix):
                return key[: -len(suffix)]
        return key

    def _dedupe_family(items):
        """Keep the first item per (entity, metric family) — lists arrive sorted by significance."""
        seen = set()
        out = []
        for item in items or []:
            get = item.get if isinstance(item, dict) else (lambda k, _i=item: getattr(_i, k, None))
            key = (str(get("entity_id") or get("team_id") or ""), _family(get("metric_key")))
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    app.add_template_filter(_dedupe_family, "dedupe_family")
    app.add_template_filter(_family, "metric_family")

    # ── warm slow caches in the background at startup ────────────────────
    if not app.config.get("TESTING") and os.getenv("FUNBA_NEXT_UI_WARM", "1") != "0":
        def _warm():
            time.sleep(3)
            try:
                published_metric_count()
                home_extra()
            except Exception:
                pass
        threading.Thread(target=_warm, name="next-ui-warm", daemon=True).start()

    # ── context processor (all templates, next pages use these) ─────────
    @app.context_processor
    def _next_ui_helpers():
        return {
            "ui_variant": current_variant(),
            "ui_switch_url": lambda variant: f"/ui/{variant}?next={request.full_path.rstrip('?')}" if request else f"/ui/{variant}",
            "team_color": team_color,
            "matchup_colors": matchup_colors,
            "ink_on": ink_on,
            "headshot_src": headshot_url,
            "team_info": team_info,
            "ticker_games": ticker_games,
            "fmt_num": _fmt_num,
            "fmt_pct": _fmt_pct,
            "fmt_money": _fmt_money,
            "ordinal": _ordinal,
            "metric_count": published_metric_count,
            "nx_now": datetime.now,
        }

    # ── APIs ─────────────────────────────────────────────────────────────
    def api_next_search():
        q = (request.args.get("q") or "").strip()
        if len(q) < 1:
            return jsonify({"q": q, "groups": []})
        like = f"%{q}%"
        prefix = f"{q}%"
        groups = []
        with SessionLocal() as session:
            teams = (
                session.query(Team)
                .filter(
                    Team.active.is_(True),
                    Team.is_legacy.is_(False),
                    or_(Team.full_name.ilike(like), Team.abbr.ilike(prefix), Team.nick_name.ilike(like), Team.full_name_zh.like(like), Team.city.ilike(prefix)),
                )
                .limit(5)
                .all()
            )
            if teams:
                groups.append({
                    "key": "teams",
                    "label": t("Teams", "球队"),
                    "items": [
                        {
                            "label": (tm.full_name_zh if getattr(g, "lang", "en") == "zh" and tm.full_name_zh else tm.full_name),
                            "sub": tm.abbr,
                            "url": deps.team_url(tm.team_id),
                            "image": deps.team_logo(tm.team_id),
                            "color": team_color(tm.team_id),
                        }
                        for tm in teams
                    ],
                })
            players = (
                session.query(Player)
                .filter(
                    Player.is_team.is_(False),
                    or_(Player.full_name.ilike(like), Player.full_name_zh.like(like), Player.last_name.ilike(prefix)),
                )
                .order_by(
                    case((or_(Player.full_name.ilike(prefix), Player.full_name_zh.like(prefix), Player.last_name.ilike(prefix)), 0), else_=1),
                    Player.is_active.desc(),
                    Player.to_year.desc(),
                )
                .limit(8)
                .all()
            )
            if players:
                groups.append({
                    "key": "players",
                    "label": t("Players", "球员"),
                    "items": [
                        {
                            "label": (p.full_name_zh if getattr(g, "lang", "en") == "zh" and p.full_name_zh else p.full_name),
                            "sub": " · ".join(x for x in [
                                (t("Active", "现役") if p.is_active else (f"{p.from_year}–{p.to_year}" if p.from_year else "")),
                                p.position or "",
                            ] if x),
                            "url": deps.player_url(p.player_id),
                            "image": headshot_url(p.player_id),
                            "kind": "player",
                        }
                        for p in players
                    ],
                })
            metrics = (
                session.query(MetricDefinition.key, MetricDefinition.name, MetricDefinition.name_zh, MetricDefinition.scope, MetricDefinition.category)
                .filter(
                    MetricDefinition.status == "published",
                    MetricDefinition.variant == "season",
                    or_(MetricDefinition.name.ilike(like), MetricDefinition.name_zh.like(like), MetricDefinition.key.ilike(like)),
                )
                .limit(8)
                .all()
            )
            if metrics:
                groups.append({
                    "key": "metrics",
                    "label": t("Metrics", "指标"),
                    "items": [
                        {
                            "label": (m.name_zh if getattr(g, "lang", "en") == "zh" and m.name_zh else m.name),
                            "sub": " · ".join(x for x in [m.scope or "", m.category or ""] if x),
                            "url": deps.url_for("metric_detail", metric_key=m.key),
                            "kind": "metric",
                        }
                        for m in metrics
                    ],
                })
        groups.append({
            "key": "ask",
            "label": t("Ask", "提问"),
            "items": [{
                "label": t(f"Find metrics for “{q}”", f"搜索与“{q}”相关的指标"),
                "sub": t("Natural-language metric search", "自然语言指标搜索"),
                "url": deps.url_for("metrics_browse") + "?q=" + quote(q),
                "kind": "ask",
            }],
        })
        return jsonify({"q": q, "groups": groups})

    limiter = getattr(deps, "limiter", None)
    search_view = limiter.limit("90 per minute")(api_next_search) if limiter else api_next_search
    app.add_url_rule("/api/next/search", endpoint="api_next_search", view_func=search_view)

    def api_next_leaders():
        with SessionLocal() as session:
            season = request.args.get("season") or _current_regular_season(session)
        if not season:
            return jsonify({"season": None, "categories": []})
        return jsonify({"season": season, "categories": league_leaders(str(season), per_cat=10)})

    app.add_url_rule("/api/next/leaders", endpoint="api_next_leaders", view_func=api_next_leaders)

    def api_next_player_percentiles(player_id: str):
        season = request.args.get("season")
        with SessionLocal() as session:
            season = season or _current_regular_season(session)
        data = player_percentiles(str(season), player_id) if season else None
        return jsonify({"season": season, "data": data})

    app.add_url_rule("/api/next/players/<player_id>/percentiles", endpoint="api_next_player_percentiles", view_func=api_next_player_percentiles)

    def api_next_team_ranks(team_id: str):
        season = request.args.get("season")
        with SessionLocal() as session:
            season = season or _current_regular_season(session)
        data = team_ranks(str(season), team_id) if season else None
        return jsonify({"season": season, "data": data})

    app.add_url_rule("/api/next/teams/<team_id>/ranks", endpoint="api_next_team_ranks", view_func=api_next_team_ranks)

    def api_next_home():
        data = home_extra()
        return jsonify({k: v for k, v in data.items()})

    app.add_url_rule("/api/next/home", endpoint="api_next_home", view_func=api_next_home)

    # ── per-page plugin modules (web/next_pages/<template_stem>.py) ──────
    # A page module may define:
    #   enrich(ctx: dict, api) -> dict   extra keys merged into ``nx``
    #   register(app, api) -> None       add routes / APIs at startup
    api = SimpleNamespace(
        app=app,
        deps=deps,
        session_local=SessionLocal,
        models=SimpleNamespace(
            Game=Game, Team=Team, Player=Player, PlayerGameStats=PlayerGameStats,
            TeamGameStats=TeamGameStats, GamePlayByPlay=GamePlayByPlay, MetricDefinition=MetricDefinition,
        ),
        cached=_cached,
        t=t,
        url_for=deps.url_for,
        player_url=deps.player_url,
        team_url=deps.team_url,
        game_url=deps.game_url,
        team_logo=deps.team_logo,
        team_color=team_color,
        matchup_colors=matchup_colors,
        headshot_url=headshot_url,
        team_info=team_info,
        current_regular_season=_current_regular_season,
        season_player_table=_season_player_table,
        season_team_table=_season_team_table,
        league_leaders=league_leaders,
        player_percentiles=player_percentiles,
        team_ranks=team_ranks,
        analyze_flow=analyze_flow,
        margin_series=_margin_series,
    )
    _page_modules: dict[str, Any] = {}
    pages_dir = Path(__file__).resolve().parent / "next_pages"
    if pages_dir.is_dir():
        import importlib

        for file in sorted(pages_dir.glob("*.py")):
            if file.stem.startswith("_"):
                continue
            try:
                module = importlib.import_module(f"web.next_pages.{file.stem}")
            except Exception:
                app.logger.warning("next-ui: failed to import page module %s", file.stem, exc_info=True)
                continue
            _page_modules[file.stem] = module
            register_fn = getattr(module, "register", None)
            if callable(register_fn):
                try:
                    register_fn(app, api)
                except Exception:
                    app.logger.warning("next-ui: register() failed for %s", file.stem, exc_info=True)

    # ── the render wrapper ───────────────────────────────────────────────
    def render(template_name: str, **context):
        if current_variant() == "next" and _has_next_template(template_name):
            enricher = _ENRICHERS.get(template_name)
            if enricher is not None:
                try:
                    context.update(enricher(context))
                except Exception:
                    app.logger.warning("next-ui enricher failed for %s", template_name, exc_info=True)
            module = _page_modules.get(Path(template_name).stem.lstrip("_"))
            page_enrich = getattr(module, "enrich", None) if module else None
            if callable(page_enrich):
                try:
                    extra = page_enrich(context, api) or {}
                    nx = dict(context.get("nx") or {})
                    nx.update(extra)
                    context["nx"] = nx
                except Exception:
                    app.logger.warning("next-ui page enrich failed for %s", template_name, exc_info=True)
            return base_render(f"next/{template_name}", **context)
        return base_render(template_name, **context)

    return render
