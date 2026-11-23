"""Next UI — player page enrichment.

Adds to ``nx``:
  hero_team_id / color / color2  franchise identity (current team, else the
                                  team the player logged the most games for)
  season_team                      {season_code: team_id} (most games that season)
  career_series                    chronological per-season rows for the career chart
  game_log                         chronological game rows for the game-log chart
  season_line                      the selected season's stat row (for the hero)
  shots_flat                       [x, y, made, x, y, made, ...] for the hexbin chart
  rankings                         {scope: [metric rows sorted by percentile]}
  top_ranks                        best ranks across scopes (headline list)
"""
from __future__ import annotations

from collections import defaultdict

from sqlalchemy import func


def _team_games(api, player_id: str) -> dict:
    def build():
        Game = api.models.Game
        PGS = api.models.PlayerGameStats
        with api.session_local() as session:
            rows = (
                session.query(Game.season, PGS.team_id, func.count(PGS.game_id))
                .join(Game, Game.game_id == PGS.game_id)
                .filter(PGS.player_id == str(player_id))
                .group_by(Game.season, PGS.team_id)
                .all()
            )
        by_season: dict[str, tuple[str, int]] = {}
        totals: dict[str, int] = defaultdict(int)
        for season, team_id, n in rows:
            if not team_id:
                continue
            totals[team_id] += int(n or 0)
            best = by_season.get(season)
            if best is None or n > best[1]:
                by_season[str(season)] = (team_id, int(n or 0))
        primary = max(totals.items(), key=lambda kv: kv[1])[0] if totals else None
        return {"season_team": {s: v[0] for s, v in by_season.items()}, "primary": primary}

    return api.cached(f"nx_player_teams:{player_id}", 600, build)


def _num(value):
    try:
        if value in (None, "", "-", "—"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio(rank, total):
    r, tot = _num(rank), _num(total)
    if not r or not tot:
        return 1.0
    return r / tot


def _metric_row(m: dict, scope: str, url_season: str | None) -> dict:
    return {
        "metric_key": m.get("metric_key"),
        "metric_name": m.get("metric_name"),
        "category": m.get("category") or "",
        "value": m.get("value_str") if m.get("value_str") else (
            ("%.3g" % m["value_num"]) if m.get("value_num") is not None else "—"
        ),
        "rank": int(_num(m.get("rank")) or 0) or None,
        "total": int(_num(m.get("total")) or 0) or None,
        "career_rank": int(_num(m.get("career_rank")) or 0) or None,
        "career_total": int(_num(m.get("career_total")) or 0) or None,
        "last3_rank": int(_num((m.get("last3") or {}).get("rank")) or 0) or None,
        "last5_rank": int(_num((m.get("last5") or {}).get("rank")) or 0) or None,
        "last10_rank": int(_num((m.get("last10") or {}).get("rank")) or 0) or None,
        "is_notable": bool(m.get("is_notable")),
        "context_label": m.get("context_label"),
        "ratio": _ratio(m.get("rank"), m.get("total")),
        "scope": scope,
        "url_season": url_season,
    }


def enrich(ctx: dict, api) -> dict:
    player = ctx.get("player")
    if player is None:
        return {}
    out: dict = {}

    # ── franchise identity ───────────────────────────────────────────────
    current_team_id = ctx.get("player_current_team_id")
    try:
        teams = _team_games(api, player.player_id)
    except Exception:
        teams = {"season_team": {}, "primary": None}
    # Active players wear their current colors; retired players their
    # defining franchise (most games), not merely their last stop.
    if getattr(player, "is_active", False):
        hero_team_id = current_team_id or teams.get("primary")
    else:
        hero_team_id = teams.get("primary") or current_team_id
    out["hero_team_id"] = hero_team_id
    out["color"] = api.team_color(hero_team_id)
    out["color2"] = api.team_color(hero_team_id, 1)
    season_team = teams.get("season_team") or {}
    out["season_team"] = season_team

    # ── career series (chronological) ────────────────────────────────────
    series = []
    for row in reversed(ctx.get("career_season_rows") or []):
        s = row.get("stats") if isinstance(row, dict) else getattr(row, "stats", None)
        season = row.get("season") if isinstance(row, dict) else getattr(row, "season", None)
        if not s:
            continue
        tid = season_team.get(str(season))
        series.append({
            "season": season,
            "year": int(str(season)[1:]) if season and len(str(season)) == 5 else None,
            "teams": list(row.get("team_abbrs") or []) if isinstance(row, dict) else [],
            "team_id": tid,
            "color": api.team_color(tid) if tid else out["color"],
            "gp": _num(s.get("games_played")),
            "ppg": _num(s.get("ppg")),
            "rpg": _num(s.get("rpg")),
            "apg": _num(s.get("apg")),
            "spg": _num(s.get("spg")),
            "bpg": _num(s.get("bpg")),
            "mpg": _num(s.get("mpg")),
            "fg_pct": _num(s.get("fg_pct")),
            "fg3_pct": _num(s.get("fg3_pct")),
            "ft_pct": _num(s.get("ft_pct")),
        })
    out["career_series"] = series

    # ── selected season line ─────────────────────────────────────────────
    selected = str(ctx.get("selected_season") or "")
    season_line = None
    for row in ctx.get("career_season_rows") or []:
        if str(row.get("season")) == selected:
            season_line = row.get("stats")
            break
    out["season_line"] = season_line

    # ── game log (chronological) ─────────────────────────────────────────
    rows = list(ctx.get("game_rows") or [])
    rows.sort(key=lambda r: str(r.get("game_date") or ""))
    log = []
    for r in rows:
        log.append({
            "game_id": r.get("game_id"),
            "date": str(r.get("game_date") or ""),
            "opp": r.get("opponent_abbr"),
            "home": bool(r.get("is_home")),
            "result": r.get("result"),
            "score": f"{r.get('player_team_score')}–{r.get('opponent_score')}" if r.get("player_team_score") is not None else "",
            "min": r.get("minutes"),
            "pts": r.get("pts"),
            "reb": r.get("reb"),
            "ast": r.get("ast"),
            "status": r.get("status"),
            "url": api.game_url(r.get("game_id")) if r.get("game_id") else None,
        })
    out["game_log"] = log

    # ── shots (compact) ──────────────────────────────────────────────────
    flat = []
    for d in ctx.get("shot_dots") or []:
        x, y = d.get("x"), d.get("y")
        if x is None or y is None:
            continue
        flat.extend((int(round(float(x))), int(round(float(y))), 1 if d.get("made") else 0))
    out["shots_flat"] = flat

    # ── rankings ─────────────────────────────────────────────────────────
    pm = ctx.get("player_metrics") or {}
    rankings: dict[str, list] = {"season": [], "all_regular": [], "all_playoffs": [], "all_playin": []}
    for m in pm.get("season") or []:
        rankings["season"].append(_metric_row(m, "season", None))
    for m in pm.get("alltime") or []:
        ctype = m.get("career_type") or ""
        if ctype in rankings:
            rankings[ctype].append(_metric_row(m, ctype, ctype))
    for key in rankings:
        rankings[key].sort(key=lambda r: (r["ratio"], r["rank"] or 9999))
    out["rankings"] = {k: v for k, v in rankings.items() if v}

    # Headline ranks: best ranks first, preferring the scopes readers care
    # about (this season, regular-season career) over playoffs / play-in,
    # with at most one play-in entry and decent pool sizes.
    scope_weight = {"season": 0, "all_regular": 1, "all_playoffs": 2, "all_playin": 3}
    top = [
        r for k, v in rankings.items() for r in v
        if r["rank"] and r["rank"] <= 10 and (r["total"] or 0) >= 50
    ]
    top.sort(key=lambda r: (r["rank"], scope_weight.get(r["scope"], 9), r["ratio"]))
    seen = set()
    headline = []
    playin = 0
    for r in top:
        fam = (r["metric_key"] or "")
        for suffix in ("_career", "_last10", "_last5", "_last3"):
            if fam.endswith(suffix):
                fam = fam[: -len(suffix)]
        if fam in seen:
            continue
        if r["scope"] == "all_playin":
            if playin >= 1:
                continue
            playin += 1
        seen.add(fam)
        headline.append(r)
        if len(headline) >= 6:
            break
    out["top_ranks"] = headline

    # ── salary fallback (no contract panel) ──────────────────────────────
    fallback = []
    if not ctx.get("salary_panel"):
        for r in ctx.get("salary_rows") or []:
            season = getattr(r, "season", None)
            fallback.append({
                "season": season,
                "cap_hit_usd": getattr(r, "salary_usd", None),
                "flavor": "other",
                "team_id": None,
                "is_future": False,
            })
        fallback.sort(key=lambda r: r["season"] or 0)
    out["money_fallback"] = fallback
    return out
