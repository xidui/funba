"""Next UI — team page enrichment.

Adds to ``nx``:
  standing      conference / league standing for the selected season's regular season
  conf_table    the team's conference standings (modern alignment, 2004+)
  results       chronological per-game point differential for the selected season
  upcoming      next scheduled games (current season only)
  champ_years   championship season start years
  top_metrics   season metrics sorted by percentile rank
  alltime_top   notable all-time (career, regular season) metrics
"""
from __future__ import annotations

from datetime import date

# Modern (2004–present) Eastern Conference alignment.
EAST_IDS = {
    "1610612737", "1610612738", "1610612751", "1610612766", "1610612741",
    "1610612739", "1610612765", "1610612754", "1610612748", "1610612749",
    "1610612752", "1610612753", "1610612755", "1610612761", "1610612764",
}
ACTIVE_IDS = EAST_IDS | {
    "1610612742", "1610612743", "1610612744", "1610612745", "1610612746",
    "1610612747", "1610612763", "1610612750", "1610612740", "1610612760",
    "1610612756", "1610612757", "1610612758", "1610612759", "1610612762",
}


def _ratio(item) -> float:
    try:
        rank = float(item.get("rank") or 0)
        total = float(item.get("total") or 0)
    except (TypeError, ValueError):
        return 9.0
    if rank <= 0 or total <= 0:
        return 9.0
    return rank / total


def _standing(api, team_id: str, season: str | None) -> tuple[dict | None, list[dict]]:
    if not season or len(str(season)) != 5:
        return None, []
    reg = "2" + str(season)[1:]
    try:
        table = api.season_team_table(reg)
    except Exception:
        return None, []
    if team_id not in table or len(table) < 8:
        return None, []

    def pct(row):
        return (row["w"] / row["gp"]) if row["gp"] else 0.0

    rows = []
    for tid, row in table.items():
        rows.append({
            "team_id": tid,
            "w": int(row["w"]),
            "l": int(row["gp"] - row["w"]),
            "pct": pct(row),
            "net": row.get("net") or 0.0,
        })
    rows.sort(key=lambda r: (-r["pct"], -r["w"], -r["net"]))
    league_rank = next(i for i, r in enumerate(rows, 1) if r["team_id"] == team_id)

    year = int(reg[1:])
    conf = None
    conf_rows: list[dict] = []
    if year >= 2004 and team_id in ACTIVE_IDS:
        is_east = team_id in EAST_IDS
        conf = "East" if is_east else "West"
        conf_rows = [r for r in rows if (r["team_id"] in EAST_IDS) == is_east and r["team_id"] in ACTIVE_IDS]
        if conf_rows:
            lead = conf_rows[0]
            for i, r in enumerate(conf_rows, 1):
                r["rank"] = i
                gb = ((lead["w"] - r["w"]) + (r["l"] - lead["l"])) / 2
                r["gb"] = gb
                info = api.team_info(r["team_id"])
                r["abbr"] = info.get("abbr")
                r["slug"] = info.get("slug")
    me_conf = next((r for r in conf_rows if r["team_id"] == team_id), None)
    standing = {
        "season": reg,
        "league_rank": league_rank,
        "league_size": len(rows),
        "conf": conf,
        "conf_rank": me_conf["rank"] if me_conf else None,
        "conf_size": len(conf_rows) or None,
        "gb": me_conf["gb"] if me_conf else None,
    }
    return standing, conf_rows


def _results(current_games: list[dict]) -> list[dict]:
    out = []
    for g in reversed(current_games or []):
        score = str(g.get("score") or "")
        if "-" not in score or g.get("result") not in ("W", "L"):
            continue
        try:
            a, b = score.split("-", 1)
            mine, opp = int(a), int(b)
        except ValueError:
            continue
        out.append({
            "game_id": g.get("game_id"),
            "date": g.get("game_date"),
            "opp_id": g.get("opponent_id"),
            "opp": g.get("opponent_name"),
            "where": g.get("where"),
            "result": g.get("result"),
            "pts": mine,
            "opp_pts": opp,
            "diff": mine - opp,
        })
    return out


def _upcoming(api, team_id: str, season: str) -> list[dict]:
    Game = api.models.Game
    with api.session_local() as session:
        rows = (
            session.query(Game)
            .filter(
                Game.season == season,
                Game.game_status == "upcoming",
                Game.game_date >= date.today(),
                (Game.home_team_id == team_id) | (Game.road_team_id == team_id),
            )
            .order_by(Game.game_date.asc(), Game.game_id.asc())
            .limit(5)
            .all()
        )
        return [
            {
                "game_id": g.game_id,
                "slug": g.slug,
                "date": g.game_date,
                "home": g.home_team_id == team_id,
                "opp_id": g.road_team_id if g.home_team_id == team_id else g.home_team_id,
                "tipoff": g.tipoff_time,
            }
            for g in rows
        ]


def enrich(ctx: dict, api) -> dict:
    team = ctx.get("team")
    if team is None:
        return {}
    team_id = str(team.team_id)
    season = ctx.get("selected_season")
    out: dict = {}

    try:
        standing, conf_rows = _standing(api, team_id, season)
    except Exception:
        standing, conf_rows = None, []
    out["standing"] = standing
    out["conf_table"] = conf_rows

    out["results"] = _results(ctx.get("current_games") or [])

    out["upcoming"] = []
    if season and season == ctx.get("current_season"):
        try:
            out["upcoming"] = _upcoming(api, team_id, season)
        except Exception:
            out["upcoming"] = []

    champ_years = []
    for c in ctx.get("team_championships") or []:
        try:
            champ_years.append(int(str(c.get("season"))[1:]))
        except (TypeError, ValueError):
            continue
    out["champ_years"] = champ_years

    metrics = ctx.get("team_metrics") or {}
    season_items = [m for m in (metrics.get("season") or []) if m.get("rank")]
    out["top_metrics"] = sorted(season_items, key=_ratio)[:12]
    alltime = [
        m for m in (metrics.get("alltime") or [])
        if m.get("rank") and (m.get("career_type") in (None, "all_regular")) and (m.get("window_type") in (None, "career"))
    ]
    out["alltime_top"] = sorted(alltime, key=_ratio)[:10]

    # Bars for the season-by-season chart (JSON-safe).
    out["bars"] = [
        {
            "season": b.get("season"),
            "year": b.get("year_start"),
            "label": b.get("year_label"),
            "w": b.get("wins") or 0,
            "l": b.get("losses") or 0,
            "po": bool(b.get("has_playoffs")),
        }
        for b in (ctx.get("season_bars") or [])
    ]
    return out
