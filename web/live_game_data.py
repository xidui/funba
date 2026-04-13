from __future__ import annotations

import math
import re
import time
from datetime import date, datetime
from types import SimpleNamespace

from db.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_LIVE,
    GAME_STATUS_UPCOMING,
)

# TTL cache for live card lookups — many home-page hits within a 15s poll
# window should share one nba_api call.
_LIVE_CARD_CACHE: dict[str, tuple[float, dict]] = {}
_LIVE_CARD_TTL_SEC = 12.0


_ISO_CLOCK_RE = re.compile(r"^PT(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?$")


def _safe_int(value, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _pct_value(raw):
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value > 1:
        value = value / 100.0
    return value


def _format_clock(raw_clock: str | None) -> str:
    text = str(raw_clock or "").strip()
    if not text:
        return "0:00"
    match = _ISO_CLOCK_RE.match(text)
    if not match:
        return text
    minutes = int(match.group("minutes") or 0)
    seconds = int(float(match.group("seconds") or 0))
    return f"{minutes}:{seconds:02d}"


def _format_minutes(raw_value: str | None) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return "—"
    if ":" in text and not text.startswith("PT"):
        return text
    return _format_clock(text)


def _status_from_code(game_status: int | str | None) -> str:
    code = _safe_int(game_status, default=1)
    if code >= 3:
        return GAME_STATUS_COMPLETED
    if code == 2:
        return GAME_STATUS_LIVE
    return GAME_STATUS_UPCOMING


def _period_label(period: int) -> str:
    if period <= 0:
        return ""
    if period <= 4:
        return f"Q{period}"
    return f"OT{period - 4}"


def _summary_text(status: str, period: int, raw_clock: str | None, status_text: str | None) -> str:
    if status == GAME_STATUS_LIVE:
        label = _period_label(period) or "LIVE"
        return f"{label} {_format_clock(raw_clock)}".strip()
    if status == GAME_STATUS_COMPLETED:
        return str(status_text or "Final")
    return str(status_text or "")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _snapshot_game_date(raw_snapshot: dict) -> date | None:
    text = str(raw_snapshot.get("game_date") or "").strip()
    if text:
        try:
            return date.fromisoformat(text)
        except ValueError:
            pass

    for key in ("gameEt", "gameTimeUTC"):
        parsed = _parse_iso_datetime(raw_snapshot.get(key))
        if parsed is not None:
            return parsed.date()
    return None


def _season_from_game_id(game_id: str | None) -> str | None:
    text = str(game_id or "").strip()
    if len(text) < 5 or not text[:5].isdigit():
        return None

    season_type = text[2]
    season_year = text[3:5]
    if season_type not in {"2", "4", "5"}:
        return None
    return f"{season_type}20{season_year}"


def build_live_game_stub(snapshot: dict | None):
    if not snapshot:
        return None

    game_id = str(snapshot.get("game_id") or "")
    if not game_id:
        return None

    status = str(snapshot.get("status") or "").strip().lower() or None
    home_team_id = str(snapshot.get("home_team_id") or "")
    road_team_id = str(snapshot.get("road_team_id") or "")
    home_score = snapshot.get("home_score")
    road_score = snapshot.get("road_score")
    winner_id = None
    if (
        status == GAME_STATUS_COMPLETED
        and home_team_id
        and road_team_id
        and home_score is not None
        and road_score is not None
        and home_score != road_score
    ):
        winner_id = home_team_id if home_score > road_score else road_team_id

    return SimpleNamespace(
        game_id=game_id,
        game_date=_snapshot_game_date(snapshot),
        season=str(snapshot.get("season") or _season_from_game_id(game_id) or "") or None,
        home_team_id=home_team_id or None,
        road_team_id=road_team_id or None,
        home_team_score=home_score,
        road_team_score=road_score,
        wining_team_id=winner_id,
        game_status=status,
    )


def fetch_live_scoreboard_map() -> dict[str, dict]:
    try:
        from nba_api.live.nba.endpoints.scoreboard import ScoreBoard
    except Exception:
        return {}

    try:
        payload = ScoreBoard().get_dict()
    except Exception:
        return {}

    games = payload.get("scoreboard", {}).get("games", [])
    result: dict[str, dict] = {}
    for game in games:
        game_id = str(game.get("gameId") or "")
        if not game_id:
            continue
        status = _status_from_code(game.get("gameStatus"))
        game_date = _snapshot_game_date(game)
        home_team = game.get("homeTeam") or {}
        away_team = game.get("awayTeam") or {}
        result[game_id] = {
            "game_id": game_id,
            "status": status,
            "season": _season_from_game_id(game_id),
            "game_date": game_date.isoformat() if game_date else None,
            "status_text": str(game.get("gameStatusText") or ""),
            "period": _safe_int(game.get("period")),
            "clock": _format_clock(game.get("gameClock")),
            "summary": _summary_text(
                status,
                _safe_int(game.get("period")),
                game.get("gameClock"),
                game.get("gameStatusText"),
            ),
            "home_team_id": str(home_team.get("teamId") or ""),
            "road_team_id": str(away_team.get("teamId") or ""),
            "home_score": _safe_int(home_team.get("score")),
            "road_score": _safe_int(away_team.get("score")),
            "game_time_et": str(game.get("gameEt") or ""),
            "game_time_utc": str(game.get("gameTimeUTC") or ""),
        }
    return result


def _build_team_stats(team: dict, *, win=None) -> dict:
    stats = team.get("statistics") or {}
    return {
        "team_id": str(team.get("teamId") or ""),
        "pts": _safe_int(team.get("score")),
        "reb": _safe_int(stats.get("reboundsTotal")),
        "ast": _safe_int(stats.get("assists")),
        "fg_pct": _pct_value(stats.get("fieldGoalsPercentage")),
        "fg3_pct": _pct_value(stats.get("threePointersPercentage")),
        "ft_pct": _pct_value(stats.get("freeThrowsPercentage")),
        "win": win,
    }


def _player_row(player: dict) -> dict:
    stats = player.get("statistics") or {}
    played = bool(player.get("played"))
    plus_minus = stats.get("plusMinusPoints")
    try:
        plus_minus_value = int(float(plus_minus))
    except (TypeError, ValueError):
        plus_minus_value = "-"
    return {
        "player_id": str(player.get("personId") or ""),
        "player_name": str(
            player.get("nameI")
            or player.get("name")
            or f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
            or player.get("personId")
            or "-"
        ),
        "status": str(player.get("status") or player.get("notPlayingDescription") or ("Active" if played else "DNP")),
        "minutes": _format_minutes(stats.get("minutes") or stats.get("minutesCalculated")),
        "is_starter": bool(player.get("starter")),
        "is_dnp": not played,
        "pts": _safe_int(stats.get("points")) if played else "-",
        "reb": _safe_int(stats.get("reboundsTotal")) if played else "-",
        "ast": _safe_int(stats.get("assists")) if played else "-",
        "stl": _safe_int(stats.get("steals")) if played else "-",
        "blk": _safe_int(stats.get("blocks")) if played else "-",
        "tov": _safe_int(stats.get("turnovers")) if played else "-",
        "fgm": _safe_int(stats.get("fieldGoalsMade")) if played else "-",
        "fga": _safe_int(stats.get("fieldGoalsAttempted")) if played else "-",
        "fg3m": _safe_int(stats.get("threePointersMade")) if played else "-",
        "fg3a": _safe_int(stats.get("threePointersAttempted")) if played else "-",
        "ftm": _safe_int(stats.get("freeThrowsMade")) if played else "-",
        "fta": _safe_int(stats.get("freeThrowsAttempted")) if played else "-",
        "plus_minus": plus_minus_value,
    }


def _build_players_by_team(team: dict) -> list[dict]:
    rows = [_player_row(player) for player in (team.get("players") or [])]
    rows.sort(
        key=lambda row: (
            0 if row["is_starter"] else 1,
            0 if not row["is_dnp"] else 1,
            -(row["pts"] if isinstance(row["pts"], int) else -1),
            row["player_name"],
        )
    )
    return rows


def _build_quarter_scores(home_team: dict, away_team: dict) -> list[dict]:
    home_periods = {
        _safe_int(period.get("period")): _safe_int(period.get("score"))
        for period in (home_team.get("periods") or [])
    }
    away_periods = {
        _safe_int(period.get("period")): _safe_int(period.get("score"))
        for period in (away_team.get("periods") or [])
    }
    periods = sorted(set(home_periods) | set(away_periods))
    return [
        {
            "period": period,
            "home": home_periods.get(period, 0),
            "road": away_periods.get(period, 0),
        }
        for period in periods
        if period > 0
    ]


def _build_pbp_rows(actions: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for action in reversed(actions):
        description = str(action.get("description") or "").strip()
        if not description:
            continue
        score_home = action.get("scoreHome")
        score_away = action.get("scoreAway")
        score = "-"
        if score_home not in (None, "") and score_away not in (None, ""):
            score = f"{score_away}-{score_home}"
        rows.append(
            {
                "event_num": _safe_int(action.get("actionNumber")) or "-",
                "period": _safe_int(action.get("period")) or "-",
                "clock": _format_clock(action.get("clock")),
                "event_type": str(action.get("actionType") or "").replace("_", " ").title() or "-",
                "event_type_code": action.get("actionType"),
                "description": description,
                "score": score,
                "margin": "-",
                "team_id": str(action.get("teamId") or "") or None,
            }
        )
    return rows


def _parse_mmss(clock: str | None) -> int:
    """Parse 'M:SS' → seconds. Returns 0 on bad input."""
    text = str(clock or "").strip()
    if not text or ":" not in text:
        return 0
    try:
        m, s = text.split(":", 1)
        return int(m) * 60 + int(float(s))
    except (ValueError, TypeError):
        return 0


def _compute_win_probability(
    road_score: int,
    home_score: int,
    period: int,
    clock: str | None,
) -> tuple[float, float]:
    """Heuristic live win probability. Returns (road_wp, home_wp) in [0.02, 0.98].

    Based on score differential scaled by sqrt(time remaining). Tanh
    squashes to [0,1]. Tuning constant derived from empirical NBA data
    where a 10-point lead at half translates to roughly 80% win rate.
    """
    period = max(1, int(period or 1))
    clock_sec = _parse_mmss(clock)
    # 12-minute regulation quarters, 5-minute OT periods.
    if period <= 4:
        remaining_sec = clock_sec + (4 - period) * 12 * 60
    else:
        remaining_sec = clock_sec  # OT
    if remaining_sec <= 0:
        if home_score > road_score:
            return (0.02, 0.98)
        if road_score > home_score:
            return (0.98, 0.02)
        return (0.50, 0.50)
    diff_home = (home_score or 0) - (road_score or 0)
    time_min = remaining_sec / 60.0
    sigma = 3.0 * math.sqrt(max(time_min, 0.5))
    z = diff_home / sigma
    home_wp = 0.5 + 0.5 * math.tanh(z * 0.75)
    home_wp = max(0.02, min(0.98, home_wp))
    return (round(1.0 - home_wp, 3), round(home_wp, 3))


def _hot_pts_threshold(period: int) -> int:
    period = max(1, min(int(period or 1), 5))
    # Quarter-scaled threshold: hotter needed later in the game.
    return {1: 10, 2: 16, 3: 22, 4: 28, 5: 32}[period]


def _live_leader(players: list[dict], field: str) -> dict | None:
    best = None
    for p in players:
        val = p.get(field)
        if not isinstance(val, int):
            continue
        if best is None or val > best.get(field, 0):
            best = p
    if not best or not isinstance(best.get(field), int) or best[field] <= 0:
        return None
    return best


def _format_live_player_leader(player: dict | None, stat_field: str) -> dict | None:
    if not player:
        return None
    pts = player.get("pts") if isinstance(player.get("pts"), int) else 0
    fg3m = player.get("fg3m") if isinstance(player.get("fg3m"), int) else 0
    fg3a = player.get("fg3a") if isinstance(player.get("fg3a"), int) else 0
    return {
        "player_id": player.get("player_id"),
        "name": player.get("player_name") or "",
        "value": player.get(stat_field) if isinstance(player.get(stat_field), int) else 0,
        "pts": pts,
        "fg3m": fg3m,
        "fg3a": fg3a,
    }


def fetch_live_card(game_id: str) -> dict | None:
    """Compact data for home-page live card: leaders, shooting, win prob.

    Pulls BoxScore once per `_LIVE_CARD_TTL_SEC` window (PBP is skipped —
    the home card doesn't need event-level data). Returns None if the
    live endpoint is unavailable.
    """
    now = time.time()
    cached = _LIVE_CARD_CACHE.get(game_id)
    if cached and (now - cached[0]) < _LIVE_CARD_TTL_SEC:
        return cached[1]

    try:
        from nba_api.live.nba.endpoints.boxscore import BoxScore
    except Exception:
        return None
    try:
        box_payload = BoxScore(game_id).get_dict()
    except Exception:
        return None

    game = box_payload.get("game") or {}
    if not game:
        return None

    home_team = game.get("homeTeam") or {}
    away_team = game.get("awayTeam") or {}
    status = _status_from_code(game.get("gameStatus"))
    period = _safe_int(game.get("period"))
    raw_clock = game.get("gameClock")
    clock = _format_clock(raw_clock)

    road_score = _safe_int(away_team.get("score"))
    home_score = _safe_int(home_team.get("score"))

    road_ts = _build_team_stats(away_team)
    home_ts = _build_team_stats(home_team)

    road_players = _build_players_by_team(away_team)
    home_players = _build_players_by_team(home_team)

    road_scorer = _format_live_player_leader(_live_leader(road_players, "pts"), "pts")
    home_scorer = _format_live_player_leader(_live_leader(home_players, "pts"), "pts")
    road_rebounder = _format_live_player_leader(_live_leader(road_players, "reb"), "reb")
    home_rebounder = _format_live_player_leader(_live_leader(home_players, "reb"), "reb")
    road_assister = _format_live_player_leader(_live_leader(road_players, "ast"), "ast")
    home_assister = _format_live_player_leader(_live_leader(home_players, "ast"), "ast")

    road_wp, home_wp = _compute_win_probability(road_score, home_score, period, clock)

    hot_threshold = _hot_pts_threshold(period) if status == GAME_STATUS_LIVE else 10**9
    hot_players: list[str] = []
    for p in road_players + home_players:
        pts = p.get("pts")
        if isinstance(pts, int) and pts >= hot_threshold:
            pid = p.get("player_id")
            if pid:
                hot_players.append(str(pid))

    card = {
        "game_id": str(game.get("gameId") or game_id),
        "status": status,
        "period": period,
        "clock": clock,
        "status_text": str(game.get("gameStatusText") or ""),
        "summary": _summary_text(status, period, raw_clock, game.get("gameStatusText")),
        "home_team_id": str(home_team.get("teamId") or ""),
        "road_team_id": str(away_team.get("teamId") or ""),
        "home_score": home_score,
        "road_score": road_score,
        "home_fg_pct": round(home_ts["fg_pct"] * 100, 1) if home_ts.get("fg_pct") is not None else None,
        "road_fg_pct": round(road_ts["fg_pct"] * 100, 1) if road_ts.get("fg_pct") is not None else None,
        "home_fg3_pct": round(home_ts["fg3_pct"] * 100, 1) if home_ts.get("fg3_pct") is not None else None,
        "road_fg3_pct": round(road_ts["fg3_pct"] * 100, 1) if road_ts.get("fg3_pct") is not None else None,
        "home_scorer": home_scorer,
        "road_scorer": road_scorer,
        "home_rebounder": home_rebounder,
        "road_rebounder": road_rebounder,
        "home_assister": home_assister,
        "road_assister": road_assister,
        "home_win_probability": home_wp,
        "road_win_probability": road_wp,
        "hot_player_ids": hot_players,
    }
    _LIVE_CARD_CACHE[game_id] = (now, card)
    return card


def fetch_live_game_detail(game_id: str) -> dict | None:
    try:
        from nba_api.live.nba.endpoints.boxscore import BoxScore
        from nba_api.live.nba.endpoints.playbyplay import PlayByPlay
    except Exception:
        return None

    try:
        box_payload = BoxScore(game_id).get_dict()
        pbp_payload = PlayByPlay(game_id).get_dict()
    except Exception:
        return None

    game = box_payload.get("game") or {}
    if not game:
        return None

    home_team = game.get("homeTeam") or {}
    away_team = game.get("awayTeam") or {}
    status = _status_from_code(game.get("gameStatus"))
    game_date = _snapshot_game_date(game)
    actions = pbp_payload.get("game", {}).get("actions", [])

    return {
        "summary": {
            "game_id": str(game.get("gameId") or game_id),
            "status": status,
            "game_date": game_date.isoformat() if game_date else None,
            "status_text": str(game.get("gameStatusText") or ""),
            "period": _safe_int(game.get("period")),
            "clock": _format_clock(game.get("gameClock")),
            "summary": _summary_text(
                status,
                _safe_int(game.get("period")),
                game.get("gameClock"),
                game.get("gameStatusText"),
            ),
            "home_score": _safe_int(home_team.get("score")),
            "road_score": _safe_int(away_team.get("score")),
            "home_team_id": str(home_team.get("teamId") or ""),
            "road_team_id": str(away_team.get("teamId") or ""),
        },
        "team_stats": [
            _build_team_stats(away_team),
            _build_team_stats(home_team),
        ],
        "players_by_team": {
            str(away_team.get("teamId") or ""): _build_players_by_team(away_team),
            str(home_team.get("teamId") or ""): _build_players_by_team(home_team),
        },
        "ordered_team_ids": [
            str(away_team.get("teamId") or ""),
            str(home_team.get("teamId") or ""),
        ],
        "quarter_scores": _build_quarter_scores(home_team, away_team),
        "pbp_rows": _build_pbp_rows(actions),
    }
