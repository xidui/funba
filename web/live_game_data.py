from __future__ import annotations

import re

from db.game_status import (
    GAME_STATUS_COMPLETED,
    GAME_STATUS_LIVE,
    GAME_STATUS_UPCOMING,
)


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
        home_team = game.get("homeTeam") or {}
        away_team = game.get("awayTeam") or {}
        result[game_id] = {
            "game_id": game_id,
            "status": status,
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
    actions = pbp_payload.get("game", {}).get("actions", [])

    return {
        "summary": {
            "game_id": str(game.get("gameId") or game_id),
            "status": status,
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
