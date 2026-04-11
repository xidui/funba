from __future__ import annotations

from datetime import date

from sqlalchemy import and_, or_


GAME_STATUS_LIVE = "live"
GAME_STATUS_COMPLETED = "completed"
GAME_STATUS_UPCOMING = "upcoming"
GAME_STATUS_VALUES = {
    GAME_STATUS_LIVE,
    GAME_STATUS_COMPLETED,
    GAME_STATUS_UPCOMING,
}


def infer_game_status(
    *,
    game_date: date | None,
    wining_team_id: str | None,
    home_team_score: int | None = None,
    road_team_score: int | None = None,
    today: date | None = None,
) -> str | None:
    """Infer coarse status from persisted Game fields."""
    if wining_team_id:
        return GAME_STATUS_COMPLETED

    if game_date is None:
        return None

    current_day = today or date.today()
    if game_date > current_day:
        return GAME_STATUS_UPCOMING

    if game_date < current_day:
        return GAME_STATUS_LIVE

    if home_team_score is not None or road_team_score is not None:
        return GAME_STATUS_LIVE

    return GAME_STATUS_UPCOMING


def normalize_game_status(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in GAME_STATUS_VALUES:
        return normalized
    return None


def get_game_status(game, *, today: date | None = None) -> str | None:
    explicit = normalize_game_status(getattr(game, "game_status", None))
    if explicit:
        return explicit
    return infer_game_status(
        game_date=getattr(game, "game_date", None),
        wining_team_id=getattr(game, "wining_team_id", None),
        home_team_score=getattr(game, "home_team_score", None),
        road_team_score=getattr(game, "road_team_score", None),
        today=today,
    )


def is_game_completed(game, *, today: date | None = None) -> bool:
    return get_game_status(game, today=today) == GAME_STATUS_COMPLETED


def completed_game_clause(Game):
    """SQL clause that treats legacy null-status rows with a winner as completed."""
    return or_(
        Game.game_status == GAME_STATUS_COMPLETED,
        and_(Game.game_status.is_(None), Game.wining_team_id.isnot(None)),
    )
