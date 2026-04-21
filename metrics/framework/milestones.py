from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
import time
from typing import Mapping, Protocol, Sequence

from sqlalchemy import and_, case, func, or_
from sqlalchemy.orm import Session, aliased

from db.models import Game, MetricMilestone, MetricResult, Player, PlayerGameStats, Team, TeamGameStats
from metrics.framework.base import career_season_for, career_season_type_code, is_career_season
from metrics.framework.family import family_base_key, family_window_key
from metrics.framework.runtime import get_all_metrics, get_metric

logger = logging.getLogger(__name__)


DEFAULT_RANK_THRESHOLDS: tuple[int, ...] = (1, 5, 10, 25, 100)
MAX_PASSED_PLAYERS = 10
SEVERITY_MIN_EMIT = 0.4
RANK_CROSSING_EVENT_TYPE = "rank_crossing"
APPROACHING_EVENT_TYPE = "approaching_target"
ABSOLUTE_THRESHOLD_EVENT_TYPE = "absolute_threshold"
APPROACHING_ABSOLUTE_EVENT_TYPE = "approaching_absolute"

STAT_LABELS: dict[str, tuple[str, str]] = {
    "pts": ("得分", "points"),
    "ast": ("助攻", "assists"),
    "reb": ("篮板", "rebounds"),
    "oreb": ("前场篮板", "offensive rebounds"),
    "dreb": ("后场篮板", "defensive rebounds"),
    "stl": ("抢断", "steals"),
    "blk": ("盖帽", "blocks"),
    "tov": ("失误", "turnovers"),
    "pf": ("犯规", "fouls"),
    "min": ("分钟", "minutes"),
    "fgm": ("投篮命中", "field goals made"),
    "fga": ("投篮出手", "field goal attempts"),
    "fg3m": ("三分命中", "three-pointers made"),
    "fg3a": ("三分出手", "three-point attempts"),
    "ftm": ("罚球命中", "free throws made"),
    "fta": ("罚球出手", "free throw attempts"),
    "win": ("胜场", "wins"),
    "loss": ("败场", "losses"),
}


class MilestoneValueProvider(Protocol):
    def __call__(
        self,
        session: Session,
        game_id: str,
        metric_key: str,
        season: str,
        entity_type: str,
        deltas: Mapping[str, float],
        current_pool: Mapping[str, float],
    ) -> dict[str, float]:
        ...


def _json_dumps(value) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _now_naive_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _clean_number(value: float | int | None) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _display_number(value: float | int | None) -> int | float | None:
    if value is None:
        return None
    number = float(value)
    rounded = round(number)
    if abs(number - rounded) < 1e-9:
        return int(rounded)
    return round(number, 4)


def _played(row: PlayerGameStats) -> bool:
    seconds = int(getattr(row, "min", 0) or 0) * 60 + int(getattr(row, "sec", 0) or 0)
    if seconds > 0:
        return True
    for field in ("pts", "reb", "ast", "stl", "blk", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta"):
        if int(getattr(row, field, 0) or 0) != 0:
            return True
    return False


def _inner_metric(metric):
    return getattr(metric, "_inner", metric)


def _metric_attr(metric, name: str, default=None):
    if hasattr(metric, name):
        return getattr(metric, name)
    return getattr(_inner_metric(metric), name, default)


def _value_from_player_row(row: PlayerGameStats, field: str | None) -> float:
    if not field:
        return 0.0
    if field == "min":
        return (int(getattr(row, "min", 0) or 0) * 60 + int(getattr(row, "sec", 0) or 0)) / 60.0
    return _clean_number(getattr(row, field, 0))


def _compare(value: float, threshold: float, comparator: str) -> bool:
    if comparator == "==":
        return value == threshold
    if comparator == "<=":
        return value <= threshold
    return value >= threshold


def _criteria_met(row, criteria: Sequence[tuple[str, float]], comparator: str) -> bool:
    for field, threshold in criteria or ():
        if not _compare(_clean_number(getattr(row, field, 0)), float(threshold), comparator):
            return False
    return True


def _double_digit_category_count(row) -> int:
    return sum(1 for field in ("pts", "reb", "ast", "stl", "blk") if _clean_number(getattr(row, field, 0)) >= 10)


def _player_split_matches(row: PlayerGameStats, game: Game, split_key: str | None) -> bool:
    if not split_key:
        return True
    team_id = str(row.team_id)
    if split_key == "wins":
        return team_id == str(game.wining_team_id)
    if split_key == "losses":
        return game.wining_team_id is not None and team_id != str(game.wining_team_id)
    if split_key == "home":
        return team_id == str(game.home_team_id)
    if split_key == "road":
        return team_id == str(game.road_team_id)
    if split_key == "starter":
        return bool(row.starter)
    if split_key == "bench":
        return not bool(row.starter)
    return True


def _load_player_game_deltas(
    session: Session,
    game: Game,
    metric,
) -> tuple[dict[str, float], dict[str, int]]:
    kind = str(_metric_attr(metric, "metric_kind", "") or "")
    value_field = _metric_attr(metric, "value_field", None)
    criteria = tuple(_metric_attr(metric, "criteria", ()) or ())
    comparator = str(_metric_attr(metric, "comparator", ">=") or ">=")
    split_key = _metric_attr(metric, "split_key", None)
    rows = (
        session.query(PlayerGameStats)
        .filter(
            PlayerGameStats.game_id == game.game_id,
            PlayerGameStats.player_id.isnot(None),
        )
        .all()
    )
    deltas: dict[str, float] = {}
    game_counts: dict[str, int] = {}
    for row in rows:
        if not _played(row):
            continue
        if not _player_split_matches(row, game, split_key):
            continue
        player_id = str(row.player_id)
        game_counts[player_id] = 1
        delta = 0.0
        if kind == "season_total":
            delta = _value_from_player_row(row, value_field)
        elif kind == "games_played":
            delta = 1.0
        elif kind == "games_started":
            delta = 1.0 if bool(row.starter) else 0.0
        elif kind in {"count_threshold", "count_combo", "count_exact"}:
            delta = 1.0 if _criteria_met(row, criteria, comparator) else 0.0
        elif kind == "double_double":
            delta = 1.0 if _double_digit_category_count(row) >= 2 else 0.0
        elif kind == "triple_double":
            delta = 1.0 if _double_digit_category_count(row) >= 3 else 0.0
        if delta:
            deltas[player_id] = delta
    return deltas, game_counts


def _team_split_matches(game: Game, row: TeamGameStats, split_key: str | None) -> bool:
    team_id = str(row.team_id)
    if split_key == "home":
        return team_id == str(game.home_team_id)
    if split_key == "road":
        return team_id == str(game.road_team_id)
    if split_key == "wins":
        return team_id == str(game.wining_team_id)
    if split_key == "losses":
        return game.wining_team_id is not None and team_id != str(game.wining_team_id)
    if split_key == "overtime":
        return int(game.home_team_score or 0) > 120 or int(game.road_team_score or 0) > 120
    return True


def _team_metric_value(row: TeamGameStats, opponent: TeamGameStats, game: Game, stat_field: str | None) -> float:
    if stat_field == "opp_pts":
        return _clean_number(opponent.pts)
    if stat_field == "opp_fg_pct":
        return _clean_number(opponent.fg_pct) * 100.0
    if stat_field == "point_diff":
        return _clean_number(row.pts) - _clean_number(opponent.pts)
    if stat_field == "close_game":
        return 1.0 if abs(int((row.pts or 0) - (opponent.pts or 0))) <= 5 else 0.0
    if stat_field == "dominant_win":
        return 1.0 if bool(row.win) and int((row.pts or 0) - (opponent.pts or 0)) >= 15 else 0.0
    if stat_field == "blowout_win":
        return 1.0 if bool(row.win) and int((row.pts or 0) - (opponent.pts or 0)) >= 10 else 0.0
    if stat_field == "blowout_loss":
        return 1.0 if not bool(row.win) and int((opponent.pts or 0) - (row.pts or 0)) >= 10 else 0.0
    if stat_field == "win":
        return 1.0 if bool(row.win) else 0.0
    if stat_field == "loss":
        return 1.0 if not bool(row.win) else 0.0
    return _clean_number(getattr(row, stat_field or "", 0))


def _load_team_game_deltas(
    session: Session,
    game: Game,
    metric,
) -> tuple[dict[str, float], dict[str, int]]:
    kind = str(_metric_attr(metric, "metric_kind", "") or "")
    stat_field = _metric_attr(metric, "stat_field", None)
    threshold = _metric_attr(metric, "threshold", None)
    split_key = _metric_attr(metric, "split_key", None)
    rows = (
        session.query(TeamGameStats)
        .filter(TeamGameStats.game_id == game.game_id, TeamGameStats.team_id.isnot(None))
        .all()
    )
    if len(rows) < 2:
        return {}, {}
    deltas: dict[str, float] = {}
    game_counts: dict[str, int] = {}
    for row in rows:
        opponent = next((other for other in rows if str(other.team_id) != str(row.team_id)), None)
        if opponent is None or not _team_split_matches(game, row, split_key):
            continue
        team_id = str(row.team_id)
        game_counts[team_id] = 1
        delta = 0.0
        if kind == "count":
            if stat_field == "win":
                delta = 1.0 if bool(row.win) else 0.0
            elif stat_field == "loss":
                delta = 1.0 if not bool(row.win) else 0.0
            elif threshold is not None:
                delta = 1.0 if _team_metric_value(row, opponent, game, stat_field) >= float(threshold) else 0.0
            else:
                delta = _team_metric_value(row, opponent, game, stat_field)
        if delta:
            deltas[team_id] = delta
    return deltas, game_counts


def _load_game_metric_deltas(
    session: Session,
    game: Game,
    metric,
) -> tuple[dict[str, float], dict[str, int]]:
    scope = str(getattr(metric, "scope", "") or "")
    if scope == "player":
        return _load_player_game_deltas(session, game, metric)
    if scope == "team":
        return _load_team_game_deltas(session, game, metric)
    return {}, {}


def _season_game_query(session: Session, season: str, cutoff_game_date, cutoff_game_id: str):
    query = session.query(Game)
    if is_career_season(season):
        season_type_code = career_season_type_code(season)
        if not season_type_code:
            return None
        query = query.filter(Game.season.like(f"{season_type_code}%"))
    else:
        query = query.filter(Game.season == season)

    if cutoff_game_date is not None:
        query = query.filter(
            (Game.game_date < cutoff_game_date)
            | ((Game.game_date == cutoff_game_date) & (Game.game_id < cutoff_game_id))
        )
    else:
        query = query.filter(Game.game_id < cutoff_game_id)
    return query.filter(Game.home_team_score.isnot(None)).order_by(Game.game_date.asc(), Game.game_id.asc())


def _game_filter_clauses(season: str, cutoff_game_date, cutoff_game_id: str, game_model=Game) -> list:
    clauses = [game_model.home_team_score.isnot(None)]
    if is_career_season(season):
        season_type_code = career_season_type_code(season)
        if not season_type_code:
            return [False]
        clauses.append(game_model.season.like(f"{season_type_code}%"))
    else:
        clauses.append(game_model.season == season)
    if cutoff_game_date is not None:
        clauses.append(
            or_(
                game_model.game_date < cutoff_game_date,
                and_(game_model.game_date == cutoff_game_date, game_model.game_id < cutoff_game_id),
            )
        )
    else:
        clauses.append(game_model.game_id < cutoff_game_id)
    return clauses


def _player_played_clause():
    seconds = func.coalesce(PlayerGameStats.min, 0) * 60 + func.coalesce(PlayerGameStats.sec, 0)
    stat_clauses = [
        func.coalesce(getattr(PlayerGameStats, field), 0) != 0
        for field in ("pts", "reb", "ast", "stl", "blk", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta")
    ]
    return or_(seconds > 0, *stat_clauses)


def _player_split_clause(split_key: str | None):
    if not split_key:
        return True
    if split_key == "wins":
        return PlayerGameStats.team_id == Game.wining_team_id
    if split_key == "losses":
        return and_(Game.wining_team_id.isnot(None), PlayerGameStats.team_id != Game.wining_team_id)
    if split_key == "home":
        return PlayerGameStats.team_id == Game.home_team_id
    if split_key == "road":
        return PlayerGameStats.team_id == Game.road_team_id
    if split_key == "starter":
        return PlayerGameStats.starter.is_(True)
    if split_key == "bench":
        return or_(PlayerGameStats.starter.is_(False), PlayerGameStats.starter.is_(None))
    return True


def _player_value_expr(field: str | None):
    if not field:
        return 0.0
    if field == "min":
        return (func.coalesce(PlayerGameStats.min, 0) * 60 + func.coalesce(PlayerGameStats.sec, 0)) / 60.0
    return func.coalesce(getattr(PlayerGameStats, field), 0)


def _criteria_clause(criteria: Sequence[tuple[str, float]], comparator: str):
    clauses = []
    for field, threshold in criteria or ():
        value = func.coalesce(getattr(PlayerGameStats, field), 0)
        threshold_value = float(threshold)
        if comparator == "==":
            clauses.append(value == threshold_value)
        elif comparator == "<=":
            clauses.append(value <= threshold_value)
        else:
            clauses.append(value >= threshold_value)
    return and_(*clauses) if clauses else True


def _double_digit_category_count_expr():
    return sum(
        case((func.coalesce(getattr(PlayerGameStats, field), 0) >= 10, 1), else_=0)
        for field in ("pts", "reb", "ast", "stl", "blk")
    )


def _aggregate_player_pool_sql(session: Session, metric, season: str, cutoff_game_date, cutoff_game_id: str) -> dict[str, float]:
    kind = str(_metric_attr(metric, "metric_kind", "") or "")
    min_sample = max(int(getattr(metric, "min_sample", 1) or 1), 1)
    split_key = _metric_attr(metric, "split_key", None)
    base_filters = [
        PlayerGameStats.player_id.isnot(None),
        _player_played_clause(),
        _player_split_clause(split_key),
        *_game_filter_clauses(season, cutoff_game_date, cutoff_game_id),
    ]
    games_count = func.count(PlayerGameStats.game_id)

    if kind == "season_total":
        value_expr = func.sum(_player_value_expr(_metric_attr(metric, "value_field", None)))
    elif kind == "games_played":
        value_expr = games_count
    elif kind == "games_started":
        value_expr = func.sum(case((PlayerGameStats.starter.is_(True), 1), else_=0))
    elif kind in {"count_threshold", "count_combo", "count_exact"}:
        criteria = tuple(_metric_attr(metric, "criteria", ()) or ())
        comparator = str(_metric_attr(metric, "comparator", ">=") or ">=")
        value_expr = func.sum(case((_criteria_clause(criteria, comparator), 1), else_=0))
    elif kind == "double_double":
        value_expr = func.sum(case((_double_digit_category_count_expr() >= 2, 1), else_=0))
    elif kind == "triple_double":
        value_expr = func.sum(case((_double_digit_category_count_expr() >= 3, 1), else_=0))
    else:
        raise ValueError(f"Unsupported player metric_kind for pool aggregation: {kind}")

    rows = (
        session.query(PlayerGameStats.player_id, value_expr.label("value"), games_count.label("games"))
        .join(Game, PlayerGameStats.game_id == Game.game_id)
        .filter(*base_filters)
        .group_by(PlayerGameStats.player_id)
        .having(games_count >= min_sample)
        .all()
    )
    return _positive_pool({str(entity_id): _clean_number(value) for entity_id, value, _games in rows if entity_id})


def _team_split_clause(row, game, split_key: str | None):
    if not split_key:
        return True
    if split_key == "home":
        return row.team_id == game.home_team_id
    if split_key == "road":
        return row.team_id == game.road_team_id
    if split_key == "wins":
        return row.team_id == game.wining_team_id
    if split_key == "losses":
        return and_(game.wining_team_id.isnot(None), row.team_id != game.wining_team_id)
    if split_key == "overtime":
        return or_(func.coalesce(game.home_team_score, 0) > 120, func.coalesce(game.road_team_score, 0) > 120)
    return True


def _team_metric_value_expr(row, opponent, game, stat_field: str | None):
    if stat_field == "opp_pts":
        return func.coalesce(opponent.pts, 0)
    if stat_field == "opp_fg_pct":
        return func.coalesce(opponent.fg_pct, 0) * 100.0
    if stat_field == "point_diff":
        return func.coalesce(row.pts, 0) - func.coalesce(opponent.pts, 0)
    if stat_field == "close_game":
        return case((func.abs(func.coalesce(row.pts, 0) - func.coalesce(opponent.pts, 0)) <= 5, 1), else_=0)
    if stat_field == "dominant_win":
        return case((and_(row.win.is_(True), (func.coalesce(row.pts, 0) - func.coalesce(opponent.pts, 0)) >= 15), 1), else_=0)
    if stat_field == "blowout_win":
        return case((and_(row.win.is_(True), (func.coalesce(row.pts, 0) - func.coalesce(opponent.pts, 0)) >= 10), 1), else_=0)
    if stat_field == "blowout_loss":
        return case((and_(or_(row.win.is_(False), row.win.is_(None)), (func.coalesce(opponent.pts, 0) - func.coalesce(row.pts, 0)) >= 10), 1), else_=0)
    if stat_field == "win":
        return case((row.win.is_(True), 1), else_=0)
    if stat_field == "loss":
        return case((or_(row.win.is_(False), row.win.is_(None)), 1), else_=0)
    return func.coalesce(getattr(row, stat_field or "", 0), 0)


def _aggregate_team_pool_sql(session: Session, metric, season: str, cutoff_game_date, cutoff_game_id: str) -> dict[str, float]:
    kind = str(_metric_attr(metric, "metric_kind", "") or "")
    if kind != "count":
        raise ValueError(f"Unsupported team metric_kind for pool aggregation: {kind}")
    row = aliased(TeamGameStats)
    opponent = aliased(TeamGameStats)
    min_sample = max(int(getattr(metric, "min_sample", 1) or 1), 1)
    stat_field = _metric_attr(metric, "stat_field", None)
    threshold = _metric_attr(metric, "threshold", None)
    split_key = _metric_attr(metric, "split_key", None)
    games_count = func.count(row.game_id)

    raw_value = _team_metric_value_expr(row, opponent, Game, stat_field)
    if stat_field == "win":
        value_expr = func.sum(case((row.win.is_(True), 1), else_=0))
    elif stat_field == "loss":
        value_expr = func.sum(case((or_(row.win.is_(False), row.win.is_(None)), 1), else_=0))
    elif threshold is not None:
        value_expr = func.sum(case((raw_value >= float(threshold), 1), else_=0))
    else:
        value_expr = func.sum(raw_value)

    rows = (
        session.query(row.team_id, value_expr.label("value"), games_count.label("games"))
        .join(Game, row.game_id == Game.game_id)
        .join(opponent, and_(opponent.game_id == row.game_id, opponent.team_id != row.team_id))
        .filter(
            row.team_id.isnot(None),
            _team_split_clause(row, Game, split_key),
            *_game_filter_clauses(season, cutoff_game_date, cutoff_game_id),
        )
        .group_by(row.team_id)
        .having(games_count >= min_sample)
        .all()
    )
    return _positive_pool({str(entity_id): _clean_number(value) for entity_id, value, _games in rows if entity_id})


def aggregate_pool_as_of(
    session: Session,
    metric,
    season: str,
    cutoff_game_date,
    cutoff_game_id: str,
) -> dict[str, float]:
    """Return the additive metric pool immediately before cutoff game."""
    started = time.perf_counter()
    kind = str(_metric_attr(metric, "metric_kind", "") or "")
    supported_kinds = {"season_total", "count_threshold", "count_combo", "count_exact", "games_played", "games_started", "count", "double_double", "triple_double"}
    if kind not in supported_kinds:
        raise ValueError(f"Unsupported metric_kind for pool aggregation: {kind}")

    scope = str(getattr(metric, "scope", "") or "")
    if scope == "player":
        pool = _aggregate_player_pool_sql(session, metric, season, cutoff_game_date, cutoff_game_id)
    elif scope == "team":
        pool = _aggregate_team_pool_sql(session, metric, season, cutoff_game_date, cutoff_game_id)
    else:
        raise ValueError(f"Unsupported scope for pool aggregation: {scope}")
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    if elapsed_ms > 1000:
        logger.warning(
            "aggregate_pool_as_of slow metric=%s season=%s cutoff=%s ms=%d rows=%d",
            getattr(metric, "key", None),
            season,
            cutoff_game_id,
            elapsed_ms,
            len(pool),
        )
    return pool


def _rank_in_pool(pool: Mapping[str, float], entity_id: str) -> int | None:
    value = pool.get(entity_id)
    if value is None or value <= 0:
        return None
    better = sum(1 for other_id, other_value in pool.items() if other_id != entity_id and other_value > value)
    return better + 1


def _positive_pool(pool: Mapping[str, float]) -> dict[str, float]:
    return {str(entity_id): float(value) for entity_id, value in pool.items() if value is not None and float(value) > 0}


def _crossed_thresholds(prev_rank: int | None, new_rank: int | None, thresholds: Sequence[int]) -> list[int]:
    if new_rank is None:
        return []
    return [threshold for threshold in thresholds if new_rank <= threshold and (prev_rank is None or prev_rank > threshold)]


def _normalize_metric_key_for_season(metric_key: str, season: str) -> str:
    if is_career_season(season) and family_base_key(metric_key) == metric_key:
        return family_window_key(metric_key, "career")
    return metric_key


def _applicable_additive_metrics(
    session: Session,
    metric_keys: Sequence[str] | None = None,
) -> list:
    metrics_by_key = {metric.key: metric for metric in get_all_metrics(session=session)}
    if metric_keys:
        candidates = [metrics_by_key.get(str(key)) or get_metric(str(key), session=session) for key in metric_keys]
    else:
        candidates = list(metrics_by_key.values())
    out = []
    seen: set[str] = set()
    for metric in candidates:
        if metric is None or metric.key in seen:
            continue
        if not bool(getattr(metric, "additive_accumulator", False)):
            continue
        if getattr(metric, "rank_order", "desc") != "desc":
            continue
        if getattr(metric, "scope", None) not in {"player", "team"}:
            continue
        if not getattr(metric, "approaching_thresholds", None):
            continue
        seen.add(metric.key)
        out.append(metric)
    return out


def _metric_season_pairs(
    session: Session,
    game: Game,
    metrics: Sequence,
    seasons: Sequence[str] | None,
) -> list[tuple[object, str]]:
    if seasons:
        season_values = [str(season) for season in seasons if season]
    else:
        season_values = [str(game.season)] if game.season else []
        career_season = career_season_for(str(game.season or ""))
        if career_season:
            season_values.append(career_season)

    pairs: list[tuple[object, str]] = []
    seen: set[tuple[str, str]] = set()
    for season in season_values:
        for metric in metrics:
            if is_career_season(season):
                if not (getattr(metric, "career", False) or getattr(metric, "supports_career", False)):
                    continue
                metric_key = _normalize_metric_key_for_season(metric.key, season)
                metric_for_season = get_metric(metric_key, session=session)
            else:
                if getattr(metric, "career", False):
                    continue
                metric_key = metric.key
                metric_for_season = metric
            pair_key = (metric_key, season)
            if pair_key in seen:
                continue
            seen.add(pair_key)
            pairs.append((metric_for_season or metric, season))
    return pairs


def _load_current_metric_pool(session: Session, metric_key: str, season: str, entity_type: str) -> dict[str, float]:
    rows = (
        session.query(MetricResult.entity_id, MetricResult.value_num)
        .filter(
            MetricResult.metric_key == metric_key,
            MetricResult.entity_type == entity_type,
            MetricResult.season == season,
            MetricResult.value_num.isnot(None),
        )
        .all()
    )
    return _positive_pool({str(entity_id): _clean_number(value) for entity_id, value in rows if entity_id})


class BoxScoreSliceProvider:
    """Single-game provider backed by box-score slices before that game."""

    def __init__(self, game: Game) -> None:
        self._game = game

    def current_pool(self, session: Session, metric_key: str, season: str, entity_type: str) -> dict[str, float]:
        del entity_type
        metric = get_metric(metric_key, session=session)
        if metric is None:
            return {}
        return aggregate_pool_as_of(
            session,
            metric,
            season,
            cutoff_game_date=self._game.game_date,
            cutoff_game_id=self._game.game_id,
        )

    def __call__(
        self,
        session: Session,
        game_id: str,
        metric_key: str,
        season: str,
        entity_type: str,
        deltas: Mapping[str, float],
        current_pool: Mapping[str, float],
    ) -> dict[str, float]:
        del session, game_id, metric_key, season, entity_type
        return {str(entity_id): _clean_number(current_pool.get(str(entity_id), 0.0)) for entity_id, delta in deltas.items()}


class InMemoryBatchProvider:
    """Chronological provider backed by in-memory running totals."""

    def __init__(self, *, event_lookup_authoritative: bool = False) -> None:
        self._totals: dict[tuple[str, str], dict[str, float]] = {}
        self._games: dict[tuple[str, str], dict[str, int]] = {}
        self._min_samples: dict[tuple[str, str], int] = {}
        self._emitted_events: set[tuple[str, str, str, str, str, str]] = set()
        self.event_lookup_authoritative = bool(event_lookup_authoritative)

    def seed(
        self,
        metric_key: str,
        season: str,
        pool: Mapping[str, float],
        *,
        min_sample: int = 1,
    ) -> None:
        key = (metric_key, season)
        self._totals[key] = {str(entity_id): _clean_number(value) for entity_id, value in pool.items()}
        self._games[key] = {str(entity_id): max(int(min_sample or 1), 1) for entity_id in pool}
        self._min_samples[key] = max(int(min_sample or 1), 1)

    def record_game_deltas(
        self,
        session: Session,
        game_id: str,
        metric_key: str,
        season: str,
        deltas: Mapping[str, float],
        game_counts: Mapping[str, int],
        min_sample: int,
    ) -> None:
        del session, game_id
        key = (metric_key, season)
        totals = self._totals.setdefault(key, {})
        games = self._games.setdefault(key, {})
        self._min_samples[key] = max(int(min_sample or 1), 1)
        for entity_id, count in game_counts.items():
            entity = str(entity_id)
            totals[entity] = totals.get(entity, 0.0) + _clean_number(deltas.get(entity, 0.0))
            games[entity] = games.get(entity, 0) + int(count or 0)

    def current_pool(self, session: Session, metric_key: str, season: str, entity_type: str) -> dict[str, float]:
        del session
        del entity_type
        key = (metric_key, season)
        min_sample = self._min_samples.get(key, 1)
        games = self._games.get(key, {})
        return _positive_pool(
            {
                entity_id: value
                for entity_id, value in self._totals.get(key, {}).items()
                if games.get(entity_id, 0) >= min_sample
            }
        )

    def __call__(
        self,
        session: Session,
        game_id: str,
        metric_key: str,
        season: str,
        entity_type: str,
        deltas: Mapping[str, float],
        current_pool: Mapping[str, float],
    ) -> dict[str, float]:
        del session, game_id, metric_key, season, entity_type
        return {str(entity_id): _clean_number(current_pool.get(str(entity_id), 0.0)) for entity_id, delta in deltas.items()}

    def event_already_emitted(
        self,
        metric_key: str,
        entity_type: str,
        entity_id: str,
        season: str,
        event_type: str,
        event_key: str,
    ) -> bool:
        return (metric_key, entity_type, entity_id, season, event_type, event_key) in self._emitted_events

    def record_emitted_event(
        self,
        metric_key: str,
        entity_type: str,
        entity_id: str,
        season: str,
        event_type: str,
        event_key: str,
    ) -> None:
        self._emitted_events.add((metric_key, entity_type, entity_id, season, event_type, event_key))

    def warm_existing_events(
        self,
        session: Session,
        *,
        season: str,
        metric_keys: Sequence[str] | None = None,
    ) -> int:
        query = session.query(
            MetricMilestone.metric_key,
            MetricMilestone.entity_type,
            MetricMilestone.entity_id,
            MetricMilestone.season,
            MetricMilestone.event_type,
            MetricMilestone.event_key,
        ).filter(MetricMilestone.season == season)
        if metric_keys:
            query = query.filter(MetricMilestone.metric_key.in_(tuple(metric_keys)))
        rows = query.all()
        for row in rows:
            self._emitted_events.add(tuple(str(value) for value in row))
        return len(rows)

def _current_pool_from_provider(
    provider: MilestoneValueProvider,
    session: Session,
    metric_key: str,
    season: str,
    entity_type: str,
) -> dict[str, float]:
    current_pool_fn = getattr(provider, "current_pool", None)
    if callable(current_pool_fn):
        return _positive_pool(current_pool_fn(session, metric_key, season, entity_type))
    return _load_current_metric_pool(session, metric_key, season, entity_type)


def _record_provider_game(
    provider: MilestoneValueProvider,
    session: Session,
    game_id: str,
    metric_key: str,
    season: str,
    deltas: Mapping[str, float],
    game_counts: Mapping[str, int],
    min_sample: int,
) -> None:
    record_fn = getattr(provider, "record_game_deltas", None)
    if callable(record_fn):
        record_fn(session, game_id, metric_key, season, deltas, game_counts, min_sample)


SEASON_CROSSING_VALUE_FLOORS: dict[str, float] = {
    "season_total": 30.0,
    "count_threshold": 3.0,
    "count_combo": 2.0,
    "count_exact": 2.0,
    "games_played": 5.0,
    "games_started": 5.0,
    "count": 3.0,
}
RANK_PRESTIGE_POOL_FULL_SCALE = 20  # pools at or above this size are not attenuated
RANK_PRESTIGE_POOL_MIN_SCALE = 0.5  # floor for pathological tiny pools (tests, synthetic data)


def _rank_prestige(rank: int | None, pool_size: int | None = None) -> float:
    """Return a rank-position prestige score.

    The value-floor is the primary noise filter for in-season crossings; this
    function just caps severity in extremely tiny pools (<20 entities, usually
    synthetic/test data) so a rank of #1 there can't mint a hero-tier event.
    Realistic NBA pools (30+ players in a playoff round, hundreds league-wide,
    thousands career) are unaffected.
    """
    if rank is None:
        return 0.0
    rank = int(rank)
    if rank == 1:
        base = 0.95
    elif rank <= 5:
        base = 0.85
    elif rank <= 10:
        base = 0.70
    elif rank <= 25:
        base = 0.50
    elif rank <= 100:
        base = 0.30
    else:
        base = 0.10
    if pool_size is None or pool_size >= RANK_PRESTIGE_POOL_FULL_SCALE:
        return base
    scale = max(RANK_PRESTIGE_POOL_MIN_SCALE, float(pool_size) / RANK_PRESTIGE_POOL_FULL_SCALE)
    return base * scale


def _season_crossing_value_floor(metric) -> float:
    """Minimum meaningful target value to emit a season-scope rank_crossing.

    Prevents the early-playoffs cascade where a player with 1 game "crosses"
    dozens of tied players with 0-1 games. Overridable per-metric by setting
    `season_crossing_min_target_value`.
    """
    explicit = getattr(metric, "season_crossing_min_target_value", None)
    if explicit is not None:
        try:
            return float(explicit)
        except (TypeError, ValueError):
            pass
    kind = str(_metric_attr(metric, "metric_kind", "") or "")
    return SEASON_CROSSING_VALUE_FLOORS.get(kind, 0.0)


def _player_payloads(session: Session, player_ids: Sequence[str]) -> dict[str, dict]:
    ids = {str(player_id) for player_id in player_ids if player_id}
    if not ids:
        return {}
    payloads: dict[str, dict] = {}
    rows = (
        session.query(Player.player_id, Player.full_name, Player.full_name_zh)
        .filter(Player.player_id.in_(ids))
        .all()
    )
    for player_id, full_name, full_name_zh in rows:
        entity_id = str(player_id)
        name_en = full_name or entity_id
        payloads[entity_id] = {
            "player_id": entity_id,
            "name_en": name_en,
            "name_zh": full_name_zh or name_en,
        }
    return payloads


def _entity_payloads(
    session: Session,
    entity_type: str,
    entity_ids: Sequence[str],
) -> dict[str, dict]:
    ids = {str(entity_id) for entity_id in entity_ids if entity_id}
    if not ids:
        return {}
    if entity_type == "player":
        return _player_payloads(session, list(ids))

    payloads: dict[str, dict] = {}
    for team_id, abbr, full_name, full_name_zh in (
        session.query(Team.team_id, Team.abbr, Team.full_name, Team.full_name_zh)
        .filter(Team.team_id.in_(ids))
        .all()
    ):
        entity_id = str(team_id)
        name_en = full_name or abbr or entity_id
        payloads[entity_id] = {
            "team_id": entity_id,
            "name_en": name_en,
            "name_zh": full_name_zh or name_en,
            "abbr": abbr,
        }
    return payloads


def _target_player_id(entity_id: str, new_value: float, current_pool: Mapping[str, float]) -> str | None:
    better = [
        (other_id, value)
        for other_id, value in current_pool.items()
        if other_id != entity_id and value > new_value
    ]
    if not better:
        return None
    better.sort(key=lambda item: (item[1], item[0]))
    return better[0][0]


def _gap_tightness(gap: float | int | None) -> float:
    if gap is None:
        return 0.3
    value = max(float(gap), 0.0)
    if value == 0:
        return 0.95
    if value <= 1:
        return 0.85
    if value <= 3:
        return 0.75
    if value <= 5:
        return 0.70
    if value <= 10:
        return 0.50
    if value <= 20:
        return 0.30
    if value <= 30:
        return 0.20
    return 0.10


def _absolute_threshold_severity(threshold: float, current_pool: Mapping[str, float]) -> float:
    count_reached = sum(1 for value in current_pool.values() if value is not None and float(value) >= threshold)
    if count_reached <= 3:
        return 0.95
    if count_reached <= 10:
        return 0.85
    if count_reached <= 25:
        return 0.70
    if count_reached <= 100:
        return 0.50
    return 0.35


def _approaching_absolute_severity(
    threshold: float,
    gap: float,
    current_pool: Mapping[str, float],
) -> float:
    return _absolute_threshold_severity(threshold, current_pool) * _gap_tightness(gap)


def _threshold_label(value: float | int, stat_label: str, stat_label_en: str) -> tuple[str, str]:
    number = int(value) if float(value).is_integer() else float(value)
    if isinstance(number, int):
        if stat_label == "得分":
            if number == 1000:
                return "千分", "1K points"
            if number == 10000:
                return "万分", "10K points"
    return f"{number}次{stat_label}", f"{number} {stat_label_en}"


def _threshold_labels(prev_rank: int | None, new_rank: int | None, thresholds: Sequence[int]) -> list[str]:
    return [f"top_{threshold}" for threshold in _crossed_thresholds(prev_rank, new_rank, thresholds)]


def _event_already_emitted(
    session: Session,
    provider: MilestoneValueProvider,
    *,
    metric_key: str,
    entity_type: str,
    entity_id: str,
    season: str,
    game_id: str,
    event_type: str,
    event_key: str,
) -> bool:
    provider_lookup = getattr(provider, "event_already_emitted", None)
    if callable(provider_lookup):
        if provider_lookup(metric_key, entity_type, entity_id, season, event_type, event_key):
            return True
        if bool(getattr(provider, "event_lookup_authoritative", False)):
            return False
    row = (
        session.query(MetricMilestone.game_id)
        .filter(
            MetricMilestone.metric_key == metric_key,
            MetricMilestone.entity_type == entity_type,
            MetricMilestone.entity_id == entity_id,
            MetricMilestone.season == season,
            MetricMilestone.event_type == event_type,
            MetricMilestone.event_key == event_key,
        )
        .first()
    )
    return row is not None and str(row.game_id) != str(game_id)


def _record_emitted_event(
    provider: MilestoneValueProvider,
    *,
    metric_key: str,
    entity_type: str,
    entity_id: str,
    season: str,
    event_type: str,
    event_key: str,
) -> None:
    record = getattr(provider, "record_emitted_event", None)
    if callable(record):
        record(metric_key, entity_type, entity_id, season, event_type, event_key)


def _crossing_event_key(target_id: str) -> str:
    return f"cross_{target_id}"


def _approaching_event_key(target_id: str, threshold: int) -> str:
    return f"approach_{target_id}_thr{int(threshold)}"


def _absolute_event_key(threshold: float | int) -> str:
    return f"reach_{int(threshold)}"


def _approaching_absolute_event_key(abs_threshold: float | int, gap_threshold: float | int) -> str:
    return f"approach_abs_{int(abs_threshold)}_thr{int(gap_threshold)}"


def _fallback_entity_payload(entity_type: str, entity_id: str) -> dict:
    key = "team_id" if entity_type == "team" else "player_id"
    return {key: entity_id, "name_en": entity_id, "name_zh": entity_id}


def _build_crossing_event(
    *,
    session: Session,
    game_id: str,
    metric_key: str,
    season: str,
    entity_type: str,
    stat_label: str,
    stat_label_en: str,
    entity_id: str,
    delta: float,
    prev_value: float,
    new_value: float,
    prev_rank: int | None,
    new_rank: int | None,
    rank_thresholds: Sequence[int],
    crossed_target_id: str,
    target_id: str | None,
    prev_pool: Mapping[str, float],
    current_pool: Mapping[str, float],
) -> dict | None:
    crossed_rank_labels = _threshold_labels(prev_rank, new_rank, rank_thresholds)
    related_ids = {crossed_target_id}
    if target_id:
        related_ids.add(target_id)
    related_ids.add(entity_id)

    payloads = _entity_payloads(session, entity_type, list(related_ids))

    passed_payload = dict(payloads.get(crossed_target_id) or _fallback_entity_payload(entity_type, crossed_target_id))
    passed_payload["value"] = _display_number(current_pool.get(crossed_target_id, prev_pool.get(crossed_target_id)))
    passed_payload["prev_value"] = _display_number(prev_pool.get(crossed_target_id))
    passed_payload["prev_rank"] = _rank_in_pool(prev_pool, crossed_target_id)
    passed_payload["new_rank"] = _rank_in_pool(current_pool, crossed_target_id)
    passed_payloads = [passed_payload]

    target_payload = None
    if target_id:
        target_payload = dict(payloads.get(target_id) or _fallback_entity_payload(entity_type, target_id))
        target_value = current_pool.get(target_id)
        target_payload["value"] = _display_number(target_value)
        target_payload["gap"] = _display_number((target_value - new_value) if target_value is not None else None)
        target_payload["rank"] = _rank_in_pool(current_pool, target_id)

    pool_size = len(current_pool) if current_pool else None
    severity = max(
        _rank_prestige(new_rank, pool_size=pool_size),
        _rank_prestige(passed_payload.get("new_rank"), pool_size=pool_size),
    )

    if severity < SEVERITY_MIN_EMIT:
        return None

    entity_payload = payloads.get(entity_id) or _fallback_entity_payload(entity_type, entity_id)
    context = {
        "source": "milestone",
        "event_type": RANK_CROSSING_EVENT_TYPE,
        "game_delta": _display_number(delta),
        "stat_label": stat_label,
        "stat_label_en": stat_label_en,
        "player": entity_payload,
        "passed": passed_payloads,
        "target": target_payload,
        "thresholds_crossed": crossed_rank_labels,
        "threshold_crossed": crossed_rank_labels[0] if crossed_rank_labels else None,
        "rank_shift": f"{prev_rank if prev_rank is not None else '?'}->{new_rank if new_rank is not None else '?'}",
    }
    return {
        "metric_key": metric_key,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "season": season,
        "game_id": game_id,
        "event_type": RANK_CROSSING_EVENT_TYPE,
        "event_key": _crossing_event_key(crossed_target_id),
        "prev_rank": prev_rank,
        "new_rank": new_rank,
        "prev_value": prev_value,
        "new_value": new_value,
        "value_delta": delta,
        "thresholds": crossed_rank_labels,
        "passed": passed_payloads,
        "target": target_payload,
        "context": context,
        "severity": round(float(severity), 4),
    }


def _build_approaching_event(
    *,
    session: Session,
    game_id: str,
    metric_key: str,
    season: str,
    entity_type: str,
    stat_label: str,
    stat_label_en: str,
    entity_id: str,
    delta: float,
    prev_value: float,
    new_value: float,
    prev_rank: int | None,
    new_rank: int | None,
    target_id: str,
    threshold: int,
    approaching_thresholds: Sequence[int],
    prev_pool: Mapping[str, float],
    current_pool: Mapping[str, float],
) -> dict | None:
    related_ids = {entity_id, target_id}
    payloads = _entity_payloads(session, entity_type, list(related_ids))

    target_payload = dict(payloads.get(target_id) or _fallback_entity_payload(entity_type, target_id))
    target_value = current_pool.get(target_id, prev_pool.get(target_id))
    prev_gap = _clean_number(prev_pool.get(target_id)) - prev_value
    new_gap = _clean_number(target_value) - new_value
    target_payload["value"] = _display_number(target_value)
    target_payload["prev_value"] = _display_number(prev_pool.get(target_id))
    target_payload["gap"] = _display_number(new_gap)
    target_payload["prev_gap"] = _display_number(prev_gap)
    target_payload["rank"] = _rank_in_pool(current_pool, target_id)

    pool_size = len(current_pool) if current_pool else None
    severity = _rank_prestige(target_payload.get("rank"), pool_size=pool_size) * _gap_tightness(new_gap)
    if severity < SEVERITY_MIN_EMIT:
        return None

    entity_payload = payloads.get(entity_id) or _fallback_entity_payload(entity_type, entity_id)
    threshold_label = f"gap_{int(threshold)}"
    context = {
        "source": "milestone",
        "event_type": APPROACHING_EVENT_TYPE,
        "game_delta": _display_number(delta),
        "stat_label": stat_label,
        "stat_label_en": stat_label_en,
        "player": entity_payload,
        "passed": [],
        "target": target_payload,
        "thresholds_crossed": [threshold_label],
        "threshold_crossed": threshold_label,
        "rank_shift": f"{prev_rank if prev_rank is not None else '?'}->{new_rank if new_rank is not None else '?'}",
    }
    return {
        "metric_key": metric_key,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "season": season,
        "game_id": game_id,
        "event_type": APPROACHING_EVENT_TYPE,
        "event_key": _approaching_event_key(target_id, threshold),
        "prev_rank": prev_rank,
        "new_rank": new_rank,
        "prev_value": prev_value,
        "new_value": new_value,
        "value_delta": delta,
        "thresholds": [threshold_label],
        "passed": [],
        "target": target_payload,
        "context": context,
        "severity": round(float(severity), 4),
    }


def _build_absolute_threshold_event(
    *,
    session: Session,
    game_id: str,
    metric_key: str,
    season: str,
    entity_type: str,
    stat_label: str,
    stat_label_en: str,
    entity_id: str,
    delta: float,
    prev_value: float,
    new_value: float,
    prev_rank: int | None,
    new_rank: int | None,
    threshold: float,
    current_pool: Mapping[str, float],
) -> dict | None:
    severity = _absolute_threshold_severity(threshold, current_pool)
    if severity < SEVERITY_MIN_EMIT:
        return None

    payloads = _entity_payloads(session, entity_type, [entity_id])
    entity_payload = payloads.get(entity_id) or _fallback_entity_payload(entity_type, entity_id)
    label_zh, label_en = _threshold_label(threshold, stat_label, stat_label_en)
    count_reached = sum(1 for value in current_pool.values() if value is not None and float(value) >= threshold)
    context = {
        "source": "milestone",
        "event_type": ABSOLUTE_THRESHOLD_EVENT_TYPE,
        "game_delta": _display_number(delta),
        "stat_label": stat_label,
        "stat_label_en": stat_label_en,
        "player": entity_payload,
        "passed": [],
        "target": None,
        "threshold_value": _display_number(threshold),
        "threshold_label_zh": label_zh,
        "threshold_label_en": label_en,
        "count_reached_before_this_game": max(int(count_reached) - 1, 0),
        "is_first_ever": count_reached <= 1,
        "rank_at_milestone": new_rank,
        "prev_value": _display_number(prev_value),
        "new_value": _display_number(new_value),
        "thresholds_crossed": [_display_number(threshold)],
        "threshold_crossed": _display_number(threshold),
    }
    return {
        "metric_key": metric_key,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "season": season,
        "game_id": game_id,
        "event_type": ABSOLUTE_THRESHOLD_EVENT_TYPE,
        "event_key": _absolute_event_key(threshold),
        "prev_rank": prev_rank,
        "new_rank": new_rank,
        "prev_value": prev_value,
        "new_value": new_value,
        "value_delta": delta,
        "thresholds": [_display_number(threshold)],
        "passed": [],
        "target": None,
        "context": context,
        "severity": round(float(severity), 4),
    }


def _build_approaching_absolute_event(
    *,
    session: Session,
    game_id: str,
    metric_key: str,
    season: str,
    entity_type: str,
    stat_label: str,
    stat_label_en: str,
    entity_id: str,
    delta: float,
    prev_value: float,
    new_value: float,
    prev_rank: int | None,
    new_rank: int | None,
    abs_threshold: float,
    gap_threshold: float,
    current_pool: Mapping[str, float],
) -> dict | None:
    new_gap = abs_threshold - new_value
    prev_gap = abs_threshold - prev_value
    severity = _approaching_absolute_severity(abs_threshold, new_gap, current_pool)
    if severity < SEVERITY_MIN_EMIT:
        return None

    payloads = _entity_payloads(session, entity_type, [entity_id])
    entity_payload = payloads.get(entity_id) or _fallback_entity_payload(entity_type, entity_id)
    label_zh, label_en = _threshold_label(abs_threshold, stat_label, stat_label_en)
    target_payload = {
        "threshold_value": _display_number(abs_threshold),
        "threshold_label_zh": label_zh,
        "threshold_label_en": label_en,
        "gap": _display_number(new_gap),
        "prev_gap": _display_number(prev_gap),
    }
    context = {
        "source": "milestone",
        "event_type": APPROACHING_ABSOLUTE_EVENT_TYPE,
        "game_delta": _display_number(delta),
        "stat_label": stat_label,
        "stat_label_en": stat_label_en,
        "player": entity_payload,
        "passed": [],
        "target": target_payload,
        "threshold_value": _display_number(abs_threshold),
        "threshold_label_zh": label_zh,
        "threshold_label_en": label_en,
        "gap_threshold_crossed": _display_number(gap_threshold),
        "prev_gap": _display_number(prev_gap),
        "new_gap": _display_number(new_gap),
        "prev_value": _display_number(prev_value),
        "new_value": _display_number(new_value),
        "thresholds_crossed": [_display_number(gap_threshold)],
        "threshold_crossed": _display_number(gap_threshold),
    }
    return {
        "metric_key": metric_key,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "season": season,
        "game_id": game_id,
        "event_type": APPROACHING_ABSOLUTE_EVENT_TYPE,
        "event_key": _approaching_absolute_event_key(abs_threshold, gap_threshold),
        "prev_rank": prev_rank,
        "new_rank": new_rank,
        "prev_value": prev_value,
        "new_value": new_value,
        "value_delta": delta,
        "thresholds": [_display_number(gap_threshold)],
        "passed": [],
        "target": target_payload,
        "context": context,
        "severity": round(float(severity), 4),
    }


def _persist_milestone_events(session: Session, events: Sequence[dict]) -> None:
    now = _now_naive_utc()
    for event in events:
        filters = dict(
            metric_key=event["metric_key"],
            entity_type=event["entity_type"],
            entity_id=event["entity_id"],
            season=event["season"],
            game_id=event["game_id"],
            event_type=event["event_type"],
            event_key=event["event_key"],
        )
        row = session.query(MetricMilestone).filter_by(**filters).one_or_none()
        if row is None:
            row = MetricMilestone(**filters)
            session.add(row)
        row.prev_rank = event["prev_rank"]
        row.new_rank = event["new_rank"]
        row.prev_value = event["prev_value"]
        row.new_value = event["new_value"]
        row.value_delta = event["value_delta"]
        row.thresholds_json = _json_dumps(event["thresholds"])
        row.passed_json = _json_dumps(event["passed"])
        row.target_json = _json_dumps(event["target"])
        row.context_json = _json_dumps(event["context"])
        row.severity = event["severity"]
        row.computed_at = now


def detect_milestones_for_metric(
    session: Session,
    game_id: str,
    metric_key: str,
    season: str,
    *,
    prev_values_provider: MilestoneValueProvider,
    thresholds: Sequence[int] = DEFAULT_RANK_THRESHOLDS,
) -> list[dict]:
    metric = get_metric(metric_key, session=session)
    if metric is None:
        logger.info("milestone detector skipped unknown metric %s", metric_key)
        return []
    if not bool(getattr(metric, "additive_accumulator", False)):
        logger.info("milestone detector refused non-additive metric %s", metric_key)
        return []
    if getattr(metric, "rank_order", "desc") != "desc":
        logger.info("milestone detector refused non-desc additive metric %s", metric_key)
        return []
    approaching_thresholds = sorted(
        {int(threshold) for threshold in (getattr(metric, "approaching_thresholds", None) or [])}
    )
    if not approaching_thresholds:
        logger.info("milestone detector refused additive metric without approach thresholds %s", metric_key)
        return []
    absolute_thresholds = sorted({float(threshold) for threshold in (getattr(metric, "absolute_thresholds", None) or [])})
    absolute_approach_thresholds = sorted({float(threshold) for threshold in (getattr(metric, "absolute_approach_thresholds", None) or [])})

    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        return []

    entity_type = str(getattr(metric, "scope", "") or "")
    if entity_type not in {"player", "team"}:
        logger.info("milestone detector refused unsupported entity scope %s for %s", entity_type, metric_key)
        return []
    stat_field = _metric_attr(metric, "value_field", None) or _metric_attr(metric, "stat_field", None)
    stat_label, stat_label_en = STAT_LABELS.get(str(stat_field or ""), (getattr(metric, "name_zh", None) or getattr(metric, "name", metric_key), getattr(metric, "name", metric_key)))

    # Non-career (concrete season) pools suppress crossings against targets
    # with trivially small pre-game values. Career/alltime pools don't need
    # this because their values are always meaningful.
    season_value_floor = 0.0 if is_career_season(season) else _season_crossing_value_floor(metric)

    all_deltas, game_counts = _load_game_metric_deltas(session, game, metric)
    event_deltas = {player_id: delta for player_id, delta in all_deltas.items() if delta > 0}
    if not event_deltas:
        _record_provider_game(
            prev_values_provider,
            session,
            game_id,
            metric_key,
            season,
            all_deltas,
            game_counts,
            int(getattr(metric, "min_sample", 1) or 1),
        )
        return []

    prev_pool = _current_pool_from_provider(prev_values_provider, session, metric_key, season, entity_type)
    prev_pool = _positive_pool(prev_pool)
    prev_values = prev_values_provider(session, game_id, metric_key, season, entity_type, event_deltas, prev_pool)
    current_pool = dict(prev_pool)
    for entity_id, delta in event_deltas.items():
        current_pool[str(entity_id)] = _clean_number(prev_pool.get(str(entity_id), 0.0)) + _clean_number(delta)
    current_pool = _positive_pool(current_pool)

    events: list[dict] = []
    for player_id, delta in event_deltas.items():
        new_value = _clean_number(current_pool.get(player_id))
        if new_value <= 0:
            continue
        prev_value = _clean_number(prev_values.get(player_id, prev_pool.get(player_id, 0.0)))
        if new_value <= prev_value:
            continue
        prev_rank = _rank_in_pool(prev_pool, player_id)
        new_rank = _rank_in_pool(current_pool, player_id)

        for target_id in sorted(set(prev_pool) | set(current_pool)):
            try:
                if target_id == player_id:
                    continue
                target_prev_value = _clean_number(prev_pool.get(target_id, current_pool.get(target_id)))
                target_new_value = _clean_number(current_pool.get(target_id, prev_pool.get(target_id)))
                if target_prev_value <= 0 and target_new_value <= 0:
                    continue
                if season_value_floor > 0 and target_prev_value < season_value_floor:
                    continue

                crossed_target = prev_value <= target_prev_value and new_value > target_new_value
                if crossed_target:
                    event_key = _crossing_event_key(target_id)
                    if _event_already_emitted(
                        session,
                        prev_values_provider,
                        metric_key=metric_key,
                        entity_type=entity_type,
                        entity_id=player_id,
                        season=season,
                        game_id=game_id,
                        event_type=RANK_CROSSING_EVENT_TYPE,
                        event_key=event_key,
                    ):
                        continue
                    event = _build_crossing_event(
                        session=session,
                        game_id=game_id,
                        metric_key=metric_key,
                        season=season,
                        entity_type=entity_type,
                        stat_label=stat_label,
                        stat_label_en=stat_label_en,
                        entity_id=player_id,
                        delta=delta,
                        prev_value=prev_value,
                        new_value=new_value,
                        prev_rank=prev_rank,
                        new_rank=new_rank,
                        rank_thresholds=thresholds,
                        crossed_target_id=target_id,
                        target_id=_target_player_id(player_id, new_value, current_pool),
                        prev_pool=prev_pool,
                        current_pool=current_pool,
                    )
                    if event is not None:
                        events.append(event)
                        _record_emitted_event(
                            prev_values_provider,
                            metric_key=metric_key,
                            entity_type=entity_type,
                            entity_id=player_id,
                            season=season,
                            event_type=RANK_CROSSING_EVENT_TYPE,
                            event_key=event_key,
                        )
                    continue

                prev_gap = target_prev_value - prev_value
                new_gap = target_new_value - new_value
                if prev_gap <= 0 or new_gap < 0:
                    continue
                for approach_threshold in approaching_thresholds:
                    if not (prev_gap > approach_threshold and new_gap <= approach_threshold):
                        continue
                    event_key = _approaching_event_key(target_id, approach_threshold)
                    if _event_already_emitted(
                        session,
                        prev_values_provider,
                        metric_key=metric_key,
                        entity_type=entity_type,
                        entity_id=player_id,
                        season=season,
                        game_id=game_id,
                        event_type=APPROACHING_EVENT_TYPE,
                        event_key=event_key,
                    ):
                        continue
                    event = _build_approaching_event(
                        session=session,
                        game_id=game_id,
                        metric_key=metric_key,
                        season=season,
                        entity_type=entity_type,
                        stat_label=stat_label,
                        stat_label_en=stat_label_en,
                        entity_id=player_id,
                        delta=delta,
                        prev_value=prev_value,
                        new_value=new_value,
                        prev_rank=prev_rank,
                        new_rank=new_rank,
                        target_id=target_id,
                        threshold=approach_threshold,
                        approaching_thresholds=approaching_thresholds,
                        prev_pool=prev_pool,
                        current_pool=current_pool,
                    )
                    if event is not None:
                        events.append(event)
                        _record_emitted_event(
                            prev_values_provider,
                            metric_key=metric_key,
                            entity_type=entity_type,
                            entity_id=player_id,
                            season=season,
                            event_type=APPROACHING_EVENT_TYPE,
                            event_key=event_key,
                        )
                    break
            except Exception:
                logger.exception("milestone %s failed for %s %s", metric_key, player_id, game_id)
                continue

        for abs_threshold in absolute_thresholds:
            try:
                if not (prev_value < abs_threshold <= new_value):
                    continue
                event_key = _absolute_event_key(abs_threshold)
                if _event_already_emitted(
                    session,
                    prev_values_provider,
                    metric_key=metric_key,
                    entity_type=entity_type,
                    entity_id=player_id,
                    season=season,
                    game_id=game_id,
                    event_type=ABSOLUTE_THRESHOLD_EVENT_TYPE,
                    event_key=event_key,
                ):
                    continue
                event = _build_absolute_threshold_event(
                    session=session,
                    game_id=game_id,
                    metric_key=metric_key,
                    season=season,
                    entity_type=entity_type,
                    stat_label=stat_label,
                    stat_label_en=stat_label_en,
                    entity_id=player_id,
                    delta=delta,
                    prev_value=prev_value,
                    new_value=new_value,
                    prev_rank=prev_rank,
                    new_rank=new_rank,
                    threshold=abs_threshold,
                    current_pool=current_pool,
                )
                if event is not None:
                    events.append(event)
                    _record_emitted_event(
                        prev_values_provider,
                        metric_key=metric_key,
                        entity_type=entity_type,
                        entity_id=player_id,
                        season=season,
                        event_type=ABSOLUTE_THRESHOLD_EVENT_TYPE,
                        event_key=event_key,
                    )
            except Exception:
                logger.exception("absolute milestone %s failed for %s %s", metric_key, player_id, game_id)
                continue

        for abs_threshold in absolute_thresholds:
            if new_value >= abs_threshold:
                continue
            prev_gap = abs_threshold - prev_value
            new_gap = abs_threshold - new_value
            if prev_gap <= 0:
                continue
            for gap_threshold in absolute_approach_thresholds:
                try:
                    if not (prev_gap > gap_threshold >= new_gap):
                        continue
                    event_key = _approaching_absolute_event_key(abs_threshold, gap_threshold)
                    if _event_already_emitted(
                        session,
                        prev_values_provider,
                        metric_key=metric_key,
                        entity_type=entity_type,
                        entity_id=player_id,
                        season=season,
                        game_id=game_id,
                        event_type=APPROACHING_ABSOLUTE_EVENT_TYPE,
                        event_key=event_key,
                    ):
                        continue
                    event = _build_approaching_absolute_event(
                        session=session,
                        game_id=game_id,
                        metric_key=metric_key,
                        season=season,
                        entity_type=entity_type,
                        stat_label=stat_label,
                        stat_label_en=stat_label_en,
                        entity_id=player_id,
                        delta=delta,
                        prev_value=prev_value,
                        new_value=new_value,
                        prev_rank=prev_rank,
                        new_rank=new_rank,
                        abs_threshold=abs_threshold,
                        gap_threshold=gap_threshold,
                        current_pool=current_pool,
                    )
                    if event is not None:
                        events.append(event)
                        _record_emitted_event(
                            prev_values_provider,
                            metric_key=metric_key,
                            entity_type=entity_type,
                            entity_id=player_id,
                            season=season,
                            event_type=APPROACHING_ABSOLUTE_EVENT_TYPE,
                            event_key=event_key,
                        )
                    break
                except Exception:
                    logger.exception("approaching absolute milestone %s failed for %s %s", metric_key, player_id, game_id)
                    continue

    _persist_milestone_events(session, events)
    _record_provider_game(
        prev_values_provider,
        session,
        game_id,
        metric_key,
        season,
        all_deltas,
        game_counts,
        int(getattr(metric, "min_sample", 1) or 1),
    )
    return events


def detect_milestones_for_game(
    session: Session,
    game_id: str,
    *,
    prev_values_provider: MilestoneValueProvider,
    metric_keys: Sequence[str] | None = None,
    seasons: Sequence[str] | None = None,
    thresholds: Sequence[int] = DEFAULT_RANK_THRESHOLDS,
) -> list[dict]:
    game = session.query(Game).filter(Game.game_id == game_id).first()
    if game is None:
        logger.info("milestone detector skipped missing game %s", game_id)
        return []

    metrics = _applicable_additive_metrics(session, metric_keys)
    events: list[dict] = []
    for metric, season in _metric_season_pairs(session, game, metrics, seasons):
        try:
            events.extend(
                detect_milestones_for_metric(
                    session,
                    game_id,
                    metric.key,
                    season,
                    prev_values_provider=prev_values_provider,
                    thresholds=thresholds,
                )
            )
        except Exception:
            logger.exception("milestone detection failed %s %s", game_id, getattr(metric, "key", None))
            continue
    return events


def detect_batch_incremental(
    session: Session,
    game_ids: Sequence[str],
    *,
    metric_keys: Sequence[str] | None = None,
) -> list[dict]:
    """Detect milestones for a batch of games in chronological order."""
    game_id_values = [str(game_id) for game_id in game_ids if game_id]
    if not game_id_values:
        return []
    games = (
        session.query(Game)
        .filter(Game.game_id.in_(game_id_values))
        .order_by(Game.game_date.asc(), Game.game_id.asc())
        .all()
    )
    if not games:
        return []

    provider = InMemoryBatchProvider()
    metrics = _applicable_additive_metrics(session, metric_keys)
    seeded: set[tuple[str, str]] = set()
    for game in games:
        for metric, season in _metric_season_pairs(session, game, metrics, None):
            key = (metric.key, season)
            if key in seeded:
                continue
            pool = aggregate_pool_as_of(
                session,
                metric,
                season,
                cutoff_game_date=game.game_date,
                cutoff_game_id=game.game_id,
            )
            provider.seed(metric.key, season, pool, min_sample=int(getattr(metric, "min_sample", 1) or 1))
            seeded.add(key)

    events: list[dict] = []
    for game in games:
        events.extend(
            detect_milestones_for_game(
                session,
                game.game_id,
                prev_values_provider=provider,
                metric_keys=metric_keys,
            )
        )
    return events
