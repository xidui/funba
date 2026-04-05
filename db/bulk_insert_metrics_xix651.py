from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from textwrap import dedent

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.orm import sessionmaker

from db.models import MetricDefinition as MetricDefinitionModel, engine
from metrics.framework.runtime import load_code_metric

SessionLocal = sessionmaker(bind=engine)

SCRIPT_MARKER = "xix651_bulk_insert_metrics_v1"
DEFAULT_LIMIT = 200


@dataclass(frozen=True)
class MetricSpec:
    key: str
    name: str
    name_zh: str
    description: str
    description_zh: str
    scope: str
    category: str
    group_key: str | None
    min_sample: int
    supports_career: bool
    max_results_per_season: int
    code_python: str
    context_label_template: str | None = None


PLAYER_STAT_META = {
    "pts": {"name": "Points", "zh": "得分", "abbr": "PTS"},
    "ast": {"name": "Assists", "zh": "助攻", "abbr": "AST"},
    "reb": {"name": "Rebounds", "zh": "篮板", "abbr": "REB"},
    "oreb": {"name": "Offensive Rebounds", "zh": "前场篮板", "abbr": "OREB"},
    "dreb": {"name": "Defensive Rebounds", "zh": "后场篮板", "abbr": "DREB"},
    "stl": {"name": "Steals", "zh": "抢断", "abbr": "STL"},
    "blk": {"name": "Blocks", "zh": "盖帽", "abbr": "BLK"},
    "tov": {"name": "Turnovers", "zh": "失误", "abbr": "TOV"},
    "min": {"name": "Minutes", "zh": "分钟", "abbr": "MIN"},
    "fgm": {"name": "Field Goals Made", "zh": "投篮命中", "abbr": "FGM"},
    "fga": {"name": "Field Goal Attempts", "zh": "投篮出手", "abbr": "FGA"},
    "fg3m": {"name": "Three-Pointers Made", "zh": "三分命中", "abbr": "3PM"},
    "fg3a": {"name": "Three-Point Attempts", "zh": "三分出手", "abbr": "3PA"},
    "ftm": {"name": "Free Throws Made", "zh": "罚球命中", "abbr": "FTM"},
    "fta": {"name": "Free Throw Attempts", "zh": "罚球出手", "abbr": "FTA"},
    "plus": {"name": "Plus-Minus", "zh": "正负值", "abbr": "+/-"},
    "pf": {"name": "Personal Fouls", "zh": "犯规", "abbr": "PF"},
}

TEAM_STAT_META = {
    "pts": {"name": "Points", "zh": "得分", "abbr": "PTS"},
    "ast": {"name": "Assists", "zh": "助攻", "abbr": "AST"},
    "reb": {"name": "Rebounds", "zh": "篮板", "abbr": "REB"},
    "oreb": {"name": "Offensive Rebounds", "zh": "前场篮板", "abbr": "OREB"},
    "dreb": {"name": "Defensive Rebounds", "zh": "后场篮板", "abbr": "DREB"},
    "stl": {"name": "Steals", "zh": "抢断", "abbr": "STL"},
    "blk": {"name": "Blocks", "zh": "盖帽", "abbr": "BLK"},
    "tov": {"name": "Turnovers", "zh": "失误", "abbr": "TOV"},
    "fgm": {"name": "Field Goals Made", "zh": "投篮命中", "abbr": "FGM"},
    "fga": {"name": "Field Goal Attempts", "zh": "投篮出手", "abbr": "FGA"},
    "fg3m": {"name": "Three-Pointers Made", "zh": "三分命中", "abbr": "3PM"},
    "fg3a": {"name": "Three-Point Attempts", "zh": "三分出手", "abbr": "3PA"},
    "ftm": {"name": "Free Throws Made", "zh": "罚球命中", "abbr": "FTM"},
    "fta": {"name": "Free Throw Attempts", "zh": "罚球出手", "abbr": "FTA"},
    "pf": {"name": "Personal Fouls", "zh": "犯规", "abbr": "PF"},
}

SHOT_ZONE_LABELS = {
    "restricted_area": ("Restricted Area", "合理冲撞区"),
    "paint_non_ra": ("Paint (Non-RA)", "油漆区非合理冲撞区"),
    "midrange": ("Mid-Range", "中距离"),
    "left_corner_three": ("Left Corner 3", "左侧底角三分"),
    "right_corner_three": ("Right Corner 3", "右侧底角三分"),
    "corner_three": ("Corner 3", "底角三分"),
    "above_break_three": ("Above-the-Break 3", "弧顶三分"),
    "backcourt": ("Backcourt", "后场"),
}

UNSUPPORTED_CANDIDATES = [
    {
        "key": "fourth_quarter_comeback_wins",
        "reason": "Needs reliable trailing-at-end-of-Q3 reconstruction across every historical line-score edge case. The current schema supports it, but the validation burden is too high for this bulk pass.",
    },
    {
        "key": "travel_back_to_back_distance",
        "reason": "Would require schedule/travel data that is not stored in the current database.",
    },
    {
        "key": "injury_adjusted_value_over_replacement",
        "reason": "Would require injury availability history that is not stored in the current database.",
    },
    {
        "key": "rest_advantage_win_rate",
        "reason": "Would require richer schedule/rest-day normalization than this ticket's no-architecture-change constraint allows.",
    },
    {
        "key": "clutch_usage_rate",
        "reason": "Needs possession-level lineup context that is not represented in the current tables.",
    },
]


def camelize(key: str) -> str:
    return "".join(part.capitalize() for part in key.split("_"))


def _player_label(field: str) -> str:
    return PLAYER_STAT_META[field]["name"]


def _player_label_zh(field: str) -> str:
    return PLAYER_STAT_META[field]["zh"]


def _team_label(field: str) -> str:
    return TEAM_STAT_META[field]["name"]


def _team_label_zh(field: str) -> str:
    return TEAM_STAT_META[field]["zh"]


def _default_player_box_career_keys(metric_kind: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    sum_keys = {
        "count_threshold": ("count", "games"),
        "count_combo": ("count", "games"),
        "count_exact": ("count", "games"),
        "games_played": ("count", "games"),
        "games_started": ("count", "games"),
        "per_game": ("total", "games"),
        "split_avg": ("total", "games"),
        "season_total": ("total", "games"),
        "per_36": ("total", "seconds", "games"),
        "ratio": ("numerator", "denominator", "games"),
        "win_pct": ("wins", "starts"),
    }.get(metric_kind, ())
    max_keys = {
        "streak": ("best_streak",),
    }.get(metric_kind, ())
    return sum_keys, max_keys, ()


def render_player_box_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    category: str,
    group_key: str | None,
    min_sample: int,
    supports_career: bool,
    rank_order: str = "desc",
    metric_kind: str,
    value_field: str | None = None,
    criteria: tuple[tuple[str, float], ...] = (),
    comparator: str = ">=",
    split_key: str | None = None,
    qualifier_field: str | None = None,
    qualifier_min: float | None = None,
    numerator_terms: tuple[tuple[str, float], ...] = (),
    denominator_terms: tuple[tuple[str, float], ...] = (),
    ratio_multiplier: float = 1.0,
    record_mode: str = "max",
    career_sum_keys: tuple[str, ...] = (),
    career_max_keys: tuple[str, ...] = (),
    career_min_keys: tuple[str, ...] = (),
    value_suffix: str = "",
    context_label_template: str | None = None,
) -> str:
    if supports_career:
        default_sum_keys, default_max_keys, default_min_keys = _default_player_box_career_keys(metric_kind)
        if not career_sum_keys:
            career_sum_keys = default_sum_keys
        if not career_max_keys:
            career_max_keys = default_max_keys
        if not career_min_keys:
            career_min_keys = default_min_keys
    else:
        career_sum_keys = ()
        career_max_keys = ()
        career_min_keys = ()

    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
        from db.models import Game, PlayerGameStats


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "player"
            category = {category!r}
            min_sample = {int(min_sample)}
            trigger = "season"
            incremental = False
            supports_career = {supports_career!r}
            rank_order = {rank_order!r}
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            context_label_template = {context_label_template!r}
            metric_kind = {metric_kind!r}
            value_field = {value_field!r}
            split_key = {split_key!r}
            comparator = {comparator!r}
            qualifier_field = {qualifier_field!r}
            qualifier_min = {qualifier_min!r}
            numerator_terms = {numerator_terms!r}
            denominator_terms = {denominator_terms!r}
            ratio_multiplier = {ratio_multiplier!r}
            criteria = {criteria!r}
            record_mode = {record_mode!r}
            career_aggregate_mode = {"season_results" if supports_career else None!r}
            career_sum_keys = {career_sum_keys!r}
            career_max_keys = {career_max_keys!r}
            career_min_keys = {career_min_keys!r}

            def _round(self, value, digits=4):
                return round(float(value), digits)

            def _minutes_played(self, row):
                return int(row.min or 0) * 60 + int(row.sec or 0)

            def _played(self, row):
                return self._minutes_played(row) > 0 or any(int(getattr(row, field, 0) or 0) != 0 for field in ("pts", "reb", "ast", "stl", "blk", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta"))

            def _season_games(self, session, season):
                query = session.query(Game)
                if is_career_season(season):
                    code = career_season_type_code(season)
                    if not code:
                        return []
                    query = query.filter(Game.season.like(f"{{code}}%"))
                else:
                    query = query.filter(Game.season == season)
                return query.order_by(Game.game_date.asc(), Game.game_id.asc()).all()

            def _compare(self, value, threshold):
                if self.comparator == "==":
                    return value == threshold
                if self.comparator == "<=":
                    return value <= threshold
                return value >= threshold

            def _criteria_met(self, row):
                if not self.criteria:
                    return True
                for field, threshold in self.criteria:
                    value = float(getattr(row, field, 0) or 0)
                    if not self._compare(value, threshold):
                        return False
                return True

            def _split_matches(self, row, game):
                if not self.split_key:
                    return True
                team_id = str(row.team_id)
                if self.split_key == "wins":
                    return team_id == str(game.wining_team_id)
                if self.split_key == "losses":
                    return game.wining_team_id is not None and team_id != str(game.wining_team_id)
                if self.split_key == "home":
                    return team_id == str(game.home_team_id)
                if self.split_key == "road":
                    return team_id == str(game.road_team_id)
                if self.split_key == "starter":
                    return bool(row.starter)
                if self.split_key == "bench":
                    return not bool(row.starter)
                return True

            def _value_from_field(self, row, field):
                if field == "min":
                    return self._minutes_played(row) / 60.0
                return float(getattr(row, field, 0) or 0)

            def _weighted_sum(self, row, terms):
                total = 0.0
                for field, weight in terms:
                    total += self._value_from_field(row, field) * float(weight)
                return total

            def _qualification_entry(self, entity_id, game_id):
                if entity_id is None or game_id is None:
                    return None
                return {{"entity_id": str(entity_id), "game_id": str(game_id), "qualified": True}}

            def compute_career_value(self, totals, season, entity_id):
                if not self.supports_career:
                    return None
                kind = self.metric_kind
                if kind in ("count_threshold", "count_combo", "count_exact", "games_played", "games_started"):
                    count = int(totals.get("count", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or count == 0:
                        return None
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=float(count),
                        value_str=f"{{count}}",
                        context={{"count": count, "games": games}},
                    )
                if kind in ("per_game", "split_avg"):
                    total = float(totals.get("total", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or total == 0:
                        return None
                    value = total / games
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.2f}}{value_suffix}",
                        context={{"total": self._round(total), "games": games, "average": self._round(value)}},
                    )
                if kind == "season_total":
                    total = float(totals.get("total", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or total == 0:
                        return None
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(total),
                        value_str=f"{{int(round(total))}}{value_suffix}",
                        context={{"total": self._round(total), "games": games}},
                    )
                if kind == "per_36":
                    total = float(totals.get("total", 0))
                    seconds = float(totals.get("seconds", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or seconds <= 0 or total == 0:
                        return None
                    value = total * 36.0 / (seconds / 3600.0) / 60.0
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.2f}}{value_suffix}",
                        context={{"total": self._round(total), "seconds": int(seconds), "games": games, "per_36": self._round(value)}},
                    )
                if kind == "ratio":
                    numerator = float(totals.get("numerator", 0))
                    denominator = float(totals.get("denominator", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or denominator <= 0:
                        return None
                    if self.qualifier_min is not None and denominator < float(self.qualifier_min):
                        return None
                    value = self.ratio_multiplier * numerator / denominator
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.2f}}{value_suffix}",
                        context={{"numerator": self._round(numerator), "denominator": self._round(denominator), "games": games, "value": self._round(value)}},
                    )
                if kind == "win_pct":
                    wins = int(totals.get("wins", 0))
                    starts = int(totals.get("starts", 0))
                    if starts < self.min_sample or starts == 0:
                        return None
                    value = 100.0 * wins / starts
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.1f}}%",
                        context={{"wins": wins, "starts": starts, "win_pct": self._round(value, 1)}},
                    )
                if kind == "streak":
                    best = int(totals.get("best_streak", 0))
                    if best < self.min_sample:
                        return None
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=float(best),
                        value_str=f"{{best}} games",
                        context={{"best_streak": best}},
                    )
                if kind == "single_game_record":
                    best = totals.get("best_value")
                    if best is None:
                        return None
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(best),
                        value_str=f"{{float(best):.2f}}{value_suffix}",
                        context={{"best_value": self._round(best)}},
                    )
                return None

            def compute_season(self, session, season):
                games = self._season_games(session, season)
                if not games:
                    return []
                game_ids = [game.game_id for game in games]
                game_map = {{game.game_id: game for game in games}}
                rows = (
                    session.query(PlayerGameStats)
                    .filter(
                        PlayerGameStats.game_id.in_(game_ids),
                        PlayerGameStats.player_id.isnot(None),
                    )
                    .all()
                )
                rows_by_player = defaultdict(list)
                for row in rows:
                    if not self._played(row):
                        continue
                    rows_by_player[str(row.player_id)].append(row)

                results = []
                qualifications = []
                for player_id, player_rows in rows_by_player.items():
                    player_rows.sort(key=lambda row: (
                        game_map[row.game_id].game_date or "",
                        str(row.game_id),
                    ))
                    kind = self.metric_kind
                    if kind in ("count_threshold", "count_combo", "count_exact", "games_played", "games_started"):
                        count = 0
                        games_count = 0
                        player_qualifications = []
                        for row in player_rows:
                            game = game_map[row.game_id]
                            if not self._split_matches(row, game):
                                continue
                            games_count += 1
                            qualified = False
                            if kind == "games_played":
                                count += 1
                            elif kind == "games_started":
                                qualified = bool(row.starter)
                                count += 1 if qualified else 0
                            else:
                                qualified = self._criteria_met(row)
                                if qualified:
                                    count += 1
                            if qualified:
                                qualification = self._qualification_entry(player_id, row.game_id)
                                if qualification is not None:
                                    player_qualifications.append(qualification)
                        if games_count < self.min_sample or count == 0:
                            continue
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=None,
                            value_num=float(count),
                            value_str=f"{{count}}",
                            context={{"count": count, "games": games_count}},
                        ))
                        if player_qualifications:
                            qualifications.extend(player_qualifications)
                        continue
                    if kind in ("per_game", "split_avg", "season_total"):
                        total = 0.0
                        games_count = 0
                        for row in player_rows:
                            game = game_map[row.game_id]
                            if not self._split_matches(row, game):
                                continue
                            total += self._value_from_field(row, self.value_field)
                            games_count += 1
                        if games_count < self.min_sample or total == 0:
                            continue
                        if kind == "season_total":
                            value = total
                            value_str = f"{{int(round(total))}}{value_suffix}"
                        else:
                            value = total / games_count
                            value_str = f"{{value:.2f}}{value_suffix}"
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=value_str,
                            context={{"total": self._round(total), "games": games_count, "average": self._round(total / games_count)}},
                        ))
                        continue
                    if kind == "per_36":
                        total = 0.0
                        seconds = 0
                        games_count = 0
                        for row in player_rows:
                            total += self._value_from_field(row, self.value_field)
                            seconds += self._minutes_played(row)
                            games_count += 1
                        if games_count < self.min_sample or seconds <= 0 or total == 0:
                            continue
                        value = total * 36.0 / (seconds / 3600.0) / 60.0
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=f"{{value:.2f}}{value_suffix}",
                            context={{"total": self._round(total), "seconds": int(seconds), "games": games_count, "per_36": self._round(value)}},
                        ))
                        continue
                    if kind == "ratio":
                        numerator = 0.0
                        denominator = 0.0
                        games_count = 0
                        for row in player_rows:
                            numerator += self._weighted_sum(row, self.numerator_terms)
                            denominator += self._weighted_sum(row, self.denominator_terms)
                            games_count += 1
                        if games_count < self.min_sample or denominator <= 0:
                            continue
                        if self.qualifier_min is not None and denominator < float(self.qualifier_min):
                            continue
                        value = self.ratio_multiplier * numerator / denominator
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=f"{{value:.2f}}{value_suffix}",
                            context={{"numerator": self._round(numerator), "denominator": self._round(denominator), "games": games_count, "value": self._round(value)}},
                        ))
                        continue
                    if kind == "win_pct":
                        starts = 0
                        wins = 0
                        for row in player_rows:
                            game = game_map[row.game_id]
                            if not bool(row.starter):
                                continue
                            starts += 1
                            if str(row.team_id) == str(game.wining_team_id):
                                wins += 1
                        if starts < self.min_sample:
                            continue
                        value = 100.0 * wins / starts
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=f"{{value:.1f}}%",
                            context={{"wins": wins, "starts": starts, "win_pct": self._round(value, 1)}},
                        ))
                        continue
                    if kind == "streak":
                        best = 0
                        current = 0
                        current_game_ids = []
                        best_game_ids = []
                        for row in player_rows:
                            game = game_map[row.game_id]
                            if self._split_matches(row, game) and self._criteria_met(row):
                                current += 1
                                current_game_ids.append(str(row.game_id))
                                if current > best:
                                    best = current
                                    best_game_ids = list(current_game_ids)
                            else:
                                current = 0
                                current_game_ids = []
                        if best < self.min_sample:
                            continue
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=None,
                            value_num=float(best),
                            value_str=f"{{best}} games",
                            context={{"best_streak": best}},
                        ))
                        for game_id in best_game_ids:
                            qualification = self._qualification_entry(player_id, game_id)
                            if qualification is not None:
                                qualifications.append(qualification)
                        continue
                    if kind == "single_game_record":
                        best_value = None
                        best_game_id = None
                        for row in player_rows:
                            raw = self._value_from_field(row, self.value_field)
                            if self.qualifier_field and self.qualifier_min is not None:
                                if self._value_from_field(row, self.qualifier_field) < float(self.qualifier_min):
                                    continue
                            if best_value is None:
                                best_value = raw
                                best_game_id = str(row.game_id)
                                continue
                            if self.record_mode == "min" and raw < best_value:
                                best_value = raw
                                best_game_id = str(row.game_id)
                            elif self.record_mode != "min" and raw > best_value:
                                best_value = raw
                                best_game_id = str(row.game_id)
                        if best_value is None:
                            continue
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="player",
                            entity_id=player_id,
                            season=season,
                            game_id=best_game_id,
                            value_num=self._round(best_value),
                            value_str=f"{{float(best_value):.2f}}{value_suffix}",
                            context={{"best_value": self._round(best_value), "game_id": best_game_id}},
                        ))
                        qualification = self._qualification_entry(player_id, best_game_id)
                        if qualification is not None:
                            qualifications.append(qualification)
                self._qualifications = qualifications or None
                return results

            def compute_qualifications(self, session, season):
                return getattr(self, "_qualifications", None)
        """
    ).strip() + "\n"


def render_player_shot_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    category: str,
    group_key: str | None,
    min_sample: int,
    supports_career: bool,
    rank_order: str = "desc",
    metric_kind: str,
    zones: tuple[str, ...] = (),
    denominator_kind: str = "all_attempts",
    qualifier_min: int | None = None,
    context_label_template: str | None = None,
) -> str:
    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
        from db.models import Game, ShotRecord


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "player"
            category = {category!r}
            min_sample = {int(min_sample)}
            trigger = "season"
            incremental = False
            supports_career = {supports_career!r}
            rank_order = {rank_order!r}
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            context_label_template = {context_label_template!r}
            metric_kind = {metric_kind!r}
            zones = {zones!r}
            denominator_kind = {denominator_kind!r}
            qualifier_min = {qualifier_min!r}
            career_aggregate_mode = {"season_results" if supports_career else None!r}
            career_sum_keys = {("made", "attempts", "distance_total", "all_three_attempts", "all_attempts") if supports_career else ()!r}

            def _round(self, value, digits=4):
                return round(float(value), digits)

            def _season_filter(self, query, season):
                if is_career_season(season):
                    code = career_season_type_code(season)
                    if not code:
                        return query.filter(False)
                    return query.filter(Game.season.like(f"{{code}}%"))
                return query.filter(Game.season == season)

            def compute_career_value(self, totals, season, entity_id):
                if not self.supports_career:
                    return None
                attempts = float(totals.get("attempts", 0))
                made = float(totals.get("made", 0))
                distance_total = float(totals.get("distance_total", 0))
                all_three_attempts = float(totals.get("all_three_attempts", 0))
                all_attempts = float(totals.get("all_attempts", 0))
                if self.metric_kind == "distance":
                    if attempts < self.min_sample:
                        return None
                    value = distance_total / attempts
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.2f}} ft",
                        context={{"distance_total": self._round(distance_total), "attempts": int(attempts), "average_distance": self._round(value)}},
                    )
                if self.metric_kind == "share":
                    denominator = all_three_attempts if self.denominator_kind == "three_attempts" else all_attempts
                    if denominator <= 0 or attempts < self.min_sample:
                        return None
                    value = 100.0 * attempts / denominator
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.1f}}%",
                        context={{"attempts": int(attempts), "denominator": int(denominator), "share_pct": self._round(value, 1)}},
                    )
                if attempts <= 0 or (self.qualifier_min is not None and attempts < self.qualifier_min):
                    return None
                value = 100.0 * made / attempts
                return MetricResult(
                    metric_key=self.key,
                    entity_type="player",
                    entity_id=entity_id,
                    season=season,
                    game_id=None,
                    value_num=self._round(value),
                    value_str=f"{{value:.1f}}%",
                    context={{"made": int(made), "attempts": int(attempts), "fg_pct": self._round(value, 1)}},
                )

            def compute_season(self, session, season):
                rows = (
                    self._season_filter(
                        session.query(ShotRecord, Game)
                        .join(Game, Game.game_id == ShotRecord.game_id)
                        .filter(
                            ShotRecord.player_id.isnot(None),
                            ShotRecord.shot_attempted.is_(True),
                        ),
                        season,
                    )
                    .all()
                )
                if not rows:
                    return []
                acc = defaultdict(lambda: {{"made": 0, "attempts": 0, "distance_total": 0.0, "all_three_attempts": 0, "all_attempts": 0}})
                for shot, _game in rows:
                    pid = str(shot.player_id)
                    data = acc[pid]
                    data["all_attempts"] += 1
                    if str(shot.shot_type or "").startswith("3PT"):
                        data["all_three_attempts"] += 1
                    if self.zones and str(shot.shot_zone_basic) not in self.zones:
                        continue
                    data["attempts"] += 1
                    data["made"] += 1 if shot.shot_made else 0
                    data["distance_total"] += float(shot.shot_distance or 0)
                results = []
                for player_id, totals in acc.items():
                    result = self.compute_career_value(totals, season, player_id)
                    if result is not None:
                        results.append(result)
                return results
        """
    ).strip() + "\n"


def render_team_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    category: str,
    group_key: str | None,
    min_sample: int,
    supports_career: bool,
    rank_order: str = "desc",
    metric_kind: str,
    stat_field: str | None = None,
    split_key: str | None = None,
    threshold: float | None = None,
    ratio_numerator: tuple[tuple[str, float], ...] = (),
    ratio_denominator: tuple[tuple[str, float], ...] = (),
    ratio_multiplier: float = 1.0,
    context_label_template: str | None = None,
) -> str:
    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
        from db.models import Game, TeamGameStats


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "team"
            category = {category!r}
            min_sample = {int(min_sample)}
            trigger = "season"
            incremental = False
            supports_career = {supports_career!r}
            rank_order = {rank_order!r}
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            context_label_template = {context_label_template!r}
            metric_kind = {metric_kind!r}
            stat_field = {stat_field!r}
            split_key = {split_key!r}
            threshold = {threshold!r}
            ratio_numerator = {ratio_numerator!r}
            ratio_denominator = {ratio_denominator!r}
            ratio_multiplier = {ratio_multiplier!r}
            career_aggregate_mode = {"season_results" if supports_career else None!r}
            career_sum_keys = {("numerator", "denominator", "total", "games", "wins", "losses") if supports_career else ()!r}
            career_max_keys = {("best_streak",) if supports_career and metric_kind == "streak" else ()!r}

            def _round(self, value, digits=4):
                return round(float(value), digits)

            def _season_games(self, session, season):
                query = session.query(Game)
                if is_career_season(season):
                    code = career_season_type_code(season)
                    if not code:
                        return []
                    query = query.filter(Game.season.like(f"{{code}}%"))
                else:
                    query = query.filter(Game.season == season)
                return query.order_by(Game.game_date.asc(), Game.game_id.asc()).all()

            def _weighted(self, row, terms):
                total = 0.0
                for field, weight in terms:
                    total += float(getattr(row, field, 0) or 0) * float(weight)
                return total

            def _split_matches(self, game, row):
                team_id = str(row.team_id)
                if self.split_key == "home":
                    return team_id == str(game.home_team_id)
                if self.split_key == "road":
                    return team_id == str(game.road_team_id)
                if self.split_key == "wins":
                    return team_id == str(game.wining_team_id)
                if self.split_key == "losses":
                    return game.wining_team_id is not None and team_id != str(game.wining_team_id)
                if self.split_key == "overtime":
                    home_score = int(game.home_team_score or 0)
                    road_score = int(game.road_team_score or 0)
                    return home_score > 120 or road_score > 120
                return True

            def _value(self, row, opponent, game):
                if self.stat_field == "opp_pts":
                    return float(opponent.pts or 0)
                if self.stat_field == "opp_fg_pct":
                    return float(opponent.fg_pct or 0) * 100.0
                if self.stat_field == "point_diff":
                    return float((row.pts or 0) - (opponent.pts or 0))
                if self.stat_field == "close_game":
                    return 1.0 if abs(int((row.pts or 0) - (opponent.pts or 0))) <= 5 else 0.0
                if self.stat_field == "dominant_win":
                    return 1.0 if bool(row.win) and int((row.pts or 0) - (opponent.pts or 0)) >= 15 else 0.0
                if self.stat_field == "blowout_win":
                    return 1.0 if bool(row.win) and int((row.pts or 0) - (opponent.pts or 0)) >= 10 else 0.0
                if self.stat_field == "blowout_loss":
                    return 1.0 if not bool(row.win) and int((opponent.pts or 0) - (row.pts or 0)) >= 10 else 0.0
                if self.stat_field == "win":
                    return 1.0 if bool(row.win) else 0.0
                if self.stat_field == "loss":
                    return 1.0 if not bool(row.win) else 0.0
                return float(getattr(row, self.stat_field, 0) or 0)

            def _qualification_entry(self, entity_id, game_id):
                if entity_id is None or game_id is None:
                    return None
                return {{"entity_id": str(entity_id), "entity_type": "team", "game_id": str(game_id), "qualified": True}}

            def compute_career_value(self, totals, season, entity_id):
                kind = self.metric_kind
                if kind in ("per_game", "split_avg"):
                    total = float(totals.get("total", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or total == 0:
                        return None
                    value = total / games
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="team",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.2f}}",
                        context={{"total": self._round(total), "games": games, "average": self._round(value)}},
                    )
                if kind in ("count", "win_pct"):
                    wins = int(totals.get("wins", 0))
                    losses = int(totals.get("losses", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample:
                        return None
                    if kind == "win_pct":
                        value = 100.0 * wins / games if games else 0.0
                        if wins == 0:
                            return None
                        return MetricResult(
                            metric_key=self.key,
                            entity_type="team",
                            entity_id=entity_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=f"{{value:.1f}}%",
                            context={{"wins": wins, "losses": losses, "games": games, "win_pct": self._round(value, 1)}},
                        )
                    value = wins if self.stat_field == "win" else losses if self.stat_field == "loss" else int(totals.get("total", 0))
                    if value == 0:
                        return None
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="team",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=float(value),
                        value_str=f"{{value}}",
                        context={{"wins": wins, "losses": losses, "games": games, "total": value}},
                    )
                if kind == "ratio":
                    numerator = float(totals.get("numerator", 0))
                    denominator = float(totals.get("denominator", 0))
                    games = int(totals.get("games", 0))
                    if games < self.min_sample or denominator <= 0:
                        return None
                    value = self.ratio_multiplier * numerator / denominator
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="team",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=self._round(value),
                        value_str=f"{{value:.2f}}",
                        context={{"numerator": self._round(numerator), "denominator": self._round(denominator), "games": games, "value": self._round(value)}},
                    )
                if kind == "streak":
                    best_streak = int(totals.get("best_streak", 0))
                    if best_streak < self.min_sample:
                        return None
                    return MetricResult(
                        metric_key=self.key,
                        entity_type="team",
                        entity_id=entity_id,
                        season=season,
                        game_id=None,
                        value_num=float(best_streak),
                        value_str=f"{{best_streak}} games",
                        context={{"best_streak": best_streak}},
                    )
                return None

            def compute_season(self, session, season):
                games = self._season_games(session, season)
                if not games:
                    return []
                game_ids = [game.game_id for game in games]
                game_map = {{game.game_id: game for game in games}}
                rows = (
                    session.query(TeamGameStats)
                    .filter(
                        TeamGameStats.game_id.in_(game_ids),
                        TeamGameStats.team_id.isnot(None),
                    )
                    .all()
                )
                by_game = defaultdict(list)
                for row in rows:
                    by_game[row.game_id].append(row)

                records = defaultdict(list)
                for game_id, pair in by_game.items():
                    if len(pair) < 2:
                        continue
                    game = game_map.get(game_id)
                    if not game:
                        continue
                    pair_map = {{str(row.team_id): row for row in pair if row.team_id is not None}}
                    for row in pair:
                        team_id = str(row.team_id)
                        opponent = None
                        for other in pair:
                            if str(other.team_id) != team_id:
                                opponent = other
                                break
                        if opponent is None or not self._split_matches(game, row):
                            continue
                        records[team_id].append((game, row, opponent))

                results = []
                qualifications = []
                for team_id, entries in records.items():
                    entries.sort(key=lambda item: (item[0].game_date or "", str(item[0].game_id)))
                    kind = self.metric_kind
                    if kind in ("per_game", "split_avg"):
                        total = 0.0
                        games_count = 0
                        for game, row, opponent in entries:
                            total += self._value(row, opponent, game)
                            games_count += 1
                        if games_count < self.min_sample or total == 0:
                            continue
                        value = total / games_count
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="team",
                            entity_id=team_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=f"{{value:.2f}}",
                            context={{"total": self._round(total), "games": games_count, "average": self._round(value)}},
                        ))
                        continue
                    if kind in ("count", "win_pct"):
                        wins = 0
                        losses = 0
                        total = 0
                        games_count = 0
                        team_qualifications = []
                        for game, row, opponent in entries:
                            wins += 1 if bool(row.win) else 0
                            losses += 0 if bool(row.win) else 1
                            games_count += 1
                            qualified = False
                            if kind == "count":
                                if self.stat_field == "win":
                                    qualified = bool(row.win)
                                    total += 1 if qualified else 0
                                elif self.stat_field == "loss":
                                    qualified = not bool(row.win)
                                    total += 1 if qualified else 0
                                elif self.threshold is not None:
                                    qualified = self._value(row, opponent, game) >= float(self.threshold)
                                    total += 1 if qualified else 0
                                else:
                                    total += int(self._value(row, opponent, game))
                            if qualified:
                                qualification = self._qualification_entry(team_id, game.game_id)
                                if qualification is not None:
                                    team_qualifications.append(qualification)
                        if games_count < self.min_sample:
                            continue
                        if kind == "win_pct":
                            value = 100.0 * wins / games_count if games_count else 0.0
                            if wins == 0:
                                continue
                            results.append(MetricResult(
                                metric_key=self.key,
                                entity_type="team",
                                entity_id=team_id,
                                season=season,
                                game_id=None,
                                value_num=self._round(value),
                                value_str=f"{{value:.1f}}%",
                                context={{"wins": wins, "losses": losses, "games": games_count, "win_pct": self._round(value, 1)}},
                            ))
                        elif total:
                            results.append(MetricResult(
                                metric_key=self.key,
                                entity_type="team",
                                entity_id=team_id,
                                season=season,
                                game_id=None,
                                value_num=float(total),
                                value_str=f"{{total}}",
                                context={{"wins": wins, "losses": losses, "games": games_count, "total": total}},
                            ))
                            if team_qualifications:
                                qualifications.extend(team_qualifications)
                        continue
                    if kind == "ratio":
                        numerator = 0.0
                        denominator = 0.0
                        games_count = 0
                        for _game, row, _opponent in entries:
                            numerator += self._weighted(row, self.ratio_numerator)
                            denominator += self._weighted(row, self.ratio_denominator)
                            games_count += 1
                        if games_count < self.min_sample or denominator <= 0:
                            continue
                        value = self.ratio_multiplier * numerator / denominator
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="team",
                            entity_id=team_id,
                            season=season,
                            game_id=None,
                            value_num=self._round(value),
                            value_str=f"{{value:.2f}}",
                            context={{"numerator": self._round(numerator), "denominator": self._round(denominator), "games": games_count, "value": self._round(value)}},
                        ))
                        continue
                    if kind == "streak":
                        current = 0
                        best = 0
                        current_game_ids = []
                        best_game_ids = []
                        for game, row, opponent in entries:
                            qualified = False
                            if self.stat_field == "win":
                                qualified = bool(row.win)
                            elif self.stat_field == "loss":
                                qualified = not bool(row.win)
                            elif self.threshold is not None:
                                qualified = self._value(row, opponent, game) >= float(self.threshold)
                            if qualified:
                                current += 1
                                current_game_ids.append(str(game.game_id))
                                if current > best:
                                    best = current
                                    best_game_ids = list(current_game_ids)
                            else:
                                current = 0
                                current_game_ids = []
                        if best < self.min_sample:
                            continue
                        results.append(MetricResult(
                            metric_key=self.key,
                            entity_type="team",
                            entity_id=team_id,
                            season=season,
                            game_id=None,
                            value_num=float(best),
                            value_str=f"{{best}} games",
                            context={{"best_streak": best}},
                        ))
                        for game_id in best_game_ids:
                            qualification = self._qualification_entry(team_id, game_id)
                            if qualification is not None:
                                qualifications.append(qualification)
                self._qualifications = qualifications or None
                return results

            def compute_qualifications(self, session, season):
                return getattr(self, "_qualifications", None)
        """
    ).strip() + "\n"


def render_team_bench_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    group_key: str | None,
    min_sample: int,
    rank_order: str,
    metric_kind: str,
    role: str,
    stat_field: str = "pts",
) -> str:
    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
        from db.models import Game, PlayerGameStats


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "team"
            category = "aggregate"
            min_sample = {int(min_sample)}
            trigger = "season"
            incremental = False
            supports_career = True
            rank_order = {rank_order!r}
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            metric_kind = {metric_kind!r}
            role = {role!r}
            stat_field = {stat_field!r}
            career_aggregate_mode = "season_results"
            career_sum_keys = ("numerator", "denominator", "games")

            def _round(self, value, digits=4):
                return round(float(value), digits)

            def _season_filter(self, query, season):
                if is_career_season(season):
                    code = career_season_type_code(season)
                    if not code:
                        return query.filter(False)
                    return query.filter(Game.season.like(f"{{code}}%"))
                return query.filter(Game.season == season)

            def compute_career_value(self, totals, season, entity_id):
                numerator = float(totals.get("numerator", 0))
                denominator = float(totals.get("denominator", 0))
                games = int(totals.get("games", 0))
                if games < self.min_sample or denominator <= 0:
                    return None
                value = numerator / denominator
                suffix = "%" if self.metric_kind == "share" else ""
                display = f"{{value:.1f}}{{suffix}}" if self.metric_kind == "share" else f"{{value:.2f}}"
                return MetricResult(
                    metric_key=self.key,
                    entity_type="team",
                    entity_id=entity_id,
                    season=season,
                    game_id=None,
                    value_num=self._round(value),
                    value_str=display,
                    context={{"numerator": self._round(numerator), "denominator": self._round(denominator), "games": games, "value": self._round(value)}},
                )

            def compute_season(self, session, season):
                rows = (
                    self._season_filter(
                        session.query(PlayerGameStats, Game)
                        .join(Game, Game.game_id == PlayerGameStats.game_id)
                        .filter(
                            PlayerGameStats.team_id.isnot(None),
                            PlayerGameStats.player_id.isnot(None),
                        ),
                        season,
                    )
                    .all()
                )
                game_team_totals = defaultdict(lambda: {{"role_points": 0.0, "team_points": 0.0}})
                for row, _game in rows:
                    team_key = (str(row.game_id), str(row.team_id))
                    stat_value = float(getattr(row, self.stat_field, 0) or 0)
                    if self.role == "starter" and bool(row.starter):
                        game_team_totals[team_key]["role_points"] += stat_value
                    elif self.role == "bench" and not bool(row.starter):
                        game_team_totals[team_key]["role_points"] += stat_value
                    game_team_totals[team_key]["team_points"] += stat_value

                acc = defaultdict(lambda: {{"numerator": 0.0, "denominator": 0.0, "games": 0}})
                for (_game_id, team_id), totals in game_team_totals.items():
                    acc[team_id]["games"] += 1
                    if self.metric_kind == "share":
                        acc[team_id]["numerator"] += 100.0 * totals["role_points"]
                        acc[team_id]["denominator"] += totals["team_points"] or 0.0
                    else:
                        acc[team_id]["numerator"] += totals["role_points"]
                        acc[team_id]["denominator"] += 1.0

                results = []
                for team_id, totals in acc.items():
                    result = self.compute_career_value(totals, season, team_id)
                    if result is not None:
                        results.append(result)
                return results
        """
    ).strip() + "\n"


def render_game_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    category: str,
    group_key: str | None,
    rank_order: str,
    metric_kind: str,
    stat_field: str | None = None,
) -> str:
    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult
        from db.models import Game, GameLineScore, GamePlayByPlay, TeamGameStats


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "game"
            category = {category!r}
            min_sample = 1
            trigger = "season"
            incremental = False
            supports_career = False
            rank_order = {rank_order!r}
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            metric_kind = {metric_kind!r}
            stat_field = {stat_field!r}

            def _round(self, value, digits=4):
                return round(float(value), digits)

            def _largest_lead(self, session, game_id):
                rows = (
                    session.query(GamePlayByPlay)
                    .filter(
                        GamePlayByPlay.game_id == game_id,
                        GamePlayByPlay.score_margin.isnot(None),
                    )
                    .order_by(GamePlayByPlay.period.asc(), GamePlayByPlay.event_num.asc(), GamePlayByPlay.id.asc())
                    .all()
                )
                best = 0
                for row in rows:
                    value = str(row.score_margin or "")
                    if value in ("", "TIE"):
                        margin = 0
                    else:
                        try:
                            margin = abs(int(value))
                        except ValueError:
                            margin = 0
                    if margin > best:
                        best = margin
                return best

            def compute_season(self, session, season):
                games = session.query(Game).filter(Game.season == season).all()
                if not games:
                    return []
                game_ids = [game.game_id for game in games]
                tstats = session.query(TeamGameStats).filter(TeamGameStats.game_id.in_(game_ids)).all()
                lines = session.query(GameLineScore).filter(GameLineScore.game_id.in_(game_ids)).all()
                stats_by_game = defaultdict(list)
                for row in tstats:
                    stats_by_game[row.game_id].append(row)
                lines_by_game = defaultdict(list)
                for row in lines:
                    lines_by_game[row.game_id].append(row)

                results = []
                for game in games:
                    rows = stats_by_game.get(game.game_id, [])
                    if len(rows) < 2:
                        continue
                    home_row = next((row for row in rows if str(row.team_id) == str(game.home_team_id)), rows[0])
                    road_row = next((row for row in rows if str(row.team_id) == str(game.road_team_id)), rows[1])
                    value = None
                    context = {{
                        "home_team_id": str(game.home_team_id),
                        "road_team_id": str(game.road_team_id),
                        "game_id": str(game.game_id),
                    }}
                    if self.metric_kind == "combined":
                        value = float(getattr(home_row, self.stat_field, 0) or 0) + float(getattr(road_row, self.stat_field, 0) or 0)
                    elif self.metric_kind == "disparity":
                        value = abs(float(getattr(home_row, self.stat_field, 0) or 0) - float(getattr(road_row, self.stat_field, 0) or 0))
                    elif self.metric_kind == "margin":
                        value = abs(int(game.home_team_score or 0) - int(game.road_team_score or 0))
                    elif self.metric_kind == "halftime_margin":
                        line_rows = lines_by_game.get(game.game_id, [])
                        if len(line_rows) >= 2:
                            home_line = next((row for row in line_rows if str(row.team_id) == str(game.home_team_id)), line_rows[0])
                            road_line = next((row for row in line_rows if str(row.team_id) == str(game.road_team_id)), line_rows[1])
                            value = abs(int(home_line.first_half_pts or 0) - int(road_line.first_half_pts or 0))
                    elif self.metric_kind == "overtime_periods":
                        line_rows = lines_by_game.get(game.game_id, [])
                        if len(line_rows) >= 2:
                            home_line = next((row for row in line_rows if str(row.team_id) == str(game.home_team_id)), line_rows[0])
                            extra = [home_line.ot1_pts, home_line.ot2_pts, home_line.ot3_pts]
                            value = sum(1 for item in extra if item is not None)
                            if home_line.ot_extra_json:
                                import json
                                try:
                                    extra_json = json.loads(home_line.ot_extra_json)
                                except json.JSONDecodeError:
                                    extra_json = []
                                value += len([item for item in extra_json if item is not None])
                    elif self.metric_kind == "largest_lead":
                        value = self._largest_lead(session, game.game_id)
                    if value is None:
                        continue
                    if value == 0 and self.metric_kind not in ("disparity", "overtime_periods"):
                        continue
                    results.append(MetricResult(
                        metric_key=self.key,
                        entity_type="game",
                        entity_id=str(game.game_id),
                        season=season,
                        game_id=str(game.game_id),
                        value_num=self._round(value),
                        value_str=f"{{float(value):.2f}}" if float(value) % 1 else f"{{int(value)}}",
                        context=context,
                    ))
                return results
        """
    ).strip() + "\n"


def render_award_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    award_types: tuple[str, ...],
    group_key: str,
) -> str:
    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
        from db.models import Award


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "player"
            category = "record"
            min_sample = 1
            trigger = "season"
            incremental = False
            supports_career = True
            rank_order = "desc"
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            award_types = {award_types!r}
            career_aggregate_mode = "season_results"
            career_sum_keys = ("count",)

            def _season_value(self, season):
                if is_career_season(season):
                    code = career_season_type_code(season)
                    if not code:
                        return None
                    return int(code)
                try:
                    return int(str(season))
                except ValueError:
                    return None

            def compute_career_value(self, totals, season, entity_id):
                count = int(totals.get("count", 0))
                if count <= 0:
                    return None
                return MetricResult(
                    metric_key=self.key,
                    entity_type="player",
                    entity_id=entity_id,
                    season=season,
                    game_id=None,
                    value_num=float(count),
                    value_str=f"{{count}}",
                    context={{"count": count}},
                )

            def compute_season(self, session, season):
                season_value = self._season_value(season)
                if season_value is None:
                    return []
                rows = (
                    session.query(Award)
                    .filter(
                        Award.player_id.isnot(None),
                        Award.season == season_value,
                        Award.award_type.in_(self.award_types),
                    )
                    .all()
                )
                acc = defaultdict(int)
                for row in rows:
                    acc[str(row.player_id)] += 1
                results = []
                for player_id, count in acc.items():
                    results.append(MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=player_id,
                        season=season,
                        game_id=None,
                        value_num=float(count),
                        value_str=f"{{count}}",
                        context={{"count": count}},
                    ))
                return results
        """
    ).strip() + "\n"


def render_salary_metric(
    *,
    class_name: str,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    group_key: str,
    metric_kind: str,
    rank_order: str,
) -> str:
    return dedent(
        f"""
        from __future__ import annotations

        from collections import defaultdict

        from metrics.framework.base import MetricDefinition, MetricResult
        from db.models import Game, PlayerGameStats, PlayerSalary


        class {class_name}(MetricDefinition):
            key = {key!r}
            name = {name!r}
            name_zh = {name_zh!r}
            description = {description!r}
            description_zh = {description_zh!r}
            scope = "player"
            category = "efficiency"
            min_sample = 1
            trigger = "season"
            incremental = False
            supports_career = False
            rank_order = {rank_order!r}
            max_results_per_season = {DEFAULT_LIMIT}
            group_key = {group_key!r}
            metric_kind = {metric_kind!r}

            def _season_salary_year(self, season):
                text = str(season)
                if len(text) >= 4 and text[-4:].isdigit():
                    return int(text[-4:])
                return None

            def compute_season(self, session, season):
                salary_year = self._season_salary_year(season)
                if salary_year is None:
                    return []
                salary_rows = session.query(PlayerSalary).filter(PlayerSalary.season == salary_year).all()
                if not salary_rows:
                    return []
                stat_rows = (
                    session.query(PlayerGameStats, Game)
                    .join(Game, Game.game_id == PlayerGameStats.game_id)
                    .filter(
                        Game.season == season,
                        PlayerGameStats.player_id.isnot(None),
                    )
                    .all()
                )
                totals = defaultdict(lambda: {{"points": 0.0, "rebounds": 0.0, "assists": 0.0, "games": 0}})
                for row, _game in stat_rows:
                    pid = str(row.player_id)
                    totals[pid]["points"] += float(row.pts or 0)
                    totals[pid]["rebounds"] += float(row.reb or 0)
                    totals[pid]["assists"] += float(row.ast or 0)
                    totals[pid]["games"] += 1
                results = []
                for salary in salary_rows:
                    pid = str(salary.player_id)
                    player_totals = totals.get(pid)
                    if not player_totals:
                        continue
                    salary_million = float(salary.salary_usd or 0) / 1000000.0
                    points = float(player_totals["points"])
                    rebounds = float(player_totals["rebounds"])
                    assists = float(player_totals["assists"])
                    games = int(player_totals["games"])
                    if salary_million <= 0 or games == 0:
                        continue
                    if self.metric_kind == "salary_per_point":
                        if points <= 0:
                            continue
                        value = salary_million / points
                        display = f"${{value:.4f}}M/pt"
                    elif self.metric_kind == "points_per_million":
                        value = points / salary_million
                        display = f"{{value:.2f}} pts/$1M"
                    elif self.metric_kind == "salary_per_game":
                        value = salary_million / games
                        display = f"${{value:.3f}}M/game"
                    elif self.metric_kind == "pra_per_million":
                        value = (points + rebounds + assists) / salary_million
                        display = f"{{value:.2f}} PRA/$1M"
                    else:
                        value = points / games / salary_million
                        display = f"{{value:.2f}} PPG/$1M"
                    results.append(MetricResult(
                        metric_key=self.key,
                        entity_type="player",
                        entity_id=pid,
                        season=season,
                        game_id=None,
                        value_num=round(float(value), 4),
                        value_str=display,
                        context={{
                            "salary_usd": int(salary.salary_usd or 0),
                            "salary_million": round(salary_million, 4),
                            "points": round(points, 2),
                            "rebounds": round(rebounds, 2),
                            "assists": round(assists, 2),
                            "games": games,
                        }},
                    ))
                return results
        """
    ).strip() + "\n"


NUMBER_WORDS = {
    3: ("three", "3"),
    5: ("five", "5"),
    7: ("seven", "7"),
    10: ("ten", "10"),
    12: ("twelve", "12"),
    15: ("fifteen", "15"),
    20: ("twenty", "20"),
    25: ("twenty_five", "25"),
    30: ("thirty", "30"),
    35: ("thirty_five", "35"),
    40: ("forty", "40"),
    45: ("forty_five", "45"),
    50: ("fifty", "50"),
    60: ("sixty", "60"),
}

PLAYER_STEMS = {
    "pts": "point",
    "ast": "assist",
    "reb": "rebound",
    "oreb": "offensive_rebound",
    "dreb": "defensive_rebound",
    "stl": "steal",
    "blk": "block",
    "tov": "turnover",
    "min": "minute",
    "fgm": "field_goal_made",
    "fga": "field_goal_attempt",
    "fg3m": "three_pm",
    "fg3a": "three_pa",
    "ftm": "free_throw_made",
    "fta": "free_throw_attempt",
    "plus": "plus_minus",
    "pf": "foul",
}


def build_metric_spec(
    *,
    key: str,
    name: str,
    name_zh: str,
    description: str,
    description_zh: str,
    scope: str,
    category: str,
    group_key: str | None,
    min_sample: int,
    supports_career: bool,
    code_python: str,
    context_label_template: str | None = None,
) -> MetricSpec:
    return MetricSpec(
        key=key,
        name=name,
        name_zh=name_zh,
        description=description,
        description_zh=description_zh,
        scope=scope,
        category=category,
        group_key=group_key,
        min_sample=min_sample,
        supports_career=supports_career,
        max_results_per_season=DEFAULT_LIMIT,
        code_python=code_python,
        context_label_template=context_label_template,
    )


def make_player_threshold_specs() -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    families = [
        ("pts", [10, 15, 20, 25, 35, 45, 60], "aggregate", "scoring_milestone_games"),
        ("reb", [10, 12, 15, 20, 25], "aggregate", "rebound_milestones"),
        ("ast", [5, 10, 12, 15, 20], "aggregate", "assist_milestones"),
        ("stl", [3, 5, 7, 10], "record", "defensive_milestones"),
        ("blk", [3, 5, 7, 10], "record", "defensive_milestones"),
        ("fg3m", [3, 5, 7, 10, 12, 15], "scoring", "three_point_milestones"),
        ("ftm", [5, 10, 15, 20], "scoring", "free_throw_milestones"),
        ("fta", [10, 15, 20], "aggregate", "free_throw_milestones"),
    ]
    for field, thresholds, category, group_key in families:
        label = _player_label(field)
        label_zh = _player_label_zh(field)
        for threshold in thresholds:
            word, zh_num = NUMBER_WORDS[threshold]
            key = f"{word}_plus_{PLAYER_STEMS[field]}_games"
            name = f"{threshold}+ {label} Games"
            name_zh = f"{zh_num}+{label_zh}比赛"
            description = f"Count of games in a season where a player recorded at least {threshold} {label.lower()}."
            description_zh = f"统计球员单赛季达到至少{zh_num}{label_zh}的比赛场次。"
            specs.append(
                build_metric_spec(
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    scope="player",
                    category=category,
                    group_key=group_key,
                    min_sample=1,
                    supports_career=True,
                    code_python=render_player_box_metric(
                        class_name=camelize(key),
                        key=key,
                        name=name,
                        name_zh=name_zh,
                        description=description,
                        description_zh=description_zh,
                        category=category,
                        group_key=group_key,
                        min_sample=1,
                        supports_career=True,
                        metric_kind="count_threshold",
                        criteria=((field, float(threshold)),),
                    ),
                )
            )
    return specs


def make_player_average_specs() -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    fields = ["pts", "ast", "reb", "oreb", "dreb", "stl", "blk", "tov", "min", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "plus", "pf"]
    for field in fields:
        key = f"{PLAYER_STEMS[field]}s_per_game" if field not in {"pts", "ast", "reb", "oreb", "dreb", "stl", "blk", "tov", "min", "plus"} else {
            "pts": "points_per_game",
            "ast": "assists_per_game",
            "reb": "rebounds_per_game",
            "oreb": "offensive_rebounds_per_game",
            "dreb": "defensive_rebounds_per_game",
            "stl": "steals_per_game",
            "blk": "blocks_per_game",
            "tov": "turnovers_per_game",
            "min": "minutes_per_game",
            "plus": "plus_minus_per_game",
        }.get(field, f"{PLAYER_STEMS[field]}s_per_game")
        name = f"{_player_label(field)} Per Game"
        name_zh = f"场均{_player_label_zh(field)}"
        description = f"Average {_player_label(field).lower()} per game in a season."
        description_zh = f"统计球员单赛季场均{_player_label_zh(field)}。"
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="aggregate" if field not in {"tov", "pf"} else "efficiency",
                group_key="per_game_averages",
                min_sample=10,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="aggregate" if field not in {"tov", "pf"} else "efficiency",
                    group_key="per_game_averages",
                    min_sample=10,
                    supports_career=True,
                    rank_order="asc" if field in {"tov", "pf"} else "desc",
                    metric_kind="per_game",
                    value_field=field,
                ),
            )
        )
    return specs


def make_player_total_specs() -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    for field in ["pts", "ast", "reb", "oreb", "dreb", "stl", "blk", "tov", "fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "pf"]:
        key = {
            "pts": "season_total_points",
            "ast": "season_total_assists",
            "reb": "season_total_rebounds",
            "oreb": "season_total_offensive_rebounds",
            "dreb": "season_total_defensive_rebounds",
            "stl": "season_total_steals",
            "blk": "season_total_blocks",
            "tov": "season_total_turnovers",
            "fgm": "season_total_field_goals_made",
            "fga": "season_total_field_goal_attempts",
            "fg3m": "season_total_three_pointers_made",
            "fg3a": "season_total_three_point_attempts",
            "ftm": "season_total_free_throws_made",
            "fta": "season_total_free_throw_attempts",
            "pf": "season_total_fouls",
        }[field]
        name = f"Season Total {_player_label(field)}"
        name_zh = f"赛季总{_player_label_zh(field)}"
        description = f"Total {_player_label(field).lower()} recorded across a season."
        description_zh = f"统计球员单赛季累计{_player_label_zh(field)}。"
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="aggregate",
                group_key="season_totals",
                min_sample=1,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="aggregate",
                    group_key="season_totals",
                    min_sample=1,
                    supports_career=True,
                    metric_kind="season_total",
                    value_field=field,
                ),
            )
        )
    return specs


def make_player_per36_specs() -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    for field in ["pts", "ast", "reb", "oreb", "dreb", "stl", "blk", "tov", "fg3m", "ftm"]:
        key = {
            "pts": "points_per_36",
            "ast": "assists_per_36",
            "reb": "rebounds_per_36",
            "oreb": "offensive_rebounds_per_36",
            "dreb": "defensive_rebounds_per_36",
            "stl": "steals_per_36",
            "blk": "blocks_per_36",
            "tov": "turnovers_per_36",
            "fg3m": "three_pointers_made_per_36",
            "ftm": "free_throws_made_per_36",
        }[field]
        name = f"{_player_label(field)} Per 36 Minutes"
        name_zh = f"每36分钟{_player_label_zh(field)}"
        description = f"{_player_label(field)} normalized to 36 minutes played."
        description_zh = f"将球员{_player_label_zh(field)}换算到每36分钟。"
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="efficiency",
                group_key="per_36_rates",
                min_sample=10,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="efficiency",
                    group_key="per_36_rates",
                    min_sample=10,
                    supports_career=True,
                    rank_order="asc" if field == "tov" else "desc",
                    metric_kind="per_36",
                    value_field=field,
                ),
            )
        )
    return specs


def make_player_ratio_specs() -> list[MetricSpec]:
    ratio_specs = [
        ("field_goal_pct", "Field Goal Percentage", "投篮命中率", "Field goal percentage on all field goal attempts.", "统计球员全部投篮命中率。", "efficiency", "efficiency_rates", 10, "desc", (("fgm", 1.0),), (("fga", 1.0),), 100.0, "fg_pct", 100),
        ("three_point_pct", "Three-Point Percentage", "三分命中率", "Three-point percentage on all three-point attempts.", "统计球员三分命中率。", "efficiency", "efficiency_rates", 10, "desc", (("fg3m", 1.0),), (("fg3a", 1.0),), 100.0, "3p_pct", 50),
        ("free_throw_pct", "Free Throw Percentage", "罚球命中率", "Free throw percentage on all free throw attempts.", "统计球员罚球命中率。", "efficiency", "efficiency_rates", 10, "desc", (("ftm", 1.0),), (("fta", 1.0),), 100.0, "ft_pct", 50),
        ("effective_field_goal_pct", "Effective Field Goal Percentage", "有效命中率", "Effective field goal percentage accounting for added value of threes.", "统计考虑三分附加价值后的有效命中率。", "efficiency", "efficiency_rates", 10, "desc", (("fgm", 1.0), ("fg3m", 0.5)), (("fga", 1.0),), 100.0, "efg_pct", 100),
        ("free_throw_rate", "Free Throw Rate", "罚球率", "Free throw attempts relative to field goal attempts.", "统计球员罚球出手相对投篮出手的比例。", "efficiency", "efficiency_rates", 10, "desc", (("fta", 1.0),), (("fga", 1.0),), 1.0, "ft_rate", 100),
        ("three_point_attempt_rate", "Three-Point Attempt Rate", "三分出手率", "Three-point attempts relative to field goal attempts.", "统计球员三分出手相对全部投篮出手的比例。", "efficiency", "efficiency_rates", 10, "desc", (("fg3a", 1.0),), (("fga", 1.0),), 1.0, "three_pa_rate", 100),
        ("turnover_rate", "Turnover Rate", "失误率", "Turnovers relative to offensive possessions used.", "统计球员失误占投篮/罚球/失误回合的比例。", "efficiency", "efficiency_rates", 10, "asc", (("tov", 1.0),), (("fga", 1.0), ("fta", 0.44), ("tov", 1.0)), 1.0, "turnover_rate", 100),
        ("assist_to_turnover_ratio_season", "Assist-To-Turnover Ratio (Season)", "赛季助攻失误比", "Assists divided by turnovers across the season.", "统计球员单赛季助攻失误比。", "efficiency", "efficiency_rates", 10, "desc", (("ast", 1.0),), (("tov", 1.0),), 1.0, "ast_to_tov_ratio", 1),
    ]
    specs: list[MetricSpec] = []
    for key, name, name_zh, description, description_zh, category, group_key, min_sample, rank_order, numerator_terms, denominator_terms, multiplier, _context_key, qualifier_min in ratio_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category=category,
                group_key=group_key,
                min_sample=min_sample,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category=category,
                    group_key=group_key,
                    min_sample=min_sample,
                    supports_career=True,
                    rank_order=rank_order,
                    metric_kind="ratio",
                    numerator_terms=numerator_terms,
                    denominator_terms=denominator_terms,
                    ratio_multiplier=multiplier,
                    qualifier_min=qualifier_min,
                    value_suffix="%" if multiplier == 100.0 else "",
                ),
            )
        )
    specs.append(
        build_metric_spec(
            key="zero_turnover_games",
            name="Zero Turnover Games",
            name_zh="零失误比赛",
            description="Count of games in a season with exactly zero turnovers.",
            description_zh="统计球员单赛季零失误比赛场次。",
            scope="player",
            category="efficiency",
            group_key="efficiency_rates",
            min_sample=1,
            supports_career=True,
            code_python=render_player_box_metric(
                class_name="ZeroTurnoverGames",
                key="zero_turnover_games",
                name="Zero Turnover Games",
                name_zh="零失误比赛",
                description="Count of games in a season with exactly zero turnovers.",
                description_zh="统计球员单赛季零失误比赛场次。",
                category="efficiency",
                group_key="efficiency_rates",
                min_sample=1,
                supports_career=True,
                metric_kind="count_exact",
                comparator="==",
                criteria=(("tov", 0.0),),
            ),
        )
    )
    specs.append(
        build_metric_spec(
            key="games_played",
            name="Games Played",
            name_zh="出场比赛",
            description="Total games played in a season.",
            description_zh="统计球员单赛季出场比赛数。",
            scope="player",
            category="aggregate",
            group_key="per_game_averages",
            min_sample=1,
            supports_career=True,
            code_python=render_player_box_metric(
                class_name="GamesPlayed",
                key="games_played",
                name="Games Played",
                name_zh="出场比赛",
                description="Total games played in a season.",
                description_zh="统计球员单赛季出场比赛数。",
                category="aggregate",
                group_key="per_game_averages",
                min_sample=1,
                supports_career=True,
                metric_kind="games_played",
            ),
        )
    )
    specs.append(
        build_metric_spec(
            key="games_started",
            name="Games Started",
            name_zh="首发场次",
            description="Total games started in a season.",
            description_zh="统计球员单赛季首发场次。",
            scope="player",
            category="aggregate",
            group_key="per_game_averages",
            min_sample=1,
            supports_career=True,
            code_python=render_player_box_metric(
                class_name="GamesStarted",
                key="games_started",
                name="Games Started",
                name_zh="首发场次",
                description="Total games started in a season.",
                description_zh="统计球员单赛季首发场次。",
                category="aggregate",
                group_key="per_game_averages",
                min_sample=1,
                supports_career=True,
                metric_kind="games_started",
            ),
        )
    )
    specs.append(
        build_metric_spec(
            key="win_rate_as_starter",
            name="Win Rate As Starter",
            name_zh="首发胜率",
            description="Winning percentage in games started.",
            description_zh="统计球员首发比赛中的胜率。",
            scope="player",
            category="conditional",
            group_key="per_game_averages",
            min_sample=5,
            supports_career=True,
            code_python=render_player_box_metric(
                class_name="WinRateAsStarter",
                key="win_rate_as_starter",
                name="Win Rate As Starter",
                name_zh="首发胜率",
                description="Winning percentage in games started.",
                description_zh="统计球员首发比赛中的胜率。",
                category="conditional",
                group_key="per_game_averages",
                min_sample=5,
                supports_career=True,
                metric_kind="win_pct",
            ),
        )
    )
    return specs


def make_player_combo_specs() -> list[MetricSpec]:
    combos = [
        ("twenty_ten_games", "20-10 Games", "20+10比赛", "Count of games with at least 20 points and 10 rebounds.", "统计单场至少20分10篮板的比赛场次。", (("pts", 20.0), ("reb", 10.0))),
        ("ten_ten_games", "10-10 Games", "10+10比赛", "Count of games with at least 10 points and 10 rebounds.", "统计单场至少10分10篮板的比赛场次。", (("pts", 10.0), ("reb", 10.0))),
        ("fifteen_ten_games", "15-10 Games", "15+10比赛", "Count of games with at least 15 points and 10 rebounds.", "统计单场至少15分10篮板的比赛场次。", (("pts", 15.0), ("reb", 10.0))),
        ("twenty_five_ten_games", "25-10 Games", "25+10比赛", "Count of games with at least 25 points and 10 rebounds.", "统计单场至少25分10篮板的比赛场次。", (("pts", 25.0), ("reb", 10.0))),
        ("thirty_ten_games", "30-10 Games", "30+10比赛", "Count of games with at least 30 points and 10 rebounds.", "统计单场至少30分10篮板的比赛场次。", (("pts", 30.0), ("reb", 10.0))),
        ("twenty_five_five_five_games", "25-5-5 Games", "25+5+5比赛", "Count of games with at least 25 points, 5 rebounds, and 5 assists.", "统计单场至少25分5篮板5助攻的比赛场次。", (("pts", 25.0), ("reb", 5.0), ("ast", 5.0))),
        ("thirty_five_five_five_games", "35-5-5 Games", "35+5+5比赛", "Count of games with at least 35 points, 5 rebounds, and 5 assists.", "统计单场至少35分5篮板5助攻的比赛场次。", (("pts", 35.0), ("reb", 5.0), ("ast", 5.0))),
        ("five_five_five_games", "5-5-5 Games", "5+5+5比赛", "Count of games with at least 5 points, 5 rebounds, and 5 assists.", "统计单场至少5分5篮板5助攻的比赛场次。", (("pts", 5.0), ("reb", 5.0), ("ast", 5.0))),
        ("twenty_points_ten_assists_games", "20 Points 10 Assists Games", "20分10助攻比赛", "Count of games with at least 20 points and 10 assists.", "统计单场至少20分10助攻的比赛场次。", (("pts", 20.0), ("ast", 10.0))),
        ("ten_rebounds_ten_assists_games", "10 Rebounds 10 Assists Games", "10篮板10助攻比赛", "Count of games with at least 10 rebounds and 10 assists.", "统计单场至少10篮板10助攻的比赛场次。", (("reb", 10.0), ("ast", 10.0))),
        ("three_steal_three_block_games", "3 Steal 3 Block Games", "3抢断3盖帽比赛", "Count of games with at least 3 steals and 3 blocks.", "统计单场至少3抢断3盖帽的比赛场次。", (("stl", 3.0), ("blk", 3.0))),
        ("double_digit_scoring_five_assist_games", "Double-Digit Scoring 5 Assist Games", "上双得分5助攻比赛", "Count of games with at least 10 points and 5 assists.", "统计单场至少10分5助攻的比赛场次。", (("pts", 10.0), ("ast", 5.0))),
    ]
    specs: list[MetricSpec] = []
    for key, name, name_zh, description, description_zh, criteria in combos:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="aggregate",
                group_key="combo_stats",
                min_sample=1,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="aggregate",
                    group_key="combo_stats",
                    min_sample=1,
                    supports_career=True,
                    metric_kind="count_combo",
                    criteria=criteria,
                ),
            )
        )
    return specs


def make_player_split_specs() -> list[MetricSpec]:
    split_specs = [
        ("points_in_wins", "Points Per Game In Wins", "胜场场均得分", "Average points per game in team wins.", "统计球员在球队获胜比赛中的场均得分。", "pts", "wins", "desc"),
        ("points_in_losses", "Points Per Game In Losses", "败场场均得分", "Average points per game in team losses.", "统计球员在球队失利比赛中的场均得分。", "pts", "losses", "desc"),
        ("assists_in_wins", "Assists Per Game In Wins", "胜场场均助攻", "Average assists per game in team wins.", "统计球员在球队获胜比赛中的场均助攻。", "ast", "wins", "desc"),
        ("rebounds_in_wins", "Rebounds Per Game In Wins", "胜场场均篮板", "Average rebounds per game in team wins.", "统计球员在球队获胜比赛中的场均篮板。", "reb", "wins", "desc"),
        ("home_ppg", "Home Points Per Game", "主场场均得分", "Average points per game in home games.", "统计球员主场比赛场均得分。", "pts", "home", "desc"),
        ("road_ppg", "Road Points Per Game", "客场场均得分", "Average points per game in road games.", "统计球员客场比赛场均得分。", "pts", "road", "desc"),
        ("home_apg", "Home Assists Per Game", "主场场均助攻", "Average assists per game in home games.", "统计球员主场比赛场均助攻。", "ast", "home", "desc"),
        ("road_apg", "Road Assists Per Game", "客场场均助攻", "Average assists per game in road games.", "统计球员客场比赛场均助攻。", "ast", "road", "desc"),
        ("home_rpg", "Home Rebounds Per Game", "主场场均篮板", "Average rebounds per game in home games.", "统计球员主场比赛场均篮板。", "reb", "home", "desc"),
        ("road_rpg", "Road Rebounds Per Game", "客场场均篮板", "Average rebounds per game in road games.", "统计球员客场比赛场均篮板。", "reb", "road", "desc"),
        ("bench_points_per_game", "Bench Points Per Game", "替补场均得分", "Average points per game when coming off the bench.", "统计球员替补出场时的场均得分。", "pts", "bench", "desc"),
        ("starter_points_per_game", "Starter Points Per Game", "首发场均得分", "Average points per game as a starter.", "统计球员首发时的场均得分。", "pts", "starter", "desc"),
    ]
    specs: list[MetricSpec] = []
    for key, name, name_zh, description, description_zh, field, split_key, rank_order in split_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="conditional",
                group_key="win_loss_splits",
                min_sample=5,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="conditional",
                    group_key="win_loss_splits",
                    min_sample=5,
                    supports_career=True,
                    rank_order=rank_order,
                    metric_kind="split_avg",
                    value_field=field,
                    split_key=split_key,
                ),
            )
        )
    return specs


def make_player_streak_specs() -> list[MetricSpec]:
    streaks = [
        ("max_consecutive_20pt_games", "Longest 20+ Point Streak", "最长20+得分连续场次", "Longest streak of consecutive games with at least 20 points.", "统计球员连续至少20分的最长场次。", (("pts", 20.0),)),
        ("max_consecutive_double_digit_games", "Longest Double-Digit Scoring Streak", "最长上双连续场次", "Longest streak of consecutive games with at least 10 points.", "统计球员连续至少10分的最长场次。", (("pts", 10.0),)),
        ("max_consecutive_30pt_games", "Longest 30+ Point Streak", "最长30+得分连续场次", "Longest streak of consecutive games with at least 30 points.", "统计球员连续至少30分的最长场次。", (("pts", 30.0),)),
        ("max_consecutive_double_double_streak", "Longest Double-Double Streak", "最长两双连续场次", "Longest streak of consecutive double-doubles.", "统计球员连续两双的最长场次。", (("pts", 10.0), ("reb", 10.0))),
        ("max_consecutive_three_pm_streak", "Longest 3+ 3PM Streak", "最长3记三分连续场次", "Longest streak of consecutive games with at least 3 made threes.", "统计球员连续至少命中3记三分的最长场次。", (("fg3m", 3.0),)),
        ("max_consecutive_five_assist_streak", "Longest 5+ Assist Streak", "最长5助攻连续场次", "Longest streak of consecutive games with at least 5 assists.", "统计球员连续至少5次助攻的最长场次。", (("ast", 5.0),)),
    ]
    specs: list[MetricSpec] = []
    for key, name, name_zh, description, description_zh, criteria in streaks:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="record",
                group_key="scoring_streaks",
                min_sample=1,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="record",
                    group_key="scoring_streaks",
                    min_sample=1,
                    supports_career=True,
                    metric_kind="streak",
                    criteria=criteria,
                ),
            )
        )
    return specs


def make_player_record_specs() -> list[MetricSpec]:
    record_specs = [
        ("best_single_game_reb", "Best Single-Game Rebounds", "单场最高篮板", "Season-high rebounds in a single game.", "统计球员单赛季单场最高篮板。", "reb", "season_records", "record", "desc", None, None, "max"),
        ("best_single_game_ast", "Best Single-Game Assists", "单场最高助攻", "Season-high assists in a single game.", "统计球员单赛季单场最高助攻。", "ast", "season_records", "record", "desc", None, None, "max"),
        ("best_single_game_stl", "Best Single-Game Steals", "单场最高抢断", "Season-high steals in a single game.", "统计球员单赛季单场最高抢断。", "stl", "season_records", "record", "desc", None, None, "max"),
        ("best_single_game_blk", "Best Single-Game Blocks", "单场最高盖帽", "Season-high blocks in a single game.", "统计球员单赛季单场最高盖帽。", "blk", "season_records", "record", "desc", None, None, "max"),
        ("best_single_game_plus_minus", "Best Single-Game Plus-Minus", "单场最高正负值", "Season-high plus-minus in a single game.", "统计球员单赛季单场最高正负值。", "plus", "season_records", "record", "desc", None, None, "max"),
        ("worst_single_game_plus_minus", "Worst Single-Game Plus-Minus", "单场最低正负值", "Season-low plus-minus in a single game.", "统计球员单赛季单场最低正负值。", "plus", "season_records", "record", "asc", None, None, "min"),
        ("best_single_game_three_pm", "Best Single-Game 3PM", "单场最高三分命中", "Season-high made threes in a single game.", "统计球员单赛季单场最高三分命中数。", "fg3m", "season_records", "record", "desc", None, None, "max"),
        ("best_single_game_ftm", "Best Single-Game FTM", "单场最高罚球命中", "Season-high free throws made in a single game.", "统计球员单赛季单场最高罚球命中数。", "ftm", "season_records", "record", "desc", None, None, "max"),
        ("highest_fg_pct_in_game", "Highest FG% In A Game", "单场最高命中率", "Best single-game field goal percentage with at least 8 attempts.", "统计单场至少8次出手时的最高投篮命中率。", "fgm", "season_records", "efficiency", "desc", "fga", 8, "max"),
    ]
    specs: list[MetricSpec] = []
    for key, name, name_zh, description, description_zh, field, group_key, category, rank_order, qualifier_field, qualifier_min, record_mode in record_specs:
        value_field = field
        if key == "highest_fg_pct_in_game":
            value_field = "fg_pct"
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category=category,
                group_key=group_key,
                min_sample=1,
                supports_career=True,
                code_python=render_player_box_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category=category,
                    group_key=group_key,
                    min_sample=1,
                    supports_career=True,
                    rank_order=rank_order,
                    metric_kind="single_game_record",
                    value_field=value_field,
                    qualifier_field=qualifier_field,
                    qualifier_min=qualifier_min,
                    record_mode=record_mode,
                    career_max_keys=("best_value",) if record_mode == "max" else (),
                    career_min_keys=("best_value",) if record_mode == "min" else (),
                ),
            )
        )
    return specs


def make_player_shot_specs() -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    fg_pct_specs = [
        ("restricted_area_fg_pct", "Restricted Area FG%", "合理冲撞区命中率", "Field goal percentage in the restricted area.", "统计球员在合理冲撞区的命中率。", ("Restricted Area",), 50),
        ("paint_fg_pct", "Paint FG%", "油漆区命中率", "Field goal percentage in the paint outside the restricted area.", "统计球员在油漆区非合理冲撞区的命中率。", ("In The Paint (Non-RA)",), 30),
        ("midrange_fg_pct", "Mid-Range FG%", "中距离命中率", "Field goal percentage on mid-range attempts.", "统计球员中距离出手命中率。", ("Mid-Range",), 50),
        ("left_corner_three_pct", "Left Corner 3 FG%", "左底角三分命中率", "Field goal percentage on left corner threes.", "统计球员左侧底角三分命中率。", ("Left Corner 3",), 20),
        ("right_corner_three_pct", "Right Corner 3 FG%", "右底角三分命中率", "Field goal percentage on right corner threes.", "统计球员右侧底角三分命中率。", ("Right Corner 3",), 20),
        ("corner_three_pct", "Corner 3 FG%", "底角三分命中率", "Field goal percentage on corner threes.", "统计球员底角三分命中率。", ("Left Corner 3", "Right Corner 3"), 30),
        ("above_break_three_pct", "Above-the-Break 3 FG%", "弧顶三分命中率", "Field goal percentage on above-the-break threes.", "统计球员弧顶三分命中率。", ("Above the Break 3",), 50),
    ]
    for key, name, name_zh, description, description_zh, zones, qualifier_min in fg_pct_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="efficiency",
                group_key="shot_zones",
                min_sample=1,
                supports_career=True,
                code_python=render_player_shot_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="efficiency",
                    group_key="shot_zones",
                    min_sample=1,
                    supports_career=True,
                    metric_kind="fg_pct",
                    zones=zones,
                    qualifier_min=qualifier_min,
                ),
            )
        )
    share_specs = [
        ("corner_three_share", "Corner 3 Share", "底角三分占比", "Corner-three attempts as a share of all three-point attempts.", "统计底角三分出手占全部三分出手的比例。", ("Left Corner 3", "Right Corner 3"), "three_attempts"),
        ("above_break_three_share", "Above-the-Break 3 Share", "弧顶三分占比", "Above-the-break three attempts as a share of all threes.", "统计弧顶三分出手占全部三分出手的比例。", ("Above the Break 3",), "three_attempts"),
        ("three_point_shot_share", "Three-Point Shot Share", "三分出手占比", "Three-point attempts as a share of all field goal attempts.", "统计三分出手占全部投篮出手的比例。", ("Left Corner 3", "Right Corner 3", "Above the Break 3"), "all_attempts"),
        ("midrange_shot_share", "Mid-Range Shot Share", "中距离出手占比", "Mid-range attempts as a share of all field goal attempts.", "统计中距离出手占全部投篮出手的比例。", ("Mid-Range",), "all_attempts"),
    ]
    for key, name, name_zh, description, description_zh, zones, denominator_kind in share_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="player",
                category="scoring",
                group_key="shot_zones",
                min_sample=10,
                supports_career=True,
                code_python=render_player_shot_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="scoring",
                    group_key="shot_zones",
                    min_sample=10,
                    supports_career=True,
                    metric_kind="share",
                    zones=zones,
                    denominator_kind=denominator_kind,
                ),
            )
        )
    specs.append(
        build_metric_spec(
            key="shot_distance_avg",
            name="Average Shot Distance",
            name_zh="平均出手距离",
            description="Average shot distance in feet across all attempts.",
            description_zh="统计球员全部出手的平均出手距离。",
            scope="player",
            category="scoring",
            group_key="shot_zones",
            min_sample=25,
            supports_career=True,
            code_python=render_player_shot_metric(
                class_name="ShotDistanceAvg",
                key="shot_distance_avg",
                name="Average Shot Distance",
                name_zh="平均出手距离",
                description="Average shot distance in feet across all attempts.",
                description_zh="统计球员全部出手的平均出手距离。",
                category="scoring",
                group_key="shot_zones",
                min_sample=25,
                supports_career=True,
                metric_kind="distance",
            ),
        )
    )
    return specs


def make_award_specs() -> list[MetricSpec]:
    award_specs = [
        ("all_star_appearances", "All-Star Appearances", "全明星次数", "Number of All-Star selections in a season bucket.", "统计球员在该赛季桶中的全明星入选次数。", ("all_star",), "awards"),
        ("championship_rings", "Championship Rings", "总冠军次数", "Number of championship-winning seasons in a season bucket.", "统计球员在该赛季桶中的冠军赛季次数。", ("champion",), "awards"),
        ("mvp_awards", "MVP Awards", "MVP次数", "Number of MVP awards in a season bucket.", "统计球员在该赛季桶中的MVP次数。", ("mvp",), "awards"),
        ("finals_mvp_awards", "Finals MVP Awards", "总决赛MVP次数", "Number of Finals MVP awards in a season bucket.", "统计球员在该赛季桶中的总决赛MVP次数。", ("finals_mvp",), "awards"),
        ("dpoy_awards", "Defensive Player Of The Year Awards", "最佳防守球员次数", "Number of DPOY awards in a season bucket.", "统计球员在该赛季桶中的最佳防守球员次数。", ("dpoy",), "awards"),
        ("sixth_man_awards", "Sixth Man Awards", "最佳第六人次数", "Number of Sixth Man awards in a season bucket.", "统计球员在该赛季桶中的最佳第六人次数。", ("sixth_man",), "awards"),
        ("most_improved_awards", "Most Improved Awards", "进步最快球员次数", "Number of Most Improved Player awards in a season bucket.", "统计球员在该赛季桶中的进步最快球员次数。", ("mip",), "awards"),
        ("rookie_of_year_awards", "Rookie Of The Year Awards", "最佳新秀次数", "Number of Rookie of the Year awards in a season bucket.", "统计球员在该赛季桶中的最佳新秀次数。", ("roy",), "awards"),
        ("all_nba_appearances", "All-NBA Appearances", "最佳阵容次数", "Number of All-NBA selections in a season bucket.", "统计球员在该赛季桶中的最佳阵容入选次数。", ("all_nba_first", "all_nba_second", "all_nba_third"), "awards"),
        ("all_defensive_appearances", "All-Defensive Appearances", "最佳防守阵容次数", "Number of All-Defensive selections in a season bucket.", "统计球员在该赛季桶中的最佳防守阵容入选次数。", ("all_defensive_first", "all_defensive_second"), "awards"),
    ]
    return [
        build_metric_spec(
            key=key,
            name=name,
            name_zh=name_zh,
            description=description,
            description_zh=description_zh,
            scope="player",
            category="record",
            group_key=group_key,
            min_sample=1,
            supports_career=True,
            code_python=render_award_metric(
                class_name=camelize(key),
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                award_types=award_types,
                group_key=group_key,
            ),
        )
        for key, name, name_zh, description, description_zh, award_types, group_key in award_specs
    ]


def make_salary_specs() -> list[MetricSpec]:
    salary_specs = [
        ("salary_per_point", "Salary Per Point", "每分薪资", "Salary paid per point scored in the season.", "统计球员该赛季每得1分对应的薪资。", "salary_per_point", "asc"),
        ("points_per_million", "Points Per Million Dollars", "每百万美元得分", "Points scored per $1M salary.", "统计球员每百万美元薪资对应的得分。", "points_per_million", "desc"),
        ("salary_per_game", "Salary Per Game", "每场薪资", "Salary paid per game played.", "统计球员每场出战对应的薪资。", "salary_per_game", "asc"),
        ("pra_per_million", "PRA Per Million Dollars", "每百万美元PRA", "Points + rebounds + assists produced per $1M salary.", "统计球员每百万美元薪资对应的PRA产出。", "pra_per_million", "desc"),
        ("ppg_per_million", "PPG Per Million Dollars", "每百万美元场均得分", "Points per game per $1M salary.", "统计球员每百万美元薪资对应的场均得分。", "ppg_per_million", "desc"),
    ]
    return [
        build_metric_spec(
            key=key,
            name=name,
            name_zh=name_zh,
            description=description,
            description_zh=description_zh,
            scope="player",
            category="efficiency",
            group_key="salary_efficiency",
            min_sample=1,
            supports_career=False,
            code_python=render_salary_metric(
                class_name=camelize(key),
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                group_key="salary_efficiency",
                metric_kind=metric_kind,
                rank_order=rank_order,
            ),
        )
        for key, name, name_zh, description, description_zh, metric_kind, rank_order in salary_specs
    ]


def make_team_specs() -> list[MetricSpec]:
    specs: list[MetricSpec] = []
    per_game_fields = [
        ("team_ppg", "Team Points Per Game", "球队场均得分", "Average team points scored per game.", "统计球队场均得分。", "pts", "team_averages", "aggregate", "desc"),
        ("team_opp_ppg", "Opponent Points Allowed Per Game", "对手场均得分", "Average opponent points allowed per game.", "统计球队每场让对手得到的平均分。", "opp_pts", "team_averages", "defense", "asc"),
        ("team_point_diff", "Point Differential Per Game", "场均净胜分", "Average point differential per game.", "统计球队场均净胜分。", "point_diff", "team_averages", "aggregate", "desc"),
        ("team_rebounds_per_game", "Team Rebounds Per Game", "球队场均篮板", "Average team rebounds per game.", "统计球队场均篮板。", "reb", "team_averages", "aggregate", "desc"),
        ("team_assists_per_game", "Team Assists Per Game", "球队场均助攻", "Average team assists per game.", "统计球队场均助攻。", "ast", "team_averages", "aggregate", "desc"),
        ("team_turnovers_per_game", "Team Turnovers Per Game", "球队场均失误", "Average team turnovers per game.", "统计球队场均失误。", "tov", "team_averages", "efficiency", "asc"),
        ("team_steals_per_game", "Team Steals Per Game", "球队场均抢断", "Average team steals per game.", "统计球队场均抢断。", "stl", "team_averages", "aggregate", "desc"),
        ("team_blocks_per_game", "Team Blocks Per Game", "球队场均盖帽", "Average team blocks per game.", "统计球队场均盖帽。", "blk", "team_averages", "aggregate", "desc"),
        ("team_opp_fg_pct", "Opponent FG%", "对手命中率", "Average opponent field goal percentage.", "统计球队让对手打出的平均命中率。", "opp_fg_pct", "team_averages", "defense", "asc"),
        ("team_fg_pct", "Team FG%", "球队命中率", "Average team field goal percentage.", "统计球队平均投篮命中率。", "fg_pct", "team_averages", "efficiency", "desc"),
        ("team_three_pct", "Team 3P%", "球队三分命中率", "Average team three-point percentage.", "统计球队平均三分命中率。", "fg3_pct", "team_averages", "efficiency", "desc"),
        ("team_ft_pct", "Team FT%", "球队罚球命中率", "Average team free throw percentage.", "统计球队平均罚球命中率。", "ft_pct", "team_averages", "efficiency", "desc"),
        ("team_three_made_per_game", "Team 3PM Per Game", "球队场均三分命中", "Average made threes per game.", "统计球队场均三分命中数。", "fg3m", "team_averages", "aggregate", "desc"),
        ("team_three_attempts_per_game", "Team 3PA Per Game", "球队场均三分出手", "Average three-point attempts per game.", "统计球队场均三分出手数。", "fg3a", "team_averages", "aggregate", "desc"),
    ]
    for key, name, name_zh, description, description_zh, stat_field, group_key, category, rank_order in per_game_fields:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="team",
                category=category,
                group_key=group_key,
                min_sample=5,
                supports_career=True,
                code_python=render_team_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category=category,
                    group_key=group_key,
                    min_sample=5,
                    supports_career=True,
                    rank_order=rank_order,
                    metric_kind="per_game",
                    stat_field=stat_field,
                ),
            )
        )
    count_specs = [
        ("wins_total", "Wins", "胜场", "Total wins in the season.", "统计球队赛季总胜场。", "win", None, "team_records", "aggregate", "desc"),
        ("losses_total", "Losses", "败场", "Total losses in the season.", "统计球队赛季总败场。", "loss", None, "team_records", "aggregate", "asc"),
        ("wins_by_10_plus", "Wins By 10+", "净胜10分胜场", "Wins by at least 10 points.", "统计球队净胜至少10分的胜场。", "blowout_win", 1, "team_records", "aggregate", "desc"),
        ("wins_by_20_plus", "Wins By 20+", "净胜20分胜场", "Wins by at least 20 points.", "统计球队净胜至少20分的胜场。", "point_diff", 20, "team_records", "aggregate", "desc"),
        ("losses_by_10_plus", "Losses By 10+", "净负10分败场", "Losses by at least 10 points.", "统计球队净负至少10分的败场。", "blowout_loss", 1, "team_records", "aggregate", "asc"),
        ("close_game_wins", "Close Game Wins", "关键球胜场", "Wins in games decided by five points or fewer.", "统计球队分差不超过5分的胜场。", "close_game", 1, "team_records", "aggregate", "desc"),
        ("total_games_played_team", "Total Games Played", "总比赛场次", "Total games played in the season.", "统计球队赛季总比赛场次。", "win", None, "team_records", "aggregate", "desc"),
    ]
    for key, name, name_zh, description, description_zh, stat_field, threshold, group_key, category, rank_order in count_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="team",
                category=category,
                group_key=group_key,
                min_sample=1,
                supports_career=True,
                code_python=render_team_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category=category,
                    group_key=group_key,
                    min_sample=1,
                    supports_career=True,
                    rank_order=rank_order,
                    metric_kind="count",
                    stat_field=stat_field,
                    threshold=threshold,
                ),
            )
        )
    split_specs = [
        ("home_win_pct", "Home Win Percentage", "主场胜率", "Win percentage in home games.", "统计球队主场胜率。", "home", "team_records", "conditional"),
        ("road_win_pct_season", "Road Win Percentage (Season)", "客场胜率", "Win percentage in road games.", "统计球队客场胜率。", "road", "team_records", "conditional"),
        ("home_points_per_game", "Home Points Per Game", "主场场均得分", "Average points per game in home games.", "统计球队主场场均得分。", "home", "team_splits", "aggregate"),
        ("road_points_per_game", "Road Points Per Game", "客场场均得分", "Average points per game in road games.", "统计球队客场场均得分。", "road", "team_splits", "aggregate"),
        ("home_points_allowed_per_game", "Home Points Allowed Per Game", "主场场均失分", "Average opponent points allowed in home games.", "统计球队主场场均失分。", "home", "team_splits", "defense"),
        ("road_points_allowed_per_game", "Road Points Allowed Per Game", "客场场均失分", "Average opponent points allowed in road games.", "统计球队客场场均失分。", "road", "team_splits", "defense"),
    ]
    for key, name, name_zh, description, description_zh, split_key, group_key, category in split_specs:
        metric_kind = "win_pct" if "win percentage" in description.lower() else "split_avg"
        stat_field = "win" if metric_kind == "win_pct" else "opp_pts" if "Allowed" in name else "pts"
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="team",
                category=category,
                group_key=group_key,
                min_sample=5,
                supports_career=True,
                code_python=render_team_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category=category,
                    group_key=group_key,
                    min_sample=5,
                    supports_career=True,
                    rank_order="desc" if category != "defense" else "asc",
                    metric_kind=metric_kind,
                    stat_field=stat_field,
                    split_key=split_key,
                ),
            )
        )
    ratio_specs = [
        ("team_assist_to_turnover_ratio", "Team Assist-To-Turnover Ratio", "球队助攻失误比", "Team assists divided by turnovers.", "统计球队助攻失误比。", (("ast", 1.0),), (("tov", 1.0),), "team_efficiency"),
        ("team_effective_fg_pct", "Team Effective FG%", "球队有效命中率", "Team effective field goal percentage.", "统计球队有效命中率。", (("fgm", 1.0), ("fg3m", 0.5)), (("fga", 1.0),), "team_efficiency"),
        ("team_three_rate", "Team Three-Point Rate", "球队三分出手率", "Three-point attempts relative to all field goal attempts.", "统计球队三分出手率。", (("fg3a", 1.0),), (("fga", 1.0),), "team_efficiency"),
        ("team_free_throw_rate", "Team Free Throw Rate", "球队罚球率", "Free throw attempts relative to field goal attempts.", "统计球队罚球率。", (("fta", 1.0),), (("fga", 1.0),), "team_efficiency"),
    ]
    for key, name, name_zh, description, description_zh, numerator, denominator, group_key in ratio_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="team",
                category="efficiency",
                group_key=group_key,
                min_sample=5,
                supports_career=True,
                code_python=render_team_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="efficiency",
                    group_key=group_key,
                    min_sample=5,
                    supports_career=True,
                    metric_kind="ratio",
                    ratio_numerator=numerator,
                    ratio_denominator=denominator,
                    ratio_multiplier=100.0 if "pct" in key or "rate" in key else 1.0,
                ),
            )
        )
    streak_specs = [
        ("longest_win_streak", "Longest Win Streak", "最长连胜", "Longest consecutive win streak in the season.", "统计球队赛季最长连胜场次。", "win"),
        ("longest_losing_streak", "Longest Losing Streak", "最长连败", "Longest consecutive losing streak in the season.", "统计球队赛季最长连败场次。", "loss"),
        ("longest_home_win_streak", "Longest Home Win Streak", "最长主场连胜", "Longest consecutive home win streak.", "统计球队主场最长连胜场次。", "win"),
        ("longest_road_win_streak", "Longest Road Win Streak", "最长客场连胜", "Longest consecutive road win streak.", "统计球队客场最长连胜场次。", "win"),
    ]
    for key, name, name_zh, description, description_zh, stat_field in streak_specs:
        split_key = "home" if "Home" in name else "road" if "Road" in name else None
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="team",
                category="record",
                group_key="team_streaks",
                min_sample=1,
                supports_career=True,
                code_python=render_team_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="record",
                    group_key="team_streaks",
                    min_sample=1,
                    supports_career=True,
                    metric_kind="streak",
                    stat_field=stat_field,
                    split_key=split_key,
                ),
            )
        )
    bench_specs = [
        ("bench_ppg_team", "Bench Points Per Game", "替补场均得分", "Average bench points per game.", "统计球队替补席场均得分。", "per_game", "bench", "desc"),
        ("starter_ppg_team", "Starter Points Per Game", "首发场均得分", "Average starter points per game.", "统计球队首发阵容场均得分。", "per_game", "starter", "desc"),
        ("bench_scoring_share_team", "Bench Scoring Share", "替补得分占比", "Bench points as a share of team scoring.", "统计球队替补得分占球队总得分的比例。", "share", "bench", "desc"),
        ("starter_scoring_share_team", "Starter Scoring Share", "首发得分占比", "Starter points as a share of team scoring.", "统计球队首发得分占球队总得分的比例。", "share", "starter", "desc"),
    ]
    for key, name, name_zh, description, description_zh, metric_kind, role, rank_order in bench_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="team",
                category="aggregate",
                group_key="team_efficiency",
                min_sample=5,
                supports_career=True,
                code_python=render_team_bench_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    group_key="team_efficiency",
                    min_sample=5,
                    rank_order=rank_order,
                    metric_kind=metric_kind,
                    role=role,
                ),
            )
        )
    return specs


def make_game_specs() -> list[MetricSpec]:
    combined_specs = [
        ("game_total_three_pointers", "Game Total Three-Pointers", "比赛总三分命中", "Combined made threes by both teams.", "统计两队合计三分命中数。", "fg3m"),
        ("game_total_assists", "Game Total Assists", "比赛总助攻", "Combined assists by both teams.", "统计两队合计助攻数。", "ast"),
        ("game_total_turnovers", "Game Total Turnovers", "比赛总失误", "Combined turnovers by both teams.", "统计两队合计失误数。", "tov"),
        ("game_total_rebounds", "Game Total Rebounds", "比赛总篮板", "Combined rebounds by both teams.", "统计两队合计篮板数。", "reb"),
        ("game_total_blocks", "Game Total Blocks", "比赛总盖帽", "Combined blocks by both teams.", "统计两队合计盖帽数。", "blk"),
        ("game_total_steals", "Game Total Steals", "比赛总抢断", "Combined steals by both teams.", "统计两队合计抢断数。", "stl"),
        ("game_total_points", "Game Total Points", "比赛总得分", "Combined points by both teams.", "统计两队合计得分。", "pts"),
        ("game_total_free_throw_attempts", "Game Total Free Throw Attempts", "比赛总罚球出手", "Combined free throw attempts by both teams.", "统计两队合计罚球出手数。", "fta"),
    ]
    specs: list[MetricSpec] = []
    for key, name, name_zh, description, description_zh, stat_field in combined_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="game",
                category="aggregate",
                group_key="game_scoring",
                min_sample=1,
                supports_career=False,
                code_python=render_game_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="aggregate",
                    group_key="game_scoring",
                    rank_order="desc",
                    metric_kind="combined",
                    stat_field=stat_field,
                ),
            )
        )
    other_specs = [
        ("game_free_throw_disparity", "Game Free Throw Disparity", "比赛罚球差值", "Absolute difference in free throw attempts between teams.", "统计两队罚球出手的绝对差值。", "game_fairness", "desc", "disparity", "fta"),
        ("game_turnover_disparity", "Game Turnover Disparity", "比赛失误差值", "Absolute difference in turnovers between teams.", "统计两队失误数的绝对差值。", "game_fairness", "desc", "disparity", "tov"),
        ("game_rebound_disparity", "Game Rebound Disparity", "比赛篮板差值", "Absolute difference in rebounds between teams.", "统计两队篮板数的绝对差值。", "game_fairness", "desc", "disparity", "reb"),
        ("game_margin", "Game Final Margin", "比赛最终分差", "Final scoring margin.", "统计比赛最终分差。", "game_momentum", "desc", "margin", None),
        ("game_halftime_margin", "Game Halftime Margin", "半场分差", "Absolute halftime score difference.", "统计半场绝对分差。", "game_momentum", "desc", "halftime_margin", None),
        ("game_overtime_periods", "Game Overtime Periods", "加时次数", "Number of overtime periods in the game.", "统计比赛加时次数。", "game_overtime", "desc", "overtime_periods", None),
        ("largest_winning_team_lead", "Largest Lead In Game", "比赛最大领先", "Largest lead held by either team during the game.", "统计比赛过程中的最大领先分差。", "game_momentum", "desc", "largest_lead", None),
    ]
    for key, name, name_zh, description, description_zh, group_key, rank_order, metric_kind, stat_field in other_specs:
        specs.append(
            build_metric_spec(
                key=key,
                name=name,
                name_zh=name_zh,
                description=description,
                description_zh=description_zh,
                scope="game",
                category="aggregate",
                group_key=group_key,
                min_sample=1,
                supports_career=False,
                code_python=render_game_metric(
                    class_name=camelize(key),
                    key=key,
                    name=name,
                    name_zh=name_zh,
                    description=description,
                    description_zh=description_zh,
                    category="aggregate",
                    group_key=group_key,
                    rank_order=rank_order,
                    metric_kind=metric_kind,
                    stat_field=stat_field,
                ),
            )
    )
    return specs


def build_metric_specs() -> list[MetricSpec]:
    specs = [
        *make_player_threshold_specs(),
        *make_player_average_specs(),
        *make_player_total_specs(),
        *make_player_per36_specs(),
        *make_player_ratio_specs(),
        *make_player_combo_specs(),
        *make_player_split_specs(),
        *make_player_streak_specs(),
        *make_player_record_specs(),
        *make_player_shot_specs(),
        *make_award_specs(),
        *make_salary_specs(),
        *make_team_specs(),
        *make_game_specs(),
    ]
    deduped: list[MetricSpec] = []
    seen: set[str] = set()
    for spec in specs:
        if spec.key in seen:
            continue
        seen.add(spec.key)
        deduped.append(spec)
    return deduped


def validate_specs(specs: list[MetricSpec]) -> None:
    seen: set[str] = set()
    for spec in specs:
        if spec.key in seen:
            raise ValueError(f"Duplicate metric key generated: {spec.key}")
        seen.add(spec.key)
        if spec.max_results_per_season != DEFAULT_LIMIT:
            raise ValueError(f"{spec.key} max_results_per_season must be {DEFAULT_LIMIT}")
        metric = load_code_metric(spec.code_python)
        if metric.key != spec.key:
            raise ValueError(f"{spec.key} loaded as {metric.key}")
        if getattr(metric, "trigger", None) != "season":
            raise ValueError(f"{spec.key} is not season-triggered")
        if getattr(metric, "max_results_per_season", None) != DEFAULT_LIMIT:
            raise ValueError(f"{spec.key} does not cap results at {DEFAULT_LIMIT}")


def existing_rows_by_key(session) -> dict[str, MetricDefinitionModel]:
    rows = session.query(MetricDefinitionModel).all()
    return {row.key: row for row in rows}


def insert_specs(specs: list[MetricSpec]) -> tuple[list[str], list[str], list[str]]:
    now = datetime.now(UTC).replace(tzinfo=None)
    inserted: list[str] = []
    updated: list[str] = []
    skipped: list[str] = []

    with SessionLocal() as session:
        existing = existing_rows_by_key(session)
        for spec in specs:
            row = existing.get(spec.key)
            if row is not None and (row.expression or "") not in ("", SCRIPT_MARKER):
                skipped.append(spec.key)
                continue
            values = {
                "key": spec.key,
                "family_key": spec.key,
                "variant": "season",
                "managed_family": False,
                "name": spec.name,
                "name_zh": spec.name_zh,
                "description": spec.description,
                "description_zh": spec.description_zh,
                "scope": spec.scope,
                "category": spec.category,
                "group_key": spec.group_key,
                "source_type": "code",
                "status": "published",
                "code_python": spec.code_python,
                "context_label_template": spec.context_label_template,
                "expression": SCRIPT_MARKER,
                "min_sample": spec.min_sample,
                "max_results_per_season": spec.max_results_per_season,
                "created_at": row.created_at if row is not None else now,
                "updated_at": now,
            }
            stmt = mysql_insert(MetricDefinitionModel).values(**values)
            stmt = stmt.on_duplicate_key_update(
                family_key=stmt.inserted.family_key,
                variant=stmt.inserted.variant,
                managed_family=stmt.inserted.managed_family,
                name=stmt.inserted.name,
                name_zh=stmt.inserted.name_zh,
                description=stmt.inserted.description,
                description_zh=stmt.inserted.description_zh,
                scope=stmt.inserted.scope,
                category=stmt.inserted.category,
                group_key=stmt.inserted.group_key,
                source_type=stmt.inserted.source_type,
                status=stmt.inserted.status,
                code_python=stmt.inserted.code_python,
                context_label_template=stmt.inserted.context_label_template,
                expression=stmt.inserted.expression,
                min_sample=stmt.inserted.min_sample,
                max_results_per_season=stmt.inserted.max_results_per_season,
                updated_at=stmt.inserted.updated_at,
            )
            session.execute(stmt)
            if row is None:
                inserted.append(spec.key)
            else:
                updated.append(spec.key)
        session.commit()
    return inserted, updated, skipped


def grouped_report(keys: list[str]) -> dict[str, int]:
    buckets = {
        "player": 0,
        "team": 0,
        "game": 0,
    }
    spec_map = {spec.key: spec for spec in build_metric_specs()}
    for key in keys:
        spec = spec_map.get(key)
        if spec is not None:
            buckets[spec.scope] += 1
    return buckets


def run() -> None:
    specs = build_metric_specs()
    validate_specs(specs)
    inserted, updated, skipped = insert_specs(specs)
    added = [*inserted, *updated]
    report = grouped_report(added)
    career_supported = sum(1 for spec in specs if spec.supports_career)
    print(f"Generated {len(specs)} unique metric specs")
    print(f"Career-enabled specs: {career_supported}")
    print(f"Inserted: {len(inserted)}")
    print(f"Updated: {len(updated)}")
    print(f"Skipped (pre-existing non-managed keys): {len(skipped)}")
    print(f"Added by scope: player={report['player']} team={report['team']} game={report['game']}")
    if inserted:
        print("Inserted keys:")
        for key in inserted:
            print(f"  - {key}")
    if updated:
        print("Updated keys:")
        for key in updated:
            print(f"  - {key}")
    if skipped:
        print("Skipped existing keys:")
        for key in skipped:
            print(f"  - {key}")
    print("Unsupported candidates:")
    for item in UNSUPPORTED_CANDIDATES:
        print(f"  - {item['key']}: {item['reason']}")


if __name__ == "__main__":
    run()
