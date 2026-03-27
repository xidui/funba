"""Base classes for the metrics framework."""
from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from typing import Any

# Sentinel season value for career / cross-season aggregation
CAREER_SEASON = "all"  # deprecated — kept for backward compat

# Career season split by type
CAREER_SEASON_PREFIX = "all_"
SEASON_TYPE_TO_CAREER = {"2": "all_regular", "4": "all_playoffs", "5": "all_playin"}
CAREER_SEASONS = set(SEASON_TYPE_TO_CAREER.values())
_CAREER_TO_TYPE = {v: k for k, v in SEASON_TYPE_TO_CAREER.items()}


def career_season_for(season: str) -> str | None:
    """Map a 5-digit season ID to its career bucket, e.g. '22025' → 'all_regular'.

    Returns None for preseason (1) and all-star (3) — skip career accumulation.
    """
    if season and len(season) == 5 and season.isdigit():
        return SEASON_TYPE_TO_CAREER.get(season[0])
    return None


def is_career_season(season: str | None) -> bool:
    """Check if a season value is any career bucket (all_regular, all_playoffs, etc.)."""
    return bool(season and season.startswith(CAREER_SEASON_PREFIX))


def career_season_type_code(career_season: str) -> str | None:
    """Reverse-map career season to type code, e.g. 'all_regular' → '2'."""
    return _CAREER_TO_TYPE.get(career_season)


@dataclass
class MetricResult:
    """In-memory result produced by a MetricDefinition before persistence."""
    metric_key: str
    entity_type: str          # 'player' | 'team' | 'game' | 'league'
    entity_id: str | None     # player_id or team_id; None for league-scope
    season: str | None
    game_id: str | None       # game that triggered the run; None for season-agg
    rank_group: str | None = None
    value_num: float | None = None
    value_str: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    # Filled in by scorer after compute
    noteworthiness: float | None = None
    notable_reason: str | None = None

    @property
    def display_value(self) -> str:
        if self.value_num is not None:
            return str(round(self.value_num, 4))
        return self.value_str or "—"


class MetricDefinition(ABC):
    """Abstract base for all metric definitions.

    Three execution modes, selected by ``trigger`` and ``incremental``:

    Mode 1 — trigger="game", incremental=True (per-game delta/reduce):
        Implement compute_delta() + compute_value().
        The runner accumulates running totals in MetricResult.context_json and
        calls compute_value() after each merge. Efficient for season and career.

    Mode 2 — trigger="game", incremental=False (per-game full recompute):
        Implement compute() instead.
        Used for game-scoped metrics and rank-based metrics that cannot be
        expressed as additive per-game deltas.

    Mode 3 — trigger="season" (whole-season computation, RECOMMENDED for new metrics):
        Implement compute_season().
        Called once per season with full data access. The metric handles entity
        discovery and computation internally, returning all results at once.
        Set supports_career=True to also run with career season values
        ("all_regular", "all_playoffs", "all_playin").

    Career variants (trigger="game"):
        Set supports_career=True on a season metric to auto-register a career
        sibling (key + "_career") that accumulates per season type
        (all_regular, all_playoffs, all_playin). The sibling inherits
        compute_delta / compute_value with a higher min_sample threshold.

    Career variants (trigger="season"):
        Set supports_career=True. The same compute_season() is called with
        career season values (e.g. "all_regular"). The metric should adapt
        its query filter based on the season parameter.
    """
    key: str
    name: str
    description: str
    scope: str       # 'player' | 'team' | 'game' | 'league'
    category: str
    min_sample: int = 10

    # Trigger: when does this metric run?
    trigger: str = "game"          # "game" (per-game pipeline) or "season" (whole-season)

    # Incremental / career flags (used by trigger="game" metrics)
    incremental: bool = True       # False → use compute() instead
    supports_career: bool = False  # True → also dispatch career season values
    career: bool = False           # True → this IS the career version (trigger="game" only)
    per_game: bool = True          # DEPRECATED — use trigger="season" instead

    # Ranking direction: "desc" (default, higher is better) or "asc" (lower is better)
    rank_order: str = "desc"

    # Career sibling overrides (customisable per metric class)
    career_min_sample: int | None = None  # None → min_sample * 5
    career_name_suffix: str = " (Career)"

    # Optional format string for context label display, e.g. "{b2b_wins}/{b2b_games} B2B".
    # Keys are interpolated from the context dict via str.format_map().
    context_label_template: str | None = None

    def compute_delta(self, session: Any, entity_id: str | None, game_id: str) -> dict | None:
        """Return this game's additive contribution to running totals.

        Return None if the entity did not participate in this game.
        Numeric fields are added to existing totals; non-numeric overwrite.
        Only called when incremental=True.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement compute_delta")

    def compute_value(
        self, totals: dict, season: str, entity_id: str | None
    ) -> MetricResult | None:
        """Derive a MetricResult from accumulated totals.

        Return None if totals are below min_sample.
        Only called when incremental=True.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement compute_value")

    def compute(
        self,
        session: Any,
        entity_id: str | None,
        season: str | None,
        game_id: str | None = None,
    ) -> MetricResult | None:
        """Full recompute path — used when trigger="game", incremental=False."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement compute")

    def compute_season(
        self,
        session: Any,
        season: str,
    ) -> list[MetricResult]:
        """Whole-season computation — used when trigger="season".

        The metric is responsible for discovering entities and computing values.
        Returns all MetricResult objects for the given season.
        The ``season`` parameter may be a concrete season (e.g. "22025") or a
        career bucket (e.g. "all_regular") when supports_career=True.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement compute_season")


def merge_totals(existing: dict, delta: dict) -> dict:
    """Merge a per-game delta into an existing totals dict (numeric fields summed)."""
    result = dict(existing)
    for k, v in delta.items():
        if isinstance(v, (int, float)):
            result[k] = result.get(k, 0) + v
        else:
            result[k] = v
    return result


def subtract_delta(totals: dict, delta: dict) -> dict:
    """Remove a game's contribution from accumulated totals (for reprocessing)."""
    result = dict(totals)
    for k, v in delta.items():
        if isinstance(v, (int, float)):
            result[k] = result.get(k, 0) - v
    return result
