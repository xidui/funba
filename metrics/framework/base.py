"""Base classes for the metrics framework."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass
class MetricResult:
    """In-memory result produced by a MetricDefinition before persistence."""
    metric_key: str
    entity_type: str          # 'player' | 'team' | 'game' | 'league'
    entity_id: str | None     # player_id or team_id; None for league-scope
    season: str | None
    game_id: str | None       # game that triggered the run; None for season-agg
    value_num: float | None = None   # primary numeric value
    value_str: str | None = None     # fallback for text/rank values
    context: dict[str, Any] = field(default_factory=dict)
    # Filled in by scorer after compute()
    noteworthiness: float | None = None
    notable_reason: str | None = None

    @property
    def display_value(self) -> str:
        if self.value_num is not None:
            return str(round(self.value_num, 4))
        return self.value_str or "—"


class MetricDefinition(ABC):
    """Abstract base class for all metric definitions.

    Subclasses must set class-level attributes and implement `compute`.
    """
    key: str         # unique snake_case identifier
    name: str        # display name
    description: str # one-sentence description
    scope: str       # 'player' | 'team' | 'game' | 'league'
    category: str    # 'streak' | 'conditional' | 'record' | 'aggregate'
    min_sample: int = 10  # minimum observations required to emit a result

    @abstractmethod
    def compute(
        self,
        session: Any,
        entity_id: str | None,
        season: str | None,
        game_id: str | None = None,
    ) -> MetricResult | None:
        """Compute the metric.

        Returns None if there is insufficient data (below min_sample).
        """
