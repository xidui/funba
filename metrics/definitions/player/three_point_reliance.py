"""Three-Point Reliance: % of total points coming from 3-pointers."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import PlayerGameStats


class ThreePointReliance(MetricDefinition):
    key = "three_point_reliance"
    name = "3-Point Reliance"
    description = "Percentage of a player's total points scored from three-point range this season."
    scope = "player"
    category = "scoring"
    min_sample = 20
    incremental = True
    supports_career = True

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        row = (
            session.query(PlayerGameStats)
            .filter(
                PlayerGameStats.player_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .first()
        )
        if row is None:
            return None
        return {
            "fg3m": int(row.fg3m or 0),
            "pts": int(row.pts or 0),
            "games": 1,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games = totals.get("games", 0)
        if games < self.min_sample:
            return None
        fg3m = totals.get("fg3m", 0)
        pts = totals.get("pts", 0)
        if pts == 0:
            return None
        reliance = (fg3m * 3) / pts
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(reliance, 4),
            value_str=f"{reliance:.1%}",
            context={
                "three_pt_pts": fg3m * 3,
                "total_pts": pts,
                "three_point_reliance": round(reliance, 4),
                "games": games,
            },
        )


register(ThreePointReliance())
