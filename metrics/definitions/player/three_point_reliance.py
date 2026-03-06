"""Three-Point Reliance: % of total points coming from 3-pointers."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game
from sqlalchemy import func


class ThreePointReliance(MetricDefinition):
    key = "three_point_reliance"
    name = "3-Point Reliance"
    description = "Percentage of a player's total points scored from three-point range this season."
    scope = "player"
    category = "scoring"
    min_sample = 20

    def compute(self, session, entity_id, season, game_id=None):
        row = (
            session.query(
                func.sum(func.coalesce(PlayerGameStats.pts, 0)).label("pts"),
                func.sum(func.coalesce(PlayerGameStats.fg3m, 0)).label("fg3m"),
                func.count(PlayerGameStats.game_id).label("games"),
            )
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, Game.season == season)
            .one()
        )

        games = int(row.games or 0)
        if games < self.min_sample:
            return None

        pts = float(row.pts or 0)
        fg3m = float(row.fg3m or 0)

        if pts == 0:
            return None

        pts_from_three = fg3m * 3
        reliance = pts_from_three / pts

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(reliance, 4),
            value_str=f"{reliance:.1%}",
            context={
                "three_point_reliance": round(reliance, 4),
                "pts_from_three": int(pts_from_three),
                "total_pts": int(pts),
                "fg3m": int(fg3m),
                "games": games,
            },
        )


register(ThreePointReliance())
