"""Double-Double Rate: % of games with a double-double."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game
from sqlalchemy import case, func


class DoubleDoubleRate(MetricDefinition):
    key = "double_double_rate"
    name = "Double-Double Rate"
    description = "Percentage of games played where the player recorded a double-double (any two of PTS/REB/AST/STL/BLK ≥ 10)."
    scope = "player"
    category = "aggregate"
    min_sample = 10

    def compute(self, session, entity_id, season, game_id=None):
        played_cond = (func.coalesce(PlayerGameStats.min, 0) > 0) | (func.coalesce(PlayerGameStats.sec, 0) > 0)

        rows = (
            session.query(
                PlayerGameStats.pts,
                PlayerGameStats.reb,
                PlayerGameStats.ast,
                PlayerGameStats.stl,
                PlayerGameStats.blk,
                PlayerGameStats.min,
                PlayerGameStats.sec,
            )
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, Game.season == season)
            .all()
        )

        played = [
            r for r in rows
            if (r.min or 0) > 0 or (r.sec or 0) > 0
        ]

        if len(played) < self.min_sample:
            return None

        def _is_dd(r) -> bool:
            cats = [r.pts or 0, r.reb or 0, r.ast or 0, r.stl or 0, r.blk or 0]
            return sum(1 for c in cats if c >= 10) >= 2

        dd_count = sum(1 for r in played if _is_dd(r))
        rate = dd_count / len(played)

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(rate, 4),
            context={
                "double_double_rate": round(rate, 4),
                "double_doubles": dd_count,
                "games_played": len(played),
            },
        )


register(DoubleDoubleRate())
