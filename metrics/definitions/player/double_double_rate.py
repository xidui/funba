"""Double-Double Rate: % of games with a double-double."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import PlayerGameStats


class DoubleDoubleRate(MetricDefinition):
    key = "double_double_rate"
    name = "Double-Double Rate"
    description = "Percentage of games played where the player recorded a double-double (any two of PTS/REB/AST/STL/BLK ≥ 10)."
    scope = "player"
    category = "aggregate"
    min_sample = 10
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
        played = 1 if (row.min or 0) > 0 or (row.sec or 0) > 0 else 0
        if played == 0:
            return {"games_played": 0, "dd_count": 0}
        cats = [
            row.pts or 0,
            row.reb or 0,
            row.ast or 0,
            row.stl or 0,
            row.blk or 0,
        ]
        is_dd = sum(1 for c in cats if c >= 10) >= 2
        return {
            "games_played": 1,
            "dd_count": 1 if is_dd else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        gp = totals.get("games_played", 0)
        if gp < self.min_sample:
            return None
        dd = totals.get("dd_count", 0)
        rate = dd / gp
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(rate, 4),
            context={
                "double_double_rate": round(rate, 4),
                "double_doubles": dd,
                "games_played": gp,
            },
        )


register(DoubleDoubleRate())
