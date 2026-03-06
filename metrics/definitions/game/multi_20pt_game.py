"""Multi-20pt Game: number of players scoring 20+ in a single game."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats
from sqlalchemy import func


class Multi20PtGame(MetricDefinition):
    key = "multi_20pt_game"
    name = "20+ Point Contributors"
    description = "Number of players who scored 20 or more points in this game."
    scope = "game"
    category = "aggregate"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        # entity_id is the game_id for game-scope metrics
        target_game = entity_id

        row = (
            session.query(
                func.count(PlayerGameStats.player_id).label("count_20_plus"),
                func.max(PlayerGameStats.pts).label("top_scorer"),
            )
            .filter(
                PlayerGameStats.game_id == target_game,
                PlayerGameStats.pts >= 20,
            )
            .one()
        )

        count = int(row.count_20_plus or 0)
        if count < self.min_sample:
            return None

        top_scorer = int(row.top_scorer or 0)

        return MetricResult(
            metric_key=self.key,
            entity_type="game",
            entity_id=target_game,
            season=season,
            game_id=target_game,
            value_num=float(count),
            value_str=f"{count} players scored 20+",
            context={
                "count_20_plus": count,
                "top_scorer_pts": top_scorer,
                "game_id": target_game,
            },
        )


register(Multi20PtGame())
