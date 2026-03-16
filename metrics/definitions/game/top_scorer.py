"""Top Scorer: highest individual point total recorded in a game."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Player


class TopScorer(MetricDefinition):
    key = "top_scorer"
    name = "Top Scorer"
    description = "The highest individual scoring performance in a game."
    scope = "game"
    category = "scoring"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        target_game = entity_id

        row = (
            session.query(
                PlayerGameStats.pts,
                Player.full_name,
                PlayerGameStats.player_id,
            )
            .join(Player, Player.player_id == PlayerGameStats.player_id)
            .filter(
                PlayerGameStats.game_id == target_game,
                PlayerGameStats.pts.isnot(None),
                PlayerGameStats.pts > 0,
            )
            .order_by(PlayerGameStats.pts.desc(), PlayerGameStats.player_id.asc())
            .first()
        )

        if row is None:
            return None

        name = row.full_name or "Unknown"
        pts = int(row.pts)

        return MetricResult(
            metric_key=self.key,
            entity_type="game",
            entity_id=target_game,
            season=season,
            game_id=target_game,
            value_num=float(pts),
            value_str=f"{name} scored {pts} pts",
            context={
                "player_name": name,
                "player_id": row.player_id,
                "pts": pts,
                "game_id": target_game,
            },
        )


register(TopScorer())
