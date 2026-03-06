"""Combined Score: total points scored by both teams in a game."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import Game


class CombinedScore(MetricDefinition):
    key = "combined_score"
    name = "Combined Score"
    description = "Total points scored by both teams — identifies high-scoring shootouts vs defensive battles."
    scope = "game"
    category = "scoring"
    min_sample = 1

    def compute(self, session, entity_id, season, game_id=None):
        target_game = entity_id

        game = (
            session.query(Game.home_team_score, Game.road_team_score)
            .filter(Game.game_id == target_game)
            .one_or_none()
        )

        if game is None or game.home_team_score is None or game.road_team_score is None:
            return None

        total = game.home_team_score + game.road_team_score

        return MetricResult(
            metric_key=self.key,
            entity_type="game",
            entity_id=target_game,
            season=season,
            game_id=target_game,
            value_num=float(total),
            value_str=f"{total} pts",
            context={
                "combined_score": total,
                "home_score": game.home_team_score,
                "road_score": game.road_team_score,
                "game_id": target_game,
            },
        )


register(CombinedScore())
