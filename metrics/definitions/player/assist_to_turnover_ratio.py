"""Assist-to-Turnover Ratio: AST / TOV — measures ball-handling quality."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game
from sqlalchemy import func


class AssistToTurnoverRatio(MetricDefinition):
    key = "assist_to_turnover_ratio"
    name = "Assist/Turnover Ratio"
    description = "Assists per turnover this season — higher is better; elite playmakers exceed 3.0."
    scope = "player"
    category = "efficiency"
    min_sample = 20

    def compute(self, session, entity_id, season, game_id=None):
        row = (
            session.query(
                func.sum(func.coalesce(PlayerGameStats.ast, 0)).label("ast"),
                func.sum(func.coalesce(PlayerGameStats.tov, 0)).label("tov"),
                func.count(PlayerGameStats.game_id).label("games"),
            )
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, Game.season == season)
            .one()
        )

        games = int(row.games or 0)
        if games < self.min_sample:
            return None

        ast = float(row.ast or 0)
        tov = float(row.tov or 0)
        if tov == 0:
            return None

        ratio = ast / tov

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(ratio, 2),
            value_str=f"{ratio:.2f}",
            context={
                "ast_to_tov_ratio": round(ratio, 2),
                "total_ast": int(ast),
                "total_tov": int(tov),
                "ast_per_game": round(ast / games, 1),
                "tov_per_game": round(tov / games, 1),
                "games": games,
            },
        )


register(AssistToTurnoverRatio())
