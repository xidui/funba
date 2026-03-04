"""Scoring Consistency: % of games with 20+ points (when player actually played)."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game
from sqlalchemy import case, func


class ScoringConsistency(MetricDefinition):
    key = "scoring_consistency"
    name = "Scoring Consistency (20+)"
    description = "Percentage of games played where the player scored 20 or more points."
    scope = "player"
    category = "aggregate"
    min_sample = 10

    def compute(self, session, entity_id, season, game_id=None):
        played_condition = (func.coalesce(PlayerGameStats.min, 0) > 0) | (func.coalesce(PlayerGameStats.sec, 0) > 0)

        row = (
            session.query(
                func.sum(case((played_condition, 1), else_=0)).label("games_played"),
                func.sum(
                    case(((played_condition) & (PlayerGameStats.pts >= 20), 1), else_=0)
                ).label("games_20_plus"),
                func.avg(case((played_condition, PlayerGameStats.pts), else_=None)).label("avg_pts"),
            )
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, Game.season == season)
            .one()
        )

        games_played = int(row.games_played or 0)
        if games_played < self.min_sample:
            return None

        games_20_plus = int(row.games_20_plus or 0)
        rate = games_20_plus / games_played
        avg_pts = round(float(row.avg_pts or 0), 1)

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(rate, 4),
            context={
                "rate_20_plus": round(rate, 4),
                "games_20_plus": games_20_plus,
                "games_played": games_played,
                "avg_pts": avg_pts,
            },
        )


register(ScoringConsistency())
