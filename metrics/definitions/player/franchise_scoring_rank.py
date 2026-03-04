"""Franchise Scoring Rank: player's all-time career points rank for their team."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game, Player
from sqlalchemy import func


class FranchiseScoringRank(MetricDefinition):
    key = "franchise_scoring_rank"
    name = "Franchise Scoring Rank"
    description = "Player's all-time rank in career points scored for their current team (franchise history)."
    scope = "player"
    category = "record"
    min_sample = 1

    def compute(self, session, entity_id, season, game_id=None):
        # Find the team this player is associated with in this season
        team_row = (
            session.query(PlayerGameStats.team_id)
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, Game.season == season)
            .group_by(PlayerGameStats.team_id)
            .order_by(func.count().desc())
            .first()
        )
        if team_row is None:
            return None
        team_id = team_row.team_id

        # Get career points for all players on this franchise (all time)
        career_pts = (
            session.query(
                PlayerGameStats.player_id,
                func.sum(func.coalesce(PlayerGameStats.pts, 0)).label("total_pts"),
            )
            .filter(PlayerGameStats.team_id == team_id)
            .group_by(PlayerGameStats.player_id)
            .order_by(func.sum(func.coalesce(PlayerGameStats.pts, 0)).desc())
            .all()
        )

        if not career_pts:
            return None

        player_pts = {row.player_id: int(row.total_pts) for row in career_pts}
        sorted_ids = [row.player_id for row in career_pts]

        if entity_id not in player_pts:
            return None

        rank = sorted_ids.index(entity_id) + 1
        pts = player_pts[entity_id]

        if pts < 100:   # not meaningful for very low scorers
            return None

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(rank),
            value_str=f"#{rank}",
            context={
                "rank": rank,
                "career_pts_for_team": pts,
                "team_id": team_id,
                "players_ranked": len(sorted_ids),
            },
        )


register(FranchiseScoringRank())
