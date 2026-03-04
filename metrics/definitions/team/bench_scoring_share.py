"""Bench Scoring Share: % of team points coming from non-starters."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game
from sqlalchemy import case, func


class BenchScoringShare(MetricDefinition):
    key = "bench_scoring_share"
    name = "Bench Scoring Share"
    description = "Percentage of total team points scored by non-starters this season."
    scope = "team"
    category = "aggregate"
    min_sample = 10

    def compute(self, session, entity_id, season, game_id=None):
        row = (
            session.query(
                func.sum(func.coalesce(PlayerGameStats.pts, 0)).label("total_pts"),
                func.sum(
                    case((PlayerGameStats.starter.is_(False), func.coalesce(PlayerGameStats.pts, 0)), else_=0)
                ).label("bench_pts"),
                func.count(PlayerGameStats.game_id.distinct()).label("games"),
            )
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.team_id == entity_id, Game.season == season)
            .one()
        )

        games = int(row.games or 0)
        if games < self.min_sample:
            return None

        total_pts = int(row.total_pts or 0)
        bench_pts = int(row.bench_pts or 0)

        if total_pts == 0:
            return None

        share = bench_pts / total_pts

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(share, 4),
            context={
                "bench_scoring_share": round(share, 4),
                "bench_pts": bench_pts,
                "total_pts": total_pts,
                "games": games,
                "bench_ppg": round(bench_pts / games, 1),
            },
        )


register(BenchScoringShare())
