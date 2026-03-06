"""Bench Scoring Share: % of team points coming from non-starters."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import PlayerGameStats


class BenchScoringShare(MetricDefinition):
    key = "bench_scoring_share"
    name = "Bench Scoring Share"
    description = "% of total team points from non-starters this season."
    scope = "team"
    category = "aggregate"
    min_sample = 10
    incremental = True
    supports_career = True

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        rows = (
            session.query(PlayerGameStats)
            .filter(
                PlayerGameStats.team_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .all()
        )
        if not rows:
            return None
        total_pts = sum(r.pts or 0 for r in rows)
        bench_pts = sum(r.pts or 0 for r in rows if r.starter is False)
        return {"total_pts": total_pts, "bench_pts": bench_pts, "games": 1}

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games = totals.get("games", 0)
        if games < self.min_sample:
            return None
        total_pts = totals.get("total_pts", 0)
        bench_pts = totals.get("bench_pts", 0)
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
