"""Franchise Scoring Rank: player's all-time career points rank for their team.

Stored value: career points scored for the team (incremental running total).
Rank is derived at query time via SQL window function — not stored — so it
stays correct without reprocessing other players when one player scores.
"""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats


class FranchiseScoringRank(MetricDefinition):
    key = "franchise_scoring_rank"
    name = "Franchise Scoring Rank"
    description = "Player's all-time career points scored for their current franchise."
    scope = "player"
    category = "record"
    min_sample = 1
    incremental = True
    career = True            # accumulate across all seasons into CAREER_SEASON="all"
    supports_career = False  # already career-scoped, don't auto-register a sibling

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        row = (
            session.query(PlayerGameStats.pts, PlayerGameStats.team_id)
            .filter(
                PlayerGameStats.player_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .first()
        )
        if row is None:
            return None
        return {
            "pts": int(row.pts or 0),
            "team_id": row.team_id,  # non-numeric: overwrites each game (keeps latest)
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        pts = totals.get("pts", 0)
        team_id = totals.get("team_id")
        if pts < 100:  # filter out garbage-time / brief stints
            return None
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(pts),
            value_str=f"{pts:,} pts",
            context={
                "career_pts_for_team": pts,
                "team_id": team_id,
            },
        )


register(FranchiseScoringRank())
