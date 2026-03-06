"""Scoring Consistency: % of games with 20+ points (when player actually played)."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import PlayerGameStats


class ScoringConsistency(MetricDefinition):
    key = "scoring_consistency"
    name = "Scoring Consistency (20+)"
    description = "Percentage of games played where the player scored 20 or more points."
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
            return {"games_played": 0, "games_20_plus": 0}
        pts = int(row.pts or 0)
        return {
            "games_played": 1,
            "games_20_plus": 1 if pts >= 20 else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games_played = totals.get("games_played", 0)
        if games_played < self.min_sample:
            return None
        games_20_plus = totals.get("games_20_plus", 0)
        rate = games_20_plus / games_played
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
            },
        )


register(ScoringConsistency())
