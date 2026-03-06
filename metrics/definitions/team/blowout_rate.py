"""Blowout Rate: % of wins by 15+ point margin."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import TeamGameStats, Game


class BlowoutRate(MetricDefinition):
    key = "blowout_rate"
    name = "Blowout Rate"
    description = "% of wins by 15+ point margin."
    scope = "team"
    category = "record"
    min_sample = 5
    incremental = True
    supports_career = True
    career_name_suffix = " (All-Time)"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = (
            session.query(TeamGameStats)
            .filter(
                TeamGameStats.team_id == entity_id,
                TeamGameStats.game_id == game_id,
            )
            .first()
        )
        if tgs is None or tgs.win is None:
            return None
        game = session.query(Game).filter(Game.game_id == game_id).first()
        if game is None or game.home_team_score is None or game.road_team_score is None:
            return {"total_games": 1, "wins": 1 if tgs.win else 0, "blowout_wins": 0}
        margin = abs(game.home_team_score - game.road_team_score)
        blowout = 1 if tgs.win and margin >= 15 else 0
        return {
            "total_games": 1,
            "wins": 1 if tgs.win else 0,
            "blowout_wins": blowout,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        wins = totals.get("wins", 0)
        if wins < self.min_sample:
            return None
        blowout_wins = totals.get("blowout_wins", 0)
        total_games = totals.get("total_games", 0)
        rate = blowout_wins / wins
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(rate, 4),
            value_str=f"{rate:.1%}",
            context={
                "blowout_rate": round(rate, 4),
                "blowout_wins": blowout_wins,
                "wins": wins,
                "total_games": total_games,
            },
        )


register(BlowoutRate())
