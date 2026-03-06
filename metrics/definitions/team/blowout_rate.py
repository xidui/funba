"""Blowout Rate: % of wins by 15+ point margin."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import TeamGameStats, Game
from sqlalchemy import func


class BlowoutRate(MetricDefinition):
    key = "blowout_rate"
    name = "Blowout Rate"
    description = "Percentage of wins where the team won by 15 or more points — a measure of dominance."
    scope = "team"
    category = "scoring"
    min_sample = 10

    def compute(self, session, entity_id, season, game_id=None):
        games = (
            session.query(
                TeamGameStats.win,
                Game.home_team_id,
                Game.road_team_id,
                Game.home_team_score,
                Game.road_team_score,
            )
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(
                TeamGameStats.team_id == entity_id,
                Game.season == season,
                TeamGameStats.win.isnot(None),
            )
            .all()
        )

        if len(games) < self.min_sample:
            return None

        wins = 0
        blowout_wins = 0

        for g in games:
            if not g.win:
                continue
            wins += 1
            is_home = g.home_team_id == entity_id
            margin = (
                (g.home_team_score or 0) - (g.road_team_score or 0)
                if is_home
                else (g.road_team_score or 0) - (g.home_team_score or 0)
            )
            if margin >= 15:
                blowout_wins += 1

        if wins == 0:
            return None

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
                "total_wins": wins,
                "total_games": len(games),
            },
        )


register(BlowoutRate())
