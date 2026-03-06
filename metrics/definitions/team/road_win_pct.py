"""Road Win %: win percentage in away games this season."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import TeamGameStats, Game
from sqlalchemy import case, func


class RoadWinPct(MetricDefinition):
    key = "road_win_pct"
    name = "Road Win %"
    description = "Win percentage in away games this season — reveals how well a team performs without home crowd support."
    scope = "team"
    category = "record"
    min_sample = 10

    def compute(self, session, entity_id, season, game_id=None):
        row = (
            session.query(
                func.sum(case((TeamGameStats.win.is_(True), 1), else_=0)).label("wins"),
                func.count(TeamGameStats.game_id).label("games"),
            )
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(
                TeamGameStats.team_id == entity_id,
                Game.season == season,
                TeamGameStats.on_road.is_(True),
                TeamGameStats.win.isnot(None),
            )
            .one()
        )

        games = int(row.games or 0)
        if games < self.min_sample:
            return None

        wins = int(row.wins or 0)
        win_pct = wins / games

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            value_str=f"{win_pct:.1%}",
            context={
                "road_win_pct": round(win_pct, 4),
                "road_wins": wins,
                "road_games": games,
            },
        )


register(RoadWinPct())
