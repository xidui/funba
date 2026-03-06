"""Road Win %: win percentage in away games this season."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import TeamGameStats


class RoadWinPct(MetricDefinition):
    key = "road_win_pct"
    name = "Road Win %"
    description = "Win percentage in away games this season — reveals how well a team performs without home crowd support."
    scope = "team"
    category = "record"
    min_sample = 10
    incremental = True
    supports_career = True
    career_name_suffix = " (All-Time)"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        row = (
            session.query(TeamGameStats)
            .filter(
                TeamGameStats.team_id == entity_id,
                TeamGameStats.game_id == game_id,
            )
            .first()
        )
        if row is None or row.win is None:
            return None
        on_road = 1 if row.on_road else 0
        if on_road == 0:
            return {"road_games": 0, "road_wins": 0}
        return {"road_games": 1, "road_wins": 1 if row.win else 0}

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        road_games = totals.get("road_games", 0)
        if road_games < self.min_sample:
            return None
        road_wins = totals.get("road_wins", 0)
        win_pct = road_wins / road_games
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
                "road_wins": road_wins,
                "road_games": road_games,
            },
        )


register(RoadWinPct())
