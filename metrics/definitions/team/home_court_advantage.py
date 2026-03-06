"""Home Court Advantage: home win% minus away win% for a team this season."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import TeamGameStats


class HomeCourtAdvantage(MetricDefinition):
    key = "home_court_advantage"
    name = "Home Court Advantage"
    description = "Home win% minus road win% — quantifies the boost from playing at home."
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
        if row.on_road:
            return {
                "road_games": 1,
                "road_wins": 1 if row.win else 0,
                "home_games": 0,
                "home_wins": 0,
            }
        else:
            return {
                "home_games": 1,
                "home_wins": 1 if row.win else 0,
                "road_games": 0,
                "road_wins": 0,
            }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        hg = totals.get("home_games", 0)
        rg = totals.get("road_games", 0)
        if hg < self.min_sample or rg < self.min_sample:
            return None
        hw = totals.get("home_wins", 0)
        rw = totals.get("road_wins", 0)
        home_pct = hw / hg
        road_pct = rw / rg
        advantage = home_pct - road_pct
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(advantage, 4),
            value_str=f"{advantage:+.1%}",
            context={
                "home_win_pct": round(home_pct, 4),
                "road_win_pct": round(road_pct, 4),
                "advantage": round(advantage, 4),
                "home_games": hg,
                "road_games": rg,
            },
        )


register(HomeCourtAdvantage())
