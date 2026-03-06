"""Home Court Advantage: home win% minus away win% for a team this season."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import TeamGameStats, Game
from sqlalchemy import case, func


class HomeCourtAdvantage(MetricDefinition):
    key = "home_court_advantage"
    name = "Home Court Advantage"
    description = "Home win% minus road win% — how much better a team performs at home vs on the road."
    scope = "team"
    category = "conditional"
    min_sample = 10

    def compute(self, session, entity_id, season, game_id=None):
        rows = (
            session.query(
                TeamGameStats.on_road,
                func.sum(case((TeamGameStats.win.is_(True), 1), else_=0)).label("wins"),
                func.count(TeamGameStats.game_id).label("games"),
            )
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(
                TeamGameStats.team_id == entity_id,
                Game.season == season,
                TeamGameStats.win.isnot(None),
            )
            .group_by(TeamGameStats.on_road)
            .all()
        )

        home_wins = home_games = away_wins = away_games = 0
        for r in rows:
            if r.on_road:
                away_wins = int(r.wins or 0)
                away_games = int(r.games or 0)
            else:
                home_wins = int(r.wins or 0)
                home_games = int(r.games or 0)

        if home_games < 5 or away_games < 5:
            return None

        home_win_pct = home_wins / home_games
        away_win_pct = away_wins / away_games
        advantage = home_win_pct - away_win_pct

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(advantage, 4),
            value_str=f"{advantage:+.1%}",
            context={
                "home_court_advantage": round(advantage, 4),
                "home_win_pct": round(home_win_pct, 4),
                "away_win_pct": round(away_win_pct, 4),
                "home_wins": home_wins,
                "home_games": home_games,
                "away_wins": away_wins,
                "away_games": away_games,
            },
        )


register(HomeCourtAdvantage())
