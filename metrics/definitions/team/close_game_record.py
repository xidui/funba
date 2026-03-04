"""Close Game Record: W-L in games decided by 5 points or fewer."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import Game, TeamGameStats


class CloseGameRecord(MetricDefinition):
    key = "close_game_record"
    name = "Close Game Record"
    description = "Win-loss record in games decided by 5 points or fewer at the final buzzer."
    scope = "team"
    category = "aggregate"
    min_sample = 5

    def compute(self, session, entity_id, season, game_id=None):
        rows = (
            session.query(TeamGameStats.win, Game.home_team_score, Game.road_team_score)
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(
                TeamGameStats.team_id == entity_id,
                Game.season == season,
                TeamGameStats.win.isnot(None),
                Game.home_team_score.isnot(None),
                Game.road_team_score.isnot(None),
            )
            .all()
        )

        if not rows:
            return None

        close_wins = 0
        close_losses = 0
        for r in rows:
            margin = abs((r.home_team_score or 0) - (r.road_team_score or 0))
            if margin <= 5:
                if r.win:
                    close_wins += 1
                else:
                    close_losses += 1

        close_total = close_wins + close_losses
        if close_total < self.min_sample:
            return None

        win_pct = close_wins / close_total

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            value_str=f"{close_wins}-{close_losses}",
            context={
                "close_wins": close_wins,
                "close_losses": close_losses,
                "close_game_total": close_total,
                "close_win_pct": round(win_pct, 4),
                "total_games": len(rows),
            },
        )


register(CloseGameRecord())
