"""Close Game Record: W-L in games decided by 5 points or fewer."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import TeamGameStats, Game


class CloseGameRecord(MetricDefinition):
    key = "close_game_record"
    name = "Close Game Record"
    description = "Win-loss record in games decided by 5 points or fewer."
    scope = "team"
    category = "aggregate"
    min_sample = 5
    incremental = True
    supports_career = True

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
            return {"close_wins": 0, "close_losses": 0, "total_games": 1}
        margin = abs(game.home_team_score - game.road_team_score)
        if margin <= 5:
            return {
                "close_wins": 1 if tgs.win else 0,
                "close_losses": 0 if tgs.win else 1,
                "total_games": 1,
            }
        return {"close_wins": 0, "close_losses": 0, "total_games": 1}

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        cw = totals.get("close_wins", 0)
        cl = totals.get("close_losses", 0)
        close_total = cw + cl
        if close_total < self.min_sample:
            return None
        win_pct = cw / close_total
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            value_str=f"{cw}-{cl}",
            context={
                "close_wins": cw,
                "close_losses": cl,
                "close_game_total": close_total,
                "close_win_pct": round(win_pct, 4),
                "total_games": totals.get("total_games", 0),
            },
        )


register(CloseGameRecord())
