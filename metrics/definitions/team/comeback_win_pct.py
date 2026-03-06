"""Comeback Win %: win% in games where team was trailing at halftime."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import TeamGameStats, Game, GamePlayByPlay
from sqlalchemy import func


class ComebackWinPct(MetricDefinition):
    key = "comeback_win_pct"
    name = "Comeback Win %"
    description = "Win percentage in games where the team was trailing at halftime — a measure of resilience."
    scope = "team"
    category = "conditional"
    min_sample = 5

    def compute(self, session, entity_id, season, game_id=None):
        games = (
            session.query(TeamGameStats.game_id, TeamGameStats.win, Game.home_team_id, Game.road_team_id)
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(TeamGameStats.team_id == entity_id, Game.season == season, TeamGameStats.win.isnot(None))
            .all()
        )

        if len(games) < self.min_sample:
            return None

        trailing_total = 0
        trailing_wins = 0

        for g in games:
            is_home = g.home_team_id == entity_id

            pbp_row = (
                session.query(GamePlayByPlay.score_margin)
                .filter(
                    GamePlayByPlay.game_id == g.game_id,
                    GamePlayByPlay.period == 2,
                    GamePlayByPlay.score_margin.isnot(None),
                )
                .order_by(GamePlayByPlay.event_num.desc())
                .first()
            )

            if pbp_row is None or pbp_row.score_margin in (None, "null", ""):
                continue

            try:
                margin = int(pbp_row.score_margin)
            except (ValueError, TypeError):
                continue

            # score_margin is home team perspective; trailing means negative for home, positive for road
            team_trailing = margin < 0 if is_home else margin > 0
            if not team_trailing:
                continue

            trailing_total += 1
            if g.win:
                trailing_wins += 1

        if trailing_total < self.min_sample:
            return None

        win_pct = trailing_wins / trailing_total

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            value_str=f"{win_pct:.1%}",
            context={
                "comeback_win_pct": round(win_pct, 4),
                "comeback_wins": trailing_wins,
                "games_trailing_at_half": trailing_total,
                "total_games": len(games),
            },
        )


register(ComebackWinPct())
