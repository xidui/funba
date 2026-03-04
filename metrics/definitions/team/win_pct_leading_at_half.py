"""Win% when leading at halftime."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import Game, GamePlayByPlay, TeamGameStats
from sqlalchemy import func


class WinPctLeadingAtHalf(MetricDefinition):
    key = "win_pct_leading_at_half"
    name = "Leads-at-Half Win%"
    description = "Win percentage in games where the team was leading at halftime."
    scope = "team"
    category = "conditional"
    min_sample = 5

    def compute(self, session, entity_id, season, game_id=None):
        # Get all games this team played this season
        games = (
            session.query(TeamGameStats.game_id, TeamGameStats.win, Game.home_team_id, Game.road_team_id)
            .join(Game, TeamGameStats.game_id == Game.game_id)
            .filter(TeamGameStats.team_id == entity_id, Game.season == season, TeamGameStats.win.isnot(None))
            .all()
        )

        if len(games) < self.min_sample:
            return None

        leading_at_half_wins = 0
        leading_at_half_total = 0

        for g in games:
            is_home = g.home_team_id == entity_id

            # Find the last PBP event in period 2 with a non-null score_margin
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
                # score_margin is from home team perspective in NBA PBP
                margin = int(pbp_row.score_margin)
            except (ValueError, TypeError):
                continue

            team_leading = margin > 0 if is_home else margin < 0
            if not team_leading:
                continue

            leading_at_half_total += 1
            if g.win:
                leading_at_half_wins += 1

        if leading_at_half_total < self.min_sample:
            return None

        win_pct = leading_at_half_wins / leading_at_half_total

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            context={
                "win_pct_leading_at_half": round(win_pct, 4),
                "wins": leading_at_half_wins,
                "games_leading_at_half": leading_at_half_total,
                "total_games": len(games),
            },
        )


register(WinPctLeadingAtHalf())
