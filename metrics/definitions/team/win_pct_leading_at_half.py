"""Win% when leading at halftime."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import Game, GamePlayByPlay, TeamGameStats


class WinPctLeadingAtHalf(MetricDefinition):
    key = "win_pct_leading_at_half"
    name = "Leads-at-Half Win%"
    description = "Win % in games where the team was leading at halftime."
    scope = "team"
    category = "conditional"
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
        if not game:
            return None
        is_home = game.home_team_id == entity_id

        pbp_row = (
            session.query(GamePlayByPlay.score_margin)
            .filter(
                GamePlayByPlay.game_id == game_id,
                GamePlayByPlay.period == 2,
                GamePlayByPlay.score_margin.isnot(None),
            )
            .order_by(GamePlayByPlay.event_num.desc())
            .first()
        )

        if pbp_row is None or pbp_row.score_margin in (None, "null", ""):
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}

        try:
            margin = int(pbp_row.score_margin)
        except (ValueError, TypeError):
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}

        team_leading = margin > 0 if is_home else margin < 0
        if not team_leading:
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}

        return {
            "total_games": 1,
            "leading_total": 1,
            "leading_wins": 1 if tgs.win else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        leading_total = totals.get("leading_total", 0)
        if leading_total < self.min_sample:
            return None
        leading_wins = totals.get("leading_wins", 0)
        win_pct = leading_wins / leading_total
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            context={
                "win_pct_leading_at_half": round(win_pct, 4),
                "wins": leading_wins,
                "games_leading_at_half": leading_total,
                "total_games": totals.get("total_games", 0),
            },
        )


register(WinPctLeadingAtHalf())
