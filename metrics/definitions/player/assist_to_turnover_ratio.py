"""Assist-to-Turnover Ratio: AST / TOV — measures ball-handling quality."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import PlayerGameStats


class AssistToTurnoverRatio(MetricDefinition):
    key = "assist_to_turnover_ratio"
    name = "Assist/Turnover Ratio"
    description = "Assists per turnover this season — higher is better; elite playmakers exceed 3.0."
    scope = "player"
    category = "efficiency"
    min_sample = 20
    incremental = True
    supports_career = True

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        row = (
            session.query(PlayerGameStats)
            .filter(
                PlayerGameStats.player_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .first()
        )
        if row is None:
            return None
        ast = int(row.ast or 0)
        tov = int(row.tov or 0)
        played = 1 if (row.min or 0) > 0 or (row.sec or 0) > 0 else 0
        return {"ast": ast, "tov": tov, "games": played}

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games = totals.get("games", 0)
        if games < self.min_sample:
            return None
        ast = totals.get("ast", 0)
        tov = totals.get("tov", 0)
        if tov == 0:
            return None
        ratio = ast / tov
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(ratio, 3),
            value_str=f"{ratio:.2f}",
            context={
                "ast": ast,
                "tov": tov,
                "games": games,
                "ratio": round(ratio, 3),
            },
        )


register(AssistToTurnoverRatio())
