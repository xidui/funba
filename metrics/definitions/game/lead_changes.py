"""Lead Changes: number of times the lead changed hands during a game."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import GamePlayByPlay


class LeadChanges(MetricDefinition):
    key = "lead_changes"
    name = "Lead Changes"
    description = "Number of times the lead changed hands during the game — high counts signal a closely-contested thriller."
    scope = "game"
    category = "aggregate"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        target_game = entity_id

        pbp_rows = (
            session.query(GamePlayByPlay.score_margin)
            .filter(
                GamePlayByPlay.game_id == target_game,
                GamePlayByPlay.score_margin.isnot(None),
            )
            .order_by(GamePlayByPlay.period, GamePlayByPlay.event_num)
            .all()
        )

        if not pbp_rows:
            return None

        lead_changes = 0
        prev_leader = None  # +1 = home leading, -1 = road leading, 0 = tied

        for row in pbp_rows:
            try:
                margin = int(row.score_margin)
            except (ValueError, TypeError):
                continue

            if margin > 0:
                leader = 1
            elif margin < 0:
                leader = -1
            else:
                leader = 0

            if prev_leader is not None and leader != 0 and leader != prev_leader and prev_leader != 0:
                lead_changes += 1

            if leader != 0:
                prev_leader = leader

        return MetricResult(
            metric_key=self.key,
            entity_type="game",
            entity_id=target_game,
            season=season,
            game_id=target_game,
            value_num=float(lead_changes),
            value_str=f"{lead_changes} lead change{'s' if lead_changes != 1 else ''}",
            context={
                "lead_changes": lead_changes,
                "game_id": target_game,
            },
        )


register(LeadChanges())
