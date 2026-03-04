"""Clutch FG%: field goal percentage in the final 2 minutes of Q4."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import ShotRecord, Game
from sqlalchemy import func


class ClutchFgPct(MetricDefinition):
    key = "clutch_fg_pct"
    name = "Clutch FG%"
    description = "FG% in the final 2 minutes of the 4th quarter (or OT), compared to overall FG%."
    scope = "player"
    category = "conditional"
    min_sample = 5   # clutch shots are rare; lower bar

    def compute(self, session, entity_id, season, game_id=None):
        base_q = (
            session.query(ShotRecord.shot_made)
            .join(Game, ShotRecord.game_id == Game.game_id)
            .filter(
                ShotRecord.player_id == entity_id,
                Game.season == season,
                ShotRecord.shot_attempted.is_(True),
            )
        )

        all_shots = base_q.all()
        if len(all_shots) < 10:
            return None

        baseline = sum(1 for s in all_shots if s.shot_made) / len(all_shots)

        # Clutch = period >= 4, minutes remaining <= 1 (i.e. inside last 2 min)
        clutch_shots = (
            base_q
            .filter(ShotRecord.period >= 4, ShotRecord.min <= 1)
            .all()
        )

        if len(clutch_shots) < self.min_sample:
            return None

        clutch_made = sum(1 for s in clutch_shots if s.shot_made)
        clutch_pct = clutch_made / len(clutch_shots)

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(clutch_pct, 4),
            context={
                "clutch_fg_pct": round(clutch_pct, 4),
                "baseline_fg_pct": round(baseline, 4),
                "lift": round(clutch_pct - baseline, 4),
                "clutch_attempts": len(clutch_shots),
                "clutch_made": clutch_made,
            },
        )


register(ClutchFgPct())
