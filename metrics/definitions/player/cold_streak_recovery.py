"""Cold Streak Recovery: FG% after 3+ consecutive misses vs. baseline."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import ShotRecord


class ColdStreakRecovery(MetricDefinition):
    key = "cold_streak_recovery"
    name = "Cold Streak Recovery"
    description = "FG% on the shot immediately after 3+ consecutive misses, compared to baseline FG%."
    scope = "player"
    category = "conditional"
    min_sample = 30

    def compute(self, session, entity_id, season, game_id=None):
        shots = (
            session.query(ShotRecord.game_id, ShotRecord.period, ShotRecord.min, ShotRecord.sec, ShotRecord.shot_made)
            .filter(
                ShotRecord.player_id == entity_id,
                ShotRecord.season == season,
                ShotRecord.shot_attempted.is_(True),
            )
            .order_by(ShotRecord.game_id, ShotRecord.period, ShotRecord.min.desc(), ShotRecord.sec.desc())
            .all()
        )

        if len(shots) < self.min_sample:
            return None

        total_made = sum(1 for s in shots if s.shot_made)
        baseline = total_made / len(shots)

        cold_opps = 0
        cold_made = 0
        for i in range(3, len(shots)):
            if shots[i].game_id != shots[i - 1].game_id:
                continue
            if all(not shots[i - j].shot_made for j in range(1, 4)):
                cold_opps += 1
                if shots[i].shot_made:
                    cold_made += 1

        if cold_opps < 5:
            return None

        cold_pct = cold_made / cold_opps
        lift = cold_pct - baseline

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(cold_pct, 4),
            context={
                "baseline_fg_pct": round(baseline, 4),
                "cold_recovery_fg_pct": round(cold_pct, 4),
                "lift": round(lift, 4),
                "cold_opportunities": cold_opps,
                "total_shots": len(shots),
            },
        )


register(ColdStreakRecovery())
