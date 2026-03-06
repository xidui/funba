"""Cold Streak Recovery: FG% after 3+ consecutive misses vs. baseline."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import ShotRecord


class ColdStreakRecovery(MetricDefinition):
    key = "cold_streak_recovery"
    name = "Cold Streak Recovery"
    description = "FG% on the shot immediately after 3+ consecutive misses, compared to baseline FG%."
    scope = "player"
    category = "conditional"
    min_sample = 30
    incremental = True
    supports_career = True
    career_min_sample = 100

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        shots = (
            session.query(ShotRecord)
            .filter(
                ShotRecord.player_id == entity_id,
                ShotRecord.game_id == game_id,
                ShotRecord.shot_attempted.is_(True),
            )
            .order_by(ShotRecord.period, ShotRecord.min.desc(), ShotRecord.sec.desc())
            .all()
        )
        if not shots:
            return None
        total_shots = len(shots)
        total_made = sum(1 for s in shots if s.shot_made)
        cold_opps = 0
        cold_made_count = 0
        for i in range(3, len(shots)):
            if all(not shots[i - j].shot_made for j in range(1, 4)):
                cold_opps += 1
                if shots[i].shot_made:
                    cold_made_count += 1
        return {
            "total_shots": total_shots,
            "total_made": total_made,
            "cold_opps": cold_opps,
            "cold_made": cold_made_count,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        ts = totals.get("total_shots", 0)
        if ts < self.min_sample:
            return None
        cold_opps = totals.get("cold_opps", 0)
        if cold_opps < 5:
            return None
        cold_made = totals.get("cold_made", 0)
        total_made = totals.get("total_made", 0)
        cold_pct = cold_made / cold_opps
        baseline = total_made / ts if ts > 0 else 0
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
                "lift": round(cold_pct - baseline, 4),
                "cold_opportunities": cold_opps,
                "total_shots": ts,
            },
        )


register(ColdStreakRecovery())
