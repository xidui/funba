"""Hot Hand: FG% after 3+ consecutive makes vs. baseline."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import ShotRecord


class HotHand(MetricDefinition):
    key = "hot_hand"
    name = "Hot Hand"
    description = "FG% on a shot immediately following 3+ consecutive makes, compared to baseline FG%."
    scope = "player"
    category = "conditional"
    min_sample = 30   # minimum total shots in season

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

        # Look for windows where the prior 3 shots were all makes
        hot_opps = 0
        hot_made = 0
        for i in range(3, len(shots)):
            # Reset at game boundary
            if shots[i].game_id != shots[i - 1].game_id:
                continue
            if all(shots[i - j].shot_made for j in range(1, 4)):
                hot_opps += 1
                if shots[i].shot_made:
                    hot_made += 1

        if hot_opps < 5:
            return None

        hot_pct = hot_made / hot_opps
        lift = hot_pct - baseline

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(hot_pct, 4),
            context={
                "baseline_fg_pct": round(baseline, 4),
                "hot_hand_fg_pct": round(hot_pct, 4),
                "lift": round(lift, 4),
                "hot_opportunities": hot_opps,
                "total_shots": len(shots),
            },
        )


register(HotHand())
