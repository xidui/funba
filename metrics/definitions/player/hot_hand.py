"""Hot Hand: FG% after 3+ consecutive makes vs. baseline."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import ShotRecord


class HotHand(MetricDefinition):
    key = "hot_hand"
    name = "Hot Hand"
    description = "FG% on a shot immediately following 3+ consecutive makes, compared to baseline FG%."
    scope = "player"
    category = "conditional"
    min_sample = 30  # minimum total shots
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
        hot_opps = 0
        hot_made_count = 0
        for i in range(3, len(shots)):
            if all(shots[i - j].shot_made for j in range(1, 4)):
                hot_opps += 1
                if shots[i].shot_made:
                    hot_made_count += 1
        return {
            "total_shots": total_shots,
            "total_made": total_made,
            "hot_opps": hot_opps,
            "hot_made": hot_made_count,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        ts = totals.get("total_shots", 0)
        if ts < self.min_sample:
            return None
        hot_opps = totals.get("hot_opps", 0)
        if hot_opps < 5:
            return None
        hot_made = totals.get("hot_made", 0)
        total_made = totals.get("total_made", 0)
        hot_pct = hot_made / hot_opps
        baseline = total_made / ts if ts > 0 else 0
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
                "lift": round(hot_pct - baseline, 4),
                "hot_opportunities": hot_opps,
                "total_shots": ts,
            },
        )


register(HotHand())
