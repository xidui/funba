"""Clutch FG%: field goal percentage in the final 2 minutes of Q4."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import ShotRecord


class ClutchFgPct(MetricDefinition):
    key = "clutch_fg_pct"
    name = "Clutch FG%"
    description = "FG% in the final 2 minutes of the 4th quarter (or OT), compared to overall FG%."
    scope = "player"
    category = "conditional"
    min_sample = 5  # clutch shots are rare; lower bar
    incremental = True
    supports_career = True
    career_min_sample = 20

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        all_shots = (
            session.query(ShotRecord)
            .filter(
                ShotRecord.player_id == entity_id,
                ShotRecord.game_id == game_id,
                ShotRecord.shot_attempted.is_(True),
            )
            .order_by(ShotRecord.period, ShotRecord.min.desc(), ShotRecord.sec.desc())
            .all()
        )
        if not all_shots:
            return None
        total_attempts = len(all_shots)
        total_made = sum(1 for s in all_shots if s.shot_made)
        clutch_shots = [
            s for s in all_shots
            if s.period >= 4 and (s.min or 0) <= 1
        ]
        clutch_attempts = len(clutch_shots)
        clutch_made = sum(1 for s in clutch_shots if s.shot_made)
        return {
            "total_attempts": total_attempts,
            "total_made": total_made,
            "clutch_attempts": clutch_attempts,
            "clutch_made": clutch_made,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        ca = totals.get("clutch_attempts", 0)
        if ca < self.min_sample:
            return None
        cm = totals.get("clutch_made", 0)
        ta = totals.get("total_attempts", 0)
        tm = totals.get("total_made", 0)
        clutch_pct = cm / ca
        baseline = tm / ta if ta > 0 else 0
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
                "clutch_attempts": ca,
                "clutch_made": cm,
            },
        )


register(ClutchFgPct())
