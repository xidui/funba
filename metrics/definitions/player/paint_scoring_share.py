"""Paint Scoring Share: % of shot attempts taken in the paint (restricted area + non-RA paint)."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import ShotRecord


_PAINT_ZONES = ("Restricted Area", "In The Paint (Non-RA)")


class PaintScoringShare(MetricDefinition):
    key = "paint_scoring_share"
    name = "Paint Shot Share"
    description = "Percentage of shot attempts taken inside the paint (restricted area + non-RA paint) this season."
    scope = "player"
    category = "scoring"
    min_sample = 20  # total shots
    incremental = True
    supports_career = True

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
        total_shots = len(all_shots)
        paint_shots = sum(1 for s in all_shots if s.shot_zone_basic in _PAINT_ZONES)
        return {
            "paint_shots": paint_shots,
            "total_shots": total_shots,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        ts = totals.get("total_shots", 0)
        if ts < self.min_sample:
            return None
        ps = totals.get("paint_shots", 0)
        share = ps / ts
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(share, 4),
            value_str=f"{share:.1%}",
            context={
                "paint_shot_share": round(share, 4),
                "paint_shots": ps,
                "total_shots": ts,
            },
        )


register(PaintScoringShare())
