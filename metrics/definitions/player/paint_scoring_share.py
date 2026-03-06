"""Paint Scoring Share: % of shot attempts taken in the paint (restricted area + non-RA paint)."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import ShotRecord, Game
from sqlalchemy import case, func


_PAINT_ZONES = ("Restricted Area", "In The Paint (Non-RA)")


class PaintScoringShare(MetricDefinition):
    key = "paint_scoring_share"
    name = "Paint Shot Share"
    description = "Percentage of shot attempts taken inside the paint (restricted area + non-RA paint) this season."
    scope = "player"
    category = "scoring"
    min_sample = 20

    def compute(self, session, entity_id, season, game_id=None):
        row = (
            session.query(
                func.count(ShotRecord.id).label("total_shots"),
                func.sum(
                    case((ShotRecord.shot_zone_basic.in_(_PAINT_ZONES), 1), else_=0)
                ).label("paint_shots"),
            )
            .join(Game, ShotRecord.game_id == Game.game_id)
            .filter(
                ShotRecord.player_id == entity_id,
                Game.season == season,
                ShotRecord.shot_attempted.is_(True),
            )
            .one()
        )

        total = int(row.total_shots or 0)
        if total < self.min_sample:
            return None

        paint = int(row.paint_shots or 0)
        share = paint / total

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
                "paint_shots": paint,
                "total_shots": total,
            },
        )


register(PaintScoringShare())
