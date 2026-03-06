"""True Shooting %: PTS / (2 * (FGA + 0.44 * FTA)) — gold-standard scoring efficiency."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult
from metrics.framework.registry import register
from db.models import PlayerGameStats, Game
from sqlalchemy import func


class TrueShootingPct(MetricDefinition):
    key = "true_shooting_pct"
    name = "True Shooting %"
    description = "Points scored per shooting opportunity, accounting for 2s, 3s, and free throws (TS% = PTS / (2 × (FGA + 0.44 × FTA)))."
    scope = "player"
    category = "efficiency"
    min_sample = 20

    def compute(self, session, entity_id, season, game_id=None):
        row = (
            session.query(
                func.sum(func.coalesce(PlayerGameStats.pts, 0)).label("pts"),
                func.sum(func.coalesce(PlayerGameStats.fga, 0)).label("fga"),
                func.sum(func.coalesce(PlayerGameStats.fta, 0)).label("fta"),
                func.count(PlayerGameStats.game_id).label("games"),
            )
            .join(Game, PlayerGameStats.game_id == Game.game_id)
            .filter(PlayerGameStats.player_id == entity_id, Game.season == season)
            .one()
        )

        games = int(row.games or 0)
        if games < self.min_sample:
            return None

        pts = float(row.pts or 0)
        fga = float(row.fga or 0)
        fta = float(row.fta or 0)
        denominator = 2 * (fga + 0.44 * fta)
        if denominator == 0:
            return None

        ts = pts / denominator

        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(ts, 4),
            value_str=f"{ts:.1%}",
            context={
                "ts_pct": round(ts, 4),
                "pts": int(pts),
                "fga": int(fga),
                "fta": int(fta),
                "games": games,
            },
        )


register(TrueShootingPct())
