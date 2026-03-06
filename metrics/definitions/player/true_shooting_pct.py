"""True Shooting %: PTS / (2 * (FGA + 0.44 * FTA)) — gold-standard scoring efficiency."""
from __future__ import annotations

from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON
from metrics.framework.registry import register
from db.models import PlayerGameStats


class TrueShootingPct(MetricDefinition):
    key = "true_shooting_pct"
    name = "True Shooting %"
    description = "Points scored per shooting opportunity, accounting for 2s, 3s, and free throws (TS% = PTS / (2 × (FGA + 0.44 × FTA)))."
    scope = "player"
    category = "efficiency"
    min_sample = 20
    incremental = True
    supports_career = True
    career_min_sample = 100

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        row = (
            session.query(PlayerGameStats)
            .filter(
                PlayerGameStats.player_id == entity_id,
                PlayerGameStats.game_id == game_id,
            )
            .first()
        )
        if row is None:
            return None
        return {
            "pts": int(row.pts or 0),
            "fga": int(row.fga or 0),
            "fta": int(row.fta or 0),
            "games": 1,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games = totals.get("games", 0)
        if games < self.min_sample:
            return None
        pts = totals.get("pts", 0)
        fga = totals.get("fga", 0)
        fta = totals.get("fta", 0)
        tsa = fga + 0.44 * fta  # True Shooting Attempts
        # Require at least 2 TSA per game on average to filter out bench players
        if tsa < games * 2:
            return None
        denom = 2 * tsa
        if denom == 0:
            return None
        ts = pts / denom
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(ts, 4),
            value_str=f"{ts:.1%}",
            context={
                "pts": pts,
                "fga": fga,
                "fta": fta,
                "games": games,
                "ts_pct": round(ts, 4),
            },
        )


register(TrueShootingPct())
