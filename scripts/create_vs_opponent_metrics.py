"""Create the 5 player-vs-opponent metrics in MetricDefinition.

Usage:
    .venv/bin/python scripts/create_vs_opponent_metrics.py
"""
from __future__ import annotations

from datetime import datetime

from db.models import MetricDefinition, engine
from sqlalchemy.orm import Session


PLAYER_3PM_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
from db.models import Game, PlayerGameStats


class Player3PMvsOpponent(MetricDefinition):
    key = "player_3pm_vs_opponent"
    name = "3PM vs Each Opponent"
    name_zh = "对位三分命中数"
    description = "Total three-pointers made by a player against each opponent team."
    description_zh = "球员对不同球队的三分球命中数累计。"
    scope = "player"
    category = "scoring"
    min_sample = 1
    trigger = "season"
    incremental = False
    supports_career = True
    rank_order = "desc"
    season_types = ("regular", "playoffs", "playin")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
        )
        if is_career_season(season):
            code = career_season_type_code(season)
            if code:
                q = q.filter(Game.season.like(f"{code}%"))
        else:
            q = q.filter(Game.season == season)

        totals = defaultdict(lambda: {"fg3m": 0, "fg3a": 0, "games": 0})
        for pgs, game in q.all():
            fg3m = int(pgs.fg3m or 0)
            fg3a = int(pgs.fg3a or 0)
            player_id = str(pgs.player_id)
            team_id = str(pgs.team_id)
            home_id = str(game.home_team_id) if game.home_team_id else ""
            road_id = str(game.road_team_id) if game.road_team_id else ""
            if not home_id or not road_id:
                continue
            opp_id = road_id if team_id == home_id else home_id
            key = (player_id, opp_id)
            totals[key]["fg3m"] += fg3m
            totals[key]["fg3a"] += fg3a
            totals[key]["games"] += 1

        results = []
        for (player_id, opp_id), t in totals.items():
            fg3m = t["fg3m"]
            if fg3m <= 0:
                continue
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=float(fg3m),
                value_str=f"{fg3m} 3PM",
                context={
                    "fg3m": fg3m,
                    "fg3a": t["fg3a"],
                    "games": t["games"],
                    "opponent_team_id": opp_id,
                },
            ))
        return results
'''


PLAYER_3P_PCT_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
from db.models import Game, PlayerGameStats


class Player3PctVsOpponent(MetricDefinition):
    key = "player_3p_pct_vs_opponent"
    name = "3PT% vs Each Opponent"
    name_zh = "对位三分命中率"
    description = "Three-point field goal percentage against each opponent (min 20 attempts)."
    description_zh = "球员对不同球队的三分命中率（至少20次出手）。"
    scope = "player"
    category = "scoring"
    min_sample = 20
    trigger = "season"
    incremental = False
    supports_career = True
    rank_order = "desc"
    season_types = ("regular", "playoffs", "playin")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
        )
        if is_career_season(season):
            code = career_season_type_code(season)
            if code:
                q = q.filter(Game.season.like(f"{code}%"))
        else:
            q = q.filter(Game.season == season)

        totals = defaultdict(lambda: {"fg3m": 0, "fg3a": 0, "games": 0})
        for pgs, game in q.all():
            fg3m = int(pgs.fg3m or 0)
            fg3a = int(pgs.fg3a or 0)
            player_id = str(pgs.player_id)
            team_id = str(pgs.team_id)
            home_id = str(game.home_team_id) if game.home_team_id else ""
            road_id = str(game.road_team_id) if game.road_team_id else ""
            if not home_id or not road_id:
                continue
            opp_id = road_id if team_id == home_id else home_id
            key = (player_id, opp_id)
            totals[key]["fg3m"] += fg3m
            totals[key]["fg3a"] += fg3a
            totals[key]["games"] += 1

        results = []
        for (player_id, opp_id), t in totals.items():
            fg3a = t["fg3a"]
            if fg3a < self.min_sample:
                continue
            fg3m = t["fg3m"]
            pct = fg3m / fg3a
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=round(pct, 4),
                value_str=f"{pct:.1%}",
                context={
                    "fg3_pct": round(pct, 4),
                    "fg3m": fg3m,
                    "fg3a": fg3a,
                    "games": t["games"],
                    "opponent_team_id": opp_id,
                },
            ))
        return results
'''


PLAYER_TRIPLE_DOUBLES_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
from db.models import Game, PlayerGameStats


class PlayerTripleDoublesVsOpponent(MetricDefinition):
    key = "player_triple_doubles_vs_opponent"
    name = "Triple-Doubles vs Each Opponent"
    name_zh = "对位三双次数"
    description = "Number of triple-doubles by a player against each opponent team."
    description_zh = "球员对不同球队拿到三双的次数。"
    scope = "player"
    category = "aggregate"
    min_sample = 1
    trigger = "season"
    incremental = False
    supports_career = True
    rank_order = "desc"
    season_types = ("regular", "playoffs", "playin")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
        )
        if is_career_season(season):
            code = career_season_type_code(season)
            if code:
                q = q.filter(Game.season.like(f"{code}%"))
        else:
            q = q.filter(Game.season == season)

        totals = defaultdict(lambda: {"td": 0, "games": 0})
        for pgs, game in q.all():
            pts = int(pgs.pts or 0)
            reb = int(pgs.reb or 0)
            ast = int(pgs.ast or 0)
            stl = int(pgs.stl or 0)
            blk = int(pgs.blk or 0)
            # Classic triple-double: any 3 of pts/reb/ast/stl/blk >= 10. Most commonly p/r/a.
            tally = sum(1 for v in (pts, reb, ast, stl, blk) if v >= 10)
            player_id = str(pgs.player_id)
            team_id = str(pgs.team_id)
            home_id = str(game.home_team_id) if game.home_team_id else ""
            road_id = str(game.road_team_id) if game.road_team_id else ""
            if not home_id or not road_id:
                continue
            opp_id = road_id if team_id == home_id else home_id
            key = (player_id, opp_id)
            totals[key]["games"] += 1
            if tally >= 3:
                totals[key]["td"] += 1

        results = []
        for (player_id, opp_id), t in totals.items():
            td = t["td"]
            if td <= 0:
                continue
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=float(td),
                value_str=f"{td} TD",
                context={
                    "triple_doubles": td,
                    "games": t["games"],
                    "opponent_team_id": opp_id,
                },
            ))
        return results
'''


PLAYER_MAX_PTS_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
from db.models import Game, PlayerGameStats


class PlayerMaxPtsVsOpponent(MetricDefinition):
    key = "player_max_pts_vs_opponent"
    name = "Season-High Points vs Each Opponent"
    name_zh = "对位单场最高得分"
    description = "Single-game high points scored by a player against each opponent team."
    description_zh = "球员对不同球队单场最高得分。"
    scope = "player"
    category = "scoring"
    min_sample = 1
    trigger = "season"
    incremental = False
    supports_career = True
    rank_order = "desc"
    season_types = ("regular", "playoffs", "playin")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
        )
        if is_career_season(season):
            code = career_season_type_code(season)
            if code:
                q = q.filter(Game.season.like(f"{code}%"))
        else:
            q = q.filter(Game.season == season)

        best = {}  # (player_id, opp_id) -> dict
        counts = defaultdict(int)
        for pgs, game in q.all():
            pts = int(pgs.pts or 0)
            player_id = str(pgs.player_id)
            team_id = str(pgs.team_id)
            home_id = str(game.home_team_id) if game.home_team_id else ""
            road_id = str(game.road_team_id) if game.road_team_id else ""
            if not home_id or not road_id:
                continue
            opp_id = road_id if team_id == home_id else home_id
            key = (player_id, opp_id)
            counts[key] += 1
            cur = best.get(key)
            if cur is None or pts > cur["max_pts"]:
                best[key] = {
                    "max_pts": pts,
                    "game_id": str(game.game_id),
                    "game_date": str(game.game_date) if game.game_date else "",
                    "season": str(game.season) if game.season else "",
                }

        results = []
        for (player_id, opp_id), b in best.items():
            if b["max_pts"] <= 0:
                continue
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=float(b["max_pts"]),
                value_str=f"{b['max_pts']} PTS",
                context={
                    "max_pts": b["max_pts"],
                    "max_game_id": b["game_id"],
                    "max_game_date": b["game_date"],
                    "max_game_season": b["season"],
                    "games": counts[(player_id, opp_id)],
                    "opponent_team_id": opp_id,
                },
            ))
        return results
'''


PLAYER_MAX_AST_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult, is_career_season, career_season_type_code
from db.models import Game, PlayerGameStats


class PlayerMaxAstVsOpponent(MetricDefinition):
    key = "player_max_ast_vs_opponent"
    name = "Season-High Assists vs Each Opponent"
    name_zh = "对位单场最高助攻"
    description = "Single-game high assists by a player against each opponent team."
    description_zh = "球员对不同球队单场最高助攻数。"
    scope = "player"
    category = "playmaking"
    min_sample = 1
    trigger = "season"
    incremental = False
    supports_career = True
    rank_order = "desc"
    season_types = ("regular", "playoffs", "playin")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
        )
        if is_career_season(season):
            code = career_season_type_code(season)
            if code:
                q = q.filter(Game.season.like(f"{code}%"))
        else:
            q = q.filter(Game.season == season)

        best = {}
        counts = defaultdict(int)
        for pgs, game in q.all():
            ast = int(pgs.ast or 0)
            player_id = str(pgs.player_id)
            team_id = str(pgs.team_id)
            home_id = str(game.home_team_id) if game.home_team_id else ""
            road_id = str(game.road_team_id) if game.road_team_id else ""
            if not home_id or not road_id:
                continue
            opp_id = road_id if team_id == home_id else home_id
            key = (player_id, opp_id)
            counts[key] += 1
            cur = best.get(key)
            if cur is None or ast > cur["max_ast"]:
                best[key] = {
                    "max_ast": ast,
                    "game_id": str(game.game_id),
                    "game_date": str(game.game_date) if game.game_date else "",
                    "season": str(game.season) if game.season else "",
                }

        results = []
        for (player_id, opp_id), b in best.items():
            if b["max_ast"] <= 0:
                continue
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=float(b["max_ast"]),
                value_str=f"{b['max_ast']} AST",
                context={
                    "max_ast": b["max_ast"],
                    "max_game_id": b["game_id"],
                    "max_game_date": b["game_date"],
                    "max_game_season": b["season"],
                    "games": counts[(player_id, opp_id)],
                    "opponent_team_id": opp_id,
                },
            ))
        return results
'''


METRICS = [
    {
        "key": "player_3pm_vs_opponent",
        "family_key": "player_3pm_vs_opponent",
        "name": "3PM vs Each Opponent",
        "name_zh": "对位三分命中数",
        "description": "Total three-pointers made by a player against each opponent team.",
        "description_zh": "球员对不同球队的三分球命中数累计。",
        "scope": "player",
        "category": "scoring",
        "min_sample": 1,
        "code_python": PLAYER_3PM_VS_OPPONENT.strip(),
    },
    {
        "key": "player_3p_pct_vs_opponent",
        "family_key": "player_3p_pct_vs_opponent",
        "name": "3PT% vs Each Opponent",
        "name_zh": "对位三分命中率",
        "description": "Three-point field goal percentage against each opponent (min 20 attempts).",
        "description_zh": "球员对不同球队的三分命中率(至少20次出手)。",
        "scope": "player",
        "category": "scoring",
        "min_sample": 20,
        "code_python": PLAYER_3P_PCT_VS_OPPONENT.strip(),
    },
    {
        "key": "player_triple_doubles_vs_opponent",
        "family_key": "player_triple_doubles_vs_opponent",
        "name": "Triple-Doubles vs Each Opponent",
        "name_zh": "对位三双次数",
        "description": "Number of triple-doubles by a player against each opponent team.",
        "description_zh": "球员对不同球队拿到三双的次数。",
        "scope": "player",
        "category": "aggregate",
        "min_sample": 1,
        "code_python": PLAYER_TRIPLE_DOUBLES_VS_OPPONENT.strip(),
    },
    {
        "key": "player_max_pts_vs_opponent",
        "family_key": "player_max_pts_vs_opponent",
        "name": "Season-High Points vs Each Opponent",
        "name_zh": "对位单场最高得分",
        "description": "Single-game high points scored by a player against each opponent team.",
        "description_zh": "球员对不同球队的单场最高得分。",
        "scope": "player",
        "category": "scoring",
        "min_sample": 1,
        "code_python": PLAYER_MAX_PTS_VS_OPPONENT.strip(),
    },
    {
        "key": "player_max_ast_vs_opponent",
        "family_key": "player_max_ast_vs_opponent",
        "name": "Season-High Assists vs Each Opponent",
        "name_zh": "对位单场最高助攻",
        "description": "Single-game high assists by a player against each opponent team.",
        "description_zh": "球员对不同球队的单场最高助攻。",
        "scope": "player",
        "category": "playmaking",
        "min_sample": 1,
        "code_python": PLAYER_MAX_AST_VS_OPPONENT.strip(),
    },
]


def upsert_metric(session: Session, spec: dict) -> None:
    existing = session.query(MetricDefinition).filter(MetricDefinition.key == spec["key"]).one_or_none()
    now = datetime.utcnow()
    if existing is None:
        md = MetricDefinition(
            key=spec["key"],
            family_key=spec["family_key"],
            variant="season",
            managed_family=False,
            name=spec["name"],
            name_zh=spec["name_zh"],
            description=spec["description"],
            description_zh=spec["description_zh"],
            scope=spec["scope"],
            category=spec["category"],
            source_type="code",
            status="published",
            code_python=spec["code_python"],
            min_sample=spec["min_sample"],
            sub_key_type="team",
            sub_key_label="Opponent",
            sub_key_label_zh="对手球队",
            sub_key_rank_scope="entity",
            created_at=now,
            updated_at=now,
        )
        session.add(md)
        action = "created"
    else:
        existing.name = spec["name"]
        existing.name_zh = spec["name_zh"]
        existing.description = spec["description"]
        existing.description_zh = spec["description_zh"]
        existing.scope = spec["scope"]
        existing.category = spec["category"]
        existing.source_type = "code"
        existing.status = "published"
        existing.code_python = spec["code_python"]
        existing.min_sample = spec["min_sample"]
        existing.sub_key_type = "team"
        existing.sub_key_label = "Opponent"
        existing.sub_key_label_zh = "对手球队"
        existing.sub_key_rank_scope = "entity"
        existing.updated_at = now
        action = "updated"
    print(f"  {action}: {spec['key']}")


def main() -> None:
    with Session(engine) as session:
        for spec in METRICS:
            upsert_metric(session, spec)
        session.commit()
    print(f"\nDone. {len(METRICS)} metrics in MetricDefinition.")


if __name__ == "__main__":
    main()
