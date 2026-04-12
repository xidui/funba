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

from metrics.framework.base import MetricDefinition, MetricResult
from db.models import Game, PlayerGameStats

QUAL_CAP = 500


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

    career_aggregate_mode = "season_results"
    career_group_by_sub_key = True
    career_sum_keys = ("fg3m", "fg3a", "games")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
            .filter(Game.season == season)
        )

        totals = defaultdict(lambda: {"fg3m": 0, "fg3a": 0, "games": 0, "game_rows": []})
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
            if fg3m > 0 or fg3a > 0:
                totals[key]["game_rows"].append((str(game.game_date) if game.game_date else "", str(game.game_id), fg3m))

        results = []
        for (player_id, opp_id), t in totals.items():
            fg3m = t["fg3m"]
            if fg3m <= 0:
                continue
            rows = sorted(t["game_rows"], key=lambda r: (-r[2], r[0]))
            total_qual = len(rows)
            capped = rows[:QUAL_CAP]
            ctx = {
                "fg3m": fg3m,
                "fg3a": t["fg3a"],
                "games": t["games"],
                "opponent_team_id": opp_id,
                "qualifying_game_ids": [r[1] for r in capped],
            }
            if total_qual > QUAL_CAP:
                ctx["qualifying_game_total"] = total_qual
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=float(fg3m),
                value_str=f"{fg3m} 3PM",
                context=ctx,
            ))
        return results

    def compute_career_value(self, totals, season, entity_id, sub_key="", rows=None):
        fg3m = int(totals.get("fg3m", 0))
        if fg3m <= 0:
            return None
        fg3a = int(totals.get("fg3a", 0))
        games = int(totals.get("games", 0))
        # Merge qualifying_game_ids across all season rows (sorted by date desc, capped).
        qual = []
        for ctx in (rows or []):
            qual.extend(ctx.get("qualifying_game_ids") or [])
        total_qual = len(qual)
        capped = qual[:QUAL_CAP]
        ctx_out = {
            "fg3m": fg3m,
            "fg3a": fg3a,
            "games": games,
            "opponent_team_id": sub_key,
            "qualifying_game_ids": capped,
        }
        if total_qual > QUAL_CAP:
            ctx_out["qualifying_game_total"] = total_qual
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            sub_key=sub_key,
            value_num=float(fg3m),
            value_str=f"{fg3m} 3PM",
            context=ctx_out,
        )
'''


PLAYER_3P_PCT_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult
from db.models import Game, PlayerGameStats

QUAL_CAP = 500


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

    career_aggregate_mode = "season_results"
    career_group_by_sub_key = True
    career_sum_keys = ("fg3m", "fg3a", "games")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
            .filter(Game.season == season)
        )

        totals = defaultdict(lambda: {"fg3m": 0, "fg3a": 0, "games": 0, "game_rows": []})
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
            if fg3a > 0:
                totals[key]["game_rows"].append((str(game.game_date) if game.game_date else "", str(game.game_id), fg3m, fg3a))

        results = []
        for (player_id, opp_id), t in totals.items():
            fg3a = t["fg3a"]
            if fg3a < self.min_sample:
                continue
            fg3m = t["fg3m"]
            pct = fg3m / fg3a
            rows = sorted(t["game_rows"], key=lambda r: r[0], reverse=True)
            total_qual = len(rows)
            capped = rows[:QUAL_CAP]
            ctx = {
                "fg3_pct": round(pct, 4),
                "fg3m": fg3m,
                "fg3a": fg3a,
                "games": t["games"],
                "opponent_team_id": opp_id,
                "qualifying_game_ids": [r[1] for r in capped],
            }
            if total_qual > QUAL_CAP:
                ctx["qualifying_game_total"] = total_qual
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=round(pct, 4),
                value_str=f"{pct:.1%}",
                context=ctx,
            ))
        return results

    def compute_career_value(self, totals, season, entity_id, sub_key="", rows=None):
        fg3a = int(totals.get("fg3a", 0))
        if fg3a < self.min_sample:
            return None
        fg3m = int(totals.get("fg3m", 0))
        games = int(totals.get("games", 0))
        pct = fg3m / fg3a
        qual = []
        for ctx in (rows or []):
            qual.extend(ctx.get("qualifying_game_ids") or [])
        total_qual = len(qual)
        capped = qual[:QUAL_CAP]
        ctx_out = {
            "fg3_pct": round(pct, 4),
            "fg3m": fg3m,
            "fg3a": fg3a,
            "games": games,
            "opponent_team_id": sub_key,
            "qualifying_game_ids": capped,
        }
        if total_qual > QUAL_CAP:
            ctx_out["qualifying_game_total"] = total_qual
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            sub_key=sub_key,
            value_num=round(pct, 4),
            value_str=f"{pct:.1%}",
            context=ctx_out,
        )
'''


PLAYER_TRIPLE_DOUBLES_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult
from db.models import Game, PlayerGameStats

QUAL_CAP = 500


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

    career_aggregate_mode = "season_results"
    career_group_by_sub_key = True
    career_sum_keys = ("triple_doubles", "games")

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
            .filter(Game.season == season)
        )

        totals = defaultdict(lambda: {"td": 0, "games": 0, "td_games": []})
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
                totals[key]["td_games"].append((str(game.game_date) if game.game_date else "", str(game.game_id)))

        results = []
        for (player_id, opp_id), t in totals.items():
            td = t["td"]
            if td <= 0:
                continue
            rows = sorted(t["td_games"], key=lambda r: r[0], reverse=True)
            total_qual = len(rows)
            capped = rows[:QUAL_CAP]
            ctx = {
                "triple_doubles": td,
                "games": t["games"],
                "opponent_team_id": opp_id,
                "qualifying_game_ids": [r[1] for r in capped],
            }
            if total_qual > QUAL_CAP:
                ctx["qualifying_game_total"] = total_qual
            results.append(MetricResult(
                metric_key=self.key,
                entity_type="player",
                entity_id=player_id,
                season=season,
                game_id=None,
                sub_key=opp_id,
                value_num=float(td),
                value_str=f"{td} TD",
                context=ctx,
            ))
        return results

    def compute_career_value(self, totals, season, entity_id, sub_key="", rows=None):
        td = int(totals.get("triple_doubles", 0))
        if td <= 0:
            return None
        games = int(totals.get("games", 0))
        qual = []
        for ctx in (rows or []):
            qual.extend(ctx.get("qualifying_game_ids") or [])
        total_qual = len(qual)
        capped = qual[:QUAL_CAP]
        ctx_out = {
            "triple_doubles": td,
            "games": games,
            "opponent_team_id": sub_key,
            "qualifying_game_ids": capped,
        }
        if total_qual > QUAL_CAP:
            ctx_out["qualifying_game_total"] = total_qual
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            sub_key=sub_key,
            value_num=float(td),
            value_str=f"{td} TD",
            context=ctx_out,
        )
'''


PLAYER_MAX_PTS_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult
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

    career_aggregate_mode = "season_results"
    career_group_by_sub_key = True
    career_max_keys = ("max_pts",)
    career_sum_keys = ("games",)

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
            .filter(Game.season == season)
        )

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
                    "qualifying_game_ids": [b["game_id"]],
                },
            ))
        return results

    def compute_career_value(self, totals, season, entity_id, sub_key="", rows=None):
        max_pts = int(totals.get("max_pts", 0) or 0)
        if max_pts <= 0:
            return None
        # Find the season row that produced the max to pull its game_id/date.
        best_ctx = None
        for ctx in (rows or []):
            pts = int(ctx.get("max_pts") or 0)
            if best_ctx is None or pts > int(best_ctx.get("max_pts") or 0):
                best_ctx = ctx
        best_ctx = best_ctx or {}
        games = int(totals.get("games", 0))
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            sub_key=sub_key,
            value_num=float(max_pts),
            value_str=f"{max_pts} PTS",
            context={
                "max_pts": max_pts,
                "max_game_id": best_ctx.get("max_game_id"),
                "max_game_date": best_ctx.get("max_game_date"),
                "max_game_season": best_ctx.get("max_game_season"),
                "games": games,
                "opponent_team_id": sub_key,
                "qualifying_game_ids": [best_ctx.get("max_game_id")] if best_ctx.get("max_game_id") else [],
            },
        )
'''


PLAYER_MAX_AST_VS_OPPONENT = '''
from __future__ import annotations

from collections import defaultdict

from metrics.framework.base import MetricDefinition, MetricResult
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

    career_aggregate_mode = "season_results"
    career_group_by_sub_key = True
    career_max_keys = ("max_ast",)
    career_sum_keys = ("games",)

    def compute_season(self, session, season):
        q = (
            session.query(PlayerGameStats, Game)
            .join(Game, Game.game_id == PlayerGameStats.game_id)
            .filter(PlayerGameStats.player_id.isnot(None))
            .filter(Game.season == season)
        )

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
                    "qualifying_game_ids": [b["game_id"]],
                },
            ))
        return results

    def compute_career_value(self, totals, season, entity_id, sub_key="", rows=None):
        max_ast = int(totals.get("max_ast", 0) or 0)
        if max_ast <= 0:
            return None
        best_ctx = None
        for ctx in (rows or []):
            ast = int(ctx.get("max_ast") or 0)
            if best_ctx is None or ast > int(best_ctx.get("max_ast") or 0):
                best_ctx = ctx
        best_ctx = best_ctx or {}
        games = int(totals.get("games", 0))
        return MetricResult(
            metric_key=self.key,
            entity_type="player",
            entity_id=entity_id,
            season=season,
            game_id=None,
            sub_key=sub_key,
            value_num=float(max_ast),
            value_str=f"{max_ast} AST",
            context={
                "max_ast": max_ast,
                "max_game_id": best_ctx.get("max_game_id"),
                "max_game_date": best_ctx.get("max_game_date"),
                "max_game_season": best_ctx.get("max_game_season"),
                "games": games,
                "opponent_team_id": sub_key,
                "qualifying_game_ids": [best_ctx.get("max_game_id")] if best_ctx.get("max_game_id") else [],
            },
        )
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
        "fill_missing_sub_keys_with_zero": True,
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
        "fill_missing_sub_keys_with_zero": False,
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
        "fill_missing_sub_keys_with_zero": True,
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
        "fill_missing_sub_keys_with_zero": True,
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
        "fill_missing_sub_keys_with_zero": True,
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
            group_key="opponent_splits",
            sub_key_type="team",
            sub_key_label="Opponent",
            sub_key_label_zh="对手球队",
            sub_key_rank_scope="entity",
            fill_missing_sub_keys_with_zero=spec.get("fill_missing_sub_keys_with_zero", False),
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
        existing.group_key = "opponent_splits"
        existing.sub_key_type = "team"
        existing.sub_key_label = "Opponent"
        existing.sub_key_label_zh = "对手球队"
        existing.sub_key_rank_scope = "entity"
        existing.fill_missing_sub_keys_with_zero = spec.get("fill_missing_sub_keys_with_zero", False)
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
