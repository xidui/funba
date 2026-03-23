"""optimize pbp metrics with helpers

Revision ID: b4c5d6e7f8a9
Revises: a4b5c6d7e8f9
Create Date: 2026-03-23 00:10:00.000000
"""
from __future__ import annotations

from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from metrics.framework.family import build_career_code_variant


revision: str = "b4c5d6e7f8a9"
down_revision: Union[str, Sequence[str], None] = "a4b5c6d7e8f9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_SEASON_CODE_BY_KEY = {
    "buzzer_beater_losses": '''from __future__ import annotations

from metrics.helpers import game_row, late_final_score_margin_rows, team_game_stat
from metrics.framework.base import MetricDefinition, MetricResult


class BuzzerBeaterLosses(MetricDefinition):
    key = "buzzer_beater_losses"
    name = "Buzzer-Beater Losses"
    description = "Number of games a team lost after being ahead with 10 seconds or less remaining in regulation or overtime."
    scope = "team"
    category = "conditional"
    min_sample = 1
    incremental = True
    supports_career = True
    rank_order = "desc"
    career_name_suffix = " (All-Time)"
    context_label_template = "{buzzer_beater_losses}/{games_played} games"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = team_game_stat(session, game_id, entity_id)
        if tgs is None or tgs.win is None:
            return None

        game = game_row(session, game_id)
        if game is None:
            return None

        is_home = str(game.home_team_id) == str(entity_id)
        if not is_home and str(game.road_team_id) != str(entity_id):
            return None

        late_rows = late_final_score_margin_rows(session, game_id, seconds_left=10)
        if not late_rows:
            return {"games_played": 1, "buzzer_beater_losses": 0}

        team_led_in_last_10 = False
        for row in late_rows:
            try:
                margin = int(row.score_margin)
            except (ValueError, TypeError):
                continue
            team_margin = margin if is_home else -margin
            if team_margin > 0:
                team_led_in_last_10 = True
                break

        return {
            "games_played": 1,
            "buzzer_beater_losses": 1 if (team_led_in_last_10 and (tgs.win is False)) else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games_played = totals.get("games_played", 0)
        if games_played < self.min_sample:
            return None

        losses = totals.get("buzzer_beater_losses", 0)
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(losses),
            value_str=f"{losses} loss{'es' if losses != 1 else ''}",
            context={
                "buzzer_beater_losses": losses,
                "games_played": games_played,
            },
        )
''',
    "buzzer_beater_wins": '''from __future__ import annotations

from metrics.helpers import game_row, late_final_score_margin_rows, team_game_stat
from metrics.framework.base import MetricDefinition, MetricResult


class BuzzerBeaterWins(MetricDefinition):
    key = "buzzer_beater_wins"
    name = "Buzzer-Beater Wins"
    description = "Number of team wins where the game was tied or the team was trailing at some point in the final 10 seconds, and the team finished the game ahead."
    scope = "team"
    category = "aggregate"
    min_sample = 1
    incremental = True
    supports_career = True
    rank_order = "desc"
    career_name_suffix = " (All-Time)"
    context_label_template = "{buzzer_beater_wins} wins"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = team_game_stat(session, game_id, entity_id)
        if tgs is None or not tgs.win:
            return None

        game = game_row(session, game_id)
        if game is None:
            return None

        is_home = str(game.home_team_id) == str(entity_id)
        if not is_home and str(game.road_team_id) != str(entity_id):
            return None

        late_rows = late_final_score_margin_rows(session, game_id, seconds_left=10)
        if not late_rows:
            return {"games": 1, "buzzer_beater_wins": 0}

        saw_tied_or_trailing = False
        for row in late_rows:
            try:
                margin = int(row.score_margin)
            except (ValueError, TypeError):
                continue
            team_margin = margin if is_home else -margin
            if team_margin <= 0:
                saw_tied_or_trailing = True
                break

        return {
            "games": 1,
            "buzzer_beater_wins": 1 if saw_tied_or_trailing else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games = totals.get("games", 0)
        if games < self.min_sample:
            return None

        wins = totals.get("buzzer_beater_wins", 0)
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(wins),
            value_str=f"{wins} wins",
            context={
                "buzzer_beater_wins": wins,
                "games": games,
            },
        )
''',
    "comeback_win_pct": '''"""Comeback Win %: win% in games where team was trailing at halftime."""
from __future__ import annotations

from metrics.helpers import game_row, period_ending_pbp_row, team_game_stat
from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON


class ComebackWinPct(MetricDefinition):
    key = 'comeback_win_pct'
    name = "Comeback Win %"
    description = "Win % in games where the team was trailing at halftime."
    scope = "team"
    category = "conditional"
    min_sample = 5
    incremental = True
    supports_career = True
    rank_order = "desc"
    context_label_template = "{trailing_wins}/{trailing_total} trailing"
    career_name_suffix = " (All-Time)"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = team_game_stat(session, game_id, entity_id)
        if tgs is None or tgs.win is None:
            return None
        game = game_row(session, game_id)
        if game is None:
            return None
        is_home = game.home_team_id == entity_id

        pbp_row = period_ending_pbp_row(session, game_id, 2)
        if pbp_row is None or pbp_row.score_margin in (None, "null", ""):
            return {"total_games": 1, "trailing_total": 0, "trailing_wins": 0}

        try:
            margin = int(pbp_row.score_margin)
        except (ValueError, TypeError):
            return {"total_games": 1, "trailing_total": 0, "trailing_wins": 0}

        team_trailing = margin < 0 if is_home else margin > 0
        if not team_trailing:
            return {"total_games": 1, "trailing_total": 0, "trailing_wins": 0}

        return {
            "total_games": 1,
            "trailing_total": 1,
            "trailing_wins": 1 if tgs.win else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        trailing_total = totals.get("trailing_total", 0)
        if trailing_total < self.min_sample:
            return None
        trailing_wins = totals.get("trailing_wins", 0)
        win_pct = trailing_wins / trailing_total
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            value_str=f"{win_pct:.1%}",
            context={
                "comeback_win_pct": round(win_pct, 4),
                "comeback_wins": trailing_wins,
                "games_trailing_at_half": trailing_total,
                "total_games": totals.get("total_games", 0),
            },
        )
''',
    "ten_plus_point_comeback_wins": '''from __future__ import annotations

from metrics.helpers import game_row, game_score_margin_rows, team_game_stat
from metrics.framework.base import MetricDefinition, MetricResult


class TenPlusPointComebackWins(MetricDefinition):
    key = "ten_plus_point_comeback_wins"
    name = "10+ Point Comeback Wins"
    description = "Number of games a team won after trailing by more than 10 points at some point during the game."
    scope = "team"
    category = "aggregate"
    min_sample = 1
    incremental = True
    supports_career = True
    rank_order = "desc"
    career_name_suffix = " (All-Time)"
    context_label_template = "{comeback_wins}/{games} games"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = team_game_stat(session, game_id, entity_id)
        if tgs is None or tgs.win is None:
            return None

        game = game_row(session, game_id)
        if game is None:
            return None

        is_home = str(game.home_team_id) == str(entity_id)
        if not is_home and str(game.road_team_id) != str(entity_id):
            return None

        pbp_rows = game_score_margin_rows(session, game_id)
        if not pbp_rows:
            return {"games": 1, "comeback_wins": 0}

        trailed_by_11_plus = False
        for row in pbp_rows:
            try:
                margin = int(row.score_margin)
            except (ValueError, TypeError):
                continue
            team_margin = margin if is_home else -margin
            if team_margin <= -11:
                trailed_by_11_plus = True
                break

        return {
            "games": 1,
            "comeback_wins": 1 if (trailed_by_11_plus and tgs.win) else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        games = totals.get("games", 0)
        if games < self.min_sample:
            return None

        comeback_wins = totals.get("comeback_wins", 0)
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(comeback_wins),
            value_str=f"{comeback_wins} wins",
            context={
                "comeback_wins": comeback_wins,
                "games": games,
            },
        )
''',
    "twenty_point_comeback_wins": '''from __future__ import annotations

from metrics.helpers import game_row, game_score_margin_rows, team_game_stat
from metrics.framework.base import MetricDefinition, MetricResult


class TwentyPointComebackWins(MetricDefinition):
    key = "twenty_point_comeback_wins"
    name = "20-Point Comeback Wins"
    description = "Number of wins in games where the team trailed by 20 or more points at any point and still came back to win."
    scope = "team"
    category = "conditional"
    min_sample = 1
    incremental = True
    supports_career = True
    rank_order = "desc"
    context_label_template = "{comeback_wins}/{qualifying_games} wins"
    career_name_suffix = " (All-Time)"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = team_game_stat(session, game_id, entity_id)
        if tgs is None or tgs.win is None:
            return None

        game = game_row(session, game_id)
        if game is None:
            return None

        is_home = str(game.home_team_id) == str(entity_id)
        if not is_home and str(game.road_team_id) != str(entity_id):
            return None

        pbp_rows = game_score_margin_rows(session, game_id)
        if not pbp_rows:
            return {
                "games": 1,
                "qualifying_games": 0,
                "comeback_wins": 0,
            }

        max_deficit = 0
        for row in pbp_rows:
            try:
                margin = int(row.score_margin)
            except (ValueError, TypeError):
                continue
            team_margin = margin if is_home else -margin
            deficit = -team_margin if team_margin < 0 else 0
            if deficit > max_deficit:
                max_deficit = deficit

        qualifying = 1 if max_deficit >= 20 else 0
        comeback_win = 1 if qualifying and tgs.win else 0

        return {
            "games": 1,
            "qualifying_games": qualifying,
            "comeback_wins": comeback_win,
            "max_deficit": max_deficit,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        comeback_wins = int(totals.get("comeback_wins", 0))
        qualifying_games = int(totals.get("qualifying_games", 0))
        if comeback_wins < self.min_sample:
            return None

        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=float(comeback_wins),
            value_str=f"{comeback_wins} wins",
            context={
                "comeback_wins": comeback_wins,
                "qualifying_games": qualifying_games,
                "games": int(totals.get("games", 0)),
                "max_deficit": int(totals.get("max_deficit", 0)),
            },
        )
''',
    "win_pct_leading_at_half": '''"""Win% when leading at halftime."""
from __future__ import annotations

from metrics.helpers import game_row, period_ending_pbp_row, team_game_stat
from metrics.framework.base import MetricDefinition, MetricResult, CAREER_SEASON


class WinPctLeadingAtHalf(MetricDefinition):
    key = 'win_pct_leading_at_half'
    name = "Leads-at-Half Win%"
    description = "Win % in games where the team was leading at halftime."
    scope = "team"
    category = "conditional"
    min_sample = 5
    incremental = True
    supports_career = True
    rank_order = "desc"
    context_label_template = "{leading_wins}/{leading_total} at half"
    career_name_suffix = " (All-Time)"

    def compute_delta(self, session, entity_id, game_id) -> dict | None:
        tgs = team_game_stat(session, game_id, entity_id)
        if tgs is None or tgs.win is None:
            return None
        game = game_row(session, game_id)
        if game is None:
            return None
        is_home = game.home_team_id == entity_id

        pbp_row = period_ending_pbp_row(session, game_id, 2)
        if pbp_row is None or pbp_row.score_margin in (None, "null", ""):
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}

        try:
            margin = int(pbp_row.score_margin)
        except (ValueError, TypeError):
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}

        team_leading = margin > 0 if is_home else margin < 0
        if not team_leading:
            return {"total_games": 1, "leading_total": 0, "leading_wins": 0}

        return {
            "total_games": 1,
            "leading_total": 1,
            "leading_wins": 1 if tgs.win else 0,
        }

    def compute_value(self, totals, season, entity_id) -> MetricResult | None:
        leading_total = totals.get("leading_total", 0)
        if leading_total < self.min_sample:
            return None
        leading_wins = totals.get("leading_wins", 0)
        win_pct = leading_wins / leading_total
        return MetricResult(
            metric_key=self.key,
            entity_type="team",
            entity_id=entity_id,
            season=season,
            game_id=None,
            value_num=round(win_pct, 4),
            context={
                "win_pct_leading_at_half": round(win_pct, 4),
                "wins": leading_wins,
                "games_leading_at_half": leading_total,
                "total_games": totals.get("total_games", 0),
            },
        )
''',
    "lead_changes": '''"""Lead Changes: number of times the lead changed hands during a game."""
from __future__ import annotations

from metrics.helpers import game_score_margin_rows
from metrics.framework.base import MetricDefinition, MetricResult


class LeadChanges(MetricDefinition):
    key = "lead_changes"
    name = "Lead Changes"
    description = "Number of times the lead changed hands during the game — high counts signal a closely-contested thriller."
    scope = "game"
    category = "aggregate"
    min_sample = 1
    incremental = False

    def compute(self, session, entity_id, season, game_id=None):
        target_game = entity_id
        pbp_rows = game_score_margin_rows(session, target_game)
        if not pbp_rows:
            return None

        lead_changes = 0
        prev_leader = None
        for row in pbp_rows:
            try:
                margin = int(row.score_margin)
            except (ValueError, TypeError):
                continue

            if margin > 0:
                leader = 1
            elif margin < 0:
                leader = -1
            else:
                leader = 0

            if prev_leader is not None and leader != 0 and leader != prev_leader and prev_leader != 0:
                lead_changes += 1

            if leader != 0:
                prev_leader = leader

        return MetricResult(
            metric_key=self.key,
            entity_type="game",
            entity_id=target_game,
            season=season,
            game_id=target_game,
            value_num=float(lead_changes),
            value_str=f"{lead_changes} lead change{'s' if lead_changes != 1 else ''}",
            context={
                "lead_changes": lead_changes,
                "game_id": target_game,
            },
        )
''',
}


def upgrade() -> None:
    bind = op.get_bind()
    metadata = sa.MetaData()
    metric_definition = sa.Table(
        "MetricDefinition",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("key", sa.String(64)),
        sa.Column("family_key", sa.String(64)),
        sa.Column("variant", sa.String(16)),
        sa.Column("name", sa.String(128)),
        sa.Column("description", sa.Text),
        sa.Column("min_sample", sa.Integer),
        sa.Column("source_type", sa.String(16)),
        sa.Column("code_python", sa.Text),
        sa.Column("updated_at", sa.DateTime),
    )

    rows = list(
        bind.execute(
            sa.select(metric_definition).where(
                metric_definition.c.source_type == "code",
                metric_definition.c.family_key.in_(list(_SEASON_CODE_BY_KEY.keys())),
            )
        ).mappings()
    )
    rows_by_key = {row["key"]: row for row in rows}
    now = datetime.utcnow()

    for family_key, season_code in _SEASON_CODE_BY_KEY.items():
        season_row = rows_by_key.get(family_key)
        if season_row is not None:
            bind.execute(
                metric_definition.update()
                .where(metric_definition.c.id == season_row["id"])
                .values(code_python=season_code, updated_at=now)
            )

        career_key = f"{family_key}_career"
        career_row = rows_by_key.get(career_key)
        if career_row is not None:
            career_code = build_career_code_variant(
                season_code,
                base_key=family_key,
                name=career_row["name"],
                description=career_row["description"] or "",
                min_sample=int(career_row["min_sample"] or 1),
            )
            bind.execute(
                metric_definition.update()
                .where(metric_definition.c.id == career_row["id"])
                .values(code_python=career_code, updated_at=now)
            )


def downgrade() -> None:
    # Irreversible code optimization; keep helper-based versions in place.
    pass
