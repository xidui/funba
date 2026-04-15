"""add team roster and coach stint tables

Revision ID: r0s1t2u3v4w5
Revises: n1e2w3s4a5b6
Create Date: 2026-04-14

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "r0s1t2u3v4w5"
down_revision: Union[str, None] = "n1e2w3s4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "TeamRosterStint",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("team_id", sa.String(50), sa.ForeignKey("Team.team_id"), nullable=False),
        sa.Column("player_id", sa.String(50), sa.ForeignKey("Player.player_id"), nullable=False),
        sa.Column("joined_at", sa.Date, nullable=False),
        sa.Column("left_at", sa.Date, nullable=True),
        sa.Column("jersey", sa.String(10), nullable=True),
        sa.Column("position", sa.String(30), nullable=True),
        sa.Column("how_acquired", sa.String(255), nullable=True),
        sa.Column("source", sa.String(32), nullable=False, server_default="game_derived"),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_team_roster_stint_team_open", "TeamRosterStint", ["team_id", "left_at"])
    op.create_index("ix_team_roster_stint_player", "TeamRosterStint", ["player_id", "joined_at"])
    op.create_index("ix_team_roster_stint_joined", "TeamRosterStint", ["joined_at"])

    op.create_table(
        "TeamCoachStint",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("team_id", sa.String(50), sa.ForeignKey("Team.team_id"), nullable=False),
        sa.Column("coach_id", sa.String(50), nullable=False),
        sa.Column("coach_name", sa.String(255), nullable=False),
        sa.Column("coach_type", sa.String(64), nullable=True),
        sa.Column("is_assistant", sa.Boolean, nullable=False, server_default=sa.text("0")),
        sa.Column("joined_at", sa.Date, nullable=False),
        sa.Column("left_at", sa.Date, nullable=True),
        sa.Column("source", sa.String(32), nullable=False, server_default="roster_snapshot"),
        sa.Column("created_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_team_coach_stint_team_open", "TeamCoachStint", ["team_id", "left_at"])
    op.create_index("ix_team_coach_stint_coach", "TeamCoachStint", ["coach_id", "joined_at"])


def downgrade() -> None:
    op.drop_index("ix_team_coach_stint_coach", "TeamCoachStint")
    op.drop_index("ix_team_coach_stint_team_open", "TeamCoachStint")
    op.drop_table("TeamCoachStint")
    op.drop_index("ix_team_roster_stint_joined", "TeamRosterStint")
    op.drop_index("ix_team_roster_stint_player", "TeamRosterStint")
    op.drop_index("ix_team_roster_stint_team_open", "TeamRosterStint")
    op.drop_table("TeamRosterStint")
