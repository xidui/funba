"""add award table

Revision ID: c4f7e2a1b9d0
Revises: 30b6729a23b7, 7a4f0d5b61c2
Create Date: 2026-03-22 03:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c4f7e2a1b9d0"
down_revision: Union[str, Sequence[str], None] = ("30b6729a23b7", "7a4f0d5b61c2")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "Award",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("award_type", sa.String(length=50), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("player_id", sa.String(length=50), nullable=True),
        sa.Column("team_id", sa.String(length=50), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["player_id"], ["Player.player_id"]),
        sa.ForeignKeyConstraint(["team_id"], ["Team.team_id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_Award_type_season", "Award", ["award_type", "season"], unique=False)
    op.create_index("ix_Award_player_type", "Award", ["player_id", "award_type"], unique=False)
    op.create_index("ix_Award_team_type", "Award", ["team_id", "award_type"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_Award_team_type", table_name="Award")
    op.drop_index("ix_Award_player_type", table_name="Award")
    op.drop_index("ix_Award_type_season", table_name="Award")
    op.drop_table("Award")
