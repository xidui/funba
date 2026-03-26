"""add player salary table

Revision ID: e8f1a2b3c4d5
Revises: 1f2e3d4c5b6a, b4c5d6e7f8a9
Create Date: 2026-03-25 21:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e8f1a2b3c4d5"
down_revision: Union[str, Sequence[str], None] = ("1f2e3d4c5b6a", "b4c5d6e7f8a9")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "PlayerSalary",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("player_id", sa.String(length=50), nullable=False),
        sa.Column("season", sa.Integer(), nullable=False),
        sa.Column("salary_usd", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["player_id"], ["Player.player_id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("player_id", "season", name="uq_PlayerSalary_player_season"),
    )
    op.create_index("ix_PlayerSalary_player_id", "PlayerSalary", ["player_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_PlayerSalary_player_id", table_name="PlayerSalary")
    op.drop_table("PlayerSalary")
