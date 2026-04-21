"""add player + team curated highlight columns to game

Revision ID: t2u3v4w5x6y7
Revises: s1t2u3v4w5x6
Create Date: 2026-04-20 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "t2u3v4w5x6y7"
down_revision: Union[str, None] = "s1t2u3v4w5x6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("Game", sa.Column("highlights_curated_player_json", sa.Text(), nullable=True))
    op.add_column("Game", sa.Column("highlights_curated_team_json", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("Game", "highlights_curated_team_json")
    op.drop_column("Game", "highlights_curated_player_json")
