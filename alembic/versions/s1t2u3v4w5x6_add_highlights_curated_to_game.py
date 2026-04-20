"""add curated highlight columns to game

Revision ID: s1t2u3v4w5x6
Revises: r0s1t2u3v4w5
Create Date: 2026-04-20 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "s1t2u3v4w5x6"
down_revision: Union[str, None] = "r0s1t2u3v4w5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("Game", sa.Column("highlights_curated_json", sa.Text(), nullable=True))
    op.add_column("Game", sa.Column("highlights_curated_at", sa.DateTime(), nullable=True))
    op.add_column("Game", sa.Column("highlights_curated_model", sa.String(64), nullable=True))


def downgrade() -> None:
    op.drop_column("Game", "highlights_curated_model")
    op.drop_column("Game", "highlights_curated_at")
    op.drop_column("Game", "highlights_curated_json")
