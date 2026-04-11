"""Add game_status column to Game

Revision ID: w6x7y8z9a0b
Revises: v5w6x7y8z9a0
Create Date: 2026-04-10
"""

from alembic import op
import sqlalchemy as sa


revision = "w6x7y8z9a0b"
down_revision = "v5w6x7y8z9a0"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("Game", sa.Column("game_status", sa.String(length=16), nullable=True))


def downgrade():
    op.drop_column("Game", "game_status")
