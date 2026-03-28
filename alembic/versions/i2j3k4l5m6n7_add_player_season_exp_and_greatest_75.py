"""Add player season experience and 75 greatest flag.

Revision ID: i2j3k4l5m6n7
Revises: h1i2j3k4l5m6
Create Date: 2026-03-27
"""
from alembic import op
import sqlalchemy as sa

revision = "i2j3k4l5m6n7"
down_revision = "h1i2j3k4l5m6"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("Player", sa.Column("season_exp", sa.Integer(), nullable=True))
    op.add_column("Player", sa.Column("greatest_75_flag", sa.Boolean(), nullable=True))


def downgrade():
    op.drop_column("Player", "greatest_75_flag")
    op.drop_column("Player", "season_exp")
