"""Add is_enabled to SocialPostDelivery

Revision ID: l5m6n7o8p9q0
Revises: k4l5m6n7o8p9
Create Date: 2026-03-29 15:40:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "l5m6n7o8p9q0"
down_revision = "k4l5m6n7o8p9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "SocialPostDelivery",
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
    )


def downgrade():
    op.drop_column("SocialPostDelivery", "is_enabled")
