"""add MetricComputeRun table

Revision ID: f9a0b1c2d3e4
Revises: b0c1d2e3f4a5
Create Date: 2026-03-22 20:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f9a0b1c2d3e4"
down_revision: Union[str, Sequence[str], None] = "b0c1d2e3f4a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "MetricComputeRun",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("metric_key", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("target_season", sa.String(length=16), nullable=True),
        sa.Column("target_date_from", sa.Date(), nullable=True),
        sa.Column("target_date_to", sa.Date(), nullable=True),
        sa.Column("target_game_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("reduce_enqueued_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("failed_at", sa.DateTime(), nullable=True),
        sa.Column("error_text", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_MetricComputeRun_metric_status",
        "MetricComputeRun",
        ["metric_key", "status"],
        unique=False,
    )
    op.create_index(
        "ix_MetricComputeRun_status_created",
        "MetricComputeRun",
        ["status", "created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_MetricComputeRun_status_created", table_name="MetricComputeRun")
    op.drop_index("ix_MetricComputeRun_metric_status", table_name="MetricComputeRun")
    op.drop_table("MetricComputeRun")
