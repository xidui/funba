"""add MetricPerfLog table

Revision ID: m6n7o8p9q0r1
Revises: l5m6n7o8p9q0
Create Date: 2026-03-30 00:35:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "m6n7o8p9q0r1"
down_revision = "l5m6n7o8p9q0"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "MetricPerfLog",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("metric_key", sa.String(length=64), nullable=False),
        sa.Column("recorded_at", sa.DateTime(), nullable=False),
        sa.Column("duration_ms", sa.Integer(), nullable=False),
        sa.Column("db_reads", sa.Integer(), nullable=True),
        sa.Column("db_writes", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_MetricPerfLog_metric_key_recorded_at",
        "MetricPerfLog",
        ["metric_key", "recorded_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_MetricPerfLog_metric_key_recorded_at", table_name="MetricPerfLog")
    op.drop_table("MetricPerfLog")
