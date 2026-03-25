"""Drop MetricJobClaim table.

Completion detection migrated from MetricJobClaim polling to Celery chord.
Idempotency now via MetricRunLog existence check.

Revision ID: g1h2i3j4k5l6
Revises: (auto — append to current head)
"""
from alembic import op
import sqlalchemy as sa

revision = "g1h2i3j4k5l6"
down_revision = None  # Will be set by alembic when applied
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table("MetricJobClaim")


def downgrade():
    op.create_table(
        "MetricJobClaim",
        sa.Column("game_id", sa.String(20), primary_key=True),
        sa.Column("metric_key", sa.String(64), primary_key=True),
        sa.Column("claimed_at", sa.DateTime, nullable=False),
        sa.Column("worker_id", sa.String(255), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="in_progress"),
    )
    op.create_index(
        "ix_MetricJobClaim_metric_status_game",
        "MetricJobClaim",
        ["metric_key", "status", "game_id"],
    )
