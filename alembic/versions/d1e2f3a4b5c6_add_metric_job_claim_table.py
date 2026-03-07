"""add_metric_job_claim_table

Revision ID: d1e2f3a4b5c6
Revises: 60d37a0e7db8
Create Date: 2026-03-07

MetricJobClaim: atomic claim table for Celery metric tasks.
One row per (game_id, metric_key). Workers INSERT IGNORE to claim;
rowcount=0 means another worker already owns it — skip without recomputing.
This prevents concurrent duplicate tasks from double-applying incremental deltas.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'd1e2f3a4b5c6'
down_revision: Union[str, None] = 'a7b8c9d0e1f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "MetricJobClaim",
        sa.Column("game_id",    sa.String(20),  nullable=False),
        sa.Column("metric_key", sa.String(64),  nullable=False),
        sa.Column("claimed_at", sa.DateTime(),  nullable=False),
        sa.Column("worker_id",  sa.String(255), nullable=True),
        sa.PrimaryKeyConstraint("game_id", "metric_key"),
    )


def downgrade() -> None:
    op.drop_table("MetricJobClaim")
