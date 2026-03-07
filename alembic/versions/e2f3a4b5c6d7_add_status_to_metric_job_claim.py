"""add_status_to_metric_job_claim

Revision ID: e2f3a4b5c6d7
Revises: d1e2f3a4b5c6
Create Date: 2026-03-07

Add a status column ('in_progress' | 'done') to MetricJobClaim so that:
- Transient failures can delete the claim row and allow retry to reclaim.
- Worker crashes leave a 'in_progress' row that can be detected and cleaned up.
- Successfully completed jobs are permanently marked 'done', blocking re-runs
  unless explicitly cleared via --force.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'e2f3a4b5c6d7'
down_revision: Union[str, None] = 'd1e2f3a4b5c6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "MetricJobClaim",
        sa.Column(
            "status",
            sa.String(16),
            nullable=False,
            server_default="done",   # existing rows (from before this migration) are treated as done
        ),
    )


def downgrade() -> None:
    op.drop_column("MetricJobClaim", "status")
