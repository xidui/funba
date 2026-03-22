"""split career seasons by type and add qualified to MetricRunLog

Revision ID: a9b0c1d2e3f4
Revises: 8f6b0f68a4a1
Create Date: 2026-03-22 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "a9b0c1d2e3f4"
down_revision: Union[str, Sequence[str], None] = "8f6b0f68a4a1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Widen season columns to fit "all_playoffs" (12 chars)
    op.alter_column(
        "MetricResult", "season",
        type_=sa.String(16), existing_type=sa.String(10), existing_nullable=True,
    )
    op.alter_column(
        "MetricRunLog", "season",
        type_=sa.String(16), existing_type=sa.String(10), existing_nullable=False,
    )

    # 2. Add qualified column to MetricRunLog
    op.add_column("MetricRunLog", sa.Column("qualified", sa.Boolean(), nullable=True))
    op.create_index(
        "ix_MetricRunLog_qualifying", "MetricRunLog",
        ["metric_key", "entity_id", "qualified"],
    )

    # 3. Delete old career data — will be rebuilt via backfill
    op.execute("DELETE FROM MetricResult WHERE season = 'all'")
    op.execute("DELETE FROM MetricRunLog WHERE season = 'all'")
    op.execute("DELETE FROM MetricJobClaim WHERE metric_key LIKE '%\\_career' ESCAPE '\\\\'")


def downgrade() -> None:
    # Remove split career data
    op.execute("DELETE FROM MetricResult WHERE season LIKE 'all\\_%' ESCAPE '\\\\'")
    op.execute("DELETE FROM MetricRunLog WHERE season LIKE 'all\\_%' ESCAPE '\\\\'")

    op.drop_index("ix_MetricRunLog_qualifying", "MetricRunLog")
    op.drop_column("MetricRunLog", "qualified")

    op.alter_column(
        "MetricRunLog", "season",
        type_=sa.String(10), existing_type=sa.String(16), existing_nullable=False,
    )
    op.alter_column(
        "MetricResult", "season",
        type_=sa.String(10), existing_type=sa.String(16), existing_nullable=True,
    )
