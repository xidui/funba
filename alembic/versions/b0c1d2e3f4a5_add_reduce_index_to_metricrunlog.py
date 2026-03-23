"""add reduce index to MetricRunLog

Revision ID: b0c1d2e3f4a5
Revises: a9b0c1d2e3f4
Create Date: 2026-03-22 20:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "b0c1d2e3f4a5"
down_revision: Union[str, Sequence[str], None] = "a9b0c1d2e3f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_MetricRunLog_reduce", "MetricRunLog",
        ["metric_key", "season", "entity_type", "entity_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_MetricRunLog_reduce", "MetricRunLog")
