"""Add season index to MetricRunLog

Revision ID: ec0b9771e634
Revises: i2j3k4l5m6n7
Create Date: 2026-03-28 01:49:22.821676

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'ec0b9771e634'
down_revision: Union[str, None] = 'i2j3k4l5m6n7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index('ix_MetricRunLog_season', 'MetricRunLog', ['season'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_MetricRunLog_season', table_name='MetricRunLog')
