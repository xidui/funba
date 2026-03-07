"""add_ranking_index_to_metricresult

Revision ID: a7b8c9d0e1f2
Revises: 60d37a0e7db8
Create Date: 2026-03-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'a7b8c9d0e1f2'
down_revision: Union[str, None] = '60d37a0e7db8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add composite index for efficient rank-at-query-time window functions
    op.create_index(
        'ix_MetricResult_ranking',
        'MetricResult',
        ['metric_key', 'season', 'value_num'],
    )
    # Drop old noteworthiness index — no longer used for ranking
    op.drop_index('ix_MetricResult_notable', table_name='MetricResult')


def downgrade() -> None:
    op.create_index(
        'ix_MetricResult_notable',
        'MetricResult',
        ['noteworthiness', 'computed_at'],
    )
    op.drop_index('ix_MetricResult_ranking', table_name='MetricResult')
