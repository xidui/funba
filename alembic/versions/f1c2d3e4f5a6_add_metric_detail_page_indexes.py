"""add_metric_detail_page_indexes

Revision ID: f1c2d3e4f5a6
Revises: e2ed9864c7fa
Create Date: 2026-03-08

Add indexes that match the metric detail page backfill-status queries:
- MetricJobClaim(metric_key, status, game_id)
- MetricRunLog(metric_key, computed_at)
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'f1c2d3e4f5a6'
down_revision: Union[str, None] = 'e2ed9864c7fa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'ix_MetricJobClaim_metric_status_game',
        'MetricJobClaim',
        ['metric_key', 'status', 'game_id'],
        unique=False,
    )
    op.create_index(
        'ix_MetricRunLog_metric_key_computed_at',
        'MetricRunLog',
        ['metric_key', 'computed_at'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_MetricRunLog_metric_key_computed_at', table_name='MetricRunLog')
    op.drop_index('ix_MetricJobClaim_metric_status_game', table_name='MetricJobClaim')
