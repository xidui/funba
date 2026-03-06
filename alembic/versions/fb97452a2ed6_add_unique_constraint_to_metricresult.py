"""add unique constraint to MetricResult

Revision ID: fb97452a2ed6
Revises: 381fd87c5f06
Create Date: 2026-03-04 21:25:46.364602

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'fb97452a2ed6'
down_revision: Union[str, None] = '381fd87c5f06'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        'uq_MetricResult_key_entity_season',
        'MetricResult',
        ['metric_key', 'entity_type', 'entity_id', 'season'],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index('uq_MetricResult_key_entity_season', table_name='MetricResult')
