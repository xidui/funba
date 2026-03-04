"""add MetricResult table

Revision ID: a1b2c3d4e5f6
Revises: 7d88e8bf8922
Create Date: 2026-03-04 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = '7d88e8bf8922'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'MetricResult',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('metric_key', sa.String(64), nullable=False),
        sa.Column('entity_type', sa.String(16), nullable=False),
        sa.Column('entity_id', sa.String(50), nullable=True),
        sa.Column('season', sa.String(10), nullable=True),
        sa.Column('game_id', sa.String(20), nullable=True),
        sa.Column('value_num', sa.Float(), nullable=True),
        sa.Column('value_str', sa.String(255), nullable=True),
        sa.Column('context_json', sa.Text(), nullable=True),
        sa.Column('noteworthiness', sa.Float(), nullable=True),
        sa.Column('notable_reason', sa.Text(), nullable=True),
        sa.Column('computed_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_MetricResult_key', 'MetricResult', ['metric_key'])
    op.create_index('ix_MetricResult_entity', 'MetricResult', ['entity_type', 'entity_id', 'season'])
    op.create_index('ix_MetricResult_notable', 'MetricResult', ['noteworthiness', 'computed_at'])


def downgrade() -> None:
    op.drop_table('MetricResult')
