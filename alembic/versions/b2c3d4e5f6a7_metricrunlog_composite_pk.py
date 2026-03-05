"""MetricRunLog composite PK (game_id, metric_key)

Revision ID: b2c3d4e5f6a7
Revises: a6601afe22b9
Create Date: 2026-03-04

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a6601afe22b9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table('MetricRunLog')
    op.create_table(
        'MetricRunLog',
        sa.Column('game_id', sa.String(20), nullable=False),
        sa.Column('metric_key', sa.String(64), nullable=False),
        sa.Column('computed_at', sa.DateTime(), nullable=False),
        sa.Column('produced_result', sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint('game_id', 'metric_key'),
    )


def downgrade() -> None:
    op.drop_table('MetricRunLog')
    op.create_table(
        'MetricRunLog',
        sa.Column('game_id', sa.String(20), nullable=False),
        sa.Column('computed_at', sa.DateTime(), nullable=False),
        sa.Column('result_count', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('game_id'),
    )
