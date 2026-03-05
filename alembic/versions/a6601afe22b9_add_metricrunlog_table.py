"""add MetricRunLog table

Revision ID: a6601afe22b9
Revises: a1b2c3d4e5f6
Create Date: 2026-03-04 17:21:15.106698

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a6601afe22b9'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'MetricRunLog',
        sa.Column('game_id', sa.String(length=20), nullable=False),
        sa.Column('computed_at', sa.DateTime(), nullable=False),
        sa.Column('result_count', sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint('game_id'),
    )


def downgrade() -> None:
    op.drop_table('MetricRunLog')
