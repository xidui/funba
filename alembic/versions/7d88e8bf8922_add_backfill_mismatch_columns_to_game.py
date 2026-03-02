"""add backfill mismatch columns to game

Revision ID: 7d88e8bf8922
Revises: ee300034a781
Create Date: 2026-03-02 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7d88e8bf8922'
down_revision: Union[str, None] = 'ee300034a781'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'Game',
        sa.Column('backfill_mismatch', sa.Boolean(), nullable=False, server_default=sa.text('0')),
    )
    op.add_column('Game', sa.Column('backfill_mismatch_note', sa.Text(), nullable=True))
    op.add_column('Game', sa.Column('backfill_mismatch_updated_at', sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column('Game', 'backfill_mismatch_updated_at')
    op.drop_column('Game', 'backfill_mismatch_note')
    op.drop_column('Game', 'backfill_mismatch')
