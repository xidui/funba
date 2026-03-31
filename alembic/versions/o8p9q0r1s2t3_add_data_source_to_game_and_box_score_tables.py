"""add data source to game and box score tables

Revision ID: o8p9q0r1s2t3
Revises: n7o8p9q0r1s2
Create Date: 2026-03-31 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'o8p9q0r1s2t3'
down_revision: Union[str, None] = 'n7o8p9q0r1s2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_DEFAULT_SOURCE = "nba_api_box_scores"


def upgrade() -> None:
    op.add_column(
        'Game',
        sa.Column('data_source', sa.String(length=64), nullable=False, server_default=_DEFAULT_SOURCE),
    )
    op.add_column(
        'TeamGameStats',
        sa.Column('data_source', sa.String(length=64), nullable=False, server_default=_DEFAULT_SOURCE),
    )
    op.add_column(
        'PlayerGameStats',
        sa.Column('data_source', sa.String(length=64), nullable=False, server_default=_DEFAULT_SOURCE),
    )


def downgrade() -> None:
    op.drop_column('PlayerGameStats', 'data_source')
    op.drop_column('TeamGameStats', 'data_source')
    op.drop_column('Game', 'data_source')
