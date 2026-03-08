"""drop PlayerSeasonMetrics table

Revision ID: e2ed9864c7fa
Revises: e2f3a4b5c6d7
Create Date: 2026-03-07 16:57:42.828599

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e2ed9864c7fa'
down_revision: Union[str, None] = 'e2f3a4b5c6d7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table('PlayerSeasonMetrics')


def downgrade() -> None:
    op.create_table(
        'PlayerSeasonMetrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('player_id', sa.String(50), sa.ForeignKey('Player.player_id'), nullable=True),
        sa.Column('team_id', sa.String(50), sa.ForeignKey('Team.team_id'), nullable=True),
        sa.Column('season', sa.String(50), nullable=True),
        sa.Column('three_pointer_made', sa.Float(), nullable=True),
        sa.Column('three_pointer_attempt', sa.Float(), nullable=True),
        sa.Column('three_pointer_made_after_one_miss', sa.Float(), nullable=True),
        sa.Column('three_pointer_attempt_after_one_miss', sa.Float(), nullable=True),
        sa.Column('three_pointer_made_after_two_miss', sa.Float(), nullable=True),
        sa.Column('three_pointer_attempt_after_two_miss', sa.Float(), nullable=True),
        sa.Column('shot_made', sa.Float(), nullable=True),
        sa.Column('shot_attempt', sa.Float(), nullable=True),
        sa.Column('shot_made_after_made', sa.Float(), nullable=True),
        sa.Column('shot_attempt_after_made', sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'idx_PlayerSeasonMetrics_player_team_season',
        'PlayerSeasonMetrics',
        ['player_id', 'team_id', 'season'],
        unique=True,
    )
