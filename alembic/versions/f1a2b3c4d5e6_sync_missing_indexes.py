"""Sync missing indexes between models.py and DB

Adds ShotRecord indexes declared in models.py but never created in DB.
Renames misnamed GamePlayByPlay index to match models.py declaration.

Revision ID: f1a2b3c4d5e6
Revises: ec0b9771e634
Create Date: 2026-03-28 02:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, None] = 'ec0b9771e634'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ShotRecord: 4 indexes declared in models.py but missing from DB
    op.create_index('ix_ShotRecord_season', 'ShotRecord', ['season'], unique=False)
    op.create_index('ix_ShotRecord_player_id_season', 'ShotRecord', ['player_id', 'season'], unique=False)
    op.create_index('ix_ShotRecord_player_id_season_team_id', 'ShotRecord', ['player_id', 'season', 'team_id'], unique=False)
    op.create_index('ix_ShotRecord_season_zone', 'ShotRecord', ['season', 'shot_zone_area'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_ShotRecord_season_zone', table_name='ShotRecord')
    op.drop_index('ix_ShotRecord_player_id_season_team_id', table_name='ShotRecord')
    op.drop_index('ix_ShotRecord_player_id_season', table_name='ShotRecord')
    op.drop_index('ix_ShotRecord_season', table_name='ShotRecord')
