"""deduplicate awards and add uniqueness guards

Revision ID: 8f6b0f68a4a1
Revises: c4f7e2a1b9d0
Create Date: 2026-03-22 10:55:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "8f6b0f68a4a1"
down_revision: Union[str, Sequence[str], None] = "c4f7e2a1b9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        DELETE dup
        FROM Award AS dup
        INNER JOIN Award AS keep
            ON dup.award_type = keep.award_type
            AND dup.season = keep.season
            AND dup.player_id = keep.player_id
            AND dup.id > keep.id
        WHERE dup.player_id IS NOT NULL
          AND keep.player_id IS NOT NULL
        """
    )
    op.execute(
        """
        DELETE dup
        FROM Award AS dup
        INNER JOIN Award AS keep
            ON dup.award_type = keep.award_type
            AND dup.season = keep.season
            AND dup.team_id = keep.team_id
            AND dup.id > keep.id
        WHERE dup.player_id IS NULL
          AND keep.player_id IS NULL
          AND dup.team_id IS NOT NULL
          AND keep.team_id IS NOT NULL
        """
    )
    op.execute(
        """
        ALTER TABLE Award
        ADD COLUMN entity_key VARCHAR(64)
            GENERATED ALWAYS AS (
                CASE
                    WHEN player_id IS NOT NULL THEN CONCAT('P:', player_id)
                    WHEN team_id IS NOT NULL THEN CONCAT('T:', team_id)
                    ELSE NULL
                END
            ) STORED
        """
    )
    op.create_unique_constraint(
        "uq_Award_type_season_entity",
        "Award",
        ["award_type", "season", "entity_key"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_Award_type_season_entity", "Award", type_="unique")
    op.drop_column("Award", "entity_key")
