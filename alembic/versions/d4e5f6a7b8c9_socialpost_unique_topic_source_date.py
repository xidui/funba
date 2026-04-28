"""SocialPost: UNIQUE(topic, source_date) for active rows — defense-in-depth

Even after row-level locks at the curator entry, having a DB-level uniqueness
constraint guarantees that a race that slips through the application layer
(e.g. someone calls the inner function from a new code path that bypasses the
lock) ends in IntegrityError instead of a silently-duplicated SocialPost row.

Implementation: MySQL doesn't support partial unique indexes ("UNIQUE WHERE
status != 'archived'"), so we synthesise one via a STORED generated column
that is NULL for archived rows. NULL values don't conflict in a MySQL UNIQUE
index, so archived rows pile up freely while active rows enforce uniqueness.

`scripts/archive_duplicate_hero_posts.py` must run BEFORE this migration —
otherwise the index creation fails with a "Duplicate entry" error against
the active duplicates already in the table.

Revision ID: d4e5f6a7b8c9
Revises: c7d8e9f0a1b3
Create Date: 2026-04-28 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4e5f6a7b8c9"
down_revision: Union[str, None] = "c7d8e9f0a1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # NULL for archived rows (don't enforce uniqueness on terminated rows),
    # CONCAT(topic, source_date) for everything else.
    op.execute(
        """
        ALTER TABLE SocialPost
        ADD COLUMN dedup_key VARCHAR(300) GENERATED ALWAYS AS (
          CASE WHEN status = 'archived' THEN NULL
               ELSE CONCAT(topic, '||', source_date)
          END
        ) STORED
        """
    )
    op.create_index(
        "uq_SocialPost_active_dedup_key",
        "SocialPost",
        ["dedup_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_SocialPost_active_dedup_key", table_name="SocialPost")
    op.execute("ALTER TABLE SocialPost DROP COLUMN dedup_key")
