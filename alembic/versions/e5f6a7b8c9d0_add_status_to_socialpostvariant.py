"""SocialPostVariant: per-variant status (decouple approve from post-level)

Approval moves from SocialPost.status to SocialPostVariant.status so admins
can approve a single platform's copy (e.g. just twitter) without locking the
whole post into 'approved' for all platforms. SocialPost.status keeps
'archived' as the only meaningful state; the rest of its enum becomes a
soft aggregate view of the variants below it.

Backfill copies post.status into every existing variant. Variants under
archived posts inherit 'in_review' so they're not ambiguously "archived" —
the post-level archive still hides them from active queries.

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-04-29 00:00:00.000000
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "SocialPostVariant",
        sa.Column("status", sa.String(length=16), nullable=False, server_default="in_review"),
    )
    op.create_index(
        "ix_SocialPostVariant_status",
        "SocialPostVariant",
        ["status"],
    )
    # Backfill from owning post.status. Archived posts → in_review variants
    # (archive is post-level; variants don't carry a separate archived state).
    op.execute(
        """
        UPDATE SocialPostVariant v
        JOIN SocialPost p ON p.id = v.post_id
        SET v.status = CASE
            WHEN p.status = 'archived' THEN 'in_review'
            ELSE p.status
        END
        """
    )


def downgrade() -> None:
    op.drop_index("ix_SocialPostVariant_status", table_name="SocialPostVariant")
    op.drop_column("SocialPostVariant", "status")
