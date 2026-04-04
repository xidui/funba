"""add review fields to social post image

Revision ID: s2t3u4v5w6x7
Revises: r1s2t3u4v5w6
Create Date: 2026-04-04 01:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "s2t3u4v5w6x7"
down_revision: Union[str, None] = "r1s2t3u4v5w6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("SocialPostImage", sa.Column("review_decision", sa.String(length=16), nullable=True))
    op.add_column("SocialPostImage", sa.Column("review_reason", sa.Text(), nullable=True))
    op.add_column("SocialPostImage", sa.Column("review_source", sa.String(length=64), nullable=True))
    op.add_column("SocialPostImage", sa.Column("reviewed_at", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("SocialPostImage", "reviewed_at")
    op.drop_column("SocialPostImage", "review_source")
    op.drop_column("SocialPostImage", "review_reason")
    op.drop_column("SocialPostImage", "review_decision")
