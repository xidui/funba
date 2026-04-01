"""add social post image table

Revision ID: q0r1s2t3u4v5
Revises: p9q0r1s2t3u4
Create Date: 2026-04-01 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "q0r1s2t3u4v5"
down_revision: Union[str, None] = "p9q0r1s2t3u4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "SocialPostImage",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("post_id", sa.Integer(), nullable=False),
        sa.Column("slot", sa.String(32), nullable=False),
        sa.Column("image_type", sa.String(16), nullable=False),
        sa.Column("spec", sa.Text(), nullable=True),
        sa.Column("note", sa.String(255), nullable=True),
        sa.Column("file_path", sa.String(512), nullable=True),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["post_id"], ["SocialPost.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_SocialPostImage_post_id", "SocialPostImage", ["post_id"])


def downgrade() -> None:
    op.drop_index("ix_SocialPostImage_post_id", table_name="SocialPostImage")
    op.drop_table("SocialPostImage")
