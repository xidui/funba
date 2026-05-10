"""add twitter engagement conversation tables

Revision ID: h8i9j0k1l2m3
Revises: g7b8c9d0e1f2
Create Date: 2026-05-06 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "h8i9j0k1l2m3"
down_revision: Union[str, None] = "g7b8c9d0e1f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "TwitterEngagementConversation",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("x_conversation_id", sa.String(length=64), nullable=False),
        sa.Column("root_tweet_id", sa.String(length=64), nullable=True),
        sa.Column("root_url", sa.String(length=1024), nullable=True),
        sa.Column("target_author_id", sa.String(length=64), nullable=True),
        sa.Column("target_author_username", sa.String(length=64), nullable=True),
        sa.Column("target_author_name", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("last_seen_tweet_id", sa.String(length=64), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(), nullable=True),
        sa.Column("last_replied_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("x_conversation_id"),
    )
    op.create_index(
        "ix_TwitterEngagementConversation_last_seen_at",
        "TwitterEngagementConversation",
        ["last_seen_at"],
    )
    op.create_index(
        "ix_TwitterEngagementConversation_status",
        "TwitterEngagementConversation",
        ["status"],
    )
    op.create_index(
        "ix_TwitterEngagementConversation_x_conversation_id",
        "TwitterEngagementConversation",
        ["x_conversation_id"],
    )

    op.create_table(
        "TwitterEngagementMessage",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("conversation_id", sa.Integer(), nullable=False),
        sa.Column("tweet_id", sa.String(length=64), nullable=False),
        sa.Column("x_conversation_id", sa.String(length=64), nullable=False),
        sa.Column("parent_tweet_id", sa.String(length=64), nullable=True),
        sa.Column("direction", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("author_id", sa.String(length=64), nullable=True),
        sa.Column("author_username", sa.String(length=64), nullable=True),
        sa.Column("author_name", sa.String(length=255), nullable=True),
        sa.Column("author_verified", sa.Boolean(), nullable=False),
        sa.Column("author_followers_count", sa.Integer(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("tweet_url", sa.String(length=1024), nullable=False),
        sa.Column("posted_at", sa.DateTime(), nullable=True),
        sa.Column("discovered_at", sa.DateTime(), nullable=False),
        sa.Column("discovered_query", sa.Text(), nullable=True),
        sa.Column("public_metrics_json", sa.Text(), nullable=True),
        sa.Column("raw_payload_json", sa.Text(), nullable=True),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("score_reason", sa.Text(), nullable=True),
        sa.Column("matched_game_ids", sa.Text(), nullable=True),
        sa.Column("reply_post_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["conversation_id"],
            ["TwitterEngagementConversation.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["reply_post_id"], ["SocialPost.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tweet_id"),
    )
    op.create_index(
        "ix_TwitterEngagementMessage_conversation_id",
        "TwitterEngagementMessage",
        ["conversation_id"],
    )
    op.create_index(
        "ix_TwitterEngagementMessage_posted_at",
        "TwitterEngagementMessage",
        ["posted_at"],
    )
    op.create_index(
        "ix_TwitterEngagementMessage_reply_post_id",
        "TwitterEngagementMessage",
        ["reply_post_id"],
    )
    op.create_index(
        "ix_TwitterEngagementMessage_status",
        "TwitterEngagementMessage",
        ["status"],
    )
    op.create_index(
        "ix_TwitterEngagementMessage_tweet_id",
        "TwitterEngagementMessage",
        ["tweet_id"],
    )
    op.create_index(
        "ix_TwitterEngagementMessage_x_conversation_id",
        "TwitterEngagementMessage",
        ["x_conversation_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_TwitterEngagementMessage_x_conversation_id", table_name="TwitterEngagementMessage")
    op.drop_index("ix_TwitterEngagementMessage_tweet_id", table_name="TwitterEngagementMessage")
    op.drop_index("ix_TwitterEngagementMessage_status", table_name="TwitterEngagementMessage")
    op.drop_index("ix_TwitterEngagementMessage_reply_post_id", table_name="TwitterEngagementMessage")
    op.drop_index("ix_TwitterEngagementMessage_posted_at", table_name="TwitterEngagementMessage")
    op.drop_index("ix_TwitterEngagementMessage_conversation_id", table_name="TwitterEngagementMessage")
    op.drop_table("TwitterEngagementMessage")
    op.drop_index(
        "ix_TwitterEngagementConversation_x_conversation_id",
        table_name="TwitterEngagementConversation",
    )
    op.drop_index("ix_TwitterEngagementConversation_status", table_name="TwitterEngagementConversation")
    op.drop_index(
        "ix_TwitterEngagementConversation_last_seen_at",
        table_name="TwitterEngagementConversation",
    )
    op.drop_table("TwitterEngagementConversation")
