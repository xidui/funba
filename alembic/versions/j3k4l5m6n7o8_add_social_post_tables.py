"""Add SocialPost, SocialPostVariant, SocialPostDelivery tables

Revision ID: j3k4l5m6n7o8
Revises: i2j3k4l5m6n7
Create Date: 2026-03-28
"""
from alembic import op
import sqlalchemy as sa

revision = "j3k4l5m6n7o8"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "SocialPost",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("topic", sa.String(255), nullable=False),
        sa.Column("source_date", sa.DATE(), nullable=False),
        sa.Column("source_metrics", sa.Text(), nullable=True),
        sa.Column("source_game_ids", sa.Text(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="draft"),
        sa.Column("admin_comments", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("llm_model", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_SocialPost_source_date", "SocialPost", ["source_date"])
    op.create_index("ix_SocialPost_status", "SocialPost", ["status"])
    op.create_index("ix_SocialPost_source_date_status", "SocialPost", ["source_date", "status"])

    op.create_table(
        "SocialPostVariant",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("post_id", sa.Integer(), sa.ForeignKey("SocialPost.id", ondelete="CASCADE"), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("content_raw", sa.Text(), nullable=False),
        sa.Column("audience_hint", sa.String(128), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_SocialPostVariant_post_id", "SocialPostVariant", ["post_id"])

    op.create_table(
        "SocialPostDelivery",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("variant_id", sa.Integer(), sa.ForeignKey("SocialPostVariant.id", ondelete="CASCADE"), nullable=False),
        sa.Column("platform", sa.String(32), nullable=False),
        sa.Column("forum", sa.String(64), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="pending"),
        sa.Column("content_final", sa.Text(), nullable=True),
        sa.Column("published_url", sa.String(1024), nullable=True),
        sa.Column("published_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_SocialPostDelivery_variant_id", "SocialPostDelivery", ["variant_id"])
    op.create_index("ix_SocialPostDelivery_status", "SocialPostDelivery", ["status"])


def downgrade():
    op.drop_index("ix_SocialPostDelivery_status", table_name="SocialPostDelivery")
    op.drop_index("ix_SocialPostDelivery_variant_id", table_name="SocialPostDelivery")
    op.drop_table("SocialPostDelivery")
    op.drop_index("ix_SocialPostVariant_post_id", table_name="SocialPostVariant")
    op.drop_table("SocialPostVariant")
    op.drop_index("ix_SocialPost_source_date_status", table_name="SocialPost")
    op.drop_index("ix_SocialPost_status", table_name="SocialPost")
    op.drop_index("ix_SocialPost_source_date", table_name="SocialPost")
    op.drop_table("SocialPost")
