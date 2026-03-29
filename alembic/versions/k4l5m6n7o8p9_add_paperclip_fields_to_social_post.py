"""Add Paperclip workflow fields to SocialPost

Revision ID: k4l5m6n7o8p9
Revises: j3k4l5m6n7o8
Create Date: 2026-03-28 18:45:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "k4l5m6n7o8p9"
down_revision = "j3k4l5m6n7o8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("SocialPost", sa.Column("paperclip_issue_id", sa.String(length=64), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_issue_identifier", sa.String(length=64), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_issue_status", sa.String(length=16), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_assignee_agent_id", sa.String(length=64), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_assignee_user_id", sa.String(length=64), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_last_comment_id", sa.String(length=64), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_last_synced_at", sa.DateTime(), nullable=True))
    op.add_column("SocialPost", sa.Column("paperclip_sync_error", sa.Text(), nullable=True))
    op.create_index("ix_SocialPost_paperclip_issue_id", "SocialPost", ["paperclip_issue_id"])


def downgrade():
    op.drop_index("ix_SocialPost_paperclip_issue_id", table_name="SocialPost")
    op.drop_column("SocialPost", "paperclip_sync_error")
    op.drop_column("SocialPost", "paperclip_last_synced_at")
    op.drop_column("SocialPost", "paperclip_last_comment_id")
    op.drop_column("SocialPost", "paperclip_assignee_user_id")
    op.drop_column("SocialPost", "paperclip_assignee_agent_id")
    op.drop_column("SocialPost", "paperclip_issue_status")
    op.drop_column("SocialPost", "paperclip_issue_identifier")
    op.drop_column("SocialPost", "paperclip_issue_id")
