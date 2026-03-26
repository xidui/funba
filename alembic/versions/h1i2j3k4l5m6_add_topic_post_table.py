"""Add TopicPost table

Revision ID: h1i2j3k4l5m6
Revises: e8f1a2b3c4d5, g1h2i3j4k5l6
Create Date: 2026-03-26
"""
from alembic import op
import sqlalchemy as sa

revision = "h1i2j3k4l5m6"
down_revision = ("e8f1a2b3c4d5", "g1h2i3j4k5l6")
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "TopicPost",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("date", sa.DATE(), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="50"),
        sa.Column("status", sa.String(16), nullable=False, server_default="draft"),
        sa.Column("source_metric_keys", sa.Text(), nullable=True),
        sa.Column("source_game_ids", sa.Text(), nullable=True),
        sa.Column("source_entity_ids", sa.Text(), nullable=True),
        sa.Column("llm_model", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_TopicPost_date", "TopicPost", ["date"])
    op.create_index("ix_TopicPost_date_status", "TopicPost", ["date", "status"])


def downgrade():
    op.drop_index("ix_TopicPost_date_status", table_name="TopicPost")
    op.drop_index("ix_TopicPost_date", table_name="TopicPost")
    op.drop_table("TopicPost")
