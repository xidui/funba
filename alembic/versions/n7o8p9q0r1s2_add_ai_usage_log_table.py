"""add AiUsageLog table

Revision ID: n7o8p9q0r1s2
Revises: m6n7o8p9q0r1
Create Date: 2026-03-30 14:20:00.000000
"""
from alembic import op
import sqlalchemy as sa


revision = "n7o8p9q0r1s2"
down_revision = "m6n7o8p9q0r1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "AiUsageLog",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("user_id", sa.String(length=36), nullable=True),
        sa.Column("visitor_id", sa.String(length=36), nullable=True),
        sa.Column("feature", sa.String(length=32), nullable=False),
        sa.Column("operation", sa.String(length=32), nullable=False),
        sa.Column("endpoint", sa.String(length=128), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("model", sa.String(length=64), nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), nullable=True),
        sa.Column("completion_tokens", sa.Integer(), nullable=True),
        sa.Column("total_tokens", sa.Integer(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("success", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("conversation_id", sa.String(length=36), nullable=True),
        sa.Column("metadata_json", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["User.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_AiUsageLog_created_at", "AiUsageLog", ["created_at"], unique=False)
    op.create_index("ix_AiUsageLog_user_id", "AiUsageLog", ["user_id"], unique=False)
    op.create_index("ix_AiUsageLog_visitor_id", "AiUsageLog", ["visitor_id"], unique=False)
    op.create_index("ix_AiUsageLog_conversation_id", "AiUsageLog", ["conversation_id"], unique=False)
    op.create_index(
        "ix_AiUsageLog_feature_created_at",
        "AiUsageLog",
        ["feature", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_AiUsageLog_user_created_at",
        "AiUsageLog",
        ["user_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_AiUsageLog_visitor_created_at",
        "AiUsageLog",
        ["visitor_id", "created_at"],
        unique=False,
    )
    op.create_index(
        "ix_AiUsageLog_conversation_created_at",
        "AiUsageLog",
        ["conversation_id", "created_at"],
        unique=False,
    )


def downgrade():
    op.drop_index("ix_AiUsageLog_conversation_created_at", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_visitor_created_at", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_user_created_at", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_feature_created_at", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_conversation_id", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_visitor_id", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_user_id", table_name="AiUsageLog")
    op.drop_index("ix_AiUsageLog_created_at", table_name="AiUsageLog")
    op.drop_table("AiUsageLog")
