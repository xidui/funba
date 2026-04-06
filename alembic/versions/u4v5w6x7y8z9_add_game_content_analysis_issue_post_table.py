"""add game content analysis issue post table

Revision ID: u4v5w6x7y8z9
Revises: t3u4v5w6x7y8
Create Date: 2026-04-06 00:18:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "u4v5w6x7y8z9"
down_revision = "t3u4v5w6x7y8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "GameContentAnalysisIssuePost",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("issue_record_id", sa.Integer(), nullable=False),
        sa.Column("post_id", sa.Integer(), nullable=False),
        sa.Column("discovered_via", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["issue_record_id"], ["GameContentAnalysisIssue.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["post_id"], ["SocialPost.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("issue_record_id", "post_id", name="uq_GameContentAnalysisIssuePost_issue_post"),
    )
    op.create_index(
        "ix_GameContentAnalysisIssuePost_issue_record_id",
        "GameContentAnalysisIssuePost",
        ["issue_record_id"],
    )
    op.create_index(
        "ix_GameContentAnalysisIssuePost_post_id",
        "GameContentAnalysisIssuePost",
        ["post_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_GameContentAnalysisIssuePost_post_id", table_name="GameContentAnalysisIssuePost")
    op.drop_index("ix_GameContentAnalysisIssuePost_issue_record_id", table_name="GameContentAnalysisIssuePost")
    op.drop_table("GameContentAnalysisIssuePost")
