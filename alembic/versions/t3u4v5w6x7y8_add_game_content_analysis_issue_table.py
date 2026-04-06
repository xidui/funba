"""add game content analysis issue table

Revision ID: t3u4v5w6x7y8
Revises: s2t3u4v5w6x7
Create Date: 2026-04-05 22:15:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "t3u4v5w6x7y8"
down_revision = "s2t3u4v5w6x7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "GameContentAnalysisIssue",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("game_id", sa.String(length=50), nullable=False),
        sa.Column("source_date", sa.DATE(), nullable=False),
        sa.Column("paperclip_issue_id", sa.String(length=64), nullable=False),
        sa.Column("paperclip_issue_identifier", sa.String(length=64), nullable=True),
        sa.Column("paperclip_issue_status", sa.String(length=16), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("trigger_source", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["game_id"], ["Game.game_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("paperclip_issue_id", name="uq_GameContentAnalysisIssue_paperclip_issue_id"),
    )
    op.create_index("ix_GameContentAnalysisIssue_game_id", "GameContentAnalysisIssue", ["game_id"])
    op.create_index("ix_GameContentAnalysisIssue_source_date", "GameContentAnalysisIssue", ["source_date"])
    op.create_index(
        "ix_GameContentAnalysisIssue_game_id_source_date",
        "GameContentAnalysisIssue",
        ["game_id", "source_date"],
    )


def downgrade() -> None:
    op.drop_index("ix_GameContentAnalysisIssue_game_id_source_date", table_name="GameContentAnalysisIssue")
    op.drop_index("ix_GameContentAnalysisIssue_source_date", table_name="GameContentAnalysisIssue")
    op.drop_index("ix_GameContentAnalysisIssue_game_id", table_name="GameContentAnalysisIssue")
    op.drop_table("GameContentAnalysisIssue")
