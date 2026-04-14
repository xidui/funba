"""add news feed tables

Revision ID: n1e2w3s4a5b6
Revises: c1f2e3d4a5b6
Create Date: 2026-04-14 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "n1e2w3s4a5b6"
down_revision: Union[str, None] = "c1f2e3d4a5b6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "NewsCluster",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("representative_article_id", sa.Integer, nullable=True),
        sa.Column("first_seen_at", sa.DateTime, nullable=False),
        sa.Column("last_seen_at", sa.DateTime, nullable=False),
        sa.Column("article_count", sa.Integer, nullable=False, server_default="1"),
        sa.Column("unique_view_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("view_count_refreshed_at", sa.DateTime, nullable=True),
        sa.Column("score", sa.Float, nullable=False, server_default="0"),
        sa.Column("score_refreshed_at", sa.DateTime, nullable=True),
    )
    op.create_index("ix_NewsCluster_first_seen_at", "NewsCluster", ["first_seen_at"])
    op.create_index("ix_NewsCluster_last_seen_at", "NewsCluster", ["last_seen_at"])
    op.create_index("ix_NewsCluster_score", "NewsCluster", ["score"])

    op.create_table(
        "NewsArticle",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column(
            "cluster_id",
            sa.Integer,
            sa.ForeignKey("NewsCluster.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column(
            "internal_social_post_id",
            sa.Integer,
            sa.ForeignKey("SocialPost.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("source_guid", sa.String(255), nullable=False),
        sa.Column("url", sa.String(1024), nullable=False),
        sa.Column("title", sa.String(512), nullable=False),
        sa.Column("summary", sa.Text, nullable=True),
        sa.Column("thumbnail_url", sa.String(1024), nullable=True),
        sa.Column("published_at", sa.DateTime, nullable=False),
        sa.Column("fetched_at", sa.DateTime, nullable=False),
        sa.Column("embedding", sa.LargeBinary(length=64 * 1024), nullable=True),
        sa.Column("embedding_model", sa.String(64), nullable=True),
        sa.Column("embedding_text_hash", sa.String(64), nullable=True),
        sa.UniqueConstraint("source", "source_guid", name="uq_NewsArticle_source_guid"),
    )
    op.create_index("ix_NewsArticle_published_at", "NewsArticle", ["published_at"])
    op.create_index("ix_NewsArticle_cluster_id", "NewsArticle", ["cluster_id"])
    op.create_index("ix_NewsArticle_source_published", "NewsArticle", ["source", "published_at"])

    # Post-create: add the FK from NewsCluster.representative_article_id -> NewsArticle.id
    # (broken out so both tables can exist before the circular reference is wired).
    op.create_foreign_key(
        "fk_NewsCluster_rep_article",
        "NewsCluster",
        "NewsArticle",
        ["representative_article_id"],
        ["id"],
        ondelete="SET NULL",
    )

    op.create_table(
        "NewsArticlePlayer",
        sa.Column(
            "article_id",
            sa.Integer,
            sa.ForeignKey("NewsArticle.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "player_id",
            sa.String(50),
            sa.ForeignKey("Player.player_id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )
    op.create_index("ix_NewsArticlePlayer_player_id", "NewsArticlePlayer", ["player_id"])

    op.create_table(
        "NewsArticleTeam",
        sa.Column(
            "article_id",
            sa.Integer,
            sa.ForeignKey("NewsArticle.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "team_id",
            sa.String(50),
            sa.ForeignKey("Team.team_id", ondelete="CASCADE"),
            primary_key=True,
        ),
    )
    op.create_index("ix_NewsArticleTeam_team_id", "NewsArticleTeam", ["team_id"])


def downgrade() -> None:
    op.drop_index("ix_NewsArticleTeam_team_id", table_name="NewsArticleTeam")
    op.drop_table("NewsArticleTeam")
    op.drop_index("ix_NewsArticlePlayer_player_id", table_name="NewsArticlePlayer")
    op.drop_table("NewsArticlePlayer")
    op.drop_constraint("fk_NewsCluster_rep_article", "NewsCluster", type_="foreignkey")
    op.drop_index("ix_NewsArticle_source_published", table_name="NewsArticle")
    op.drop_index("ix_NewsArticle_cluster_id", table_name="NewsArticle")
    op.drop_index("ix_NewsArticle_published_at", table_name="NewsArticle")
    op.drop_table("NewsArticle")
    op.drop_index("ix_NewsCluster_score", table_name="NewsCluster")
    op.drop_index("ix_NewsCluster_last_seen_at", table_name="NewsCluster")
    op.drop_index("ix_NewsCluster_first_seen_at", table_name="NewsCluster")
    op.drop_table("NewsCluster")
