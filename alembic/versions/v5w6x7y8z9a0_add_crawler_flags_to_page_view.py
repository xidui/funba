"""add crawler flags to page view

Revision ID: v5w6x7y8z9a0
Revises: u4v5w6x7y8z9
Create Date: 2026-04-09 17:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "v5w6x7y8z9a0"
down_revision = "u4v5w6x7y8z9"
branch_labels = None
depends_on = None

_CRAWLER_PATTERNS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("googlebot", ("googlebot",)),
    ("google-inspectiontool", ("google-inspectiontool",)),
    ("apis-google", ("apis-google",)),
    ("bingbot", ("bingbot",)),
    ("bingpreview", ("bingpreview",)),
    ("baiduspider", ("baiduspider",)),
    ("facebookexternalhit", ("facebookexternalhit",)),
    ("meta-webindexer", ("meta-webindexer",)),
    ("twitterbot", ("twitterbot",)),
    ("linkedinbot", ("linkedinbot",)),
    ("duckduckbot", ("duckduckbot",)),
    ("yandexbot", ("yandexbot",)),
    ("applebot", ("applebot",)),
    ("sogou", ("sogou",)),
    ("360spider", ("360spider",)),
    ("slurp", ("slurp",)),
    ("mediapartners-google", ("mediapartners-google",)),
    ("bytespider", ("bytespider",)),
    ("gptbot", ("gptbot",)),
    ("claudebot", ("claudebot",)),
)


def upgrade() -> None:
    op.add_column(
        "PageView",
        sa.Column("is_crawler", sa.Boolean(), nullable=False, server_default=sa.text("0")),
    )
    op.add_column(
        "PageView",
        sa.Column("crawler_name", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_PageView_is_crawler_created_at",
        "PageView",
        ["is_crawler", "created_at"],
    )
    op.create_index(
        "ix_PageView_crawler_name_created_at",
        "PageView",
        ["crawler_name", "created_at"],
    )

    connection = op.get_bind()
    for crawler_name, patterns in _CRAWLER_PATTERNS:
        clauses = []
        params: dict[str, str] = {"crawler_name": crawler_name}
        for index, pattern in enumerate(patterns):
            key = f"pattern_{index}"
            clauses.append(f"LOWER(COALESCE(user_agent, '')) LIKE :{key}")
            params[key] = f"%{pattern}%"
        connection.execute(
            sa.text(
                f"""
                UPDATE PageView
                SET is_crawler = 1,
                    crawler_name = :crawler_name
                WHERE {' OR '.join(clauses)}
                """
            ),
            params,
        )

    op.alter_column("PageView", "is_crawler", server_default=None)


def downgrade() -> None:
    op.drop_index("ix_PageView_crawler_name_created_at", table_name="PageView")
    op.drop_index("ix_PageView_is_crawler_created_at", table_name="PageView")
    op.drop_column("PageView", "crawler_name")
    op.drop_column("PageView", "is_crawler")
