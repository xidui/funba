"""Add Kaggle metadata columns to Team and Game.

Revision ID: x7y8z9a0b1c2
Revises: 37273a0884ad, 5bb7cf6ad45e
Create Date: 2026-04-11
"""

from alembic import op
import sqlalchemy as sa


revision = "x7y8z9a0b1c2"
down_revision = ("37273a0884ad", "5bb7cf6ad45e")
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("Team", sa.Column("arena", sa.String(length=100), nullable=True))
    op.add_column("Team", sa.Column("arena_capacity", sa.Integer(), nullable=True))
    op.add_column("Team", sa.Column("owner", sa.String(length=100), nullable=True))
    op.add_column("Team", sa.Column("general_manager", sa.String(length=100), nullable=True))
    op.add_column("Team", sa.Column("head_coach", sa.String(length=100), nullable=True))
    op.add_column("Team", sa.Column("g_league_affiliation", sa.String(length=100), nullable=True))
    op.add_column("Team", sa.Column("facebook_url", sa.String(length=255), nullable=True))
    op.add_column("Team", sa.Column("instagram_url", sa.String(length=255), nullable=True))
    op.add_column("Team", sa.Column("twitter_url", sa.String(length=255), nullable=True))

    op.add_column("Game", sa.Column("attendance", sa.Integer(), nullable=True))
    op.add_column("Game", sa.Column("tipoff_time", sa.String(length=32), nullable=True))
    op.add_column("Game", sa.Column("external_game_code", sa.String(length=32), nullable=True))
    op.add_column("Game", sa.Column("national_tv_broadcaster", sa.String(length=32), nullable=True))


def downgrade():
    op.drop_column("Game", "national_tv_broadcaster")
    op.drop_column("Game", "external_game_code")
    op.drop_column("Game", "tipoff_time")
    op.drop_column("Game", "attendance")

    op.drop_column("Team", "twitter_url")
    op.drop_column("Team", "instagram_url")
    op.drop_column("Team", "facebook_url")
    op.drop_column("Team", "g_league_affiliation")
    op.drop_column("Team", "head_coach")
    op.drop_column("Team", "general_manager")
    op.drop_column("Team", "owner")
    op.drop_column("Team", "arena_capacity")
    op.drop_column("Team", "arena")
