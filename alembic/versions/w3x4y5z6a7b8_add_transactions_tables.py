"""add transactions tables and Player.br_slug

Revision ID: w3x4y5z6a7b8
Revises: v2w3x4y5z6a7
Create Date: 2026-04-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "w3x4y5z6a7b8"
down_revision: Union[str, None] = "v2w3x4y5z6a7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Player.br_slug (Basketball-Reference ID like "kuminjo01")
    op.add_column("Player", sa.Column("br_slug", sa.String(20), nullable=True))
    op.create_index("ix_Player_br_slug", "Player", ["br_slug"])

    # Each row = one BR transaction paragraph (a trade leg, signing, waive...)
    op.create_table(
        "TeamTransaction",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("transaction_date", sa.Date, nullable=False),
        sa.Column("season", sa.Integer, nullable=False),
        sa.Column("transaction_type", sa.String(32), nullable=False),
        sa.Column("multi_team_count", sa.Integer, nullable=False, server_default="1"),
        sa.Column("raw_text", sa.Text, nullable=False),
        sa.Column("raw_html", sa.Text, nullable=True),
        sa.Column("text_hash", sa.String(40), nullable=False),
        sa.Column("source_url", sa.String(255), nullable=True),
        sa.Column("scraped_at", sa.DateTime, nullable=True),
        sa.UniqueConstraint("transaction_date", "text_hash", name="uq_TeamTransaction_date_hash"),
    )
    op.create_index("ix_TeamTransaction_date", "TeamTransaction", ["transaction_date"])
    op.create_index("ix_TeamTransaction_season", "TeamTransaction", ["season"])
    op.create_index("ix_TeamTransaction_type", "TeamTransaction", ["transaction_type"])

    # Each row = one moving piece in a transaction
    op.create_table(
        "TransactionAsset",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("transaction_id", sa.Integer, sa.ForeignKey("TeamTransaction.id", ondelete="CASCADE"), nullable=False),
        sa.Column("asset_type", sa.String(16), nullable=False),
        sa.Column("from_team_id", sa.String(50), sa.ForeignKey("Team.team_id"), nullable=True),
        sa.Column("to_team_id", sa.String(50), sa.ForeignKey("Team.team_id"), nullable=True),
        sa.Column("player_id", sa.String(50), sa.ForeignKey("Player.player_id"), nullable=True),
        sa.Column("player_br_slug", sa.String(20), nullable=True),
        sa.Column("player_name_raw", sa.String(120), nullable=True),
        sa.Column("pick_year", sa.Integer, nullable=True),
        sa.Column("pick_round", sa.Integer, nullable=True),
        sa.Column("pick_origin_team_id", sa.String(50), sa.ForeignKey("Team.team_id"), nullable=True),
        sa.Column("pick_protection", sa.Text, nullable=True),
        sa.Column("cash_usd", sa.BigInteger, nullable=True),
        sa.Column("notes", sa.Text, nullable=True),
    )
    op.create_index("ix_TransactionAsset_tr", "TransactionAsset", ["transaction_id"])
    op.create_index("ix_TransactionAsset_player", "TransactionAsset", ["player_id"])
    op.create_index("ix_TransactionAsset_from", "TransactionAsset", ["from_team_id"])
    op.create_index("ix_TransactionAsset_to", "TransactionAsset", ["to_team_id"])


def downgrade() -> None:
    op.drop_index("ix_TransactionAsset_to", "TransactionAsset")
    op.drop_index("ix_TransactionAsset_from", "TransactionAsset")
    op.drop_index("ix_TransactionAsset_player", "TransactionAsset")
    op.drop_index("ix_TransactionAsset_tr", "TransactionAsset")
    op.drop_table("TransactionAsset")
    op.drop_index("ix_TeamTransaction_type", "TeamTransaction")
    op.drop_index("ix_TeamTransaction_season", "TeamTransaction")
    op.drop_index("ix_TeamTransaction_date", "TeamTransaction")
    op.drop_table("TeamTransaction")
    op.drop_index("ix_Player_br_slug", "Player")
    op.drop_column("Player", "br_slug")
