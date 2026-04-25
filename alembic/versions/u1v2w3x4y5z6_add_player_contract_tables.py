"""add player contract tables

Revision ID: u1v2w3x4y5z6
Revises: m1n2o3p4q5r6
Create Date: 2026-04-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "u1v2w3x4y5z6"
down_revision: Union[str, None] = "m1n2o3p4q5r6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "PlayerContract",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("player_id", sa.String(50), sa.ForeignKey("Player.player_id"), nullable=False),
        sa.Column("spotrac_id", sa.Integer, nullable=True),
        sa.Column("signed_with_team_id", sa.String(50), sa.ForeignKey("Team.team_id"), nullable=True),
        sa.Column("signed_at_age", sa.Integer, nullable=True),
        sa.Column("start_season", sa.Integer, nullable=False),
        sa.Column("end_season", sa.Integer, nullable=False),
        sa.Column("years", sa.Integer, nullable=False),
        sa.Column("total_value_usd", sa.BigInteger, nullable=True),
        sa.Column("aav_usd", sa.BigInteger, nullable=True),
        sa.Column("guaranteed_usd", sa.BigInteger, nullable=True),
        sa.Column("contract_type", sa.String(64), nullable=True),
        sa.Column("source_url", sa.String(512), nullable=True),
        sa.Column("scraped_at", sa.DateTime, nullable=True),
        sa.UniqueConstraint("player_id", "start_season", "end_season", name="uq_PlayerContract_player_range"),
    )
    op.create_index("ix_PlayerContract_player", "PlayerContract", ["player_id"])
    op.create_index("ix_PlayerContract_spotrac", "PlayerContract", ["spotrac_id"])
    op.create_index("ix_PlayerContract_team", "PlayerContract", ["signed_with_team_id"])

    op.create_table(
        "PlayerContractYear",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("contract_id", sa.Integer, sa.ForeignKey("PlayerContract.id", ondelete="CASCADE"), nullable=False),
        sa.Column("player_id", sa.String(50), sa.ForeignKey("Player.player_id"), nullable=False),
        sa.Column("season", sa.Integer, nullable=False),
        sa.Column("age", sa.Integer, nullable=True),
        sa.Column("status", sa.String(64), nullable=True),
        sa.Column("cap_hit_usd", sa.BigInteger, nullable=True),
        sa.Column("base_salary_usd", sa.BigInteger, nullable=True),
        sa.Column("incentives_likely_usd", sa.BigInteger, nullable=True),
        sa.Column("incentives_unlikely_usd", sa.BigInteger, nullable=True),
        sa.Column("cash_guaranteed_usd", sa.BigInteger, nullable=True),
        sa.Column("cash_annual_usd", sa.BigInteger, nullable=True),
        sa.UniqueConstraint("contract_id", "season", name="uq_PlayerContractYear_contract_season"),
    )
    op.create_index("ix_PlayerContractYear_contract", "PlayerContractYear", ["contract_id"])
    op.create_index("ix_PlayerContractYear_player_season", "PlayerContractYear", ["player_id", "season"])


def downgrade() -> None:
    op.drop_index("ix_PlayerContractYear_player_season", "PlayerContractYear")
    op.drop_index("ix_PlayerContractYear_contract", "PlayerContractYear")
    op.drop_table("PlayerContractYear")
    op.drop_index("ix_PlayerContract_team", "PlayerContract")
    op.drop_index("ix_PlayerContract_spotrac", "PlayerContract")
    op.drop_index("ix_PlayerContract_player", "PlayerContract")
    op.drop_table("PlayerContract")
