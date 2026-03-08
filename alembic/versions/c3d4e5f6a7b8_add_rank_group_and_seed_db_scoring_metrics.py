"""add_rank_group_and_seed_db_scoring_metrics

Revision ID: c3d4e5f6a7b8
Revises: f1c2d3e4f5a6
Create Date: 2026-03-08

Add MetricResult.rank_group so metrics can rank within a subgroup
(for example, a franchise), and seed a few scoring/record metrics that
exercise the richer DB-rule definition format.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "c3d4e5f6a7b8"
down_revision: Union[str, None] = "f1c2d3e4f5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _metric_definition_table() -> sa.Table:
    return sa.table(
        "MetricDefinition",
        sa.column("key", sa.String(64)),
        sa.column("name", sa.String(128)),
        sa.column("description", sa.Text()),
        sa.column("scope", sa.String(16)),
        sa.column("category", sa.String(32)),
        sa.column("group_key", sa.String(64)),
        sa.column("source_type", sa.String(16)),
        sa.column("status", sa.String(16)),
        sa.column("definition_json", sa.Text()),
        sa.column("expression", sa.Text()),
        sa.column("min_sample", sa.Integer()),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
    )


def upgrade() -> None:
    op.add_column("MetricResult", sa.Column("rank_group", sa.String(64), nullable=True))
    op.drop_index("ix_MetricResult_ranking", table_name="MetricResult")
    op.create_index(
        "ix_MetricResult_ranking",
        "MetricResult",
        ["metric_key", "season", "rank_group", "value_num"],
        unique=False,
    )

    metric_definition = _metric_definition_table()
    bind = op.get_bind()
    now = datetime.utcnow()

    seeded_metrics = [
        {
            "key": "career_points_regular",
            "name": "Career Points (Regular Season)",
            "description": "Player career regular-season points.",
            "scope": "player",
            "category": "scoring",
            "group_key": "career_points",
            "source_type": "rule",
            "status": "published",
            "definition_json": json.dumps({
                "source": "player_game_stats",
                "time_scope": "career",
                "season_types": ["regular"],
                "aggregation": "sum",
                "stat": "pts",
                "display": {"value_format": "integer", "unit": "pts"},
            }),
            "expression": "Player career regular-season points.",
            "min_sample": 1,
            "created_at": now,
            "updated_at": now,
        },
        {
            "key": "career_points_combined",
            "name": "Career Points (Regular + Playoffs)",
            "description": "Player career points across regular season and playoffs.",
            "scope": "player",
            "category": "scoring",
            "group_key": "career_points",
            "source_type": "rule",
            "status": "published",
            "definition_json": json.dumps({
                "source": "player_game_stats",
                "time_scope": "career",
                "season_types": ["regular", "playoffs"],
                "aggregation": "sum",
                "stat": "pts",
                "display": {"value_format": "integer", "unit": "pts"},
            }),
            "expression": "Player career points across regular season and playoffs.",
            "min_sample": 1,
            "created_at": now,
            "updated_at": now,
        },
        {
            "key": "franchise_scoring_rank_regular",
            "name": "Franchise Scoring Rank (Regular Season)",
            "description": "Player career regular-season points for their current franchise, ranked within that franchise.",
            "scope": "player",
            "category": "record",
            "group_key": "franchise_scoring_rank",
            "source_type": "rule",
            "status": "published",
            "definition_json": json.dumps({
                "source": "player_game_stats",
                "time_scope": "career",
                "season_types": ["regular"],
                "aggregation": "sum",
                "stat": "pts",
                "filters": [
                    {"field": "franchise_id", "op": "=", "value_from": "entity.current_franchise_id"},
                ],
                "ranking": {"partition_by": ["entity.current_franchise_id"]},
                "display": {"value_format": "integer", "unit": "pts"},
            }),
            "expression": "Career regular-season points for the player's current franchise, ranked within that franchise.",
            "min_sample": 1,
            "created_at": now,
            "updated_at": now,
        },
        {
            "key": "franchise_scoring_rank_combined",
            "name": "Franchise Scoring Rank (Regular + Playoffs)",
            "description": "Player career points for their current franchise across regular season and playoffs, ranked within that franchise.",
            "scope": "player",
            "category": "record",
            "group_key": "franchise_scoring_rank",
            "source_type": "rule",
            "status": "published",
            "definition_json": json.dumps({
                "source": "player_game_stats",
                "time_scope": "career",
                "season_types": ["regular", "playoffs"],
                "aggregation": "sum",
                "stat": "pts",
                "filters": [
                    {"field": "franchise_id", "op": "=", "value_from": "entity.current_franchise_id"},
                ],
                "ranking": {"partition_by": ["entity.current_franchise_id"]},
                "display": {"value_format": "integer", "unit": "pts"},
            }),
            "expression": "Career regular-season plus playoff points for the player's current franchise, ranked within that franchise.",
            "min_sample": 1,
            "created_at": now,
            "updated_at": now,
        },
    ]

    keys = [row["key"] for row in seeded_metrics]
    bind.execute(
        sa.delete(metric_definition).where(metric_definition.c.key.in_(keys))
    )
    bind.execute(sa.insert(metric_definition), seeded_metrics)


def downgrade() -> None:
    metric_definition = _metric_definition_table()
    bind = op.get_bind()
    bind.execute(
        sa.delete(metric_definition).where(
            metric_definition.c.key.in_(
                [
                    "career_points_regular",
                    "career_points_combined",
                    "franchise_scoring_rank_regular",
                    "franchise_scoring_rank_combined",
                ]
            )
        )
    )

    op.drop_index("ix_MetricResult_ranking", table_name="MetricResult")
    op.create_index(
        "ix_MetricResult_ranking",
        "MetricResult",
        ["metric_key", "season", "value_num"],
        unique=False,
    )
    op.drop_column("MetricResult", "rank_group")
