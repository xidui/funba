"""rebuild_franchise_scoring_rank_as_player_franchise

Revision ID: e3f4a5b6c7d8
Revises: c3d4e5f6a7b8
Create Date: 2026-03-08

Convert franchise scoring rank DB metrics from "current franchise for a player"
to "one row per player-franchise stint", and clear previously computed rows so
the metrics can be backfilled from the corrected definition.
"""
from __future__ import annotations

import json
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e3f4a5b6c7d8"
down_revision: Union[str, None] = "c3d4e5f6a7b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_FRANCHISE_KEYS = [
    "franchise_scoring_rank_regular",
    "franchise_scoring_rank_combined",
]


def upgrade() -> None:
    bind = op.get_bind()

    metric_definition = sa.table(
        "MetricDefinition",
        sa.column("key", sa.String(64)),
        sa.column("name", sa.String(128)),
        sa.column("description", sa.Text()),
        sa.column("scope", sa.String(16)),
        sa.column("definition_json", sa.Text()),
        sa.column("expression", sa.Text()),
    )

    updated = {
        "franchise_scoring_rank_regular": {
            "name": "Franchise Scoring Rank (Regular Season)",
            "description": "Player regular-season points for a franchise stint, ranked within that franchise.",
            "scope": "player_franchise",
            "definition_json": json.dumps({
                "source": "player_game_stats",
                "time_scope": "career",
                "season_types": ["regular"],
                "aggregation": "sum",
                "stat": "pts",
                "ranking": {"partition_by": ["entity.franchise_id"]},
                "display": {"value_format": "integer", "unit": "pts"},
            }),
            "expression": "Career regular-season points for each player-franchise stint, ranked within that franchise.",
        },
        "franchise_scoring_rank_combined": {
            "name": "Franchise Scoring Rank (Regular + Playoffs)",
            "description": "Player combined regular-season and playoff points for a franchise stint, ranked within that franchise.",
            "scope": "player_franchise",
            "definition_json": json.dumps({
                "source": "player_game_stats",
                "time_scope": "career",
                "season_types": ["regular", "playoffs"],
                "aggregation": "sum",
                "stat": "pts",
                "ranking": {"partition_by": ["entity.franchise_id"]},
                "display": {"value_format": "integer", "unit": "pts"},
            }),
            "expression": "Career regular-season plus playoff points for each player-franchise stint, ranked within that franchise.",
        },
    }

    for key, payload in updated.items():
        bind.execute(
            sa.update(metric_definition)
            .where(metric_definition.c.key == key)
            .values(**payload)
        )

    metric_result = sa.table(
        "MetricResult",
        sa.column("metric_key", sa.String(64)),
    )
    metric_run_log = sa.table(
        "MetricRunLog",
        sa.column("metric_key", sa.String(64)),
    )
    metric_job_claim = sa.table(
        "MetricJobClaim",
        sa.column("metric_key", sa.String(64)),
    )

    bind.execute(sa.delete(metric_result).where(metric_result.c.metric_key.in_(_FRANCHISE_KEYS)))
    bind.execute(sa.delete(metric_run_log).where(metric_run_log.c.metric_key.in_(_FRANCHISE_KEYS)))
    bind.execute(sa.delete(metric_job_claim).where(metric_job_claim.c.metric_key.in_(_FRANCHISE_KEYS)))


def downgrade() -> None:
    bind = op.get_bind()

    metric_definition = sa.table(
        "MetricDefinition",
        sa.column("key", sa.String(64)),
        sa.column("name", sa.String(128)),
        sa.column("description", sa.Text()),
        sa.column("scope", sa.String(16)),
        sa.column("definition_json", sa.Text()),
        sa.column("expression", sa.Text()),
    )

    reverted = {
        "franchise_scoring_rank_regular": {
            "name": "Franchise Scoring Rank (Regular Season)",
            "description": "Player career regular-season points for their current franchise, ranked within that franchise.",
            "scope": "player",
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
        },
        "franchise_scoring_rank_combined": {
            "name": "Franchise Scoring Rank (Regular + Playoffs)",
            "description": "Player career points for their current franchise across regular season and playoffs, ranked within that franchise.",
            "scope": "player",
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
        },
    }

    for key, payload in reverted.items():
        bind.execute(
            sa.update(metric_definition)
            .where(metric_definition.c.key == key)
            .values(**payload)
        )

    metric_result = sa.table(
        "MetricResult",
        sa.column("metric_key", sa.String(64)),
    )
    metric_run_log = sa.table(
        "MetricRunLog",
        sa.column("metric_key", sa.String(64)),
    )
    metric_job_claim = sa.table(
        "MetricJobClaim",
        sa.column("metric_key", sa.String(64)),
    )

    bind.execute(sa.delete(metric_result).where(metric_result.c.metric_key.in_(_FRANCHISE_KEYS)))
    bind.execute(sa.delete(metric_run_log).where(metric_run_log.c.metric_key.in_(_FRANCHISE_KEYS)))
    bind.execute(sa.delete(metric_job_claim).where(metric_job_claim.c.metric_key.in_(_FRANCHISE_KEYS)))
