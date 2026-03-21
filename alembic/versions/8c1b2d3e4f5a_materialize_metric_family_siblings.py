"""materialize metric family siblings

Revision ID: 8c1b2d3e4f5a
Revises: 7a4f0d5b61c2
Create Date: 2026-03-21 13:10:00.000000
"""

from __future__ import annotations

from datetime import datetime
import json

from alembic import op
import sqlalchemy as sa

from metrics.framework.family import (
    FAMILY_VARIANT_CAREER,
    FAMILY_VARIANT_SEASON,
    build_career_code_variant,
    build_career_rule_definition,
    derive_career_description,
    derive_career_min_sample,
    derive_career_name,
    family_career_key,
    rule_is_career_variant,
    rule_supports_career,
)
from metrics.framework.runtime import load_code_metric


# revision identifiers, used by Alembic.
revision = "8c1b2d3e4f5a"
down_revision = "7a4f0d5b61c2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col["name"] for col in inspector.get_columns("MetricDefinition")}
    indexes = {idx["name"] for idx in inspector.get_indexes("MetricDefinition")}

    if "family_key" not in columns:
        op.add_column("MetricDefinition", sa.Column("family_key", sa.String(length=64), nullable=True))
    if "variant" not in columns:
        op.add_column(
            "MetricDefinition",
            sa.Column("variant", sa.String(length=16), nullable=False, server_default=FAMILY_VARIANT_SEASON),
        )
    if "base_metric_key" not in columns:
        op.add_column("MetricDefinition", sa.Column("base_metric_key", sa.String(length=64), nullable=True))
    if "managed_family" not in columns:
        op.add_column(
            "MetricDefinition",
            sa.Column("managed_family", sa.Boolean(), nullable=False, server_default=sa.false()),
        )
    if "ix_MetricDefinition_family_key" not in indexes:
        op.create_index("ix_MetricDefinition_family_key", "MetricDefinition", ["family_key"], unique=False)
    if "ix_MetricDefinition_base_metric_key" not in indexes:
        op.create_index("ix_MetricDefinition_base_metric_key", "MetricDefinition", ["base_metric_key"], unique=False)

    metadata = sa.MetaData()
    metric_definition = sa.Table(
        "MetricDefinition",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("key", sa.String(64)),
        sa.Column("family_key", sa.String(64)),
        sa.Column("variant", sa.String(16)),
        sa.Column("base_metric_key", sa.String(64)),
        sa.Column("managed_family", sa.Boolean),
        sa.Column("name", sa.String(128)),
        sa.Column("description", sa.Text),
        sa.Column("scope", sa.String(16)),
        sa.Column("category", sa.String(32)),
        sa.Column("group_key", sa.String(64)),
        sa.Column("source_type", sa.String(16)),
        sa.Column("status", sa.String(16)),
        sa.Column("definition_json", sa.Text),
        sa.Column("code_python", sa.Text),
        sa.Column("expression", sa.Text),
        sa.Column("min_sample", sa.Integer),
        sa.Column("created_at", sa.DateTime),
        sa.Column("updated_at", sa.DateTime),
    )

    rows = list(bind.execute(sa.select(metric_definition)).mappings())
    existing_keys = {row["key"] for row in rows}
    now = datetime.utcnow()
    sibling_rows: list[dict] = []

    for row in rows:
        definition = {}
        supports_career = False
        is_career_metric = False
        career_name_suffix = " (Career)"
        career_min_sample = None

        if row["source_type"] == "code" and row["code_python"]:
            metric = load_code_metric(row["code_python"])
            supports_career = bool(getattr(metric, "supports_career", False)) and row["scope"] != "game"
            is_career_metric = bool(getattr(metric, "career", False))
            career_name_suffix = str(getattr(metric, "career_name_suffix", " (Career)") or " (Career)")
            career_min_sample = getattr(metric, "career_min_sample", None)
        elif row["source_type"] == "rule":
            definition = json.loads(row["definition_json"] or "{}")
            supports_career = rule_supports_career(definition, row["scope"])
            is_career_metric = rule_is_career_variant(definition)
            career_name_suffix = str(definition.get("career_name_suffix") or " (Career)")
            career_min_sample = definition.get("career_min_sample")

        variant = FAMILY_VARIANT_CAREER if is_career_metric else FAMILY_VARIANT_SEASON
        managed_family = bool(supports_career)
        bind.execute(
            metric_definition.update()
            .where(metric_definition.c.id == row["id"])
            .values(
                family_key=row["key"],
                variant=variant,
                base_metric_key=None,
                managed_family=managed_family,
            )
        )

        if not supports_career:
            continue

        sibling_key = family_career_key(row["key"])
        if sibling_key in existing_keys:
            continue

        sibling_name = derive_career_name(row["name"], career_name_suffix)
        sibling_description = derive_career_description(row["description"] or "")
        sibling_min_sample = derive_career_min_sample(row["min_sample"] or 1, career_min_sample)

        sibling_row = {
            "key": sibling_key,
            "family_key": row["key"],
            "variant": FAMILY_VARIANT_CAREER,
            "base_metric_key": row["key"],
            "managed_family": True,
            "name": sibling_name,
            "description": sibling_description,
            "scope": row["scope"],
            "category": row["category"],
            "group_key": row["group_key"],
            "source_type": row["source_type"],
            "status": row["status"],
            "definition_json": None,
            "code_python": None,
            "expression": row["expression"],
            "min_sample": sibling_min_sample,
            "created_at": row["created_at"] or now,
            "updated_at": row["updated_at"] or now,
        }

        if row["source_type"] == "code":
            sibling_row["code_python"] = build_career_code_variant(
                row["code_python"],
                base_key=row["key"],
                name=sibling_name,
                description=sibling_description,
                min_sample=sibling_min_sample,
            )
        elif row["source_type"] == "rule":
            sibling_row["definition_json"] = json.dumps(build_career_rule_definition(definition))

        sibling_rows.append(sibling_row)
        existing_keys.add(sibling_key)

    if sibling_rows:
        bind.execute(metric_definition.insert(), sibling_rows)

    op.alter_column("MetricDefinition", "family_key", existing_type=sa.String(length=64), nullable=False)
    if "uq_MetricDefinition_family_variant" not in indexes:
        op.create_index("uq_MetricDefinition_family_variant", "MetricDefinition", ["family_key", "variant"], unique=True)
    op.alter_column("MetricDefinition", "variant", existing_type=sa.String(length=16), server_default=None)
    op.alter_column("MetricDefinition", "managed_family", existing_type=sa.Boolean(), server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    metadata = sa.MetaData()
    metric_definition = sa.Table(
        "MetricDefinition",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("variant", sa.String(16)),
        sa.Column("managed_family", sa.Boolean),
    )
    bind.execute(
        metric_definition.delete().where(
            sa.and_(
                metric_definition.c.managed_family.is_(True),
                metric_definition.c.variant == FAMILY_VARIANT_CAREER,
            )
        )
    )

    op.drop_index("uq_MetricDefinition_family_variant", table_name="MetricDefinition")
    op.drop_index("ix_MetricDefinition_base_metric_key", table_name="MetricDefinition")
    op.drop_index("ix_MetricDefinition_family_key", table_name="MetricDefinition")
    op.drop_column("MetricDefinition", "managed_family")
    op.drop_column("MetricDefinition", "base_metric_key")
    op.drop_column("MetricDefinition", "variant")
    op.drop_column("MetricDefinition", "family_key")
