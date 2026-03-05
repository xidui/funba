"""add MetricDefinition table

Revision ID: 381fd87c5f06
Revises: b2c3d4e5f6a7
Create Date: 2026-03-04 17:56:37.941341

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '381fd87c5f06'
down_revision: Union[str, None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'MetricDefinition',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('key', sa.String(64), nullable=False),
        sa.Column('name', sa.String(128), nullable=False),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('scope', sa.String(16), nullable=False),
        sa.Column('category', sa.String(32), nullable=True),
        sa.Column('group_key', sa.String(64), nullable=True),
        sa.Column('source_type', sa.String(16), nullable=False),
        sa.Column('status', sa.String(16), nullable=False),
        sa.Column('definition_json', sa.Text(), nullable=True),
        sa.Column('expression', sa.Text(), nullable=True),
        sa.Column('min_sample', sa.Integer(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_MetricDefinition_key', 'MetricDefinition', ['key'], unique=True)
    op.create_index('ix_MetricDefinition_group_key', 'MetricDefinition', ['group_key'], unique=False)


def downgrade() -> None:
    op.drop_index('ix_MetricDefinition_group_key', table_name='MetricDefinition')
    op.drop_index('ix_MetricDefinition_key', table_name='MetricDefinition')
    op.drop_table('MetricDefinition')
