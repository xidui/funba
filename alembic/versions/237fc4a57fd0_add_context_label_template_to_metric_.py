"""add_context_label_template_to_metric_definition

Revision ID: 237fc4a57fd0
Revises: 9b7c6d5e4f3a
Create Date: 2026-03-21 17:50:14.850113

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '237fc4a57fd0'
down_revision: Union[str, None] = '9b7c6d5e4f3a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('MetricDefinition', sa.Column('context_label_template', sa.String(length=256), nullable=True))


def downgrade() -> None:
    op.drop_column('MetricDefinition', 'context_label_template')
