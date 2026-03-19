"""add_code_python_to_metric_definition

Revision ID: 365bd0dc2039
Revises: d2e3f4a5b6c7
Create Date: 2026-03-18 22:19:05.035536

"""
from typing import Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '365bd0dc2039'
down_revision: Union[str, None] = 'd2e3f4a5b6c7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('MetricDefinition', sa.Column('code_python', sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column('MetricDefinition', 'code_python')
