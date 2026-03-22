"""add created_by_user_id to MetricDefinition

Revision ID: da6b6933fc31
Revises: d7890cf56168
Create Date: 2026-03-21 20:24:56.219869

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'da6b6933fc31'
down_revision: Union[str, None] = 'd7890cf56168'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('MetricDefinition', sa.Column('created_by_user_id', sa.String(length=36), nullable=True))
    op.create_foreign_key('fk_metricdef_created_by', 'MetricDefinition', 'User', ['created_by_user_id'], ['id'])


def downgrade() -> None:
    op.drop_constraint('fk_metricdef_created_by', 'MetricDefinition', type_='foreignkey')
    op.drop_column('MetricDefinition', 'created_by_user_id')
