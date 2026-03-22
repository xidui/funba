"""add_stripe_subscription_to_user

Revision ID: d7890cf56168
Revises: 237fc4a57fd0
Create Date: 2026-03-21 18:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7890cf56168'
down_revision: Union[str, None] = '237fc4a57fd0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('User', sa.Column('stripe_customer_id', sa.String(length=255), nullable=True))
    op.add_column('User', sa.Column('subscription_tier', sa.String(length=16), nullable=False, server_default='free'))
    op.add_column('User', sa.Column('subscription_status', sa.String(length=32), nullable=True))
    op.add_column('User', sa.Column('subscription_expires_at', sa.DateTime(), nullable=True))
    op.create_index('ix_User_stripe_customer_id', 'User', ['stripe_customer_id'], unique=True)


def downgrade() -> None:
    op.drop_index('ix_User_stripe_customer_id', table_name='User')
    op.drop_column('User', 'subscription_expires_at')
    op.drop_column('User', 'subscription_status')
    op.drop_column('User', 'subscription_tier')
    op.drop_column('User', 'stripe_customer_id')
