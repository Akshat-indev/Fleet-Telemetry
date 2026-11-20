"""vehicle seq counters for fleet summary

Revision ID: 3e21795c6853
Revises: e929c0d46158
Create Date: 2026-11-20 16:30:40.631858

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '3e21795c6853'
down_revision: Union[str, None] = 'e929c0d46158'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('vehicles', sa.Column('min_seq', sa.BigInteger(), nullable=True))
    op.add_column('vehicles', sa.Column('max_seq', sa.BigInteger(), nullable=True))
    op.add_column('vehicles', sa.Column('received_count', sa.BigInteger(), server_default=sa.text('0'), nullable=False))
    # Backfill from stored history so existing fleets get correct counters.
    op.execute("""
        UPDATE vehicles v
        SET min_seq = s.min_seq, max_seq = s.max_seq, received_count = s.received
        FROM (
            SELECT vehicle_id, min(seq) AS min_seq, max(seq) AS max_seq, count(*) AS received
            FROM telemetry_readings GROUP BY vehicle_id
        ) s
        WHERE s.vehicle_id = v.id
    """)


def downgrade() -> None:
    op.drop_column('vehicles', 'received_count')
    op.drop_column('vehicles', 'max_seq')
    op.drop_column('vehicles', 'min_seq')