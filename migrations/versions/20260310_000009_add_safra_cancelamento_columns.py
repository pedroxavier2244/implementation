"""add safra and cancelamento columns to visao cliente tables

Revision ID: 20260310_000009
Revises: 20260302_000008
Create Date: 2026-03-10 00:00:00
"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "20260310_000009"
down_revision: Union[str, None] = "20260302_000008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

NEW_COLS = [
    "cancelamento_maq",
    "elegivel_c6",
    "safra_boleto",
    "idade_safra_boleto",
    "safra_maquina",
    "idade_safra_maquina",
]


def upgrade() -> None:
    for col in NEW_COLS:
        op.add_column("staging_visao_cliente", sa.Column(col, sa.Text(), nullable=True))
    for col in NEW_COLS:
        op.add_column("final_visao_cliente", sa.Column(col, sa.Text(), nullable=True))


def downgrade() -> None:
    for col in reversed(NEW_COLS):
        op.drop_column("final_visao_cliente", col)
    for col in reversed(NEW_COLS):
        op.drop_column("staging_visao_cliente", col)
