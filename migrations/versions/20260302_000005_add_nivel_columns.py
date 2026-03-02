"""add nivel columns to visao cliente tables

Revision ID: 20260302_000005
Revises: 20260227_000004
Create Date: 2026-03-02 10:55:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260302_000005"
down_revision: Union[str, None] = "20260227_000004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("staging_visao_cliente", sa.Column("nivel_cartao", sa.Text(), nullable=True))
    op.add_column("staging_visao_cliente", sa.Column("nivel_conta", sa.Text(), nullable=True))

    op.add_column("final_visao_cliente", sa.Column("nivel_cartao", sa.Text(), nullable=True))
    op.add_column("final_visao_cliente", sa.Column("nivel_conta", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("final_visao_cliente", "nivel_conta")
    op.drop_column("final_visao_cliente", "nivel_cartao")

    op.drop_column("staging_visao_cliente", "nivel_conta")
    op.drop_column("staging_visao_cliente", "nivel_cartao")
