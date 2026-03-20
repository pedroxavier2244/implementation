"""Add status_qualificacao column to staging and final visao_cliente tables.

Revision ID: 20260320_000015
Revises: 20260320_000014
Create Date: 2026-03-20
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260320_000015"
down_revision: Union[str, None] = "20260320_000014"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "etl"


def upgrade() -> None:
    op.add_column(
        "staging_visao_cliente",
        sa.Column("status_qualificacao", sa.Text(), nullable=True),
        schema=SCHEMA,
    )
    op.add_column(
        "final_visao_cliente",
        sa.Column("status_qualificacao", sa.Text(), nullable=True),
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_column("final_visao_cliente", "status_qualificacao", schema=SCHEMA)
    op.drop_column("staging_visao_cliente", "status_qualificacao", schema=SCHEMA)
