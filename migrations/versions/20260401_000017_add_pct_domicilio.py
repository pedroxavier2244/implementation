"""add pct_domicilio column to visao_cliente tables

Revision ID: 000017
Revises: 000016
Create Date: 2026-04-01
"""
from alembic import op
import sqlalchemy as sa

revision = "000017"
down_revision = "000016"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add pct_domicilio to final_visao_cliente
    op.add_column(
        "final_visao_cliente",
        sa.Column("pct_domicilio", sa.Numeric, nullable=True),
        schema="etl",
    )
    # Add pct_domicilio to staging_visao_cliente
    op.add_column(
        "staging_visao_cliente",
        sa.Column("pct_domicilio", sa.Numeric, nullable=True),
        schema="etl",
    )


def downgrade() -> None:
    op.drop_column("final_visao_cliente", "pct_domicilio", schema="etl")
    op.drop_column("staging_visao_cliente", "pct_domicilio", schema="etl")
