"""add historico_only to etl_job_run

Revision ID: 000018
Revises: 000017
Create Date: 2026-04-02
"""
from alembic import op
import sqlalchemy as sa

revision = "000018"
down_revision = "000017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "etl_job_run",
        sa.Column("historico_only", sa.Boolean(), nullable=True, server_default="false"),
        schema="etl",
    )


def downgrade() -> None:
    op.drop_column("etl_job_run", "historico_only", schema="etl")
