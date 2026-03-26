"""create change history archive and make etl_job_id nullable

Revision ID: 20260326_000016
Revises: 20260320_000015
Create Date: 2026-03-26 00:00:16
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260326_000016"
down_revision: Union[str, None] = "20260320_000015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Torna etl_job_id nullable para suportar MANUAL_EDIT (sem job associado)
    op.alter_column(
        "visao_cliente_change_history",
        "etl_job_id",
        existing_type=sa.String(length=36),
        nullable=True,
        schema="etl",
    )

    # Tabela de arquivo — mesma estrutura, sem FK (registros antigos podem
    # referenciar jobs/files já deletados)
    op.create_table(
        "visao_cliente_change_history_archive",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("documento", sa.Text(), nullable=False),
        sa.Column("etl_job_id", sa.String(length=36), nullable=True),
        sa.Column("file_id", sa.String(length=36), nullable=True),
        sa.Column("data_base", sa.Text(), nullable=True),
        sa.Column("change_type", sa.String(length=20), nullable=False),
        sa.Column("field_name", sa.Text(), nullable=True),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=True),
        schema="etl",
    )
    op.create_index(
        "idx_vc_change_history_archive_documento",
        "visao_cliente_change_history_archive",
        ["documento", "changed_at"],
        schema="etl",
    )


def downgrade() -> None:
    op.drop_index(
        "idx_vc_change_history_archive_documento",
        table_name="visao_cliente_change_history_archive",
        schema="etl",
    )
    op.drop_table("visao_cliente_change_history_archive", schema="etl")
    op.alter_column(
        "visao_cliente_change_history",
        "etl_job_id",
        existing_type=sa.String(length=36),
        nullable=False,
        schema="etl",
    )
