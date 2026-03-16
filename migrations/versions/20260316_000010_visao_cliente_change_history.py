"""create visao cliente change history table

Revision ID: 20260316_000010
Revises: 20260313_000009
Create Date: 2026-03-16 00:00:10
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260316_000010"
down_revision: Union[str, None] = "20260313_000009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "visao_cliente_change_history",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("documento", sa.Text(), nullable=False),
        sa.Column("etl_job_id", sa.String(length=36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("file_id", sa.String(length=36), sa.ForeignKey("etl_file.id"), nullable=True),
        sa.Column("data_base", sa.Text(), nullable=True),
        sa.Column("change_type", sa.String(length=20), nullable=False),
        sa.Column("field_name", sa.Text(), nullable=True),
        sa.Column("old_value", sa.Text(), nullable=True),
        sa.Column("new_value", sa.Text(), nullable=True),
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_visao_cliente_change_history_documento_changed_at",
        "visao_cliente_change_history",
        ["documento", "changed_at"],
    )
    op.create_index(
        "idx_visao_cliente_change_history_etl_job_id",
        "visao_cliente_change_history",
        ["etl_job_id"],
    )
    op.create_index(
        "idx_visao_cliente_change_history_file_id",
        "visao_cliente_change_history",
        ["file_id"],
    )


def downgrade() -> None:
    op.drop_index("idx_visao_cliente_change_history_file_id", table_name="visao_cliente_change_history")
    op.drop_index("idx_visao_cliente_change_history_etl_job_id", table_name="visao_cliente_change_history")
    op.drop_index("idx_visao_cliente_change_history_documento_changed_at", table_name="visao_cliente_change_history")
    op.drop_table("visao_cliente_change_history")
