"""create analytics indicator snapshot table

Revision ID: 20260302_000008
Revises: 20260302_000007
Create Date: 2026-03-02 00:00:08
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260302_000008"
down_revision: Union[str, None] = "20260302_000007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "analytics_indicator_snapshot",
        sa.Column("indicator", sa.Text(), nullable=False),
        sa.Column("reference_date", sa.Date(), nullable=False),
        sa.Column("total", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("source_sheet", sa.Text(), nullable=True),
        sa.Column("source_column", sa.Text(), nullable=True),
        sa.Column("job_id", sa.String(length=36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("file_id", sa.String(length=36), sa.ForeignKey("etl_file.id"), nullable=True),
        sa.Column("loaded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.PrimaryKeyConstraint("indicator", "reference_date", name="pk_analytics_indicator_snapshot"),
    )
    op.create_index(
        "ix_analytics_indicator_snapshot_reference_date",
        "analytics_indicator_snapshot",
        ["reference_date"],
    )
    op.create_index(
        "ix_analytics_indicator_snapshot_file_id",
        "analytics_indicator_snapshot",
        ["file_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_analytics_indicator_snapshot_file_id", table_name="analytics_indicator_snapshot")
    op.drop_index("ix_analytics_indicator_snapshot_reference_date", table_name="analytics_indicator_snapshot")
    op.drop_table("analytics_indicator_snapshot")
