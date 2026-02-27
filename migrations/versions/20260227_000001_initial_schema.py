"""initial schema

Revision ID: 20260227_000001
Revises:
Create Date: 2026-02-27 00:00:01
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260227_000001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "etl_file",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("file_date", sa.Date(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("filename", sa.Text(), nullable=True),
        sa.Column("hash_sha256", sa.String(length=64), nullable=False),
        sa.Column("minio_path", sa.Text(), nullable=True),
        sa.Column("downloaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_valid", sa.Boolean(), nullable=True),
        sa.Column("is_processed", sa.Boolean(), nullable=True),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.UniqueConstraint("file_date", "hash_sha256", name="uq_file_date_hash"),
    )

    op.create_table(
        "etl_job_run",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("file_id", sa.String(length=36), sa.ForeignKey("etl_file.id"), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("triggered_by", sa.String(length=20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rows_total", sa.Integer(), nullable=True),
        sa.Column("rows_ok", sa.Integer(), nullable=True),
        sa.Column("rows_bad", sa.Integer(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=True),
        sa.Column("max_retries", sa.Integer(), nullable=True),
        sa.Column("last_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )

    op.create_table(
        "etl_job_step",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("job_id", sa.String(length=36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("step_name", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )

    op.create_table(
        "etl_bad_rows",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("job_id", sa.String(length=36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=True),
        sa.Column("raw_data", sa.JSON(), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "alert_event",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("dedup_key", sa.Text(), nullable=False),
        sa.Column("event_type", sa.String(length=30), nullable=False),
        sa.Column("severity", sa.String(length=10), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("dedup_key"),
    )

    op.create_table(
        "alert_event_channel",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("alert_id", sa.String(length=36), sa.ForeignKey("alert_event.id"), nullable=False),
        sa.Column("channel", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=True),
        sa.Column("max_retries", sa.Integer(), nullable=True),
        sa.Column("last_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("idx_job_status", "etl_job_run", ["status"])
    op.create_index("idx_job_file_id", "etl_job_run", ["file_id"])
    op.create_index("idx_alert_severity", "alert_event", ["severity"])
    op.create_index("idx_file_date", "etl_file", ["file_date"])


def downgrade() -> None:
    op.drop_index("idx_file_date", table_name="etl_file")
    op.drop_index("idx_alert_severity", table_name="alert_event")
    op.drop_index("idx_job_file_id", table_name="etl_job_run")
    op.drop_index("idx_job_status", table_name="etl_job_run")
    op.drop_table("alert_event_channel")
    op.drop_table("alert_event")
    op.drop_table("etl_bad_rows")
    op.drop_table("etl_job_step")
    op.drop_table("etl_job_run")
    op.drop_table("etl_file")
