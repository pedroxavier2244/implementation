"""add CNAE column to historico and backup tables

Revision ID: 20260413_000019
Revises: 20260402_000018
Create Date: 2026-04-13

CNAE was added to projetinho_pai but was missing from historico,
historico_arquivo (and its partitions), and projetinho_pai_backup_20260402.
Without this column the upsert archiving step would fail because it builds
the INSERT column list dynamically from projetinho_pai.

NOTE: Already applied directly to production on 2026-04-13.
"""
from alembic import op
import sqlalchemy as sa

revision = "20260413_000019"
down_revision = "000018"
branch_labels = None
depends_on = None

_TABLES = [
    "historico",
    "historico_arquivo",
    "historico_arquivo_2026_03",
    "historico_arquivo_2026_04",
    "historico_arquivo_2026_05",
    "historico_arquivo_2026_06",
    "projetinho_pai_backup_20260402",
]


def upgrade() -> None:
    conn = op.get_bind()
    for table in _TABLES:
        exists = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :t"
            ),
            {"t": table},
        ).fetchone()
        if exists:
            op.execute(
                sa.text(f'ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS "CNAE" text')
            )


def downgrade() -> None:
    conn = op.get_bind()
    for table in _TABLES:
        exists = conn.execute(
            sa.text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :t"
            ),
            {"t": table},
        ).fetchone()
        if exists:
            op.execute(
                sa.text(f'ALTER TABLE public.{table} DROP COLUMN IF EXISTS "CNAE"')
            )
