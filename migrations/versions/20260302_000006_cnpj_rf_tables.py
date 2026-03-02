"""create cnpj_rf_cache and cnpj_divergencia tables

Revision ID: 20260302_000006
Revises: 20260302_000005
Create Date: 2026-03-02 00:00:06
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "20260302_000006"
down_revision: Union[str, None] = "20260302_000005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cnpj_rf_cache",
        sa.Column("cnpj",               sa.Text(), primary_key=True),
        sa.Column("razao_social",        sa.Text(), nullable=True),
        sa.Column("nome_fantasia",       sa.Text(), nullable=True),
        sa.Column("situacao_cadastral",  sa.Text(), nullable=True),
        sa.Column("descricao_situacao",  sa.Text(), nullable=True),
        sa.Column("cnae_fiscal",         sa.Text(), nullable=True),
        sa.Column("cnae_descricao",      sa.Text(), nullable=True),
        sa.Column("natureza_juridica",   sa.Text(), nullable=True),
        sa.Column("capital_social",      sa.Text(), nullable=True),
        sa.Column("porte",               sa.Text(), nullable=True),
        sa.Column("uf",                  sa.Text(), nullable=True),
        sa.Column("municipio",           sa.Text(), nullable=True),
        sa.Column("email",               sa.Text(), nullable=True),
        sa.Column("data_inicio_ativ",    sa.Text(), nullable=True),
        sa.Column("last_checked_at",     sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "cnpj_divergencia",
        sa.Column("id",       sa.String(36), primary_key=True),
        sa.Column("job_id",   sa.String(36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("cnpj",     sa.Text(), nullable=False),
        sa.Column("campo",    sa.Text(), nullable=False),
        sa.Column("valor_c6", sa.Text(), nullable=True),
        sa.Column("valor_rf", sa.Text(), nullable=True),
        sa.Column("found_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_cnpj_divergencia_job_id", "cnpj_divergencia", ["job_id"])
    op.create_index("idx_cnpj_divergencia_cnpj",   "cnpj_divergencia", ["cnpj"])


def downgrade() -> None:
    op.drop_index("idx_cnpj_divergencia_cnpj",   table_name="cnpj_divergencia")
    op.drop_index("idx_cnpj_divergencia_job_id", table_name="cnpj_divergencia")
    op.drop_table("cnpj_divergencia")
    op.drop_table("cnpj_rf_cache")
