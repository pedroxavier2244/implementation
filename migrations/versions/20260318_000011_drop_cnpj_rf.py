"""drop cnpj rf columns and tables

Revision ID: 20260318_000011
Revises: 20260316_000010
Create Date: 2026-03-18 00:00:11
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260318_000011"
down_revision: Union[str, None] = "20260316_000010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RF_COLUMNS = [
    "rf_razao_social",
    "rf_natureza_juridica",
    "rf_capital_social",
    "rf_porte_empresa",
    "rf_nome_fantasia",
    "rf_situacao_cadastral",
    "rf_data_inicio_ativ",
    "rf_cnae_principal",
    "rf_uf",
    "rf_municipio",
    "rf_email",
]


def upgrade() -> None:
    for col in RF_COLUMNS:
        op.drop_column("final_visao_cliente", col)

    op.drop_table("cnpj_divergencia")
    op.drop_table("cnpj_rf_cache")


def downgrade() -> None:
    op.create_table(
        "cnpj_rf_cache",
        sa.Column("cnpj", sa.Text(), primary_key=True),
        sa.Column("razao_social", sa.Text()),
        sa.Column("nome_fantasia", sa.Text()),
        sa.Column("situacao_cadastral", sa.Text()),
        sa.Column("descricao_situacao", sa.Text()),
        sa.Column("cnae_fiscal", sa.Text()),
        sa.Column("cnae_descricao", sa.Text()),
        sa.Column("natureza_juridica", sa.Text()),
        sa.Column("capital_social", sa.Text()),
        sa.Column("porte", sa.Text()),
        sa.Column("uf", sa.Text()),
        sa.Column("municipio", sa.Text()),
        sa.Column("email", sa.Text()),
        sa.Column("data_inicio_ativ", sa.Text()),
        sa.Column("last_checked_at", sa.DateTime(timezone=True)),
    )
    op.create_table(
        "cnpj_divergencia",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("job_id", sa.String(36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("cnpj", sa.Text(), nullable=False),
        sa.Column("campo", sa.Text(), nullable=False),
        sa.Column("valor_c6", sa.Text()),
        sa.Column("valor_rf", sa.Text()),
        sa.Column("found_at", sa.DateTime(timezone=True)),
    )
    for col in RF_COLUMNS:
        op.add_column("final_visao_cliente", sa.Column(col, sa.Text(), nullable=True))
