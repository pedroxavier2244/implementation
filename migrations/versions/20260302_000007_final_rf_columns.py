"""add rf_ columns to final_visao_cliente

Revision ID: 20260302_000007
Revises: 20260302_000006
Create Date: 2026-03-02 00:00:07
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "20260302_000007"
down_revision: Union[str, None] = "20260302_000006"
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
        op.add_column("final_visao_cliente", sa.Column(col, sa.Text(), nullable=True))


def downgrade() -> None:
    for col in reversed(RF_COLUMNS):
        op.drop_column("final_visao_cliente", col)
