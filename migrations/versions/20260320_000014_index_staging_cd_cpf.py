"""Add index on staging_visao_cliente.cd_cpf_cnpj_cliente for upsert join performance.

Revision ID: 20260320_000014
Revises: 20260320_000013
Create Date: 2026-03-20
"""

from typing import Sequence, Union

from alembic import op

revision: str = "20260320_000014"
down_revision: Union[str, None] = "20260320_000013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "etl"


def upgrade() -> None:
    op.create_index(
        "ix_staging_visao_cliente_cd_cpf",
        "staging_visao_cliente",
        ["cd_cpf_cnpj_cliente"],
        schema=SCHEMA,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_staging_visao_cliente_cd_cpf",
        table_name="staging_visao_cliente",
        schema=SCHEMA,
    )
