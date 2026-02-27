"""add lookup index for final visao cliente by documento

Revision ID: 20260227_000003
Revises: 20260227_000002
Create Date: 2026-02-27 00:00:03
"""

from typing import Sequence, Union

from alembic import op


revision: str = "20260227_000003"
down_revision: Union[str, None] = "20260227_000002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_final_visao_cliente_doc_data_base",
        "final_visao_cliente",
        ["cd_cpf_cnpj_cliente", "data_base"],
    )


def downgrade() -> None:
    op.drop_index("ix_final_visao_cliente_doc_data_base", table_name="final_visao_cliente")
