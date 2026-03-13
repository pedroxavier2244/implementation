"""dedupe final table and enforce unique latest row per client

Revision ID: 20260227_000004
Revises: 20260227_000003
Create Date: 2026-02-27 00:00:04
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20260227_000004"
down_revision: Union[str, None] = "20260227_000003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Keep only the most recent data_base per client before enforcing uniqueness.
    op.execute(
        """
        WITH ranked AS (
            SELECT
                ctid,
                ROW_NUMBER() OVER (
                    PARTITION BY cd_cpf_cnpj_cliente
                    ORDER BY data_base DESC NULLS LAST, ctid DESC
                ) AS rn
            FROM final_visao_cliente
            WHERE cd_cpf_cnpj_cliente IS NOT NULL
        )
        DELETE FROM final_visao_cliente f
        USING ranked r
        WHERE f.ctid = r.ctid
          AND r.rn > 1
        """
    )

    op.create_index(
        "uq_final_visao_cliente_cliente",
        "final_visao_cliente",
        ["cd_cpf_cnpj_cliente"],
        unique=True,
        postgresql_where=sa.text("cd_cpf_cnpj_cliente IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_final_visao_cliente_cliente", table_name="final_visao_cliente")
