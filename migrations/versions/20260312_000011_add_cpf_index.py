"""add index on cd_cpf_cnpj_cliente for upsert and backfill performance

Revision ID: 20260312_000011
Revises: 20260312_000010
Create Date: 2026-03-12 00:00:00

Sem este índice o upsert (INSERT ON CONFLICT) e o level_backfill
(UPDATE WHERE EXISTS SELECT) fazem full scan na final_visao_cliente.
CONCURRENTLY permite criar sem travar a tabela em produção.
"""

from typing import Sequence, Union
from alembic import op

revision: str = "20260312_000011"
down_revision: Union[str, None] = "20260312_000010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CONCURRENTLY não pode rodar dentro de uma transação explícita.
    # O Alembic envolve migrations em transação por padrão, então usamos
    # execution_options(isolation_level="AUTOCOMMIT").
    op.execute("COMMIT")
    op.execute(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "idx_final_visao_cliente_cpf_cnpj "
        "ON final_visao_cliente (cd_cpf_cnpj_cliente)"
    )


def downgrade() -> None:
    op.execute("COMMIT")
    op.execute(
        "DROP INDEX CONCURRENTLY IF EXISTS "
        "idx_final_visao_cliente_cpf_cnpj"
    )
