"""add metrica e score_perfil columns to visao cliente tables

Revision ID: 20260312_000010
Revises: 20260310_000009
Create Date: 2026-03-12 00:00:00

Adiciona as 6 colunas de métricas e score composto derivadas pelo ETL em enrich.py:
  - metrica_ativacao   : score baseado em ja_pago_comiss  (Float)
  - metrica_progresso  : maior_progresso_pct * 0.35       (Float)
  - metrica_urgencia   : score baseado em mes_ref/progresso/dias (Float)
  - metrica_financeiro : score baseado em limites cartao/conta   (Float)
  - metrica_intencao   : score baseado em chaves_pix_forte       (Float)
  - score_perfil       : MIN(soma das 5 métricas, 1)             (Float)
"""

from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "20260312_000010"
down_revision: Union[str, None] = "20260310_000009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

NEW_COLS = [
    "metrica_ativacao",
    "metrica_progresso",
    "metrica_urgencia",
    "metrica_financeiro",
    "metrica_intencao",
    "score_perfil",
]


def upgrade() -> None:
    for col in NEW_COLS:
        op.add_column("staging_visao_cliente", sa.Column(col, sa.Float(), nullable=True))
    for col in NEW_COLS:
        op.add_column("final_visao_cliente", sa.Column(col, sa.Float(), nullable=True))


def downgrade() -> None:
    for col in reversed(NEW_COLS):
        op.drop_column("final_visao_cliente", col)
    for col in reversed(NEW_COLS):
        op.drop_column("staging_visao_cliente", col)
