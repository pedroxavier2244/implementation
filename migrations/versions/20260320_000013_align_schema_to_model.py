"""Align staging_visao_cliente and final_visao_cliente to the official model (107 cols).

Changes vs current schema:
  ADD   9 new columns: total_tpv, status_cartao, status_maq, status_bolcbob,
                       insight_cartao, insight_maq, insight_bolcob,
                       insight_pix_forte, insight_conta_global
  RENAME faixa_max         → faixa_maximo
  RENAME threshiold_cash_in → threshold_cash_in
  RENAME threshold_saldo_medio → thereshold_saldo_medio
  DROP  19 derived cols no longer in model (nivel_*, safra_*, metrica_*, etc.)

Both tables (staging_visao_cliente, final_visao_cliente) receive identical changes.
Schema: etl (all tables moved there in migration 000012).

Revision ID: 20260320_000013
Revises: 20260318_000012
Create Date: 2026-03-20 00:00:13
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "20260320_000013"
down_revision: Union[str, None] = "20260318_000012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "etl"
TABLES = ("staging_visao_cliente", "final_visao_cliente")

# ------------------------------------------------------------------
# Colunas novas (ADD)
# ------------------------------------------------------------------
NEW_NUMERIC_COLS = [
    "total_tpv",
]

NEW_TEXT_COLS = [
    "status_cartao",
    "status_maq",
    "status_bolcbob",
    "insight_cartao",
    "insight_maq",
    "insight_bolcob",
    "insight_pix_forte",
    "insight_conta_global",
]

# ------------------------------------------------------------------
# Renomeações  old_name → new_name
# ------------------------------------------------------------------
RENAMES = [
    ("faixa_max",           "faixa_maximo"),
    ("threshiold_cash_in",  "threshold_cash_in"),
    ("threshold_saldo_medio", "thereshold_saldo_medio"),
]

# ------------------------------------------------------------------
# Colunas a remover (DROP) — não existem no modelo oficial
# ------------------------------------------------------------------
DROP_COLS = [
    "ja_recebeu_comissao",
    "comissao_prox_mes",
    "status_qualificacao",
    "dias_desde_abertura",
    "m2_dias_faltantes",
    "nivel_cartao",
    "nivel_conta",
    "cancelamento_maq",
    "elegivel_c6",
    "safra_boleto",
    "idade_safra_boleto",
    "safra_maquina",
    "idade_safra_maquina",
    "metrica_ativacao",
    "metrica_progresso",
    "metrica_urgencia",
    "metrica_financeiro",
    "metrica_intencao",
    "score_perfil",
]


def _column_exists(table: str, column: str) -> bool:
    """Verifica se uma coluna existe na tabela (evita erro em re-runs parciais)."""
    result = op.get_bind().execute(
        sa.text(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table AND column_name = :col"
        ),
        {"schema": SCHEMA, "table": table, "col": column},
    )
    return result.fetchone() is not None


def upgrade() -> None:
    for table in TABLES:
        # 1. ADD novas colunas numéricas
        for col in NEW_NUMERIC_COLS:
            if not _column_exists(table, col):
                op.add_column(table, sa.Column(col, sa.Numeric(), nullable=True), schema=SCHEMA)

        # 2. ADD novas colunas texto
        for col in NEW_TEXT_COLS:
            if not _column_exists(table, col):
                op.add_column(table, sa.Column(col, sa.Text(), nullable=True), schema=SCHEMA)

        # 3. RENAME colunas
        for old_name, new_name in RENAMES:
            if _column_exists(table, old_name) and not _column_exists(table, new_name):
                op.alter_column(table, old_name, new_column_name=new_name, schema=SCHEMA)

        # 4. DROP colunas removidas do modelo
        for col in DROP_COLS:
            if _column_exists(table, col):
                op.drop_column(table, col, schema=SCHEMA)


def downgrade() -> None:
    for table in TABLES:
        # 4. Restaura colunas removidas
        text_restore = [
            "cancelamento_maq", "elegivel_c6", "safra_boleto", "idade_safra_boleto",
            "safra_maquina", "idade_safra_maquina",
            "ja_recebeu_comissao", "comissao_prox_mes", "status_qualificacao",
        ]
        for col in text_restore:
            if not _column_exists(table, col):
                op.add_column(table, sa.Column(col, sa.Text(), nullable=True), schema=SCHEMA)

        float_restore = [
            "metrica_ativacao", "metrica_progresso", "metrica_urgencia",
            "metrica_financeiro", "metrica_intencao", "score_perfil",
        ]
        for col in float_restore:
            if not _column_exists(table, col):
                op.add_column(table, sa.Column(col, sa.Float(), nullable=True), schema=SCHEMA)

        int_restore = ["dias_desde_abertura", "m2_dias_faltantes"]
        for col in int_restore:
            if not _column_exists(table, col):
                op.add_column(table, sa.Column(col, sa.Integer(), nullable=True), schema=SCHEMA)

        text_nivel = ["nivel_cartao", "nivel_conta"]
        for col in text_nivel:
            if not _column_exists(table, col):
                op.add_column(table, sa.Column(col, sa.Text(), nullable=True), schema=SCHEMA)

        # 3. Reverte renomeações
        for old_name, new_name in reversed(RENAMES):
            if _column_exists(table, new_name) and not _column_exists(table, old_name):
                op.alter_column(table, new_name, new_column_name=old_name, schema=SCHEMA)

        # 2. Remove colunas texto novas
        for col in reversed(NEW_TEXT_COLS):
            if _column_exists(table, col):
                op.drop_column(table, col, schema=SCHEMA)

        # 1. Remove colunas numéricas novas
        for col in reversed(NEW_NUMERIC_COLS):
            if _column_exists(table, col):
                op.drop_column(table, col, schema=SCHEMA)
