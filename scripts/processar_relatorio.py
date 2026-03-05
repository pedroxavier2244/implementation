"""
Script standalone: adiciona colunas calculadas a um Relatorio de Producao C6 Bank.

Uso:
    python scripts/processar_relatorio.py <caminho_do_xlsx>

Saida:
    <mesmo_diretorio>/<nome_original> (processado).xlsx
"""
import sys
import os
from pathlib import Path

import pandas as pd
import numpy as np

# Adiciona raiz do projeto ao path para importar worker/shared
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared.visao_cliente_schema import normalize_column_name
from worker.steps.enrich import (
    _compute_levels,
    _compute_gap_columns,
    _compute_status_columns,
    _compute_day_metrics,
)

SHEET_NAME = "Visão Cliente"

OUTPUT_RENAME = {
    "faixa_max":              "FAIXA_MAX",
    "faixa_alvo":             "FAIXA_ALVO",
    "threshiold_cash_in":     "THRESHIOLD_CASH_IN",
    "threshold_spending":     "THRESHOLD_SPENDING",
    "threshold_saldo_medio":  "THRESHOLD_SALDO_MEDIO",
    "threshold_conta_global": "THRESHOLD_CONTA_GLOBAL",
    "threshold_domicilio":    "THRESHOLD_DOMICILIO",
    "gap_cash_in":            "GAP_CASH_IN",
    "gap_spending":           "GAP_SPENDING",
    "gap_saldo_medio":        "GAP_SALDO_MEDIO",
    "gap_conta_global":       "GAP_CONTA_GLOBAL",
    "gap_domicilio":          "GAP_DOMICILIO",
    "pct_cash_in":            "%_CASH_IN",
    "pct_spending":           "%_SPENDING",
    "pct_saldo_medio":        "%_SALDO_MEDIO",
    "pct_conta_global":       "%_CONTA_GLOBAL",
    "maior_progresso_pct":    "MAIOR_PROGRESSO%",
    "criterio_proximo":       "CRITERIO_PROXIMO",
    "ja_recebeu_comissao":    "JA_RECEBEU_COMISSAO",
    "comissao_prox_mes":      "COMISSAO_PROX_MES",
    "status_qualificacao":    "STATUS_QUALIFICAÇÃO",
    "dias_desde_abertura":    "DIAS_DESDE_ABERTURA",
    "m2_dias_faltantes":      "M2_DIAS_FALTANTES",
    "nivel_cartao":           "NIVEL_CARTAO",
    "nivel_conta":            "NIVEL_CONTA",
}

ORIGINAL_COLS_UPPERCASE = [
    "DATA_BASE", "CD_CPF_CNPJ_CLIENTE", "NOME_CLIENTE", "TIPO_PESSOA",
    "CD_CPF_CNPJ_PARCEIRO", "NOME_PARCEIRO", "CD_CPF_CNPJ_CONSULTOR", "NOME_CONSULTOR",
    "UF", "CIDADE", "BAIRRO", "TELEFONE", "TELEFONE_MASTER", "EMAIL",
    "DT_FUNDACAO_EMPRESA", "RAMO_ATUACAO", "NUM_CONTA", "LIMITE_CONTA",
    "DT_CONTA_CRIADA", "DT_ENCER_CC", "STATUS_CC", "CONTA_ATIVA_90D",
    "CHAVES_PIX_FORTE", "VL_CASH_IN_MTD", "LIMITE_CARTAO", "LIMITE_ALOCADO_CARTAO_CDB",
    "DT_ENTREGA_CARTAO", "DT_ATIV_CARTAO_CRED", "VL_SPENDING_TOTAL_MTD",
    "STATUS_PAGAMENTO_FATURA", "FL_PROPENSAO_C6PAY", "TPV_C6PAY_POTENCIAL",
    "FL_ELEGIVEL_VENDA_C6PAY", "STATUS_PROPOSTA_SF_PAY", "DT_APROVACAO_PAY",
    "DT_INSTALL_MAQ", "DT_ATIVACAO_PAY", "C6PAY_ATIVA_30", "DT_CANCELAMENTO_MAQ",
    "DT_ULT_TRANS_PAY", "RECEBIMENTO", "BANCO_DOMICILIO", "TPV_M2", "TPV_M1", "TPV_M0",
    "FAIXA_TPV_PROMETIDO", "FL_PROPENSAO_BOLCOB", "TPV_BOLCOB_POTENCIAL",
    "FL_BOLCOB_CADASTRADO", "DT_PRIM_LIQ_BOLCOB", "DT_ULT_EMISSAO_BOLCOB",
    "QTD_BOLCOB_EMTD_MTD", "VL_BOLCOB_EMTD_MTD", "QTD_BOLCOB_LIQ_MTD",
    "VL_BOLCOB_LIQ_MTD", "VOLUME_ANTECIPADO", "AGENDA_DISPONIVEL", "TAXA_ANTECIPACAO",
    "VL_SALDO_MEDIO_MENSALIZADO", "DT_CONTA_CRIADA_GLOBAL", "VL_CASH_IN_CONTA_GLOBAL_MTD",
    "FL_CASH_IN_PURO", "FL_CASH_IN_BOLETO", "FL_CASH_IN_SETUP", "FL_CASH_IN_SETUP_PIX_CNPJ",
    "FL_CASH_IN_SETUP_CDB_CARTAO", "FL_CASH_IN_SETUP_PAGAMENTOS", "FL_CASH_IN_SETUP_DEB_AUTO",
    "MES_REF_COMISS", "FL_QUALIFICADO_COMISS", "FAIXA_CASH_IN", "FAIXA_DOMICILIO",
    "FAIXA_SALDO_MEDIO", "FAIXA_SPENDING", "FAIXA_CASH_IN_GLOBAL",
    "CRITERIOS_ATINGIDOS_COMISS", "APURACAO_COMISS", "MULTIPLICADOR",
    "JA_PAGO_COMISS", "PREVISAO_COMISS",
]


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [normalize_column_name(c) for c in df.columns]
    return df


def _restore_original_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        if col in OUTPUT_RENAME:
            rename_map[col] = OUTPUT_RENAME[col]
        else:
            upper = col.upper()
            if upper in ORIGINAL_COLS_UPPERCASE:
                rename_map[col] = upper
    return df.rename(columns=rename_map)


def processar(input_path: str) -> str:
    input_path = Path(input_path)
    print(f"Lendo: {input_path.name}")

    df = pd.read_excel(input_path, sheet_name=SHEET_NAME)
    print(f"  {len(df):,} linhas x {len(df.columns)} colunas originais")

    # Remove colunas calculadas existentes para recalcular
    cols_to_drop = [c for c in df.columns if c in list(OUTPUT_RENAME.values())]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)
        print(f"  Removendo {len(cols_to_drop)} colunas existentes para recalcular")

    df = _normalize_columns(df)

    required_input = [
        "faixa_cash_in", "faixa_domicilio", "faixa_saldo_medio",
        "faixa_spending", "faixa_cash_in_global",
        "vl_cash_in_mtd", "vl_spending_total_mtd",
        "vl_saldo_medio_mensalizado", "vl_cash_in_conta_global_mtd",
        "ja_pago_comiss", "previsao_comiss",
        "limite_cartao", "limite_conta",
        "data_base", "dt_conta_criada",
    ]
    missing = [c for c in required_input if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatorias ausentes: {missing}")

    print("  Calculando colunas derivadas...")
    _compute_levels(df)
    _compute_gap_columns(df)
    _compute_status_columns(df)
    _compute_day_metrics(df)

    df = _restore_original_columns(df)

    added = [c for c in df.columns if c in list(OUTPUT_RENAME.values())]
    print(f"  {len(added)} colunas adicionadas: {added}")
    print(f"  Total: {len(df.columns)} colunas")

    stem = input_path.stem
    output_path = input_path.parent / f"{stem} (processado).xlsx"
    print(f"Salvando: {output_path.name}")
    df.to_excel(output_path, sheet_name=SHEET_NAME, index=False)
    print(f"Concluido: {output_path}")
    return str(output_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python scripts/processar_relatorio.py <caminho_do_xlsx>")
        sys.exit(1)
    processar(sys.argv[1])
