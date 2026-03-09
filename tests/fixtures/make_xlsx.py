"""
Gera um arquivo .xlsx mínimo válido para o pipeline ETL.
Inclui apenas a aba "Visão Cliente".
"""
import io
from datetime import date

import openpyxl

# Colunas obrigatórias do schema (já no formato normalizado → usar como header)
VISAO_CLIENTE_HEADERS = [
    "data_base", "cd_cpf_cnpj_cliente", "nome_cliente", "tipo_pessoa",
    "cd_cpf_cnpj_parceiro", "nome_parceiro", "cd_cpf_cnpj_consultor",
    "nome_consultor", "uf", "cidade", "bairro", "telefone", "telefone_master",
    "email", "dt_fundacao_empresa", "ramo_atuacao", "num_conta", "limite_conta",
    "dt_conta_criada", "dt_encer_cc", "status_cc", "conta_ativa_90d",
    "chaves_pix_forte", "vl_cash_in_mtd", "limite_cartao",
    "limite_alocado_cartao_cdb", "dt_entrega_cartao", "dt_ativ_cartao_cred",
    "vl_spending_total_mtd", "status_pagamento_fatura", "fl_propensao_c6pay",
    "tpv_c6pay_potencial", "fl_elegivel_venda_c6pay", "status_proposta_sf_pay",
    "dt_aprovacao_pay", "dt_install_maq", "dt_ativacao_pay", "c6pay_ativa_30",
    "dt_cancelamento_maq", "dt_ult_trans_pay", "recebimento", "banco_domicilio",
    "tpv_m2", "tpv_m1", "tpv_m0", "faixa_tpv_prometido", "fl_propensao_bolcob",
    "tpv_bolcob_potencial", "fl_bolcob_cadastrado", "dt_prim_liq_bolcob",
    "dt_ult_emissao_bolcob", "qtd_bolcob_emtd_mtd", "vl_bolcob_emtd_mtd",
    "qtd_bolcob_liq_mtd", "vl_bolcob_liq_mtd", "volume_antecipado",
    "agenda_disponivel", "taxa_antecipacao", "vl_saldo_medio_mensalizado",
    "dt_conta_criada_global", "vl_cash_in_conta_global_mtd", "fl_cash_in_puro",
    "fl_cash_in_boleto", "fl_cash_in_setup", "fl_cash_in_setup_pix_cnpj",
    "fl_cash_in_setup_cdb_cartao", "fl_cash_in_setup_pagamentos",
    "fl_cash_in_setup_deb_auto", "mes_ref_comiss", "fl_qualificado_comiss",
    "faixa_cash_in", "faixa_domicilio", "faixa_saldo_medio", "faixa_spending",
    "faixa_cash_in_global", "criterios_atingidos_comiss", "apuracao_comiss",
    "multiplicador", "ja_pago_comiss", "previsao_comiss", "faixa_max",
    "faixa_alvo", "threshiold_cash_in", "threshold_spending",
    "threshold_saldo_medio", "threshold_conta_global", "threshold_domicilio",
    "gap_cash_in", "gap_spending", "gap_saldo_medio", "gap_conta_global",
    "gap_domicilio", "pct_cash_in", "pct_spending", "pct_saldo_medio",
    "pct_conta_global", "maior_progresso_pct", "criterio_proximo",
    "ja_recebeu_comissao", "comissao_prox_mes", "status_qualificacao",
    "dias_desde_abertura", "m2_dias_faltantes", "nivel_cartao", "nivel_conta",
]

# CNPJ de teste (14 dígitos, fictício)
TEST_CNPJ = "12345678000195"
TEST_FILENAME = "Relatorio de Producao - 21.02.26.xlsx"


def _row_values(reference_date: date) -> list:
    """Retorna uma linha de dados mínimos válidos."""
    row = []
    for col in VISAO_CLIENTE_HEADERS:
        if col == "data_base":
            row.append(reference_date)
        elif col == "cd_cpf_cnpj_cliente":
            row.append(TEST_CNPJ)
        elif col == "nome_cliente":
            row.append("Empresa Teste LTDA")
        elif col == "tipo_pessoa":
            row.append("PJ")
        elif col == "uf":
            row.append("SP")
        elif col == "cidade":
            row.append("São Paulo")
        elif col == "nivel_cartao":
            row.append("sem_cartao")
        elif col == "nivel_conta":
            row.append("sem_conta")
        elif col in ("status_cc",):
            row.append("ATIVA")
        elif col in ("status_qualificacao",):
            row.append("NAO_QUALIFICADO")
        elif col in ("ja_recebeu_comissao", "comissao_prox_mes"):
            row.append("NAO")
        elif col.startswith("vl_") or col.startswith("tpv_") or col.startswith("gap_") \
                or col.startswith("pct_") or col.startswith("threshold") \
                or col.startswith("threshiold") or col in (
                    "limite_conta", "limite_cartao", "limite_alocado_cartao_cdb",
                    "volume_antecipado", "agenda_disponivel", "taxa_antecipacao",
                    "vl_saldo_medio_mensalizado", "maior_progresso_pct",
                    "multiplicador", "previsao_comiss", "apuracao_comiss",
                    "faixa_max", "faixa_alvo",
                ):
            row.append(0.0)
        elif col.startswith("qtd_") or col in ("chaves_pix_forte", "dias_desde_abertura", "m2_dias_faltantes"):
            row.append(0)
        elif col.startswith("fl_") or col in (
                "conta_ativa_90d", "c6pay_ativa_30", "fl_bolcob_cadastrado",
                "fl_cash_in_puro", "fl_cash_in_boleto", "fl_cash_in_setup",
                "fl_cash_in_setup_pix_cnpj", "fl_cash_in_setup_cdb_cartao",
                "fl_cash_in_setup_pagamentos", "fl_cash_in_setup_deb_auto",
                "fl_qualificado_comiss", "ja_pago_comiss",
        ):
            row.append(0)
        else:
            row.append(None)
    return row


def make_test_xlsx(reference_date: date | None = None) -> bytes:
    """Gera bytes de um xlsx válido com a aba Visão Cliente."""
    if reference_date is None:
        reference_date = date(2026, 2, 21)

    wb = openpyxl.Workbook()

    # ── Aba: Visão Cliente ────────────────────────────────────────────────────
    ws_vc = wb.active
    ws_vc.title = "Visão Cliente"
    ws_vc.append(VISAO_CLIENTE_HEADERS)
    ws_vc.append(_row_values(reference_date))

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
