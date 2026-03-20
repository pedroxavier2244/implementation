import re
import unicodedata

SOURCE_SHEET_NAME = "Visão Cliente"
STAGING_TABLE_NAME = "staging_visao_cliente"
FINAL_TABLE_NAME = "final_visao_cliente"
UPSERT_CONFLICT_COLUMNS = ("cd_cpf_cnpj_cliente",)
UPSERT_CONFLICT_WHERE = "cd_cpf_cnpj_cliente IS NOT NULL"

# Exatamente 108 colunas — espelho fiel da planilha MODELO (RELATORIO.FORMULASS).
# Ordem e nomes normalizados via normalize_column_name().
# Qualquer divergência aqui quebra o pipeline: não adicionar, não remover.
REQUIRED_COLUMNS = [
    # --- Colunas source (1–80): passam direto do arquivo bruto ---
    "data_base",
    "cd_cpf_cnpj_cliente",
    "nome_cliente",
    "tipo_pessoa",
    "cd_cpf_cnpj_parceiro",
    "nome_parceiro",
    "cd_cpf_cnpj_consultor",
    "nome_consultor",
    "uf",
    "cidade",
    "bairro",
    "telefone",
    "telefone_master",
    "email",
    "dt_fundacao_empresa",
    "ramo_atuacao",
    "num_conta",
    "limite_conta",
    "dt_conta_criada",
    "dt_encer_cc",
    "status_cc",
    "conta_ativa_90d",
    "chaves_pix_forte",
    "vl_cash_in_mtd",
    "limite_cartao",
    "limite_alocado_cartao_cdb",
    "dt_entrega_cartao",
    "dt_ativ_cartao_cred",
    "vl_spending_total_mtd",
    "status_pagamento_fatura",
    "fl_propensao_c6pay",
    "tpv_c6pay_potencial",
    "fl_elegivel_venda_c6pay",
    "status_proposta_sf_pay",
    "dt_aprovacao_pay",
    "dt_install_maq",
    "dt_ativacao_pay",
    "c6pay_ativa_30",
    "dt_cancelamento_maq",
    "dt_ult_trans_pay",
    "recebimento",
    "banco_domicilio",
    "tpv_m2",
    "tpv_m1",
    "tpv_m0",
    "faixa_tpv_prometido",
    "fl_propensao_bolcob",
    "tpv_bolcob_potencial",
    "fl_bolcob_cadastrado",
    "dt_prim_liq_bolcob",
    "dt_ult_emissao_bolcob",
    "qtd_bolcob_emtd_mtd",
    "vl_bolcob_emtd_mtd",
    "qtd_bolcob_liq_mtd",
    "vl_bolcob_liq_mtd",
    "volume_antecipado",
    "agenda_disponivel",
    "taxa_antecipacao",
    "vl_saldo_medio_mensalizado",
    "dt_conta_criada_global",
    "vl_cash_in_conta_global_mtd",
    "fl_cash_in_puro",
    "fl_cash_in_boleto",
    "fl_cash_in_setup",
    "fl_cash_in_setup_pix_cnpj",
    "fl_cash_in_setup_cdb_cartao",
    "fl_cash_in_setup_pagamentos",
    "fl_cash_in_setup_deb_auto",
    "mes_ref_comiss",
    "fl_qualificado_comiss",
    "faixa_cash_in",
    "faixa_domicilio",
    "faixa_saldo_medio",
    "faixa_spending",
    "faixa_cash_in_global",
    "criterios_atingidos_comiss",
    "apuracao_comiss",
    "multiplicador",
    "ja_pago_comiss",
    "previsao_comiss",
    # --- Colunas derivadas (81–107): calculadas pelo ETL ---
    "total_tpv",                # TOTAL_TPV
    "status_cartao",            # STATUS_CARTAO
    "status_maq",               # STATUS_MAQ
    "status_bolcbob",           # STATUS_BOLCBOB  (nome exato do modelo)
    "insight_cartao",           # INSIGHT_CARTAO
    "insight_maq",              # INSIGHT_MAQ
    "insight_bolcob",           # INSIGHT_BOLCOB
    "insight_pix_forte",        # INSIGHT_PIX FORTE → normalizado
    "insight_conta_global",     # INSIGHT_CONTA_GLOBAL
    "faixa_maximo",             # FAIXA_MAXIMO
    "faixa_alvo",               # FAIXA_ALVO
    "threshold_cash_in",        # THRESHOLD_CASH_IN
    "threshold_spending",       # THRESHOLD_SPENDING
    "thereshold_saldo_medio",   # THERESHOLD_SALDO_MEDIO (nome exato do modelo)
    "threshold_conta_global",   # THRESHOLD_CONTA_GLOBAL
    "threshold_domicilio",      # THRESHOLD_DOMICILIO
    "gap_cash_in",              # GAP_CASH_IN
    "gap_spending",             # GAP_SPENDING
    "gap_saldo_medio",          # GAP_SALDO_MEDIO
    "gap_conta_global",         # GAP_CONTA_GLOBAL
    "gap_domicilio",            # GAP_DOMICILIO
    "pct_cash_in",              # %_CASH_IN
    "pct_spending",             # %_SPENDING
    "pct_saldo_medio",          # %_SALDO_MEDIO
    "pct_conta_global",         # %_CONTA_GLOBAL
    "maior_progresso_pct",      # MAIOR_PROGRESSO%
    "criterio_proximo",         # CRITERIO_PROXIMO
    "status_qualificacao",      # STATUS_QUALIFICAÇÃO
]


def normalize_column_name(name: str) -> str:
    normalized = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = normalized.replace("%", " pct ").replace("/", " ")
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if normalized and normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized
