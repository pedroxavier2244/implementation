import logging

from sqlalchemy.orm import Session

from shared.visao_cliente_schema import REQUIRED_COLUMNS
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe, set_cached_dataframe

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers de coerção de tipos
# ---------------------------------------------------------------------------

def _coerce_numeric(series):
    import pandas as pd

    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.replace({"": None, "nan": None, "none": None, "nat": None, "None": None})
    cleaned = cleaned.str.replace(r"[^0-9,.\-]", "", regex=True)

    has_comma = cleaned.str.contains(",", na=False)
    has_dot = cleaned.str.contains(r"\.", na=False)

    mask_brl = has_comma & has_dot
    cleaned.loc[mask_brl] = (
        cleaned.loc[mask_brl].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    )

    mask_only_comma = has_comma & ~has_dot
    cleaned.loc[mask_only_comma] = cleaned.loc[mask_only_comma].str.replace(",", ".", regex=False)

    cleaned = cleaned.replace({"": None, "-": None, ".": None, ",": None, "-.": None})
    return pd.to_numeric(cleaned, errors="coerce")


def _coerce_datetime(series):
    import pandas as pd

    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, errors="coerce")

    numeric = pd.to_numeric(series, errors="coerce")
    from_excel_serial = pd.to_datetime(numeric, errors="coerce", unit="D", origin="1899-12-30")
    from_strings = pd.to_datetime(series, errors="coerce", dayfirst=True)
    return from_strings.fillna(from_excel_serial)


def _fmt_brl(value) -> str:
    """Formata um valor numérico como moeda BRL: R$ 1.234,56"""
    import pandas as pd

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "R$ 0,00"
    s = f"{float(value):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


# ---------------------------------------------------------------------------
# Colunas derivadas: total_tpv
# ---------------------------------------------------------------------------

def _compute_total_tpv(dataframe) -> None:
    """TOTAL_TPV = TPV_M0 + TPV_M1 + TPV_M2"""
    m0 = _coerce_numeric(dataframe["tpv_m0"]).fillna(0)
    m1 = _coerce_numeric(dataframe["tpv_m1"]).fillna(0)
    m2 = _coerce_numeric(dataframe["tpv_m2"]).fillna(0)
    dataframe["total_tpv"] = m0 + m1 + m2


# ---------------------------------------------------------------------------
# Colunas derivadas: status_cartao
# ---------------------------------------------------------------------------

def _compute_status_cartao(dataframe) -> None:
    """
    STATUS_CARTAO — 10 categorias baseadas em limite, ativação e spending.

    Lógica (por prioridade, espelho do IFS da planilha modelo):
      NAO POSSUI CARTAO                                  : sem limite, sem entrega, sem ativação, sem spending
      DEBITO - UTILIZANDO                                : sem limite, com entrega, sem ativação, com spending
      DEBITO - NAO UTILIZANDO                            : sem limite, com entrega, sem ativação, sem spending
      NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (SEM CDB)  : tem limite, sem ativação, sem spending, sem CDB
      NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (COM CDB)  : tem limite, sem ativação, sem spending, com CDB
      NAO ATIVOU CREDITO - UTILIZA DEBITO (SEM CDB)      : tem limite, sem ativação, com spending, sem CDB
      NAO ATIVOU CREDITO - UTILIZA DEBITO (COM CDB)      : tem limite, sem ativação, com spending, com CDB
      ATIVOU CREDITO - UTILIZANDO                        : tem limite, com ativação, com spending
      ATIVOU CREDITO - NAO UTILIZANDO                    : tem limite, com ativação, sem spending
      VALIDAR                                            : demais casos
    """
    import numpy as np

    dt_entrega = _coerce_datetime(dataframe["dt_entrega_cartao"])
    dt_ativ = _coerce_datetime(dataframe["dt_ativ_cartao_cred"])
    lim_cartao = _coerce_numeric(dataframe["limite_cartao"]).fillna(0)
    lim_aloc = _coerce_numeric(dataframe["limite_alocado_cartao_cdb"]).fillna(0)
    spending = _coerce_numeric(dataframe["vl_spending_total_mtd"]).fillna(0)

    has_entrega = ~dt_entrega.isna()
    has_ativ = ~dt_ativ.isna()
    has_credit = lim_cartao > 0
    has_cdb = lim_aloc > 0
    is_spending = spending > 0

    dataframe["status_cartao"] = np.select(
        [
            ~has_credit & ~has_cdb & ~has_entrega & ~has_ativ & ~is_spending,
            ~has_credit & has_entrega & ~has_ativ & is_spending,
            ~has_credit & has_entrega & ~has_ativ & ~is_spending,
            has_credit & ~has_ativ & ~is_spending & ~has_cdb,
            has_credit & ~has_ativ & ~is_spending & has_cdb,
            has_credit & ~has_ativ & is_spending & ~has_cdb,
            has_credit & ~has_ativ & is_spending & has_cdb,
            has_credit & has_ativ & is_spending,
            has_credit & has_ativ & ~is_spending,
        ],
        [
            "NAO POSSUI CARTAO",
            "DEBITO - UTILIZANDO",
            "DEBITO - NAO UTILIZANDO",
            "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (SEM CDB)",
            "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (COM CDB)",
            "NAO ATIVOU CREDITO - UTILIZA DEBITO (SEM CDB)",
            "NAO ATIVOU CREDITO - UTILIZA DEBITO (COM CDB)",
            "ATIVOU CREDITO - UTILIZANDO",
            "ATIVOU CREDITO - NAO UTILIZANDO",
        ],
        default="VALIDAR",
    )


# ---------------------------------------------------------------------------
# Colunas derivadas: status_maq
# ---------------------------------------------------------------------------

def _compute_status_maq(dataframe) -> None:
    """
    STATUS_MAQ — 10 categorias baseadas em proposta, instalação, ativação, cancelamento e TPV.

    Depende de total_tpv (deve ser calculado antes).
    """
    import numpy as np

    status_proposta = dataframe["status_proposta_sf_pay"].astype(str).str.strip()
    fl_eleg = _coerce_numeric(dataframe["fl_elegivel_venda_c6pay"]).fillna(0)
    dt_install = _coerce_datetime(dataframe["dt_install_maq"])
    dt_ativ = _coerce_datetime(dataframe["dt_ativacao_pay"])
    c6pay_30 = _coerce_numeric(dataframe["c6pay_ativa_30"]).fillna(0)
    dt_cancel = _coerce_datetime(dataframe["dt_cancelamento_maq"])
    total_tpv = _coerce_numeric(dataframe["total_tpv"]).fillna(0)

    is_em_analise = status_proposta == "EM ANALISE C6 | AGUARDANDO APROVACAO DO CLIENTE"
    has_install = ~dt_install.isna()
    has_ativ = ~dt_ativ.isna()
    is_eleg = fl_eleg == 1
    is_active_30 = c6pay_30 == 1
    has_cancel = ~dt_cancel.isna()
    has_tpv = total_tpv > 0

    dataframe["status_maq"] = np.select(
        [
            is_em_analise,
            has_install & has_ativ & has_cancel & has_tpv,
            has_install & ~has_ativ & has_cancel & ~has_tpv,
            has_install & ~has_ativ & ~has_cancel & has_tpv,
            has_install & has_ativ & ~is_active_30 & ~has_cancel & has_tpv,
            has_install & has_ativ & is_active_30 & ~has_cancel & has_tpv,
            has_install & ~has_ativ & ~is_active_30 & ~has_cancel,
            is_eleg & ~has_install & ~has_ativ & ~is_active_30 & ~has_cancel,
            ~is_eleg & ~has_install & ~has_ativ & ~is_active_30 & ~has_cancel & ~has_tpv,
        ],
        [
            "PEDIDO EM ANALISE",
            "CANCELADA - COM TPV",
            "CANCELADA",
            "INSTALADA - COM TPV SEM ATIVAR",
            "ATIVA - INATIVA 30D",
            "ATIVA - TRANSACIONANDO",
            "INSTALADA - NAO ATIVADA",
            "ELEGIVEL - SEM VENDA",
            "NAO ELEGIVEL",
        ],
        default="VERIFICAR",
    )


# ---------------------------------------------------------------------------
# Colunas derivadas: status_bolcbob
# ---------------------------------------------------------------------------

def _compute_status_bolcob(dataframe) -> None:
    """
    STATUS_BOLCBOB — 5 categorias baseadas em cadastro, emissão e liquidação.

    Lógica (espelho do IFS da planilha modelo):
      SEM BOLETO CADASTRADO           : fl_bolcob = 0
      BOLETO CADASTRADO - NUNCA EMITIDO : fl_bolcob = 1, sem liquidação prévia
      BOLETO EMITIDO MAS NAO LIQUIDADO  : fl_bolcob = 1, com liquidação, sem liquidação no mês
      ATIVO - UTILIZANDO              : fl_bolcob = 1, com liquidação no mês
      VERIFICAR                       : demais casos
    """
    import numpy as np

    fl_bolcob = _coerce_numeric(dataframe["fl_bolcob_cadastrado"]).fillna(0)
    dt_prim = _coerce_datetime(dataframe["dt_prim_liq_bolcob"])
    qtd_liq_mtd = _coerce_numeric(dataframe["qtd_bolcob_liq_mtd"]).fillna(0)

    is_cadastrado = fl_bolcob == 1
    has_prim = ~dt_prim.isna()
    has_liq_mtd = qtd_liq_mtd > 0

    dataframe["status_bolcbob"] = np.select(
        [
            ~is_cadastrado,
            is_cadastrado & ~has_prim,
            is_cadastrado & has_prim & ~has_liq_mtd,
            is_cadastrado & has_prim & has_liq_mtd,
        ],
        [
            "SEM BOLETO CADASTRADO",
            "BOLETO CADASTRADO - NUNCA EMITIDO",
            "BOLETO EMITIDO MAS NAO LIQUIDADO",
            "ATIVO - UTILIZANDO",
        ],
        default="VERIFICAR",
    )


# ---------------------------------------------------------------------------
# Colunas derivadas: insight_* (5 colunas de texto)
# ---------------------------------------------------------------------------

def _compute_insight_columns(dataframe) -> None:
    """
    Gera as 5 colunas de insight textual:
      insight_cartao, insight_maq, insight_bolcob,
      insight_pix_forte, insight_conta_global
    """
    import pandas as pd

    def _fmt_date(series):
        dt = _coerce_datetime(series)
        return dt.dt.strftime("%d/%m/%Y").where(~dt.isna(), "")

    def _fmt_series_brl(series):
        return _coerce_numeric(series).map(_fmt_brl)

    # ------------------------------------------------------------------ #
    # insight_cartao
    # ------------------------------------------------------------------ #
    sc = dataframe["status_cartao"].astype(str)
    fmt_entrega = _fmt_date(dataframe["dt_entrega_cartao"])
    fmt_ativ_cred = _fmt_date(dataframe["dt_ativ_cartao_cred"])
    fmt_lim = _fmt_series_brl(dataframe["limite_cartao"])
    fmt_aloc = _fmt_series_brl(dataframe["limite_alocado_cartao_cdb"])
    fmt_spending = _fmt_series_brl(dataframe["vl_spending_total_mtd"])

    insight_cartao = pd.Series("Validar situação do cartão.", index=dataframe.index, dtype=object)
    insight_cartao = insight_cartao.where(
        sc != "NAO POSSUI CARTAO",
        "Cliente sem cartão C6.",
    )
    insight_cartao = insight_cartao.where(
        sc != "DEBITO - NAO UTILIZANDO",
        "Cartão de débito entregue em " + fmt_entrega + ", sem uso no mês.",
    )
    insight_cartao = insight_cartao.where(
        sc != "DEBITO - UTILIZANDO",
        "Cartão de débito entregue em " + fmt_entrega + ", utilizando " + fmt_spending + " no mês.",
    )
    insight_cartao = insight_cartao.where(
        sc != "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (SEM CDB)",
        "Possui limite de " + fmt_lim + " mas não ativou o crédito. Sem CDB.",
    )
    insight_cartao = insight_cartao.where(
        sc != "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (COM CDB)",
        "Possui limite de " + fmt_lim + " e CDB alocado de " + fmt_aloc + " mas não ativou o crédito.",
    )
    insight_cartao = insight_cartao.where(
        sc != "NAO ATIVOU CREDITO - UTILIZA DEBITO (SEM CDB)",
        "Limite de " + fmt_lim + " disponível, usando débito (" + fmt_spending + " no mês). Oportunidade: ativar crédito.",
    )
    insight_cartao = insight_cartao.where(
        sc != "NAO ATIVOU CREDITO - UTILIZA DEBITO (COM CDB)",
        "Limite de " + fmt_lim + " + CDB (" + fmt_aloc + "), usando débito (" + fmt_spending + " no mês). Oportunidade: ativar crédito.",
    )
    insight_cartao = insight_cartao.where(
        sc != "ATIVOU CREDITO - NAO UTILIZANDO",
        "Crédito ativo (limite " + fmt_lim + ") desde " + fmt_ativ_cred + ", sem spending no mês.",
    )
    insight_cartao = insight_cartao.where(
        sc != "ATIVOU CREDITO - UTILIZANDO",
        "Crédito ativo (limite " + fmt_lim + "), spending de " + fmt_spending + " no mês.",
    )
    dataframe["insight_cartao"] = insight_cartao

    # ------------------------------------------------------------------ #
    # insight_maq
    # ------------------------------------------------------------------ #
    sm = dataframe["status_maq"].astype(str)
    fmt_install = _fmt_date(dataframe["dt_install_maq"])
    fmt_ativ_pay = _fmt_date(dataframe["dt_ativacao_pay"])
    fmt_cancel = _fmt_date(dataframe["dt_cancelamento_maq"])
    fmt_ult_trans = _fmt_date(dataframe["dt_ult_trans_pay"])
    fmt_total_tpv = _fmt_series_brl(dataframe["total_tpv"])
    fmt_m0 = _fmt_series_brl(dataframe["tpv_m0"])
    fmt_m1 = _fmt_series_brl(dataframe["tpv_m1"])
    fmt_m2 = _fmt_series_brl(dataframe["tpv_m2"])

    insight_maq = pd.Series("Verificar situação da maquininha.", index=dataframe.index, dtype=object)
    insight_maq = insight_maq.where(
        sm != "NAO ELEGIVEL",
        "Cliente não elegível para C6 Pay.",
    )
    insight_maq = insight_maq.where(
        sm != "ELEGIVEL - SEM VENDA",
        "Elegível para C6 Pay.",
    )
    insight_maq = insight_maq.where(
        sm != "INSTALADA - NAO ATIVADA",
        "Maquininha instalada em " + fmt_install + ", aguardando ativação.",
    )
    insight_maq = insight_maq.where(
        sm != "INSTALADA - COM TPV SEM ATIVAR",
        "Maquininha instalada em " + fmt_install + " com TPV de " + fmt_total_tpv + " mas ainda não ativada.",
    )
    insight_maq = insight_maq.where(
        sm != "CANCELADA",
        "Maquininha instalada em " + fmt_install + " e cancelada em " + fmt_cancel + " sem uso.",
    )
    insight_maq = insight_maq.where(
        sm != "CANCELADA - COM TPV",
        "Maquininha cancelada em " + fmt_cancel + " com TPV acumulado de " + fmt_total_tpv + ". Verificar reativação.",
    )
    insight_maq = insight_maq.where(
        sm != "ATIVA - INATIVA 30D",
        "Maquininha ativa desde " + fmt_ativ_pay
        + ", sem transações nos últimos 30 dias. Última: "
        + fmt_ult_trans.where(fmt_ult_trans != "", "não registrada") + ".",
    )
    insight_maq = insight_maq.where(
        sm != "ATIVA - TRANSACIONANDO",
        "Maquininha ativa (desde " + fmt_ativ_pay + "): TPV M0 "
        + fmt_m0 + " | M1 " + fmt_m1 + " | M2 " + fmt_m2 + ".",
    )
    insight_maq = insight_maq.where(
        sm != "PEDIDO EM ANALISE",
        "Proposta em análise C6 / aguardando aprovação do cliente.",
    )
    dataframe["insight_maq"] = insight_maq

    # ------------------------------------------------------------------ #
    # insight_bolcob
    # ------------------------------------------------------------------ #
    sb = dataframe["status_bolcbob"].astype(str)

    # sufixo de idade do CNPJ: " CNPJ com mais de 1 ano de fundação"
    dt_fundacao = _coerce_datetime(dataframe["dt_fundacao_empresa"])
    data_base_dt = _coerce_datetime(dataframe["data_base"])
    has_fundacao = ~dt_fundacao.isna() & ~data_base_dt.isna()
    dias_fundacao = (data_base_dt - dt_fundacao).dt.days.fillna(0)
    cnpj_age_suffix = pd.Series("", index=dataframe.index, dtype=object)
    cnpj_age_suffix[has_fundacao & (dias_fundacao > 365)] = " CNPJ com mais de 1 ano de fundação"

    fmt_ult_emissao = _fmt_date(dataframe["dt_ult_emissao_bolcob"])
    qtd_emitido = dataframe["qtd_bolcob_emtd_mtd"].astype(str).str.replace(r"\.0$", "", regex=True).replace("nan", "0")
    qtd_liq = dataframe["qtd_bolcob_liq_mtd"].astype(str).str.replace(r"\.0$", "", regex=True).replace("nan", "0")
    fmt_val_emitido = _fmt_series_brl(dataframe["vl_bolcob_emtd_mtd"])
    fmt_val_liq = _fmt_series_brl(dataframe["vl_bolcob_liq_mtd"])

    insight_bolcob = pd.Series("Verificar situação do boleto.", index=dataframe.index, dtype=object)
    insight_bolcob = insight_bolcob.where(
        sb != "SEM BOLETO CADASTRADO",
        "Sem boleto cadastrado." + cnpj_age_suffix,
    )
    insight_bolcob = insight_bolcob.where(
        sb != "BOLETO CADASTRADO - NUNCA EMITIDO",
        "Boleto cadastrado mas nunca emitido." + cnpj_age_suffix,
    )
    insight_bolcob = insight_bolcob.where(
        sb != "BOLETO EMITIDO MAS NAO LIQUIDADO",
        "Boleto emitido (última emissão: " + fmt_ult_emissao + ") sem liquidação no mês.",
    )
    insight_bolcob = insight_bolcob.where(
        sb != "ATIVO - UTILIZANDO",
        "Boleto ativo: " + qtd_emitido + " emitido(s) (" + fmt_val_emitido
        + ") e " + qtd_liq + " liquidado(s) (" + fmt_val_liq
        + ") no mês. Última emissão: " + fmt_ult_emissao + ".",
    )
    dataframe["insight_bolcob"] = insight_bolcob

    # ------------------------------------------------------------------ #
    # insight_pix_forte
    # ------------------------------------------------------------------ #
    chaves = dataframe["chaves_pix_forte"].astype(str).str.strip()
    _null_pix = {"", "-", "'-", "nan", "none", "nat", "None", "NaT", "NULL", "null"}
    has_pix = ~chaves.isin(_null_pix)
    has_cnpj = chaves.str.upper().str.contains("CNPJ", na=False)

    insight_pix = pd.Series(
        "Sem chave PIX cadastrada. Incentivar cadastro da chave CNPJ.",
        index=dataframe.index,
        dtype=object,
    )
    insight_pix = insight_pix.where(
        ~(has_pix & has_cnpj),
        "Chave(s) PIX cadastrada(s): " + chaves + ". Chave forte ativa.",
    )
    insight_pix = insight_pix.where(
        ~(has_pix & ~has_cnpj),
        "Chave(s) PIX cadastrada(s): " + chaves + ". Sem chave CNPJ \u2014 incentivar cadastro.",
    )
    dataframe["insight_pix_forte"] = insight_pix

    # ------------------------------------------------------------------ #
    # insight_conta_global
    # ------------------------------------------------------------------ #
    dt_global = _coerce_datetime(dataframe["dt_conta_criada_global"])
    dataframe["insight_conta_global"] = (
        pd.Series("Sem Conta Global.", index=dataframe.index, dtype=object)
        .where(dt_global.isna(), "Possui Conta Global.")
    )


# ---------------------------------------------------------------------------
# Colunas derivadas: gap / threshold / faixa / pct (modelo cols 90–107)
# ---------------------------------------------------------------------------

def _compute_gap_columns(dataframe) -> None:
    """
    Calcula as colunas de faixa, threshold, gap e percentual de progresso.

    FAIXA_MAXIMO = MAX(cash_in, domicilio, saldo_medio, cash_in_global)
    Nota: faixa_spending não entra no MAX (modelo RELATORIO.FORMULASS).
    """
    import numpy as np
    import pandas as pd

    faixa_cash = _coerce_numeric(dataframe["faixa_cash_in"]).fillna(0)
    faixa_domicilio = _coerce_numeric(dataframe["faixa_domicilio"]).fillna(0)
    faixa_saldo = _coerce_numeric(dataframe["faixa_saldo_medio"]).fillna(0)
    faixa_spending = _coerce_numeric(dataframe["faixa_spending"]).fillna(0)
    faixa_global = _coerce_numeric(dataframe["faixa_cash_in_global"]).fillna(0)

    # FAIXA_MAXIMO: sem faixa_spending (modelo RELATORIO.FORMULASS)
    faixa_frame = pd.concat(
        [faixa_cash, faixa_domicilio, faixa_saldo, faixa_global],
        axis=1,
    )
    faixa_maximo = faixa_frame.max(axis=1).fillna(0).astype(int)
    dataframe["faixa_maximo"] = faixa_maximo

    faixa_alvo_num = faixa_maximo + 1
    is_max = faixa_maximo >= 4
    dataframe["faixa_alvo"] = np.where(is_max, "MAX", faixa_alvo_num.astype(str))

    target_cash_in = faixa_alvo_num.map({1: 6000, 2: 15000, 3: 30000, 4: 50000}).fillna(0)
    target_spending = faixa_alvo_num.map({1: 5000, 2: 8000, 3: 11000, 4: 15000}).fillna(0)
    target_saldo = faixa_alvo_num.map({1: 1000, 2: 2000, 3: 4000, 4: 8000}).fillna(0)
    target_global = faixa_alvo_num.map({1: 5000, 2: 10000, 3: 20000, 4: 30000}).fillna(0)
    target_domicilio = faixa_alvo_num.map({1: 5000, 2: 12000, 3: 18000, 4: 25000}).fillna(0)

    for col, values in (
        ("threshold_cash_in", target_cash_in),
        ("threshold_spending", target_spending),
        ("thereshold_saldo_medio", target_saldo),
        ("threshold_conta_global", target_global),
        ("threshold_domicilio", target_domicilio),
    ):
        dataframe[col] = np.where(is_max, 0, values)

    current_cash = _coerce_numeric(dataframe["vl_cash_in_mtd"]).fillna(0)
    current_spending = _coerce_numeric(dataframe["vl_spending_total_mtd"]).fillna(0)
    current_saldo = _coerce_numeric(dataframe["vl_saldo_medio_mensalizado"]).fillna(0)
    current_global = _coerce_numeric(dataframe["vl_cash_in_conta_global_mtd"]).fillna(0)

    dataframe["gap_cash_in"] = np.where(
        is_max | (faixa_cash >= faixa_alvo_num), 0,
        np.maximum(target_cash_in - current_cash, 0),
    )
    dataframe["gap_spending"] = np.where(
        is_max | (faixa_spending >= faixa_alvo_num), 0,
        np.maximum(target_spending - current_spending, 0),
    )
    dataframe["gap_saldo_medio"] = np.where(
        is_max | (faixa_saldo >= faixa_alvo_num), 0,
        np.maximum(target_saldo - current_saldo, 0),
    )
    dataframe["gap_conta_global"] = np.where(
        is_max | (faixa_global >= faixa_alvo_num), 0,
        np.maximum(target_global - current_global, 0),
    )
    dataframe["gap_domicilio"] = np.where(
        is_max | (faixa_domicilio >= faixa_alvo_num), 0,
        None,
    )

    def _progress(gap, target):
        return np.where((gap == 0) | (target == 0), 1, 1 - (gap / target))

    pct_cash = _progress(dataframe["gap_cash_in"], target_cash_in)
    pct_spending = _progress(dataframe["gap_spending"], target_spending)
    pct_saldo = _progress(dataframe["gap_saldo_medio"], target_saldo)
    pct_global = _progress(dataframe["gap_conta_global"], target_global)

    dataframe["pct_cash_in"] = pct_cash
    dataframe["pct_spending"] = pct_spending
    dataframe["pct_saldo_medio"] = pct_saldo
    dataframe["pct_conta_global"] = pct_global

    pct_frame = pd.DataFrame(
        {
            "CASH_IN": np.where(pct_cash < 1, pct_cash, 0),
            "SPENDING": np.where(pct_spending < 1, pct_spending, 0),
            "SALDO_MEDIO": np.where(pct_saldo < 1, pct_saldo, 0),
            "CONTA_GLOBAL": np.where(pct_global < 1, pct_global, 0),
        }
    )
    dataframe["maior_progresso_pct"] = np.where(is_max, 1, pct_frame.max(axis=1))

    pct_row_max = pct_frame.max(axis=1)
    criterio = np.full(len(pct_frame), "CASH_IN", dtype=object)
    for name in reversed(["CASH_IN", "SPENDING", "SALDO_MEDIO", "CONTA_GLOBAL"]):
        criterio[pct_frame[name].values == pct_row_max.values] = name
    criterio[is_max.values] = "MAX"
    dataframe["criterio_proximo"] = criterio


# ---------------------------------------------------------------------------
# Colunas derivadas: status_qualificacao
# ---------------------------------------------------------------------------

def _compute_status_qualificacao(dataframe) -> None:
    """
    STATUS_QUALIFICAÇÃO — 5 categorias + default baseadas em comissão recebida,
    previsão e faixa_alvo.

    Depende de faixa_alvo (deve ser calculado antes via _compute_gap_columns).

    Categorias:
      A: Nunca qualificou    — ja_pago=0 e previsao=0
      B: Primeira qualificação — ja_pago=0 e previsao>0
      C: Qualificação recorrente — ja_pago>0 e previsao>0
      D: Topo atingido        — ja_pago>0, previsao=0, faixa_alvo="MAX"
      E: Perdeu qualificação  — ja_pago>0, previsao=0, faixa_alvo≠"MAX"
      -: Não classificado     — demais casos
    """
    import numpy as np

    ja_pago = _coerce_numeric(dataframe["ja_pago_comiss"]).fillna(0)
    previsao = _coerce_numeric(dataframe["previsao_comiss"]).fillna(0)
    faixa_alvo = dataframe["faixa_alvo"].astype(str)

    dataframe["status_qualificacao"] = np.select(
        [
            (ja_pago == 0) & (previsao == 0),
            (ja_pago == 0) & (previsao > 0),
            (ja_pago > 0) & (previsao > 0),
            (ja_pago > 0) & (previsao == 0) & (faixa_alvo == "MAX"),
            (ja_pago > 0) & (previsao == 0) & (faixa_alvo != "MAX"),
        ],
        [
            "Status: A\nDescrição: Nunca qualificou — cliente nunca recebeu comissão e não há nenhuma prevista.",
            "Status: B\nDescrição: Primeira qualificação — cliente ainda não recebeu comissão, mas há uma prevista.",
            "Status: C\nDescrição: Qualificação recorrente — cliente já recebeu comissões anteriores e tem uma nova prevista.",
            "Status: D\nDescrição: Topo atingido — cliente já recebeu comissões e atingiu a faixa máxima.",
            "Status: E\nDescrição: Perdeu qualificação — cliente já recebeu comissões, mas não há nova prevista e não atingiu o nível máximo.",
        ],
        default="Status: -\nDescrição: Não classificado.",
    )


# ---------------------------------------------------------------------------
# Ponto de entrada do step
# ---------------------------------------------------------------------------

def run_enrich(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "enrich"):
        return
    begin_step(session, job_id, "enrich")

    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache")

    # Garante que colunas source ausentes existam como None
    for column in REQUIRED_COLUMNS:
        if column not in dataframe.columns:
            dataframe[column] = None

    # Derivações — ordem importa:
    # total_tpv antes de status_maq (depende de total_tpv)
    # gap_columns antes de status_qualificacao (depende de faixa_alvo)
    _compute_total_tpv(dataframe)
    _compute_status_cartao(dataframe)
    _compute_status_maq(dataframe)
    _compute_status_bolcob(dataframe)
    _compute_insight_columns(dataframe)
    _compute_gap_columns(dataframe)
    _compute_status_qualificacao(dataframe)

    # Filtra o dataframe para exatamente as colunas do modelo
    dataframe = dataframe[REQUIRED_COLUMNS]
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "enrich")
