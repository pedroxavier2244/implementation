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
    # troca separadores: 1,234.56 → 1.234,56
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
    STATUS_CARTAO — 8 categorias baseadas em limite, ativação e spending.

    Lógica (por prioridade):
      VALIDAR                                  : dt_ativ presente mas sem limite de crédito
      ATIVOU CREDITO - UTILIZANDO              : crédito ativo + spending > 0
      ATIVOU CREDITO - NAO UTILIZANDO          : crédito ativo, spending = 0
      NAO ATIVOU CREDITO (COM CDB)             : tem limite + CDB alocado, sem ativação
      NAO ATIVOU CREDITO (SEM CDB)             : tem limite, sem CDB, sem ativação
      DEBITO - UTILIZANDO                      : só débito (sem limite) + spending > 0
      DEBITO - NAO UTILIZANDO                  : só débito (sem limite), spending = 0
      NAO POSSUI CARTAO                        : nenhum dos anteriores
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
            has_ativ & ~has_credit,
            has_ativ & has_credit & is_spending,
            has_ativ & has_credit & ~is_spending,
            has_entrega & ~has_ativ & has_credit & has_cdb,
            has_entrega & ~has_ativ & has_credit & ~has_cdb,
            has_entrega & ~has_ativ & ~has_credit & is_spending,
            has_entrega & ~has_ativ & ~has_credit & ~is_spending,
        ],
        [
            "VALIDAR",
            "ATIVOU CREDITO - UTILIZANDO",
            "ATIVOU CREDITO - NAO UTILIZANDO",
            "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (COM CDB)",
            "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (SEM CDB)",
            "DEBITO - UTILIZANDO",
            "DEBITO - NAO UTILIZANDO",
        ],
        default="NAO POSSUI CARTAO",
    )


# ---------------------------------------------------------------------------
# Colunas derivadas: status_maq
# ---------------------------------------------------------------------------

def _compute_status_maq(dataframe) -> None:
    """
    STATUS_MAQ — 5 categorias baseadas em instalação, ativação e atividade 30d.

    Lógica (por prioridade):
      ATIVA - TRANSACIONANDO  : máquina ativada + c6pay_ativa_30 = 1
      ATIVA - INATIVA 30D     : máquina ativada + c6pay_ativa_30 = 0
      INSTALADA - NAO ATIVADA : instalada mas sem ativação
      ELEGIVEL - SEM VENDA    : elegível mas sem instalação
      NAO ELEGIVEL            : demais casos
    """
    import numpy as np

    fl_eleg = _coerce_numeric(dataframe["fl_elegivel_venda_c6pay"]).fillna(0)
    dt_install = _coerce_datetime(dataframe["dt_install_maq"])
    dt_ativ = _coerce_datetime(dataframe["dt_ativacao_pay"])
    c6pay_30 = _coerce_numeric(dataframe["c6pay_ativa_30"]).fillna(0)

    has_install = ~dt_install.isna()
    has_ativ = ~dt_ativ.isna()
    is_eleg = fl_eleg == 1
    is_active_30 = c6pay_30 == 1

    dataframe["status_maq"] = np.select(
        [
            has_ativ & is_active_30,
            has_ativ & ~is_active_30,
            has_install & ~has_ativ,
            is_eleg & ~has_install,
        ],
        [
            "ATIVA - TRANSACIONANDO",
            "ATIVA - INATIVA 30D",
            "INSTALADA - NAO ATIVADA",
            "ELEGIVEL - SEM VENDA",
        ],
        default="NAO ELEGIVEL",
    )


# ---------------------------------------------------------------------------
# Colunas derivadas: status_bolcbob
# ---------------------------------------------------------------------------

def _compute_status_bolcob(dataframe) -> None:
    """
    STATUS_BOLCBOB — categorias baseadas em cadastro e histórico de liquidação.

    Lógica:
      BOLETO CADASTRADO - NUNCA EMITIDO : fl_bolcob=1, sem liquidação prévia
      BOLETO CADASTRADO - JA EMITIDO    : fl_bolcob=1, com liquidação prévia
      SEM BOLETO CADASTRADO             : fl_bolcob=0 ou nulo
    """
    import numpy as np

    fl_bolcob = _coerce_numeric(dataframe["fl_bolcob_cadastrado"]).fillna(0)
    dt_prim = _coerce_datetime(dataframe["dt_prim_liq_bolcob"])

    is_cadastrado = fl_bolcob == 1
    has_prim = ~dt_prim.isna()

    dataframe["status_bolcbob"] = np.select(
        [
            is_cadastrado & ~has_prim,
            is_cadastrado & has_prim,
        ],
        [
            "BOLETO CADASTRADO - NUNCA EMITIDO",
            "BOLETO CADASTRADO - JA EMITIDO",
        ],
        default="SEM BOLETO CADASTRADO",
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

    # -- helpers --
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
        sc != "ATIVOU CREDITO - NAO UTILIZANDO",
        "Crédito ativo (limite " + fmt_lim + ") desde " + fmt_ativ_cred + ", sem spending no mês.",
    )
    insight_cartao = insight_cartao.where(
        sc != "ATIVOU CREDITO - UTILIZANDO",
        "Crédito ativo (limite " + fmt_lim + "), spending de " + fmt_spending + " no mês.",
    )
    insight_cartao = insight_cartao.where(
        sc != "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (COM CDB)",
        "Possui limite de " + fmt_lim + " e CDB alocado de " + fmt_aloc + " mas não ativou o crédito.",
    )
    insight_cartao = insight_cartao.where(
        sc != "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (SEM CDB)",
        "Possui limite de " + fmt_lim + " mas não ativou o crédito. Sem CDB.",
    )
    dataframe["insight_cartao"] = insight_cartao

    # ------------------------------------------------------------------ #
    # insight_maq
    # ------------------------------------------------------------------ #
    sm = dataframe["status_maq"].astype(str)
    fmt_install = _fmt_date(dataframe["dt_install_maq"])
    fmt_ativ_pay = _fmt_date(dataframe["dt_ativacao_pay"])
    fmt_ult_trans = _fmt_date(dataframe["dt_ult_trans_pay"])
    fmt_m0 = _fmt_series_brl(dataframe["tpv_m0"])
    fmt_m1 = _fmt_series_brl(dataframe["tpv_m1"])
    fmt_m2 = _fmt_series_brl(dataframe["tpv_m2"])

    insight_maq = pd.Series("Cliente não elegível para C6 Pay.", index=dataframe.index, dtype=object)
    insight_maq = insight_maq.where(
        sm != "ELEGIVEL - SEM VENDA",
        "Elegível para C6 Pay.",
    )
    insight_maq = insight_maq.where(
        sm != "INSTALADA - NAO ATIVADA",
        "Maquininha instalada em " + fmt_install + ", aguardando ativação.",
    )
    insight_maq = insight_maq.where(
        sm != "ATIVA - INATIVA 30D",
        "Maquininha ativa desde " + fmt_ativ_pay
        + ", sem transações nos últimos 30 dias. Última: " + fmt_ult_trans + ".",
    )
    insight_maq = insight_maq.where(
        sm != "ATIVA - TRANSACIONANDO",
        "Maquininha ativa (desde " + fmt_ativ_pay + "): TPV M0 "
        + fmt_m0 + " | M1 " + fmt_m1 + " | M2 " + fmt_m2 + ".",
    )
    dataframe["insight_maq"] = insight_maq

    # ------------------------------------------------------------------ #
    # insight_bolcob
    # ------------------------------------------------------------------ #
    sb = dataframe["status_bolcbob"].astype(str)
    fmt_potencial = _fmt_series_brl(dataframe["tpv_bolcob_potencial"])

    insight_bolcob = pd.Series("Sem boleto cadastrado.", index=dataframe.index, dtype=object)
    insight_bolcob = insight_bolcob.where(
        sb != "SEM BOLETO CADASTRADO",
        "Sem boleto cadastrado. Potencial estimado: " + fmt_potencial + ".",
    )
    insight_bolcob = insight_bolcob.where(
        sb != "BOLETO CADASTRADO - NUNCA EMITIDO",
        "Boleto cadastrado mas nunca emitido.",
    )
    insight_bolcob = insight_bolcob.where(
        sb != "BOLETO CADASTRADO - JA EMITIDO",
        "Boleto ativo.",
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

    Nomes seguem exatamente o modelo:
      faixa_maximo (era faixa_max)
      threshold_cash_in (era threshiold_cash_in)
      thereshold_saldo_medio (nome do modelo, com typo)
    """
    import numpy as np
    import pandas as pd

    faixa_cash = _coerce_numeric(dataframe["faixa_cash_in"]).fillna(0)
    faixa_domicilio = _coerce_numeric(dataframe["faixa_domicilio"]).fillna(0)
    faixa_saldo = _coerce_numeric(dataframe["faixa_saldo_medio"]).fillna(0)
    faixa_spending = _coerce_numeric(dataframe["faixa_spending"]).fillna(0)
    faixa_global = _coerce_numeric(dataframe["faixa_cash_in_global"]).fillna(0)

    faixa_frame = pd.concat(
        [faixa_cash, faixa_domicilio, faixa_saldo, faixa_spending, faixa_global],
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

    # Derivações — ordem importa: status_* antes dos insight_*
    _compute_total_tpv(dataframe)
    _compute_status_cartao(dataframe)
    _compute_status_maq(dataframe)
    _compute_status_bolcob(dataframe)
    _compute_insight_columns(dataframe)
    _compute_gap_columns(dataframe)

    # Filtra o dataframe para exatamente as colunas do modelo
    dataframe = dataframe[REQUIRED_COLUMNS]
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "enrich")
