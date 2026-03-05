from sqlalchemy.orm import Session

from shared.visao_cliente_schema import REQUIRED_COLUMNS
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done
from worker.steps.extract import get_cached_dataframe, set_cached_dataframe


def _coerce_numeric(series):
    import pandas as pd

    cleaned = series.astype(str).str.strip()
    cleaned = cleaned.replace({"": None, "nan": None, "none": None, "nat": None, "None": None})
    cleaned = cleaned.str.replace(r"[^0-9,.\-]", "", regex=True)

    has_comma = cleaned.str.contains(",", na=False)
    has_dot = cleaned.str.contains(r"\.", na=False)

    mask_brl = has_comma & has_dot
    cleaned.loc[mask_brl] = cleaned.loc[mask_brl].str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

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


def _compute_levels(dataframe):
    import numpy as np

    limite_cartao = _coerce_numeric(dataframe["limite_cartao"])
    limite_conta = _coerce_numeric(dataframe["limite_conta"])

    dataframe["nivel_cartao"] = np.select(
        [
            limite_cartao.isna() | (limite_cartao <= 0),
            limite_cartao <= 1000,
            limite_cartao <= 5000,
            limite_cartao > 5000,
        ],
        ["Sem Cartao", "Baixo", "Medio", "Alto"],
        default=None,
    )

    dataframe["nivel_conta"] = np.select(
        [
            limite_conta.isna() | (limite_conta <= 0),
            limite_conta <= 1000,
            limite_conta <= 3000,
            limite_conta > 3000,
        ],
        ["Sem Conta", "Baixo", "Medio", "Alto"],
        default=None,
    )


def _compute_gap_columns(dataframe):
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
    faixa_max = faixa_frame.max(axis=1).fillna(0).astype(int)
    dataframe["faixa_max"] = faixa_max

    faixa_alvo_num = faixa_max + 1
    is_max = faixa_max >= 4
    dataframe["faixa_alvo"] = np.where(is_max, "MAX", faixa_alvo_num.astype(str))

    target_cash_in = faixa_alvo_num.map({1: 6000, 2: 15000, 3: 30000, 4: 50000}).fillna(0)
    target_spending = faixa_alvo_num.map({1: 5000, 2: 8000, 3: 11000, 4: 15000}).fillna(0)
    target_saldo = faixa_alvo_num.map({1: 1000, 2: 2000, 3: 4000, 4: 8000}).fillna(0)
    target_global = faixa_alvo_num.map({1: 5000, 2: 10000, 3: 20000, 4: 30000}).fillna(0)
    target_domicilio = faixa_alvo_num.map({1: 5000, 2: 12000, 3: 18000, 4: 25000}).fillna(0)

    for col, values in (
        ("threshiold_cash_in", target_cash_in),
        ("threshold_spending", target_spending),
        ("threshold_saldo_medio", target_saldo),
        ("threshold_conta_global", target_global),
        ("threshold_domicilio", target_domicilio),
    ):
        dataframe[col] = np.where(is_max, 0, values)

    current_cash = _coerce_numeric(dataframe["vl_cash_in_mtd"]).fillna(0)
    current_spending = _coerce_numeric(dataframe["vl_spending_total_mtd"]).fillna(0)
    current_saldo = _coerce_numeric(dataframe["vl_saldo_medio_mensalizado"]).fillna(0)
    current_global = _coerce_numeric(dataframe["vl_cash_in_conta_global_mtd"]).fillna(0)

    dataframe["gap_cash_in"] = np.where(
        is_max | (faixa_cash >= faixa_alvo_num),
        0,
        np.maximum(target_cash_in - current_cash, 0),
    )
    dataframe["gap_spending"] = np.where(
        is_max | (faixa_spending >= faixa_alvo_num),
        0,
        np.maximum(target_spending - current_spending, 0),
    )
    dataframe["gap_saldo_medio"] = np.where(
        is_max | (faixa_saldo >= faixa_alvo_num),
        0,
        np.maximum(target_saldo - current_saldo, 0),
    )
    dataframe["gap_conta_global"] = np.where(
        is_max | (faixa_global >= faixa_alvo_num),
        0,
        np.maximum(target_global - current_global, 0),
    )
    dataframe["gap_domicilio"] = np.where(
        is_max | (faixa_domicilio >= faixa_alvo_num),
        "0",
        "SEM DADOS",
    )

    def _progress(gap, target):
        # Excel formula: =(IF(OR(GAP=0,THRESHOLD=0),0,GAP/THRESHOLD)-1)*-1
        # When gap=0 or threshold=0 the inner IF returns 0, then (0-1)*-1 = 1 (100%)
        # When gap>0: (gap/threshold - 1) * -1 = 1 - gap/threshold
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

    # Vectorized: mesmo criterio do Excel — criterio com maior progresso (primeiro em caso de empate)
    # Excel: =IF(MAX,"MAX",IF(CS=CO,"CASH_IN",IF(CS=CP,"SPENDING",...)))
    # Prioridade de empate: CASH_IN > SPENDING > SALDO_MEDIO > CONTA_GLOBAL
    pct_row_max = pct_frame.max(axis=1)
    criterio = np.full(len(pct_frame), "CASH_IN", dtype=object)
    for name in reversed(["CASH_IN", "SPENDING", "SALDO_MEDIO", "CONTA_GLOBAL"]):
        criterio[pct_frame[name].values == pct_row_max.values] = name
    criterio[is_max.values] = "MAX"
    dataframe["criterio_proximo"] = criterio


def _compute_status_columns(dataframe):
    import numpy as np

    ja_pago = _coerce_numeric(dataframe["ja_pago_comiss"]).fillna(0)
    previsao = _coerce_numeric(dataframe["previsao_comiss"]).fillna(0)

    # Excel: =IF([@JA_PAGO_COMISS]>0,"SIM","Não")  — nota: "Não" com til
    ja_recebeu = np.where(ja_pago > 0, "SIM", "Não")
    # Excel: =IF([@PREVISAO_COMISS]>0,"SIM","NÃO")  — "NÃO" tudo maiusculo com til
    prox_mes = np.where(previsao > 0, "SIM", "NÃO")
    dataframe["ja_recebeu_comissao"] = ja_recebeu
    dataframe["comissao_prox_mes"] = prox_mes

    faixa_alvo = dataframe["faixa_alvo"].astype(str).str.upper()
    is_max = faixa_alvo == "MAX"

    status = np.where(
        (ja_recebeu == "Não") & (prox_mes == "NÃO"),
        "A - Nunca qualificou",
        np.where(
            (ja_recebeu == "Não") & (prox_mes == "SIM"),
            "B - Primeira qualificação",
            np.where(
                (ja_recebeu == "SIM") & (prox_mes == "SIM"),
                "C - Qualificação recorrente",
                np.where(is_max, "D - Topo atingido", "E - Perdeu qualificação"),
            ),
        ),
    )
    dataframe["status_qualificacao"] = status


def _compute_day_metrics(dataframe):
    import numpy as np

    data_base = _coerce_datetime(dataframe["data_base"])
    dt_conta = _coerce_datetime(dataframe["dt_conta_criada"])
    days_since = (data_base.dt.floor("D") - dt_conta.dt.floor("D")).dt.days

    dataframe["dias_desde_abertura"] = np.where(
        dt_conta.isna(),
        "SEM DADOS",
        days_since,
    )
    dataframe["m2_dias_faltantes"] = np.where(
        dt_conta.isna(),
        "SEM DADOS",
        days_since - 60,
    )


def run_enrich(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "enrich"):
        return
    begin_step(session, job_id, "enrich")

    dataframe = get_cached_dataframe(job_id)
    if dataframe is None:
        raise RuntimeError("No dataframe in cache")

    for column in REQUIRED_COLUMNS:
        if column not in dataframe.columns:
            dataframe[column] = None

    _compute_levels(dataframe)
    _compute_gap_columns(dataframe)
    _compute_status_columns(dataframe)
    _compute_day_metrics(dataframe)

    # Keep output shape aligned with the target spreadsheet model.
    dataframe = dataframe[REQUIRED_COLUMNS]
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "enrich")
