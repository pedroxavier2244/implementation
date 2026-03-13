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
        0,
        None,
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


def _compute_safra_columns(dataframe):
    """Deriva as 6 colunas de safra/cancelamento/elegibilidade.

    Fonte das fórmulas: RELATORIO_LIMPO_2025_COM_FORMULAS.xlsx (colunas DB–DG).

    Observação importante:
    - safra_maquina / idade_safra_maquina usam DT_ATIVACAO_PAY (col AK),
      NÃO DT_INSTALL_MAQ — conforme fórmula Excel original.
    - idade_safra_* e cancelamento_maq retornam strings ("SEM BOLETO" etc.)
      quando o campo de data de referência está ausente, pois as colunas são
      do tipo Text no banco.
    """
    import numpy as np
    import pandas as pd

    dt_install = _coerce_datetime(dataframe["dt_install_maq"])
    dt_cancel  = _coerce_datetime(dataframe["dt_cancelamento_maq"])
    dt_ativacao = _coerce_datetime(dataframe["dt_ativacao_pay"])
    dt_bolcob  = _coerce_datetime(dataframe["dt_prim_liq_bolcob"])
    data_base  = _coerce_datetime(dataframe["data_base"])

    # -- cancelamento_maq --
    # Excel: =IF(DT_INSTALL_MAQ="","Sem máquina instalada",
    #            IF(DT_CANCELAMENTO_MAQ="","Máquina ativa desde "&TEXT(DT_INSTALL_MAQ,"dd/mm/aaaa"),
    #            "Instalada em "&TEXT(DT_INSTALL_MAQ,"dd/mm/aaaa")&
    #            " | Cancelada em "&TEXT(DT_CANCELAMENTO_MAQ,"dd/mm/aaaa")))
    install_str = dt_install.dt.strftime("%d/%m/%Y").where(~dt_install.isna(), "")
    cancel_str  = dt_cancel.dt.strftime("%d/%m/%Y").where(~dt_cancel.isna(), "")
    dataframe["cancelamento_maq"] = np.where(
        dt_install.isna(),
        "Sem máquina instalada",
        np.where(
            dt_cancel.isna(),
            "Máquina ativa desde " + install_str,
            "Instalada em " + install_str + " | Cancelada em " + cancel_str,
        ),
    )

    # -- elegivel_c6 --
    # Excel: =IF(AND(FL_ELEGIVEL_VENDA_C6PAY=1, DT_INSTALL_MAQ="", DT_CANCELAMENTO_MAQ=""),
    #            "Elegível","Não Elegível")
    fl_elegivel = _coerce_numeric(dataframe["fl_elegivel_venda_c6pay"]).fillna(0)
    dataframe["elegivel_c6"] = np.where(
        (fl_elegivel == 1) & dt_install.isna() & dt_cancel.isna(),
        "Elegível",
        "Não Elegível",
    )

    # -- safra_boleto --
    # Excel: =IF(DT_PRIM_LIQ_BOLCOB="","SEM BOLETO",
    #            YEAR(DT_PRIM_LIQ_BOLCOB)&"-"&TEXT(MONTH(DT_PRIM_LIQ_BOLCOB),"00"))
    safra_boleto_str = dt_bolcob.dt.strftime("%Y-%m")
    dataframe["safra_boleto"] = np.where(dt_bolcob.isna(), "SEM BOLETO", safra_boleto_str)

    # -- idade_safra_boleto --
    # Excel: =IF(DT_PRIM_LIQ_BOLCOB="","SEM BOLETO", DATA_BASE - DT_PRIM_LIQ_BOLCOB) [dias]
    idade_boleto = (data_base.dt.floor("D") - dt_bolcob.dt.floor("D")).dt.days
    dataframe["idade_safra_boleto"] = np.where(
        dt_bolcob.isna(),
        "SEM BOLETO",
        idade_boleto.astype("Int64").astype(str).where(~dt_bolcob.isna(), "SEM BOLETO"),
    )

    # -- safra_maquina --
    # Excel: =IF(DT_ATIVACAO_PAY="","SEM MÁQUINA",
    #            YEAR(DT_ATIVACAO_PAY)&"-"&TEXT(MONTH(DT_ATIVACAO_PAY),"00"))
    # ATENÇÃO: usa DT_ATIVACAO_PAY (ativação), não DT_INSTALL_MAQ (instalação)
    safra_maq_str = dt_ativacao.dt.strftime("%Y-%m")
    dataframe["safra_maquina"] = np.where(dt_ativacao.isna(), "SEM MÁQUINA", safra_maq_str)

    # -- idade_safra_maquina --
    # Excel: =IF(DT_ATIVACAO_PAY="","SEM MÁQUINA", DATA_BASE - DT_ATIVACAO_PAY) [dias]
    idade_maq = (data_base.dt.floor("D") - dt_ativacao.dt.floor("D")).dt.days
    dataframe["idade_safra_maquina"] = np.where(
        dt_ativacao.isna(),
        "SEM MÁQUINA",
        idade_maq.astype("Int64").astype(str).where(~dt_ativacao.isna(), "SEM MÁQUINA"),
    )


def _compute_metrica_columns(dataframe):
    """Deriva as 6 colunas de métricas e o score_perfil composto.

    Fonte das fórmulas: screenshot fornecido pelo usuário (colunas DH–DM do modelo limpo).

    Fórmulas Excel originais:
      metrica_ativacao  = SE(OU(CA2="";CA2=0);0,15;SE(CA2<=210;0,12;SE(CA2<=345;0,08;SE(CA2<=600;0,03;0))))
      metrica_progresso = CS2*0,35
      metrica_urgencia  = SE(BQ2="M0";0,35;SE(BQ2="M1";0,2;SE(BQ2="M2";SE(CS2>=0,6;SE(CY2<=15;0,3;0,25);0,15);0)))
      metrica_financeiro= ((SE(OU(Y2="";Y2=0);0;SE(Y2<=1000;0,33;SE(Y2<=5000;0,66;1)))+
                            SE(OU(R2="";R2=0);0;SE(R2<=1000;0,33;SE(R2<=3000;0,66;1))))/2)*0,05
      metrica_intencao  = SE(W2="CNPJ";0;0,1)
      score_perfil      = MÍNIMO(DH2+DI2+DJ2+DK2+DL2;1)

    Mapeamento de colunas Excel → banco:
      CA = ja_pago_comiss (col 79)
      CS = maior_progresso_pct (col 97)
      BQ = mes_ref_comiss (col 69)
      CY = m2_dias_faltantes (col 103)
      Y  = limite_cartao (col 25)
      R  = limite_conta (col 18)
      W  = chaves_pix_forte (col 23)
    """
    import numpy as np

    ja_pago          = _coerce_numeric(dataframe["ja_pago_comiss"]).fillna(0)
    maior_progresso  = _coerce_numeric(dataframe["maior_progresso_pct"]).fillna(0)
    mes_ref          = dataframe["mes_ref_comiss"].astype(str).str.strip().str.upper()
    dias_faltantes   = _coerce_numeric(dataframe["m2_dias_faltantes"]).fillna(999)
    limite_cartao    = _coerce_numeric(dataframe["limite_cartao"]).fillna(0)
    limite_conta     = _coerce_numeric(dataframe["limite_conta"]).fillna(0)
    chaves_pix       = dataframe["chaves_pix_forte"].astype(str).str.strip().str.upper()

    # -- metrica_ativacao --
    # SE(OU(CA=0)→0,15; CA<=210→0,12; CA<=345→0,08; CA<=600→0,03; senão 0)
    dataframe["metrica_ativacao"] = np.select(
        [
            ja_pago == 0,
            ja_pago <= 210,
            ja_pago <= 345,
            ja_pago <= 600,
        ],
        [0.15, 0.12, 0.08, 0.03],
        default=0.0,
    )

    # -- metrica_progresso --
    # maior_progresso_pct * 0,35
    dataframe["metrica_progresso"] = maior_progresso * 0.35

    # -- metrica_urgencia --
    # M0→0,35 | M1→0,2 | M2 com progresso≥0,6: dias≤15→0,3 senão 0,25; M2 com progresso<0,6→0,15 | demais→0
    urgencia_m2 = np.where(
        maior_progresso >= 0.6,
        np.where(dias_faltantes <= 15, 0.3, 0.25),
        0.15,
    )
    dataframe["metrica_urgencia"] = np.select(
        [
            mes_ref == "M0",
            mes_ref == "M1",
            mes_ref == "M2",
        ],
        [0.35, 0.2, urgencia_m2],
        default=0.0,
    )

    # -- metrica_financeiro --
    # ((score_cartao + score_conta) / 2) * 0,05
    # F10: limite <= 0 (inclui negativos) → score 0, igual a sem limite
    # score_cartao: ≤0→0; ≤1000→0,33; ≤5000→0,66; >5000→1
    # score_conta:  ≤0→0; ≤1000→0,33; ≤3000→0,66; >3000→1
    score_cartao = np.select(
        [limite_cartao <= 0, limite_cartao <= 1000, limite_cartao <= 5000],
        [0.0, 0.33, 0.66],
        default=1.0,
    )
    score_conta = np.select(
        [limite_conta <= 0, limite_conta <= 1000, limite_conta <= 3000],
        [0.0, 0.33, 0.66],
        default=1.0,
    )
    dataframe["metrica_financeiro"] = ((score_cartao + score_conta) / 2) * 0.05

    # -- metrica_intencao --
    # SE(chaves_pix_forte="CNPJ";0;0,1)
    dataframe["metrica_intencao"] = np.where(chaves_pix == "CNPJ", 0.0, 0.1)

    # -- score_perfil --
    # MÍNIMO(soma das 5 métricas; 1)
    soma = (
        dataframe["metrica_ativacao"]
        + dataframe["metrica_progresso"]
        + dataframe["metrica_urgencia"]
        + dataframe["metrica_financeiro"]
        + dataframe["metrica_intencao"]
    )
    dataframe["score_perfil"] = np.minimum(soma, 1.0)


def _compute_status_columns(dataframe):
    import numpy as np

    ja_pago = _coerce_numeric(dataframe["ja_pago_comiss"]).fillna(0)
    previsao = _coerce_numeric(dataframe["previsao_comiss"]).fillna(0)

    # Padronizado: "SIM" / "NAO" sem acento e em maiúsculas — consistente em todos os campos
    ja_recebeu = np.where(ja_pago > 0, "SIM", "NAO")
    prox_mes = np.where(previsao > 0, "SIM", "NAO")
    dataframe["ja_recebeu_comissao"] = ja_recebeu
    dataframe["comissao_prox_mes"] = prox_mes

    faixa_alvo = dataframe["faixa_alvo"].astype(str).str.upper()
    is_max = faixa_alvo == "MAX"

    status = np.where(
        (ja_recebeu == "NAO") & (prox_mes == "NAO"),
        "A - Nunca qualificou",
        np.where(
            (ja_recebeu == "NAO") & (prox_mes == "SIM"),
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

    # Use pandas .where() to preserve numeric dtype and emit NULL (not a string) for missing dates
    import pandas as pd
    dataframe["dias_desde_abertura"] = days_since.where(~dt_conta.isna(), other=None)
    dataframe["m2_dias_faltantes"] = (days_since - 60).where(~dt_conta.isna(), other=None)


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
    _compute_safra_columns(dataframe)
    _compute_metrica_columns(dataframe)

    # Keep output shape aligned with the target spreadsheet model.
    dataframe = dataframe[REQUIRED_COLUMNS]
    set_cached_dataframe(job_id, dataframe)

    mark_step_done(session, job_id, "enrich")
