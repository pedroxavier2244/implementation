"""
Testes de transformação de dados (regras de negócio):

clean.py:
- Filtros: PJ, LIBERADA, MES_REF_COMISS preenchido
- Normalização: DATA_BASE (formatos Excel), documentos (CPF/CNPJ)

enrich.py (via funções privadas):
- total_tpv
- status_cartao (todas as categorias relevantes)
- status_maq (todas as categorias relevantes)
- status_bolcob
- status_qualificacao (categorias A–E)
- faixa_maximo (exclui faixa_spending)
- gaps e thresholds
"""
import uuid
import pytest
from unittest.mock import MagicMock, patch

pd = pytest.importorskip("pandas")
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_clean_on(df: pd.DataFrame) -> pd.DataFrame:
    """Executa run_clean em df com checkpoint mockado. Retorna o DataFrame resultante."""
    from worker.steps.clean import set_cached_dataframe, get_cached_dataframe, run_clean
    job_id = str(uuid.uuid4())
    set_cached_dataframe(job_id, df.copy())
    with patch("worker.steps.clean.is_step_done", return_value=False), \
         patch("worker.steps.clean.begin_step"), \
         patch("worker.steps.clean.mark_step_done"):
        run_clean(MagicMock(), job_id)
    return get_cached_dataframe(job_id)


def _make_row(**kwargs) -> dict:
    """Linha base completa para testes de enrich. Todos os campos relevantes preenchidos
    com valores neutros; override com kwargs."""
    defaults = {
        # status_cartao
        "limite_cartao": None, "limite_alocado_cartao_cdb": None,
        "dt_entrega_cartao": None, "dt_ativ_cartao_cred": None,
        "vl_spending_total_mtd": None,
        # status_maq
        "status_proposta_sf_pay": None, "fl_elegivel_venda_c6pay": "0",
        "dt_install_maq": None, "dt_ativacao_pay": None, "c6pay_ativa_30": "0",
        "dt_cancelamento_maq": None,
        # total_tpv
        "tpv_m0": "0", "tpv_m1": "0", "tpv_m2": "0",
        # status_bolcob
        "fl_bolcob_cadastrado": "0", "dt_prim_liq_bolcob": None, "qtd_bolcob_liq_mtd": "0",
        # insight_bolcob
        "data_base": "01/01/2026", "dt_fundacao_empresa": None,
        "dt_ult_emissao_bolcob": None, "qtd_bolcob_emtd_mtd": "0",
        "vl_bolcob_emtd_mtd": "0", "vl_bolcob_liq_mtd": "0",
        # insight_pix_forte
        "chaves_pix_forte": None,
        # insight_conta_global
        "dt_conta_criada_global": None,
        # gaps
        "faixa_cash_in": "0", "faixa_domicilio": "0", "faixa_saldo_medio": "0",
        "faixa_spending": "0", "faixa_cash_in_global": "0",
        "vl_cash_in_mtd": "0", "vl_saldo_medio_mensalizado": "0",
        "vl_cash_in_conta_global_mtd": "0",
        # status_qualificacao
        "ja_pago_comiss": "0", "previsao_comiss": "0",
        # insight_cartao
        "dt_conta_criada": None,
        # insight_maq extra
        "dt_ult_trans_pay": None,
    }
    defaults.update(kwargs)
    return defaults


def _enrich_single(**kwargs) -> pd.Series:
    """Aplica a cadeia completa de enrich em 1 linha e retorna a Series resultante."""
    from worker.steps.enrich import (
        _compute_total_tpv, _compute_status_cartao, _compute_status_maq,
        _compute_status_bolcob, _compute_insight_columns, _compute_gap_columns,
        _compute_status_qualificacao,
    )
    df = pd.DataFrame([_make_row(**kwargs)])
    _compute_total_tpv(df)
    _compute_status_cartao(df)
    _compute_status_maq(df)
    _compute_status_bolcob(df)
    _compute_insight_columns(df)
    _compute_gap_columns(df)
    _compute_status_qualificacao(df)
    return df.iloc[0]


# ===========================================================================
# CLEAN — filtros de negócio
# ===========================================================================

def test_clean_removes_pessoa_fisica_rows():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ", "PF", "PJ"],
        "STATUS_CC": ["LIBERADA", "LIBERADA", "LIBERADA"],
        "MES_REF_COMISS": ["2026-01", "2026-01", "2026-01"],
    })
    result = _run_clean_on(df)
    assert len(result) == 2
    assert (result["tipo_pessoa"] == "PJ").all()


def test_clean_removes_status_cc_not_liberada():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ", "PJ", "PJ"],
        "STATUS_CC": ["LIBERADA", "BLOQUEADA", "ENCERRADA"],
        "MES_REF_COMISS": ["2026-01", "2026-01", "2026-01"],
    })
    result = _run_clean_on(df)
    assert len(result) == 1
    assert result.iloc[0]["status_cc"] == "LIBERADA"


def test_clean_removes_rows_with_null_mes_ref_comiss():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ", "PJ"],
        "STATUS_CC": ["LIBERADA", "LIBERADA"],
        "MES_REF_COMISS": [None, "2026-01"],
    })
    result = _run_clean_on(df)
    assert len(result) == 1
    assert result.iloc[0]["mes_ref_comiss"] == "2026-01"


def test_clean_removes_rows_with_empty_mes_ref_comiss():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"],
        "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": [""],
    })
    result = _run_clean_on(df)
    assert len(result) == 0


def test_clean_keeps_all_rows_that_pass_all_three_filters():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ", "PJ"],
        "STATUS_CC": ["LIBERADA", "LIBERADA"],
        "MES_REF_COMISS": ["2026-01", "2026-02"],
    })
    result = _run_clean_on(df)
    assert len(result) == 2


# ===========================================================================
# CLEAN — normalização de DATA_BASE
# ===========================================================================

def test_clean_normalizes_data_base_from_excel_timestamp():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "DATA_BASE": ["2026-03-01 00:00:00"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["data_base"] == "01/03/2026"


def test_clean_normalizes_data_base_from_yyyy_mm_dd():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "DATA_BASE": ["2026-04-01"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["data_base"] == "01/04/2026"


def test_clean_preserves_data_base_already_in_dd_mm_yyyy():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "DATA_BASE": ["15/03/2026"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["data_base"] == "15/03/2026"


# ===========================================================================
# CLEAN — normalização de documentos
# ===========================================================================

def test_clean_removes_decimal_zero_suffix_from_document():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "CD_CPF_CNPJ_CLIENTE": ["12345678000195.0"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["cd_cpf_cnpj_cliente"] == "12345678000195"


def test_clean_removes_punctuation_from_document():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "CD_CPF_CNPJ_CLIENTE": ["12.345.678/0001-95"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["cd_cpf_cnpj_cliente"] == "12345678000195"


# ===========================================================================
# ENRICH — total_tpv
# ===========================================================================

def test_total_tpv_sums_three_months():
    row = _enrich_single(tpv_m0="100", tpv_m1="200", tpv_m2="300")
    assert row["total_tpv"] == 600.0


def test_total_tpv_treats_null_values_as_zero():
    row = _enrich_single(tpv_m0=None, tpv_m1="500", tpv_m2=None)
    assert row["total_tpv"] == 500.0


def test_total_tpv_all_null_results_in_zero():
    row = _enrich_single(tpv_m0=None, tpv_m1=None, tpv_m2=None)
    assert row["total_tpv"] == 0.0


# ===========================================================================
# ENRICH — status_cartao
# ===========================================================================

def test_status_cartao_nao_possui_cartao():
    row = _enrich_single(
        limite_cartao=None, limite_alocado_cartao_cdb=None,
        dt_entrega_cartao=None, dt_ativ_cartao_cred=None,
        vl_spending_total_mtd=None,
    )
    assert row["status_cartao"] == "NAO POSSUI CARTAO"


def test_status_cartao_debito_utilizando():
    row = _enrich_single(
        limite_cartao=None, limite_alocado_cartao_cdb=None,
        dt_entrega_cartao="2026-01-10", dt_ativ_cartao_cred=None,
        vl_spending_total_mtd="500",
    )
    assert row["status_cartao"] == "DEBITO - UTILIZANDO"


def test_status_cartao_debito_nao_utilizando():
    row = _enrich_single(
        limite_cartao=None, limite_alocado_cartao_cdb=None,
        dt_entrega_cartao="2026-01-10", dt_ativ_cartao_cred=None,
        vl_spending_total_mtd=None,
    )
    assert row["status_cartao"] == "DEBITO - NAO UTILIZANDO"


def test_status_cartao_nao_ativou_credito_sem_cdb():
    row = _enrich_single(
        limite_cartao="5000", limite_alocado_cartao_cdb=None,
        dt_entrega_cartao=None, dt_ativ_cartao_cred=None,
        vl_spending_total_mtd=None,
    )
    assert row["status_cartao"] == "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (SEM CDB)"


def test_status_cartao_nao_ativou_credito_com_cdb():
    row = _enrich_single(
        limite_cartao="5000", limite_alocado_cartao_cdb="2000",
        dt_entrega_cartao=None, dt_ativ_cartao_cred=None,
        vl_spending_total_mtd=None,
    )
    assert row["status_cartao"] == "NAO ATIVOU CREDITO - NAO UTILIZA CARTAO (COM CDB)"


def test_status_cartao_nao_ativou_utiliza_debito_sem_cdb():
    row = _enrich_single(
        limite_cartao="5000", limite_alocado_cartao_cdb=None,
        dt_entrega_cartao=None, dt_ativ_cartao_cred=None,
        vl_spending_total_mtd="300",
    )
    assert row["status_cartao"] == "NAO ATIVOU CREDITO - UTILIZA DEBITO (SEM CDB)"


def test_status_cartao_nao_ativou_utiliza_debito_com_cdb():
    row = _enrich_single(
        limite_cartao="5000", limite_alocado_cartao_cdb="1000",
        dt_entrega_cartao=None, dt_ativ_cartao_cred=None,
        vl_spending_total_mtd="300",
    )
    assert row["status_cartao"] == "NAO ATIVOU CREDITO - UTILIZA DEBITO (COM CDB)"


def test_status_cartao_ativou_credito_utilizando():
    row = _enrich_single(
        limite_cartao="6000", limite_alocado_cartao_cdb=None,
        dt_entrega_cartao="2026-01-10", dt_ativ_cartao_cred="2026-01-15",
        vl_spending_total_mtd="800",
    )
    assert row["status_cartao"] == "ATIVOU CREDITO - UTILIZANDO"


def test_status_cartao_ativou_credito_nao_utilizando():
    row = _enrich_single(
        limite_cartao="6000", limite_alocado_cartao_cdb=None,
        dt_entrega_cartao="2026-01-10", dt_ativ_cartao_cred="2026-01-15",
        vl_spending_total_mtd=None,
    )
    assert row["status_cartao"] == "ATIVOU CREDITO - NAO UTILIZANDO"


# ===========================================================================
# ENRICH — status_maq
# ===========================================================================

def test_status_maq_nao_elegivel():
    row = _enrich_single(fl_elegivel_venda_c6pay="0")
    assert row["status_maq"] == "NAO ELEGIVEL"


def test_status_maq_elegivel_sem_venda():
    row = _enrich_single(fl_elegivel_venda_c6pay="1")
    assert row["status_maq"] == "ELEGIVEL - SEM VENDA"


def test_status_maq_instalada_nao_ativada():
    row = _enrich_single(
        fl_elegivel_venda_c6pay="1",
        dt_install_maq="2026-01-01", dt_ativacao_pay=None,
        c6pay_ativa_30="0", dt_cancelamento_maq=None,
    )
    assert row["status_maq"] == "INSTALADA - NAO ATIVADA"


def test_status_maq_ativa_transacionando():
    row = _enrich_single(
        fl_elegivel_venda_c6pay="1",
        dt_install_maq="2026-01-01", dt_ativacao_pay="2026-01-05",
        c6pay_ativa_30="1", dt_cancelamento_maq=None,
        tpv_m0="5000", tpv_m1="0", tpv_m2="0",
    )
    assert row["status_maq"] == "ATIVA - TRANSACIONANDO"


def test_status_maq_ativa_inativa_30d():
    row = _enrich_single(
        fl_elegivel_venda_c6pay="1",
        dt_install_maq="2026-01-01", dt_ativacao_pay="2026-01-05",
        c6pay_ativa_30="0", dt_cancelamento_maq=None,
        tpv_m0="1000", tpv_m1="0", tpv_m2="0",
    )
    assert row["status_maq"] == "ATIVA - INATIVA 30D"


def test_status_maq_cancelada():
    row = _enrich_single(
        fl_elegivel_venda_c6pay="1",
        dt_install_maq="2025-11-01", dt_ativacao_pay=None,
        c6pay_ativa_30="0", dt_cancelamento_maq="2026-01-15",
        tpv_m0="0", tpv_m1="0", tpv_m2="0",
    )
    assert row["status_maq"] == "CANCELADA"


def test_status_maq_cancelada_com_tpv():
    row = _enrich_single(
        fl_elegivel_venda_c6pay="1",
        dt_install_maq="2025-11-01", dt_ativacao_pay="2025-12-01",
        c6pay_ativa_30="0", dt_cancelamento_maq="2026-01-15",
        tpv_m0="3000", tpv_m1="0", tpv_m2="0",
    )
    assert row["status_maq"] == "CANCELADA - COM TPV"


# ===========================================================================
# ENRICH — status_bolcob
# ===========================================================================

def test_status_bolcob_sem_boleto_cadastrado():
    row = _enrich_single(fl_bolcob_cadastrado="0")
    assert row["status_bolcbob"] == "SEM BOLETO CADASTRADO"


def test_status_bolcob_cadastrado_nunca_emitido():
    row = _enrich_single(fl_bolcob_cadastrado="1", dt_prim_liq_bolcob=None)
    assert row["status_bolcbob"] == "BOLETO CADASTRADO - NUNCA EMITIDO"


def test_status_bolcob_emitido_nao_liquidado():
    row = _enrich_single(
        fl_bolcob_cadastrado="1",
        dt_prim_liq_bolcob="2025-12-01",
        qtd_bolcob_liq_mtd="0",
    )
    assert row["status_bolcbob"] == "BOLETO EMITIDO MAS NAO LIQUIDADO"


def test_status_bolcob_ativo_utilizando():
    row = _enrich_single(
        fl_bolcob_cadastrado="1",
        dt_prim_liq_bolcob="2025-12-01",
        qtd_bolcob_liq_mtd="3",
    )
    assert row["status_bolcbob"] == "ATIVO - UTILIZANDO"


# ===========================================================================
# ENRICH — status_qualificacao
# ===========================================================================

def test_status_qualificacao_A_nunca_qualificou():
    row = _enrich_single(ja_pago_comiss="0", previsao_comiss="0")
    assert row["status_qualificacao"].startswith("Status: A")


def test_status_qualificacao_B_primeira_qualificacao():
    row = _enrich_single(ja_pago_comiss="0", previsao_comiss="500")
    assert row["status_qualificacao"].startswith("Status: B")


def test_status_qualificacao_C_recorrente():
    row = _enrich_single(ja_pago_comiss="300", previsao_comiss="400")
    assert row["status_qualificacao"].startswith("Status: C")


def test_status_qualificacao_D_topo_atingido():
    # faixa_maximo >= 4 → faixa_alvo = "MAX"
    row = _enrich_single(
        ja_pago_comiss="300", previsao_comiss="0",
        faixa_cash_in="4", faixa_domicilio="4",
        faixa_saldo_medio="4", faixa_cash_in_global="4",
    )
    assert row["status_qualificacao"].startswith("Status: D")


def test_status_qualificacao_E_perdeu_qualificacao():
    # faixa_maximo < 4 → faixa_alvo != "MAX"
    row = _enrich_single(
        ja_pago_comiss="300", previsao_comiss="0",
        faixa_cash_in="1", faixa_domicilio="1",
        faixa_saldo_medio="1", faixa_cash_in_global="1",
    )
    assert row["status_qualificacao"].startswith("Status: E")


# ===========================================================================
# ENRICH — faixa_maximo exclui faixa_spending
# ===========================================================================

def test_faixa_maximo_does_not_include_faixa_spending():
    """faixa_spending=5 não deve elevar faixa_maximo acima do máximo das outras."""
    row = _enrich_single(
        faixa_cash_in="1",
        faixa_domicilio="2",
        faixa_saldo_medio="1",
        faixa_spending="5",       # alta, mas excluída do cálculo
        faixa_cash_in_global="2",
    )
    assert row["faixa_maximo"] == 2


def test_faixa_maximo_uses_max_of_cash_domicilio_saldo_global():
    row = _enrich_single(
        faixa_cash_in="3",
        faixa_domicilio="2",
        faixa_saldo_medio="1",
        faixa_spending="0",
        faixa_cash_in_global="2",
    )
    assert row["faixa_maximo"] == 3


# ===========================================================================
# ENRICH — thresholds e gaps
# ===========================================================================

def test_threshold_is_zero_when_faixa_maximo_is_4_or_more():
    row = _enrich_single(
        faixa_cash_in="4", faixa_domicilio="4",
        faixa_saldo_medio="4", faixa_cash_in_global="4",
    )
    assert row["threshold_cash_in"] == 0
    assert row["threshold_spending"] == 0
    assert row["thereshold_saldo_medio"] == 0


def test_gap_cash_in_is_zero_when_current_value_exceeds_target():
    # faixa_alvo_num = 2 → target_cash_in = 15000; current = 20000 → gap = 0
    row = _enrich_single(
        faixa_cash_in="1", faixa_domicilio="1",
        faixa_saldo_medio="1", faixa_cash_in_global="1",
        vl_cash_in_mtd="20000",
    )
    assert row["gap_cash_in"] == 0


def test_gap_cash_in_is_positive_when_below_target():
    # faixa_alvo_num = 2 → target_cash_in = 15000; current = 5000 → gap = 10000
    row = _enrich_single(
        faixa_cash_in="1", faixa_domicilio="1",
        faixa_saldo_medio="1", faixa_cash_in_global="1",
        vl_cash_in_mtd="5000",
    )
    assert row["gap_cash_in"] == 10000
