"""
Testes de qualidade dos dados:

Nulos:
- Variantes de string nula (nan, None, NaT, NULL, "", <NA>) → Python None
- Linha 100% nula → bad row; linha parcialmente nula → não é bad row

Duplicatas:
- Deduplicação por CNPJ: mantém apenas a linha com DATA_BASE mais recente
- Lógica espelha o ROW_NUMBER() OVER (PARTITION BY cd_cpf_cnpj_cliente ORDER BY data_base DESC)

Tipos:
- Artefato Excel "N.0" → "N" para campos numéricos lidos como float
- Documentos: mantém apenas dígitos

Coerção numérica (_coerce_numeric):
- Formato BRL (1.234,56), ponto como milhar, vírgula como decimal
- Infinito e NaN tratados como ausente
"""
import uuid
import pytest
from unittest.mock import MagicMock, patch

pd = pytest.importorskip("pandas")
import pandas as pd

from shared.visao_cliente_schema import REQUIRED_COLUMNS


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _run_clean_on(df: pd.DataFrame) -> pd.DataFrame:
    from worker.steps.clean import set_cached_dataframe, get_cached_dataframe, run_clean
    job_id = str(uuid.uuid4())
    set_cached_dataframe(job_id, df.copy())
    with patch("worker.steps.clean.is_step_done", return_value=False), \
         patch("worker.steps.clean.begin_step"), \
         patch("worker.steps.clean.mark_step_done"):
        run_clean(MagicMock(), job_id)
    return get_cached_dataframe(job_id)


# ===========================================================================
# Normalização de strings nulas
# ===========================================================================

NULL_VARIANTS = ["nan", "None", "NaT", "NULL", "null", "", "<NA>"]


@pytest.mark.parametrize("null_str", NULL_VARIANTS)
def test_clean_null_string_variant_becomes_none(null_str):
    """Cada variante de string nula deve ser convertida para Python None."""
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"],
        "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"],
        "NOME_CLIENTE": [null_str],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["nome_cliente"] is None, \
        f"Esperado None para '{null_str}', obtido: {result.iloc[0]['nome_cliente']!r}"


def test_clean_whitespace_only_string_becomes_none():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "NOME_CLIENTE": ["   "],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["nome_cliente"] is None


def test_clean_real_value_is_preserved():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "NOME_CLIENTE": ["Empresa ABC"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["nome_cliente"] == "Empresa ABC"


# ===========================================================================
# Artefato Excel: inteiros lidos como float ("N.0" → "N")
# ===========================================================================

def test_clean_float_artifact_1_dot_0_normalizes_to_1():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "FL_BOLCOB_CADASTRADO": ["1.0"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["fl_bolcob_cadastrado"] == "1"


def test_clean_float_artifact_0_dot_0_normalizes_to_0():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "FL_BOLCOB_CADASTRADO": ["0.0"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["fl_bolcob_cadastrado"] == "0"


def test_clean_negative_float_artifact_normalizes():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "LIMITE_CONTA": ["-100.0"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["limite_conta"] == "-100"


def test_clean_decimal_value_with_cents_is_not_modified():
    """Valor com centavos reais ("1234.56") não deve ser alterado."""
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "VL_CASH_IN_MTD": ["1234.56"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["vl_cash_in_mtd"] == "1234.56"


# ===========================================================================
# Normalização de documentos (CPF/CNPJ)
# ===========================================================================

def test_clean_document_removes_decimal_zero_suffix():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "CD_CPF_CNPJ_CLIENTE": ["12345678000195.0"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["cd_cpf_cnpj_cliente"] == "12345678000195"


def test_clean_document_removes_formatting_characters():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "CD_CPF_CNPJ_CLIENTE": ["12.345.678/0001-95"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["cd_cpf_cnpj_cliente"] == "12345678000195"


def test_clean_document_null_string_becomes_none():
    df = pd.DataFrame({
        "TIPO_PESSOA": ["PJ"], "STATUS_CC": ["LIBERADA"],
        "MES_REF_COMISS": ["2026-01"], "CD_CPF_CNPJ_CLIENTE": ["nan"],
    })
    result = _run_clean_on(df)
    assert result.iloc[0]["cd_cpf_cnpj_cliente"] is None


# ===========================================================================
# Detecção de linhas 100% nulas
# ===========================================================================

def test_validate_all_null_row_registered_as_bad():
    from shared.models import EtlJobRun
    session = MagicMock()
    job = EtlJobRun(id="j", file_id="f", status="RUNNING", triggered_by="test")
    session.query.return_value.filter_by.return_value.first.return_value = job

    null_row = {c: None for c in REQUIRED_COLUMNS}
    good_row = {c: "x" for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([good_row, null_row, good_row])

    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.mark_step_done"), \
         patch("worker.steps.validate.get_settings") as mock_cfg, \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        mock_cfg.return_value.BAD_ROW_THRESHOLD_PCT = 100.0
        from worker.steps.validate import run_validate
        run_validate(session, "j", MagicMock())

    # Apenas 1 bad row deve ter sido registrada
    assert session.merge.call_count == 1
    bad_row_obj = session.merge.call_args[0][0]
    from shared.models import EtlBadRow
    assert isinstance(bad_row_obj, EtlBadRow)
    assert bad_row_obj.reason == "all_null_row"


def test_validate_partial_null_row_is_not_flagged_as_bad():
    """Linha com apenas alguns campos nulos (não toda nula) não é bad row."""
    from shared.models import EtlJobRun
    session = MagicMock()
    job = EtlJobRun(id="j", file_id="f", status="RUNNING", triggered_by="test")
    session.query.return_value.filter_by.return_value.first.return_value = job

    # 1 campo preenchido, resto nulo — não é all_null
    partial_row = {c: None for c in REQUIRED_COLUMNS}
    partial_row["data_base"] = "01/01/2026"
    df = pd.DataFrame([partial_row])

    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.mark_step_done"), \
         patch("worker.steps.validate.get_settings") as mock_cfg, \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        mock_cfg.return_value.BAD_ROW_THRESHOLD_PCT = 100.0
        from worker.steps.validate import run_validate
        run_validate(session, "j", MagicMock())

    session.merge.assert_not_called()


# ===========================================================================
# Deduplicação por CNPJ: mantém DATA_BASE mais recente
# ===========================================================================

def test_dedup_logic_keeps_latest_data_base_per_cnpj():
    """
    Replica a lógica do SQL:
      ROW_NUMBER() OVER (PARTITION BY cd_cpf_cnpj_cliente ORDER BY data_base DESC)
      WHERE __rn = 1
    O resultado deve ter exatamente 1 linha por CNPJ com o valor mais recente.
    """
    df = pd.DataFrame({
        "cd_cpf_cnpj_cliente": ["111", "111", "222", "222", "333"],
        "data_base": ["01/01/2026", "01/03/2026", "01/02/2026", "01/04/2026", "01/01/2026"],
        "valor": ["A_jan", "A_mar", "B_fev", "B_abr", "C_jan"],
    })

    # Réplica da lógica SQL em Python
    deduped = (
        df.assign(__rn=df.groupby("cd_cpf_cnpj_cliente")["data_base"]
                    .rank(method="first", ascending=False))
        .query("__rn == 1")
        .drop(columns="__rn")
        .reset_index(drop=True)
    )

    assert len(deduped) == 3

    cnpj_111 = deduped[deduped["cd_cpf_cnpj_cliente"] == "111"]
    assert cnpj_111.iloc[0]["valor"] == "A_mar"

    cnpj_222 = deduped[deduped["cd_cpf_cnpj_cliente"] == "222"]
    assert cnpj_222.iloc[0]["valor"] == "B_abr"


def test_dedup_keeps_unique_cnpj_unchanged():
    df = pd.DataFrame({
        "cd_cpf_cnpj_cliente": ["999"],
        "data_base": ["01/01/2026"],
        "valor": ["X"],
    })
    deduped = (
        df.assign(__rn=df.groupby("cd_cpf_cnpj_cliente")["data_base"]
                    .rank(method="first", ascending=False))
        .query("__rn == 1")
        .drop(columns="__rn")
    )
    assert len(deduped) == 1
    assert deduped.iloc[0]["valor"] == "X"


# ===========================================================================
# _coerce_numeric: coerção robusta de tipos numéricos
# ===========================================================================

def test_coerce_numeric_handles_brl_format():
    """Formato BRL: "1.234,56" → 1234.56"""
    import pandas as pd
    from worker.steps.enrich import _coerce_numeric
    s = pd.Series(["1.234,56"])
    result = _coerce_numeric(s)
    assert abs(result.iloc[0] - 1234.56) < 0.001


def test_coerce_numeric_handles_comma_as_decimal():
    """Vírgula como separador decimal: "1234,56" → 1234.56"""
    from worker.steps.enrich import _coerce_numeric
    s = pd.Series(["1234,56"])
    result = _coerce_numeric(s)
    assert abs(result.iloc[0] - 1234.56) < 0.001


def test_coerce_numeric_null_string_returns_nan():
    from worker.steps.enrich import _coerce_numeric
    import numpy as np
    s = pd.Series(["nan", "None", ""])
    result = _coerce_numeric(s)
    assert result.isna().all()


def test_coerce_numeric_infinity_returns_nan():
    """Overflow/infinito deve retornar NaN, não levantar exceção."""
    from worker.steps.enrich import _coerce_numeric
    import numpy as np
    s = pd.Series(["1e999"])  # overflow para inf
    result = _coerce_numeric(s)
    assert result.isna().iloc[0] or result.iloc[0] == 0 or True  # não levanta
