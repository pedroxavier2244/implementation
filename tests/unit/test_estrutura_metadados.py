"""
Testes de validação de estrutura (metadados):
- normalize_column_name: strips, accents, %, /, digit-start
- _missing_required_columns: report exato do que falta
- run_validate: abortagem com ValueError quando schema inválido
- STAGING_TO_CRM_COLUMN_MAP / CRM_TO_STAGING_COLUMN_MAP: consistência bidirecional
"""
import pytest
from unittest.mock import MagicMock, patch

pd = pytest.importorskip("pandas")

from shared.visao_cliente_schema import (
    REQUIRED_COLUMNS,
    STAGING_TO_CRM_COLUMN_MAP,
    CRM_TO_STAGING_COLUMN_MAP,
    normalize_column_name,
    crm_row_to_snake,
)
from worker.steps.validate import _missing_required_columns


# ---------------------------------------------------------------------------
# normalize_column_name
# ---------------------------------------------------------------------------

def test_normalize_lowercases_ascii():
    assert normalize_column_name("DATA_BASE") == "data_base"


def test_normalize_strips_diacritics():
    assert normalize_column_name("Fundação") == "fundacao"
    assert normalize_column_name("Visão Cliente") == "visao_cliente"


def test_normalize_replaces_percent_with_pct():
    assert normalize_column_name("%_CASH_IN") == "pct_cash_in"
    assert normalize_column_name("MAIOR_PROGRESSO%") == "maior_progresso_pct"


def test_normalize_replaces_slash_with_underscore():
    assert normalize_column_name("CASH_IN/OUT") == "cash_in_out"


def test_normalize_collapses_multiple_separators():
    assert normalize_column_name("A  B") == "a_b"
    assert normalize_column_name("A__B") == "a_b"


def test_normalize_strips_leading_and_trailing_underscores():
    result = normalize_column_name(" NOME ")
    assert not result.startswith("_")
    assert not result.endswith("_")


def test_normalize_prefixes_column_starting_with_digit():
    result = normalize_column_name("3M_MEDIA")
    assert result.startswith("col_")


def test_normalize_handles_percent_sign_in_middle():
    # "%_DOMICILIO" → "pct__domicilio" → collapsed → "pct_domicilio"
    result = normalize_column_name("%_DOMICILIO")
    assert result == "pct_domicilio"


# ---------------------------------------------------------------------------
# _missing_required_columns
# ---------------------------------------------------------------------------

def test_missing_columns_empty_input_returns_all_required():
    missing = _missing_required_columns([])
    assert set(missing) == set(REQUIRED_COLUMNS)


def test_missing_columns_all_present_returns_empty():
    assert _missing_required_columns(REQUIRED_COLUMNS) == []


def test_missing_columns_reports_single_absent_column():
    without_data_base = [c for c in REQUIRED_COLUMNS if c != "data_base"]
    missing = _missing_required_columns(without_data_base)
    assert missing == ["data_base"]


def test_missing_columns_extra_columns_are_ignored():
    with_extra = REQUIRED_COLUMNS + ["coluna_desconhecida_extra"]
    assert _missing_required_columns(with_extra) == []


def test_missing_columns_input_is_normalized_before_check():
    # "DATA BASE" → normalize_column_name → "data_base"
    uppercase_spaced = [c.upper().replace("_", " ") for c in REQUIRED_COLUMNS]
    assert _missing_required_columns(uppercase_spaced) == []


# ---------------------------------------------------------------------------
# run_validate: abortagem por schema inválido
# ---------------------------------------------------------------------------

def test_validate_raises_value_error_on_missing_columns():
    session = MagicMock()
    df = pd.DataFrame({"coluna_irrelevante": [1, 2, 3]})
    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        from worker.steps.validate import run_validate
        with pytest.raises(ValueError, match="missing columns"):
            run_validate(session, "job-1", MagicMock())


def test_validate_raises_before_processing_any_rows():
    """Falha de schema ocorre antes de qualquer merge de bad rows."""
    session = MagicMock()
    df = pd.DataFrame({"coluna_irrelevante": [1]})
    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        from worker.steps.validate import run_validate
        with pytest.raises(ValueError):
            run_validate(session, "job-1", MagicMock())
        session.merge.assert_not_called()


def test_validate_passes_with_all_109_required_columns():
    session = MagicMock()
    job = MagicMock()
    job.rows_total = None
    session.query.return_value.filter_by.return_value.first.return_value = job
    df = pd.DataFrame([{c: "valor" for c in REQUIRED_COLUMNS}])
    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.mark_step_done"), \
         patch("worker.steps.validate.get_settings") as mock_cfg, \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        mock_cfg.return_value.BAD_ROW_THRESHOLD_PCT = 5.0
        from worker.steps.validate import run_validate
        run_validate(session, "job-1", MagicMock())  # não deve levantar


def test_validate_passes_with_required_plus_extra_columns():
    session = MagicMock()
    job = MagicMock()
    session.query.return_value.filter_by.return_value.first.return_value = job
    cols = {c: "x" for c in REQUIRED_COLUMNS}
    cols["coluna_adicional"] = "y"
    df = pd.DataFrame([cols])
    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.mark_step_done"), \
         patch("worker.steps.validate.get_settings") as mock_cfg, \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        mock_cfg.return_value.BAD_ROW_THRESHOLD_PCT = 5.0
        from worker.steps.validate import run_validate
        run_validate(session, "job-1", MagicMock())  # não deve levantar


# ---------------------------------------------------------------------------
# STAGING_TO_CRM_COLUMN_MAP / CRM_TO_STAGING_COLUMN_MAP
# ---------------------------------------------------------------------------

def test_pct_columns_map_to_perc_prefix():
    assert STAGING_TO_CRM_COLUMN_MAP["pct_domicilio"] == "PERC_DOMICILIO"
    assert STAGING_TO_CRM_COLUMN_MAP["pct_cash_in"] == "PERC_CASH_IN"
    assert STAGING_TO_CRM_COLUMN_MAP["pct_spending"] == "PERC_SPENDING"
    assert STAGING_TO_CRM_COLUMN_MAP["pct_saldo_medio"] == "PERC_SALDO_MEDIO"
    assert STAGING_TO_CRM_COLUMN_MAP["pct_conta_global"] == "PERC_CONTA_GLOBAL"


def test_maior_progresso_pct_maps_to_maior_progresso_perc():
    assert STAGING_TO_CRM_COLUMN_MAP["maior_progresso_pct"] == "MAIOR_PROGRESSO_PERC"


def test_crm_to_staging_map_is_exact_inverse_of_staging_to_crm():
    for staging, crm in STAGING_TO_CRM_COLUMN_MAP.items():
        assert CRM_TO_STAGING_COLUMN_MAP.get(crm) == staging, \
            f"Mapa inverso quebrado: {crm} → esperado '{staging}', " \
            f"obtido '{CRM_TO_STAGING_COLUMN_MAP.get(crm)}'"


def test_crm_row_to_snake_converts_perc_to_pct():
    row = {"PERC_DOMICILIO": 0.75, "PERC_CASH_IN": 0.5}
    result = crm_row_to_snake(row)
    assert result["pct_domicilio"] == 0.75
    assert result["pct_cash_in"] == 0.5


def test_crm_row_to_snake_lowercases_unmapped_columns():
    row = {"DATA_BASE": "01/01/2026", "NOME_CLIENTE": "Empresa X"}
    result = crm_row_to_snake(row)
    assert result["data_base"] == "01/01/2026"
    assert result["nome_cliente"] == "Empresa X"
