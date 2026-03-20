from unittest.mock import MagicMock, patch

import pytest

from shared.visao_cliente_schema import REQUIRED_COLUMNS, UPSERT_CONFLICT_COLUMNS, UPSERT_CONFLICT_WHERE
from worker.steps.clean import _normalize_data_base, _normalize_document


def test_conflict_key_is_cliente_only():
    assert UPSERT_CONFLICT_COLUMNS == ("cd_cpf_cnpj_cliente",)
    assert UPSERT_CONFLICT_WHERE == "cd_cpf_cnpj_cliente IS NOT NULL"


def test_required_columns_include_model_fields():
    # nivel_cartao / nivel_conta foram removidos do modelo oficial
    assert "nivel_cartao" not in REQUIRED_COLUMNS
    assert "nivel_conta" not in REQUIRED_COLUMNS
    # Colunas novas do modelo devem estar presentes
    assert "status_cartao" in REQUIRED_COLUMNS
    assert "status_maq" in REQUIRED_COLUMNS
    assert "faixa_maximo" in REQUIRED_COLUMNS


def test_normalize_document_removes_mask_and_excel_decimal():
    assert _normalize_document("12.345.678/0001-90") == "12345678000190"
    assert _normalize_document("7501147000104.0") == "7501147000104"
    assert _normalize_document("nan") is None


def test_normalize_data_base_to_canonical_timestamp():
    pytest.importorskip("pandas")
    assert _normalize_data_base("21/02/2026") == "2026-02-21 00:00:00"
    assert _normalize_data_base("nan") is None


def test_upsert_sql_uses_latest_data_base_per_cliente():
    session = MagicMock()
    session.execute.side_effect = [
        [("data_base",), ("cd_cpf_cnpj_cliente",), ("nome_cliente",)],
        None,  # CREATE TEMP TABLE _upsert_source
        None,  # CREATE INDEX ON _upsert_source
        None,  # INSERT change_history new rows
        None,  # INSERT change_history update rows
        None,  # main upsert
    ]

    with patch("worker.steps.upsert.is_step_done", return_value=False), patch(
        "worker.steps.upsert.begin_step"
    ), patch("worker.steps.upsert.mark_step_done") as mock_mark_done:
        from worker.steps.upsert import run_upsert

        run_upsert(session, "job-1")

    assert session.execute.call_count == 6

    # CREATE TEMP TABLE deve conter ROW_NUMBER e receber job_id como param
    create_temp_sql = str(session.execute.call_args_list[1].args[0])
    assert "ROW_NUMBER() OVER" in create_temp_sql
    assert "PARTITION BY cd_cpf_cnpj_cliente" in create_temp_sql
    assert "ORDER BY data_base DESC NULLS LAST" in create_temp_sql
    params_temp = session.execute.call_args_list[1].args[1]
    assert params_temp["job_id"] == "job-1"

    insert_new_sql = str(session.execute.call_args_list[3].args[0])
    assert "visao_cliente_change_history" in insert_new_sql
    assert "'INSERT'" in insert_new_sql

    insert_updates_sql = str(session.execute.call_args_list[4].args[0])
    assert "jsonb_build_object" in insert_updates_sql
    assert "visao_cliente_change_history" in insert_updates_sql
    assert "jsonb_object_keys" in insert_updates_sql

    sql_text = str(session.execute.call_args_list[5].args[0])
    assert "ON CONFLICT (cd_cpf_cnpj_cliente) WHERE cd_cpf_cnpj_cliente IS NOT NULL" in sql_text
    assert "COALESCE(EXCLUDED.data_base, '') >= COALESCE(final_visao_cliente.data_base, '')" in sql_text
    assert "_upsert_source" in sql_text

    mock_mark_done.assert_called_once()


def test_upsert_does_not_backfill_levels():
    """nivel_cartao/nivel_conta foram removidos do modelo — backfill não deve ocorrer."""
    session = MagicMock()
    session.execute.side_effect = [
        [("data_base",), ("cd_cpf_cnpj_cliente",), ("nome_cliente",)],
        None,  # CREATE TEMP TABLE _upsert_source
        None,  # CREATE INDEX ON _upsert_source
        None,  # INSERT change_history new rows
        None,  # INSERT change_history update rows
        None,  # main upsert
    ]

    with patch("worker.steps.upsert.is_step_done", return_value=False), patch(
        "worker.steps.upsert.begin_step"
    ), patch("worker.steps.upsert.mark_step_done"):
        from worker.steps.upsert import run_upsert

        run_upsert(session, "job-level")

    # 6 chamadas: schema query + CREATE TEMP + CREATE INDEX + change_history (insert) + change_history (update) + upsert
    assert session.execute.call_count == 6
    for call in session.execute.call_args_list:
        sql = str(call.args[0])
        assert "nivel_cartao" not in sql
        assert "nivel_conta" not in sql
