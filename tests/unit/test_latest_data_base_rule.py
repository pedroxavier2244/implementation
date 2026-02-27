from unittest.mock import MagicMock, patch

import pytest

from shared.visao_cliente_schema import UPSERT_CONFLICT_COLUMNS
from worker.steps.clean import _normalize_data_base, _normalize_document


def test_conflict_key_is_cliente_only():
    assert UPSERT_CONFLICT_COLUMNS == ("cd_cpf_cnpj_cliente",)


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
        None,
    ]

    with patch("worker.steps.upsert.is_step_done", return_value=False), patch(
        "worker.steps.upsert.begin_step"
    ), patch("worker.steps.upsert.mark_step_done") as mock_mark_done:
        from worker.steps.upsert import run_upsert

        run_upsert(session, "job-1")

    assert session.execute.call_count == 2

    sql_text = str(session.execute.call_args_list[1].args[0])
    assert "ROW_NUMBER() OVER" in sql_text
    assert "PARTITION BY cd_cpf_cnpj_cliente" in sql_text
    assert "ORDER BY data_base DESC NULLS LAST" in sql_text
    assert "ON CONFLICT (cd_cpf_cnpj_cliente)" in sql_text
    assert "COALESCE(EXCLUDED.data_base, '') >= COALESCE(final_visao_cliente.data_base, '')" in sql_text

    params = session.execute.call_args_list[1].args[1]
    assert params["job_id"] == "job-1"
    mock_mark_done.assert_called_once()
