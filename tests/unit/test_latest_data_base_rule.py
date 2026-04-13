from unittest.mock import MagicMock, patch

import pytest

from shared.visao_cliente_schema import REQUIRED_COLUMNS, UPSERT_CONFLICT_COLUMNS, UPSERT_CONFLICT_WHERE
from worker.steps.clean import _normalize_data_base, _normalize_document


def test_conflict_key_is_cliente_only():
    assert UPSERT_CONFLICT_COLUMNS == ("CD_CPF_CNPJ_CLIENTE",)
    assert '"CD_CPF_CNPJ_CLIENTE" IS NOT NULL' in UPSERT_CONFLICT_WHERE


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
    assert _normalize_data_base("21/02/2026") == "21/02/2026"
    assert _normalize_data_base("nan") is None


def test_upsert_sql_uses_latest_data_base_per_cliente():
    """Temp table deve deduplicar por CNPJ usando DATA_BASE mais recente."""
    session = MagicMock()
    # Ordem das chamadas a session.execute em run_upsert (sem historico_only):
    # 0: SELECT column_name (staging cols)
    # 1: DROP TABLE IF EXISTS _upsert_source
    # 2: CREATE TEMP TABLE _upsert_source
    # 3: CREATE INDEX ON _upsert_source
    # 4: SELECT tablename FROM pg_tables (backups antigos)
    # 5: CREATE TABLE backup
    # 6: SELECT DISTINCT DATA_BASE (existing — vazio neste teste)
    # 7: INSERT INTO projetinho_pai (upsert)
    # 8: DELETE FROM lead_atribuicoes (stale)
    # 9: DELETE FROM projetinho_pai (stale)
    session.execute.side_effect = [
        [("data_base",), ("cd_cpf_cnpj_cliente",), ("nome_cliente",)],
        None,  # DROP TABLE IF EXISTS _upsert_source
        None,  # CREATE TEMP TABLE _upsert_source
        None,  # CREATE INDEX ON _upsert_source
        MagicMock(fetchall=MagicMock(return_value=[])),  # pg_tables: nenhum backup antigo
        None,  # CREATE TABLE backup
        MagicMock(fetchall=MagicMock(return_value=[])),  # SELECT DISTINCT DATA_BASE (vazio)
        MagicMock(rowcount=3),  # INSERT INTO projetinho_pai
        MagicMock(rowcount=0),  # DELETE FROM lead_atribuicoes stale
        MagicMock(rowcount=0),  # DELETE FROM projetinho_pai stale
    ]

    with patch("worker.steps.upsert.is_step_done", return_value=False), patch(
        "worker.steps.upsert.begin_step"
    ), patch("worker.steps.upsert.mark_step_done") as mock_mark_done, patch(
        "worker.steps.upsert._prune_historico"
    ):
        from worker.steps.upsert import run_upsert
        run_upsert(session, "job-1")

    # CREATE TEMP TABLE deve usar ROW_NUMBER particionado por CNPJ
    create_temp_sql = str(session.execute.call_args_list[2].args[0])
    assert "ROW_NUMBER() OVER" in create_temp_sql
    assert "PARTITION BY cd_cpf_cnpj_cliente" in create_temp_sql
    assert "ORDER BY data_base DESC NULLS LAST" in create_temp_sql

    # INSERT final deve ir para projetinho_pai (índice 7 após adição de backup)
    insert_sql = str(session.execute.call_args_list[7].args[0])
    assert "projetinho_pai" in insert_sql
    assert "_upsert_source" in insert_sql

    mock_mark_done.assert_called_once()


def test_upsert_does_not_backfill_levels():
    """nivel_cartao/nivel_conta foram removidos do modelo — backfill não deve ocorrer."""
    session = MagicMock()
    session.execute.side_effect = [
        [("data_base",), ("cd_cpf_cnpj_cliente",), ("nome_cliente",)],
        None,  # DROP TEMP
        None,  # CREATE TEMP TABLE _upsert_source
        None,  # CREATE INDEX ON _upsert_source
        MagicMock(fetchall=MagicMock(return_value=[])),  # pg_tables: nenhum backup antigo
        None,  # CREATE TABLE backup
        MagicMock(fetchall=MagicMock(return_value=[])),  # SELECT DISTINCT DATA_BASE (vazio)
        MagicMock(rowcount=0),  # INSERT INTO projetinho_pai
        MagicMock(rowcount=0),  # DELETE FROM lead_atribuicoes stale
        MagicMock(rowcount=0),  # DELETE FROM projetinho_pai stale
    ]

    with patch("worker.steps.upsert.is_step_done", return_value=False), patch(
        "worker.steps.upsert.begin_step"
    ), patch("worker.steps.upsert.mark_step_done"), patch(
        "worker.steps.upsert._prune_historico"
    ):
        from worker.steps.upsert import run_upsert
        run_upsert(session, "job-level")

    for call in session.execute.call_args_list:
        sql = str(call.args[0])
        assert "nivel_cartao" not in sql
        assert "nivel_conta" not in sql
