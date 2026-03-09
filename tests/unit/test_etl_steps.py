from unittest.mock import MagicMock, patch

import pytest

pd = pytest.importorskip("pandas")


def make_session_mock(step_exists=False):
    session = MagicMock()
    from shared.models import EtlJobStep

    existing = EtlJobStep(status="DONE") if step_exists else None
    session.query().filter_by().first.return_value = existing
    return session


def make_file_mock(minio_path="2026/02/27/f.xlsx"):
    file_obj = MagicMock()
    file_obj.minio_path = minio_path
    return file_obj


def test_extract_skips_when_done():
    session = make_session_mock(step_exists=True)
    with patch("worker.steps.extract.MinioClient") as mock_minio:
        from worker.steps.extract import run_extract

        run_extract(session, "job-1", make_file_mock())
        mock_minio.assert_not_called()


def test_validate_aborts_when_threshold_exceeded():
    session = make_session_mock(step_exists=False)
    dataframe = pd.DataFrame({"col1": [None] * 10, "col2": [None] * 10})

    with patch("worker.steps.validate.is_step_done", return_value=False), patch("worker.steps.validate.begin_step"), patch(
        "worker.steps.validate.get_cached_dataframe", return_value=dataframe
    ), patch("worker.steps.validate.get_settings") as mock_settings:
        mock_settings.return_value.BAD_ROW_THRESHOLD_PCT = 5.0
        from worker.steps.validate import run_validate

        with pytest.raises(ValueError, match="threshold"):
            run_validate(session, "job-1", make_file_mock())


def test_clean_skips_when_done():
    with patch("worker.steps.clean.is_step_done", return_value=True):
        from worker.steps.clean import run_clean

        session = MagicMock()
        run_clean(session, "job-1")


def test_normalize_level_value_handles_accents_and_labels():
    from worker.steps.validate import _normalize_level_value

    assert _normalize_level_value("Sem Cartão") == "sem_cartao"
    assert _normalize_level_value("Sem Conta") == "sem_conta"
    assert _normalize_level_value("Médio") == "medio"
    assert _normalize_level_value("nan") is None


def test_validate_marks_invalid_nivel_cartao_as_bad_row():
    from shared.visao_cliente_schema import REQUIRED_COLUMNS

    session = make_session_mock(step_exists=False)
    base_row = {column: None for column in REQUIRED_COLUMNS}
    base_row["cd_cpf_cnpj_cliente"] = "12345678000190"
    base_row["nivel_cartao"] = "FAIXA_X"
    base_row["nivel_conta"] = "Alto"
    dataframe = pd.DataFrame([base_row])

    with patch("worker.steps.validate.is_step_done", return_value=False), patch("worker.steps.validate.begin_step"), patch(
        "worker.steps.validate.mark_step_done"
    ) as mock_mark_done, patch("worker.steps.validate.get_cached_dataframe", return_value=dataframe), patch(
        "worker.steps.validate.get_settings"
    ) as mock_settings:
        mock_settings.return_value.BAD_ROW_THRESHOLD_PCT = 100.0
        from worker.steps.validate import run_validate

        run_validate(session, "job-1", make_file_mock())

    assert session.merge.call_count == 1
    bad_row = session.merge.call_args.args[0]
    assert "invalid_level_value" in bad_row.reason
    assert "nivel_cartao" in bad_row.reason
    mock_mark_done.assert_called_once()


def test_enrich_computes_additional_business_columns():
    base_df = pd.DataFrame(
        [
            {
                "data_base": "2026-03-02 00:00:00",
                "dt_conta_criada": "2026-02-20",
                "limite_cartao": "6000",
                "limite_conta": "2000",
                "ja_pago_comiss": "120",
                "previsao_comiss": "0",
                "faixa_cash_in": "1",
                "faixa_domicilio": "2",
                "faixa_saldo_medio": "4",
                "faixa_spending": "2",
                "faixa_cash_in_global": "1",
                "vl_cash_in_mtd": "3000",
                "vl_spending_total_mtd": "1000",
                "vl_saldo_medio_mensalizado": "500",
                "vl_cash_in_conta_global_mtd": "1000",
            }
        ]
    )

    with patch("worker.steps.enrich.is_step_done", return_value=False), patch("worker.steps.enrich.begin_step"), patch(
        "worker.steps.enrich.get_cached_dataframe", return_value=base_df
    ), patch("worker.steps.enrich.set_cached_dataframe") as mock_set, patch("worker.steps.enrich.mark_step_done"):
        from worker.steps.enrich import run_enrich

        run_enrich(MagicMock(), "job-1")

    enriched = mock_set.call_args.args[1]
    row = enriched.iloc[0]

    assert row["nivel_cartao"] == "Alto"
    assert row["nivel_conta"] == "Medio"
    assert row["ja_recebeu_comissao"] == "SIM"
    assert row["comissao_prox_mes"] == "NÃO"
    assert row["status_qualificacao"] == "D - Topo atingido"
    assert row["faixa_alvo"] == "MAX"


