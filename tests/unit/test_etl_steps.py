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


def test_enrich_computes_model_columns():
    """Verifica que run_enrich produz as colunas do modelo (108) e não as antigas."""
    from shared.visao_cliente_schema import REQUIRED_COLUMNS

    base_df = pd.DataFrame(
        [
            {
                "data_base": "2026-03-02 00:00:00",
                "dt_conta_criada": "2026-02-20",
                "limite_cartao": "6000",
                "limite_conta": "2000",
                "limite_alocado_cartao_cdb": "0",
                "dt_entrega_cartao": "2026-01-15",
                "dt_ativ_cartao_cred": "2026-01-16",
                "vl_spending_total_mtd": "500",
                "fl_elegivel_venda_c6pay": "1",
                "dt_install_maq": None,
                "dt_ativacao_pay": None,
                "c6pay_ativa_30": "0",
                "dt_cancelamento_maq": None,
                "dt_ult_trans_pay": None,
                "fl_bolcob_cadastrado": "1",
                "dt_prim_liq_bolcob": None,
                "tpv_bolcob_potencial": "0",
                "chaves_pix_forte": "CNPJ",
                "dt_conta_criada_global": None,
                "ja_pago_comiss": "120",
                "previsao_comiss": "0",
                "faixa_cash_in": "1",
                "faixa_domicilio": "2",
                "faixa_saldo_medio": "4",
                "faixa_spending": "2",
                "faixa_cash_in_global": "1",
                "vl_cash_in_mtd": "3000",
                "vl_spending_total_mtd": "500",
                "vl_saldo_medio_mensalizado": "500",
                "vl_cash_in_conta_global_mtd": "1000",
                "tpv_m0": "100",
                "tpv_m1": "200",
                "tpv_m2": "300",
            }
        ]
    )

    with patch("worker.steps.enrich.is_step_done", return_value=False), \
         patch("worker.steps.enrich.begin_step"), \
         patch("worker.steps.enrich.get_cached_dataframe", return_value=base_df), \
         patch("worker.steps.enrich.set_cached_dataframe") as mock_set, \
         patch("worker.steps.enrich.mark_step_done"):
        from worker.steps.enrich import run_enrich

        run_enrich(MagicMock(), "job-1")

    enriched = mock_set.call_args.args[1]
    row = enriched.iloc[0]

    # Colunas do modelo presentes
    assert list(enriched.columns) == REQUIRED_COLUMNS, "Colunas não batem com REQUIRED_COLUMNS"

    # Novas colunas calculadas corretamente
    assert row["total_tpv"] == 600.0
    assert row["status_cartao"] == "ATIVOU CREDITO - UTILIZANDO"
    assert row["status_maq"] == "ELEGIVEL - SEM VENDA"
    assert row["status_bolcbob"] == "BOLETO CADASTRADO - NUNCA EMITIDO"
    assert "Chave forte ativa" in row["insight_pix_forte"]
    assert row["insight_conta_global"] == "Sem Conta Global."
    assert row["faixa_maximo"] == 4
    assert row["faixa_alvo"] == "MAX"
    assert row["threshold_cash_in"] == 0  # is_max=True → threshold=0
    # ja_pago=120, previsao=0, faixa_alvo="MAX" → Status D
    assert row["status_qualificacao"].startswith("Status: D")

    # Colunas antigas não devem existir
    removed = [
        "nivel_cartao", "nivel_conta", "cancelamento_maq", "elegivel_c6",
        "safra_boleto", "idade_safra_boleto", "safra_maquina", "idade_safra_maquina",
        "metrica_ativacao", "metrica_progresso", "metrica_urgencia",
        "metrica_financeiro", "metrica_intencao", "score_perfil",
        "ja_recebeu_comissao", "comissao_prox_mes",
        "dias_desde_abertura", "m2_dias_faltantes",
        "faixa_max", "threshiold_cash_in", "threshold_saldo_medio",
    ]
    for col in removed:
        assert col not in enriched.columns, f"Coluna removida ainda presente: {col}"


def test_required_columns_match_model():
    """REQUIRED_COLUMNS deve ter exatamente 108 colunas do modelo."""
    from shared.visao_cliente_schema import REQUIRED_COLUMNS

    assert len(REQUIRED_COLUMNS) == 108
    # Novas colunas obrigatórias
    for col in ("total_tpv", "status_cartao", "status_maq", "status_bolcbob",
                "insight_cartao", "insight_maq", "insight_bolcob",
                "insight_pix_forte", "insight_conta_global",
                "faixa_maximo", "threshold_cash_in", "thereshold_saldo_medio",
                "status_qualificacao"):
        assert col in REQUIRED_COLUMNS, f"Coluna do modelo ausente: {col}"
    # Colunas antigas não devem estar presentes
    for col in ("faixa_max", "threshiold_cash_in", "threshold_saldo_medio",
                "nivel_cartao", "nivel_conta", "score_perfil",
                "cancelamento_maq", "safra_boleto"):
        assert col not in REQUIRED_COLUMNS, f"Coluna removida ainda presente: {col}"
