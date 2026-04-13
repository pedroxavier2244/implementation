# tests/unit/test_cnpj_enrich.py
from unittest.mock import MagicMock, patch


def _make_session(cnpjs: list[str]):
    """Simula session que retorna CNPJs do arquivo atual."""
    session = MagicMock()
    rows = [(cnpj,) for cnpj in cnpjs]
    session.execute.return_value.fetchall.return_value = rows
    return session


def test_cnpj_enrich_updates_cnae_ramo_and_propensao():
    """Quando API retorna CNAE mapeado, atualiza os 3 campos."""
    session = _make_session(["12345678000195"])
    with patch("worker.steps.cnpj_enrich.fetch_cnae_code", return_value="4721-1"):
        from worker.steps.cnpj_enrich import run_cnpj_enrich
        run_cnpj_enrich(session, "job-1")
    update_calls = [str(c[0][0]) for c in session.execute.call_args_list]
    update_sql = " ".join(update_calls)
    assert "CNAE" in update_sql
    assert "RAMO_ATUACAO" in update_sql
    assert "FL_PROPENSAO_C6PAY" in update_sql
    session.commit.assert_called_once()


def test_cnpj_enrich_uses_fallback_when_cnae_not_in_map():
    """CNAE válido mas não mapeado → ramo='outros', propensao='0'."""
    session = _make_session(["12345678000195"])
    with patch("worker.steps.cnpj_enrich.fetch_cnae_code", return_value="9999-9"):
        from worker.steps.cnpj_enrich import run_cnpj_enrich
        run_cnpj_enrich(session, "job-1")
    params = session.execute.call_args_list[-1][0][1]
    assert params["ramo"] == "outros"
    assert params["c6pay"] == "0"


def test_cnpj_enrich_skips_update_when_api_returns_none():
    """API retorna None → não gera UPDATE, apenas commit."""
    session = _make_session(["12345678000195"])
    with patch("worker.steps.cnpj_enrich.fetch_cnae_code", return_value=None):
        from worker.steps.cnpj_enrich import run_cnpj_enrich
        run_cnpj_enrich(session, "job-1")
    update_calls = [str(c[0][0]) for c in session.execute.call_args_list
                    if "UPDATE" in str(c[0][0]).upper()]
    assert len(update_calls) == 0
    session.commit.assert_called_once()


def test_cnpj_enrich_skips_cpfs():
    """CPFs (11 dígitos) são ignorados, API não é chamada."""
    session = _make_session(["12345678901"])
    with patch("worker.steps.cnpj_enrich.fetch_cnae_code") as mock_fetch:
        from worker.steps.cnpj_enrich import run_cnpj_enrich
        run_cnpj_enrich(session, "job-1")
    mock_fetch.assert_not_called()


def test_cnpj_enrich_skips_when_no_cnpjs():
    """Arquivo sem CNPJs → nenhuma chamada à API, só commit."""
    session = _make_session([])
    with patch("worker.steps.cnpj_enrich.fetch_cnae_code") as mock_fetch:
        from worker.steps.cnpj_enrich import run_cnpj_enrich
        run_cnpj_enrich(session, "job-1")
    mock_fetch.assert_not_called()
    session.commit.assert_called_once()
