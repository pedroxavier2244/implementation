import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch


def make_session_mock(step_exists=False):
    from shared.models import EtlJobStep
    session = MagicMock()
    existing = EtlJobStep(status="DONE") if step_exists else None
    session.query().filter_by().first.return_value = existing
    return session


def test_cnpj_verify_skips_when_already_done():
    with patch("worker.steps.cnpj_verify.is_step_done", return_value=True):
        from worker.steps.cnpj_verify import run_cnpj_verify
        session = MagicMock()
        run_cnpj_verify(session, "job-1")
        session.execute.assert_not_called()


def test_cnpj_is_stale_when_never_checked():
    from worker.steps.cnpj_verify import _is_stale
    assert _is_stale(None, ttl_days=30) is True


def test_cnpj_is_stale_when_old():
    from worker.steps.cnpj_verify import _is_stale
    old_date = datetime.now(timezone.utc) - timedelta(days=31)
    assert _is_stale(old_date, ttl_days=30) is True


def test_cnpj_is_not_stale_when_recent():
    from worker.steps.cnpj_verify import _is_stale
    recent = datetime.now(timezone.utc) - timedelta(days=1)
    assert _is_stale(recent, ttl_days=30) is False


def test_cnpj_verify_skips_cpf_length():
    from worker.steps.cnpj_verify import _is_valid_cnpj
    assert _is_valid_cnpj("12345678901") is False   # 11 digitos — CPF
    assert _is_valid_cnpj("12345678000195") is True  # 14 digitos — CNPJ
    assert _is_valid_cnpj(None) is False


def test_build_divergencia_records():
    from worker.steps.cnpj_verify import _build_divergencias
    divergencias_raw = [
        {"campo": "nome_cliente", "valor_c6": "EMPRESA A", "valor_rf": "EMPRESA B"},
    ]
    records = _build_divergencias("job-1", "11222333000181", divergencias_raw)
    assert len(records) == 1
    assert records[0].job_id == "job-1"
    assert records[0].cnpj == "11222333000181"
    assert records[0].campo == "nome_cliente"
