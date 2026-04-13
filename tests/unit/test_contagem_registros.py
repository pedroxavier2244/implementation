"""
Testes de contagem de registros:
- rows_total, rows_ok, rows_bad setados corretamente em EtlJobRun
- bad rows: apenas linhas 100% nulas contam
- threshold de linhas ruins: passar no limite exato; abortar acima
- DataFrame vazio: não divide por zero
- bad rows salvas na sessão via merge
"""
import pytest
from unittest.mock import MagicMock, patch, call

pd = pytest.importorskip("pandas")

from shared.visao_cliente_schema import REQUIRED_COLUMNS


def _make_df_with_required_cols(rows: list[dict]) -> "pd.DataFrame":
    """Constrói DataFrame garantindo que todas as REQUIRED_COLUMNS existam."""
    base = {c: None for c in REQUIRED_COLUMNS}
    data = [{**base, **row} for row in rows]
    return pd.DataFrame(data)


def _run_validate(df, bad_threshold_pct=5.0):
    """Roda run_validate com session/checkpoint mockados. Retorna (session, job_mock)."""
    from shared.models import EtlJobRun
    session = MagicMock()
    job = EtlJobRun(id="job-1", file_id="f-1", status="RUNNING",
                    triggered_by="test", max_retries=3)
    session.query.return_value.filter_by.return_value.first.return_value = job

    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.mark_step_done"), \
         patch("worker.steps.validate.get_settings") as mock_cfg, \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        mock_cfg.return_value.BAD_ROW_THRESHOLD_PCT = bad_threshold_pct
        from worker.steps.validate import run_validate
        run_validate(session, "job-1", MagicMock())

    return session, job


# ---------------------------------------------------------------------------
# rows_total / rows_ok / rows_bad
# ---------------------------------------------------------------------------

def test_validate_sets_rows_total_equal_to_dataframe_length():
    df = _make_df_with_required_cols([{"data_base": "01/01/2026"} for _ in range(7)])
    _, job = _run_validate(df)
    assert job.rows_total == 7


def test_validate_rows_ok_equals_total_minus_bad():
    # 5 linhas normais + 2 linhas totalmente nulas
    good_row = {c: "valor" for c in REQUIRED_COLUMNS}
    null_row = {c: None for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([good_row] * 5 + [null_row] * 2)
    _, job = _run_validate(df, bad_threshold_pct=100.0)
    assert job.rows_bad == 2
    assert job.rows_ok == 5
    assert job.rows_total == 7


def test_validate_zero_bad_rows_when_all_rows_have_data():
    good_row = {c: "x" for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([good_row] * 3)
    _, job = _run_validate(df)
    assert job.rows_bad == 0
    assert job.rows_ok == 3


def test_validate_all_rows_null_counts_all_as_bad():
    null_row = {c: None for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([null_row] * 4)
    _, job = _run_validate(df, bad_threshold_pct=100.0)
    assert job.rows_bad == 4
    assert job.rows_ok == 0


# ---------------------------------------------------------------------------
# bad rows salvas via session.merge
# ---------------------------------------------------------------------------

def test_validate_saves_each_bad_row_via_session_merge():
    good_row = {c: "x" for c in REQUIRED_COLUMNS}
    null_row = {c: None for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([good_row, null_row, null_row])
    session, _ = _run_validate(df, bad_threshold_pct=100.0)
    assert session.merge.call_count == 2


def test_validate_does_not_merge_when_no_bad_rows():
    good_row = {c: "x" for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([good_row, good_row])
    session, _ = _run_validate(df)
    session.merge.assert_not_called()


# ---------------------------------------------------------------------------
# threshold de linhas ruins
# ---------------------------------------------------------------------------

def test_validate_passes_when_bad_rows_exactly_at_threshold():
    """5% ruins com threshold de 5% deve passar (condição usa >, não >=)."""
    good = {c: "x" for c in REQUIRED_COLUMNS}
    null = {c: None for c in REQUIRED_COLUMNS}
    # 1 ruim em 20 = exatamente 5%
    df = pd.DataFrame([null] + [good] * 19)
    _run_validate(df, bad_threshold_pct=5.0)  # não deve levantar


def test_validate_aborts_when_bad_rows_exceed_threshold():
    """Mais de 5% ruins deve levantar ValueError."""
    good = {c: "x" for c in REQUIRED_COLUMNS}
    null = {c: None for c in REQUIRED_COLUMNS}
    # 2 ruins em 20 = 10% > threshold de 5%
    df = pd.DataFrame([null] * 2 + [good] * 18)
    with pytest.raises(ValueError, match="threshold"):
        _run_validate(df, bad_threshold_pct=5.0)


def test_validate_zero_rows_does_not_raise_division_error():
    df = pd.DataFrame(columns=REQUIRED_COLUMNS)
    _run_validate(df, bad_threshold_pct=5.0)  # não deve levantar ZeroDivisionError


def test_validate_one_row_completely_null_with_high_threshold_passes():
    """1 linha nula em 1 = 100% ruins, mas threshold de 100% passa."""
    null = {c: None for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([null])
    _run_validate(df, bad_threshold_pct=100.0)  # não deve levantar


def test_validate_one_row_null_with_default_threshold_aborts():
    """1 linha totalmente nula = 100% ruins > 5% default → aborta."""
    null = {c: None for c in REQUIRED_COLUMNS}
    df = pd.DataFrame([null])
    with pytest.raises(ValueError, match="threshold"):
        _run_validate(df, bad_threshold_pct=5.0)
