"""
Testes de desempenho:
Todos os testes rodam exclusivamente em memória (sem DB, sem MinIO).

Limites conservadores para CI em qualquer máquina moderna:
- clean em 100k linhas   < 10s
- validate em 100k linhas < 5s
- enrich em 30k linhas   < 30s  (enrich é o step mais pesado: numpy vectorized)

Para desativar estes testes em ambientes lentos:
  pytest -m "not performance"
"""
import time
import uuid
import pytest
from unittest.mock import MagicMock, patch

pd = pytest.importorskip("pandas")
import pandas as pd
import numpy as np

pytestmark = pytest.mark.performance


# ---------------------------------------------------------------------------
# Builders de DataFrame grande
# ---------------------------------------------------------------------------

def _make_large_clean_df(n: int) -> pd.DataFrame:
    """DataFrame com as colunas mínimas para passar pelo filtro do clean."""
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "TIPO_PESSOA":   np.where(rng.integers(0, 2, n) == 0, "PJ", "PF"),
        "STATUS_CC":     np.where(rng.integers(0, 3, n) == 0, "LIBERADA", "BLOQUEADA"),
        "MES_REF_COMISS": np.where(rng.integers(0, 5, n) > 0, "2026-03", None),
        "NOME_CLIENTE":  [f"Empresa {i}" for i in range(n)],
        "CD_CPF_CNPJ_CLIENTE": [f"{i:014d}.0" for i in range(n)],
        "DATA_BASE":     np.where(rng.integers(0, 2, n) == 0, "01/03/2026", "2026-03-01 00:00:00"),
    })


def _make_large_validate_df(n: int) -> pd.DataFrame:
    """DataFrame com todas as REQUIRED_COLUMNS para o validate."""
    from shared.visao_cliente_schema import REQUIRED_COLUMNS
    rng = np.random.default_rng(42)
    data = {c: [f"val_{i}" for i in range(n)] for c in REQUIRED_COLUMNS}
    # ~2% de linhas totalmente nulas (abaixo do threshold padrão de 5%)
    null_indices = rng.choice(n, size=int(n * 0.02), replace=False)
    for col in REQUIRED_COLUMNS:
        arr = np.array(data[col], dtype=object)
        arr[null_indices] = None
        data[col] = arr.tolist()
    return pd.DataFrame(data)


def _make_large_enrich_df(n: int) -> pd.DataFrame:
    """DataFrame mínimo para rodar a cadeia completa de enrich."""
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        # status_cartao
        "limite_cartao":           rng.choice(["0", "5000", "10000", None], n),
        "limite_alocado_cartao_cdb": rng.choice(["0", "1000", None], n),
        "dt_entrega_cartao":       rng.choice(["2026-01-10", None], n),
        "dt_ativ_cartao_cred":     rng.choice(["2026-01-15", None], n),
        "vl_spending_total_mtd":   [str(rng.integers(0, 5000)) for _ in range(n)],
        # status_maq
        "status_proposta_sf_pay":  rng.choice([None, "EM ANALISE C6 | AGUARDANDO APROVACAO DO CLIENTE"], n),
        "fl_elegivel_venda_c6pay": rng.choice(["0", "1"], n),
        "dt_install_maq":          rng.choice(["2025-12-01", None], n),
        "dt_ativacao_pay":         rng.choice(["2025-12-15", None], n),
        "c6pay_ativa_30":          rng.choice(["0", "1"], n),
        "dt_cancelamento_maq":     rng.choice([None, "2026-01-01"], n),
        # tpv
        "tpv_m0":  [str(rng.integers(0, 10000)) for _ in range(n)],
        "tpv_m1":  [str(rng.integers(0, 10000)) for _ in range(n)],
        "tpv_m2":  [str(rng.integers(0, 10000)) for _ in range(n)],
        # bolcob
        "fl_bolcob_cadastrado":    rng.choice(["0", "1"], n),
        "dt_prim_liq_bolcob":      rng.choice([None, "2025-11-01"], n),
        "qtd_bolcob_liq_mtd":      [str(rng.integers(0, 5)) for _ in range(n)],
        # insight_bolcob
        "data_base":               ["01/03/2026"] * n,
        "dt_fundacao_empresa":     rng.choice([None, "2020-01-01"], n),
        "dt_ult_emissao_bolcob":   rng.choice([None, "2026-02-01"], n),
        "qtd_bolcob_emtd_mtd":     [str(rng.integers(0, 10)) for _ in range(n)],
        "vl_bolcob_emtd_mtd":      [str(rng.integers(0, 50000)) for _ in range(n)],
        "vl_bolcob_liq_mtd":       [str(rng.integers(0, 50000)) for _ in range(n)],
        # insight_pix_forte
        "chaves_pix_forte":        rng.choice([None, "CNPJ", "EMAIL"], n),
        # insight_conta_global
        "dt_conta_criada_global":  rng.choice([None, "2026-01-01"], n),
        # insight_maq extra
        "dt_ult_trans_pay":        rng.choice([None, "2026-02-15"], n),
        # gaps
        "faixa_cash_in":           [str(rng.integers(0, 5)) for _ in range(n)],
        "faixa_domicilio":         [str(rng.integers(0, 5)) for _ in range(n)],
        "faixa_saldo_medio":       [str(rng.integers(0, 5)) for _ in range(n)],
        "faixa_spending":          [str(rng.integers(0, 5)) for _ in range(n)],
        "faixa_cash_in_global":    [str(rng.integers(0, 5)) for _ in range(n)],
        "vl_cash_in_mtd":          [str(rng.integers(0, 60000)) for _ in range(n)],
        "vl_saldo_medio_mensalizado": [str(rng.integers(0, 10000)) for _ in range(n)],
        "vl_cash_in_conta_global_mtd": [str(rng.integers(0, 35000)) for _ in range(n)],
        # status_qualificacao
        "ja_pago_comiss":          [str(rng.integers(0, 1000)) for _ in range(n)],
        "previsao_comiss":         [str(rng.integers(0, 1000)) for _ in range(n)],
        # insight_cartao extra
        "dt_conta_criada":         rng.choice([None, "2025-06-01"], n),
    })


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------

def test_clean_100k_rows_completes_under_10_seconds():
    n = 100_000
    df = _make_large_clean_df(n)

    from worker.steps.clean import set_cached_dataframe, get_cached_dataframe, run_clean
    job_id = str(uuid.uuid4())
    set_cached_dataframe(job_id, df)

    with patch("worker.steps.clean.is_step_done", return_value=False), \
         patch("worker.steps.clean.begin_step"), \
         patch("worker.steps.clean.mark_step_done"):
        t0 = time.perf_counter()
        run_clean(MagicMock(), job_id)
        elapsed = time.perf_counter() - t0

    result = get_cached_dataframe(job_id)
    assert result is not None, "clean não produziu resultado"
    assert (result["tipo_pessoa"] == "PJ").all(), "filtro PJ falhou"
    assert elapsed < 10.0, f"clean em {n} linhas levou {elapsed:.2f}s (limite: 10s)"


def test_validate_100k_rows_completes_under_5_seconds():
    n = 100_000
    df = _make_large_validate_df(n)

    from shared.models import EtlJobRun
    session = MagicMock()
    job = EtlJobRun(id="j", file_id="f", status="RUNNING", triggered_by="test")
    session.query.return_value.filter_by.return_value.first.return_value = job

    with patch("worker.steps.validate.is_step_done", return_value=False), \
         patch("worker.steps.validate.begin_step"), \
         patch("worker.steps.validate.mark_step_done"), \
         patch("worker.steps.validate.get_settings") as mock_cfg, \
         patch("worker.steps.validate.get_cached_dataframe", return_value=df):
        mock_cfg.return_value.BAD_ROW_THRESHOLD_PCT = 5.0
        from worker.steps.validate import run_validate
        t0 = time.perf_counter()
        run_validate(session, "j", MagicMock())
        elapsed = time.perf_counter() - t0

    assert job.rows_total == n
    assert elapsed < 5.0, f"validate em {n} linhas levou {elapsed:.2f}s (limite: 5s)"


def test_enrich_30k_rows_completes_under_30_seconds():
    n = 30_000
    df = _make_large_enrich_df(n)

    from worker.steps.enrich import (
        _compute_total_tpv, _compute_status_cartao, _compute_status_maq,
        _compute_status_bolcob, _compute_insight_columns, _compute_gap_columns,
        _compute_status_qualificacao,
    )

    # Garante colunas ausentes (enrich aceita DataFrames incompletos)
    from shared.visao_cliente_schema import REQUIRED_COLUMNS
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    t0 = time.perf_counter()
    _compute_total_tpv(df)
    _compute_status_cartao(df)
    _compute_status_maq(df)
    _compute_status_bolcob(df)
    _compute_insight_columns(df)
    _compute_gap_columns(df)
    _compute_status_qualificacao(df)
    elapsed = time.perf_counter() - t0

    # Verifica que as colunas foram produzidas
    for col in ("total_tpv", "status_cartao", "status_maq", "status_bolcbob",
                "status_qualificacao", "faixa_maximo", "gap_cash_in"):
        assert col in df.columns, f"Coluna '{col}' não produzida pelo enrich"

    assert elapsed < 30.0, f"enrich em {n} linhas levou {elapsed:.2f}s (limite: 30s)"


def test_dedup_100k_rows_completes_under_3_seconds():
    """Deduplicação pandas (equivalente ao ROW_NUMBER SQL) em 100k linhas."""
    n = 100_000
    rng = np.random.default_rng(42)
    cnpjs = [f"{i % 20_000:014d}" for i in range(n)]  # ~5 linhas por CNPJ
    dates = [f"{rng.integers(1, 28):02d}/0{rng.integers(1, 4)}/2026" for _ in range(n)]

    df = pd.DataFrame({"cd_cpf_cnpj_cliente": cnpjs, "data_base": dates})

    t0 = time.perf_counter()
    deduped = (
        df.assign(__rn=df.groupby("cd_cpf_cnpj_cliente")["data_base"]
                    .rank(method="first", ascending=False))
        .query("__rn == 1")
        .drop(columns="__rn")
    )
    elapsed = time.perf_counter() - t0

    assert len(deduped) == 20_000, "Número de CNPJs únicos incorreto"
    assert elapsed < 3.0, f"dedup em {n} linhas levou {elapsed:.2f}s (limite: 3s)"
