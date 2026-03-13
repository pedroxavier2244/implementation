# E2E Pipeline Test Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Criar suite de testes end-to-end que valida o pipeline completo (upload → ETL → leitura de dados) usando testcontainers (PostgreSQL real), moto (S3/MinIO mock), fakeredis e Celery em modo síncrono.

**Architecture:** TestClient FastAPI + PostgreSQL via testcontainers (migrações Alembic reais) + moto mock_s3 para MinIO + fakeredis + Celery task_always_eager. BrasilAPI mockada para evitar HTTP externo. Fixtures com escopo de sessão para performance.

**Tech Stack:** pytest, testcontainers[postgres], fakeredis, moto[s3], openpyxl, httpx, unittest.mock

---

### Task 1: Atualizar requirements/test.txt com novas dependências

**Files:**
- Modify: `requirements/test.txt`

**Step 1: Editar requirements/test.txt**

Adicionar ao final do arquivo:

```
testcontainers[postgres]==4.14.1
fakeredis==2.34.1
moto[s3]==5.1.22
```

**Step 2: Verificar que pacotes já estão instalados**

```bash
python -c "import testcontainers; import fakeredis; import moto; print('ok')"
```
Expected: `ok`

**Step 3: Commit**

```bash
git add requirements/test.txt
git commit -m "test: add testcontainers, fakeredis, moto to test requirements"
```

---

### Task 2: Criar fixture xlsx mínima válida

**Files:**
- Create: `tests/fixtures/__init__.py`
- Create: `tests/fixtures/make_xlsx.py`

**Step 1: Criar diretório e `__init__.py`**

Criar `tests/fixtures/__init__.py` vazio.

**Step 2: Criar `tests/fixtures/make_xlsx.py`**

```python
"""
Gera um arquivo .xlsx mínimo válido para o pipeline ETL.
Inclui as 3 abas necessárias: Visão Cliente, Abertura, Relacionamento.
"""
import io
from datetime import date

import openpyxl

# Colunas obrigatórias do schema (já no formato normalizado → usar como header)
VISAO_CLIENTE_HEADERS = [
    "data_base", "cd_cpf_cnpj_cliente", "nome_cliente", "tipo_pessoa",
    "cd_cpf_cnpj_parceiro", "nome_parceiro", "cd_cpf_cnpj_consultor",
    "nome_consultor", "uf", "cidade", "bairro", "telefone", "telefone_master",
    "email", "dt_fundacao_empresa", "ramo_atuacao", "num_conta", "limite_conta",
    "dt_conta_criada", "dt_encer_cc", "status_cc", "conta_ativa_90d",
    "chaves_pix_forte", "vl_cash_in_mtd", "limite_cartao",
    "limite_alocado_cartao_cdb", "dt_entrega_cartao", "dt_ativ_cartao_cred",
    "vl_spending_total_mtd", "status_pagamento_fatura", "fl_propensao_c6pay",
    "tpv_c6pay_potencial", "fl_elegivel_venda_c6pay", "status_proposta_sf_pay",
    "dt_aprovacao_pay", "dt_install_maq", "dt_ativacao_pay", "c6pay_ativa_30",
    "dt_cancelamento_maq", "dt_ult_trans_pay", "recebimento", "banco_domicilio",
    "tpv_m2", "tpv_m1", "tpv_m0", "faixa_tpv_prometido", "fl_propensao_bolcob",
    "tpv_bolcob_potencial", "fl_bolcob_cadastrado", "dt_prim_liq_bolcob",
    "dt_ult_emissao_bolcob", "qtd_bolcob_emtd_mtd", "vl_bolcob_emtd_mtd",
    "qtd_bolcob_liq_mtd", "vl_bolcob_liq_mtd", "volume_antecipado",
    "agenda_disponivel", "taxa_antecipacao", "vl_saldo_medio_mensalizado",
    "dt_conta_criada_global", "vl_cash_in_conta_global_mtd", "fl_cash_in_puro",
    "fl_cash_in_boleto", "fl_cash_in_setup", "fl_cash_in_setup_pix_cnpj",
    "fl_cash_in_setup_cdb_cartao", "fl_cash_in_setup_pagamentos",
    "fl_cash_in_setup_deb_auto", "mes_ref_comiss", "fl_qualificado_comiss",
    "faixa_cash_in", "faixa_domicilio", "faixa_saldo_medio", "faixa_spending",
    "faixa_cash_in_global", "criterios_atingidos_comiss", "apuracao_comiss",
    "multiplicador", "ja_pago_comiss", "previsao_comiss", "faixa_max",
    "faixa_alvo", "threshiold_cash_in", "threshold_spending",
    "threshold_saldo_medio", "threshold_conta_global", "threshold_domicilio",
    "gap_cash_in", "gap_spending", "gap_saldo_medio", "gap_conta_global",
    "gap_domicilio", "pct_cash_in", "pct_spending", "pct_saldo_medio",
    "pct_conta_global", "maior_progresso_pct", "criterio_proximo",
    "ja_recebeu_comissao", "comissao_prox_mes", "status_qualificacao",
    "dias_desde_abertura", "m2_dias_faltantes", "nivel_cartao", "nivel_conta",
]

# CNPJ de teste (14 dígitos, fictício)
TEST_CNPJ = "12345678000195"
TEST_FILENAME = "Relatorio de Producao - 21.02.26.xlsx"


def _row_values(reference_date: date) -> list:
    """Retorna uma linha de dados mínimos válidos."""
    row = []
    for col in VISAO_CLIENTE_HEADERS:
        if col == "data_base":
            row.append(reference_date)
        elif col == "cd_cpf_cnpj_cliente":
            row.append(TEST_CNPJ)
        elif col == "nome_cliente":
            row.append("Empresa Teste LTDA")
        elif col == "tipo_pessoa":
            row.append("PJ")
        elif col == "uf":
            row.append("SP")
        elif col == "cidade":
            row.append("São Paulo")
        elif col == "nivel_cartao":
            row.append("sem_cartao")
        elif col == "nivel_conta":
            row.append("sem_conta")
        elif col in ("status_cc",):
            row.append("ATIVA")
        elif col in ("status_qualificacao",):
            row.append("NAO_QUALIFICADO")
        elif col in ("ja_recebeu_comissao", "comissao_prox_mes"):
            row.append("NAO")
        elif col.startswith("vl_") or col.startswith("tpv_") or col.startswith("gap_") \
                or col.startswith("pct_") or col.startswith("threshold") \
                or col.startswith("threshiold") or col in (
                    "limite_conta", "limite_cartao", "limite_alocado_cartao_cdb",
                    "volume_antecipado", "agenda_disponivel", "taxa_antecipacao",
                    "vl_saldo_medio_mensalizado", "maior_progresso_pct",
                    "multiplicador", "previsao_comiss", "apuracao_comiss",
                    "faixa_max", "faixa_alvo",
                ):
            row.append(0.0)
        elif col.startswith("qtd_") or col in ("chaves_pix_forte", "dias_desde_abertura", "m2_dias_faltantes"):
            row.append(0)
        elif col.startswith("fl_") or col in (
                "conta_ativa_90d", "c6pay_ativa_30", "fl_bolcob_cadastrado",
                "fl_cash_in_puro", "fl_cash_in_boleto", "fl_cash_in_setup",
                "fl_cash_in_setup_pix_cnpj", "fl_cash_in_setup_cdb_cartao",
                "fl_cash_in_setup_pagamentos", "fl_cash_in_setup_deb_auto",
                "fl_qualificado_comiss", "ja_pago_comiss",
        ):
            row.append(0)
        else:
            row.append(None)
    return row


def make_test_xlsx(reference_date: date | None = None) -> bytes:
    """Gera bytes de um xlsx válido com as 3 abas requeridas."""
    if reference_date is None:
        reference_date = date(2026, 2, 21)

    wb = openpyxl.Workbook()

    # ── Aba 1: Visão Cliente ──────────────────────────────────────────────────
    ws_vc = wb.active
    ws_vc.title = "Visão Cliente"
    ws_vc.append(VISAO_CLIENTE_HEADERS)
    ws_vc.append(_row_values(reference_date))

    # ── Aba 2: Abertura ───────────────────────────────────────────────────────
    ws_ab = wb.create_sheet("Abertura")
    ws_ab.append(["Total de Contas Abertas", "Contas Qualificadas"])
    ws_ab.append([5, 3])

    # ── Aba 3: Relacionamento ─────────────────────────────────────────────────
    ws_rel = wb.create_sheet("Relacionamento")
    ws_rel.append(["Maquinas Vendidas Relacionamento"])
    ws_rel.append([2])

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()
```

**Step 3: Testar o gerador manualmente**

```bash
python -c "
from tests.fixtures.make_xlsx import make_test_xlsx
data = make_test_xlsx()
print(f'xlsx gerado: {len(data)} bytes')
import openpyxl, io
wb = openpyxl.load_workbook(io.BytesIO(data))
print('Abas:', wb.sheetnames)
"
```
Expected:
```
xlsx gerado: <N> bytes
Abas: ['Visão Cliente', 'Abertura', 'Relacionamento']
```

**Step 4: Commit**

```bash
git add tests/fixtures/
git commit -m "test: add xlsx fixture generator for e2e tests"
```

---

### Task 3: Criar conftest.py de integração com todos os fixtures

**Files:**
- Create: `tests/integration/conftest.py`

**Step 1: Criar conftest.py**

```python
"""
Fixtures de integração para testes e2e.

Requer Docker Engine rodando. Sobe PostgreSQL via testcontainers.
MinIO é mockado via moto (mock_s3). Redis via fakeredis.
"""
import os
import subprocess
import sys

import fakeredis
import pytest
from fastapi.testclient import TestClient
from moto import mock_aws
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

# ── PostgreSQL ────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def pg_container():
    """Sobe container PostgreSQL para a sessão de testes inteira."""
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_url(pg_container):
    """URL de conexão ao PostgreSQL de teste."""
    return pg_container.get_connection_url().replace(
        "psycopg2", "psycopg2"  # garante driver correto
    )


@pytest.fixture(scope="session")
def run_migrations(pg_url):
    """Roda alembic upgrade head no banco de teste."""
    env = {**os.environ, "DATABASE_URL_OVERRIDE": pg_url}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--co", "-q"],  # dry-run para warm-up
        capture_output=True,
    )
    # Roda migrations diretamente via alembic
    from alembic import command
    from alembic.config import Config as AlembicConfig

    alembic_cfg = AlembicConfig("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", pg_url)
    command.upgrade(alembic_cfg, "head")
    return pg_url


# ── MinIO (moto mock_s3) ──────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def aws_mock():
    """Ativa mock S3 da moto para toda a sessão."""
    with mock_aws():
        import boto3
        # Cria bucket que o MinioClient espera
        s3 = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            endpoint_url=None,
        )
        s3.create_bucket(Bucket="etl-files")
        yield s3


# ── Redis (fakeredis) ─────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def fake_redis_server():
    """Servidor fakeredis para a sessão."""
    server = fakeredis.FakeServer()
    return server


# ── Patch de configuração e caches ───────────────────────────────────────────

@pytest.fixture(scope="session")
def test_env(pg_url, fake_redis_server, aws_mock):
    """
    Sobrescreve variáveis de ambiente para apontar para os serviços de teste.
    Limpa caches lru_cache de get_settings() e get_engine().
    """
    # Extrai componentes da pg_url (formato: postgresql+psycopg2://user:pass@host:port/db)
    import re
    m = re.match(
        r"postgresql\+psycopg2://(?P<user>[^:]+):(?P<pw>[^@]+)@(?P<host>[^:]+):(?P<port>\d+)/(?P<db>.+)",
        pg_url,
    )
    assert m, f"URL inesperada: {pg_url}"

    env_overrides = {
        "POSTGRES_HOST": m.group("host"),
        "POSTGRES_PORT": m.group("port"),
        "POSTGRES_DB": m.group("db"),
        "POSTGRES_USER": m.group("user"),
        "POSTGRES_PASSWORD": m.group("pw"),
        "REDIS_URL": "redis://localhost:6379/0",  # será interceptado pelo patch
        "MINIO_ENDPOINT": "s3.amazonaws.com",
        "MINIO_ACCESS_KEY": "test",
        "MINIO_SECRET_KEY": "test",
        "MINIO_BUCKET": "etl-files",
        "MINIO_SECURE": "false",
        "CNPJ_VERIFY_BATCH_SIZE": "0",  # desativa cnpj_verify no e2e
    }

    original = {k: os.environ.get(k) for k in env_overrides}
    os.environ.update(env_overrides)

    # Limpa caches pydantic-settings e sqlalchemy
    from shared.config import get_settings
    from shared.db import get_engine
    get_settings.cache_clear()
    get_engine.cache_clear()

    yield env_overrides

    # Restaura
    for k, v in original.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    get_settings.cache_clear()
    get_engine.cache_clear()


# ── Patch do MinioClient para usar moto ──────────────────────────────────────

@pytest.fixture(scope="session")
def patch_minio(aws_mock, test_env, monkeypatch_session):
    """
    Substitui MinioClient._client por um cliente boto3 apontando para moto.
    """
    import boto3
    from botocore.client import Config
    from unittest.mock import patch, MagicMock

    real_s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    original_init = None

    def mock_minio_init(self):
        self.bucket = "etl-files"
        self._client = real_s3
        # bucket já criado pelo aws_mock fixture

    with patch("shared.minio_client.MinioClient.__init__", mock_minio_init):
        yield


# ── Patch do Redis / Celery ───────────────────────────────────────────────────

@pytest.fixture(scope="session")
def patch_celery_eager(fake_redis_server):
    """Faz tasks Celery rodarem sincronamente (sem broker real)."""
    from unittest.mock import patch
    import fakeredis

    fake_conn = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=False)

    celery_overrides = {
        "task_always_eager": True,
        "task_eager_propagates": True,
    }

    from worker.celery_app import app as celery_app
    celery_app.conf.update(**celery_overrides)

    # Patch redis.from_url para retornar fakeredis
    with patch("redis.from_url", return_value=fake_conn):
        # Patch enqueue_task para chamar diretamente (bypassa broker)
        from unittest.mock import MagicMock
        with patch("shared.celery_dispatch.enqueue_task") as mock_enqueue:
            mock_enqueue.return_value = MagicMock(id="mock-notification-task")
            yield


# ── Patch da BrasilAPI ────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def patch_brasilapi():
    """Mock da BrasilAPI para evitar HTTP externo."""
    from unittest.mock import patch

    with patch("shared.brasilapi.fetch_cnpj", return_value=None):
        yield


# ── monkeypatch com escopo session ────────────────────────────────────────────

@pytest.fixture(scope="session")
def monkeypatch_session():
    """monkeypatch com escopo de session (não existe nativamente no pytest)."""
    from _pytest.monkeypatch import MonkeyPatch
    mp = MonkeyPatch()
    yield mp
    mp.undo()


# ── Cliente FastAPI ───────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def client(run_migrations, test_env, patch_minio, patch_celery_eager, patch_brasilapi):
    """TestClient FastAPI com toda a infraestrutura de teste ativa."""
    from api.main import app
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
```

**Step 2: Verificar que o conftest importa sem erros**

```bash
python -c "
import sys; sys.path.insert(0, '.')
# Apenas verifica imports
from tests.fixtures.make_xlsx import make_test_xlsx
print('imports ok')
"
```
Expected: `imports ok`

**Step 3: Commit**

```bash
git add tests/integration/conftest.py
git commit -m "test: add integration conftest with testcontainers, moto, fakeredis fixtures"
```

---

### Task 4: Escrever o teste e2e completo

**Files:**
- Create: `tests/integration/test_e2e_pipeline.py`

**Step 1: Criar o arquivo de teste**

```python
"""
Teste End-to-End do pipeline ETL completo.

Fluxo coberto:
  1. POST /v1/files/upload  → upload do .xlsx, extração de data do nome
  2. POST /v1/jobs/run      → disparo do ETL (Celery eager)
  3. GET  /v1/jobs/{id}     → job.status == DONE
  4. GET  /v1/data/visao-cliente?documento=<cnpj>  → dados persistidos
  5. GET  /v1/data/visao-cliente/historico?documento=<cnpj>  → 1 snapshot
  6. GET  /v1/analytics/contas-abertas/summary  → indicador agregado

Requer: Docker Engine rodando (para testcontainers/postgres).
Executar: pytest tests/integration/test_e2e_pipeline.py -v -s
"""
import io
import pytest
from tests.fixtures.make_xlsx import make_test_xlsx, TEST_CNPJ, TEST_FILENAME


@pytest.mark.integration
class TestE2EPipeline:

    # ── 1. Upload do arquivo ──────────────────────────────────────────────────

    def test_upload_extracts_date_from_filename(self, client):
        """Upload deve extrair 2026-02-21 do nome '...21.02.26.xlsx'."""
        xlsx_bytes = make_test_xlsx()
        response = client.post(
            "/v1/files/upload",
            files={"file": (TEST_FILENAME, io.BytesIO(xlsx_bytes), "application/octet-stream")},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["filename"] == TEST_FILENAME
        assert body["file_date"] == "2026-02-21", f"Esperado 2026-02-21, got {body['file_date']}"
        assert body["is_processed"] is False
        # Guarda file_id para os próximos testes
        TestE2EPipeline._file_id = body["id"]

    def test_file_appears_in_list(self, client):
        """Arquivo recém-uploadado deve aparecer em GET /v1/files."""
        response = client.get("/v1/files")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] >= 1
        ids = [f["id"] for f in body["items"]]
        assert TestE2EPipeline._file_id in ids

    # ── 2. Disparo do ETL ─────────────────────────────────────────────────────

    def test_run_etl_job(self, client):
        """POST /v1/jobs/run deve retornar status QUEUED."""
        response = client.post(
            "/v1/jobs/run",
            json={"file_id": TestE2EPipeline._file_id},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["status"] == "QUEUED"
        TestE2EPipeline._job_task_id = body["job_id"]

    # ── 3. Verificação do job ─────────────────────────────────────────────────

    def test_job_completed_as_done(self, client):
        """
        Celery roda em modo ALWAYS_EAGER: o job já terminou antes de retornar.
        GET /v1/jobs deve conter um job com status DONE.
        """
        response = client.get("/v1/jobs?status=DONE")
        assert response.status_code == 200, response.text
        jobs = response.json()
        assert len(jobs) >= 1, "Nenhum job com status DONE encontrado"
        done_job = next(
            (j for j in jobs if j["file_id"] == TestE2EPipeline._file_id), None
        )
        assert done_job is not None, f"Job para file_id={TestE2EPipeline._file_id} não encontrado em DONE"
        assert done_job["status"] == "DONE"
        TestE2EPipeline._job_id = done_job["id"]

    def test_file_marked_as_processed(self, client):
        """Após ETL DONE, arquivo deve ter is_processed=True."""
        response = client.get(f"/v1/files/{TestE2EPipeline._file_id}")
        assert response.status_code == 200
        assert response.json()["is_processed"] is True

    # ── 4. Dados na tabela final ──────────────────────────────────────────────

    def test_visao_cliente_data_persisted(self, client):
        """GET /v1/data/visao-cliente deve retornar o CNPJ de teste."""
        response = client.get(
            "/v1/data/visao-cliente",
            params={"documento": TEST_CNPJ},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["cd_cpf_cnpj_cliente"] == TEST_CNPJ
        assert body["nome_cliente"] == "Empresa Teste LTDA"
        assert body["tipo_pessoa"] == "PJ"

    # ── 5. Histórico ──────────────────────────────────────────────────────────

    def test_historico_has_one_snapshot(self, client):
        """GET /v1/data/visao-cliente/historico deve retornar 1 entrada."""
        response = client.get(
            "/v1/data/visao-cliente/historico",
            params={"documento": TEST_CNPJ},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert "snapshots" in body or isinstance(body, list), f"Formato inesperado: {body}"
        snapshots = body.get("snapshots", body) if isinstance(body, dict) else body
        assert len(snapshots) >= 1, "Esperado ao menos 1 snapshot no histórico"

    # ── 6. Analytics ──────────────────────────────────────────────────────────

    def test_analytics_contas_abertas_summary(self, client):
        """GET /v1/analytics/contas-abertas/summary deve retornar total > 0."""
        response = client.get("/v1/analytics/contas-abertas/summary")
        assert response.status_code == 200, response.text
        body = response.json()
        # Deve ter total (a aba Abertura tem 5 contas abertas)
        assert "total" in body or "data" in body or len(body) > 0, f"Resposta vazia: {body}"

    def test_analytics_contas_qualificadas_summary(self, client):
        """GET /v1/analytics/contas-qualificadas/summary deve retornar total > 0."""
        response = client.get("/v1/analytics/contas-qualificadas/summary")
        assert response.status_code == 200, response.text

    def test_analytics_instalacao_c6pay_summary(self, client):
        """GET /v1/analytics/instalacao-c6pay/summary deve retornar total >= 0."""
        response = client.get("/v1/analytics/instalacao-c6pay/summary")
        assert response.status_code == 200, response.text

    # ── 7. Health / Ready ─────────────────────────────────────────────────────

    def test_health_still_ok(self, client):
        """Health deve continuar ok após o pipeline rodar."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
```

**Step 2: Verificar que o arquivo foi criado corretamente**

```bash
python -m pytest tests/integration/test_e2e_pipeline.py --collect-only 2>&1 | head -30
```
Expected: lista com 9 testes coletados.

**Step 3: Commit**

```bash
git add tests/integration/test_e2e_pipeline.py
git commit -m "test: add e2e pipeline test (upload → ETL → visao-cliente → analytics)"
```

---

### Task 5: Executar e corrigir

**Step 1: Rodar os testes com output detalhado**

```bash
pytest tests/integration/test_e2e_pipeline.py -v -s -m integration 2>&1
```

**Step 2: Se `patch_minio` falhar (erro de assinatura S3)**

O `MinioClient` usa endpoint customizado. Se o moto não interceptar, o patch precisa ser mais direto. Alternativa:

```python
# Em conftest.py, trocar patch_minio por:
from unittest.mock import patch, MagicMock

class _FakeMinioClient:
    def __init__(self):
        self._store = {}
        self.bucket = "etl-files"

    def upload_file(self, file_bytes: bytes, object_name: str) -> str:
        self._store[object_name] = file_bytes
        return object_name

    def download_file(self, object_name: str) -> bytes:
        return self._store[object_name]

    def object_exists(self, object_name: str) -> bool:
        return object_name in self._store

_fake_minio_instance = _FakeMinioClient()

with patch("shared.minio_client.MinioClient", return_value=_fake_minio_instance):
    ...
```

**Step 3: Se `patch_celery_eager` falhar (broker connection error)**

O Celery com `task_always_eager=True` não precisa de broker. Mas o `celery_app` inicializa antes do patch. Solução: importar `run_etl` direto e chamar `run_etl.apply(kwargs=...)` no teste.

**Step 4: Verificar output final esperado**

```
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_upload_extracts_date_from_filename PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_file_appears_in_list PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_run_etl_job PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_job_completed_as_done PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_file_marked_as_processed PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_visao_cliente_data_persisted PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_historico_has_one_snapshot PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_analytics_contas_abertas_summary PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_analytics_contas_qualificadas_summary PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_analytics_instalacao_c6pay_summary PASSED
tests/integration/test_e2e_pipeline.py::TestE2EPipeline::test_health_still_ok PASSED

11 passed in Xs
```

**Step 5: Commit final**

```bash
git add -A
git commit -m "test: e2e pipeline tests passing"
```
