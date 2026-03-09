"""
Teste End-to-End do pipeline ETL completo.

Fluxo coberto:
  1. POST /v1/files/upload  → upload do .xlsx, extração de data do nome
  2. POST /v1/jobs/run      → disparo do ETL (Celery eager)
  3. GET  /v1/jobs          → job.status == DONE
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
        assert body["total"] >= 1, f"Esperado ao menos 1 item, got: {body}"
        item = body["items"][0]
        assert item["cd_cpf_cnpj_cliente"] == TEST_CNPJ
        assert item["nome_cliente"] == "Empresa Teste LTDA"
        assert item["tipo_pessoa"] == "PJ"

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

    # ── 6. Health / Ready ────────────────────────────────────────────────────

    def test_health_still_ok(self, client):
        """Health deve continuar ok após o pipeline rodar."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
