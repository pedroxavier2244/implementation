from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fastapi")


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from api.main import app

    return TestClient(app)


def test_get_files_returns_list(client):
    with patch("api.routes.files.get_db_session") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.query().count.return_value = 0
        mock_session.query().order_by().offset().limit().all.return_value = []

        response = client.get("/v1/files")
        assert response.status_code == 200
        assert "items" in response.json()


def test_post_jobs_run_queues_job(client):
    with patch("api.routes.jobs.get_db_session") as mock_db, patch("api.routes.jobs.enqueue_task") as mock_enqueue:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        from shared.models import EtlFile

        # first call: EtlFile lookup; second call (filter active_job): None
        mock_session.query.return_value.filter_by.return_value.first.return_value = EtlFile(
            id="f-1", hash_sha256="h", file_date=None
        )
        mock_session.query.return_value.filter.return_value.first.return_value = None
        mock_enqueue.return_value.id = "task-1"

        response = client.post("/v1/jobs/run", json={"file_id": "f-1"})
        assert response.status_code == 200
        assert response.json()["status"] == "QUEUED"


def test_get_data_visao_cliente_by_documento(client):
    with patch("api.routes.data.get_db_session") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        rows_result = MagicMock()
        rows_result.mappings.return_value.all.return_value = [
            {
                "__total": 1,
                "cd_cpf_cnpj_cliente": "12.345.678/0001-90",
                "nome_cliente": "Cliente Teste",
            }
        ]
        mock_session.execute.return_value = rows_result

        response = client.get("/v1/data/visao-cliente?documento=12.345.678/0001-90")
        assert response.status_code == 200
        assert response.json()["documento_consultado"] == "12345678000190"
        assert response.json()["total"] == 1
        assert len(response.json()["items"]) == 1


def test_get_data_visao_cliente_requires_digits(client):
    response = client.get("/v1/data/visao-cliente?documento=abc")
    assert response.status_code == 400


def test_get_data_visao_cliente_returns_empty_when_not_found(client):
    with patch("api.routes.data.get_db_session") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        rows_result_empty = MagicMock()
        rows_result_empty.mappings.return_value.all.return_value = []
        mock_session.execute.side_effect = [rows_result_empty, rows_result_empty]

        response = client.get("/v1/data/visao-cliente?documento=12.345.678/0001-90")
        assert response.status_code == 200
        payload = response.json()
        assert payload["documento_consultado"] == "12345678000190"
        assert payload["total"] == 0
        assert payload["items"] == []


def test_get_data_visao_cliente_cpf_returns_empty(client):
    with patch("api.routes.data.get_db_session") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        rows_result_empty = MagicMock()
        rows_result_empty.mappings.return_value.all.return_value = []
        mock_session.execute.side_effect = [rows_result_empty, rows_result_empty]

        response = client.get("/v1/data/visao-cliente?documento=123.456.789-01")
        assert response.status_code == 200
        payload = response.json()
        assert payload["documento_consultado"] == "12345678901"
        assert payload["total"] == 0




def test_get_data_visao_cliente_historico_alteracoes(client):
    with patch("api.routes.data.get_db_session") as mock_db:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        rows_result = MagicMock()
        rows_result.mappings.return_value.all.return_value = [
            {
                "id": 1,
                "data_base": "2026-02-21 00:00:00",
                "changed_at": "2026-03-16T12:00:00+00:00",
                "etl_job_id": "job-1",
                "file_id": "file-1",
                "file_date": "2026-02-21",
                "filename": "Relatorio.xlsx",
                "change_type": "UPDATE",
                "field_name": "nome_cliente",
                "old_value": "EMPRESA A",
                "new_value": "EMPRESA B",
                "__total": 1,
            }
        ]
        mock_session.execute.return_value = rows_result

        response = client.get("/v1/data/visao-cliente/historico-alteracoes?documento=12.345.678/0001-90")
        assert response.status_code == 200
        payload = response.json()
        assert payload["documento_consultado"] == "12345678000190"
        assert payload["total_eventos"] == 1
        assert payload["items"][0]["change_type"] == "UPDATE"
        assert payload["items"][0]["field_name"] == "nome_cliente"
