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

        mock_session.query().filter_by().first.return_value = EtlFile(id="f-1", hash_sha256="h", file_date=None)
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


def test_get_data_visao_cliente_fallbacks_to_brasilapi_when_not_found_locally(client):
    with patch("api.routes.data.get_db_session") as mock_db, patch("api.routes.data.get_settings") as mock_settings, patch(
        "api.routes.data.fetch_cnpj"
    ) as mock_fetch:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        rows_result_empty_1 = MagicMock()
        rows_result_empty_1.mappings.return_value.all.return_value = []
        rows_result_empty_2 = MagicMock()
        rows_result_empty_2.mappings.return_value.all.return_value = []
        mock_session.execute.side_effect = [rows_result_empty_1, rows_result_empty_2]

        mock_session.query().filter_by().first.return_value = None

        mock_settings.return_value.CNPJ_CACHE_TTL_DAYS = 30
        mock_settings.return_value.BRASILAPI_TIMEOUT = 5
        mock_fetch.return_value = {
            "razao_social": "EMPRESA TESTE LTDA",
            "nome_fantasia": "EMPRESA TESTE",
            "situacao_cadastral": "ATIVA",
            "descricao_situacao": "ATIVA",
            "cnae_fiscal": "4751201",
            "cnae_descricao": "COMERCIO VAREJISTA",
            "natureza_juridica": "2062",
            "capital_social": "50000",
            "porte": "DEMAIS",
            "uf": "SP",
            "municipio": "SAO PAULO",
            "email": "contato@teste.com",
            "data_inicio_ativ": "2010-01-01",
        }

        response = client.get("/v1/data/visao-cliente?documento=12.345.678/0001-90")
        assert response.status_code == 200
        payload = response.json()

        assert payload["documento_consultado"] == "12345678000190"
        assert payload["total"] == 1
        assert payload["items"][0]["data_source"] == "receita_federal_brasilapi"
        assert payload["items"][0]["nome_cliente"] == "EMPRESA TESTE LTDA"
        assert "status_cc" in payload["items"][0]
        assert payload["items"][0]["status_cc"] is None
        mock_fetch.assert_called_once_with("12345678000190", timeout=5)


def test_get_data_visao_cliente_does_not_call_brasilapi_for_cpf(client):
    with patch("api.routes.data.get_db_session") as mock_db, patch("api.routes.data.fetch_cnpj") as mock_fetch:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        rows_result_empty_1 = MagicMock()
        rows_result_empty_1.mappings.return_value.all.return_value = []
        rows_result_empty_2 = MagicMock()
        rows_result_empty_2.mappings.return_value.all.return_value = []
        mock_session.execute.side_effect = [rows_result_empty_1, rows_result_empty_2]

        response = client.get("/v1/data/visao-cliente?documento=123.456.789-01")
        assert response.status_code == 200
        payload = response.json()
        assert payload["documento_consultado"] == "12345678901"
        assert payload["total"] == 0
        mock_fetch.assert_not_called()


def test_get_cnpj_endpoint_fallbacks_to_brasilapi_when_cache_missing(client):
    with patch("api.routes.cnpj.get_db_session") as mock_db, patch("api.routes.cnpj.get_settings") as mock_settings, patch(
        "api.routes.cnpj.fetch_cnpj"
    ) as mock_fetch:
        mock_session = MagicMock()
        mock_db.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_db.return_value.__exit__ = MagicMock(return_value=False)

        mock_session.query().filter_by().first.return_value = None
        mock_settings.return_value.BRASILAPI_TIMEOUT = 5
        mock_fetch.return_value = {
            "razao_social": "EMPRESA TESTE LTDA",
            "nome_fantasia": "EMPRESA TESTE",
            "situacao_cadastral": "ATIVA",
            "descricao_situacao": "ATIVA",
            "cnae_fiscal": "4751201",
            "cnae_descricao": "COMERCIO VAREJISTA",
            "natureza_juridica": "2062",
            "capital_social": "50000",
            "porte": "DEMAIS",
            "uf": "SP",
            "municipio": "SAO PAULO",
            "email": "contato@teste.com",
            "data_inicio_ativ": "2010-01-01",
        }

        response = client.get("/v1/cnpj/12345678000190")
        assert response.status_code == 200
        payload = response.json()
        assert payload["cnpj"] == "12345678000190"
        assert payload["data_source"] == "receita_federal_brasilapi"
        assert payload["razao_social"] == "EMPRESA TESTE LTDA"
        mock_fetch.assert_called_once_with("12345678000190", timeout=5)


def test_get_cnpj_endpoint_rejects_invalid_length(client):
    response = client.get("/v1/cnpj/123")
    assert response.status_code == 400
