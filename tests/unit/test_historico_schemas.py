from api.schemas.data import SnapshotItem, VisaoClienteHistoricoOut


def test_snapshot_item_with_no_diff():
    item = SnapshotItem(
        data_base="2026-02-02",
        carregado_em="2026-03-03T10:00:00Z",
        etl_job_id="abc-123",
        campos_alterados=None,
        dados={"cd_cpf_cnpj_cliente": "12345678000190"},
    )
    assert item.campos_alterados is None
    assert item.dados["cd_cpf_cnpj_cliente"] == "12345678000190"


def test_snapshot_item_with_diff():
    item = SnapshotItem(
        data_base="2026-02-21",
        carregado_em="2026-03-01T15:00:00Z",
        etl_job_id="def-456",
        campos_alterados={"vl_cash_in_mtd": {"de": "3000", "para": "8500"}},
        dados={"cd_cpf_cnpj_cliente": "12345678000190"},
    )
    assert item.campos_alterados["vl_cash_in_mtd"]["de"] == "3000"


def test_historico_out_structure():
    out = VisaoClienteHistoricoOut(
        documento_consultado="12345678000190",
        total_snapshots=0,
        limit=50,
        offset=0,
        snapshots=[],
    )
    assert out.total_snapshots == 0
    assert out.snapshots == []


def test_snapshot_item_requires_dados():
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        SnapshotItem(
            data_base="2026-01-01",
            carregado_em=None,
            etl_job_id=None,
            campos_alterados=None,
        )


def test_compute_diff_detects_changes():
    from api.routes.data import _compute_diff

    anterior = {"campo_a": "100", "campo_b": "igual", "campo_c": None}
    atual = {"campo_a": "200", "campo_b": "igual", "campo_c": None}
    diff = _compute_diff(anterior, atual)
    assert "campo_a" in diff
    assert diff["campo_a"] == {"de": "100", "para": "200"}
    assert "campo_b" not in diff  # sem mudanca
    assert "campo_c" not in diff  # ambos None, ignorar


def test_compute_diff_ignores_metadata_fields():
    from api.routes.data import _compute_diff

    anterior = {"etl_job_id": "job-1", "loaded_at": "2026-01-01", "nome_cliente": "ABC"}
    atual = {"etl_job_id": "job-2", "loaded_at": "2026-02-01", "nome_cliente": "ABC"}
    diff = _compute_diff(anterior, atual)
    assert diff is None


def test_compute_diff_first_snapshot_returns_none():
    from api.routes.data import _compute_diff

    result = _compute_diff(None, {"nome_cliente": "ABC"})
    assert result is None


def test_get_historico_returns_snapshots_with_diff():
    from unittest.mock import patch, MagicMock
    from fastapi.testclient import TestClient

    cnpj = "12345678000190"
    row1 = {
        "cd_cpf_cnpj_cliente": cnpj,
        "data_base": "2026-02-02",
        "loaded_at": "2026-03-03T10:00:00+00:00",
        "etl_job_id": "job-1",
        "nome_cliente": "EMPRESA LTDA",
        "vl_cash_in_mtd": "3000",
        "__total": 2,
    }
    row2 = {
        "cd_cpf_cnpj_cliente": cnpj,
        "data_base": "2026-02-21",
        "loaded_at": "2026-03-01T15:00:00+00:00",
        "etl_job_id": "job-2",
        "nome_cliente": "EMPRESA LTDA",
        "vl_cash_in_mtd": "8500",
        "__total": 2,
    }

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = [row1, row2]

    mock_session = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_session.__enter__ = lambda s: s
    mock_session.__exit__ = MagicMock(return_value=False)

    with patch("api.routes.data.get_db_session", return_value=mock_session):
        from api.main import app
        from fastapi.testclient import TestClient
        client = TestClient(app)
        response = client.get(f"/v1/data/visao-cliente/historico?documento={cnpj}")

    assert response.status_code == 200
    body = response.json()
    assert body["total_snapshots"] == 2
    assert len(body["snapshots"]) == 2
    assert body["snapshots"][0]["campos_alterados"] is None
    diff = body["snapshots"][1]["campos_alterados"]
    assert diff is not None
    assert diff["vl_cash_in_mtd"]["de"] == "3000"
    assert diff["vl_cash_in_mtd"]["para"] == "8500"


def test_get_historico_returns_empty_for_unknown_cnpj():
    from unittest.mock import patch, MagicMock
    from fastapi.testclient import TestClient

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = []

    mock_session = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_session.__enter__ = lambda s: s
    mock_session.__exit__ = MagicMock(return_value=False)

    with patch("api.routes.data.get_db_session", return_value=mock_session):
        from api.main import app
        from fastapi.testclient import TestClient
        client = TestClient(app)
        response = client.get("/v1/data/visao-cliente/historico?documento=99999999000199")

    assert response.status_code == 200
    body = response.json()
    assert body["total_snapshots"] == 0
    assert body["snapshots"] == []
