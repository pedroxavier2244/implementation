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
