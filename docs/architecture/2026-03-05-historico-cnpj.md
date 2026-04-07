# Histórico Cronológico de CNPJ — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Expor via API a linha do tempo de mudanças de um CNPJ, mostrando todas as snapshots históricas e quais campos mudaram entre elas.

**Architecture:** A tabela `staging_visao_cliente` já armazena todas as linhas de todos os jobs ETL com `etl_job_id` e `loaded_at`. Um novo endpoint `GET /v1/data/visao-cliente/historico` consulta essa tabela por CNPJ, ordena por `data_base` e calcula o diff entre snapshots consecutivas em Python. Nenhuma mudança de banco necessária.

**Tech Stack:** FastAPI, SQLAlchemy (raw SQL via `text()`), Pydantic v2, PostgreSQL, pytest

---

## Contexto para o implementador

### Estrutura relevante do projeto

```
etl-system/
  api/
    routes/data.py        <- onde mora GET /v1/data/visao-cliente (adicionar nova rota aqui)
    schemas/data.py       <- schemas Pydantic de resposta (adicionar novos schemas aqui)
  shared/
    visao_cliente_schema.py  <- STAGING_TABLE_NAME = "staging_visao_cliente", REQUIRED_COLUMNS (116 colunas)
    db.py                    <- get_db_session() context manager
  tests/
    unit/
      test_etl_steps.py   <- testes unitários existentes (para referência de estilo)
```

### Como rodar os testes

```bash
# No diretório do worktree: C:\Users\MB NEGOCIOS\etl-system\.worktrees\implementation
pytest tests/unit/ -v
```

### Staging table schema relevante

A `staging_visao_cliente` tem todas as colunas de `REQUIRED_COLUMNS` mais:
- `etl_job_id` TEXT — ID do job ETL que inseriu a linha
- `loaded_at` TIMESTAMP — quando foi inserida

### Como o endpoint existente funciona (referência)

`GET /v1/data/visao-cliente` em `api/routes/data.py` usa `text()` do SQLAlchemy para fazer query direta. O mesmo padrão deve ser usado no novo endpoint.

O router é registrado em `api/main.py` (ou similar) com prefixo `/v1`. A nova rota usa o mesmo `router` do `data.py`.

---

### Task 1: Schemas Pydantic para o novo endpoint

**Files:**
- Modify: `api/schemas/data.py`
- Test: `tests/unit/test_historico_schemas.py` (criar)

**Step 1: Criar o arquivo de teste**

```bash
# Criar tests/unit/test_historico_schemas.py
```

```python
# tests/unit/test_historico_schemas.py
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
```

**Step 2: Rodar para confirmar falha**

```bash
pytest tests/unit/test_historico_schemas.py -v
```
Expected: FAIL com `ImportError: cannot import name 'SnapshotItem'`

**Step 3: Adicionar os schemas em `api/schemas/data.py`**

```python
# Adicionar ao final do arquivo api/schemas/data.py
from typing import Any, Optional


class SnapshotItem(BaseModel):
    data_base: Optional[str]
    carregado_em: Optional[str]
    etl_job_id: Optional[str]
    campos_alterados: Optional[dict[str, dict[str, Any]]]
    dados: dict[str, Any]


class VisaoClienteHistoricoOut(BaseModel):
    documento_consultado: str
    total_snapshots: int
    limit: int
    offset: int
    snapshots: list[SnapshotItem]
```

**Step 4: Rodar testes**

```bash
pytest tests/unit/test_historico_schemas.py -v
```
Expected: PASS (3 testes)

**Step 5: Commit**

```bash
git add api/schemas/data.py tests/unit/test_historico_schemas.py
git commit -m "feat: add Pydantic schemas for CNPJ history endpoint"
```

---

### Task 2: Função `_compute_diff` para calcular campos alterados

Esta função recebe dois dicts (snapshot anterior e atual) e retorna um dict com os campos que mudaram.

**Files:**
- Modify: `api/routes/data.py`
- Test: `tests/unit/test_historico_schemas.py` (adicionar)

**Step 1: Adicionar testes para `_compute_diff`**

Adicionar ao final de `tests/unit/test_historico_schemas.py`:

```python
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
    assert "etl_job_id" not in diff
    assert "loaded_at" not in diff
    assert diff == {}


def test_compute_diff_first_snapshot_returns_none():
    from api.routes.data import _compute_diff

    result = _compute_diff(None, {"nome_cliente": "ABC"})
    assert result is None
```

**Step 2: Rodar para confirmar falha**

```bash
pytest tests/unit/test_historico_schemas.py::test_compute_diff_detects_changes -v
```
Expected: FAIL com `ImportError: cannot import name '_compute_diff'`

**Step 3: Implementar `_compute_diff` em `api/routes/data.py`**

Adicionar logo após os imports existentes (antes do `router = APIRouter(...)`):

```python
_DIFF_IGNORE_FIELDS = frozenset({"etl_job_id", "loaded_at", "__total"})


def _compute_diff(
    anterior: dict | None, atual: dict
) -> dict[str, dict[str, str]] | None:
    """Retorna campos que mudaram entre duas snapshots. None se for a primeira."""
    if anterior is None:
        return None
    diff = {}
    for key, val_atual in atual.items():
        if key in _DIFF_IGNORE_FIELDS:
            continue
        val_anterior = anterior.get(key)
        # Ignora campos onde ambos são None/vazios
        if val_anterior is None and val_atual is None:
            continue
        if str(val_anterior) != str(val_atual):
            diff[key] = {"de": str(val_anterior) if val_anterior is not None else None,
                         "para": str(val_atual) if val_atual is not None else None}
    return diff if diff else None
```

**Step 4: Rodar testes**

```bash
pytest tests/unit/test_historico_schemas.py -v
```
Expected: PASS (todos os testes do arquivo)

**Step 5: Commit**

```bash
git add api/routes/data.py tests/unit/test_historico_schemas.py
git commit -m "feat: add _compute_diff helper for snapshot comparison"
```

---

### Task 3: Endpoint `GET /data/visao-cliente/historico`

**Files:**
- Modify: `api/routes/data.py`
- Test: `tests/unit/test_historico_schemas.py` (adicionar)

**Step 1: Adicionar testes de integração unitária para o endpoint**

Adicionar ao final de `tests/unit/test_historico_schemas.py`:

```python
import pandas as pd
from unittest.mock import patch, MagicMock


def _make_staging_rows(cnpj: str):
    """Retorna 2 linhas simulando staging_visao_cliente."""
    row1 = {col: None for col in ["cd_cpf_cnpj_cliente", "data_base", "loaded_at",
                                   "etl_job_id", "nome_cliente", "vl_cash_in_mtd"]}
    row1["cd_cpf_cnpj_cliente"] = cnpj
    row1["data_base"] = "2026-02-02"
    row1["loaded_at"] = "2026-03-03T10:00:00Z"
    row1["etl_job_id"] = "job-1"
    row1["nome_cliente"] = "EMPRESA LTDA"
    row1["vl_cash_in_mtd"] = "3000"
    row1["__total"] = 2

    row2 = dict(row1)
    row2["data_base"] = "2026-02-21"
    row2["loaded_at"] = "2026-03-01T15:00:00Z"
    row2["etl_job_id"] = "job-2"
    row2["vl_cash_in_mtd"] = "8500"
    row2["__total"] = 2
    return [row1, row2]


def test_get_historico_returns_snapshots_with_diff():
    from fastapi.testclient import TestClient
    from api.main import app

    rows = _make_staging_rows("12345678000190")

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = rows

    mock_session = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_session.__enter__ = lambda s: s
    mock_session.__exit__ = MagicMock(return_value=False)

    with patch("api.routes.data.get_db_session", return_value=mock_session):
        client = TestClient(app)
        response = client.get("/v1/data/visao-cliente/historico?documento=12345678000190")

    assert response.status_code == 200
    body = response.json()
    assert body["total_snapshots"] == 2
    assert len(body["snapshots"]) == 2
    # Primeira snapshot sem diff
    assert body["snapshots"][0]["campos_alterados"] is None
    # Segunda snapshot com diff no vl_cash_in_mtd
    assert body["snapshots"][1]["campos_alterados"]["vl_cash_in_mtd"]["de"] == "3000"
    assert body["snapshots"][1]["campos_alterados"]["vl_cash_in_mtd"]["para"] == "8500"


def test_get_historico_returns_empty_for_unknown_cnpj():
    from fastapi.testclient import TestClient
    from api.main import app

    mock_result = MagicMock()
    mock_result.mappings.return_value.all.return_value = []

    mock_session = MagicMock()
    mock_session.execute.return_value = mock_result
    mock_session.__enter__ = lambda s: s
    mock_session.__exit__ = MagicMock(return_value=False)

    with patch("api.routes.data.get_db_session", return_value=mock_session):
        client = TestClient(app)
        response = client.get("/v1/data/visao-cliente/historico?documento=99999999000199")

    assert response.status_code == 200
    body = response.json()
    assert body["total_snapshots"] == 0
    assert body["snapshots"] == []
```

**Step 2: Rodar para confirmar falha**

```bash
pytest tests/unit/test_historico_schemas.py::test_get_historico_returns_snapshots_with_diff -v
```
Expected: FAIL com 404 ou erro de rota não encontrada

**Step 3: Implementar o endpoint em `api/routes/data.py`**

Adicionar ao final do arquivo (após o endpoint `get_visao_cliente_by_documento`):

```python
@router.get("/visao-cliente/historico", response_model=VisaoClienteHistoricoOut)
def get_visao_cliente_historico(
    documento: str = Query(..., description="CPF/CNPJ com ou sem pontuacao"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    from api.schemas.data import SnapshotItem, VisaoClienteHistoricoOut
    from shared.visao_cliente_schema import STAGING_TABLE_NAME

    documento_consultado = _only_digits(documento)
    if not documento_consultado:
        raise HTTPException(status_code=400, detail="documento must contain digits")

    with get_db_session() as session:
        rows = session.execute(
            text(
                f"""
                SELECT *, COUNT(*) OVER() AS __total
                FROM {STAGING_TABLE_NAME}
                WHERE cd_cpf_cnpj_cliente = :documento
                ORDER BY data_base ASC NULLS LAST, loaded_at ASC
                LIMIT :limit OFFSET :offset
                """
            ),
            {"documento": documento_consultado, "limit": limit, "offset": offset},
        ).mappings().all()

    total = int(rows[0]["__total"]) if rows else 0

    snapshots = []
    anterior: dict | None = None
    for row in rows:
        row_dict = dict(row)
        row_dict.pop("__total", None)

        dados = {k: v for k, v in row_dict.items()
                 if k not in ("etl_job_id", "loaded_at")}
        diff = _compute_diff(anterior, row_dict)

        snapshots.append(SnapshotItem(
            data_base=str(row_dict.get("data_base")) if row_dict.get("data_base") else None,
            carregado_em=str(row_dict.get("loaded_at")) if row_dict.get("loaded_at") else None,
            etl_job_id=str(row_dict.get("etl_job_id")) if row_dict.get("etl_job_id") else None,
            campos_alterados=diff,
            dados=dados,
        ))
        anterior = row_dict

    return VisaoClienteHistoricoOut(
        documento_consultado=documento_consultado,
        total_snapshots=total,
        limit=limit,
        offset=offset,
        snapshots=snapshots,
    )
```

Também adicionar o import do schema no topo do arquivo (junto com os outros imports de `api.schemas.data`):

```python
from api.schemas.data import VisaoClienteSearchOut, SnapshotItem, VisaoClienteHistoricoOut
```

**Step 4: Rodar todos os testes**

```bash
pytest tests/unit/ -v
```
Expected: PASS em todos (incluindo os testes existentes em `test_etl_steps.py`)

**Step 5: Commit**

```bash
git add api/routes/data.py tests/unit/test_historico_schemas.py
git commit -m "feat: add GET /v1/data/visao-cliente/historico endpoint"
```

---

### Task 4: Atualizar README e fazer deploy

**Files:**
- Modify: `README.md`
- Deploy: containers Docker no diretório `C:\Users\MB NEGOCIOS\etl-system` (não no worktree)

**Step 1: Adicionar endpoint ao README**

Em `README.md`, na seção `### Dados (/v1/data)`, adicionar após a linha do `visao-cliente`:

```markdown
- `GET /v1/data/visao-cliente/historico?documento=<cpf_ou_cnpj>&limit=50&offset=0`
  - Retorna linha do tempo de snapshots do CNPJ/CPF.
  - Cada snapshot inclui `campos_alterados` com diff em relação à anterior.
  - Ordenado por `data_base` ASC (mais antigo primeiro).
```

**Step 2: Commit do README**

```bash
git add README.md
git commit -m "docs: add historico endpoint to README"
```

**Step 3: Merge para master e rebuild do container `api`**

```bash
# Merge feature branch -> master
cd "C:\Users\MB NEGOCIOS\etl-system"
git checkout master
git merge feature/implementation

# Rebuild apenas o container da API
docker compose build api
docker compose up -d api
```

**Step 4: Verificar deploy**

```bash
# Aguardar alguns segundos e testar
curl "http://localhost:8000/v1/data/visao-cliente/historico?documento=12345678000190"
```
Expected: JSON com `total_snapshots`, `snapshots` (pode ser 0 se CNPJ não existir)

**Step 5: Verificar no Swagger**

Abrir `http://localhost:8000/docs` e confirmar que o endpoint `/v1/data/visao-cliente/historico` aparece documentado.
