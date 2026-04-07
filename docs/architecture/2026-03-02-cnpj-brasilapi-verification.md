# CNPJ BrasilAPI Verification — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Apos cada ETL, buscar dados de CNPJ na BrasilAPI, salvar campos RF_ na tabela final e registrar divergencias entre C6 Bank e Receita Federal com alerta.

**Architecture:** Novo passo `cnpj_verify` no pipeline ETL (roda apos `upsert`), usa cache de 30 dias em `cnpj_rf_cache` para evitar re-consultas, registra divergencias em `cnpj_divergencia` e dispara alerta via notifier existente. Rf_* columns ficam somente na `final_visao_cliente` — NAO entram no staging nem no REQUIRED_COLUMNS.

**Tech Stack:** Python, SQLAlchemy, httpx (ja no requirements/worker.txt), PostgreSQL, Celery, FastAPI

---

## Task 1: Adicionar configuracoes em `shared/config.py`

**Files:**
- Modify: `shared/config.py`
- Test: `tests/unit/test_config.py`

**Step 1: Escrever teste que falha**

Abrir `tests/unit/test_config.py` e adicionar ao final:

```python
def test_cnpj_settings_defaults():
    from shared.config import Settings
    s = Settings()
    assert s.CNPJ_CACHE_TTL_DAYS == 30
    assert s.BRASILAPI_TIMEOUT == 10
```

**Step 2: Rodar para confirmar falha**

```
python -m pytest tests/unit/test_config.py::test_cnpj_settings_defaults -v
```
Esperado: `FAILED` — `Settings has no attribute CNPJ_CACHE_TTL_DAYS`

**Step 3: Implementar — adicionar ao final da classe Settings em `shared/config.py`**

```python
    # CNPJ Verification
    CNPJ_CACHE_TTL_DAYS: int = 30
    BRASILAPI_TIMEOUT: int = 10
```

Inserir antes da linha `@property` de `database_url`.

**Step 4: Rodar para confirmar aprovacao**

```
python -m pytest tests/unit/test_config.py -v
```
Esperado: todos PASSED

**Step 5: Commit**

```bash
git add shared/config.py tests/unit/test_config.py
git commit -m "feat: add CNPJ_CACHE_TTL_DAYS and BRASILAPI_TIMEOUT settings"
```

---

## Task 2: Adicionar modelos `CnpjRfCache` e `CnpjDivergencia` em `shared/models.py`

**Files:**
- Modify: `shared/models.py`
- Test: `tests/unit/test_models.py`

**Step 1: Escrever testes que falham**

Adicionar ao final de `tests/unit/test_models.py`:

```python
def test_cnpj_rf_cache_table_exists():
    from shared.models import CnpjRfCache
    assert CnpjRfCache.__tablename__ == "cnpj_rf_cache"
    cols = {c.name for c in CnpjRfCache.__table__.columns}
    assert "cnpj" in cols
    assert "razao_social" in cols
    assert "last_checked_at" in cols


def test_cnpj_divergencia_table_exists():
    from shared.models import CnpjDivergencia
    assert CnpjDivergencia.__tablename__ == "cnpj_divergencia"
    cols = {c.name for c in CnpjDivergencia.__table__.columns}
    assert "cnpj" in cols
    assert "campo" in cols
    assert "valor_c6" in cols
    assert "valor_rf" in cols
```

**Step 2: Rodar para confirmar falha**

```
python -m pytest tests/unit/test_models.py::test_cnpj_rf_cache_table_exists tests/unit/test_models.py::test_cnpj_divergencia_table_exists -v
```
Esperado: `FAILED` — `cannot import name 'CnpjRfCache'`

**Step 3: Implementar — adicionar ao final de `shared/models.py` (antes dos Index estrategicos)**

```python
class CnpjRfCache(Base):
    __tablename__ = "cnpj_rf_cache"

    cnpj               = Column(Text, primary_key=True)
    razao_social       = Column(Text)
    nome_fantasia      = Column(Text)
    situacao_cadastral = Column(Text)
    descricao_situacao = Column(Text)
    cnae_fiscal        = Column(Text)
    cnae_descricao     = Column(Text)
    natureza_juridica  = Column(Text)
    capital_social     = Column(Text)
    porte              = Column(Text)
    uf                 = Column(Text)
    municipio          = Column(Text)
    email              = Column(Text)
    data_inicio_ativ   = Column(Text)
    last_checked_at    = Column(DateTime(timezone=True))


class CnpjDivergencia(Base):
    __tablename__ = "cnpj_divergencia"

    id       = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    job_id   = Column(String(36), ForeignKey("etl_job_run.id"), nullable=False)
    cnpj     = Column(Text, nullable=False)
    campo    = Column(Text, nullable=False)
    valor_c6 = Column(Text)
    valor_rf = Column(Text)
    found_at = Column(DateTime(timezone=True), default=utcnow)
```

**Step 4: Rodar todos os testes de modelos**

```
python -m pytest tests/unit/test_models.py -v
```
Esperado: todos PASSED

**Step 5: Commit**

```bash
git add shared/models.py tests/unit/test_models.py
git commit -m "feat: add CnpjRfCache and CnpjDivergencia models"
```

---

## Task 3: Migration 006 — criar tabelas `cnpj_rf_cache` e `cnpj_divergencia`

**Files:**
- Create: `migrations/versions/20260302_000006_cnpj_rf_tables.py`

**Step 1: Criar o arquivo de migration**

```python
"""create cnpj_rf_cache and cnpj_divergencia tables

Revision ID: 20260302_000006
Revises: 20260302_000005
Create Date: 2026-03-02 00:00:06
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "20260302_000006"
down_revision: Union[str, None] = "20260302_000005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "cnpj_rf_cache",
        sa.Column("cnpj",               sa.Text(), primary_key=True),
        sa.Column("razao_social",        sa.Text(), nullable=True),
        sa.Column("nome_fantasia",       sa.Text(), nullable=True),
        sa.Column("situacao_cadastral",  sa.Text(), nullable=True),
        sa.Column("descricao_situacao",  sa.Text(), nullable=True),
        sa.Column("cnae_fiscal",         sa.Text(), nullable=True),
        sa.Column("cnae_descricao",      sa.Text(), nullable=True),
        sa.Column("natureza_juridica",   sa.Text(), nullable=True),
        sa.Column("capital_social",      sa.Text(), nullable=True),
        sa.Column("porte",               sa.Text(), nullable=True),
        sa.Column("uf",                  sa.Text(), nullable=True),
        sa.Column("municipio",           sa.Text(), nullable=True),
        sa.Column("email",               sa.Text(), nullable=True),
        sa.Column("data_inicio_ativ",    sa.Text(), nullable=True),
        sa.Column("last_checked_at",     sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "cnpj_divergencia",
        sa.Column("id",       sa.String(36), primary_key=True),
        sa.Column("job_id",   sa.String(36), sa.ForeignKey("etl_job_run.id"), nullable=False),
        sa.Column("cnpj",     sa.Text(), nullable=False),
        sa.Column("campo",    sa.Text(), nullable=False),
        sa.Column("valor_c6", sa.Text(), nullable=True),
        sa.Column("valor_rf", sa.Text(), nullable=True),
        sa.Column("found_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_cnpj_divergencia_job_id", "cnpj_divergencia", ["job_id"])
    op.create_index("idx_cnpj_divergencia_cnpj",   "cnpj_divergencia", ["cnpj"])


def downgrade() -> None:
    op.drop_index("idx_cnpj_divergencia_cnpj",   table_name="cnpj_divergencia")
    op.drop_index("idx_cnpj_divergencia_job_id", table_name="cnpj_divergencia")
    op.drop_table("cnpj_divergencia")
    op.drop_table("cnpj_rf_cache")
```

**Step 2: Verificar que o arquivo foi criado corretamente**

```
python -c "import ast; ast.parse(open('migrations/versions/20260302_000006_cnpj_rf_tables.py').read()); print('syntax ok')"
```
Esperado: `syntax ok`

**Step 3: Commit**

```bash
git add migrations/versions/20260302_000006_cnpj_rf_tables.py
git commit -m "feat: migration 006 — create cnpj_rf_cache and cnpj_divergencia tables"
```

---

## Task 4: Migration 007 — adicionar colunas `rf_*` em `final_visao_cliente`

**Files:**
- Create: `migrations/versions/20260302_000007_final_rf_columns.py`

**Importante:** As colunas rf_* vao SOMENTE em `final_visao_cliente`.
NAO entram em `staging_visao_cliente` nem em `REQUIRED_COLUMNS`.
Sao preenchidas diretamente pelo passo `cnpj_verify`, nao pelo pipeline ETL.

**Step 1: Criar o arquivo de migration**

```python
"""add rf_ columns to final_visao_cliente

Revision ID: 20260302_000007
Revises: 20260302_000006
Create Date: 2026-03-02 00:00:07
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = "20260302_000007"
down_revision: Union[str, None] = "20260302_000006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

RF_COLUMNS = [
    "rf_razao_social",
    "rf_natureza_juridica",
    "rf_capital_social",
    "rf_porte_empresa",
    "rf_nome_fantasia",
    "rf_situacao_cadastral",
    "rf_data_inicio_ativ",
    "rf_cnae_principal",
    "rf_uf",
    "rf_municipio",
    "rf_email",
]


def upgrade() -> None:
    for col in RF_COLUMNS:
        op.add_column("final_visao_cliente", sa.Column(col, sa.Text(), nullable=True))


def downgrade() -> None:
    for col in reversed(RF_COLUMNS):
        op.drop_column("final_visao_cliente", col)
```

**Step 2: Verificar sintaxe**

```
python -c "import ast; ast.parse(open('migrations/versions/20260302_000007_final_rf_columns.py').read()); print('syntax ok')"
```
Esperado: `syntax ok`

**Step 3: Commit**

```bash
git add migrations/versions/20260302_000007_final_rf_columns.py
git commit -m "feat: migration 007 — add rf_* columns to final_visao_cliente"
```

---

## Task 5: Cliente BrasilAPI em `shared/brasilapi.py`

**Files:**
- Create: `shared/brasilapi.py`
- Test: `tests/unit/test_brasilapi.py`

**Step 1: Criar `tests/unit/test_brasilapi.py` com testes que falham**

```python
from unittest.mock import MagicMock, patch
import pytest


def test_fetch_cnpj_returns_mapped_dict():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "cnpj": "11222333000181",
        "razao_social": "EMPRESA TESTE LTDA",
        "nome_fantasia": "TESTE",
        "situacao_cadastral": 2,
        "descricao_situacao_cadastral": "ATIVA",
        "cnae_fiscal": 6201501,
        "cnae_fiscal_descricao": "Desenvolvimento de programas",
        "natureza_juridica": "Sociedade Empresaria Limitada",
        "capital_social": 10000.0,
        "porte": "ME",
        "uf": "SP",
        "municipio": "SAO PAULO",
        "email": "contato@teste.com",
        "data_inicio_atividade": "2020-01-15",
    }

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5)

    assert result["razao_social"] == "EMPRESA TESTE LTDA"
    assert result["situacao_cadastral"] == "2"
    assert result["cnae_fiscal"] == "6201501"
    assert result["capital_social"] == "10000.0"


def test_fetch_cnpj_returns_none_on_404():
    mock_response = MagicMock()
    mock_response.status_code = 404

    with patch("httpx.get", return_value=mock_response):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("00000000000000", timeout=5)

    assert result is None


def test_fetch_cnpj_returns_none_on_exception():
    with patch("httpx.get", side_effect=Exception("timeout")):
        from shared.brasilapi import fetch_cnpj
        result = fetch_cnpj("11222333000181", timeout=5)

    assert result is None


def test_normalize_for_comparison_removes_accents_and_punctuation():
    from shared.brasilapi import normalize_for_comparison
    assert normalize_for_comparison("Sao Paulo") == "SAO PAULO"
    assert normalize_for_comparison("LTDA.") == "LTDA"
    assert normalize_for_comparison("S/A") == "S A"
    assert normalize_for_comparison(None) == ""
    assert normalize_for_comparison("") == ""


def test_compare_fields_detects_divergence():
    from shared.brasilapi import compare_fields
    c6_data = {"nome_cliente": "EMPRESA VELHA SA", "uf": "SP", "cidade": "SAO PAULO"}
    rf_data = {"razao_social": "EMPRESA NOVA SA", "uf": "RJ", "municipio": "RIO DE JANEIRO"}

    divergencias = compare_fields(c6_data, rf_data)
    campos = [d["campo"] for d in divergencias]
    assert "nome_cliente" in campos
    assert "uf" in campos
    assert "cidade" in campos


def test_compare_fields_no_divergence_when_equal():
    from shared.brasilapi import compare_fields
    c6_data = {"nome_cliente": "EMPRESA TESTE LTDA", "uf": "SP", "cidade": "SAO PAULO"}
    rf_data = {"razao_social": "EMPRESA TESTE LTDA", "uf": "SP", "municipio": "SAO PAULO"}

    divergencias = compare_fields(c6_data, rf_data)
    assert divergencias == []
```

**Step 2: Rodar para confirmar falha**

```
python -m pytest tests/unit/test_brasilapi.py -v
```
Esperado: `FAILED` — `cannot import name 'fetch_cnpj'`

**Step 3: Implementar `shared/brasilapi.py`**

```python
import unicodedata
import re
import logging

import httpx

logger = logging.getLogger(__name__)

BRASILAPI_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

# Mapeamento C6 Bank -> BrasilAPI para comparacao de campos
FIELD_MAP = [
    ("nome_cliente", "razao_social"),
    ("uf",           "uf"),
    ("cidade",       "municipio"),
    ("ramo_atuacao", "cnae_descricao"),
]


def normalize_for_comparison(value) -> str:
    """Remove acentos, pontuacao e converte para maiusculas para comparacao."""
    if not value:
        return ""
    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip().upper()
    return text


def fetch_cnpj(cnpj: str, timeout: int = 10) -> dict | None:
    """
    Consulta a BrasilAPI para um CNPJ.
    Retorna dict com campos normalizados para TEXT, ou None se nao encontrado/erro.
    """
    url = BRASILAPI_URL.format(cnpj=cnpj)
    try:
        response = httpx.get(url, timeout=timeout)
        if response.status_code == 404:
            logger.warning("CNPJ nao encontrado na BrasilAPI: %s", cnpj)
            return None
        if response.status_code != 200:
            logger.warning("BrasilAPI retornou status %s para CNPJ %s", response.status_code, cnpj)
            return None

        data = response.json()
        return {
            "razao_social":        str(data.get("razao_social") or ""),
            "nome_fantasia":       str(data.get("nome_fantasia") or ""),
            "situacao_cadastral":  str(data.get("situacao_cadastral") or ""),
            "descricao_situacao":  str(data.get("descricao_situacao_cadastral") or ""),
            "cnae_fiscal":         str(data.get("cnae_fiscal") or ""),
            "cnae_descricao":      str(data.get("cnae_fiscal_descricao") or ""),
            "natureza_juridica":   str(data.get("natureza_juridica") or ""),
            "capital_social":      str(data.get("capital_social") or ""),
            "porte":               str(data.get("porte") or ""),
            "uf":                  str(data.get("uf") or ""),
            "municipio":           str(data.get("municipio") or ""),
            "email":               str(data.get("email") or ""),
            "data_inicio_ativ":    str(data.get("data_inicio_atividade") or ""),
        }
    except Exception as exc:
        logger.warning("Erro ao consultar BrasilAPI para CNPJ %s: %s", cnpj, exc)
        return None


def compare_fields(c6_row: dict, rf_data: dict) -> list[dict]:
    """
    Compara campos do C6 Bank com dados da Receita Federal.
    Retorna lista de divergencias: [{"campo": str, "valor_c6": str, "valor_rf": str}]
    """
    divergencias = []
    for c6_field, rf_field in FIELD_MAP:
        c6_val = normalize_for_comparison(c6_row.get(c6_field))
        rf_val = normalize_for_comparison(rf_data.get(rf_field))
        if c6_val and rf_val and c6_val != rf_val:
            divergencias.append({
                "campo":    c6_field,
                "valor_c6": str(c6_row.get(c6_field) or ""),
                "valor_rf": str(rf_data.get(rf_field) or ""),
            })
    return divergencias
```

**Step 4: Rodar testes**

```
python -m pytest tests/unit/test_brasilapi.py -v
```
Esperado: todos PASSED

**Step 5: Commit**

```bash
git add shared/brasilapi.py tests/unit/test_brasilapi.py
git commit -m "feat: add BrasilAPI client with CNPJ fetch and field comparison"
```

---

## Task 6: Passo `cnpj_verify` em `worker/steps/cnpj_verify.py`

**Files:**
- Create: `worker/steps/cnpj_verify.py`
- Test: `tests/unit/test_cnpj_verify.py`

**Step 1: Criar `tests/unit/test_cnpj_verify.py` com testes que falham**

```python
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
```

**Step 2: Rodar para confirmar falha**

```
python -m pytest tests/unit/test_cnpj_verify.py -v
```
Esperado: `FAILED` — `cannot import name 'run_cnpj_verify'`

**Step 3: Implementar `worker/steps/cnpj_verify.py`**

```python
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta

from sqlalchemy import text
from sqlalchemy.orm import Session

from shared.brasilapi import fetch_cnpj, compare_fields
from shared.config import get_settings
from shared.models import CnpjRfCache, CnpjDivergencia
from worker.steps.checkpoint import begin_step, is_step_done, mark_step_done

logger = logging.getLogger(__name__)

# Campos da BrasilAPI -> colunas rf_* em final_visao_cliente
RF_COLUMN_MAP = {
    "razao_social":      "rf_razao_social",
    "natureza_juridica": "rf_natureza_juridica",
    "capital_social":    "rf_capital_social",
    "porte":             "rf_porte_empresa",
    "nome_fantasia":     "rf_nome_fantasia",
    "situacao_cadastral":"rf_situacao_cadastral",
    "data_inicio_ativ":  "rf_data_inicio_ativ",
    "cnae_fiscal":       "rf_cnae_principal",
    "uf":                "rf_uf",
    "municipio":         "rf_municipio",
    "email":             "rf_email",
}

SLEEP_BETWEEN_REQUESTS = 0.35  # ~3 req/s


def _is_stale(last_checked_at: datetime | None, ttl_days: int) -> bool:
    if last_checked_at is None:
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    return last_checked_at < cutoff


def _is_valid_cnpj(cnpj) -> bool:
    if not cnpj:
        return False
    digits = str(cnpj).strip()
    return len(digits) == 14 and digits.isdigit()


def _build_divergencias(
    job_id: str, cnpj: str, divergencias_raw: list[dict]
) -> list[CnpjDivergencia]:
    return [
        CnpjDivergencia(
            id=str(uuid.uuid4()),
            job_id=job_id,
            cnpj=cnpj,
            campo=d["campo"],
            valor_c6=d.get("valor_c6"),
            valor_rf=d.get("valor_rf"),
        )
        for d in divergencias_raw
    ]


def _get_cnpjs_for_job(session: Session, job_id: str) -> list[str]:
    """Retorna CNPJs distintos e validos do staging para o job atual."""
    rows = session.execute(
        text(
            "SELECT DISTINCT cd_cpf_cnpj_cliente "
            "FROM staging_visao_cliente "
            "WHERE etl_job_id = :job_id "
            "  AND cd_cpf_cnpj_cliente IS NOT NULL"
        ),
        {"job_id": job_id},
    ).fetchall()
    return [r[0] for r in rows if _is_valid_cnpj(r[0])]


def _get_cache(session: Session, cnpj: str) -> CnpjRfCache | None:
    return session.query(CnpjRfCache).filter_by(cnpj=cnpj).first()


def _get_c6_row(session: Session, cnpj: str) -> dict:
    row = session.execute(
        text(
            "SELECT nome_cliente, uf, cidade, ramo_atuacao "
            "FROM final_visao_cliente "
            "WHERE cd_cpf_cnpj_cliente = :cnpj "
            "LIMIT 1"
        ),
        {"cnpj": cnpj},
    ).fetchone()
    if row is None:
        return {}
    return {
        "nome_cliente": row[0],
        "uf": row[1],
        "cidade": row[2],
        "ramo_atuacao": row[3],
    }


def _update_final_rf_columns(session: Session, cnpj: str, rf_data: dict) -> None:
    set_parts = ", ".join(
        f"{rf_col} = :{rf_col}" for rf_col in RF_COLUMN_MAP.values()
    )
    params = {"cnpj": cnpj}
    for api_field, rf_col in RF_COLUMN_MAP.items():
        params[rf_col] = rf_data.get(api_field)

    session.execute(
        text(
            f"UPDATE final_visao_cliente SET {set_parts} "
            "WHERE cd_cpf_cnpj_cliente = :cnpj"
        ),
        params,
    )


def run_cnpj_verify(session: Session, job_id: str) -> None:
    if is_step_done(session, job_id, "cnpj_verify"):
        return
    begin_step(session, job_id, "cnpj_verify")

    settings = get_settings()
    ttl_days = settings.CNPJ_CACHE_TTL_DAYS
    timeout = settings.BRASILAPI_TIMEOUT

    cnpjs = _get_cnpjs_for_job(session, job_id)
    logger.info("cnpj_verify: %d CNPJs no job %s", len(cnpjs), job_id)

    all_divergencias: list[CnpjDivergencia] = []
    checked = 0

    for cnpj in cnpjs:
        cache = _get_cache(session, cnpj)
        if not _is_stale(getattr(cache, "last_checked_at", None), ttl_days):
            continue  # cache ainda valido

        try:
            rf_data = fetch_cnpj(cnpj, timeout=timeout)
        except Exception as exc:
            logger.warning("cnpj_verify: erro ao buscar %s: %s", cnpj, exc)
            continue

        now = datetime.now(timezone.utc)

        if rf_data is None:
            # CNPJ nao encontrado — registra no cache para nao tentar de novo
            if cache is None:
                cache = CnpjRfCache(cnpj=cnpj)
                session.add(cache)
            cache.situacao_cadastral = "NAO_ENCONTRADO"
            cache.last_checked_at = now
            session.flush()
            checked += 1
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            continue

        # Salvar/atualizar cache
        if cache is None:
            cache = CnpjRfCache(cnpj=cnpj)
            session.add(cache)

        for api_field in RF_COLUMN_MAP:
            setattr(cache, api_field, rf_data.get(api_field))
        # cnae_descricao nao e coluna rf_, e apenas para comparacao
        cache.cnae_descricao = rf_data.get("cnae_descricao")
        cache.last_checked_at = now
        session.flush()

        # Atualizar colunas rf_* na tabela final
        _update_final_rf_columns(session, cnpj, rf_data)

        # Comparar com dados do C6 Bank
        c6_row = _get_c6_row(session, cnpj)
        raw_divs = compare_fields(c6_row, rf_data)
        if raw_divs:
            records = _build_divergencias(job_id, cnpj, raw_divs)
            all_divergencias.extend(records)
            for rec in records:
                session.add(rec)

        session.flush()
        checked += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    logger.info(
        "cnpj_verify: %d CNPJs verificados, %d divergencias encontradas",
        checked, len(all_divergencias),
    )

    if all_divergencias:
        _send_divergencia_alert(job_id, all_divergencias)

    mark_step_done(session, job_id, "cnpj_verify")


def _send_divergencia_alert(job_id: str, divergencias: list[CnpjDivergencia]) -> None:
    try:
        from shared.celery_dispatch import enqueue_task
        exemplos = [
            f"{d.cnpj}: {d.campo} (C6={d.valor_c6!r} / RF={d.valor_rf!r})"
            for d in divergencias[:5]
        ]
        enqueue_task(
            "notifier.tasks.dispatch_notification",
            kwargs={
                "event_type": "CNPJ_DIVERGENCIA",
                "severity": "WARNING",
                "message": (
                    f"{len(divergencias)} divergencia(s) CNPJ encontrada(s) no job {job_id}"
                ),
                "metadata": {
                    "job_id": job_id,
                    "total_divergencias": len(divergencias),
                    "exemplos": exemplos,
                },
            },
            queue="notification_jobs",
        )
    except Exception as exc:
        logger.warning("cnpj_verify: falha ao enviar alerta: %s", exc)
```

**Step 4: Rodar testes**

```
python -m pytest tests/unit/test_cnpj_verify.py -v
```
Esperado: todos PASSED

**Step 5: Commit**

```bash
git add worker/steps/cnpj_verify.py tests/unit/test_cnpj_verify.py
git commit -m "feat: add cnpj_verify ETL step with BrasilAPI enrichment and divergence detection"
```

---

## Task 7: Integrar `cnpj_verify` no pipeline em `worker/tasks.py`

**Files:**
- Modify: `worker/tasks.py`

**Step 1: Adicionar import no topo de `worker/tasks.py`**

Na linha que lista os imports dos steps, adicionar:

```python
from worker.steps.cnpj_verify import run_cnpj_verify
```

**Step 2: Adicionar chamada apos `run_upsert` em `worker/tasks.py`**

Localizar o bloco:
```python
            current_step = "upsert"
            run_upsert(session, job_id)

            job.status = "DONE"
```

Alterar para:
```python
            current_step = "upsert"
            run_upsert(session, job_id)

            current_step = "cnpj_verify"
            run_cnpj_verify(session, job_id)

            job.status = "DONE"
```

**Step 3: Rodar todos os testes unitarios para verificar que nada quebrou**

```
python -m pytest tests/unit/ -v
```
Esperado: todos PASSED (o `run_cnpj_verify` e chamado mas o step usa checkpoint)

**Step 4: Commit**

```bash
git add worker/tasks.py
git commit -m "feat: wire cnpj_verify step into ETL pipeline after upsert"
```

---

## Task 8: Rotas da API em `api/routes/cnpj.py`

**Files:**
- Create: `api/routes/cnpj.py`
- Modify: `api/main.py`

**Step 1: Criar `api/routes/cnpj.py`**

```python
from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import text

from shared.db import get_db_session
from shared.models import CnpjRfCache, CnpjDivergencia

router = APIRouter(prefix="/cnpj", tags=["cnpj"])


@router.get("/{cnpj}")
def get_cnpj_cache(cnpj: str):
    """Retorna os dados da Receita Federal em cache para um CNPJ."""
    with get_db_session() as session:
        cache = session.query(CnpjRfCache).filter_by(cnpj=cnpj).first()
        if cache is None:
            raise HTTPException(status_code=404, detail="CNPJ nao encontrado no cache")
        return {
            "cnpj":               cache.cnpj,
            "razao_social":       cache.razao_social,
            "nome_fantasia":      cache.nome_fantasia,
            "situacao_cadastral": cache.situacao_cadastral,
            "descricao_situacao": cache.descricao_situacao,
            "cnae_fiscal":        cache.cnae_fiscal,
            "cnae_descricao":     cache.cnae_descricao,
            "natureza_juridica":  cache.natureza_juridica,
            "capital_social":     cache.capital_social,
            "porte":              cache.porte,
            "uf":                 cache.uf,
            "municipio":          cache.municipio,
            "email":              cache.email,
            "data_inicio_ativ":   cache.data_inicio_ativ,
            "last_checked_at":    cache.last_checked_at.isoformat() if cache.last_checked_at else None,
        }


@router.get("/divergencias/list")
def list_divergencias(
    cnpj: str | None = Query(default=None),
    campo: str | None = Query(default=None),
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0),
):
    """Lista divergencias entre C6 Bank e Receita Federal."""
    with get_db_session() as session:
        query = session.query(CnpjDivergencia)
        if cnpj:
            query = query.filter(CnpjDivergencia.cnpj == cnpj)
        if campo:
            query = query.filter(CnpjDivergencia.campo == campo)
        total = query.count()
        items = query.order_by(CnpjDivergencia.found_at.desc()).offset(offset).limit(limit).all()
        return {
            "total": total,
            "items": [
                {
                    "id":       item.id,
                    "job_id":   item.job_id,
                    "cnpj":     item.cnpj,
                    "campo":    item.campo,
                    "valor_c6": item.valor_c6,
                    "valor_rf": item.valor_rf,
                    "found_at": item.found_at.isoformat() if item.found_at else None,
                }
                for item in items
            ],
        }
```

**Step 2: Registrar router em `api/main.py`**

Adicionar ao bloco de imports:
```python
from api.routes import alerts, analytics, cnpj, data, files, jobs
```

Adicionar apos as outras rotas:
```python
app.include_router(cnpj.router, prefix="/v1")
```

**Step 3: Verificar sintaxe dos dois arquivos**

```
python -c "import ast; ast.parse(open('api/routes/cnpj.py').read()); print('cnpj.py ok')"
python -c "import ast; ast.parse(open('api/main.py').read()); print('main.py ok')"
```
Esperado: ambos `ok`

**Step 4: Rodar todos os testes**

```
python -m pytest tests/unit/ -v
```
Esperado: todos PASSED

**Step 5: Commit**

```bash
git add api/routes/cnpj.py api/main.py
git commit -m "feat: add /v1/cnpj/{cnpj} and /v1/cnpj/divergencias/list API routes"
```

---

## Task 9: Verificacao final — rodar suite completa

**Step 1: Rodar todos os testes unitarios**

```
python -m pytest tests/unit/ -v
```
Esperado: todos PASSED, nenhum FAILED

**Step 2: Verificar sintaxe de todos os arquivos novos/alterados**

```
python -c "
files = [
    'shared/config.py', 'shared/models.py', 'shared/brasilapi.py',
    'worker/steps/cnpj_verify.py', 'worker/tasks.py',
    'api/routes/cnpj.py', 'api/main.py',
    'migrations/versions/20260302_000006_cnpj_rf_tables.py',
    'migrations/versions/20260302_000007_final_rf_columns.py',
]
import ast
for f in files:
    ast.parse(open(f).read())
    print(f'OK: {f}')
"
```
Esperado: todos `OK`

**Step 3: Commit final de verificacao**

```bash
git add .
git commit -m "chore: cnpj brasilapi verification feature complete"
```

---

## Resumo das rotas da API adicionadas

| Metodo | Rota | Descricao |
|--------|------|-----------|
| GET | `/v1/cnpj/{cnpj}` | Dados da RF em cache para um CNPJ |
| GET | `/v1/cnpj/divergencias/list` | Lista divergencias (filtravel por cnpj e campo) |

## Variaveis de ambiente adicionadas

```env
CNPJ_CACHE_TTL_DAYS=30     # dias para considerar o cache valido (padrao: 30)
BRASILAPI_TIMEOUT=10       # timeout em segundos para chamadas BrasilAPI (padrao: 10)
```
