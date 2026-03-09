# Limpeza de Código Morto — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remover todo código, arquivos e configurações que não são mais utilizados após simplificação do ETL.

**Architecture:** Remoções cirúrgicas sem afetar o pipeline ativo (extract→clean→enrich→validate→stage→upsert→cnpj_verify) nem os endpoints ativos (/v1/files, /v1/jobs, /v1/data, /v1/cnpj).

**Tech Stack:** Python, FastAPI, SQLAlchemy, Docker

---

### Task 1: Deletar pastas inteiras desnecessárias

**Files:**
- Delete: `notifier/` (pasta completa — serviço removido do Docker)
- Delete: `checker/` (pasta completa — serviço removido do Docker)
- Delete: `monitoring/` (pasta completa — Prometheus/Grafana removidos)
- Delete: `local_watcher/` (pasta completa — ferramenta dev local, não é parte do ETL)
- Delete: `.tmp_test_strategies/` (pasta temporária de testes)
- Delete: `.tmp_test_watcher/` (pasta temporária de testes)
- Delete: `docker-compose.hml.yml` (arquivo obsoleto de homologação com services mortos)

**Step 1: Deletar as pastas e arquivo**

```bash
cd "C:\Users\MB NEGOCIOS\etl-system"
git rm -rf notifier/ checker/ monitoring/ local_watcher/
rm -rf .tmp_test_strategies/ .tmp_test_watcher/
git rm docker-compose.hml.yml
```

**Step 2: Verificar que não há imports dos módulos deletados em código ativo**

```bash
grep -r "from notifier\|import notifier\|from checker\|import checker\|from local_watcher\|import local_watcher" api/ worker/ shared/ --include="*.py"
```
Expected: sem output

**Step 3: Commit**

```bash
git add -A
git commit -m "refactor: remove dead folders (notifier, checker, monitoring, local_watcher)"
```

---

### Task 2: Deletar schemas Pydantic orphans e shared/analytics_snapshot_schema.py

**Files:**
- Delete: `api/schemas/alerts.py`
- Delete: `api/schemas/analytics.py`
- Delete: `shared/analytics_snapshot_schema.py`

**Step 1: Verificar que não são importados em código ativo**

```bash
grep -r "from api.schemas.alerts\|from api.schemas.analytics\|from shared.analytics_snapshot_schema\|analytics_snapshot_schema" api/ worker/ shared/ --include="*.py"
```
Expected: sem output (confirmar antes de deletar)

**Step 2: Deletar os arquivos**

```bash
git rm api/schemas/alerts.py api/schemas/analytics.py shared/analytics_snapshot_schema.py
```

**Step 3: Verificar que api/schemas/__init__.py não referencia os deletados**

```bash
cat api/schemas/__init__.py
```

**Step 4: Commit**

```bash
git commit -m "refactor: remove orphan schemas (alerts, analytics) and analytics_snapshot_schema"
```

---

### Task 3: Limpar shared/models.py — remover modelos orphans

**Files:**
- Modify: `shared/models.py`

**Modelos a remover:**
- `AlertEvent` (tabela `alert_event`) — populado pelo notifier removido
- `AlertEventChannel` (tabela `alert_event_channel`) — idem
- `AnalyticsIndicatorSnapshot` (tabela `analytics_indicator_snapshot`) — step removido

**Indexes a remover:**
- `Index("idx_alert_severity", AlertEvent.severity)`
- `Index("idx_analytics_snapshot_reference_date", AnalyticsIndicatorSnapshot.reference_date)`
- `Index("idx_analytics_snapshot_file_id", AnalyticsIndicatorSnapshot.file_id)`

**Imports a remover se não mais usados após a limpeza:**
- `BigInteger` do import SQLAlchemy (usado apenas por AnalyticsIndicatorSnapshot)

**Step 1: Verificar que nenhum código ativo usa os modelos a remover**

```bash
grep -r "AlertEvent\|AlertEventChannel\|AnalyticsIndicatorSnapshot" api/ worker/ shared/ --include="*.py"
```
Expected: apenas referências em `shared/models.py` — se houver outras, investigar antes de remover

**Step 2: Editar shared/models.py**

Remover:
- Classe `AlertEvent` (linhas 87-98)
- Classe `AlertEventChannel` (linhas 101-115)
- Classe `AnalyticsIndicatorSnapshot` (linhas 150-160)
- Indexes nas linhas 166, 168-169
- `BigInteger` do import SQLAlchemy se não mais usado

**Step 3: Verificar sintaxe**

```bash
python -c "from shared.models import EtlFile, EtlJobRun, EtlJobStep, EtlBadRow, CnpjRfCache, CnpjDivergencia; print('OK')"
```
Expected: `OK`

**Step 4: Commit**

```bash
git add shared/models.py
git commit -m "refactor: remove orphan models (AlertEvent, AlertEventChannel, AnalyticsIndicatorSnapshot)"
```

---

### Task 4: Limpar shared/config.py — remover variáveis orphans

**Files:**
- Modify: `shared/config.py`

**Variáveis a remover:**

Seção `# Notifier`:
```python
TELEGRAM_BOT_TOKEN: str = ""
TELEGRAM_CHAT_ID: str = ""
SMTP_HOST: str = ""
SMTP_PORT: int = 587
SMTP_USER: str = ""
SMTP_PASSWORD: str = ""
```

Seção `# ETL Config` (remover apenas as orphans, manter BAD_ROW_THRESHOLD_PCT e MAX_RETRIES):
```python
ETL_SCHEDULE_HOUR: int = 18
ETL_SCHEDULE_MINUTE: int = 0
ETL_TIMEZONE: str = "America/Sao_Paulo"
ETL_SOURCE_API_URL: str = "https://example.com/api/file"
ETL_SOURCE_API_KEY: str = ""
```

Seção `# Alerts`:
```python
FLAG_FILE_DIR: str = "/app/alerts"
```

**Step 1: Verificar que nenhum código ativo usa essas variáveis**

```bash
grep -r "TELEGRAM\|SMTP_HOST\|SMTP_PORT\|SMTP_USER\|SMTP_PASSWORD\|ETL_SCHEDULE\|ETL_TIMEZONE\|ETL_SOURCE_API\|FLAG_FILE_DIR" api/ worker/ shared/ --include="*.py"
```
Expected: apenas referências em `shared/config.py`

**Step 2: Editar shared/config.py removendo as variáveis listadas acima e seus comentários de seção**

**Step 3: Verificar sintaxe**

```bash
python -c "from shared.config import get_settings; s = get_settings(); print('OK')"
```
Expected: `OK`

**Step 4: Commit**

```bash
git add shared/config.py
git commit -m "refactor: remove orphan config vars (notifier, scheduler, alerts)"
```

---

### Task 5: Deletar requirements orphans e testes para código morto

**Files:**
- Delete: `requirements/notifier.txt`
- Delete: `requirements/local_watcher.txt`
- Delete: `tests/unit/test_checker.py`
- Delete: `tests/unit/test_dedup.py`
- Delete: `tests/unit/test_notifier_task.py`
- Delete: `tests/unit/test_strategies.py`
- Delete: `tests/unit/test_watcher.py`

**Step 1: Deletar requirements orphans**

```bash
git rm requirements/notifier.txt requirements/local_watcher.txt
```

**Step 2: Deletar testes para código morto**

```bash
git rm tests/unit/test_checker.py tests/unit/test_dedup.py tests/unit/test_notifier_task.py tests/unit/test_strategies.py tests/unit/test_watcher.py
```

**Step 3: Rodar testes restantes para garantir que nada quebrou**

```bash
cd "C:\Users\MB NEGOCIOS\etl-system"
python -m pytest tests/unit/ -v --ignore=tests/unit/test_brasilapi.py 2>&1 | tail -20
```
Expected: todos passando (ou falhas pré-existentes não relacionadas à limpeza)

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor: remove orphan requirements and dead test files"
```

---

### Task 6: Atualizar README.md

**Files:**
- Modify: `README.md`

**O que remover/atualizar:**
- Remover menções a Prometheus (`:9090`) e Grafana (`:3000`)
- Remover seção de analytics endpoints (`/v1/analytics/*`)
- Remover seção de alerts endpoints (`/v1/alerts/*`)
- Remover menção ao endpoint `POST /v1/files/sync`
- Atualizar lista de endpoints para refletir apenas os ativos

**Endpoints ativos a manter no README:**
```
GET  /health
GET  /ready
GET  /v1/files
GET  /v1/files/:file_id
POST /v1/files/upload
POST /v1/jobs/run
POST /v1/jobs/reprocess/:file_id
GET  /v1/jobs
GET  /v1/jobs/:job_id
GET  /v1/data/visao-cliente
GET  /v1/data/visao-cliente/historico
GET  /v1/cnpj/:cnpj
GET  /v1/cnpj/divergencias/list
```

**Step 1: Ler README.md e identificar seções a remover**

**Step 2: Editar o README removendo as seções obsoletas**

**Step 3: Verificar que README ainda descreve corretamente o sistema**

**Step 4: Commit**

```bash
git add README.md
git commit -m "docs: remove obsolete sections (analytics, alerts, prometheus, grafana) from README"
```
