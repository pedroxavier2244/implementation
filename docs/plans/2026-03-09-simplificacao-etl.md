# Simplificação ETL — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remover componentes não utilizados do ETL, mantendo apenas o fluxo de upload de planilha para banco de dados.

**Architecture:** Remoção de steps do pipeline (analytics_snapshot), endpoints de API (analytics, alerts, sync) e services do Docker (worker-notifier, checker-worker, beat, prometheus, grafana). Nenhuma alteração no fluxo de dados principal.

**Tech Stack:** Python, FastAPI, Celery, Docker Compose

---

### Task 1: Remover step analytics_snapshot do pipeline

**Files:**
- Modify: `worker/tasks.py:12-13,92-94`

**Step 1: Remover import e chamada do analytics_snapshot**

Em `worker/tasks.py`, remover:
- linha `from worker.steps.analytics_snapshot import run_analytics_snapshot`
- bloco:
```python
current_step = "analytics_snapshot"
run_analytics_snapshot(session, job_id, etl_file)
```

Resultado final do bloco try em `tasks.py`:
```python
current_step = "extract"
run_extract(session, job_id, etl_file)

current_step = "clean"
run_clean(session, job_id)

current_step = "enrich"
run_enrich(session, job_id)

current_step = "validate"
run_validate(session, job_id, etl_file)

current_step = "stage"
run_stage(session, job_id)

current_step = "upsert"
run_upsert(session, job_id)

current_step = "cnpj_verify"
run_cnpj_verify(session, job_id)
```

**Step 2: Verificar que o arquivo não tem mais referência ao analytics_snapshot**

```bash
grep -n "analytics_snapshot" worker/tasks.py
```
Expected: sem output

**Step 3: Commit**
```bash
git add worker/tasks.py
git commit -m "refactor: remove analytics_snapshot step from pipeline"
```

---

### Task 2: Remover endpoints de analytics e alerts da API

**Files:**
- Modify: `api/main.py:5-21`

**Step 1: Remover imports e rotas de analytics e alerts**

Em `api/main.py`, trocar:
```python
from api.routes import alerts, analytics, cnpj, data, files, jobs
...
app.include_router(alerts.router, prefix="/v1")
...
app.include_router(analytics.router, prefix="/v1")
```

Por:
```python
from api.routes import cnpj, data, files, jobs
...
```

Remover as duas linhas `app.include_router(alerts...)` e `app.include_router(analytics...)`.

**Step 2: Verificar**
```bash
grep -n "alerts\|analytics" api/main.py
```
Expected: sem output

**Step 3: Commit**
```bash
git add api/main.py
git commit -m "refactor: remove analytics and alerts routes from API"
```

---

### Task 3: Remover endpoint POST /v1/files/sync

**Files:**
- Modify: `api/routes/files.py:83-86`

**Step 1: Remover o endpoint sync**

Em `api/routes/files.py`, remover:
```python
@router.post("/sync")
def sync_file():
    task = enqueue_task("checker.checker.run_daily", queue="celery")
    return {"task_id": task.id, "status": "QUEUED"}
```

**Step 2: Verificar**
```bash
grep -n "sync" api/routes/files.py
```
Expected: sem output

**Step 3: Commit**
```bash
git add api/routes/files.py
git commit -m "refactor: remove POST /v1/files/sync endpoint"
```

---

### Task 4: Remover services do Docker Compose

**Files:**
- Modify: `docker-compose.yml`

**Step 1: Remover services worker-notifier, checker-worker, beat, prometheus, grafana**

Em `docker-compose.yml`:
- Remover bloco `worker-notifier:` (linhas 78-89)
- Remover bloco `checker-worker:` (linhas 91-100)
- Remover bloco `beat:` (linhas 102-113)
- Remover bloco `prometheus:` (linhas 115-127)
- Remover bloco `grafana:` (linhas 129-146)

Na seção `volumes:`, remover:
- `alerts_data:`
- `beat_data:`
- `prometheus_data:`
- `grafana_data:`

Também remover o volume `alerts_data` do service `worker-etl`:
```yaml
worker-etl:
    ...
    # remover estas linhas:
    volumes:
      - alerts_data:/app/alerts
```

**Step 2: Verificar sintaxe do docker-compose**
```bash
docker compose config --quiet
```
Expected: sem erros

**Step 3: Parar e remover containers obsoletos**
```bash
docker compose down worker-notifier checker-worker beat prometheus grafana 2>/dev/null || true
```

**Step 4: Subir apenas os services necessários**
```bash
docker compose up -d
```

**Step 5: Verificar que API e worker-etl estão rodando**
```bash
docker compose ps
curl -s http://localhost:8000/health
```
Expected: apenas postgres, redis, minio, api, worker-etl — health retorna `{"status":"ok"}`

**Step 6: Commit**
```bash
git add docker-compose.yml
git commit -m "refactor: remove unused docker services (notifier, checker, beat, prometheus, grafana)"
```
