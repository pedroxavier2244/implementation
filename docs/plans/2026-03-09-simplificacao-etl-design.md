# Simplificação ETL — Design

**Data:** 2026-03-09

## Objetivo
Remover componentes não utilizados. O ETL serve apenas para subir planilhas no banco de dados.

## O que remover

### Steps do pipeline
- `worker/steps/analytics_snapshot.py` — remover chamada no `tasks.py`

### API endpoints
- `/v1/analytics/*` — remover `api/routes/analytics.py` e registro no `main.py`
- `/v1/alerts/*` — remover `api/routes/alerts.py` e registro no `main.py`
- `POST /v1/files/sync` — remover do `api/routes/files.py`

### Docker services (docker-compose.yml)
- `worker-notifier`
- `beat`
- `prometheus`
- `grafana`

## O que manter
- Pipeline completo: extract → clean → enrich → validate → stage → upsert → cnpj_verify
- Endpoints: `/health`, `/ready`, `POST /v1/files/upload`, `GET /v1/files`, `GET /v1/jobs`, `GET /v1/jobs/:id`, `POST /v1/jobs/run`, `GET /v1/data/visao-cliente`, `/v1/cnpj/*`
