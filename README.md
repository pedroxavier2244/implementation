# ETL System

Automated ETL with daily scheduling, multichannel alerts, and an idempotent pipeline.

## Quick Start

```bash
cp .env.example .env
# Edit .env with real values
docker compose build
docker compose up -d
docker compose exec api alembic upgrade head
```

## Services

| Service | URL |
|---|---|
| API | http://localhost:8000/docs |
| MinIO Console | http://localhost:9001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## API Reference

Swagger UI is available at `http://localhost:8000/docs`.
Integration guide: `docs/api-integracao.md`.

Data freshness rule:
- The ETL keeps one row per `cd_cpf_cnpj_cliente` in `final_visao_cliente`.
- For duplicates, the row with latest `data_base` wins.
- Older `data_base` values do not overwrite newer rows already saved.

Swagger by environment:

- Production/Local: `http://localhost:8000/docs`
- Staging/HML: `http://localhost:8100/docs`

Grafana default login: `admin` / `admin`.
Operational dashboard: `http://localhost:3000/d/etl-control-center/etl-control-center`.

## HML/Test Environment

Run an isolated test stack in parallel with production/local:

```bash
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml up -d --build
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml exec api alembic upgrade head
```

Stop HML stack:

```bash
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml down
```

HML URLs:

| Service | URL |
|---|---|
| API | http://localhost:8100/docs |
| MinIO Console | http://localhost:9101 |
| Prometheus | http://localhost:9190 |
| Grafana | http://localhost:3100 |

## Local Watcher (Windows)

```bash
pip install -r requirements/local_watcher.txt
python -m local_watcher.watcher --dir C:/path/to/alerts/volume
```

Map `alerts_data` volume to a Windows path in `docker-compose.yml`.

## Entity Configuration

Adjust these files for your actual business entity:

- `worker/steps/validate.py`: `REQUIRED_COLUMNS`
- `worker/steps/stage.py`: `STAGING_TABLE`
- `worker/steps/upsert.py`: `FINAL_TABLE`, `CONFLICT_KEY`
- create staging/final table migrations via Alembic

## Tests

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (docker compose up)
pytest tests/integration/ -v -m integration
```
