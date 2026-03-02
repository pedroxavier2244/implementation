# ETL System

ETL diario com orquestracao por filas, alertas e API para consumo de dados e indicadores.

## Quick Start (Dev)

```bash
cp .env.example .env
# edite .env com valores reais

docker compose up -d --build
docker compose exec api alembic upgrade head
```

Check basico:

```bash
curl http://localhost:8000/health
```

## URLs locais

| Servico | URL |
|---|---|
| API (Swagger) | http://localhost:8000/docs |
| API OpenAPI JSON | http://localhost:8000/openapi.json |
| MinIO Console | http://localhost:9001 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

Grafana default: `admin` / `admin`.

## Integracao da API (guia rapido)

Base URL local: `http://localhost:8000`

Fluxo recomendado para integracao:

1. Chamar `POST /v1/files/sync` para baixar o arquivo mais recente.
2. Listar arquivos em `GET /v1/files` e pegar o `file_id`.
3. Processar arquivo com `POST /v1/jobs/run`.
4. Acompanhar execucao em `GET /v1/jobs` ou `GET /v1/jobs/{job_id}`.
5. Consumir dados consolidados em:
   - `GET /v1/data/visao-cliente`
   - `GET /v1/analytics/...`

Regra de consolidacao:

- A tabela final mantem 1 linha por `cd_cpf_cnpj_cliente`.
- Em conflito, vence o registro com `data_base` mais recente.
- Um arquivo com `data_base` mais antiga nao sobrescreve um registro mais novo.

## Catalogo de endpoints

### Health/Observabilidade

- `GET /health`
- `GET /ready`
- `GET /metrics`

### Arquivos (`/v1/files`)

- `GET /v1/files`
- `GET /v1/files/{file_id}`
- `POST /v1/files/upload` (`multipart/form-data`, campo `file`)
- `POST /v1/files/sync`

### Jobs (`/v1/jobs`)

- `POST /v1/jobs/run` body: `{"file_id":"<uuid>"}`
- `POST /v1/jobs/reprocess/{file_id}`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`

### Dados (`/v1/data`)

- `GET /v1/data/visao-cliente?documento=<cpf_ou_cnpj>&limit=1&offset=0`

### Analytics (`/v1/analytics`)

Cada indicador tem 2 rotas: `summary` e `details`.

- `GET /v1/analytics/contas-abertas/summary`
- `GET /v1/analytics/contas-abertas/details`
- `GET /v1/analytics/qualificacao-c6pay/summary`
- `GET /v1/analytics/qualificacao-c6pay/details`
- `GET /v1/analytics/instalacao-c6pay/summary`
- `GET /v1/analytics/instalacao-c6pay/details`
- `GET /v1/analytics/contas-qualificadas/summary`
- `GET /v1/analytics/contas-qualificadas/details`

Parametros comuns:

- `period`: `daily | weekly | monthly`
- `as_of`: data de referencia (`YYYY-MM-DD`)
- `limit` e `offset` (somente em `details`)

Exemplo:

```bash
curl "http://localhost:8000/v1/analytics/contas-abertas/summary?period=monthly&as_of=2026-02-21"
```

### Alertas (`/v1/alerts`)

- `GET /v1/alerts`
- `GET /v1/alerts/{alert_id}`

### CNPJ (`/v1/cnpj`)

- `GET /v1/cnpj/divergencias/list`
- `GET /v1/cnpj/{cnpj}`

## Contratos JSON (request/response)

### 1) `POST /v1/jobs/run`

Request JSON:

```json
{
  "file_id": "96358a3a-dcd1-4497-b874-f69bcf3b22f7"
}
```

Response 200:

```json
{
  "job_id": "8b427c66-3842-4eaa-9722-59d887c9e8c4",
  "status": "QUEUED"
}
```

Response de erro (exemplo 404):

```json
{
  "detail": "File not found"
}
```

### 2) `GET /v1/jobs/{job_id}`

Response 200 (exemplo):

```json
{
  "id": "4492ab5f-ebe4-4b8c-9386-77b32c294212",
  "status": "DONE",
  "triggered_by": "scheduler",
  "rows_total": 161657,
  "rows_ok": 161657,
  "rows_bad": 0,
  "retry_count": 0,
  "started_at": "2026-03-02T15:56:51.009266Z",
  "finished_at": "2026-03-02T16:22:50.431701Z",
  "steps": [
    {
      "step_name": "extract",
      "status": "DONE",
      "started_at": "2026-03-02T15:56:51.087469Z",
      "finished_at": "2026-03-02T15:58:08.090232Z",
      "error_message": null
    }
  ]
}
```

### 3) `GET /v1/files`

Response 200 (exemplo):

```json
{
  "items": [
    {
      "id": "96358a3a-dcd1-4497-b874-f69bcf3b22f7",
      "file_date": "2026-03-02",
      "filename": "Relatorio de Producao 19.02.26.xlsx",
      "hash_sha256": "86a398bfda92b48c0b2627bbf181770b348b8f01e0370b5ae1251a72757c90ca",
      "is_valid": true,
      "is_processed": true,
      "downloaded_at": "2026-03-02T14:06:19.887956Z"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

### 4) `POST /v1/files/upload`

Nao recebe JSON. Recebe `multipart/form-data` com campo `file`.

Response 200:

```json
{
  "id": "f5d2bb22-1b6b-4b5d-9834-7f8f3ec9f3f2",
  "file_date": "2026-03-02",
  "filename": "arquivo.xlsx",
  "hash_sha256": "....",
  "is_valid": true,
  "is_processed": false,
  "downloaded_at": "2026-03-02T20:10:00.000000Z"
}
```

### 5) `GET /v1/data/visao-cliente`

Request:

```text
/v1/data/visao-cliente?documento=7501147000104&limit=1&offset=0
```

Response 200 (exemplo):

```json
{
  "documento_consultado": "7501147000104",
  "total": 1,
  "limit": 1,
  "offset": 0,
  "items": [
    {
      "cd_cpf_cnpj_cliente": "7501147000104",
      "nome_cliente": "LUG MATERIAL DE CONSTRUCAO LTDA",
      "data_base": "2026-02-21 00:00:00"
    }
  ]
}
```

### 6) `GET /v1/analytics/.../summary`

Exemplo request:

```text
/v1/analytics/contas-abertas/summary?period=monthly&as_of=2026-02-21
```

Response 200:

```json
{
  "indicator": "contas-abertas",
  "period": "monthly",
  "as_of": "2026-02-21",
  "period_start": "2026-02-01",
  "period_end": "2026-02-28",
  "total": 5249
}
```

### 7) `GET /v1/analytics/.../details`

Exemplo request:

```text
/v1/analytics/contas-abertas/details?period=monthly&as_of=2026-02-21&limit=20&offset=0
```

Response 200:

```json
{
  "indicator": "contas-abertas",
  "period": "monthly",
  "as_of": "2026-02-21",
  "period_start": "2026-02-01",
  "period_end": "2026-02-28",
  "total": 5249,
  "limit": 20,
  "offset": 0,
  "items": []
}
```

### 8) Erros padrao da API

A maioria dos erros retorna:

```json
{
  "detail": "mensagem de erro"
}
```

Exemplos:

- `400`: parametro invalido (`documento must contain digits`)
- `404`: recurso nao encontrado (`File not found`, `Job not found`)
- `500`: erro interno

## Tunnels (acesso externo)

Iniciar:

```bash
docker start etl_tunnel etl_grafana_tunnel
```

Pegar URL publica atual:

```bash
docker logs --tail=60 etl_tunnel
docker logs --tail=60 etl_grafana_tunnel
```

Observacao: link `trycloudflare.com` muda quando o container reinicia.

## Ambiente HML (isolado)

Subir:

```bash
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml up -d --build
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml exec api alembic upgrade head
```

Parar:

```bash
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml down
```

URLs HML:

| Servico | URL |
|---|---|
| API (Swagger) | http://localhost:8100/docs |
| MinIO Console | http://localhost:9101 |
| Prometheus | http://localhost:9190 |
| Grafana | http://localhost:3100 |

## Testes

```bash
# unitarios
pytest tests/unit/ -v

# integracao (com docker ativo)
pytest tests/integration/ -v -m integration
```

## Documentacao complementar

- Guia detalhado de integracao: `docs/api-integracao.md`
