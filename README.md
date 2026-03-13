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

## Integracao da API (guia rapido)

Base URL local: `http://localhost:8000`

Fluxo recomendado para integracao:

1. Listar arquivos em `GET /v1/files` e pegar o `file_id`.
2. Processar arquivo com `POST /v1/jobs/run`.
3. Acompanhar execucao em `GET /v1/jobs` ou `GET /v1/jobs/{job_id}`.
4. Consumir dados consolidados em `GET /v1/data/visao-cliente`.

Regra de consolidacao:

- A tabela final mantem 1 linha por `cd_cpf_cnpj_cliente`.
- Em conflito, vence o registro com `data_base` mais recente.
- Um arquivo com `data_base` mais antiga nao sobrescreve um registro mais novo.

## Catalogo de endpoints

### Health

- `GET /health`
- `GET /ready`

### Arquivos (`/v1/files`)

- `GET /v1/files`
- `GET /v1/files/{file_id}`
- `POST /v1/files/upload` (`multipart/form-data`, campo `file`)

### Jobs (`/v1/jobs`)

- `POST /v1/jobs/run` body: `{"file_id":"<uuid>"}`
- `POST /v1/jobs/reprocess/{file_id}`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`

### Dados (`/v1/data`)

- `GET /v1/data/visao-cliente?documento=<cpf_ou_cnpj>&limit=1&offset=0`
  - Parametro opcional: `fallback_rf=true|false` (padrao `true`).
  - Regra: se nao encontrar no banco local e o documento for CNPJ (14 digitos), consulta Receita Federal via BrasilAPI.
  - O JSON de retorno e padronizado com o mesmo conjunto de chaves (campos ausentes retornam `null`).
- `GET /v1/data/visao-cliente/historico?documento=<cpf_ou_cnpj>&limit=50&offset=0`
  - Retorna linha do tempo cronologica de snapshots do CNPJ/CPF.
  - Cada snapshot inclui `campos_alterados` com diff em relacao a anterior (null na primeira).
  - Ordenado por `data_base` ASC (mais antigo primeiro).

### CNPJ (`/v1/cnpj`)

- `GET /v1/cnpj/divergencias/list`
- `GET /v1/cnpj/{cnpj}`
  - Parametro opcional: `fallback_live=true|false` (padrao `true`).
  - Regra: se nao existir no cache local, consulta Receita Federal via BrasilAPI e grava cache.

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
  "file_date": "2026-02-21",
  "filename": "Relatorio de Producao - 21.02.26.xlsx",
  "hash_sha256": "....",
  "is_valid": true,
  "is_processed": false,
  "downloaded_at": "2026-03-06T20:10:00.000000Z"
}
```

Nota: `file_date` e extraido automaticamente do nome do arquivo (padrao `DD.MM.AA`).
`21.02.26` -> `2026-02-21`. Se o nome nao contiver data, usa a data de upload.

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

### 6) Erros padrao da API

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
docker start etl_tunnel
```

Pegar URL publica atual:

```bash
docker logs --tail=60 etl_tunnel
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

## Testes

```bash
# unitarios
pytest tests/unit/ -v

# integracao (com docker ativo)
pytest tests/integration/ -v -m integration
```

## Documentacao complementar

- Guia detalhado de integracao: `docs/api-integracao.md`
- Fluxo completo do ETL e arquitetura: `docs/fluxo-etl.md`
