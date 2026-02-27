# Guia de Integracao da API

Este guia explica para que serve cada endpoint, como testar no Swagger e como integrar o ETL em um sistema externo.

## Base URL e Documentacao

- Base URL local: `http://localhost:8000`
- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`
- Metrics (Prometheus): `http://localhost:8000/metrics`

## Como funciona o fluxo

1. Dispara sincronizacao (`/v1/files/sync`) para baixar o arquivo mais recente do Drive.
2. O checker salva o arquivo no MinIO e registra metadados em `etl_file`.
3. O worker ETL processa o arquivo em etapas e grava no banco (`final_visao_cliente`).
4. Voce consulta o status em `/v1/jobs`.
5. Voce consulta dados da planilha por CPF/CNPJ em `/v1/data/visao-cliente`.

## Healthcheck

### `GET /health`
- Uso: verificar se a API esta no ar.
- Retorno esperado: `{"status":"ok"}`.

### `GET /ready`
- Uso: verificar se dependencias estao prontas (Postgres, Redis, MinIO).
- Retorno esperado:
  - `200` quando tudo esta pronto.
  - `503` quando algum servico esta indisponivel.

## Endpoints de Arquivos (`files`)

### `GET /v1/files`
- Uso: listar arquivos ja sincronizados/uploadados.
- Query params:
  - `limit` (padrao `20`, max `100`)
  - `offset` (padrao `0`)

Exemplo:
```http
GET /v1/files?limit=20&offset=0
```

### `GET /v1/files/{file_id}`
- Uso: consultar detalhes de um arquivo especifico.

Exemplo:
```http
GET /v1/files/8c9fc880-51f8-47ce-ad32-2139a0dc1116
```

### `POST /v1/files/upload`
- Uso: enviar arquivo manualmente para o pipeline.
- Tipo: `multipart/form-data`
- Campo: `file`

### `POST /v1/files/sync`
- Uso: buscar o arquivo mais recente na fonte configurada (`ETL_SOURCE_API_URL`, hoje Google Drive folder).
- Efeito: publica tarefa do checker na fila.
- Resposta:
```json
{
  "task_id": "05579079-3e82-4cae-8f86-e2e0cd4b1aa8",
  "status": "QUEUED"
}
```

## Endpoints de Jobs (`jobs`)

### `POST /v1/jobs/run`
- Uso: iniciar ETL para um arquivo especifico.
- Body JSON:
```json
{
  "file_id": "8c9fc880-51f8-47ce-ad32-2139a0dc1116"
}
```

### `POST /v1/jobs/reprocess/{file_id}`
- Uso: reprocessar um arquivo ja existente.

### `GET /v1/jobs`
- Uso: listar execucoes do ETL.
- Query params:
  - `status` (opcional: `QUEUED`, `RUNNING`, `RETRYING`, `DONE`, `DEAD`)
  - `limit` (padrao `20`, max `100`)
  - `offset` (padrao `0`)
- Retorna:
  - status do job
  - contagem de linhas (`rows_total`, `rows_ok`, `rows_bad`)
  - etapas (`steps`) com inicio/fim/erro

Exemplo:
```http
GET /v1/jobs?status=DONE&limit=10&offset=0
```

### `GET /v1/jobs/{job_id}`
- Uso: detalhes de uma execucao especifica.

## Endpoints de Alertas (`alerts`)

### `GET /v1/alerts`
- Uso: listar alertas gerados (falhas, warnings etc.).
- Query params:
  - `severity` (opcional: `INFO`, `WARNING`, `CRITICAL`)
  - `limit` (padrao `20`, max `100`)
  - `offset` (padrao `0`)

### `GET /v1/alerts/{alert_id}`
- Uso: consultar um alerta especifico.

## Endpoint de Consulta de Dados (`data`)

### `GET /v1/data/visao-cliente`
- Uso: consultar dados da planilha ja processada no banco por CPF/CNPJ.
- Query params:
  - `documento` (obrigatorio, com ou sem pontuacao)
  - `limit` (padrao `1`, max `500`)
  - `offset` (padrao `0`)
- Regra:
  - A API remove pontuacao e compara apenas digitos.
  - O resultado vem ordenado por `data_base` decrescente.
  - No ETL, a consolidacao final e por `cd_cpf_cnpj_cliente`, mantendo sempre o registro mais recente por `data_base`.
  - Se houver duplicidade no mesmo arquivo, o ETL usa a linha com `data_base` mais nova.
  - Se houver duplicidade com dados ja existentes no banco, o ETL so atualiza quando a `data_base` recebida e mais recente (ou igual).

Exemplo:
```http
GET /v1/data/visao-cliente?documento=75.011.470/0010-4&limit=1&offset=0
```

Exemplo de resposta:
```json
{
  "documento_consultado": "7501147000104",
  "total": 1,
  "limit": 1,
  "offset": 0,
  "items": [
    {
      "data_base": "2026-02-21 00:00:00",
      "cd_cpf_cnpj_cliente": "7501147000104",
      "nome_cliente": "LUG MATERIAL DE CONSTRUCAO LTDA"
    }
  ]
}
```

## Integracao recomendada (passo a passo)

### 1. Sincronizar o arquivo mais recente
- Chame `POST /v1/files/sync`.
- Guarde o `task_id` de resposta.

### 2. Verificar se o arquivo entrou
- Chame `GET /v1/files?limit=1`.
- Pegue o `file_id` mais recente.

### 3. Acompanhar processamento
- Chame `GET /v1/jobs?limit=1` periodicamente.
- Considere concluido quando `status = DONE`.
- Trate erro quando `status = DEAD`.

### 4. Consultar resultado por CNPJ/CPF
- Chame `GET /v1/data/visao-cliente?documento=<cpf_ou_cnpj>`.

## Teste rapido no Swagger

1. Abra `http://localhost:8000/docs`.
2. Execute `POST /v1/files/sync`.
3. Execute `GET /v1/jobs?limit=1` ate ver `DONE`.
4. Execute `GET /v1/data/visao-cliente` com um documento existente.

## Notas operacionais

- No estado atual, nao ha autenticacao na API.
- A rota raiz `GET /` nao existe; use `/docs`, `/health` ou `/ready`.
- O ETL nao altera arquivo no Drive; ele baixa, processa e grava no banco.
