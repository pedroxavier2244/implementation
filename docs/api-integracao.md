# Guia de Integracao da API

Este guia explica para que serve cada endpoint, os parametros aceitos, e exemplos completos de request e response.

**Ultima atualizacao:** 2026-03-06

---

## Base URL e Documentacao

| Ambiente | URL |
|----------|-----|
| Producao | `http://5.189.163.33:8000` |
| Local (dev) | `http://localhost:8000` |
| Swagger UI | `<base_url>/docs` |
| OpenAPI JSON | `<base_url>/openapi.json` |

---

## Fluxo recomendado de integracao

```
1. POST /v1/files/sync          — baixar arquivo mais recente automaticamente
      OU
   POST /v1/files/upload         — enviar arquivo manualmente

2. GET  /v1/files?limit=1        — obter o file_id do arquivo recem registrado

3. POST /v1/jobs/run             — iniciar o ETL com o file_id

4. GET  /v1/jobs/{job_id}        — acompanhar ate status = "DONE" (ou "DEAD")

5. GET  /v1/data/visao-cliente?documento=<cnpj>   — consultar dados do cliente
```

---

## Health / Observabilidade

### `GET /health`

Verifica se a API esta no ar.

**Request:**
```http
GET /health
```

**Response 200:**
```json
{"status": "ok"}
```

---

### `GET /ready`

Verifica se todas as dependencias estao prontas (Postgres, Redis, MinIO).

**Request:**
```http
GET /ready
```

**Response 200:**
```json
{"status": "ready"}
```

**Response 503** (quando algum servico esta fora):
```json
{"status": "unavailable", "detail": "postgres unreachable"}
```

---

## Arquivos (`/v1/files`)

### `GET /v1/files`

Lista arquivos registrados no sistema.

**Query params:**
- `limit` — maximo de itens (padrao `20`, max `100`)
- `offset` — paginacao (padrao `0`)

**Request:**
```http
GET /v1/files?limit=10&offset=0
```

**Response 200:**
```json
{
  "items": [
    {
      "id": "a3358374-bd8a-4920-884a-ef3f1c9ccade",
      "file_date": "2026-03-04",
      "filename": "Relatorio de Producao 04.03.26.xlsx",
      "hash_sha256": "86a398bfda92b48c0b2627bbf181770b348b8f01e0370b5ae1251a72757c90ca",
      "is_valid": true,
      "is_processed": true,
      "downloaded_at": "2026-03-05T18:41:19.090746Z"
    },
    {
      "id": "570f96e5-c8de-4f23-b308-9b2192e7cdb6",
      "file_date": "2026-02-21",
      "filename": "Relatorio de Producao - 21.02.26.xlsx",
      "hash_sha256": "c3d4e5f6...",
      "is_valid": true,
      "is_processed": true,
      "downloaded_at": "2026-03-04T21:01:34.985495Z"
    }
  ],
  "total": 3,
  "limit": 10,
  "offset": 0
}
```

> `file_date` e extraido do nome do arquivo (padrao `DD.MM.AA`). Ex: `21.02.26.xlsx` -> `2026-02-21`.

---

### `GET /v1/files/{file_id}`

Detalhe de um arquivo especifico.

**Request:**
```http
GET /v1/files/a3358374-bd8a-4920-884a-ef3f1c9ccade
```

**Response 200:**
```json
{
  "id": "a3358374-bd8a-4920-884a-ef3f1c9ccade",
  "file_date": "2026-03-04",
  "filename": "Relatorio de Producao 04.03.26.xlsx",
  "hash_sha256": "86a398bfda92b48c0b2627bbf181770b348b8f01e0370b5ae1251a72757c90ca",
  "is_valid": true,
  "is_processed": true,
  "downloaded_at": "2026-03-05T18:41:19.090746Z"
}
```

**Response 404:**
```json
{"detail": "File not found"}
```

---

### `POST /v1/files/upload`

Upload manual de planilha `.xlsx`.

**Request** (`multipart/form-data`, campo `file`):
```bash
curl -X POST http://localhost:8000/v1/files/upload \
  -F "file=@Relatorio de Producao - 21.02.26.xlsx"
```

**Response 200:**
```json
{
  "id": "f5d2bb22-1b6b-4b5d-9834-7f8f3ec9f3f2",
  "file_date": "2026-02-21",
  "filename": "Relatorio de Producao - 21.02.26.xlsx",
  "hash_sha256": "a1b2c3d4e5f6...",
  "is_valid": true,
  "is_processed": false,
  "downloaded_at": "2026-03-06T10:00:00.000000Z"
}
```

---

### `POST /v1/files/sync`

Dispara o checker para baixar o arquivo mais recente da fonte configurada.

**Request:**
```http
POST /v1/files/sync
```

**Response 200:**
```json
{
  "task_id": "05579079-3e82-4cae-8f86-e2e0cd4b1aa8",
  "status": "QUEUED"
}
```

---

## Jobs (`/v1/jobs`)

### `POST /v1/jobs/run`

Inicia o pipeline ETL para um arquivo especifico.

**Request:**
```http
POST /v1/jobs/run
Content-Type: application/json
```
```json
{
  "file_id": "a3358374-bd8a-4920-884a-ef3f1c9ccade"
}
```

**Response 200:**
```json
{
  "job_id": "1084d82c-173c-4577-8a01-bf46e6700622",
  "status": "QUEUED"
}
```

**Response 404** (file_id invalido):
```json
{"detail": "File not found"}
```

---

### `POST /v1/jobs/reprocess/{file_id}`

Reprocessa um arquivo ja existente do zero (apaga steps anteriores e reinicia).

**Request:**
```http
POST /v1/jobs/reprocess/a3358374-bd8a-4920-884a-ef3f1c9ccade
```

**Response 200:**
```json
{
  "job_id": "2c3d4e5f-6789-abcd-ef01-234567890abc",
  "status": "QUEUED"
}
```

---

### `GET /v1/jobs`

Lista execucoes do ETL.

**Query params:**
- `status` (opcional): `QUEUED | RUNNING | RETRYING | DONE | DEAD`
- `limit` (padrao `20`, max `100`)
- `offset` (padrao `0`)

**Request:**
```http
GET /v1/jobs?status=DONE&limit=5&offset=0
```

**Response 200:**
```json
[
  {
    "id": "1084d82c-173c-4577-8a01-bf46e6700622",
    "status": "DONE",
    "triggered_by": "scheduler",
    "rows_total": 166863,
    "rows_ok": 166863,
    "rows_bad": 0,
    "retry_count": 0,
    "started_at": "2026-03-05T18:47:05.710251Z",
    "finished_at": "2026-03-05T19:10:23.932959Z",
    "steps": []
  }
]
```

---

### `GET /v1/jobs/{job_id}`

Detalhes completos de uma execucao, incluindo todos os steps.

**Request:**
```http
GET /v1/jobs/1084d82c-173c-4577-8a01-bf46e6700622
```

**Response 200:**
```json
{
  "id": "1084d82c-173c-4577-8a01-bf46e6700622",
  "status": "DONE",
  "triggered_by": "scheduler",
  "rows_total": 166863,
  "rows_ok": 166863,
  "rows_bad": 0,
  "retry_count": 0,
  "started_at": "2026-03-05T18:47:05.710251Z",
  "finished_at": "2026-03-05T19:10:23.932959Z",
  "steps": [
    {
      "step_name": "extract",
      "status": "DONE",
      "started_at": "2026-03-05T18:47:05.759351Z",
      "finished_at": "2026-03-05T18:48:28.759981Z",
      "error_message": null
    },
    {
      "step_name": "clean",
      "status": "DONE",
      "started_at": "2026-03-05T18:48:28.768782Z",
      "finished_at": "2026-03-05T18:48:32.375106Z",
      "error_message": null
    },
    {
      "step_name": "enrich",
      "status": "DONE",
      "started_at": "2026-03-05T18:48:32.378946Z",
      "finished_at": "2026-03-05T18:48:36.529827Z",
      "error_message": null
    },
    {
      "step_name": "validate",
      "status": "DONE",
      "started_at": "2026-03-05T18:48:36.533461Z",
      "finished_at": "2026-03-05T18:48:38.147720Z",
      "error_message": null
    },
    {
      "step_name": "stage",
      "status": "DONE",
      "started_at": "2026-03-05T18:48:38.185814Z",
      "finished_at": "2026-03-05T18:59:10.322022Z",
      "error_message": null
    },
    {
      "step_name": "upsert",
      "status": "DONE",
      "started_at": "2026-03-05T18:59:10.358932Z",
      "finished_at": "2026-03-05T18:59:35.474934Z",
      "error_message": null
    },
    {
      "step_name": "analytics_snapshot",
      "status": "DONE",
      "started_at": "2026-03-05T18:59:35.479734Z",
      "finished_at": "2026-03-05T18:59:35.482990Z",
      "error_message": null
    },
    {
      "step_name": "cnpj_verify",
      "status": "DONE",
      "started_at": "2026-03-05T18:59:35.487464Z",
      "finished_at": "2026-03-05T19:10:23.910368Z",
      "error_message": null
    }
  ]
}
```

**Status possiveis do job:**

| Status | Descricao |
|--------|-----------|
| `QUEUED` | Aguardando worker disponivel |
| `RUNNING` | Sendo processado agora |
| `RETRYING` | Falhou, aguardando retry automatico (ate 3x) |
| `DONE` | Concluido com sucesso |
| `DEAD` | Falhou apos todas as 3 tentativas |

**Response 404:**
```json
{"detail": "Job not found"}
```

---

## Dados (`/v1/data`)

### `GET /v1/data/visao-cliente`

Consulta o dado mais recente de um CPF/CNPJ na base consolidada.

**Query params:**
- `documento` (obrigatorio) — CPF ou CNPJ, com ou sem pontuacao
- `limit` (padrao `1`, max `500`)
- `offset` (padrao `0`)
- `fallback_rf` (padrao `true`) — se nao achar no banco e for CNPJ, consulta Receita Federal

**Request:**
```http
GET /v1/data/visao-cliente?documento=75.011.470/0010-4&limit=1&offset=0
```

**Response 200** (encontrado no banco):
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
      "data_base": "2026-03-04 00:00:00",
      "nivel_cartao": "Alto",
      "nivel_conta": "Medio",
      "status_qualificacao": "B - Primeira qualificacao",
      "vl_cash_in_mtd": "12000",
      "faixa_alvo": "3",
      "gap_cash_in": "18000",
      "ja_recebeu_comissao": "SIM",
      "comissao_prox_mes": "NAO",
      "rf_razao_social": "LUG MATERIAL DE CONSTRUCAO LTDA",
      "rf_situacao_cadastral": "ATIVA",
      "rf_cnae_principal": "4744001",
      "rf_uf": "SP",
      "rf_municipio": "SAO PAULO",
      "data_source": null,
      "...": "todos os 116+ campos, campos ausentes retornam null"
    }
  ]
}
```

**Response 200** (nao encontrado no banco, fallback Receita Federal):
```json
{
  "documento_consultado": "12345678000190",
  "total": 1,
  "limit": 1,
  "offset": 0,
  "items": [
    {
      "cd_cpf_cnpj_cliente": "12345678000190",
      "nome_cliente": "EMPRESA EXEMPLO LTDA",
      "data_source": "receita_federal_brasilapi",
      "rf_razao_social": "EMPRESA EXEMPLO LTDA",
      "rf_situacao_cadastral": "ATIVA",
      "rf_cnae_principal": "6201500",
      "rf_uf": "RJ",
      "rf_municipio": "RIO DE JANEIRO",
      "data_base": null,
      "nivel_cartao": null,
      "...": "campos da planilha retornam null pois nao existe no banco"
    }
  ]
}
```

**Response 200** (nao encontrado em nenhuma fonte):
```json
{
  "documento_consultado": "99999999000199",
  "total": 0,
  "limit": 1,
  "offset": 0,
  "items": []
}
```

**Response 400:**
```json
{"detail": "documento must contain digits"}
```

---

### `GET /v1/data/visao-cliente/historico`

Retorna a linha do tempo completa de snapshots de um CPF/CNPJ, mostrando o diff entre cada relatorio.

**Query params:**
- `documento` (obrigatorio) — CPF ou CNPJ, com ou sem pontuacao
- `limit` (padrao `50`, max `500`)
- `offset` (padrao `0`)

**Request:**
```http
GET /v1/data/visao-cliente/historico?documento=12345678000190&limit=50&offset=0
```

**Response 200:**
```json
{
  "documento_consultado": "12345678000190",
  "total_snapshots": 3,
  "limit": 50,
  "offset": 0,
  "snapshots": [
    {
      "data_base": "2026-02-19",
      "carregado_em": "2026-03-03T18:41:20Z",
      "etl_job_id": "23de8b71-e244-4e94-9f5c-f29cc11eeb8b",
      "campos_alterados": null,
      "dados": {
        "cd_cpf_cnpj_cliente": "12345678000190",
        "nome_cliente": "EMPRESA EXEMPLO LTDA",
        "vl_cash_in_mtd": "3000",
        "status_qualificacao": "C - Qualificacao recorrente",
        "...": "todos os campos"
      }
    },
    {
      "data_base": "2026-02-21",
      "carregado_em": "2026-03-04T21:01:35Z",
      "etl_job_id": "dbdc5e72-96e8-48f3-80dc-06d44769c1ff",
      "campos_alterados": {
        "vl_cash_in_mtd": {
          "de": "3000",
          "para": "8500"
        },
        "status_qualificacao": {
          "de": "C - Qualificacao recorrente",
          "para": "B - Primeira qualificacao"
        }
      },
      "dados": {
        "cd_cpf_cnpj_cliente": "12345678000190",
        "nome_cliente": "EMPRESA EXEMPLO LTDA",
        "vl_cash_in_mtd": "8500",
        "status_qualificacao": "B - Primeira qualificacao",
        "...": "todos os campos"
      }
    },
    {
      "data_base": "2026-03-04",
      "carregado_em": "2026-03-05T18:47:05Z",
      "etl_job_id": "1084d82c-173c-4577-8a01-bf46e6700622",
      "campos_alterados": {
        "vl_cash_in_mtd": {
          "de": "8500",
          "para": "12000"
        }
      },
      "dados": {
        "vl_cash_in_mtd": "12000",
        "...": "todos os campos"
      }
    }
  ]
}
```

**Regras do `campos_alterados`:**
- Primeira snapshot: sempre `null`
- Snapshots seguintes: apenas os campos que mudaram de valor
- Campos `null` em ambos os lados sao ignorados no diff

**Response 200** (CNPJ sem historico):
```json
{
  "documento_consultado": "99999999000199",
  "total_snapshots": 0,
  "limit": 50,
  "offset": 0,
  "snapshots": []
}
```

---

## Analytics (`/v1/analytics`)

Cada indicador tem dois endpoints: `summary` (total agregado) e `details` (registros com paginacao).

**Query params comuns:**
- `period`: `daily | weekly | monthly`
- `as_of`: data de referencia no formato `YYYY-MM-DD` (padrao: hoje)
- `limit` e `offset` (somente em `details`, padrao `20`)

### `GET /v1/analytics/{indicador}/summary`

Indicadores disponiveis: `contas-abertas`, `contas-qualificadas`, `instalacao-c6pay`, `qualificacao-c6pay`

**Request:**
```http
GET /v1/analytics/contas-abertas/summary?period=monthly&as_of=2026-02-21
```

**Response 200:**
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

---

### `GET /v1/analytics/{indicador}/details`

**Request:**
```http
GET /v1/analytics/contas-abertas/details?period=monthly&as_of=2026-02-21&limit=20&offset=0
```

**Response 200:**
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
  "items": [
    {
      "indicator": "contas-abertas",
      "reference_date": "2026-02-21",
      "total": 3107,
      "source_sheet": "Abertura",
      "source_column": "Total de Contas Abertas",
      "file_id": "570f96e5-c8de-4f23-b308-9b2192e7cdb6",
      "job_id": "dbdc5e72-96e8-48f3-80dc-06d44769c1ff",
      "loaded_at": "2026-03-04T21:16:11.008086Z"
    },
    {
      "indicator": "contas-abertas",
      "reference_date": "2026-02-19",
      "total": 2142,
      "source_sheet": "Abertura",
      "source_column": "Total de Contas Abertas",
      "file_id": "3e96f10c-be7a-4b5a-91c6-40a34d8a9f8a",
      "job_id": "07d23bd9-9f65-48ac-8872-e7a8785ef760",
      "loaded_at": "2026-03-06T10:00:00.000000Z"
    }
  ]
}
```

**Logica de calculo por indicador:**

| Indicador | Fonte | Formula |
|-----------|-------|---------|
| `contas-abertas` | Aba "Abertura" do xlsx | `SUM(Total de Contas Abertas)` |
| `contas-qualificadas` | Aba "Abertura" do xlsx | `SUM(Contas Qualificadas)` |
| `instalacao-c6pay` | Aba "Relacionamento" do xlsx | `SUM(Maquinas Vendidas Relacionamento)` |
| `qualificacao-c6pay` | Tabela `final_visao_cliente` | COUNT com filtros abaixo |

**Filtros de `qualificacao-c6pay`:**
- `TIPO_PESSOA = PJ` e `STATUS_CC = LIBERADA`
- `DT_CANCELAMENTO_MAQ` vazio
- `TPV_M0 >= 5000`, `TPV_M1 < 5000`, `TPV_M2 < 5000`
- `DT_INSTALL_MAQ >= as_of - 90 dias`

> **Importante:** para `contas-abertas`, `contas-qualificadas` e `instalacao-c6pay`, o `as_of`
> deve corresponder a data real do relatorio (ex: `2026-02-21` para `21.02.26.xlsx`).

---

## CNPJ / Receita Federal (`/v1/cnpj`)

### `GET /v1/cnpj/{cnpj}`

Consulta dados da Receita Federal por CNPJ, via cache local ou BrasilAPI em tempo real.

**Query params:**
- `fallback_live` (padrao `true`) — se nao houver cache, consulta BrasilAPI

**Request:**
```http
GET /v1/cnpj/12345678000190?fallback_live=true
```

**Response 200:**
```json
{
  "data_source": "cache",
  "cnpj": "12345678000190",
  "razao_social": "EMPRESA EXEMPLO LTDA",
  "nome_fantasia": "EXEMPLO",
  "situacao_cadastral": "ATIVA",
  "descricao_situacao": "Empresa em atividade normal",
  "cnae_fiscal": "6201500",
  "cnae_descricao": "Desenvolvimento de programas de computador sob encomenda",
  "natureza_juridica": "206-2",
  "capital_social": "100000.00",
  "porte": "MICRO EMPRESA",
  "uf": "SP",
  "municipio": "SAO PAULO",
  "email": "contato@empresa.com.br",
  "data_inicio_ativ": "2015-03-10",
  "last_checked_at": "2026-03-05T19:10:23.910368Z"
}
```

**`data_source` possiveis:**
- `"cache"` — retornado do cache local (verificado nos ultimos 30 dias)
- `"receita_federal_brasilapi"` — consultado em tempo real na BrasilAPI agora

**Response 400:**
```json
{"detail": "cnpj must have 14 digits"}
```

**Response 404:**
```json
{"detail": "CNPJ nao encontrado no cache e na BrasilAPI"}
```

---

### `GET /v1/cnpj/divergencias/list`

Lista CNPJs onde os dados do C6 Bank divergem dos dados da Receita Federal, detectados automaticamente no step `cnpj_verify`.

**Query params:**
- `cnpj` (opcional) — filtrar por CNPJ especifico
- `campo` (opcional) — filtrar por campo especifico (ex: `razao_social`, `situacao_cadastral`)
- `limit` (padrao `50`, max `500`)
- `offset` (padrao `0`)

**Request:**
```http
GET /v1/cnpj/divergencias/list?limit=10&offset=0
```

**Response 200:**
```json
{
  "total": 2,
  "items": [
    {
      "id": "div-uuid-1",
      "job_id": "1084d82c-173c-4577-8a01-bf46e6700622",
      "cnpj": "12345678000190",
      "campo": "razao_social",
      "valor_c6": "EMPRESA EXEMPLO",
      "valor_rf": "EMPRESA EXEMPLO LTDA",
      "found_at": "2026-03-05T19:10:23.910368Z"
    },
    {
      "id": "div-uuid-2",
      "job_id": "1084d82c-173c-4577-8a01-bf46e6700622",
      "cnpj": "98765432000100",
      "campo": "situacao_cadastral",
      "valor_c6": "ATIVA",
      "valor_rf": "BAIXADA",
      "found_at": "2026-03-05T19:08:11.123456Z"
    }
  ]
}
```

**Request filtrado por CNPJ:**
```http
GET /v1/cnpj/divergencias/list?cnpj=12345678000190
```

---

## Alertas (`/v1/alerts`)

### `GET /v1/alerts`

Lista alertas gerados pelo sistema.

**Query params:**
- `severity` (opcional): `INFO | WARNING | CRITICAL`
- `limit` (padrao `20`, max `100`)
- `offset` (padrao `0`)

**Request:**
```http
GET /v1/alerts?severity=CRITICAL&limit=5&offset=0
```

**Response 200:**
```json
[
  {
    "id": "alert-uuid-1",
    "event_type": "ETL_FAILED",
    "severity": "CRITICAL",
    "message": "Job falhou apos 3 tentativas: QueuePool limit overflow",
    "created_at": "2026-03-03T19:31:23.080820Z",
    "channels": [
      {
        "channel": "telegram",
        "status": "SENT",
        "sent_at": "2026-03-03T19:31:25.000000Z",
        "retry_count": 0,
        "error_message": null
      },
      {
        "channel": "email",
        "status": "FAILED",
        "sent_at": null,
        "retry_count": 3,
        "error_message": "SMTP connection refused"
      }
    ]
  }
]
```

---

### `GET /v1/alerts/{alert_id}`

Detalhe de um alerta especifico.

**Request:**
```http
GET /v1/alerts/alert-uuid-1
```

**Response 200:**
```json
{
  "id": "alert-uuid-1",
  "event_type": "ETL_FAILED",
  "severity": "CRITICAL",
  "message": "Job falhou apos 3 tentativas: QueuePool limit overflow",
  "created_at": "2026-03-03T19:31:23.080820Z",
  "channels": [
    {
      "channel": "telegram",
      "status": "SENT",
      "sent_at": "2026-03-03T19:31:25.000000Z",
      "retry_count": 0,
      "error_message": null
    }
  ]
}
```

**Response 404:**
```json
{"detail": "Alert not found"}
```

---

## Erros padrao

Todos os erros retornam o mesmo formato:

```json
{"detail": "mensagem descrevendo o erro"}
```

| Codigo HTTP | Situacao tipica |
|-------------|-----------------|
| `400` | Parametro invalido (ex: `documento must contain digits`, `cnpj must have 14 digits`) |
| `404` | Recurso nao encontrado (ex: `File not found`, `Job not found`, `Alert not found`) |
| `500` | Erro interno do servidor |

---

## Consultando varios CNPJs em paralelo (frontend)

A API aceita um CNPJ por vez. Para consultar varios simultaneamente no frontend:

```javascript
const cnpjs = ["12345678000190", "98765432000100", "11222333000181"];

const resultados = await Promise.all(
  cnpjs.map(cnpj =>
    fetch(`/v1/data/visao-cliente?documento=${cnpj}`).then(r => r.json())
  )
);

// resultados[0] -> dados do primeiro CNPJ
// resultados[1] -> dados do segundo CNPJ
// resultados[2] -> dados do terceiro CNPJ
```

> Para listas grandes (>20 CNPJs), faca em lotes de 10 para nao sobrecarregar o servidor:
> ```javascript
> for (let i = 0; i < cnpjs.length; i += 10) {
>   const lote = cnpjs.slice(i, i + 10);
>   const resultados = await Promise.all(lote.map(...));
> }
> ```

---

## Notas operacionais

- **Sem autenticacao** no estado atual — proteger com firewall ou proxy reverso em producao
- A rota raiz `GET /` nao existe; use `/docs`, `/health` ou `/ready`
- O Swagger em `/docs` permite testar todos os endpoints interativamente sem precisar de curl
- O ETL nao altera o arquivo original na fonte; apenas le, processa e grava no banco
