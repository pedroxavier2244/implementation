# Fluxo ETL — Visao Cliente

Documentacao do fluxo completo desde a entrada do arquivo ate o dado disponivel na API.

**Ultima atualizacao:** 2026-03-09

---

## Visao Geral

```
Upload .xlsx ──► MinIO ──► Worker ETL ──► PostgreSQL ──► API
(manual via          (7 steps               (consulta
 /v1/files/upload)   sequenciais)            por CNPJ/CPF)
```

---

## 1. Entrada do Arquivo

O sistema recebe o arquivo via upload manual:

```http
POST /v1/files/upload
Content-Type: multipart/form-data
campo: file (arquivo .xlsx)
```

> **Regra do `file_date`:** o sistema extrai a data do nome do arquivo usando o padrao `DD.MM.AA`.
> Exemplo: `Relatorio de Producao - 21.02.26.xlsx` -> `file_date = 2026-02-21`.
> Se o nome nao contiver data, usa a data de upload.

---

## 2. Estrutura do Arquivo Esperado

O arquivo `.xlsx` deve conter a aba **Visao Cliente** com os campos de todos os clientes.

---

## 3. Pipeline ETL — 7 Steps Sequenciais

O worker processa o arquivo em **7 steps sequenciais**. Cada step e idempotente — se o job reiniciar, steps ja concluidos sao pulados (checkpoint por `etl_job_step`).

```
EXTRACT ──► CLEAN ──► ENRICH ──► VALIDATE ──► STAGE ──► UPSERT ──► CNPJ_VERIFY
```

### Step 1 — EXTRACT

- Baixa o arquivo `.xlsx` do MinIO
- Carrega a aba "Visao Cliente" em memoria

### Step 2 — CLEAN

- Normaliza encoding (remove caracteres especiais corrompidos)
- Remove linhas completamente vazias
- Padroniza nomes de colunas para lowercase com underscore

### Step 3 — ENRICH

- Aplica as **25 colunas calculadas** com base nas regras de negocio C6 Bank
- Exemplos de colunas calculadas:

| Coluna | Logica |
|--------|--------|
| `nivel_cartao` | Sem Cartao / Baixo / Medio / Alto |
| `nivel_conta` | Sem Conta / Baixo / Medio / Alto |
| `faixa_alvo` | Faixa atual + 1, ou "MAX" se ja no topo |
| `status_qualificacao` | A/B/C/D/E com base em comissoes e faixas |
| `ja_recebeu_comissao` | SIM / Nao (baseado em `ja_pago_comiss`) |
| `comissao_prox_mes` | SIM / NAO (baseado em `previsao_comiss`) |
| `gap_cash_in` | Quanto falta para meta de cash in |
| `dias_desde_abertura` | DATA_BASE - DT_CONTA_CRIADA |

### Step 4 — VALIDATE

- Conta linhas invalidas (sem CPF/CNPJ, campos obrigatorios vazios, valores de nivel invalidos)
- Grava detalhes das linhas invalidas em `etl_bad_rows`
- Se mais de **5%** das linhas forem invalidas, aborta o job inteiro
- Grava `rows_total`, `rows_ok`, `rows_bad` no job

### Step 5 — STAGE

- Insere todas as linhas validas na tabela `staging_visao_cliente`
- **A staging NAO e limpa entre jobs** — ela acumula o historico completo de todos os jobs
- Cada linha e marcada com `etl_job_id` e `loaded_at`
- Essa tabela e a fonte do endpoint `/v1/data/visao-cliente/historico`

### Step 6 — UPSERT

- Faz merge de `staging_visao_cliente` em `final_visao_cliente`
- Regra de conflito: **vence o registro com `data_base` mais recente**
- Garante **1 linha por `cd_cpf_cnpj_cliente`** na tabela final
- Arquivo mais antigo nunca sobrescreve dado mais novo

### Step 7 — CNPJ VERIFY

- Seleciona CNPJs do job atual que nao foram verificados nos ultimos 30 dias (TTL)
- Processa ate **300 CNPJs por execucao** na BrasilAPI (~0.35s por CNPJ)
- Atualiza colunas `rf_*` em `final_visao_cliente` com dados da Receita Federal
- Registra divergencias entre dados C6 Bank e Receita Federal em `cnpj_divergencia`

---

## 4. Banco de Dados (PostgreSQL)

### Tabelas principais

| Tabela | Descricao |
|--------|-----------|
| `etl_file` | Registro de cada arquivo uploadado. `file_date` e a data do relatorio (extraida do nome) |
| `etl_job_run` | Historico de execucoes do ETL com status e contadores |
| `etl_job_step` | Detalhe de cada step por job (inicio, fim, erro, status) |
| `etl_bad_rows` | Linhas invalidas com motivo de rejeicao |
| `staging_visao_cliente` | Historico completo de todas as linhas de todos os jobs (fonte do historico de CNPJ) |
| `final_visao_cliente` | Tabela consolidada final — 1 linha por cliente, com o dado mais recente |
| `cnpj_rf_cache` | Cache de consultas BrasilAPI (TTL 30 dias) |
| `cnpj_divergencia` | Divergencias entre dados C6 Bank e Receita Federal |

### Regra de consolidacao da `final_visao_cliente`

- Chave unica: `cd_cpf_cnpj_cliente`
- Em conflito: vence `data_base` mais recente
- Arquivo antigo nunca retrocede dado mais novo

### Como consultar o historico de um CNPJ

A `staging_visao_cliente` guarda todas as snapshots de todos os jobs. Use o endpoint:

```bash
GET /v1/data/visao-cliente/historico?documento=12345678000190
```

Retorna todas as snapshots ordenadas por `data_base` ASC, com `campos_alterados` mostrando o diff entre cada uma.

---

## 5. API

Base URL producao: `http://5.189.163.33:8000`

Swagger UI: `http://5.189.163.33:8000/docs`

Documentacao completa de todos os endpoints: `docs/api-integracao.md`

### Endpoints disponiveis

```
GET  /health                                          — API no ar?
GET  /ready                                           — dependencias prontas?

GET  /v1/files                                        — arquivos registrados
GET  /v1/files/{file_id}                              — detalhe de arquivo
POST /v1/files/upload                                 — upload manual de planilha

POST /v1/jobs/run                                     — iniciar ETL
POST /v1/jobs/reprocess/{file_id}                     — reprocessar arquivo
GET  /v1/jobs                                         — historico de execucoes
GET  /v1/jobs/{job_id}                                — detalhe de execucao

GET  /v1/data/visao-cliente?documento=<cpf_cnpj>      — dado atual do cliente
GET  /v1/data/visao-cliente/historico?documento=<...> — linha do tempo do cliente

GET  /v1/cnpj/{cnpj}                                  — dados Receita Federal
GET  /v1/cnpj/divergencias/list                       — divergencias C6 vs RF
```

---

## 6. Retry e Tolerancia a Falhas

- Jobs com falha entram em `RETRYING` automaticamente
- Delays crescentes: 300s -> 600s -> 1200s
- Apos **3 retries**: status `DEAD` + log CRITICAL no worker
- Steps sao idempotentes: reinicio nao reprocessa o que ja foi feito
- Concorrencia do worker ETL: **1** worker

---

## 7. Estrutura do Projeto

```
etl-system/
  api/
    main.py                    # App FastAPI, registro de routers
    routes/
      data.py                  # GET /v1/data/visao-cliente e /historico
      files.py                 # GET|POST /v1/files
      jobs.py                  # GET|POST /v1/jobs
      cnpj.py                  # GET /v1/cnpj
    schemas/
      data.py                  # Pydantic: VisaoClienteSearchOut, SnapshotItem, VisaoClienteHistoricoOut
      files.py                 # Pydantic: FileOut, FileListOut
      jobs.py                  # Pydantic: JobOut, JobRunOut
  worker/
    tasks.py                   # Task Celery: run_etl (orquestra os steps)
    steps/
      extract.py               # Step 1: baixa xlsx do MinIO
      clean.py                 # Step 2: limpeza
      enrich.py                # Step 3: colunas calculadas (regras C6 Bank)
      validate.py              # Step 4: validacao de linhas
      stage.py                 # Step 5: insere em staging_visao_cliente
      upsert.py                # Step 6: merge em final_visao_cliente
      cnpj_verify.py           # Step 7: verifica CNPJs na BrasilAPI
  shared/
    models.py                  # Modelos SQLAlchemy (todas as tabelas)
    config.py                  # Settings (pydantic-settings, lido do .env)
    db.py                      # get_db_session() context manager
    visao_cliente_schema.py    # REQUIRED_COLUMNS, FINAL_TABLE_NAME, STAGING_TABLE_NAME
    brasilapi.py               # fetch_cnpj() — cliente da BrasilAPI
  migrations/
    versions/                  # Alembic migrations
  tests/
    unit/                      # Testes unitarios (pytest)
    integration/               # Testes de integracao (requer Docker)
  docs/
    api-integracao.md          # Guia de integracao da API
    fluxo-etl.md               # Este documento
  docker-compose.yml           # Infra (postgres, redis, minio, api, worker-etl)
  .env.example                 # Template de variaveis de ambiente
```
