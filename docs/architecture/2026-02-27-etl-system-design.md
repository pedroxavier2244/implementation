# Design: Sistema ETL Automatizado com Monitoramento e Alertas Multicanal

**Data:** 2026-02-27
**Status:** Aprovado

---

## 1. Visão Geral

Sistema automatizado que:
- Executa diariamente via scheduler (Celery Beat)
- Valida planilha recebida de API externa
- Processa dados com higienização, enriquecimento e UPSERT no banco
- Detecta ausência, defasagem ou repetição do arquivo
- Dispara alertas multicanal (Telegram, Email, alerta local)
- Permite reprocessamento manual e consulta de logs via API REST

---

## 2. Stack Tecnológica

| Componente | Tecnologia |
|---|---|
| API | FastAPI (Python 3.12) |
| Scheduler | Celery Beat |
| Checker | Task Celery `checker.run_daily` |
| Worker ETL | Celery — fila `etl_jobs` |
| Worker Notifier | Celery — fila `notification_jobs` |
| Broker | Redis 7 (somente fila — volátil por design) |
| Hash/Dedup | PostgreSQL (persistente) |
| Banco | PostgreSQL 16 |
| Storage | MinIO |
| Infra | Docker Compose v2 |
| Alerta local | `local_watcher.py` fora do Docker (flag file) |

---

## 3. Arquitetura

### Fluxo do Checker

```
Celery Beat
    └─► checker.run_daily (task)
            │
            ├─ Baixa arquivo da API externa
            ├─ Salva no MinIO
            ├─ Cria registro em etl_file
            ├─ Valida: existe? data ok? hash != anterior? schema ok?
            │       (hash comparado contra PostgreSQL — não Redis)
            │
            ├─ [VÁLIDO]   → publica job_id em fila: etl_jobs
            └─ [INVÁLIDO] → publica alerta em fila: notification_jobs
```

### Filas separadas

| Fila | Worker | Concorrência | Motivo |
|---|---|---|---|
| `etl_jobs` | worker-etl | 1–2 | ETL pesado — pode travar |
| `notification_jobs` | worker-notifier | 4–8 | Leve, crítico — nunca atrasa |

**Separação garante:** ETL travado não bloqueia alertas.

### Redis — escopo restrito

Redis é broker puro. Sem hash, sem cache de estado crítico. Tudo que precisa de persistência vai ao PostgreSQL.

---

## 4. Banco de Dados (PostgreSQL)

### Schema de controle ETL

```sql
-- Arquivo recebido + hash (unificado, sem redundância)
etl_file (
    id              UUID PRIMARY KEY,
    file_date       DATE NOT NULL,
    source_url      TEXT,
    filename        TEXT,
    hash_sha256     TEXT NOT NULL,
    minio_path      TEXT,
    downloaded_at   TIMESTAMPTZ,
    is_valid        BOOLEAN,
    is_processed    BOOLEAN DEFAULT FALSE,
    validation_error TEXT,
    UNIQUE(file_date, hash_sha256)
)

-- Execução do job ETL
etl_job_run (
    id              UUID PRIMARY KEY,
    file_id         UUID FK→etl_file,
    status          TEXT CHECK (status IN ('QUEUED','RUNNING','RETRYING','DONE','FAILED','DEAD')),
    triggered_by    TEXT CHECK (triggered_by IN ('scheduler','api','manual')),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    rows_total      INT,
    rows_ok         INT,
    rows_bad        INT,
    retry_count     INT DEFAULT 0,
    max_retries     INT DEFAULT 3,
    last_retry_at   TIMESTAMPTZ,
    error_message   TEXT
)

-- Checkpoints por etapa
etl_job_step (
    id              UUID PRIMARY KEY,
    job_id          UUID FK→etl_job_run,
    step_name       TEXT CHECK (step_name IN ('extract','validate','clean','enrich','stage','upsert')),
    status          TEXT CHECK (status IN ('RUNNING','DONE','FAILED')),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    error_message   TEXT
)

-- Linhas rejeitadas
etl_bad_rows (
    id              UUID PRIMARY KEY,
    job_id          UUID FK→etl_job_run,
    row_number      INT,
    raw_data        JSONB,
    reason          TEXT,
    created_at      TIMESTAMPTZ
)

-- Evento de alerta
alert_event (
    id              UUID PRIMARY KEY,
    dedup_key       TEXT UNIQUE NOT NULL,   -- deduplicação determinística
    event_type      TEXT CHECK (event_type IN (
                        'FILE_MISSING','FILE_STALE','HASH_REPEAT',
                        'SCHEMA_ERROR','ETL_FAILED','ETL_DEAD')),
    severity        TEXT CHECK (severity IN ('WARNING','CRITICAL')),
    message         TEXT,
    metadata        JSONB,
    created_at      TIMESTAMPTZ
)

-- Auditoria por canal (tabela filha)
alert_event_channel (
    id              UUID PRIMARY KEY,
    alert_id        UUID FK→alert_event,
    channel         TEXT CHECK (channel IN ('telegram','email','toast','flag_file')),
    status          TEXT CHECK (status IN ('SENT','FAILED','RETRYING')),
    sent_at         TIMESTAMPTZ,
    error_message   TEXT,
    retry_count     INT DEFAULT 0,
    max_retries     INT DEFAULT 3,
    last_retry_at   TIMESTAMPTZ,
    next_retry_at   TIMESTAMPTZ
)
```

### Índices estratégicos

```sql
CREATE INDEX idx_job_status     ON etl_job_run(status);
CREATE INDEX idx_job_file_id    ON etl_job_run(file_id);
CREATE INDEX idx_alert_severity ON alert_event(severity);
CREATE INDEX idx_file_date      ON etl_file(file_date);
```

### Schema de dados de negócio

```sql
-- Staging versionado por job — nunca truncado automaticamente
staging_<entidade> (
    ...,
    etl_job_id  UUID FK→etl_job_run,
    loaded_at   TIMESTAMPTZ
)

-- Tabela de produção
final_<entidade> ( ... )
```

### Estratégia UPSERT

1. Worker carrega staging com `etl_job_id` preenchido
2. `DELETE FROM staging WHERE etl_job_id = ?` antes de inserir (idempotência)
3. UPSERT: `INSERT INTO final SELECT FROM staging WHERE etl_job_id = ? ON CONFLICT DO UPDATE SET ...`
4. Staging preservado para auditoria e reprocessamento
5. Limpeza de staging antigo via job de manutenção periódico (configurável)

---

## 5. ETL Pipeline (Worker)

### Separação de responsabilidades

- **Checker:** download da API externa → salva no MinIO → cria `etl_file`
- **Worker:** lê apenas do MinIO — nunca acessa a API externa diretamente

### Etapas com checkpoints idempotentes

Antes de executar cada etapa, o worker verifica `etl_job_step` para `(job_id, step_name)`.
Se `status=DONE` → skip. Retoma sempre do último checkpoint concluído.

```
[1] EXTRACT
    └─ Guarda: já DONE? → skip
    └─ Baixa do MinIO (etl_file.minio_path)
    └─ Valida estrutura básica

[2] VALIDATE
    └─ Guarda: já DONE? → skip
    └─ Schema completo por entidade
    └─ Linhas inválidas → etl_bad_rows (ON CONFLICT DO NOTHING por row_number+job_id)
    └─ Se rows_bad / rows_total > threshold_pct (configurável por entidade) → aborta + alerta

[3] CLEAN
    └─ Guarda: já DONE? → skip
    └─ Normalização pura — idempotente por natureza

[4] ENRICH
    └─ Guarda: já DONE? → skip
    └─ Cálculo puro + joins de referência — idempotente
    └─ Opcional (pulada se sem regras)

[5] STAGE
    └─ Guarda: já DONE? → skip
    └─ DELETE FROM staging WHERE etl_job_id = ? (limpa execução parcial)
    └─ INSERT com etl_job_id dentro de transação

[6] UPSERT
    └─ Guarda: já DONE? → skip
    └─ INSERT INTO final ON CONFLICT DO UPDATE (sempre idempotente)
    └─ Commit → job.status = DONE
```

### Retry — sem efeitos duplicados

```python
@app.task(bind=True, max_retries=3, queue='etl_jobs')
def run_etl(self, job_id):
    ...
    except Exception as exc:
        mark_step_failed(job_id, current_step, error=str(exc))
        mark_job_retrying(job_id, retry_count=self.request.retries + 1)

        if self.request.retries >= self.max_retries:
            mark_job_dead(job_id)
            send_alert_once(job_id, event_type='ETL_DEAD')  # deduplicado por dedup_key
            return

        delay = 300 * (2 ** self.request.retries)  # 5min, 10min, 20min
        raise self.retry(exc=exc, countdown=delay)
```

---

## 6. API (FastAPI)

### Prefixo: `/v1/`

```
POST   /v1/files/upload               — upload manual
POST   /v1/files/sync                 — força sincronização com API externa
GET    /v1/files                      — lista arquivos (paginado)
GET    /v1/files/{id}                 — detalhe

POST   /v1/jobs/run                   — disparo manual
POST   /v1/jobs/reprocess/{file_id}   — reprocessamento
GET    /v1/jobs                       — lista jobs (filtros: status, data)
GET    /v1/jobs/{id}                  — detalhe + steps
GET    /v1/jobs/{id}/logs             — etl_job_step + etl_bad_rows
GET    /v1/jobs/{id}/bad-rows         — linhas rejeitadas (paginado)

GET    /v1/alerts                     — lista alertas
GET    /v1/alerts/{id}                — detalhe + canais

GET    /health                        — liveness (sempre 200, sem query)
GET    /ready                         — readiness (verifica DB, Redis, MinIO)
```

### Contratos-chave

```json
// POST /v1/jobs/run — request
{ "file_id": "uuid" }

// POST /v1/jobs/run — response
{ "job_id": "uuid", "status": "QUEUED" }

// GET /ready — 200 ou 503
{ "postgres": "ok", "redis": "ok", "minio": "ok", "ready": true }
```

### Decisões de design

- Sem autenticação no MVP (API Key adicionada depois via middleware)
- Paginação obrigatória em todos os endpoints de listagem
- Validação via Pydantic v2 em todos os endpoints de escrita
- API não executa trabalho longo — publica em fila Celery e retorna `job_id`
- `/health` nunca falha (liveness); `/ready` verifica dependências (readiness — pronto para K8s)

---

## 7. Notification Service

### Strategy Pattern

```python
class NotificationStrategy(ABC):
    def send(self, event: AlertEvent) -> ChannelResult: ...

# Implementações: TelegramStrategy, EmailSMTPStrategy, FlagFileStrategy
# Futuro: WhatsAppStrategy, SlackStrategy
```

### Deduplicação — `dedup_key` determinístico

```python
def build_dedup_key(event_type: str, metadata: dict) -> str:
    job_id    = metadata.get("job_id")
    file_date = metadata.get("file_date")
    schema_v  = metadata.get("schema_version", "")

    if job_id:
        return f"job:{job_id}:{event_type}"
    elif schema_v:
        return f"file_date:{file_date}:{event_type}:{schema_v}"
    else:
        return f"file_date:{file_date}:{event_type}"
```

| Evento | dedup_key |
|---|---|
| ETL_DEAD | `job:{uuid}:ETL_DEAD` |
| FILE_MISSING | `file_date:2026-02-27:FILE_MISSING` |
| FILE_STALE | `file_date:2026-02-27:FILE_STALE` |
| SCHEMA_ERROR | `file_date:2026-02-27:SCHEMA_ERROR:v2` |

### Retry por canal — backoff exponencial

- `max_retries` por canal (padrão: 3)
- Delay: `60 * (2 ** retry_count)` → 1min, 2min, 4min
- Após `max_retries`: status `FAILED` definitivo — para de tentar

### Canais no MVP

| Canal | Implementação | Onde roda |
|---|---|---|
| Telegram | `python-telegram-bot` | container `worker-notifier` |
| Email SMTP | `smtplib` nativo | container `worker-notifier` |
| Alerta local | `flag_file.py` → `local_watcher.py` | fora do Docker |

**Toast Windows em container:** não confiável (requer sessão de usuário). Estratégia: `worker-notifier` escreve arquivo flag em volume compartilhado; `local_watcher.py` monitora e dispara toast/beep fora do Docker.

---

## 8. Docker Compose

```yaml
services:
  postgres:
    image: postgres:16-alpine
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etl_user -d etl_db"]
      interval: 10s / timeout: 5s / retries: 5

  redis:
    image: redis:7-alpine
    command: >
      redis-server --save "" --appendonly no
      --maxmemory 256mb --maxmemory-policy allkeys-lru
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]

  api:
    build: ./api
    depends_on:
      postgres/redis/minio: { condition: service_healthy }

  worker-etl:
    build: ./worker
    command: celery -A worker.celery_app worker -Q etl_jobs -c 2
    depends_on: [postgres, redis, minio] (service_healthy)

  worker-notifier:
    build: ./checker          # imagem compartilhada
    command: celery -A notifier.celery_app worker -Q notification_jobs -c 4
    depends_on: [postgres, redis] (service_healthy)

  beat:
    build: ./checker          # mesma imagem, entrypoint diferente
    command: celery -A checker.celery_app beat --loglevel=info
    depends_on: [postgres, redis] (service_healthy)
    # NUNCA escalar além de 1 réplica
```

---

## 9. Estrutura do Projeto

```
etl-system/
├── api/
│   ├── Dockerfile
│   ├── main.py
│   ├── routes/         (files.py, jobs.py, alerts.py)
│   └── schemas/        (files.py, jobs.py, alerts.py)
├── worker/
│   ├── Dockerfile
│   ├── celery_app.py
│   ├── tasks.py
│   └── steps/
│       ├── extract.py, validate.py, clean.py
│       ├── enrich.py, stage.py, upsert.py
│       └── checkpoint.py    ← guarda idempotente por (job_id, step_name)
├── checker/
│   ├── Dockerfile           ← compartilhado com beat e worker-notifier
│   ├── celery_app.py
│   └── checker.py
├── notifier/
│   ├── celery_app.py
│   ├── tasks.py
│   ├── dedup.py             ← build_dedup_key()
│   └── strategies/
│       ├── base.py, telegram.py, email_smtp.py
│       └── flag_file.py
├── local_watcher/
│   └── watcher.py           ← roda FORA do Docker no Windows
├── shared/
│   ├── db.py, models.py
│   ├── minio_client.py
│   └── config.py            ← Settings via pydantic-settings
├── migrations/              ← Alembic
├── docker-compose.yml
├── .env.example
└── requirements/
    ├── base.txt, api.txt
    ├── worker.txt, notifier.txt
    └── local_watcher.txt
```

---

## 10. Variáveis de Ambiente

```env
# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_DB=etl_db
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=...

# Redis
REDIS_URL=redis://redis:6379/0

# MinIO
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=...
MINIO_SECRET_KEY=...
MINIO_BUCKET=etl-files

# Notifier
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=...
SMTP_PASSWORD=...

# ETL Config
ETL_SCHEDULE_HOUR=6
ETL_SOURCE_API_URL=...
BAD_ROW_THRESHOLD_PCT=5.0
```

---

## 11. Critérios de Sucesso (MVP)

- Scheduler dispara automaticamente todos os dias
- Checker valida planilha e alerta se ausente/defasada/repetida
- ETL executa com checkpoints idempotentes e retry com backoff
- Reprocessamento manual disponível via API
- Logs e status consultáveis via API (`/v1/jobs/{id}/logs`)
- Alertas entregues via Telegram + Email com deduplicação correta
- Alerta local via flag file monitorado pelo `local_watcher.py`

---

## 12. Fora do Escopo do MVP

- Autenticação na API
- Observabilidade (Prometheus/Grafana/Loki)
- Escalabilidade Kubernetes / KEDA
- Canal WhatsApp
- MinIO → S3 migration
- Dashboard web
