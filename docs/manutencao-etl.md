# Guia de Manutencao do ETL

## 1. Objetivo

Este documento descreve como operar, publicar, diagnosticar e evoluir o `etl-system`.
O foco aqui e sustentacao tecnica e operacional. Para consumo da API HTTP, use o [Guia de Integracao do ETL](guia-integracao-etl.md).

## 2. Visao geral da arquitetura

### 2.1 Componentes principais

- API HTTP: FastAPI
- Banco principal: PostgreSQL
- Broker e backend assinc: Redis
- Storage de arquivo bruto: MinIO
- Worker ETL: Celery (`worker-etl`)
- Scheduler: Celery Beat + `checker-worker`
- Notificacao: `worker-notifier`
- Watcher local opcional: `local_watcher`

### 2.2 Papel de cada componente

`api`

- expoe health, jobs, arquivos, dados consolidados e cache CNPJ
- nao processa a planilha diretamente

`checker-worker` e `beat`

- executam a rotina agendada
- baixam o arquivo fonte
- detectam duplicidade por hash
- disparam job ETL

`worker-etl`

- executa as etapas de processamento da planilha
- escreve `staging_visao_cliente`
- consolida `final_visao_cliente`
- realiza verificacao de CNPJ e enriquecimento RF

`worker-notifier`

- processa alertas e retentativas
- envia para Telegram, email e/ou arquivo `.flag`

`local_watcher`

- roda fora do Docker, no Windows
- observa a pasta de flags e dispara notificacao local

### 2.3 Fluxo ponta a ponta

1. o arquivo e obtido via `POST /v1/files/upload` ou `POST /v1/files/sync`
2. o arquivo bruto vai para o MinIO e gera um registro em `etl_file`
3. um job e enfileirado na fila `etl_jobs`
4. o worker roda as etapas `extract`, `clean`, `enrich`, `validate`, `stage`, `upsert` e `cnpj_verify`
5. o historico fica em `staging_visao_cliente`
6. a visao consolidada mais recente por documento fica em `final_visao_cliente`
7. a API responde consultas em `/v1/data/visao-cliente` e `/v1/data/visao-cliente/historico`

### 2.4 Ownership de dados

- `staging_visao_cliente` e `final_visao_cliente` pertencem a este projeto
- `cnpj_rf_cache` e `cnpj_divergencia` tambem pertencem a este projeto
- a `integration-api` e outros consumidores devem tratar `final_visao_cliente` como fonte de leitura

## 3. Tabelas e artefatos operacionais

### 3.1 Controle de arquivo e job

- `etl_file`
- `etl_job_run`
- `etl_job_step`
- `etl_bad_rows`

### 3.2 Dados de negocio

- `staging_visao_cliente`
- `final_visao_cliente`

### 3.3 Enriquecimento de CNPJ

- `cnpj_rf_cache`
- `cnpj_divergencia`

### 3.4 Artefatos fora do banco

- bucket MinIO configurado em `MINIO_BUCKET`
- arquivos `.flag` na pasta `FLAG_FILE_DIR`
- objeto bruto do arquivo por data em caminho `YYYY/MM/DD/<nome>`

## 4. API exposta no estado atual do codigo

### 4.1 Saude e observabilidade

- `GET /health`
- `GET /ready`
- `GET /metrics`

### 4.2 Arquivos

- `GET /v1/files`
- `GET /v1/files/{file_id}`
- `POST /v1/files/upload`
- `POST /v1/files/sync`

### 4.3 Jobs

- `POST /v1/jobs/run`
- `POST /v1/jobs/reprocess/{file_id}`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`

### 4.4 Dados

- `GET /v1/data/visao-cliente`
- `GET /v1/data/visao-cliente/historico`

### 4.5 CNPJ

- `GET /v1/cnpj/divergencias/list`
- `GET /v1/cnpj/{cnpj}`

### 4.6 Observacao sobre escopo

Documentos antigos do repositorio citam analytics e alertas HTTP, mas o `api.main` atual registra apenas os modulos `files`, `jobs`, `data` e `cnpj`, alem de `health`, `ready` e `metrics`.

## 5. Variaveis de ambiente

As variaveis abaixo estao documentadas em [.env.example](../.env.example) e [.env.hml.example](../.env.hml.example).

### 5.1 PostgreSQL

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `DB_POOL_SIZE`
- `DB_MAX_OVERFLOW`
- `DB_POOL_TIMEOUT`

### 5.2 Redis

- `REDIS_URL`

### 5.3 MinIO

- `MINIO_ENDPOINT`
- `MINIO_ACCESS_KEY`
- `MINIO_SECRET_KEY`
- `MINIO_BUCKET`
- `MINIO_SECURE`

### 5.4 Scheduler e ETL

- `ETL_SOURCE_API_URL`
- `ETL_SOURCE_API_KEY`
- `ETL_SCHEDULE_HOUR`
- `ETL_SCHEDULE_MINUTE`
- `ETL_TIMEZONE`
- `BAD_ROW_THRESHOLD_PCT`
- `MAX_RETRIES`

### 5.5 Notificacao e alertas

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USER`
- `SMTP_PASSWORD`
- `FLAG_FILE_DIR`

### 5.6 Verificacao de CNPJ

- `CNPJ_CACHE_TTL_DAYS`
- `CNPJ_API_URL`
- `CNPJ_API_KEY`
- `CNPJ_API_TIMEOUT`

## 6. Compose e topologia de ambientes

### 6.1 Ambiente local (`docker-compose.yml`)

Servicos principais:

- `postgres`
- `redis`
- `minio`
- `api`
- `worker-etl`

Uso recomendado:

- desenvolvimento da API
- testes de fluxo ETL sem scheduler completo
- validacao manual por upload e `jobs/run`

### 6.2 Ambiente HML (`docker-compose.hml.yml`)

Servicos principais:

- `postgres`
- `redis`
- `minio`
- `api`
- `worker-etl`
- `worker-notifier`
- `checker-worker`
- `beat`
- `prometheus`
- `grafana`

Uso recomendado:

- homologacao mais proxima de producao
- scheduler automatico
- notificacao e observabilidade

### 6.3 Dependencias de runtime

- PostgreSQL deve estar saudavel antes da API e dos workers
- Redis e obrigatorio para filas e scheduler
- MinIO e obrigatorio para armazenamento do arquivo bruto

## 7. Deploy e bootstrap

### 7.1 Subida local

```bash
cp .env.example .env
docker compose up -d --build
docker compose exec api alembic upgrade head
```

### 7.2 Subida HML

```bash
cp .env.hml.example .env.hml
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml up -d --build
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml exec api alembic upgrade head
```

### 7.3 Validacoes minimas pos-subida

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
curl http://localhost:8000/v1/files
```

No ambiente HML:

```bash
curl http://localhost:8100/health
curl http://localhost:8100/ready
```

## 8. Operacao de rotina

### 8.1 Sincronizacao automatica

O fluxo automatico usa:

- `beat` para agendar
- `checker.checker.run_daily` para baixar o arquivo
- `worker.tasks.run_etl` para processar

### 8.2 Upload manual de arquivo

Fluxo recomendado:

1. `POST /v1/files/upload`
2. obter `file_id`
3. `POST /v1/jobs/run`
4. acompanhar em `GET /v1/jobs/{job_id}`

### 8.3 Reprocessamento

Use:

- `POST /v1/jobs/reprocess/{file_id}`

### 8.4 Verificacao operacional

Checagens uteis:

- se o arquivo entrou em `etl_file`
- se o job gerou steps em `etl_job_step`
- se o job terminou `DONE`, `RETRYING` ou `DEAD`
- se `final_visao_cliente` foi atualizada

### 8.5 Logs

Exemplos:

```bash
docker compose logs --tail=200 api
docker compose logs --tail=200 worker-etl
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml logs --tail=200 checker-worker
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml logs --tail=200 beat
docker compose -p etl-hml --env-file .env.hml -f docker-compose.hml.yml logs --tail=200 worker-notifier
```

## 9. Etapas do pipeline

### 9.1 `extract`

- le o arquivo bruto
- prepara o dataframe de origem

### 9.2 `clean`

- normaliza colunas
- prepara tipos e padroes basicos

### 9.3 `enrich`

- completa colunas derivadas
- calcula `nivel_cartao`, `nivel_conta`
- calcula `cancelamento_maq`, `elegivel_c6`, `safra_*`, `idade_safra_*`
- calcula `metrica_ativacao`, `metrica_progresso`, `metrica_urgencia`, `metrica_financeiro`, `metrica_intencao` e `score_perfil`

### 9.4 `validate`

- garante aderencia ao schema obrigatorio em `shared/visao_cliente_schema.py`
- controla linhas invalidas
- respeita `BAD_ROW_THRESHOLD_PCT`

### 9.5 `stage`

- grava snapshot do job em `staging_visao_cliente`

### 9.6 `upsert`

- promove o estado consolidado para `final_visao_cliente`
- a consolidacao e por `cd_cpf_cnpj_cliente`

### 9.7 `cnpj_verify`

- valida e enriquece colunas `rf_*`
- grava cache em `cnpj_rf_cache`
- registra divergencias em `cnpj_divergencia`

## 10. Schema da planilha e do dado consolidado

### 10.1 Fonte do contrato

O contrato de colunas obrigatorias fica em:

- `shared/visao_cliente_schema.py`

### 10.2 Regra operacional

Quando uma coluna nova da planilha precisar aparecer no output:

1. adicionar a coluna em `REQUIRED_COLUMNS`
2. ajustar `extract/clean/enrich/validate` conforme a regra
3. aplicar migration no banco
4. garantir persistencia em `staging_visao_cliente` e `final_visao_cliente`
5. validar o endpoint `GET /v1/data/visao-cliente`

## 11. Migrations

### 11.1 Ferramenta

- Alembic

### 11.2 Comandos

```bash
alembic upgrade head
alembic revision --autogenerate -m "mensagem"
```

### 11.3 Cuidados

- nao criar migration sem refletir o schema real da planilha
- confirmar se a coluna precisa existir em `staging` e `final`
- validar compatibilidade com consumidores como a `integration-api`

## 12. Notificacao e alertas

### 12.1 Canais suportados

- Telegram
- SMTP
- arquivo `.flag`

### 12.2 Dedupe

Alertas usam chave de deduplicacao para evitar spam repetido.

### 12.3 Watcher local

O `local_watcher/watcher.py` pode rodar no Windows para observar `FLAG_FILE_DIR` e gerar notificacoes locais.

## 13. Troubleshooting

### 13.1 `/ready` retorna `503`

Verifique:

- conectividade com PostgreSQL
- conectividade com Redis
- configuracao do MinIO

### 13.2 `POST /v1/files/sync` nao baixa planilha

Verifique:

- `ETL_SOURCE_API_URL`
- `ETL_SOURCE_API_KEY`
- se a origem retornou HTML em vez de planilha
- se o link do Google Drive exige confirmacao adicional

### 13.3 Arquivo foi baixado mas job nao andou

Verifique:

- fila Redis
- worker `worker-etl`
- registro em `etl_job_run`
- logs do Celery

### 13.4 Job ficou `RETRYING` ou `DEAD`

Verifique:

- step que falhou em `etl_job_step`
- `error_message` do job
- schema da planilha
- thresholds de validacao

### 13.5 `GET /v1/data/visao-cliente` voltou vazio

Verifique:

- se o documento existe em `final_visao_cliente`
- se a consolidacao ja ocorreu
- se o documento foi consultado sem mascara ou com mascara limpa
- se `fallback_rf` deveria estar habilitado para CNPJ

### 13.6 CNPJ sem enriquecimento RF

Verifique:

- `CNPJ_API_URL`
- `CNPJ_API_KEY`
- cache `cnpj_rf_cache`
- step `cnpj_verify`

### 13.7 Coluna da planilha nao aparece no endpoint

Verifique:

1. se a coluna esta em `shared/visao_cliente_schema.py`
2. se o ETL preencheu a coluna
3. se a migration criou a coluna no banco
4. se a API incluiu a coluna em `OUTPUT_COLUMNS`

## 14. Checklist de release

Antes de publicar:

1. revisar `git status`
2. validar `.env` do ambiente alvo
3. subir stack ou rebuildar os servicos afetados
4. executar `alembic upgrade head` se houver migration
5. validar `GET /health` e `GET /ready`
6. validar upload ou sync de um arquivo
7. validar `GET /v1/jobs/{job_id}`
8. validar `GET /v1/data/visao-cliente?documento=<doc>`
9. revisar logs de `api`, `worker-etl`, `checker-worker` e `beat`

## 15. Melhorias futuras recomendadas

- expor no codigo atual um catalogo HTTP consistente para analytics, se isso continuar sendo requisito
- padronizar documentacao historica do repositorio com o escopo real do `api.main`
- normalizar valores literais `"nan"` para `null` antes da exposicao
- registrar de forma formal as tabelas e canais usados pelo notifier
