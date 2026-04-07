# AI START HERE

> Status: current
> Last validated against code: 2026-04-07

Use este arquivo para onboarding rapido de IA e para reduzir leitura desnecessaria.

## Leia nesta ordem

1. `README.md`
2. `docs/README.md`
3. `docs/guia-integracao-etl.md` se a tarefa for consumir a API ou operar jobs
4. `docs/manutencao-etl.md` se a tarefa for deploy, troubleshooting ou operacao
5. `api/main.py`
6. `worker/tasks.py`
7. `shared/visao_cliente_schema.py`
8. `shared/models.py`

## Verdade atual em poucas linhas

- O pipeline validado no codigo atual e `extract -> clean -> enrich -> validate -> stage -> upsert`.
- O `api.main` atual registra `files`, `jobs` e `data`, alem de `health`, `ready` e `metrics`.
- Upload manual registra o arquivo, mas nao dispara o job ETL sozinho.
- `etl.final_visao_cliente` e a visao consolidada principal.
- `etl.visao_cliente_change_history` guarda diffs persistidos por documento.
- `worker/tasks.py` e `shared/visao_cliente_schema.py` sao a fonte de verdade do comportamento atual.

## Alertas de leitura rapida

- `POST /v1/files/sync` enfileira `worker.integrations.gdrive.run_daily` (Google Drive sync).
- A integracao com Google Drive esta em `worker/integrations/gdrive.py` (era `checker/checker.py`).
- Docker Compose esta em `infra/` (nao na raiz): `infra/docker-compose.yml` e `infra/docker-compose.hml.yml`.
- Testes e `__pycache__` ainda guardam rastros de modulos antigos como `analytics`, `cnpj`, `alerts`. Confirme no source antes de confiar.

## Nao leia primeiro

- `docs/fluxo-etl.md`
- `docs/api-integracao.md`
- `docs/architecture/` (historico de planos — nao e fonte de verdade atual)
- `tests/` sem antes ler o source principal
- `migrations/`
- `__pycache__/`

## Arquivos de codigo para abrir primeiro

- `api/main.py`
- `api/routes/files.py`
- `api/routes/jobs.py`
- `api/routes/data.py`
- `worker/tasks.py`
- `worker/steps/enrich.py`
- `shared/visao_cliente_schema.py`
- `shared/models.py`
