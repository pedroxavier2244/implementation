# ETL System

Pipeline ETL para ingestao da planilha de visao cliente, consolidacao em PostgreSQL, enriquecimento de CNPJ e exposicao via API HTTP.

## Documentacao principal

Este repositorio agora possui dois guias formais, com focos diferentes:

- [Guia de Manutencao do ETL](docs/manutencao-etl.md)
- [Guia de Integracao do ETL](docs/guia-integracao-etl.md)

## Para quem e cada documento

- manutencao: operacao do pipeline, deploy, scheduler, workers, banco, MinIO, troubleshooting e release
- integracao: consumo da API HTTP, contratos, exemplos, jobs, consulta de dados e fallback RF

## Escopo atual do projeto

O ETL e composto por:

- API FastAPI para consulta e operacao manual
- scheduler (`checker-worker` + `beat`) para baixar o arquivo fonte e disparar jobs
- worker ETL para executar `extract -> clean -> enrich -> validate -> stage -> upsert -> cnpj_verify`
- notifier para alertas por Telegram, email e arquivo `.flag`
- `local_watcher` para notificacao local no Windows
- PostgreSQL, Redis e MinIO como base operacional

## Endpoints expostos no codigo atual

- `GET /health`
- `GET /ready`
- `GET /metrics`
- `GET /v1/files`
- `GET /v1/files/{file_id}`
- `POST /v1/files/upload`
- `POST /v1/files/sync`
- `POST /v1/jobs/run`
- `POST /v1/jobs/reprocess/{file_id}`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`
- `GET /v1/data/visao-cliente`
- `GET /v1/data/visao-cliente/historico`
- `GET /v1/cnpj/divergencias/list`
- `GET /v1/cnpj/{cnpj}`

## Fluxo resumido

1. o arquivo entra por upload manual ou sincronizacao automatica
2. o arquivo bruto e guardado no MinIO e registrado em `etl_file`
3. um job e enfileirado para o worker ETL
4. o worker processa a planilha, valida, enriquece e grava `staging_visao_cliente`
5. o upsert consolida o estado mais recente em `final_visao_cliente`
6. a API responde consultas operacionais e historico diretamente desse banco

## Ambientes e compose

- `docker-compose.yml`: ambiente local enxuto para API + worker ETL
- `docker-compose.hml.yml`: ambiente mais completo com scheduler, notifier, Prometheus e Grafana

## Links rapidos

- [Indice de documentacao](docs/README.md)
- [Fluxo detalhado legado](docs/fluxo-etl.md)
- [Guia tecnico legado de integracao](docs/api-integracao.md)
- [Exemplo de ambiente local](.env.example)
- [Exemplo de ambiente HML](.env.hml.example)
