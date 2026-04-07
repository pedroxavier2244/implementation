# ETL System

Pipeline ETL para ingestao da planilha de visao cliente e consolidacao em PostgreSQL para consumo por APIs e operacao.

> Status: current
> Last validated against code: 2026-03-26

## Entrada rapida para IA

- [AI START HERE](AI-START-HERE.md)
- [Indice de documentacao](docs/README.md)

## Documentacao principal

| Documento | Status | Uso |
| --- | --- | --- |
| `AI-START-HERE.md` | current | onboarding rapido para IA |
| `docs/guia-integracao-etl.md` | current | consumo da API e operacao manual de jobs |
| `docs/manutencao-etl.md` | current | deploy, sustentacao e troubleshooting |
| `docs/fluxo-etl.md` | legacy | contexto historico e desenho anterior |
| `docs/api-integracao.md` | legacy | contrato legado mais amplo que o wiring atual |

## Escopo atual validado no codigo

- API FastAPI com rotas de `files`, `jobs` e `data`
- health e readiness em `/health` e `/ready`
- metricas em `/metrics` quando o instrumentador esta disponivel
- worker ETL com orquestracao em `worker/tasks.py`
- contrato de colunas em `shared/visao_cliente_schema.py`
- consolidacao final em `etl.final_visao_cliente`
- historico persistido em `etl.visao_cliente_change_history`

## Pipeline validado no worker atual

```text
extract -> clean -> enrich -> validate -> stage -> upsert
```

## Observacoes importantes

- `POST /v1/files/upload` grava o arquivo e cria `etl.etl_file`, mas nao inicia o ETL sozinho.
- O drift entre docs antigas e codigo atual existe; use `AI-START-HERE.md` e `docs/README.md` antes de abrir documentos historicos.
- Se houver conflito entre docs e codigo, trate o codigo atual como fonte principal.

## Links rapidos

- [Indice de documentacao](docs/README.md)
- [Guia de Integracao do ETL](docs/guia-integracao-etl.md)
- [Guia de Manutencao do ETL](docs/manutencao-etl.md)
- [Exemplo de ambiente local](.env.example)
- [Exemplo de ambiente HML](.env.hml.example)