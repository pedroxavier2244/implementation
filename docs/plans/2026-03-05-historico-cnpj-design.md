# Design: Histórico Cronológico de CNPJ

**Data:** 2026-03-05

## Contexto

O sistema ETL processa arquivos C6 Bank periodicamente. Cada arquivo tem uma `data_base` (data de referência do relatório). A tabela `final_visao_cliente` mantém apenas 1 linha por CNPJ (a mais recente), mas a tabela `staging_visao_cliente` já guarda todas as linhas de todos os jobs históricos com `etl_job_id` e `loaded_at`.

## Problema

Não há forma de ver a evolução de um CNPJ ao longo do tempo — quais campos mudaram entre um relatório e o próximo.

## Solução

### Armazenamento

Nenhuma mudança no banco. A `staging_visao_cliente` já contém o histórico completo:
- Uma linha por CNPJ por job ETL
- Campo `data_base`: data de referência do relatório C6 Bank
- Campo `loaded_at`: quando foi carregado no sistema
- Campo `etl_job_id`: qual job gerou a linha

### Novo endpoint

```
GET /v1/data/visao-cliente/historico?documento=<cnpj>&limit=50&offset=0
```

**Parâmetros:**
- `documento` — CNPJ ou CPF (obrigatório)
- `limit` — máximo de snapshots (padrão 50)
- `offset` — paginação

**Response:**
```json
{
  "documento_consultado": "12345678000190",
  "total_snapshots": 2,
  "limit": 50,
  "offset": 0,
  "snapshots": [
    {
      "data_base": "2026-02-02",
      "carregado_em": "2026-03-03T10:00:00Z",
      "etl_job_id": "abc-123",
      "campos_alterados": null,
      "dados": { "...todos os 116 campos..." }
    },
    {
      "data_base": "2026-02-21",
      "carregado_em": "2026-03-01T15:00:00Z",
      "etl_job_id": "def-456",
      "campos_alterados": {
        "vl_cash_in_mtd": { "de": "3000", "para": "8500" },
        "status_qualificacao": { "de": "C - Qualificação recorrente", "para": "B - Primeira qualificação" }
      },
      "dados": { "...todos os 116 campos..." }
    }
  ]
}
```

**Lógica do diff (`campos_alterados`):**
- A primeira snapshot sempre tem `campos_alterados: null`
- Snapshots subsequentes mostram apenas os campos que mudaram em relação à snapshot anterior
- Campos com valor `null` em ambos são ignorados no diff

### Arquivos modificados

- `api/routes/data.py` — adicionar rota `GET /data/visao-cliente/historico`
- `api/schemas/data.py` — adicionar `SnapshotItem` e `VisaoClienteHistoricoOut`
- `tests/unit/test_api_historico.py` — testes do cálculo de diff e do endpoint

### Sem mudanças em

- Schema do banco (nenhuma migração)
- Worker/ETL pipeline
- Tabela `final_visao_cliente`

## Deploy

1. Implementar em `feature/implementation` (worktree)
2. Merge para `master`
3. `docker compose build api && docker compose up -d api` no diretório `etl-system`
