# Design: Correção Completa de Bugs — Sistema ETL
**Data:** 2026-03-25
**Branch:** feature/implementation
**Abordagem:** TDD (testes primeiro, correções depois)
**Commits:** um por arquivo de produção

---

## 1. Contexto

Auditoria do sistema ETL (`c:/Users/MB NEGOCIOS/etl-system`) identificou **40+ problemas** classificados em:
- **6 críticos** — causam dados errados ou pipeline quebrado em produção
- **8 altos** — riscos de crash, race conditions, endpoints inutilizáveis
- **15+ médios/baixos** — qualidade de código, logging, cosméticos

---

## 2. Escopo

### Incluído
- Escrever testes unitários que reproduzem cada bug (vermelho)
- Corrigir todos os bugs críticos, altos e médios
- Um commit por arquivo de produção corrigido

### Excluído
- Refatoração de arquitetura
- Novos endpoints ou features
- Migrations de banco de dados (bugs são de lógica, não de schema)

---

## 3. Bugs a Corrigir

### 3.1 Críticos

| ID | Arquivo | Linha | Descrição |
|----|---------|-------|-----------|
| C1 | `worker/steps/enrich.py` | 433 | `insight_conta_global` com lógica invertida — `.where(dt_global.isna(), "Possui Conta Global.")` retorna "Possui Conta Global" para clientes SEM conta |
| C2 | `worker/steps/upsert.py` | 223 | `ON CONFLICT ... WHERE` — sintaxe inválida no PostgreSQL; a cláusula `WHERE` deve ser removida ou reescrita |
| C3 | `api/routes/data.py` | 134 | Endpoint `/historico` lê de `staging_visao_cliente` que é apagada após cada job; deve ler de `visao_cliente_change_history` |
| C4 | `worker/steps/extract.py` | 10–23 | Cache global (`_dataframe_cache`, `_workbook_cache`) sem thread-safety e sem limite, causando race condition com concurrency > 1 |
| C5 | `worker/steps/enrich.py` | 478 | Typo `thereshold_saldo_medio` — inconsistente com `REQUIRED_COLUMNS` em `visao_cliente_schema.py` (ambos têm o typo; corrigir nos dois) |
| C6 | `worker/steps/enrich.py` | 507 | `gap_domicilio` sempre retorna `None`; deve calcular `max(threshold_domicilio - faixa_domicilio, 0)` como as outras gap_* |

### 3.2 Altos

| ID | Arquivo | Linha | Descrição |
|----|---------|-------|-----------|
| A1 | `worker/tasks.py` | 105–115 | Cleanup de staging e change_history fora de transação; se DELETE falha, job já foi marcado DONE |
| A2 | `worker/tasks.py` | 39–46 | TOCTOU: verifica `already_running` sem lock atômico; dois workers podem criar jobs para o mesmo arquivo |
| A3 | `api/routes/jobs.py` | 18–27 | Mesmo TOCTOU da API: check e enqueue não são atômicos |
| A4 | `api/routes/files.py` | 59–66 | Upload sem limite de tamanho; arquivo de 10 GB pode derrubar a API |
| A5 | `docker-compose.yml` | 73 | Worker com `-c 2` mas cache global não é thread-safe; reduzir para `-c 1` ou adicionar locking |
| A6 | `shared/minio_client.py` | 34–35 | `except Exception` silencioso na criação de bucket; falhas não são logadas |
| A7 | `shared/config.py` | 48–50 | `CNPJ_API_URL`, `CNPJ_API_KEY`, `CNPJ_API_TIMEOUT` são código morto (step removido); remover |
| A8 | `api/main.py` | 41 | Health check expõe stack trace de exceção em endpoint público |

### 3.3 Médios

| ID | Arquivo | Descrição |
|----|---------|-----------|
| M1 | `worker/steps/enrich.py` | Imports de `pandas`, `numpy`, `logging` dentro de funções — mover para topo |
| M2 | `worker/steps/extract.py` | Import de `pandas` dentro de função — mover para topo |
| M3 | `worker/steps/clean.py` | Import de `pandas` dentro de função — mover para topo |
| M4 | `shared/celery_dispatch.py` | Instancia novo `Celery()` a cada chamada; deve reusar `app` de `celery_app.py` |
| M5 | `api/routes/data.py` | `OUTPUT_COLUMNS = tuple(REQUIRED_COLUMNS)` criado mas nunca usado — remover |
| M6 | `worker/steps/enrich.py` | `insight_pix` usa em-dash unicode (`\u2014`) — substituir por `--` ou manter mas documentar |
| M7 | `worker/steps/validate.py` | `col.lower()` redundante em REQUIRED_COLUMNS que já está em lowercase |
| M8 | `shared/minio_client.py` | Adicionar logging em `upload_file()` e `download_file()` |
| M9 | `worker/steps/upsert.py` | Introspection de `information_schema` a cada run; resultado pode ser cacheado em module scope |

---

## 4. Estrutura de Testes

```
tests/
├── steps/
│   ├── test_enrich.py       # 8 testes (C1, C5, C6 + smoke tests)
│   ├── test_upsert.py       # 2 testes (C2)
│   ├── test_clean.py        # 1 teste (M3 — normalização)
│   ├── test_validate.py     # 1 teste (M7)
│   └── test_extract.py      # 1 teste (C4 — cache)
├── api/
│   ├── test_data.py         # 1 teste (C3 — historico)
│   └── test_files.py        # 1 teste (A4 — upload size)
├── test_tasks.py            # 2 testes (A1, A2)
└── test_config.py           # 1 teste (A7 — código morto)
```

### Padrão por bug
1. Escrever teste que **falha** reproduzindo o bug
2. Commitar: `test(<módulo>): add failing test for <bug>`
3. Corrigir o arquivo de produção
4. Confirmar que teste **passa**
5. Commitar: `fix(<módulo>): <descrição curta>`

---

## 5. Ordem de Execução

1. `worker/steps/enrich.py` — 3 críticos + 4 médios (maior impacto)
2. `worker/steps/upsert.py` — 1 crítico (SQL inválido)
3. `api/routes/data.py` — 1 crítico (historico) + 1 médio
4. `worker/steps/extract.py` — 1 crítico (cache) + 1 médio
5. `worker/tasks.py` — 2 altos (cleanup, TOCTOU)
6. `api/routes/jobs.py` — 1 alto (TOCTOU)
7. `api/routes/files.py` — 1 alto (upload size)
8. `api/main.py` — 1 alto (health check)
9. `shared/minio_client.py` — 1 alto (logging silencioso)
10. `shared/config.py` — 1 alto (código morto)
11. `shared/celery_dispatch.py` — 1 médio
12. `worker/steps/clean.py` — 1 médio
13. `worker/steps/validate.py` — 1 médio
14. `docker-compose.yml` — 1 alto (concurrency)
15. `shared/visao_cliente_schema.py` — corrigir typo junto com enrich (se decidido)

---

## 6. Critérios de Sucesso

- [ ] Todos os testes escritos passam após as correções
- [ ] `pytest tests/` executa sem erros
- [ ] Pipeline ETL completo (extract → upsert) não lança exceção com dados de exemplo
- [ ] Endpoint `/historico` retorna dados reais após um job executado
- [ ] `docker-compose up` sobe sem erro de rede (`cnpj_net` tratado)
