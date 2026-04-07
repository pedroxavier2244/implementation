# Design: Correção Completa de Bugs — Sistema ETL
**Data:** 2026-03-25
**Branch:** feature/implementation
**Abordagem:** TDD (testes primeiro, correções depois)
**Commits:** um por arquivo de produção

---

## 1. Contexto

Auditoria do sistema ETL (`c:/Users/MB NEGOCIOS/etl-system`) identificou problemas classificados em:
- **3 críticos** — causam dados errados ou pipeline com comportamento incorreto
- **9 altos** — riscos de crash, race conditions, endpoints inutilizáveis, dados sensíveis expostos
- **8 médios** — qualidade de código, logging, dead code

**Comparação com modelo Excel** (`relatorio enriquecido.xlsx`): 23 de 24 colunas derivadas estão corretas.
`insight_conta_global` NÃO é bug — `.where(isna, "Possui Conta Global.")` já é correto.
Único erro confirmado: `gap_domicilio` retorna `None` em vez de `MAX(threshold_domicilio - tpv_m2, 0)` (Excel usa AQ=TPV_M2 como variável de comparação).

---

## 2. Escopo

### Incluído
- Escrever testes unitários que reproduzem cada bug (vermelho) antes de corrigir
- Corrigir todos os bugs críticos, altos e médios
- Um commit de teste + um commit de correção por arquivo de produção

### Excluído
- Refatoração de arquitetura
- Novos endpoints ou features
- Migrations de banco de dados (bugs são de lógica, não de schema)
- Typo `thereshold_saldo_medio` — intencional, alinhado ao nome exato do modelo upstream (comentado em `visao_cliente_schema.py` linha 109)

---

## 3. Bugs a Corrigir

### 3.1 Críticos

| ID | Arquivo | Linha | Descrição | Fix |
|----|---------|-------|-----------|-----|
| C3 | `api/routes/data.py` | ~134 | Endpoint `/historico` lê de `staging_visao_cliente`, que é deletada após cada job (tasks.py linha 106). Resultado: endpoint sempre retorna vazio após o primeiro job completar | Migrar query para `etl.visao_cliente_change_history` com JOIN em `etl_file` |
| C4 | `worker/steps/extract.py` | 10–23 | `_dataframe_cache` e `_workbook_cache` são dicts globais sem limite de tamanho e sem thread-safety. Com concorrência > 1, dois jobs podem ler/escrever no mesmo cache simultaneamente | Adicionar `threading.Lock` para acesso ao cache; ou limitar via `functools.lru_cache(maxsize=...)` |
| C6 | `worker/steps/enrich.py` | ~507 | `gap_domicilio` é atribuído como `None` em vez de calcular `MAX(threshold_domicilio - tpv_m2, 0)`. Confirmado via Excel: a variável de comparação é `AQ=TPV_M2`, não uma coluna "domicilio" separada. | `np.maximum(target_domicilio - tpv_m2, 0)` onde `tpv_m2 = _coerce_numeric(df["tpv_m2"]).fillna(0)` |

### 3.2 Altos

| ID | Arquivo | Linha | Descrição | Fix |
|----|---------|-------|-----------|-----|
| A1 | `worker/tasks.py` | 104–118 | Cleanup de staging (linha 106) e change_history (linha 110) ocorrem em commit separado (linha 116) do upsert (linha 102). Se o DELETE falhar, o upsert já foi comittado mas staging persiste indefinidamente, causando acúmulo | Mover cleanup para o mesmo bloco transacional do upsert ou usar `try/finally` |
| A2 | `worker/tasks.py` | 39–46 | TOCTOU: verifica `already_running` sem lock — dois workers podem criar jobs para o mesmo arquivo | Usar `SELECT ... FOR UPDATE` ou unique constraint no banco |
| A3 | `api/routes/jobs.py` | 18–27 | Mesmo TOCTOU de A2 na camada API | Mesma solução |
| A3b | `api/routes/jobs.py` | 37–44 | `/reprocess` enfileira task sem verificar se já existe job ativo, ao contrário de `/run` que tem a guarda | Adicionar mesma guarda de job ativo presente em `/run` |
| A4 | `api/routes/files.py` | 59–66 | Upload sem limite de tamanho — arquivo de vários GB pode consumir toda a RAM da API | Adicionar validação de `content_length` ou limite via middleware FastAPI |
| A5 | `docker-compose.yml` | 73 | Worker configurado com `-c 2` (2 workers concorrentes). O fix de C4 adiciona locking ao cache, tornando isso seguro — mas enquanto C4 não for aplicado, concorrência > 1 causa race condition. **A5 e C4 são acoplados: fixar C4 primeiro, depois confirmar se A5 ainda é necessário** | Reduzir para `-c 1` temporariamente; reavaliar após C4 |
| A6 | `shared/minio_client.py` | 34–35 | `except Exception` silencioso em `_ensure_bucket()` — falhas de bucket não são logadas, causando erros confusos downstream | Adicionar `logger.error(...)` no except |
| A7 | `shared/config.py` | 48–50 | `CNPJ_API_URL`, `CNPJ_API_KEY`, `CNPJ_API_TIMEOUT` são dead code (step `cnpj_verify` removido) | Remover os três campos |
| A8 | `api/main.py` | 41 | Health check retorna `f"error: {exc}"` em endpoint público. Para psycopg2, `str(exc)` pode incluir a connection string com usuário e senha | Retornar mensagem genérica (`"connection failed"`) sem detalhes da exceção |

### 3.3 Médios

| ID | Arquivo | Descrição | Fix |
|----|---------|-----------|-----|
| M1 | `worker/steps/enrich.py` | Imports de `pandas`, `numpy`, `logging` dentro de funções | Mover para topo do arquivo |
| M2 | `worker/steps/extract.py` | Import de `pandas` dentro de função | Mover para topo |
| M3 | `worker/steps/clean.py` | Import de `pandas` dentro de função | Mover para topo |
| M5 | `api/routes/data.py` | `OUTPUT_COLUMNS = tuple(REQUIRED_COLUMNS)` criado em module scope mas nunca referenciado | Remover |
| M6 | `worker/steps/enrich.py` | `insight_pix` usa em-dash unicode (`\u2014`) — pode causar surpresas de encoding em logs ou comparações | Substituir por `" - "` (hífen ASCII) |
| M7 | `worker/steps/validate.py` | `col.lower()` redundante pois `REQUIRED_COLUMNS` já está em lowercase | Remover `.lower()` |
| M8 | `shared/minio_client.py` | Sem logging em `upload_file()`, `download_file()`, e `object_exists()` | Adicionar `logger.debug/error` |
| M9 | `worker/steps/upsert.py` | Consulta `information_schema` a cada execução para descobrir colunas — resultado é estático | Cachear em module scope após primeira chamada |

---

## 4. Estrutura de Testes

```
tests/
├── steps/
│   ├── test_enrich.py        # C1, C6 + smoke tests
│   ├── test_upsert.py        # M9 — instrospection
│   ├── test_clean.py         # M3 — normalização
│   ├── test_validate.py      # M7 — col.lower() redundante
│   └── test_extract.py       # C4 — cache thread-safety
├── api/
│   ├── test_data.py          # C3 — historico não lê de staging
│   └── test_files.py         # A4 — upload sem limite
└── test_tasks.py             # A1, A2 — cleanup e TOCTOU
```

### Especificação de testes por bug

#### C1 — `insight_conta_global` invertido
```python
# Input: linha com dt_conta_criada_global = NaN
# Expected: resultado é string vazia ou "Sem Conta Global."
def test_insight_conta_global_sem_conta_retorna_vazio(df_sem_conta):
    result = run_enrich(df_sem_conta)
    assert result["insight_conta_global"].iloc[0] in ("", "Sem Conta Global.")

# Input: linha com dt_conta_criada_global = "2024-01-01"
# Expected: resultado contém "Possui Conta Global."
def test_insight_conta_global_com_conta_retorna_mensagem(df_com_conta):
    result = run_enrich(df_com_conta)
    assert "Possui Conta Global." in result["insight_conta_global"].iloc[0]
```

#### C3 — Endpoint historico lê tabela errada
```python
# Fixture: staging_visao_cliente vazia, change_history com 2 registros
# Expected: endpoint retorna 2 registros (não 0)
def test_historico_le_de_change_history_nao_staging(client, db_with_history):
    response = client.get("/v1/data/visao-cliente/historico?documento=12345678000190")
    assert response.status_code == 200
    assert len(response.json()) >= 1
```

#### C4 — Cache sem thread-safety
```python
# Simula dois threads acessando o cache simultaneamente
# Expected: nenhuma exceção de concorrência (KeyError, corrupção)
def test_cache_acesso_concorrente_e_seguro():
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(set_and_get_cache, f"job-{i}") for i in range(20)]
        results = [f.result() for f in futures]
    assert all(r is not None for r in results)
```

#### C6 — `gap_domicilio` sempre None
```python
# Input: linha com threshold_domicilio=5000, faixa_domicilio=3000
# Expected: gap_domicilio = 2000
def test_gap_domicilio_retorna_diferenca(df_com_domicilio):
    result = run_enrich(df_com_domicilio)
    assert result["gap_domicilio"].iloc[0] == 2000

# Input: threshold <= faixa (meta atingida)
# Expected: gap = 0 (não negativo)
def test_gap_domicilio_nao_negativo_quando_meta_atingida(df_meta_atingida):
    result = run_enrich(df_meta_atingida)
    assert result["gap_domicilio"].iloc[0] == 0
```

#### A1 — Cleanup fora de transação
```python
# Simula falha no DELETE de staging após upsert commitado
# Expected: job NÃO marcado como DONE se cleanup falha
def test_cleanup_falho_nao_marca_job_done(mock_session_delete_raises):
    with pytest.raises(Exception):
        run_etl_pipeline(job_id="test-job", file_id="test-file")
    job = fetch_job("test-job")
    assert job.status != "DONE"
```

#### A4 — Upload sem limite
```python
# Input: arquivo com 60 MB (acima do limite)
# Expected: 413 Request Entity Too Large
def test_upload_arquivo_grande_retorna_413(client, large_file_bytes):
    response = client.post("/v1/files/upload", files={"file": large_file_bytes})
    assert response.status_code == 413
```

### Padrão por bug
1. Escrever teste que **falha** reproduzindo o bug
2. Commitar: `test(<módulo>): add failing tests for <descrição>`
3. Corrigir o arquivo de produção
4. Confirmar que teste **passa**
5. Commitar: `fix(<módulo>): <descrição curta>`

---

## 5. Ordem de Execução

1. `worker/steps/enrich.py` — C1, C6, M1, M6 (maior impacto de dados)
2. `worker/steps/extract.py` — C4, M2 (cache thread-safety — desbloqueia A5)
3. `docker-compose.yml` — A5 (após C4 fixado; avaliar se ainda necessário)
4. `api/routes/data.py` — C3, M5 (historico retornando vazio)
5. `worker/tasks.py` — A1, A2 (cleanup e TOCTOU)
6. `api/routes/jobs.py` — A3, A3b (TOCTOU e reprocess sem guarda)
7. `api/routes/files.py` — A4 (upload sem limite)
8. `api/main.py` — A8 (health check expõe detalhes de exceção)
9. `shared/minio_client.py` — A6, M8 (logging silencioso)
10. `shared/config.py` — A7 (dead code CNPJ)
11. `worker/steps/clean.py` — M3
12. `worker/steps/validate.py` — M7
13. `worker/steps/upsert.py` — M9

---

## 6. Critérios de Sucesso

- [ ] `pytest tests/` executa sem erros após todas as correções
- [ ] `insight_conta_global` retorna `"Possui Conta Global."` somente quando `dt_conta_criada_global` é não-nulo
- [ ] `gap_domicilio` retorna valor numérico não-nulo para linhas com `threshold_domicilio > 0`
- [ ] Endpoint `GET /v1/data/visao-cliente/historico?documento=<doc>` retorna pelo menos um registro após um job completado
- [ ] Dois uploads simultâneos do mesmo arquivo não geram dois jobs ativos
- [ ] `POST /v1/files/upload` com arquivo > limite configurado retorna HTTP 413
- [ ] `GET /ready` não expõe informações de conexão em caso de erro de banco
