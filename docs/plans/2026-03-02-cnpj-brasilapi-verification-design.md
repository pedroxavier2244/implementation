# Design: Verificacao e Enriquecimento CNPJ via BrasilAPI

**Data:** 2026-03-02
**Status:** Aprovado

---

## Objetivo

Apos cada execucao do ETL, verificar os CNPJs dos clientes da tabela `final_visao_cliente` contra a BrasilAPI (Receita Federal), armazenar os dados oficiais nas colunas `rf_*` e registrar divergencias entre os dados do C6 Bank e os da Receita Federal, enviando alerta quando houver divergencias.

---

## Abordagem Escolhida

**Novo passo `cnpj_verify` no pipeline ETL** — roda apos o `upsert`, usa o mesmo sistema de checkpoints de idempotencia, mesmo `job_id` para rastreabilidade. Sem novo container ou fila.

---

## Estrutura de Dados

### Nova tabela: `cnpj_rf_cache`

Cache dos dados da BrasilAPI por CNPJ, com TTL de 30 dias.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| cnpj | TEXT PK | CNPJ sem formatacao (14 digitos) |
| razao_social | TEXT | Razao social (Receita Federal) |
| nome_fantasia | TEXT | Nome fantasia |
| situacao_cadastral | TEXT | Codigo (ex: "2") |
| descricao_situacao | TEXT | Descricao (ex: "ATIVA") |
| cnae_fiscal | TEXT | Codigo CNAE principal |
| cnae_descricao | TEXT | Descricao do CNAE |
| natureza_juridica | TEXT | Natureza juridica |
| capital_social | TEXT | Capital social declarado |
| porte | TEXT | Porte da empresa |
| uf | TEXT | UF do registro |
| municipio | TEXT | Municipio do registro |
| email | TEXT | Email da Receita Federal |
| data_inicio_ativ | TEXT | Data de inicio das atividades |
| last_checked_at | TIMESTAMPTZ | Ultima consulta |

### Nova tabela: `cnpj_divergencia`

Registro de divergencias encontradas entre C6 Bank e Receita Federal.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| id | UUID PK | |
| job_id | TEXT FK -> etl_job_run | Job que detectou a divergencia |
| cnpj | TEXT | CNPJ do cliente |
| campo | TEXT | Nome do campo divergente (ex: "nome_cliente") |
| valor_c6 | TEXT | Valor no relatorio C6 Bank |
| valor_rf | TEXT | Valor na Receita Federal |
| found_at | TIMESTAMPTZ | Quando foi encontrada |

### 11 novas colunas em `final_visao_cliente`

Conforme documento de regras de negocio (colunas RF_):

`rf_razao_social`, `rf_natureza_juridica`, `rf_capital_social`, `rf_porte_empresa`,
`rf_nome_fantasia`, `rf_situacao_cadastral`, `rf_data_inicio_ativ`, `rf_cnae_principal`,
`rf_uf`, `rf_municipio`, `rf_email`

---

## Fluxo de Execucao

```
ETL pipeline:
  extract → clean → enrich → validate → stage → upsert → cnpj_verify [NOVO]

cnpj_verify:
  1. Coleta CNPJs distintos do staging para o job atual
  2. Filtra: sem cache OU last_checked_at < agora - 30 dias
  3. Para cada CNPJ a verificar:
     a. GET brasilapi.com.br/api/cnpj/v1/{cnpj}
     b. Salva/atualiza cnpj_rf_cache
     c. UPDATE final_visao_cliente SET rf_* = ... WHERE cd_cpf_cnpj_cliente = cnpj
     d. Normaliza e compara campos → insere em cnpj_divergencia se divergir
     e. sleep(0.3s) para respeitar rate limit (~3 req/s)
  4. Se total de divergencias > 0 → envia alerta CNPJ_DIVERGENCIA (WARNING)
```

---

## Campos Comparados

| Campo C6 Bank | Campo BrasilAPI | O que detecta |
|---|---|---|
| `nome_cliente` | `razao_social` | Nome cadastrado diferente da Receita |
| `uf` | `uf` | Estado incorreto |
| `cidade` | `municipio` | Cidade incorreta |
| `ramo_atuacao` | `cnae_fiscal_descricao` | CNAE divergente |
| — | `descricao_situacao_cadastral` | CNPJ BAIXADO/SUSPENSO ainda ativo |

**Normalizacao antes de comparar:** remove acentos, converte para maiusculas, remove pontuacao — evita falso positivo por "S/A" vs "SA" ou "LTDA." vs "LTDA".

---

## Tratamento de Erros

| Situacao | Comportamento |
|----------|---------------|
| CNPJ nao encontrado (404) | Registra `rf_situacao_cadastral = "NAO_ENCONTRADO"` no cache, nao gera divergencia |
| Rate limit (429) | Para o lote, job continua sem falhar (ETL ja gravou os dados) |
| Timeout / erro generico | Loga, pula o CNPJ, continua o proximo |
| BrasilAPI totalmente indisponivel | Passo marcado como DONE com warning, ETL nao e afetado |
| CPF (< 14 digitos) | Pula sem consultar |

**O ETL nunca falha por causa da BrasilAPI** — o passo e "best effort".

---

## Configuracao (`.env`)

```env
CNPJ_CACHE_TTL_DAYS=30
BRASILAPI_TIMEOUT=10
```

---

## Arquivos Criados/Alterados

| Arquivo | Acao |
|---------|------|
| `shared/brasilapi.py` | NOVO — cliente HTTP com retry e rate limit |
| `shared/models.py` | ALTERAR — + CnpjRfCache, CnpjDivergencia |
| `shared/config.py` | ALTERAR — + CNPJ_CACHE_TTL_DAYS, BRASILAPI_TIMEOUT |
| `shared/visao_cliente_schema.py` | ALTERAR — + 11 colunas rf_* em REQUIRED_COLUMNS |
| `worker/steps/cnpj_verify.py` | NOVO — logica do passo |
| `worker/tasks.py` | ALTERAR — adiciona cnpj_verify() apos upsert |
| `api/routes/cnpj.py` | NOVO — GET /v1/cnpj/{cnpj}, GET /v1/cnpj/divergencias |
| `api/main.py` | ALTERAR — registra router cnpj |
| `migrations/versions/20260302_000006_cnpj_rf_tables.py` | NOVO — tabelas cnpj_rf_cache e cnpj_divergencia |
| `migrations/versions/20260302_000007_final_rf_columns.py` | NOVO — colunas rf_* em final_visao_cliente |
| `tests/unit/test_cnpj_verify.py` | NOVO — testes do passo |
| `tests/unit/test_brasilapi.py` | NOVO — testes do cliente HTTP |

---

## Alerta de Divergencia

Usa o notifier existente:

```python
event_type = "CNPJ_DIVERGENCIA"
severity   = "WARNING"
message    = "3 divergencias encontradas no job abc-123"
metadata   = {
    "job_id": "abc-123",
    "total_divergencias": 3,
    "exemplos": ["24745893000162: nome_cliente diverge", ...]
}
```

---

## Nao esta no escopo

- Atualizacao automatica dos campos do C6 Bank com dados da RF (usuario decide o que fazer com a divergencia)
- Interface grafica para resolucao de divergencias
- Suporte a CPF (apenas CNPJ — base e toda PJ)
