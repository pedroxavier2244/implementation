# Guia de Integracao do ETL

## 1. Objetivo

Este documento orienta quem precisa consumir a API HTTP do `etl-system`, seja para operar carga manualmente, consultar dados consolidados ou integrar o pipeline com outros sistemas.

## 2. Visao geral

### 2.1 Base URL

Ambiente local padrao:

```text
http://localhost:8000
```

Ambiente HML padrao:

```text
http://localhost:8100
```

### 2.2 Autenticacao

No codigo atual, a API nao implementa autenticacao por token ou sessao.
Se houver restricao de acesso, ela deve ser feita por rede, proxy, VPN ou camada externa de seguranca.

### 2.3 Casos de uso principais

- subir um arquivo manualmente
- sincronizar o arquivo mais recente da origem configurada
- iniciar e acompanhar jobs ETL
- consultar a visao consolidada por CPF/CNPJ
- consultar historico de snapshots
- consultar cache e divergencias de CNPJ

## 3. Health e observabilidade

### 3.1 `GET /health`

Retorna saude basica do processo.

Resposta:

```json
{
  "status": "ok"
}
```

### 3.2 `GET /ready`

Valida dependencias externas.

Resposta tipica:

```json
{
  "postgres": "ok",
  "redis": "ok",
  "minio": "ok",
  "ready": true
}
```

### 3.3 `GET /metrics`

Expone metricas Prometheus quando `prometheus_fastapi_instrumentator` estiver disponivel.

## 4. Endpoints de arquivos

### 4.1 `GET /v1/files`

Lista arquivos conhecidos pelo ETL.

Parametros:

- `limit`
- `offset`

Resposta:

```json
{
  "items": [
    {
      "id": "96358a3a-dcd1-4497-b874-f69bcf3b22f7",
      "file_date": "2026-03-02",
      "filename": "Relatorio de Producao 19.02.26.xlsx",
      "hash_sha256": "86a398bfda92b48c0b2627bbf181770b348b8f01e0370b5ae1251a72757c90ca",
      "is_valid": true,
      "is_processed": true,
      "downloaded_at": "2026-03-02T14:06:19.887956Z"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

### 4.2 `GET /v1/files/{file_id}`

Retorna metadados de um arquivo especifico.

Erros comuns:

- `404`: `File not found`

### 4.3 `POST /v1/files/upload`

Recebe `multipart/form-data` com campo `file`.

Exemplo:

```bash
curl -X POST "http://localhost:8000/v1/files/upload" -F "file=@RELATORIO.xlsx"
```

Comportamento:

- calcula `sha256`
- extrai `file_date` do nome quando encontrar padrao `DD.MM.AA`
- grava o bruto no MinIO
- cria registro em `etl_file`

### 4.4 `POST /v1/files/sync`

Dispara sincronizacao do arquivo fonte configurado em `ETL_SOURCE_API_URL`.

Resposta:

```json
{
  "task_id": "mock-task-id",
  "status": "QUEUED"
}
```

Observacoes:

- o sync nao processa a planilha diretamente na API
- ele enfileira o `checker.checker.run_daily`

## 5. Endpoints de jobs

### 5.1 `POST /v1/jobs/run`

Body JSON:

```json
{
  "file_id": "96358a3a-dcd1-4497-b874-f69bcf3b22f7"
}
```

Resposta:

```json
{
  "job_id": "8b427c66-3842-4eaa-9722-59d887c9e8c4",
  "status": "QUEUED"
}
```

### 5.2 `POST /v1/jobs/reprocess/{file_id}`

Reenfileira o processamento de um arquivo conhecido.

### 5.3 `GET /v1/jobs`

Lista jobs.

Parametros:

- `status`
- `limit`
- `offset`

### 5.4 `GET /v1/jobs/{job_id}`

Retorna o job com steps.

Exemplo:

```json
{
  "id": "4492ab5f-ebe4-4b8c-9386-77b32c294212",
  "status": "DONE",
  "triggered_by": "scheduler",
  "rows_total": 161657,
  "rows_ok": 161657,
  "rows_bad": 0,
  "retry_count": 0,
  "started_at": "2026-03-02T15:56:51.009266Z",
  "finished_at": "2026-03-02T16:22:50.431701Z",
  "steps": [
    {
      "step_name": "extract",
      "status": "DONE",
      "started_at": "2026-03-02T15:56:51.087469Z",
      "finished_at": "2026-03-02T15:58:08.090232Z",
      "error_message": null
    }
  ]
}
```

### 5.5 Estados relevantes de job

- `QUEUED`
- `RUNNING`
- `RETRYING`
- `DONE`
- `DEAD`

## 6. Endpoint principal de dados

### 6.1 `GET /v1/data/visao-cliente`

Este e o endpoint principal para consultar o dado consolidado do ETL.

Parametros:

- `documento` obrigatorio
- `limit` opcional, padrao `1`
- `offset` opcional, padrao `0`
- `fallback_rf` opcional, padrao `true`

Exemplo:

```http
GET /v1/data/visao-cliente?documento=26018023000117&limit=1&offset=0&fallback_rf=true
```

### 6.2 Comportamento de busca

Regras:

1. a API limpa a mascara e fica apenas com digitos
2. busca primeiro em `final_visao_cliente`
3. se nao encontrar, tenta compatibilidade com linhas antigas contendo pontuacao
4. se ainda nao encontrar e `fallback_rf=true` para um CNPJ de 14 digitos, usa cache RF ou consulta a API configurada

### 6.3 Envelope de resposta

```json
{
  "documento_consultado": "26018023000117",
  "total": 1,
  "limit": 1,
  "offset": 0,
  "items": [
    {
      "cd_cpf_cnpj_cliente": "26018023000117",
      "nome_cliente": "JESSE JAMES MONIZ FRANCO",
      "data_base": "2026-03-04 00:00:00"
    }
  ]
}
```

### 6.4 Contrato de colunas do item

O item retorna:

- todas as colunas de `shared/visao_cliente_schema.REQUIRED_COLUMNS`
- todas as colunas RF persistidas em `final_visao_cliente`
- aliases RF adicionais

Em outras palavras, o contrato cobre:

- identificacao e relacionamento
- dados de conta e cartao
- C6Pay e maquininha
- boleto e cobranca
- cash in, comissao e qualificacao
- safra, cancelamento e elegibilidade
- metricas `metrica_*` e `score_perfil`
- colunas `rf_*`
- aliases como `nome_fantasia`, `situacao_cadastral`, `cnae_fiscal`, `natureza_juridica`, `capital_social`, `porte`, `data_inicio_ativ`, `descricao_situacao`, `cnae_descricao`, `data_source`

### 6.5 Garantia de chaves

Mesmo quando um campo nao existir para aquela linha, a resposta tende a manter a chave e devolver `null`.
Isso vale especialmente para:

- colunas RF em registros ainda nao enriquecidos
- aliases RF
- colunas vindas apenas de fallback externo

### 6.6 Campo `data_source`

Comportamento esperado:

- linha local de `final_visao_cliente`: normalmente `null`
- fallback do cache RF: `receita_federal_cache`
- fallback da consulta ao servico RF: `receita_federal_brasilapi`

### 6.7 Tipagem

Cuidados importantes:

- muitas datas e numericos ainda saem como string
- as metricas `metrica_*` e `score_perfil` saem como numero quando presentes
- valores ausentes podem aparecer como `null`

## 7. Historico por documento

### 7.1 `GET /v1/data/visao-cliente/historico`

Consulta a linha do tempo em `staging_visao_cliente`.

Parametros:

- `documento` obrigatorio
- `limit` opcional, padrao `50`
- `offset` opcional, padrao `0`

### 7.2 Resposta

Cada snapshot devolve:

- `data_base`
- `carregado_em`
- `etl_job_id`
- `campos_alterados`
- `dados`

`campos_alterados` representa o diff em relacao ao snapshot anterior.

### 7.3 Quando usar

Use esse endpoint quando precisar:

- investigar mudanca de valores entre cargas
- validar se uma coluna veio errada de uma data base especifica
- auditar a evolucao de um cliente sem depender apenas da tabela final

## 8. Endpoints de CNPJ

### 8.1 `GET /v1/cnpj/{cnpj}`

Busca no cache local e, opcionalmente, consulta a fonte RF ao vivo.

Parametros:

- `fallback_live` opcional, padrao `true`

Resposta:

```json
{
  "data_source": "cache",
  "cnpj": "26018023000117",
  "razao_social": "EMPRESA EXEMPLO LTDA",
  "nome_fantasia": "EMPRESA EXEMPLO",
  "situacao_cadastral": "02",
  "descricao_situacao": "ATIVA",
  "cnae_fiscal": "4751201",
  "cnae_descricao": "Comercio varejista especializado",
  "natureza_juridica": "2062",
  "capital_social": "10000.00",
  "porte": "03",
  "uf": "SP",
  "municipio": "SAO PAULO",
  "email": "contato@empresa.com",
  "data_inicio_ativ": "20200101",
  "last_checked_at": "2026-03-13T12:00:00+00:00"
}
```

Erros comuns:

- `400`: `cnpj must have 14 digits`
- `404`: CNPJ nao encontrado no cache e na API CNPJ

### 8.2 `GET /v1/cnpj/divergencias/list`

Lista divergencias entre dado operacional e dado da Receita Federal.

Filtros:

- `cnpj`
- `campo`
- `limit`
- `offset`

## 9. Fluxos recomendados de integracao

### 9.1 Carga manual

1. subir o arquivo com `POST /v1/files/upload`
2. guardar o `file_id`
3. iniciar o processamento em `POST /v1/jobs/run`
4. acompanhar em `GET /v1/jobs/{job_id}`
5. consultar resultado em `GET /v1/data/visao-cliente`

### 9.2 Carga automatica

1. disparar `POST /v1/files/sync` ou aguardar o scheduler
2. acompanhar o job gerado
3. validar `final_visao_cliente` via endpoint de dados

### 9.3 Consulta operacional por documento

1. chamar `GET /v1/data/visao-cliente?documento=<cpf_ou_cnpj>`
2. se precisar evolucao no tempo, chamar `GET /v1/data/visao-cliente/historico`
3. se precisar dados RF isolados, chamar `GET /v1/cnpj/{cnpj}`

## 10. Erros e convencoes

### 10.1 Envelope de erro

A maioria dos erros retorna:

```json
{
  "detail": "mensagem de erro"
}
```

### 10.2 Erros comuns

- `400`: documento ou CNPJ invalido
- `404`: arquivo ou job nao encontrado
- `500`: erro interno de processamento

### 10.3 Dicas de consumo

- envie CPF/CNPJ com ou sem mascara; a API normaliza
- trate `null` como ausencia de dado
- nao assuma que todo valor numerico veio tipado como numero
- logue `job_id` e `file_id` nos clientes integradores para facilitar suporte

## 11. Recomendacao para consumidores downstream

### 11.1 Se o objetivo e integracao operacional

Prefira consumir:

- `GET /v1/data/visao-cliente`

### 11.2 Se o objetivo e auditoria de mudanca

Prefira consumir:

- `GET /v1/data/visao-cliente/historico`

### 11.3 Se o objetivo e so Receita Federal

Prefira consumir:

- `GET /v1/cnpj/{cnpj}`

## 12. Checklist do integrador

Antes de homologar:

1. validar `GET /health` e `GET /ready`
2. validar upload ou sync de arquivo
3. validar fila de job
4. validar `GET /v1/data/visao-cliente` para documento conhecido
5. validar fallback RF para CNPJ ausente localmente
6. validar historico por documento
7. validar tratamento de `null` e tipagem legado
