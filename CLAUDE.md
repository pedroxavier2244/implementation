# ETL System — Estado Atual e Histórico de Trabalho

## O que este sistema faz
ETL de arquivos Excel (.xlsx) com pipeline: upload → MinIO → Celery worker → PostgreSQL (Supabase).
O arquivo de produção é o **"Relatório de Produção"** com aba **"Visão Cliente"**.
Após o ETL, os leads ficam em `public.projetinho_pai` e as atribuições em `public.lead_atribuicoes`.

---

## Estado atual do banco de produção (06/04/2026)

| Tabela | Linhas | IDs |
|--------|--------|-----|
| `projetinho_pai` | 25.369 | 97.445–197.039 (originais do backup) |
| `lead_atribuicoes` | 17.468 | todos com lead_id válido |
| `projetinho_pai_backup_20260402` | 25.369 | mesmo que acima |
| FK órfãs | 0 | banco íntegro |

**O ETL de produção NÃO foi rodado com o arquivo novo (01/04/26).** Aguardando correção do bug do `ON DELETE SET NULL`.

---

## Infraestrutura

- **VPS:** 5.189.163.33 (root / cb5D75sc41Txr)
- **Repo na VPS:** `/opt/apps/implementation`
- **Docker Compose v2** (usar `docker compose`, NÃO `docker-compose` — v1 quebra com imagens novas)
- **Supabase prod:** `db.qdkiksitojyeembgzdey.supabase.co` (IPv6 only — só acessível via VPS)
- **MinIO prod:** `5.189.163.33:9000` — user: `minioadmin` / senha: `OutraSenhaForte_MinIO!2026`
- **Banco prod URL:** `postgresql://postgres:Mbfinance%402026@db.qdkiksitojyeembgzdey.supabase.co/postgres`
- **Containers prod relevantes:**
  - `implementation-api-1` (porta 8000)
  - `implementation-worker-etl-1` (Celery, queue `etl_jobs`)
  - `54bc71b1fc31_implementation-postgres-1` (postgres local — proxy para Supabase via psql)
  - `eb14ea740b0b_implementation-redis-1`
  - `4ef26914bfbd_implementation-minio-1`

---

## FK crítica descoberta (ROOT CAUSE dos bugs)

```sql
-- lead_atribuicoes tem esta FK:
FOREIGN KEY (lead_id) REFERENCES projetinho_pai(id)
ON UPDATE CASCADE
ON DELETE SET NULL   ← ESTE É O PROBLEMA
```

**Qualquer DELETE em `projetinho_pai` seta `lead_id = NULL` automaticamente em `lead_atribuicoes`.**

Isso causou todos os bugs de lead_id nulo. O ETL deletava linhas de `projetinho_pai` e o banco silenciosamente zerava os lead_ids.

---

## Bugs corrigidos (em código, aguardando deploy seguro)

### 1. `worker/steps/upsert.py` — Preservação de IDs
**Problema:** `arquivar_por_data_base()` (função SQL) copiava para `historico` E **deletava** de `projetinho_pai`. Com projetinho_pai vazio, o upsert fazia INSERT puro → IDs novos → lead_atribuicoes quebrava.

**Fix aplicado:**
- Arquivo sem delete: copia para `historico` manualmente sem chamar `arquivar_por_data_base`
- Upsert usa `ON CONFLICT ("CD_CPF_CNPJ_CLIENTE") DO UPDATE` → preserva IDs existentes
- DELETE stale ao final: **ainda causa ON DELETE SET NULL** — precisa ser corrigido antes do deploy

**Fix aplicado (DELETE stale):**
Deletar `lead_atribuicoes` dos leads stale ANTES de deletar de `projetinho_pai` — evita `ON DELETE SET NULL` silencioso. Ver `worker/steps/upsert.py` linhas ~141-168.

### 2. `worker/steps/upsert.py` — DatetimeFieldOverflow
**Problema:** `TO_DATE(DATA_REFERENCIA, 'DD/MM/YYYY')` quebrava quando DATA_REFERENCIA estava em formato `YYYY-MM-DD HH:MM:SS`.

**Fix aplicado:** parse da data no Python antes de passar ao SQL.

### 3. `worker/tasks.py` — Retry status não persistia
**Problema:** `session.rollback()` dentro do except apagava o status RETRYING/DEAD antes de sair.

**Fix aplicado:** `session.commit()` antes de `self.retry()`.

### 4. `worker/steps/assign_leads.py` — CNPJs novos sem atribuição
**Fix aplicado:** fallback por `NOME_CONSULTOR` para novos CNPJs que não estão na carteira.

### 5. `worker/steps/upsert.py` — Backup automático
**Fix aplicado:** antes de qualquer alteração, cria `projetinho_pai_backup_YYYYMMDD`.
Script de rollback em `scripts/rollback-projetinho-pai.sql`.

---

## Ambiente HML local

```bash
& "C:\Program Files\Git\bin\bash.exe" scripts/setup-hml.sh
```

- **API HML:** http://localhost:8100
- **MinIO HML:** http://localhost:9101 (minioadmin / minioadmin_hml_2026)
- **Banco HML:** Supabase branch "teste" — `db.xentrwrpnhcrunvupmhx.supabase.co`
- Postgres local não é mais usado no HML — banco é o Supabase de testes

---

## Próximos passos obrigatórios antes de rodar ETL em prod

1. **Testar no HML** que:
   - IDs são preservados após ETL
   - lead_atribuicoes não ficam nulos
   - Leads que saem do arquivo têm suas atribuições removidas (não setadas a null)

3. **Fazer commit + deploy** na VPS usando `docker compose` (v2, não v1)

4. **Rodar o ETL** com o arquivo `Relatório de Produção - 01.04.26 (1).xlsx`
   - Arquivo já está no MinIO de prod: `2026/04/01/Relatório de Produção - 01.04.26 (1).xlsx`
   - File ID em prod: `30359019-5d21-43dc-aeb1-ca9d7817d8bb`
   - Disparar: `POST http://5.189.163.33:8000/v1/jobs/reprocess/30359019-5d21-43dc-aeb1-ca9d7817d8bb`

---

## Rollback de emergência (2 comandos)

```sql
-- Executa via psql no container 54bc71b1fc31_implementation-postgres-1
BEGIN;
DELETE FROM public.projetinho_pai;
INSERT INTO public.projetinho_pai SELECT * FROM public.projetinho_pai_backup_20260402;
COMMIT;
```

Se lead_atribuicoes ficar nulo, restaurar do dump:
```bash
# Na VPS, o dump está em /tmp/prod_dump.dump
docker run --rm \
  --network container:implementation-api-1 \
  -v /tmp/prod_dump.dump:/tmp/prod_dump.dump \
  postgres:17-alpine \
  pg_restore -d "postgresql://postgres:Mbfinance%402026@db.qdkiksitojyeembgzdey.supabase.co/postgres" \
  --no-owner --no-acl -t lead_atribuicoes /tmp/prod_dump.dump
```
