# Erros cometidos — nunca repetir

Este arquivo documenta erros reais que causaram bugs em produção.
Antes de qualquer mudança no pipeline ETL, leia isso.

---

## ERRO 1 — Chamar `arquivar_por_data_base()` antes do upsert

**O que aconteceu:**
A função SQL `arquivar_por_data_base()` copia dados de `projetinho_pai` para `historico` **E depois deleta** de `projetinho_pai`. Com a tabela vazia, o upsert a seguir fazia INSERT puro → novos IDs → `lead_atribuicoes.lead_id` passava a referenciar IDs que não existiam mais.

**Consequência:** Todos os leads apareciam "sem dono" no frontend.

**Regra:**
> Nunca chamar `arquivar_por_data_base()`. Arquivar manualmente com `INSERT INTO historico ... SELECT ... FROM projetinho_pai` sem DELETE.

---

## ERRO 2 — Deletar de `projetinho_pai` sem antes deletar de `lead_atribuicoes`

**O que aconteceu:**
A FK em `lead_atribuicoes` tem `ON DELETE SET NULL`:
```sql
FOREIGN KEY (lead_id) REFERENCES projetinho_pai(id) ON DELETE SET NULL
```
Qualquer `DELETE FROM projetinho_pai` seta `lead_id = NULL` silenciosamente em `lead_atribuicoes`. O banco não dá erro — simplesmente zera os IDs.

**Consequência:** Todo lead deletado de `projetinho_pai` perde sua atribuição de consultor sem aviso.

**Regra:**
> Sempre que for deletar linhas de `projetinho_pai`, primeiro deletar as linhas correspondentes de `lead_atribuicoes`:
> ```sql
> DELETE FROM public.lead_atribuicoes
> WHERE lead_id IN (SELECT id FROM public.projetinho_pai WHERE ...);
> -- SÓ DEPOIS:
> DELETE FROM public.projetinho_pai WHERE ...;
> ```

---

## ERRO 3 — Não verificar FKs antes de escrever lógica de DELETE

**O que aconteceu:**
Escrevi o step de DELETE stale sem antes verificar as FKs que dependem de `projetinho_pai`. Se tivesse rodado `\d lead_atribuicoes` no início, o `ON DELETE SET NULL` teria aparecido imediatamente.

**Regra:**
> Antes de escrever qualquer DELETE em tabela que outros objetos possam referenciar, rodar:
> ```sql
> SELECT conname, confupdtype, confdeltype
> FROM pg_constraint
> WHERE confrelid = 'public.projetinho_pai'::regclass;
> ```
> `confdeltype = 'n'` significa `ON DELETE SET NULL` — alerta máximo.

---

## ERRO 4 — Usar `docker-compose` (v1) na VPS

**O que aconteceu:**
`docker-compose up` (v1) com imagens mais novas do Docker lança `KeyError: 'ContainerConfig'` e derruba os containers de infra (postgres, redis, minio) no meio do processo.

**Consequência:** API e worker caem em produção enquanto tentamos subir.

**Regra:**
> Sempre usar `docker compose` (v2, sem hífen) na VPS. Nunca `docker-compose`.

---

## ERRO 5 — Usar pg_dump 17 e restaurar com pg_restore 16

**O que aconteceu:**
O dump de produção era feito pelo Supabase (pg_dump 17). Ao restaurar com `pg_restore` de um container `postgres:16-alpine`, o comando falha com "unsupported version 1.16".

**Regra:**
> Sempre usar `postgres:17-alpine` para dump E restore quando o banco de origem é Supabase.
> `docker-compose.hml.yml` já foi corrigido para `postgres:17-alpine`.

---

## ERRO 6 — Fazer sugestões de trabalho não solicitadas

**O que aconteceu:**
Ao investigar o bug de lead_id, comecei a sugerir redesign de API e outros trabalhos que não foram pedidos. O usuário ficou bravo: *"o fdp isso nao e seu trabalho"*.

**Regra:**
> Só fazer o que foi pedido. Nada além. Se identificar um problema adjacente, mencionar em uma linha e perguntar antes de agir.

---

## ERRO 7 — Deployar em produção sem testar o fluxo completo de FK no HML

**O que aconteceu:**
O deploy do ETL foi feito sem validar a cadeia FK completa (projetinho_pai → lead_atribuicoes) no ambiente HML. O bug do `ON DELETE SET NULL` só foi descoberto depois de estragar o banco de produção.

**Regra:**
> Antes de qualquer deploy que toque em DELETE/UPDATE em `projetinho_pai`:
> 1. Subir HML com dados reais de prod
> 2. Rodar o ETL completo no HML
> 3. Verificar: `SELECT COUNT(*) FROM lead_atribuicoes WHERE lead_id IS NULL` → deve ser 0
> 4. Só então fazer deploy

---

## ERRO 8 — `DatetimeFieldOverflow` por passar string de data direto ao SQL

**O que aconteceu:**
`TO_DATE("DATA_REFERENCIA", 'DD/MM/YYYY')` quebrava quando `DATA_REFERENCIA` estava no formato `YYYY-MM-DD HH:MM:SS`. O PostgreSQL tentava parsear o timestamp com o formato errado e lançava `DatetimeFieldOverflow`.

**Regra:**
> Nunca converter datas em SQL quando o formato pode variar. Parsear no Python com múltiplos formatos e passar o objeto `date` como parâmetro tipado ao SQLAlchemy.

---

## ERRO 9 — Reprocessar job sem verificar o estado atual no banco

**O que aconteceu:**
Tentei reprocessar um file_id que tinha um job em status `RUNNING` vindo do dump de prod. A API recusou porque já havia um job ativo. Só descobri após investigar manualmente.

**Regra:**
> Antes de disparar reprocess, verificar:
> ```sql
> SELECT id, status, created_at FROM etl_job_run
> WHERE file_id = '<id>' ORDER BY created_at DESC LIMIT 3;
> ```
> Se houver job em `RUNNING`, setar para `DEAD` antes:
> ```sql
> UPDATE etl_job_run SET status = 'DEAD' WHERE id = '<job_id>';
> ```
