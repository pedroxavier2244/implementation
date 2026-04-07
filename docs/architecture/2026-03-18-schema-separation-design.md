# Schema Separation — etl_db

**Date:** 2026-03-18
**Status:** Approved
**Scope:** ETL system (`etl-system`) + Integration API (`integration-api`)

---

## Problem

Both applications share the same PostgreSQL database (`etl_db`) and write all tables to the `public` schema. This causes:

- 18 tables from two unrelated systems mixed in one schema
- Two Alembic migration histories coexisting in the same namespace
- No isolation: a migration in one app can affect the other
- Violates the documented convention ("banco corporativo — nunca usar public") already followed by `cnpj_db`

---

## Goal

Separate tables into two named schemas within `etl_db`:

- `etl` — owned and managed by `etl-system`
- `integration` — owned and managed by `integration-api`

The `integration-api` reads ETL tables (read-only) and will reference them via the `etl` schema.

---

## Target State

```
etl_db
├── schema: etl                        (owner: etl-system / Alembic)
│   ├── etl_file
│   ├── etl_job_run
│   ├── etl_job_step
│   ├── etl_bad_rows
│   ├── staging_visao_cliente
│   ├── final_visao_cliente
│   ├── visao_cliente_change_history
│   ├── analytics_indicator_snapshot
│   ├── alert_event
│   ├── alert_event_channel
│   └── alembic_version               (ETL migration tracking)
│
└── schema: integration                (owner: integration-api / Alembic)
    ├── users
    ├── refresh_tokens
    ├── password_reset_tokens
    ├── audit_logs
    ├── crm_inbound_events
    ├── crm_outbound_jobs
    └── integration_api_alembic_version
```

---

## search_path Strategy

All worker and API code uses bare (unqualified) table names in raw SQL and pandas `to_sql()`. Rather than rewriting every SQL statement, the PostgreSQL role gets a permanent `search_path` that includes `etl`:

```sql
ALTER ROLE etl_user SET search_path = etl, integration, public;
```

This runs once in the migration and persists. New connections by `etl_user` will resolve bare names against `etl` first, then `integration`, then `public`. No raw SQL statements need to change.

---

## Changes

### ETL System (`etl-system`)

#### `shared/models.py`
1. Add `__table_args__ = {"schema": "etl"}` to all SQLAlchemy models that have ORM classes:
   - `EtlFile`, `EtlJobRun`, `EtlJobStep`, `EtlBadRow`
   - `AlertEvent`, `AlertEventChannel`, `AnalyticsIndicatorSnapshot`
   - `VisaoClienteChangeHistory`

   Note: `staging_visao_cliente` and `final_visao_cliente` have no ORM class — they are managed via raw pandas `to_sql()` and raw SQL. The `search_path` change covers them.

2. Update all `ForeignKey()` string references to be schema-qualified:
   ```python
   # Before
   ForeignKey("etl_file.id")
   ForeignKey("etl_job_run.id")
   ForeignKey("alert_event.id")
   # After
   ForeignKey("etl.etl_file.id")
   ForeignKey("etl.etl_job_run.id")
   ForeignKey("etl.alert_event.id")
   ```
   Affected FKs:
   - `EtlJobRun.file_id` → `etl.etl_file.id`
   - `EtlJobStep.job_id` → `etl.etl_job_run.id`
   - `EtlBadRow.job_id` → `etl.etl_job_run.id`
   - `AlertEventChannel.alert_id` → `etl.alert_event.id`
   - `AnalyticsIndicatorSnapshot.job_id` → `etl.etl_job_run.id`
   - `AnalyticsIndicatorSnapshot.file_id` → `etl.etl_file.id`
   - `VisaoClienteChangeHistory.etl_job_id` → `etl.etl_job_run.id`
   - `VisaoClienteChangeHistory.file_id` → `etl.etl_file.id`

#### `worker/steps/stage.py`
Add `schema="etl"` to the `DataFrame.to_sql()` call:
```python
df.to_sql(STAGING_TABLE, con=conn, schema="etl", if_exists="append", ...)
```

#### `worker/steps/upsert.py`
Add `AND table_schema = 'etl'` to the `information_schema.columns` query:
```sql
SELECT column_name
FROM information_schema.columns
WHERE table_name = :table_name
  AND table_schema = 'etl'
ORDER BY ordinal_position
```

#### `scripts/backfill_change_history.py`
Two changes:
1. Add `AND table_schema = 'etl'` to the `information_schema.columns` query (same as upsert.py).
2. Schema-qualify the `to_regclass()` check:
   ```python
   text("SELECT to_regclass('etl.visao_cliente_change_history')")
   ```

#### `alembic.ini`
```ini
version_table = alembic_version
version_table_schema = etl
```

#### `migrations/env.py`
Add to both offline and online `context.configure()` calls:
```python
version_table="alembic_version",
version_table_schema="etl",
include_schemas=True,
```

#### New Alembic migration (`000012_move_to_etl_schema.py`)

The `alembic_version` table is NOT moved inside this migration function — Alembic records the revision after `upgrade()` returns. Moving the version table within the same migration creates a race condition. Instead:

- `alembic.ini` and `env.py` changes (above) are deployed first.
- The migration creates `etl.alembic_version` via Alembic's normal startup (when `version_table_schema = etl` takes effect), then the old `public.alembic_version` is dropped at the end of the migration.

```sql
-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS etl;

-- 2. Grant permissions to app role (tables + sequences for autoincrement columns)
GRANT USAGE ON SCHEMA etl TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA etl TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT USAGE, SELECT ON SEQUENCES TO etl_user;
ALTER ROLE etl_user SET search_path = etl, integration, public;
-- Note: during the window between this migration (step 3) and the integration migration (step 5),
-- the integration schema does not yet exist. PostgreSQL silently ignores nonexistent schemas
-- in search_path, so there is no runtime error. The constraint is: no table name may exist
-- in both etl and integration schemas (currently satisfied by the target-state table list).

-- 3. Move ETL tables
ALTER TABLE public.etl_file                     SET SCHEMA etl;
ALTER TABLE public.etl_job_run                  SET SCHEMA etl;
ALTER TABLE public.etl_job_step                 SET SCHEMA etl;
ALTER TABLE public.etl_bad_rows                 SET SCHEMA etl;
ALTER TABLE public.staging_visao_cliente        SET SCHEMA etl;
ALTER TABLE public.final_visao_cliente          SET SCHEMA etl;
ALTER TABLE public.visao_cliente_change_history SET SCHEMA etl;
ALTER TABLE public.analytics_indicator_snapshot SET SCHEMA etl;
ALTER TABLE public.alert_event                  SET SCHEMA etl;
ALTER TABLE public.alert_event_channel          SET SCHEMA etl;

-- 4. Drop old public.alembic_version only after confirming etl.alembic_version is populated.
-- If alembic.ini/env.py were NOT deployed before this migration, this block raises an error
-- instead of silently leaving the system in an inconsistent state.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM etl.alembic_version) THEN
    DROP TABLE IF EXISTS public.alembic_version;
  ELSE
    RAISE EXCEPTION 'etl.alembic_version is empty — deploy alembic.ini and env.py changes before running this migration';
  END IF;
END $$;
```

Downgrade:
```sql
-- Move all tables back to public (no ordering constraint — SET SCHEMA does not drop FK constraints)
ALTER TABLE etl.alert_event_channel          SET SCHEMA public;
ALTER TABLE etl.alert_event                  SET SCHEMA public;
ALTER TABLE etl.analytics_indicator_snapshot SET SCHEMA public;
ALTER TABLE etl.visao_cliente_change_history SET SCHEMA public;
ALTER TABLE etl.final_visao_cliente          SET SCHEMA public;
ALTER TABLE etl.staging_visao_cliente        SET SCHEMA public;
ALTER TABLE etl.etl_bad_rows                 SET SCHEMA public;
ALTER TABLE etl.etl_job_step                 SET SCHEMA public;
ALTER TABLE etl.etl_job_run                  SET SCHEMA public;
ALTER TABLE etl.etl_file                     SET SCHEMA public;

-- Restore alembic_version to public (insert current revision then drop etl copy)
INSERT INTO public.alembic_version SELECT version_num FROM etl.alembic_version;
DROP TABLE IF EXISTS etl.alembic_version;

-- Revoke grants and reset role
REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA etl FROM etl_user;
REVOKE ALL ON ALL TABLES IN SCHEMA etl FROM etl_user;
REVOKE USAGE ON SCHEMA etl FROM etl_user;
ALTER ROLE etl_user SET search_path = DEFAULT;
DROP SCHEMA IF EXISTS etl;
```

---

### Integration API (`integration-api`)

#### `app/models/` — owned models
Add `__table_args__ = {"schema": "integration"}` to:
- `User`, `RefreshToken`, `PasswordResetToken`, `AuditLog`
- `CrmInboundEvent`, `CrmOutboundJob`

#### `app/models/` — read-only ETL models
Add `__table_args__ = {"schema": "etl"}` to:
- `VisaoCliente` (`final_visao_cliente`)
- `VisaoClienteChangeHistory` (`visao_cliente_change_history`)
- `EtlFile` (`etl_file`)

#### `alembic.ini`
```ini
version_table = integration_api_alembic_version
version_table_schema = integration
```

#### `alembic/env.py`
Add to both offline and online `context.configure()` calls:
```python
version_table="integration_api_alembic_version",
version_table_schema="integration",
include_schemas=True,
```

Update the `include_object` filter: instead of excluding by table name, exclude by schema:
```python
def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and object.schema == "etl":
        return False
    return True
```

#### New Alembic migration (`003_move_to_integration_schema.py`)

Same sequencing rule applies: `integration_api_alembic_version` is NOT moved inside the function. `alembic.ini` changes are deployed before running this migration.

```sql
-- 1. Create schema
CREATE SCHEMA IF NOT EXISTS integration;

-- 2. Grant permissions (tables + sequences)
GRANT USAGE ON SCHEMA integration TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA integration TO etl_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA integration GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA integration GRANT USAGE, SELECT ON SEQUENCES TO etl_user;

-- 3. Move integration tables
ALTER TABLE public.users                           SET SCHEMA integration;
ALTER TABLE public.refresh_tokens                  SET SCHEMA integration;
ALTER TABLE public.password_reset_tokens           SET SCHEMA integration;
ALTER TABLE public.audit_logs                      SET SCHEMA integration;
ALTER TABLE public.crm_inbound_events              SET SCHEMA integration;
ALTER TABLE public.crm_outbound_jobs               SET SCHEMA integration;

-- 4. Drop old version table only after confirming integration schema has the record
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM integration.integration_api_alembic_version) THEN
    DROP TABLE IF EXISTS public.integration_api_alembic_version;
  ELSE
    RAISE EXCEPTION 'integration.integration_api_alembic_version is empty — deploy alembic.ini and env.py changes before running this migration';
  END IF;
END $$;
```

---

## Execution Order on Server

1. **Backup**
   ```bash
   docker exec implementation-postgres-1 pg_dump -U etl_user etl_db > etl_db_backup_$(date +%Y%m%d).sql
   ```

2. **Deploy ETL code changes** (models.py, alembic.ini, env.py, stage.py, upsert.py) — redeploy container

3. **Run ETL migration**
   ```bash
   docker exec implementation-worker-etl-1 alembic upgrade head
   ```

4. **Deploy integration-api code changes** (models, alembic.ini, env.py) — redeploy container

5. **Run integration-api migration**
   ```bash
   docker exec integration-api alembic upgrade head
   ```

6. **Restart all containers**
   ```bash
   docker compose restart
   ```

7. **Verify**
   ```sql
   SELECT schemaname, tablename FROM pg_tables
   WHERE schemaname IN ('etl', 'integration')
   ORDER BY schemaname, tablename;
   ```

No data is lost. `ALTER TABLE ... SET SCHEMA` is a DDL rename — it moves the table without copying data.

---

## Out of Scope

- Moving either app to a separate database
- Changing the `cnpj_db` (already correctly uses named schemas)
- Any changes to the CNPJ service
- Modifying business logic or ETL pipeline steps
