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

## Changes

### ETL System (`etl-system`)

#### `shared/models.py`
Add `__table_args__ = {"schema": "etl"}` to all 10 SQLAlchemy models:
- `EtlFile`, `EtlJobRun`, `EtlJobStep`, `EtlBadRow`
- `StagingVisaoCliente`, `FinalVisaoCliente`, `VisaoClienteChangeHistory`
- `AnalyticsIndicatorSnapshot`, `AlertEvent`, `AlertEventChannel`

#### `alembic.ini`
```ini
version_table = alembic_version
version_table_schema = etl
```

#### `migrations/env.py`
Add to both offline and online `context.configure()` calls:
```python
version_table = "alembic_version",
version_table_schema = "etl",
include_schemas = True,
```

#### New Alembic migration (`000012_move_to_etl_schema.py`)
```sql
CREATE SCHEMA IF NOT EXISTS etl;

ALTER TABLE public.etl_file                   SET SCHEMA etl;
ALTER TABLE public.etl_job_run                SET SCHEMA etl;
ALTER TABLE public.etl_job_step               SET SCHEMA etl;
ALTER TABLE public.etl_bad_rows               SET SCHEMA etl;
ALTER TABLE public.staging_visao_cliente      SET SCHEMA etl;
ALTER TABLE public.final_visao_cliente        SET SCHEMA etl;
ALTER TABLE public.visao_cliente_change_history SET SCHEMA etl;
ALTER TABLE public.analytics_indicator_snapshot SET SCHEMA etl;
ALTER TABLE public.alert_event                SET SCHEMA etl;
ALTER TABLE public.alert_event_channel        SET SCHEMA etl;
ALTER TABLE public.alembic_version            SET SCHEMA etl;
```

Downgrade reverses each `SET SCHEMA` back to `public` and drops the schema if empty.

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
version_table = "integration_api_alembic_version",
version_table_schema = "integration",
include_schemas = True,
```

Also update the `include_object` filter: ETL tables to exclude are now in schema `etl` — filter by `schema == "etl"` instead of by table name.

#### New Alembic migration (`003_move_to_integration_schema.py`)
```sql
CREATE SCHEMA IF NOT EXISTS integration;

ALTER TABLE public.users                          SET SCHEMA integration;
ALTER TABLE public.refresh_tokens                 SET SCHEMA integration;
ALTER TABLE public.password_reset_tokens          SET SCHEMA integration;
ALTER TABLE public.audit_logs                     SET SCHEMA integration;
ALTER TABLE public.crm_inbound_events             SET SCHEMA integration;
ALTER TABLE public.crm_outbound_jobs              SET SCHEMA integration;
ALTER TABLE public.integration_api_alembic_version SET SCHEMA integration;
```

Downgrade reverses each `SET SCHEMA` back to `public` and drops the schema if empty.

---

## Execution Order on Server

1. **Backup** — `pg_dump etl_db > etl_db_backup_$(date +%Y%m%d).sql`
2. **Deploy ETL migration** — `alembic upgrade head` in `etl-system` container
3. **Deploy integration-api migration** — `alembic upgrade head` in `integration-api` container
4. **Restart containers** — `docker compose restart api worker-etl` + `docker compose restart` in integration-api

No data is lost. `ALTER TABLE ... SET SCHEMA` is a DDL rename operation — it moves the table without recreating or copying data.

---

## Out of Scope

- Moving either app to a separate database
- Changing the `cnpj_db` (already correctly uses named schemas)
- Any changes to the CNPJ service
- Modifying business logic or ETL pipeline steps
