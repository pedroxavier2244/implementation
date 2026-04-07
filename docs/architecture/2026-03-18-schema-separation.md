# Schema Separation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move ETL tables to schema `etl` and integration-api tables to schema `integration` within `etl_db`, eliminating the mixed `public` schema.

**Architecture:** Two Alembic migrations (one per repo) use `ALTER TABLE ... SET SCHEMA` — a DDL rename with no data movement. The PostgreSQL role gets a persistent `search_path = etl, integration, public` so bare table names in all raw SQL continue to resolve without rewriting SQL statements. Code changes touch only ORM model declarations and the two files that query `information_schema` directly.

**Tech Stack:** Python 3, SQLAlchemy (sync ORM in etl-system, async ORM in integration-api), Alembic, PostgreSQL 16, pandas

**Spec:** `docs/superpowers/specs/2026-03-18-schema-separation-design.md`

---

## Files Changed

### etl-system
| File | Change |
|---|---|
| `shared/models.py` | Add `schema="etl"` to all models; schema-qualify all `ForeignKey()` strings |
| `alembic.ini` | Add `version_table` and `version_table_schema` |
| `migrations/env.py` | Add `version_table`, `version_table_schema`, `include_schemas` to both configure calls |
| `worker/steps/stage.py` | Add `schema="etl"` to `df.to_sql()` |
| `worker/steps/upsert.py` | Add `AND table_schema = 'etl'` to `information_schema.columns` query |
| `scripts/backfill_change_history.py` | Add `AND table_schema = 'etl'`; schema-qualify `to_regclass()` |
| `migrations/versions/20260318_000012_move_to_etl_schema.py` | New migration: CREATE SCHEMA etl, move tables, grants, search_path |

### integration-api
| File | Change |
|---|---|
| `app/models/user.py` | Add `schema="integration"` |
| `app/models/refresh_token.py` | Add `schema="integration"` |
| `app/models/password_reset_token.py` | Add `schema="integration"` |
| `app/models/audit_log.py` | Add `schema="integration"` |
| `app/models/crm_event.py` | Add `schema="integration"` to both models |
| `app/models/visao_cliente.py` | Change `schema` to `"etl"` (read-only) |
| `app/models/visao_cliente_change_history.py` | Add `schema="etl"` (read-only) |
| `app/models/etl_file.py` | Add `schema="etl"` (read-only) |
| `alembic.ini` | Add `version_table_schema = integration` |
| `alembic/env.py` | Add `version_table_schema`, `include_schemas`; replace `include_object` to filter by schema |
| `alembic/versions/003_move_to_integration_schema.py` | New migration: CREATE SCHEMA integration, move tables, grants |

---

## Task 1: Update `shared/models.py` — schema and ForeignKey strings

**Files:**
- Modify: `shared/models.py`
- Test: `tests/unit/test_models.py`

- [ ] **Step 1: Write a failing test that checks schema assignment on ORM metadata**

Add to `tests/unit/test_models.py`:

```python
def test_all_etl_models_use_etl_schema():
    """All ORM models managed by etl-system must declare schema='etl'."""
    from shared.models import (
        EtlFile, EtlJobRun, EtlJobStep, EtlBadRow,
        AlertEvent, AlertEventChannel,
        AnalyticsIndicatorSnapshot, VisaoClienteChangeHistory,
    )
    models = [
        EtlFile, EtlJobRun, EtlJobStep, EtlBadRow,
        AlertEvent, AlertEventChannel,
        AnalyticsIndicatorSnapshot, VisaoClienteChangeHistory,
    ]
    for model in models:
        assert model.__table__.schema == "etl", (
            f"{model.__name__} must have schema='etl', got: {model.__table__.schema}"
        )


def test_foreign_keys_are_schema_qualified():
    """All ForeignKey references must use 'etl.<table>' format."""
    from shared.models import (
        EtlJobRun, EtlJobStep, EtlBadRow,
        AlertEventChannel, AnalyticsIndicatorSnapshot, VisaoClienteChangeHistory,
    )
    from sqlalchemy import inspect as sa_inspect

    expected_fks = {
        "EtlJobRun": {"etl.etl_file.id"},
        "EtlJobStep": {"etl.etl_job_run.id"},
        "EtlBadRow": {"etl.etl_job_run.id"},
        "AlertEventChannel": {"etl.alert_event.id"},
        "AnalyticsIndicatorSnapshot": {"etl.etl_job_run.id", "etl.etl_file.id"},
        "VisaoClienteChangeHistory": {"etl.etl_job_run.id", "etl.etl_file.id"},
    }
    for model_cls in [EtlJobRun, EtlJobStep, EtlBadRow, AlertEventChannel,
                      AnalyticsIndicatorSnapshot, VisaoClienteChangeHistory]:
        mapper = sa_inspect(model_cls)
        actual = {
            fk.target_fullname
            for col in mapper.columns
            for fk in col.foreign_keys
        }
        assert actual == expected_fks[model_cls.__name__], (
            f"{model_cls.__name__} FK mismatch: {actual}"
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "C:\Users\MB NEGOCIOS\etl-system"
python -m pytest tests/unit/test_models.py::test_all_etl_models_use_etl_schema tests/unit/test_models.py::test_foreign_keys_are_schema_qualified -v
```

Expected: FAIL — `EtlFile must have schema='etl', got: None`

- [ ] **Step 3: Update `shared/models.py`**

`EtlFile` already has a tuple `__table_args__`. Convert all models to include `{"schema": "etl"}`:

```python
# EtlFile — has existing UniqueConstraint, so use tuple form:
class EtlFile(Base):
    __tablename__ = "etl_file"
    __table_args__ = (
        UniqueConstraint("file_date", "hash_sha256", name="uq_file_date_hash"),
        {"schema": "etl"},
    )

# All others — use dict form:
class EtlJobRun(Base):
    __tablename__ = "etl_job_run"
    __table_args__ = {"schema": "etl"}
    ...
    file_id = Column(String(36), ForeignKey("etl.etl_file.id"), nullable=False)

class EtlJobStep(Base):
    __tablename__ = "etl_job_step"
    __table_args__ = {"schema": "etl"}
    ...
    job_id = Column(String(36), ForeignKey("etl.etl_job_run.id"), nullable=False)

class EtlBadRow(Base):
    __tablename__ = "etl_bad_rows"
    __table_args__ = {"schema": "etl"}
    ...
    job_id = Column(String(36), ForeignKey("etl.etl_job_run.id"), nullable=False)

class AlertEvent(Base):
    __tablename__ = "alert_event"
    __table_args__ = {"schema": "etl"}
    # (no FK changes needed)

class AlertEventChannel(Base):
    __tablename__ = "alert_event_channel"
    __table_args__ = {"schema": "etl"}
    ...
    alert_id = Column(String(36), ForeignKey("etl.alert_event.id"), nullable=False)

class AnalyticsIndicatorSnapshot(Base):
    __tablename__ = "analytics_indicator_snapshot"
    __table_args__ = {"schema": "etl"}
    ...
    job_id = Column(String(36), ForeignKey("etl.etl_job_run.id"), nullable=False)
    file_id = Column(String(36), ForeignKey("etl.etl_file.id"))

class VisaoClienteChangeHistory(Base):
    __tablename__ = "visao_cliente_change_history"
    __table_args__ = {"schema": "etl"}
    ...
    etl_job_id = Column(String(36), ForeignKey("etl.etl_job_run.id"), nullable=False)
    file_id = Column(String(36), ForeignKey("etl.etl_file.id"))
```

- [ ] **Step 4: Run test to verify it passes**

```bash
python -m pytest tests/unit/test_models.py::test_all_etl_models_use_etl_schema tests/unit/test_models.py::test_foreign_keys_are_schema_qualified -v
```

Expected: PASS

- [ ] **Step 5: Run full unit test suite to confirm nothing broke**

```bash
python -m pytest tests/unit/ -v
```

Expected: all existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add shared/models.py tests/unit/test_models.py
git commit -m "feat: add schema='etl' to all ORM models and qualify ForeignKey strings"
```

---

## Task 2: Update `alembic.ini` and `migrations/env.py`

**Files:**
- Modify: `alembic.ini`
- Modify: `migrations/env.py`

- [ ] **Step 1: Update `alembic.ini`**

Add after line 3 (`script_location = migrations`):

```ini
version_table = alembic_version
version_table_schema = etl
```

Result:
```ini
[alembic]
script_location = migrations
prepend_sys_path = .
version_table = alembic_version
version_table_schema = etl
sqlalchemy.url = sqlite:///./dev.db
```

- [ ] **Step 2: Update `migrations/env.py`**

Replace the two `context.configure(...)` calls to add the three new parameters:

```python
def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table="alembic_version",
        version_table_schema="etl",
        include_schemas=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table="alembic_version",
            version_table_schema="etl",
            include_schemas=True,
        )
        with context.begin_transaction():
            context.run_migrations()
```

- [ ] **Step 3: Verify Alembic can still read its config (no syntax errors)**

```bash
python -m alembic current
```

Expected: prints current revision (e.g. `20260318_000011`) with no errors

- [ ] **Step 4: Commit**

```bash
git add alembic.ini migrations/env.py
git commit -m "feat: configure alembic to use etl schema for version tracking"
```

---

## Task 3: Fix `worker/steps/stage.py` and `worker/steps/upsert.py`

**Files:**
- Modify: `worker/steps/stage.py:30`
- Modify: `worker/steps/upsert.py:146-154`

- [ ] **Step 1: Fix `stage.py` — add `schema="etl"` to `to_sql()`**

At line 30, change:
```python
    df_to_insert.to_sql(
        STAGING_TABLE,
        con=session.get_bind(),
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
```
To:
```python
    df_to_insert.to_sql(
        STAGING_TABLE,
        con=session.get_bind(),
        schema="etl",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
```

- [ ] **Step 2: Fix `upsert.py` — add `table_schema` filter**

At lines 146-154, change:
```python
    result = session.execute(
        text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = :table_name "
            "ORDER BY ordinal_position"
        ),
        {"table_name": STAGING_TABLE},
    )
```
To:
```python
    result = session.execute(
        text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = :table_name "
            "  AND table_schema = 'etl' "
            "ORDER BY ordinal_position"
        ),
        {"table_name": STAGING_TABLE},
    )
```

- [ ] **Step 3: Run ETL step tests to confirm no regression**

```bash
python -m pytest tests/unit/test_etl_steps.py -v
```

Expected: all pass

- [ ] **Step 4: Commit**

```bash
git add worker/steps/stage.py worker/steps/upsert.py
git commit -m "feat: schema-qualify staging table access in stage and upsert steps"
```

---

## Task 4: Fix `scripts/backfill_change_history.py`

**Files:**
- Modify: `scripts/backfill_change_history.py:62-72` and `:116-120`

- [ ] **Step 1: Fix `_load_staging_columns` — add `table_schema` filter**

At lines 62-72, change:
```python
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
            """
        ),
```
To:
```python
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = :table_name
              AND table_schema = 'etl'
            ORDER BY ordinal_position
            """
        ),
```

- [ ] **Step 2: Fix `_history_table_exists` — schema-qualify `to_regclass()`**

At lines 116-120, change:
```python
def _history_table_exists(session) -> bool:
    return session.execute(
        text("SELECT to_regclass(:table_name)"),
        {"table_name": HISTORY_TABLE},
    ).scalar_one() is not None
```
To:
```python
def _history_table_exists(session) -> bool:
    return session.execute(
        text("SELECT to_regclass('etl.visao_cliente_change_history')"),
    ).scalar_one() is not None
```

- [ ] **Step 3: Commit**

```bash
git add scripts/backfill_change_history.py
git commit -m "feat: fix schema references in backfill script for etl schema"
```

---

## Task 5: Write Alembic migration `000012_move_to_etl_schema`

**Files:**
- Create: `migrations/versions/20260318_000012_move_to_etl_schema.py`

- [ ] **Step 1: Create the migration file**

```python
"""move all etl tables to etl schema

Revision ID: 20260318_000012
Revises: 20260318_000011
Create Date: 2026-03-18 00:00:12
"""

from typing import Sequence, Union
from alembic import op

revision: str = "20260318_000012"
down_revision: Union[str, None] = "20260318_000011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create schema
    op.execute("CREATE SCHEMA IF NOT EXISTS etl")

    # 2. Grant permissions to app role (tables + sequences for autoincrement)
    op.execute("GRANT USAGE ON SCHEMA etl TO etl_user")
    op.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl TO etl_user")
    op.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA etl TO etl_user")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT ALL ON TABLES TO etl_user")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA etl GRANT USAGE, SELECT ON SEQUENCES TO etl_user")

    # 3. Set permanent search_path for the role
    # The integration schema does not exist yet — PostgreSQL silently ignores
    # nonexistent schemas in search_path, so this is safe.
    # Constraint: no table name may exist in both etl and integration schemas.
    op.execute("ALTER ROLE etl_user SET search_path = etl, integration, public")

    # 4. Move ETL tables (no specific ordering needed — SET SCHEMA does not drop FK constraints)
    op.execute("ALTER TABLE public.etl_file                     SET SCHEMA etl")
    op.execute("ALTER TABLE public.etl_job_run                  SET SCHEMA etl")
    op.execute("ALTER TABLE public.etl_job_step                 SET SCHEMA etl")
    op.execute("ALTER TABLE public.etl_bad_rows                 SET SCHEMA etl")
    op.execute("ALTER TABLE public.staging_visao_cliente        SET SCHEMA etl")
    op.execute("ALTER TABLE public.final_visao_cliente          SET SCHEMA etl")
    op.execute("ALTER TABLE public.visao_cliente_change_history SET SCHEMA etl")
    op.execute("ALTER TABLE public.analytics_indicator_snapshot SET SCHEMA etl")
    op.execute("ALTER TABLE public.alert_event                  SET SCHEMA etl")
    op.execute("ALTER TABLE public.alert_event_channel          SET SCHEMA etl")

    # 5. Drop public.alembic_version only after confirming etl.alembic_version is populated.
    # If alembic.ini/env.py were NOT deployed before this migration, this raises an error
    # instead of silently leaving the system in an inconsistent state.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM etl.alembic_version) THEN
                DROP TABLE IF EXISTS public.alembic_version;
            ELSE
                RAISE EXCEPTION 'etl.alembic_version is empty — deploy alembic.ini and env.py changes before running this migration';
            END IF;
        END $$
    """)


def downgrade() -> None:
    # Recreate public.alembic_version and restore the current revision
    # (Alembic will have dropped it after the downgrade records the revision)
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.alembic_version (
            version_num VARCHAR(32) NOT NULL PRIMARY KEY
        )
    """)
    op.execute("INSERT INTO public.alembic_version SELECT version_num FROM etl.alembic_version")

    # Drop etl.alembic_version — Alembic will have stamped it with the downgraded revision;
    # leaving it would cause alembic current to find the record in etl schema even after
    # alembic.ini has been reverted to use public.
    op.execute("DROP TABLE IF EXISTS etl.alembic_version")

    # Move all tables back to public
    op.execute("ALTER TABLE etl.alert_event_channel          SET SCHEMA public")
    op.execute("ALTER TABLE etl.alert_event                  SET SCHEMA public")
    op.execute("ALTER TABLE etl.analytics_indicator_snapshot SET SCHEMA public")
    op.execute("ALTER TABLE etl.visao_cliente_change_history SET SCHEMA public")
    op.execute("ALTER TABLE etl.final_visao_cliente          SET SCHEMA public")
    op.execute("ALTER TABLE etl.staging_visao_cliente        SET SCHEMA public")
    op.execute("ALTER TABLE etl.etl_bad_rows                 SET SCHEMA public")
    op.execute("ALTER TABLE etl.etl_job_step                 SET SCHEMA public")
    op.execute("ALTER TABLE etl.etl_job_run                  SET SCHEMA public")
    op.execute("ALTER TABLE etl.etl_file                     SET SCHEMA public")

    # Revoke grants and reset role
    op.execute("REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA etl FROM etl_user")
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA etl FROM etl_user")
    op.execute("REVOKE USAGE ON SCHEMA etl FROM etl_user")
    op.execute("ALTER ROLE etl_user SET search_path = DEFAULT")
    op.execute("DROP SCHEMA IF EXISTS etl")
```

- [ ] **Step 2: Verify Alembic can parse the new migration**

```bash
python -m alembic heads
```

Expected: shows `20260318_000012` as the new head

- [ ] **Step 3: Commit**

```bash
git add migrations/versions/20260318_000012_move_to_etl_schema.py
git commit -m "feat: migration 000012 — move etl tables to etl schema"
```

---

## Task 6: Update integration-api models

**Files:**
- Modify: `app/models/user.py`
- Modify: `app/models/refresh_token.py`
- Modify: `app/models/password_reset_token.py`
- Modify: `app/models/audit_log.py`
- Modify: `app/models/crm_event.py`
- Modify: `app/models/visao_cliente.py`
- Modify: `app/models/visao_cliente_change_history.py`
- Modify: `app/models/etl_file.py`

Working directory for this task: `C:\Users\MB NEGOCIOS\integration-api`

- [ ] **Step 1: Write a failing test**

Create `tests/unit/test_models_schema.py`:

```python
def test_integration_owned_models_use_integration_schema():
    from app.models.user import User
    from app.models.refresh_token import RefreshToken
    from app.models.password_reset_token import PasswordResetToken
    from app.models.audit_log import AuditLog
    from app.models.crm_event import CrmInboundEvent, CrmOutboundJob

    for model in [User, RefreshToken, PasswordResetToken, AuditLog,
                  CrmInboundEvent, CrmOutboundJob]:
        assert model.__table__.schema == "integration", (
            f"{model.__name__} must have schema='integration', got: {model.__table__.schema}"
        )


def test_etl_read_only_models_use_etl_schema():
    from app.models.visao_cliente import VisaoCliente
    from app.models.visao_cliente_change_history import VisaoClienteChangeHistory
    from app.models.etl_file import EtlFile

    for model in [VisaoCliente, VisaoClienteChangeHistory, EtlFile]:
        assert model.__table__.schema == "etl", (
            f"{model.__name__} must have schema='etl', got: {model.__table__.schema}"
        )


def test_integration_foreign_keys_are_schema_qualified():
    """ForeignKey strings in integration models must use 'integration.<table>' format."""
    from sqlalchemy import inspect as sa_inspect
    from app.models.user import User
    from app.models.refresh_token import RefreshToken
    from app.models.audit_log import AuditLog

    expected_fks = {
        "User": {"integration.users.id"},          # gestor_id (self-referential)
        "RefreshToken": {"integration.users.id"},
        "AuditLog": {"integration.users.id"},
    }
    for model_cls in [User, RefreshToken, AuditLog]:
        mapper = sa_inspect(model_cls)
        actual = {
            fk.target_fullname
            for col in mapper.columns
            for fk in col.foreign_keys
        }
        assert actual == expected_fks[model_cls.__name__], (
            f"{model_cls.__name__} FK mismatch: {actual}"
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd "C:\Users\MB NEGOCIOS\integration-api"
python -m pytest tests/unit/test_models_schema.py -v
```

Expected: FAIL

- [ ] **Step 3: Add `schema="integration"` to owned models AND qualify ForeignKey strings**

For each of the 6 owned models, add `__table_args__`. Examples:

`app/models/user.py` — add after `__tablename__`, and update the self-referential FK:
```python
__table_args__ = {"schema": "integration"}
# line 33: change ForeignKey("users.id", ...) → ForeignKey("integration.users.id", ...)
```

`app/models/refresh_token.py` — add schema and update FK:
```python
__table_args__ = {"schema": "integration"}
# line 25: change ForeignKey("users.id", ondelete="CASCADE") → ForeignKey("integration.users.id", ondelete="CASCADE")
```

`app/models/audit_log.py` — add schema and update FK:
```python
__table_args__ = {"schema": "integration"}
# line 27: change ForeignKey("users.id", ondelete="SET NULL") → ForeignKey("integration.users.id", ondelete="SET NULL")
```

`app/models/crm_event.py` — both `CrmInboundEvent` and `CrmOutboundJob` get (no FK changes needed):
```python
__table_args__ = {"schema": "integration"}
```

`app/models/password_reset_token.py` — add schema (check for any FK to users and qualify if present):
```python
__table_args__ = {"schema": "integration"}
```

- [ ] **Step 4: Add `schema="etl"` to read-only models**

`app/models/visao_cliente.py` — add/update `__table_args__`:
```python
__table_args__ = {"schema": "etl"}
```

`app/models/visao_cliente_change_history.py`:
```python
__table_args__ = {"schema": "etl"}
```

`app/models/etl_file.py`:
```python
__table_args__ = {"schema": "etl"}
```

- [ ] **Step 5: Run test to verify it passes**

```bash
python -m pytest tests/unit/test_models_schema.py -v
```

Expected: all 3 tests PASS

- [ ] **Step 6: Run full unit test suite**

```bash
python -m pytest tests/unit/ -v
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add app/models/ tests/unit/test_models_schema.py
git commit -m "feat: add schema declarations to integration-api models (etl + integration schemas)"
```

---

## Task 7: Update integration-api Alembic config and write migration

**Files:**
- Modify: `alembic.ini`
- Modify: `alembic/env.py`
- Create: `alembic/versions/003_move_to_integration_schema.py`

- [ ] **Step 1: Update `alembic.ini`**

In the `[alembic]` section, add after `version_table`:
```ini
version_table_schema = integration
```

The file already has `version_table = integration_api_alembic_version`. Final `[alembic]` block:
```ini
[alembic]
script_location = alembic
prepend_sys_path = .
version_path_separator = os
version_table = integration_api_alembic_version
version_table_schema = integration
sqlalchemy.url = driver://user:pass@localhost/dbname
```

- [ ] **Step 2: Update `alembic/env.py`**

**Change 1** — Delete the `EXCLUDE_TABLES` set (lines 43-53) AND the `include_object` function (lines 56-59) in their entirety. Replace both with the new schema-based function:

```python
# Tables in the etl schema — ignore in autogenerate (read-only for integration-api)
def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and getattr(object, "schema", None) == "etl":
        return False
    return True
```

**Change 2** — Add `version_table_schema` and `include_schemas` to both configure calls:

In `run_migrations_offline()`:
```python
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        version_table="integration_api_alembic_version",
        version_table_schema="integration",
        include_schemas=True,
    )
```

In `do_run_migrations()`:
```python
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=include_object,
        version_table="integration_api_alembic_version",
        version_table_schema="integration",
        include_schemas=True,
    )
```

- [ ] **Step 3: Verify Alembic can parse config**

```bash
python -m alembic current
```

Expected: prints `002` with no errors

- [ ] **Step 4: Create migration `003_move_to_integration_schema.py`**

```python
"""move integration tables to integration schema

Revision ID: 003
Revises: 002
Create Date: 2026-03-18
"""

from typing import Sequence, Union
from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Create schema
    op.execute("CREATE SCHEMA IF NOT EXISTS integration")

    # 2. Grant permissions (tables + sequences)
    op.execute("GRANT USAGE ON SCHEMA integration TO etl_user")
    op.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA integration TO etl_user")
    op.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration TO etl_user")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA integration GRANT ALL ON TABLES TO etl_user")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA integration GRANT USAGE, SELECT ON SEQUENCES TO etl_user")

    # 3. Move integration tables
    op.execute("ALTER TABLE public.users                  SET SCHEMA integration")
    op.execute("ALTER TABLE public.refresh_tokens         SET SCHEMA integration")
    op.execute("ALTER TABLE public.password_reset_tokens  SET SCHEMA integration")
    op.execute("ALTER TABLE public.audit_logs             SET SCHEMA integration")
    op.execute("ALTER TABLE public.crm_inbound_events     SET SCHEMA integration")
    op.execute("ALTER TABLE public.crm_outbound_jobs      SET SCHEMA integration")

    # 4. Drop old version table only after confirming integration schema has the record
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM integration.integration_api_alembic_version) THEN
                DROP TABLE IF EXISTS public.integration_api_alembic_version;
            ELSE
                RAISE EXCEPTION 'integration.integration_api_alembic_version is empty — deploy alembic.ini and env.py changes before running this migration';
            END IF;
        END $$
    """)


def downgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS public.integration_api_alembic_version (
            version_num VARCHAR(32) NOT NULL PRIMARY KEY
        )
    """)
    op.execute("INSERT INTO public.integration_api_alembic_version SELECT version_num FROM integration.integration_api_alembic_version")

    op.execute("ALTER TABLE integration.crm_outbound_jobs     SET SCHEMA public")
    op.execute("ALTER TABLE integration.crm_inbound_events    SET SCHEMA public")
    op.execute("ALTER TABLE integration.audit_logs            SET SCHEMA public")
    op.execute("ALTER TABLE integration.password_reset_tokens SET SCHEMA public")
    op.execute("ALTER TABLE integration.refresh_tokens        SET SCHEMA public")
    op.execute("ALTER TABLE integration.users                 SET SCHEMA public")

    op.execute("REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration FROM etl_user")
    op.execute("REVOKE ALL ON ALL TABLES IN SCHEMA integration FROM etl_user")
    op.execute("REVOKE USAGE ON SCHEMA integration FROM etl_user")
    op.execute("DROP SCHEMA IF EXISTS integration")
```

- [ ] **Step 5: Verify Alembic sees the new head**

```bash
python -m alembic heads
```

Expected: prints `003`

- [ ] **Step 6: Commit**

```bash
git add alembic.ini alembic/env.py alembic/versions/003_move_to_integration_schema.py
git commit -m "feat: migration 003 — move integration tables to integration schema"
```

---

## Task 8: Deploy on server

Working directory on server: `/opt/apps/implementation` (etl-system) and `/opt/apps/integration-api`

- [ ] **Step 1: Backup the database**

```bash
docker exec implementation-postgres-1 pg_dump -U etl_user etl_db > /root/etl_db_backup_$(date +%Y%m%d_%H%M%S).sql
echo "Backup size: $(ls -lh /root/etl_db_backup_*.sql | tail -1)"
```

- [ ] **Step 2: Deploy etl-system code to server**

```bash
cd /opt/apps/implementation
git pull origin <branch>
docker compose build api worker-etl
```

- [ ] **Step 3: Run ETL migration (creates etl schema, moves tables)**

```bash
docker compose run --rm worker-etl alembic upgrade head
```

Expected output ends with: `Running upgrade 20260318_000011 -> 20260318_000012`
No error means `etl.alembic_version` was populated and `public.alembic_version` was dropped.

- [ ] **Step 4: Verify ETL schema**

```bash
docker exec implementation-postgres-1 psql -U etl_user -d etl_db -c \
  "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'etl' ORDER BY tablename;"
```

Expected: 10 rows, all in schema `etl`

- [ ] **Step 5: Deploy integration-api code to server**

```bash
cd /opt/apps/integration-api
git pull origin <branch>
docker compose build
```

- [ ] **Step 6: Run integration-api migration (creates integration schema, moves tables)**

```bash
docker compose run --rm api alembic upgrade head
```

Expected output ends with: `Running upgrade 002 -> 003`

- [ ] **Step 7: Verify integration schema**

```bash
docker exec implementation-postgres-1 psql -U etl_user -d etl_db -c \
  "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'integration' ORDER BY tablename;"
```

Expected: 6 rows, all in schema `integration`

- [ ] **Step 8: Confirm public schema is clean**

```bash
docker exec implementation-postgres-1 psql -U etl_user -d etl_db -c \
  "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE 'pg_%';"
```

Expected: 0 rows (only system tables remain in public)

- [ ] **Step 9: Restart all containers and do smoke test**

```bash
cd /opt/apps/implementation && docker compose restart
cd /opt/apps/integration-api && docker compose restart

# Smoke test: API health
curl -s http://localhost:8000/health
curl -s http://localhost:8002/api/v1/health
```

Expected: both return `{"status": "ok"}` or similar

- [ ] **Step 10: Final commit (tag deployment)**

```bash
cd /opt/apps/implementation
git tag schema-separation-deployed-$(date +%Y%m%d)
```
