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

    # Move all tables back to public (no ordering constraint — SET SCHEMA does not drop FK constraints)
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
