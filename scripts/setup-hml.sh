#!/usr/bin/env bash
# =============================================================
# setup-hml.sh — Sobe o ambiente HML local com dados de prod
#
# Uso:
#   bash scripts/setup-hml.sh           # dump + restore + sobe tudo
#   bash scripts/setup-hml.sh --no-dump # só sobe (dados já existem)
# =============================================================

set -euo pipefail

COMPOSE="docker-compose -f docker-compose.hml.yml --env-file .env.hml"
DUMP_FILE="tmp/prod_dump.dump"
LOCAL_DB="etl_hml"
LOCAL_USER="postgres"

NO_DUMP=false
for arg in "$@"; do
  [[ "$arg" == "--no-dump" ]] && NO_DUMP=true
done

mkdir -p tmp

echo ""
echo "========================================"
echo " ETL — Ambiente HML Local"
echo "========================================"
echo ""

# ── 1. Recria infra do zero (garante usuário correto) ────────
echo "[1/5] Subindo infra local (postgres, redis, minio)..."
if [ "$NO_DUMP" = false ]; then
  # Remove volumes para garantir postgres inicializa com usuário correto
  $COMPOSE down -v --remove-orphans 2>/dev/null || true
else
  $COMPOSE down --remove-orphans 2>/dev/null || true
fi
$COMPOSE up -d postgres redis minio
echo "      Aguardando postgres ficar saudável..."
for i in $(seq 1 30); do
  $COMPOSE exec -T postgres pg_isready -U "$LOCAL_USER" -q && break || sleep 2
done
echo "      ✓ Postgres pronto"
echo ""

if [ "$NO_DUMP" = false ]; then
  # ── 2. Dump via Python local (SSH → VPS → SFTP) ───────────
  echo "[2/5] Fazendo dump do banco de produção via VPS..."
  python scripts/dump_via_vps.py
  echo ""

  # ── 3. Restaura no banco local ────────────────────────────
  echo "[3/5] Restaurando no banco local..."

  # Cria banco + extensões + stub auth
  $COMPOSE exec -T postgres psql -U "$LOCAL_USER" -d postgres <<SQL
DROP DATABASE IF EXISTS $LOCAL_DB;
CREATE DATABASE $LOCAL_DB;
SQL

  $COMPOSE exec -T postgres psql -U "$LOCAL_USER" -d "$LOCAL_DB" <<'SQL'
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS auth;
CREATE TABLE IF NOT EXISTS auth.users (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  email text
);
SQL

  # Copia dump para o container e restaura
  # MSYS_NO_PATHCONV=1 evita Git Bash converter tmp/ → AppData/Local/Temp
  POSTGRES_CONTAINER=$($COMPOSE ps -q postgres | head -1)
  DUMP_ABS="$(pwd -W)/tmp/prod_dump.dump"
  MSYS_NO_PATHCONV=1 docker cp "$DUMP_ABS" "$POSTGRES_CONTAINER:/tmp/prod_dump.dump"
  MSYS_NO_PATHCONV=1 docker exec "$POSTGRES_CONTAINER" pg_restore \
    -U "$LOCAL_USER" \
    -d "$LOCAL_DB" \
    --no-owner \
    --no-acl \
    -j 2 \
    /tmp/prod_dump.dump 2>&1 | grep -v "already exists" | grep -v "^$" || true

  echo "      ✓ Dados restaurados"
  echo ""
else
  echo "[2/5] Pulando dump (--no-dump)"
  echo "[3/5] Pulando restauração (--no-dump)"
  echo ""
fi

# ── 4. Build + Migrations Alembic ────────────────────────
echo "[4/5] Build das imagens + migrations Alembic..."
$COMPOSE build api worker-etl
$COMPOSE run --rm api alembic upgrade head
echo "      ✓ Migrations aplicadas"
echo ""

# ── 5. Sobe api + worker ──────────────────────────────────
echo "[5/5] Subindo API e Worker..."
$COMPOSE up -d api worker-etl
echo ""

echo "========================================"
echo " ✅  Ambiente HML pronto!"
echo "========================================"
echo ""
echo "  API:           http://localhost:8100"
echo "  MinIO console: http://localhost:9101"
echo "                 usuário: minioadmin"
echo "                 senha:   minioadmin_hml_2026"
echo "  Postgres:      localhost:5433 / DB: $LOCAL_DB / user: $LOCAL_USER"
echo ""
echo "  Logs:  docker-compose -f docker-compose.hml.yml logs -f"
echo "  Parar: docker-compose -f docker-compose.hml.yml down"
echo ""
