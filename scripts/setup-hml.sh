#!/usr/bin/env bash
# =============================================================
# setup-hml.sh — Sobe o ambiente HML local
# Banco: Supabase branch "teste" (externo)
# Redis + MinIO: containers Docker locais
#
# Uso:
#   bash scripts/setup-hml.sh
# =============================================================

set -euo pipefail

COMPOSE="docker compose -f infra/docker-compose.hml.yml --env-file .env.hml"

mkdir -p tmp

echo ""
echo "========================================"
echo " ETL — Ambiente HML"
echo "========================================"
echo ""

# ── 1. Sobe redis e minio ─────────────────────────────────────
echo "[1/3] Subindo Redis e MinIO..."
$COMPOSE down --remove-orphans 2>/dev/null || true
$COMPOSE up -d redis minio
echo "      Aguardando serviços ficarem saudáveis..."
for i in $(seq 1 20); do
  $COMPOSE exec -T minio curl -sf http://localhost:9000/minio/health/live && break || sleep 2
done
echo "      ✓ Redis e MinIO prontos"
echo ""

# ── 2. Build + Migrations Alembic ────────────────────────────
echo "[2/3] Build das imagens + migrations Alembic..."
$COMPOSE build api worker-etl
$COMPOSE run --rm api alembic upgrade head
echo "      ✓ Migrations aplicadas no Supabase HML"
echo ""

# ── 3. Sobe api + worker ──────────────────────────────────────
echo "[3/3] Subindo API e Worker..."
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
echo "  Banco:         Supabase branch 'teste'"
echo "                 db.xentrwrpnhcrunvupmhx.supabase.co"
echo ""
echo "  Logs:  docker compose -f infra/docker-compose.hml.yml logs -f"
echo "  Parar: docker compose -f infra/docker-compose.hml.yml down"
echo ""
