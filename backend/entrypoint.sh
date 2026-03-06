#!/bin/bash
# ══════════════════════════════════════════════════════════
# GeoRiesgo Perú — Entrypoint v4.0
# ══════════════════════════════════════════════════════════
set -euo pipefail

echo ""
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║  GeoRiesgo Perú — Backend v4.0                     ║"
echo "  ║  FastAPI + PostgreSQL/PostGIS                      ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo "  ENV: ${APP_ENV:-production}"
echo ""

# ── Variables ──────────────────────────────────────────────
DB_HOST="${DB_HOST:-db}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-georiesgo}"
DB_USER="${POSTGRES_USER:-georiesgo}"
DB_PASS="${DB_PASSWORD:-georiesgo_secret}"
export PGPASSWORD="${DB_PASS}"

# ── Esperar PostGIS ────────────────────────────────────────
echo "  Esperando PostgreSQL en ${DB_HOST}:${DB_PORT}..."
TRIES=0
MAX=60
until pg_isready -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -q 2>/dev/null; do
    TRIES=$((TRIES+1))
    if [ "$TRIES" -ge "$MAX" ]; then
        echo "  ✗ PostgreSQL no responde tras ${MAX} intentos — abortando"
        exit 1
    fi
    printf "  ... %d/%d\r" "$TRIES" "$MAX"
    sleep 3
done
echo "  ✓ PostgreSQL listo                              "
echo ""

# ── Verificar tablas mínimas ────────────────────────────────
SISMOS_N=$(
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -t -A -c "SELECT COUNT(*) FROM sismos;" 2>/dev/null || echo "0"
)
SISMOS_N="${SISMOS_N//[[:space:]]/}"
FALLAS_N=$(
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -t -A -c "SELECT COUNT(*) FROM fallas;" 2>/dev/null || echo "0"
)
FALLAS_N="${FALLAS_N//[[:space:]]/}"

echo "  Registros en BD: sismos=${SISMOS_N}  fallas=${FALLAS_N}"

# ── ETL inicial o forzado ──────────────────────────────────
if [ "${SISMOS_N}" = "0" ] || [ "${FORCE_SYNC:-0}" = "1" ]; then
    echo ""
    echo "  ► Iniciando ETL v4.0 (primera carga o FORCE_SYNC=1)"
    echo "    Cobertura: Perú completo — fuentes: USGS·IGP·ANA·OSM"
    echo "    Tiempo estimado: 5-15 min según conexión"
    echo ""

    if python /app/procesar_datos.py; then
        echo "  ✓ ETL completado exitosamente"
    else
        echo "  ⚠ ETL terminó con errores parciales — la API iniciará de todos modos"
        echo "    Para reintentar: docker exec georiesgo_api python etl.py --force"
    fi
else
    echo "  ✓ BD con datos — omitiendo ETL"
    echo "    Para re-sincronizar: docker exec georiesgo_api python etl.py --force"
    echo "    Para una sola capa: docker exec georiesgo_api python etl.py --solo sismos"
fi

echo ""
echo "  ► Iniciando FastAPI"
echo "  ► Endpoints: http://0.0.0.0:8000/"
echo "  ► Docs:      http://0.0.0.0:8000/docs"
echo ""

WORKERS="${WORKERS:-2}"

if [ "${APP_ENV:-production}" = "development" ]; then
    exec uvicorn main:app \
        --host 0.0.0.0 \
        --port 8000 \
        --reload \
        --reload-dir /app \
        --log-level debug
else
    exec uvicorn main:app \
        --host 0.0.0.0 \
        --port 8000 \
        --workers "${WORKERS}" \
        --loop asyncio \
        --log-level info \
        --proxy-headers \
        --forwarded-allow-ips "*"
fi