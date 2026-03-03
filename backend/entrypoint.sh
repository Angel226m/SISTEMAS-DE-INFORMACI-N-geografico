#!/bin/bash
set -e

echo ""
echo "  ╔══════════════════════════════════════════════╗"
echo "  ║  GeoRiesgo Ica — Backend v3.0               ║"
echo "  ║  FastAPI + PostgreSQL/PostGIS               ║"
echo "  ╚══════════════════════════════════════════════╝"
echo "  ENV: ${APP_ENV:-production}"
echo ""

# ── Variables de conexión ──────────────────────────────────
DB_HOST="${DB_HOST:-db}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-georiesgo}"
DB_USER="${POSTGRES_USER:-georiesgo}"
DB_PASS="${DB_PASSWORD:-georiesgo_secret}"

export PGPASSWORD="${DB_PASS}"

# ── Esperar a que PostGIS esté listo ──────────────────────
echo "  Esperando PostgreSQL en ${DB_HOST}:${DB_PORT}..."
MAX_TRIES=40
TRIES=0
until pg_isready -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" -q 2>/dev/null; do
    TRIES=$((TRIES+1))
    if [ $TRIES -ge $MAX_TRIES ]; then
        echo "  ✗ PostgreSQL no responde tras ${MAX_TRIES} intentos"
        exit 1
    fi
    printf "  ... %d/%d\r" "$TRIES" "$MAX_TRIES"
    sleep 3
done
echo "  ✓ PostgreSQL listo                    "
echo ""

# ── Verificar conteo de sismos en la BD ───────────────────
SISMOS_COUNT=$(
    psql \
        -h "${DB_HOST}" \
        -p "${DB_PORT}" \
        -U "${DB_USER}" \
        -d "${DB_NAME}" \
        -t -A \
        -c "SELECT COUNT(*) FROM sismos;" \
        2>/dev/null \
    || echo "0"
)

SISMOS_COUNT=$(echo "${SISMOS_COUNT}" | tr -d '[:space:]')
echo "  Sismos en BD: ${SISMOS_COUNT}"

# ── Lanzar ETL si la BD está vacía o se fuerza ────────────
if [ "${SISMOS_COUNT}" = "0" ] || [ "${FORCE_SYNC:-0}" = "1" ]; then
    echo ""
    echo "  ► Iniciando ETL (primera carga o FORCE_SYNC=1)..."
    echo "    Esto puede tardar 3-10 minutos descargando de USGS."
    echo ""

    if python /app/procesar_datos.py; then
        echo ""
        echo "  ✓ ETL completado"
    else
        echo ""
        echo "  ⚠ ETL terminó con errores parciales."
        echo "    El servidor arranca de todos modos con los datos disponibles."
        echo "    Para reintentar: docker exec georiesgo_api python procesar_datos.py --force"
    fi
else
    echo "  ✓ BD con datos — omitiendo ETL"
    echo "    Para re-sincronizar: docker exec georiesgo_api python procesar_datos.py --force"
fi

echo ""
echo "  ► Iniciando FastAPI en http://0.0.0.0:8000"
echo "  ► Docs: http://0.0.0.0:8000/docs"
echo ""

if [ "${APP_ENV}" = "development" ]; then
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
        --workers 2 \
        --loop asyncio \
        --log-level info
fi