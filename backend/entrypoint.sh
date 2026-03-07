#!/bin/bash
# ══════════════════════════════════════════════════════════
# GeoRiesgo Perú — Entrypoint v6.0
# ══════════════════════════════════════════════════════════
set -euo pipefail

echo ""
echo "  ╔══════════════════════════════════════════════════════════╗"
echo "  ║  GeoRiesgo Perú — Backend v6.0                         ║"
echo "  ║  FastAPI + PostgreSQL/PostGIS                          ║"
echo "  ║  Región: ST_Covers + KNN (sin NULL)                    ║"
echo "  ╚══════════════════════════════════════════════════════════╝"
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
echo "  ✓ PostgreSQL listo                                       "
echo ""

# ── Verificar extensiones PostGIS ─────────────────────────
echo "  Verificando extensión PostGIS..."
POSTGIS_OK=$(
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -t -A -c "SELECT COUNT(*) FROM pg_extension WHERE extname='postgis';" 2>/dev/null || echo "0"
)
POSTGIS_OK="${POSTGIS_OK//[[:space:]]/}"
if [ "${POSTGIS_OK}" = "0" ]; then
    echo "  ⚠ PostGIS no instalado — ejecutando init.sql..."
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -f /app/init.sql 2>&1 | tail -10
else
    echo "  ✓ PostGIS disponible"
    # Verificar y actualizar funciones v6.0 si ya existe la BD
    echo "  Verificando funciones v6.0 (ST_Covers + KNN)..."
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -c "SELECT 1 FROM pg_proc WHERE proname='f_asignar_region'" -t -A 2>/dev/null | grep -q "1" || {
        echo "  ⚠ Función f_asignar_region no encontrada — re-ejecutando init.sql"
        psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
             -f /app/init.sql 2>&1 | tail -10
    }
fi
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

DPTOS_N=$(
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -t -A -c "SELECT COUNT(*) FROM departamentos;" 2>/dev/null || echo "0"
)
DPTOS_N="${DPTOS_N//[[:space:]]/}"

INFRA_SIN_REGION=$(
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -t -A -c "SELECT COUNT(*) FROM infraestructura WHERE region IS NULL;" 2>/dev/null || echo "0"
)
INFRA_SIN_REGION="${INFRA_SIN_REGION//[[:space:]]/}"

echo "  Registros en BD:"
echo "    sismos=${SISMOS_N}  fallas=${FALLAS_N}  departamentos=${DPTOS_N}"
echo "    infra_sin_region=${INFRA_SIN_REGION} (0 = perfecto)"

# ── ETL inicial o forzado ──────────────────────────────────
if [ "${SISMOS_N}" = "0" ] || [ "${FORCE_SYNC:-0}" = "1" ]; then
    echo ""
    echo "  ► Iniciando ETL v6.0 (primera carga o FORCE_SYNC=1)"
    echo "    Pasos: departamentos → sismos → distritos → fallas →"
    echo "           inundaciones → tsunamis → deslizamientos →"
    echo "           infraestructura → estaciones → heatmap → regiones"
    echo "    PASO 10: ST_Covers + KNN — región nunca NULL"
    echo "    Tiempo estimado: 5-20 min según conexión"
    echo ""

    if python /app/procesar_datos.py; then
        echo "  ✓ ETL v6.0 completado exitosamente"
    else
        echo "  ⚠ ETL terminó con errores parciales — la API iniciará de todos modos"
        echo "    Para reintentar: docker exec georiesgo_api python procesar_datos.py --force"
    fi
else
    echo "  ✓ BD con datos — omitiendo ETL"
    echo ""
    echo "    Comandos útiles:"
    echo "      Sync completo:     docker exec georiesgo_api python procesar_datos.py --force"
    echo "      Solo sismos:       docker exec georiesgo_api python procesar_datos.py --solo sismos"
    echo "      Solo desliz:       docker exec georiesgo_api python procesar_datos.py --solo deslizamientos"
    echo "      Corregir regiones: docker exec georiesgo_api python procesar_datos.py --solo regiones"
    echo "      Refrescar mapa:    docker exec georiesgo_api python procesar_datos.py --solo heatmap"
    echo ""
    echo "    Verificar cobertura de regiones:"
    echo "      curl http://localhost:8000/api/v1/diagnostico/regiones"
fi

echo ""
echo "  ► Iniciando FastAPI v6.0"
echo "  ► Endpoints: http://0.0.0.0:8000/"
echo "  ► Docs:      http://0.0.0.0:8000/docs"
echo "  ► Diagnóstico: http://0.0.0.0:8000/api/v1/diagnostico/regiones"
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