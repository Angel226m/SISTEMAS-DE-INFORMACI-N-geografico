#!/bin/bash
# ══════════════════════════════════════════════════════════════════
# GeoRiesgo Perú — Entrypoint v8.0  ENTERPRISE
# ══════════════════════════════════════════════════════════════════
set -euo pipefail

echo ""
echo "  ╔════════════════════════════════════════════════════════════╗"
echo "  ║  GeoRiesgo Perú — Backend v8.0  ENTERPRISE                ║"
echo "  ║  FastAPI + PostgreSQL/PostGIS + NTE E.030/E.031-2020      ║"
echo "  ║  🆕 Precipitaciones (SENAMHI/CHIRPS) + FEN (NOAA-CPC)    ║"
echo "  ╚════════════════════════════════════════════════════════════╝"
echo "  ENV: ${APP_ENV:-production}"
echo ""

# ── Variables ──────────────────────────────────────────────────────
DB_HOST="${DB_HOST:-db}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-georiesgo}"
DB_USER="${POSTGRES_USER:-georiesgo}"
DB_PASS="${DB_PASSWORD:-georiesgo_secret}"
export PGPASSWORD="${DB_PASS}"

# ── Helper: query BD con valor por defecto ─────────────────────────
db_count() {
    # db_count "SELECT COUNT(*) FROM tabla" → número o "0" si falla
    local result
    result=$(
        psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
             -t -A -c "$1" 2>/dev/null || echo "0"
    )
    echo "${result//[[:space:]]/}"
}

# ── Esperar PostgreSQL ─────────────────────────────────────────────
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
echo "  ✓ PostgreSQL listo                                         "
echo ""

# ── Verificar / instalar extensiones PostGIS ──────────────────────
echo "  Verificando extensión PostGIS..."
POSTGIS_OK=$(db_count "SELECT COUNT(*) FROM pg_extension WHERE extname='postgis'")

if [ "${POSTGIS_OK}" = "0" ]; then
    echo "  ⚠ PostGIS no instalado — ejecutando init.sql..."
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -f /app/init.sql 2>&1 | tail -15
else
    echo "  ✓ PostGIS disponible"

    # Verificar función requerida y tablas nuevas v8.0
    FUNC_OK=$(db_count "SELECT COUNT(*) FROM pg_proc WHERE proname='f_actualizar_regiones'")
    PRECIP_OK=$(db_count "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='zonas_precipitacion'")
    FEN_OK=$(db_count "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='eventos_fen'")

    if [ "${FUNC_OK}" = "0" ] || [ "${PRECIP_OK}" = "0" ] || [ "${FEN_OK}" = "0" ]; then
        echo "  ⚠ Schema incompleto (falta f_actualizar_regiones o tablas v8.0)"
        echo "    Re-ejecutando init.sql para aplicar schema v8.0..."
        psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
             -f /app/init.sql 2>&1 | tail -15
    else
        echo "  ✓ Funciones v8.0 OK (f_actualizar_regiones, zonas_precipitacion, eventos_fen)"
    fi
fi
echo ""

# ── Estado actual de la BD ─────────────────────────────────────────
SISMOS_N=$(db_count "SELECT COUNT(*) FROM sismos")
FALLAS_N=$(db_count "SELECT COUNT(*) FROM fallas")
DPTOS_N=$(db_count "SELECT COUNT(*) FROM departamentos")
INFRA_N=$(db_count "SELECT COUNT(*) FROM infraestructura")
PRECIP_N=$(db_count "SELECT COUNT(*) FROM zonas_precipitacion")
FEN_N=$(db_count "SELECT COUNT(*) FROM eventos_fen")
INFRA_SIN_REGION=$(db_count "SELECT COUNT(*) FROM infraestructura WHERE region IS NULL")
DIST_SIN_ZONA=$(db_count "SELECT COUNT(*) FROM distritos WHERE zona_sismica IS NULL")

echo "  Estado de la BD:"
echo "    sismos=${SISMOS_N}  fallas=${FALLAS_N}  departamentos=${DPTOS_N}"
echo "    infraestructura=${INFRA_N}  infra_sin_region=${INFRA_SIN_REGION}"
echo "    zonas_precipitacion=${PRECIP_N}  eventos_fen=${FEN_N}"
echo "    distritos_sin_zona_sismica=${DIST_SIN_ZONA} (0 = perfecto)"

# ── ETL inicial o forzado ──────────────────────────────────────────
if [ "${SISMOS_N}" = "0" ] || [ "${FORCE_SYNC:-0}" = "1" ]; then
    echo ""
    echo "  ► Iniciando ETL v8.0 ENTERPRISE (primera carga o FORCE_SYNC=1)"
    echo ""
    echo "    Pasos:"
    echo "      00 departamentos  → GADM L1 + 25 fallback bboxes"
    echo "      01 sismos         → USGS M≥2.5 1900-hoy (paralelo)"
    echo "      02 distritos      → INEI WFS → GADM L3 → 75 fallback"
    echo "      03 fallas         → IGP/Audin 19 fallas activas"
    echo "      04 inundaciones   → ANA + CENEPRED"
    echo "      05 tsunamis       → PREDES/IGP/DHN"
    echo "      06 deslizamientos → CENEPRED + INGEMMET"
    echo "      07 infraestructura→ MTC+APN+OSINERGMIN+MINSA+CGBVP+OSM"
    echo "      08 estaciones     → IGP+SENAMHI+ANA+DHN+IPEN"
    echo "      09 precipitaciones→ 🆕 SENAMHI/CHIRPS 22 zonas climáticas"
    echo "      10 eventos_fen    → 🆕 NOAA-CPC ENSO 1957-2024"
    echo "      11 heatmap        → REFRESH mv_heatmap_sismos"
    echo "      12 regiones       → f_actualizar_regiones() + zona_sismica"
    echo "      13 riesgo_constr  → REFRESH mv_riesgo_construccion (IRC)"
    echo ""
    echo "    Tiempo estimado: 5-25 min según conexión"
    echo ""

    if python /app/procesar_datos.py; then
        echo ""
        echo "  ✓ ETL v8.0 completado exitosamente"
    else
        echo ""
        echo "  ⚠ ETL terminó con errores parciales — la API iniciará de todos modos"
        echo "    Para reintentar: docker exec georiesgo_api python procesar_datos.py --force"
    fi

elif [ "${PRECIP_N}" = "0" ] || [ "${FEN_N}" = "0" ]; then
    # BD tiene sismos pero le faltan los pasos nuevos de v8.0
    echo ""
    echo "  ► BD existente detectada — cargando pasos nuevos v8.0..."

    if [ "${PRECIP_N}" = "0" ]; then
        echo "    → Cargando zonas de precipitación (SENAMHI/CHIRPS)..."
        python /app/procesar_datos.py --solo precipitaciones || true
    fi

    if [ "${FEN_N}" = "0" ]; then
        echo "    → Cargando eventos FEN históricos (NOAA-CPC)..."
        python /app/procesar_datos.py --solo eventos_fen || true
    fi

    echo "  ✓ Pasos v8.0 aplicados"

else
    echo ""
    echo "  ✓ BD con datos — omitiendo ETL completo"
    echo ""
    echo "    Comandos útiles:"
    echo "      Sync completo:        docker exec georiesgo_api python procesar_datos.py --force"
    echo "      Solo sismos:          docker exec georiesgo_api python procesar_datos.py --solo sismos"
    echo "      Solo precipitaciones: docker exec georiesgo_api python procesar_datos.py --solo precipitaciones"
    echo "      Solo eventos FEN:     docker exec georiesgo_api python procesar_datos.py --solo eventos_fen"
    echo "      Solo regiones:        docker exec georiesgo_api python procesar_datos.py --solo regiones"
    echo "      Refrescar heatmap:    docker exec georiesgo_api python procesar_datos.py --solo heatmap"
    echo "      Saltar pasos:         docker exec georiesgo_api python procesar_datos.py --skip sismos fallas"
    echo "      Dry-run:              docker exec georiesgo_api python procesar_datos.py --dry-run"
    echo ""
    echo "    Verificar cobertura:"
    echo "      curl http://localhost:8000/api/v1/diagnostico/regiones"
    echo "      curl http://localhost:8000/api/v1/fen/estadisticas"
fi

echo ""
echo "  ► Iniciando FastAPI v8.0 ENTERPRISE"
echo "  ► API:     http://0.0.0.0:8000/"
echo "  ► Docs:    http://0.0.0.0:8000/docs"
echo "  ► Redoc:   http://0.0.0.0:8000/redoc"
echo "  ► FEN:     http://0.0.0.0:8000/api/v1/fen"
echo "  ► Lluvia:  http://0.0.0.0:8000/api/v1/precipitaciones"
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