#!/bin/bash
# ══════════════════════════════════════════════════════════════════════
# GeoRiesgo Perú — Entrypoint v9.0  ENTERPRISE
# Cambios v9.0:
#   + TimescaleDB HA image (db service)
#   + Redis healthcheck antes de iniciar API
#   + Verificación schema v9 (volcanes, alertas_rt, susceptibilidad_ml,
#     exposicion_distritos, sendai_snapshots, lecturas_estaciones)
#   + Directorio /app/models para modelos ML
#   + Pasos ETL nuevos: volcanes, cascada, irc_v9, exposicion_ivs
# ══════════════════════════════════════════════════════════════════════
set -euo pipefail

echo ""
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║  GeoRiesgo Perú — Backend v9.0  ENTERPRISE                  ║"
echo "  ║  FastAPI + TimescaleDB/PostGIS + Redis + ML + EWS + STAC    ║"
echo "  ║  7 amenazas · cascada · IRC incertidumbre · CAP v1.2        ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo "  ENV: ${APP_ENV:-production}"
echo ""

# ── Variables ──────────────────────────────────────────────────────────
DB_HOST="${DB_HOST:-db}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${POSTGRES_DB:-georiesgo}"
DB_USER="${POSTGRES_USER:-georiesgo}"
DB_PASS="${DB_PASSWORD:-georiesgo_secret}"
REDIS_URL="${REDIS_URL:-redis://redis:6379/0}"
MODEL_DIR="${MODEL_DIR:-/app/models}"
export PGPASSWORD="${DB_PASS}"

# ── Helper: query BD con valor por defecto ──────────────────────────────
db_count() {
    local result
    result=$(
        psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
             -t -A -c "$1" 2>/dev/null || echo "0"
    )
    echo "${result//[[:space:]]/}"
}

# ── Crear directorio de modelos si no existe ───────────────────────────
mkdir -p "${MODEL_DIR}"
echo "  ✓ Directorio de modelos ML: ${MODEL_DIR}"
echo ""

# ── 1. Esperar PostgreSQL / TimescaleDB ────────────────────────────────
echo "  [1/4] Esperando PostgreSQL/TimescaleDB en ${DB_HOST}:${DB_PORT}..."
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
echo "  ✓ PostgreSQL/TimescaleDB listo                              "
echo ""

# ── 2. Verificar / aplicar schema ─────────────────────────────────────
echo "  [2/4] Verificando schema v9.0..."

POSTGIS_OK=$(db_count "SELECT COUNT(*) FROM pg_extension WHERE extname='postgis'")
TIMESCALE_OK=$(db_count "SELECT COUNT(*) FROM pg_extension WHERE extname='timescaledb'")

# Tablas v8 base
PRECIP_OK=$(db_count   "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='zonas_precipitacion'")
FEN_OK=$(db_count      "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='eventos_fen'")
FUNC_OK=$(db_count     "SELECT COUNT(*) FROM pg_proc WHERE proname='f_actualizar_regiones'")

# Tablas v9 nuevas
VOLCAN_OK=$(db_count   "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='volcanes'")
ALERTAS_OK=$(db_count  "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='alertas_rt'")
SUSCEPT_OK=$(db_count  "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='susceptibilidad_ml'")
EXPOSIC_OK=$(db_count  "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='exposicion_distritos'")
SENDAI_OK=$(db_count   "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='sendai_snapshots'")
LECTURAS_OK=$(db_count "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='lecturas_estaciones'")

# Columnas v9 en distritos
COL_V9_OK=$(db_count   "SELECT COUNT(*) FROM information_schema.columns
                         WHERE table_name='distritos' AND column_name='indice_riesgo_v9'")

SCHEMA_COMPLETO=1
if [ "${POSTGIS_OK}" = "0" ] || [ "${FUNC_OK}" = "0" ] ||
   [ "${PRECIP_OK}" = "0" ] || [ "${FEN_OK}" = "0" ] ||
   [ "${VOLCAN_OK}" = "0" ] || [ "${ALERTAS_OK}" = "0" ] ||
   [ "${SUSCEPT_OK}" = "0" ] || [ "${EXPOSIC_OK}" = "0" ] ||
   [ "${SENDAI_OK}" = "0" ] || [ "${LECTURAS_OK}" = "0" ] ||
   [ "${COL_V9_OK}" = "0" ]; then
    SCHEMA_COMPLETO=0
fi

if [ "${SCHEMA_COMPLETO}" = "0" ]; then
    echo "  ⚠ Schema incompleto — ejecutando init.sql v9.0..."
    psql -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
         -f /app/init.sql 2>&1 | grep -E "(NOTICE|WARNING|ERROR|✅|⚠)" || true
    echo "  ✓ init.sql v9.0 aplicado"
else
    echo "  ✓ Schema v9.0 completo"
    if [ "${TIMESCALE_OK}" = "1" ]; then
        echo "  ✓ TimescaleDB activo (hypertables + CAG)"
    else
        echo "  ⚠ TimescaleDB no disponible — modo PostgreSQL estándar"
    fi
fi
echo ""

# ── 3. Estado actual de la BD ──────────────────────────────────────────
echo "  [3/4] Estado de la BD v9.0:"
SISMOS_N=$(db_count      "SELECT COUNT(*) FROM sismos")
FALLAS_N=$(db_count      "SELECT COUNT(*) FROM fallas")
DPTOS_N=$(db_count       "SELECT COUNT(*) FROM departamentos")
INFRA_N=$(db_count       "SELECT COUNT(*) FROM infraestructura")
PRECIP_N=$(db_count      "SELECT COUNT(*) FROM zonas_precipitacion")
FEN_N=$(db_count         "SELECT COUNT(*) FROM eventos_fen")
VOLCAN_N=$(db_count      "SELECT COUNT(*) FROM volcanes")
ALERTAS_N=$(db_count     "SELECT COUNT(*) FROM alertas_rt")
SUSCEPT_N=$(db_count     "SELECT COUNT(*) FROM susceptibilidad_ml")
EXPOSIC_N=$(db_count     "SELECT COUNT(*) FROM exposicion_distritos")
DIST_V9=$(db_count       "SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 IS NOT NULL")
DIST_TOTAL=$(db_count    "SELECT COUNT(*) FROM distritos")

echo ""
echo "    Tablas v8 base:"
echo "      sismos=${SISMOS_N}  fallas=${FALLAS_N}  departamentos=${DPTOS_N}"
echo "      infraestructura=${INFRA_N}  precipitaciones=${PRECIP_N}  fen=${FEN_N}"
echo ""
echo "    Tablas v9 nuevas:"
echo "      volcanes=${VOLCAN_N}  alertas_rt=${ALERTAS_N}  susceptibilidad_ml=${SUSCEPT_N}"
echo "      exposicion_distritos=${EXPOSIC_N}"
echo "      distritos con IRC v9: ${DIST_V9}/${DIST_TOTAL}"
echo ""

# ── 4. ETL inicial o forzado ──────────────────────────────────────────
if [ "${SISMOS_N}" = "0" ] || [ "${FORCE_SYNC:-0}" = "1" ]; then
    echo "  [4/4] Iniciando ETL v9.0 ENTERPRISE (primera carga o FORCE_SYNC=1)"
    echo ""
    echo "    Pasos v8 (base):"
    echo "      00 departamentos  → GADM L1 + 25 fallback bboxes"
    echo "      01 sismos         → USGS M≥2.5 1900-hoy (paralelo)"
    echo "      02 distritos      → INEI WFS → GADM L3 → 75 fallback"
    echo "      03 fallas         → INGEMMET/IGP 19 fallas activas"
    echo "      04 inundaciones   → ANA + CENEPRED"
    echo "      05 tsunamis       → PREDES/IGP/DHN"
    echo "      06 deslizamientos → CENEPRED + INGEMMET"
    echo "      07 infraestructura→ MTC+APN+OSINERGMIN+MINSA+CGBVP+OSM"
    echo "      08 estaciones     → IGP+SENAMHI+ANA+DHN+IPEN"
    echo "      09 precipitaciones→ SENAMHI/CHIRPS 22 zonas climáticas"
    echo "      10 eventos_fen    → NOAA-CPC ENSO 1957-2024"
    echo ""
    echo "    Pasos v9 (nuevos):"
    echo "      11 volcanes       → INGEMMET/OVI-IGP 2021 (20 volcanes)"
    echo "      12 cascada        → factor_cascada (Gill & Malamud 2014)"
    echo "      13 irc_v9         → 7 amenazas + incertidumbre bootstrap"
    echo "      14 exposicion_ivs → GEM 2023 + INEI 2017 + MIDIS SISFOH 2022"
    echo "      15 heatmap        → REFRESH mv_heatmap_sismos"
    echo "      16 regiones       → f_actualizar_regiones() + zona_sismica"
    echo "      17 riesgo_constr  → REFRESH mv_riesgo_construccion (IRC v9)"
    echo "      18 sendai         → snapshot métricas proxy año actual"
    echo ""
    echo "    Tiempo estimado: 8-30 min según conexión"
    echo ""

    if python /app/procesar_datos.py; then
        echo ""
        echo "  ✓ ETL v9.0 completado exitosamente"
    else
        echo ""
        echo "  ⚠ ETL terminó con errores parciales — la API iniciará de todos modos"
        echo "    Para reintentar: docker exec georiesgo_api python procesar_datos.py --force"
    fi

elif [ "${VOLCAN_N}" = "0" ] || [ "${DIST_V9}" = "0" ] || [ "${EXPOSIC_N}" = "0" ]; then
    # BD tiene datos v8 pero le faltan pasos v9
    echo "  [4/4] BD v8 detectada — aplicando pasos nuevos v9.0..."
    echo ""

    if [ "${VOLCAN_N}" = "0" ]; then
        echo "    → Cargando volcanes (INGEMMET/OVI-IGP 2021)..."
        python /app/procesar_datos.py --solo volcanes || true
    fi

    if [ "${DIST_V9}" = "0" ]; then
        echo "    → Calculando IRC v9 (7 amenazas + cascada + bootstrap)..."
        python /app/procesar_datos.py --solo cascada irc_v9 || true
    fi

    if [ "${EXPOSIC_N}" = "0" ]; then
        echo "    → Cargando exposición / IVS (GEM 2023 + INEI 2017)..."
        python /app/procesar_datos.py --solo exposicion_ivs || true
    fi

    echo "  ✓ Pasos v9.0 aplicados"

elif [ "${PRECIP_N}" = "0" ] || [ "${FEN_N}" = "0" ]; then
    echo "  [4/4] BD existente — cargando pasos nuevos v8.0..."

    [ "${PRECIP_N}" = "0" ] && python /app/procesar_datos.py --solo precipitaciones || true
    [ "${FEN_N}" = "0" ]    && python /app/procesar_datos.py --solo eventos_fen     || true

    echo "  ✓ Pasos v8.0 aplicados"

else
    echo "  [4/4] BD con datos completos — omitiendo ETL"
    echo ""
    echo "    Comandos útiles (v9.0):"
    echo "      Sync completo:        docker exec georiesgo_api python procesar_datos.py --force"
    echo "      Solo volcanes:        docker exec georiesgo_api python procesar_datos.py --solo volcanes"
    echo "      Solo IRC v9:          docker exec georiesgo_api python procesar_datos.py --solo irc_v9"
    echo "      Solo cascada:         docker exec georiesgo_api python procesar_datos.py --solo cascada"
    echo "      Solo exposicion/IVS:  docker exec georiesgo_api python procesar_datos.py --solo exposicion_ivs"
    echo "      Solo sismos:          docker exec georiesgo_api python procesar_datos.py --solo sismos"
    echo "      Solo precipitaciones: docker exec georiesgo_api python procesar_datos.py --solo precipitaciones"
    echo "      Solo eventos FEN:     docker exec georiesgo_api python procesar_datos.py --solo eventos_fen"
    echo "      Solo regiones:        docker exec georiesgo_api python procesar_datos.py --solo regiones"
    echo "      Refrescar heatmap:    docker exec georiesgo_api python procesar_datos.py --solo heatmap"
    echo "      Dry-run:              docker exec georiesgo_api python procesar_datos.py --dry-run"
    echo ""
    echo "    Diagnóstico:"
    echo "      curl http://localhost:8000/api/v1/diagnostico/regiones"
    echo "      curl http://localhost:8000/api/v1/susceptibilidad/modelo/info"
    echo "      curl http://localhost:8000/api/v1/sendai/report"
    echo "      curl http://localhost:8000/api/v1/volcanes"
fi

echo ""
echo "  ► Iniciando FastAPI v9.0 ENTERPRISE"
echo "  ► API:          http://0.0.0.0:8000/"
echo "  ► Docs:         http://0.0.0.0:8000/docs"
echo "  ► Redoc:        http://0.0.0.0:8000/redoc"
echo "  ► Health:       http://0.0.0.0:8000/health"
echo "  ► EWS SSE:      http://0.0.0.0:8000/api/v1/alertas/stream"
echo "  ► EWS WS:       ws://0.0.0.0:8000/ws/sismos"
echo "  ► Volcanes:     http://0.0.0.0:8000/api/v1/volcanes"
echo "  ► IRC v9 mapa:  http://0.0.0.0:8000/api/v1/riesgo/construccion/ranking"
echo "  ► Sendai:       http://0.0.0.0:8000/api/v1/sendai/report"
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