 # ══════════════════════════════════════════════════════════════════
#  GeoRiesgo Perú — Dockerfile backend v9.0  OPTIMIZADO
#  Requiere Docker BuildKit (DOCKER_BUILDKIT=1 o compose ≥ 2.x)
#
#  Optimizaciones vs versión anterior:
#  · pip cache mount → reinstalar deps sólo si requirements.txt cambia
#  · libgeos en runtime via libgeos-c1v5 (sin -dev innecesario en prod)
#  · GDAL: sólo gdal-bin + libgdal32 (no libgdal-dev, reduce ~60MB)
#  · entrypoint copiado ANTES de COPY . . para no invalidar su capa
#  · cleanup apt en un solo RUN para no acumular capas
#
#  Tamaño estimado: ~380 MB (vs ~420 MB anterior)
# ══════════════════════════════════════════════════════════════════

FROM python:3.12-slim AS base

LABEL maintainer="GeoRiesgo Perú <georiesgo@ica.gob.pe>"
LABEL version="9.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONFAULTHANDLER=1 \
    TOKENIZERS_PARALLELISM=false \
    PROJ_NETWORK=OFF \
    # BuildKit inline cache
    BUILDKIT_INLINE_CACHE=1

# ── Dependencias del sistema en un solo RUN ──────────────────────
# libpq5           → asyncpg/psycopg2 runtime
# postgresql-client → pg_isready en entrypoint
# libgeos-c1v5     → Shapely 2.x
# libgdal32        → rasterio runtime (sin -dev, ahorra ~60 MB)
# gdal-bin         → gdal-config necesario para pip install rasterio
# libgomp1         → OpenMP / XGBoost
# curl             → healthcheck Docker
# ca-certificates  → HTTPS a IGP/USGS/MinIO
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        postgresql-client \
        libgeos-c1v5 \
        libgomp1 \
        gdal-bin \
        libgdal-dev \
        libproj-dev \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# ── Exportar GDAL_VERSION para rasterio ─────────────────────────
RUN GDAL_VERSION=$(gdal-config --version 2>/dev/null || echo "3.x") && \
    echo "GDAL: ${GDAL_VERSION}"

WORKDIR /app

# ── Directorio de modelos ML (volumen externo en compose) ────────
RUN mkdir -p /app/models && chmod 777 /app/models

# ── Entrypoint PRIMERO para no invalidar capa en deploys de código
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# ── Dependencias Python con cache mount (BuildKit) ───────────────
# Esta capa se invalida SÓLO si requirements.txt cambia
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip==24.3.1 && \
    pip install -r requirements.txt && \
    # Limpiar __pycache__ generados por pip
    find /usr/local/lib/python3.12 -type d -name "__pycache__" \
         -exec rm -rf {} + 2>/dev/null || true

# ── Código fuente ─────────────────────────────────────────────────
# Esta capa se invalida en cada cambio de código (normal)
COPY . .

# ── Usuario no-root ───────────────────────────────────────────────
RUN useradd -m -u 1001 appuser \
    && chown -R appuser:appuser /app /app/models
USER appuser

EXPOSE 8000

# start_period largo porque el ETL tarda 8-30 min en primera carga
HEALTHCHECK --interval=30s --timeout=10s --start-period=300s --retries=10 \
    CMD curl -sf http://localhost:8000/health || exit 1

CMD ["/entrypoint.sh"]