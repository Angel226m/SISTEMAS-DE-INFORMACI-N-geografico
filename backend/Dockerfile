# ══════════════════════════════════════════════════════════
#  GeoRiesgo Perú — Dockerfile v4.0
#  Sin GDAL/GeoPandas → imagen ~180MB en lugar de ~1.2GB
#  Usamos shapely + pyproj que son mucho más ligeros
# ══════════════════════════════════════════════════════════

FROM python:3.12-slim AS base

LABEL maintainer="GeoRiesgo Perú <georiesgo@ica.gob.pe>"
LABEL version="4.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONFAULTHANDLER=1

# ── Deps del sistema mínimas ──────────────────────────────
# libpq5:       cliente PostgreSQL (runtime)
# postgresql-client: pg_isready en entrypoint
# curl:         healthcheck
# No necesitamos GDAL porque parseamos GeoJSON nativamente con json+shapely
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq5 \
        libgeos-c1v5 \
        postgresql-client \
        curl \
        ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /app

# ── Dependencias Python ───────────────────────────────────
COPY requirements.txt .
RUN pip install --upgrade pip==24.3.1 \
    && pip install --no-cache-dir -r requirements.txt \
    && find /usr/local/lib/python3.12 -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# ── Código fuente ─────────────────────────────────────────
COPY . .

# ── Entrypoint ────────────────────────────────────────────
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Usuario no-root para seguridad
RUN useradd -m -u 1001 appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=5 \
    CMD curl -sf http://localhost:8000/health || exit 1

CMD ["/entrypoint.sh"]