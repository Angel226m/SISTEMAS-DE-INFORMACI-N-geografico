# ══════════════════════════════════════════════════════════════════
#  GeoRiesgo Perú — Dockerfile backend v9.2  MULTI-STAGE (CORREGIDO)
#  Base: python:3.12-slim-bookworm (Debian 12) → compatible con GDAL
# ══════════════════════════════════════════════════════════════════

# ── Stage 1: Builder ──────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PROJ_NETWORK=OFF

# Paquetes de desarrollo (incluye headers necesarios para compilar GDAL/Shapely/etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        libgeos-dev \
        libgdal-dev \
        libproj-dev \
        proj-bin \
        gdal-bin \
        curl \
    && rm -rf /var/lib/apt/lists/*

RUN GDAL_VERSION=$(gdal-config --version 2>/dev/null || echo "3.x") && \
    echo "GDAL builder: ${GDAL_VERSION}"

WORKDIR /build

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip==24.3.1 && \
    pip wheel -r requirements.txt -w /build/wheels && \
    pip install --no-index --find-links=/build/wheels -r requirements.txt

# ── Stage 2: Runtime (slim, sin -dev) ─────────────────────────────
FROM python:3.12-slim-bookworm AS base

LABEL maintainer="GeoRiesgo Perú <georiesgo@ica.gob.pe>"
LABEL version="9.2"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONFAULTHANDLER=1 \
    TOKENIZERS_PARALLELISM=false \
    PROJ_NETWORK=OFF \
    BUILDKIT_INLINE_CACHE=1

# Runtime-only libs (solo las librerías necesarias, sin headers -dev)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        postgresql-client \
        libgeos-c1v5 \
        libgomp1 \
        gdal-bin \
        libgdal32 \
        libproj25 \
        proj-bin \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /app

# Directorio de modelos ML
RUN mkdir -p /app/models && chmod 777 /app/models

# Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Instalar desde wheels precompilados
COPY requirements.txt .
COPY --from=builder /build/wheels /tmp/wheels
RUN pip install --upgrade pip==24.3.1 && \
    pip install --no-index --find-links=/tmp/wheels -r requirements.txt && \
    rm -rf /tmp/wheels && \
    find /usr/local/lib/python3.12 -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Código fuente
COPY . .

# Usuario no-root (seguridad)
RUN useradd -m -u 1001 appuser \
    && chown -R appuser:appuser /app /app/models
USER appuser

EXPOSE 8000

# Healthcheck (el ETL puede tardar)
HEALTHCHECK --interval=30s --timeout=10s --start-period=300s --retries=10 \
    CMD curl -sf http://localhost:8000/health || exit 1

CMD ["/entrypoint.sh"]