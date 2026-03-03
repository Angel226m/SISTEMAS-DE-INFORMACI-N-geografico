FROM python:3.12-slim

WORKDIR /app

# ── Variables de entorno ──────────────────────────────────
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# ── Dependencias del sistema ──────────────────────────────
# GDAL/GEOS para GeoPandas + curl para healthcheck + cliente psql
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc g++ \
        libgdal-dev gdal-bin \
        libgeos-dev \
        libproj-dev \
        libpq-dev \
        postgresql-client \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Dependencias Python ───────────────────────────────────
# Copiar primero solo requirements para aprovechar caché de capas
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# ── Código fuente ──────────────────────────────────────────
COPY . .

# ── Script de inicio ──────────────────────────────────────
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=5 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["/entrypoint.sh"]