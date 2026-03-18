# GeoRiesgo Peru -- Frontend Dockerfile v9.0 OPTIMIZADO
# Estrategia: 3 etapas (deps -> build -> nginx)
# Resultado: imagen final ~25MB (solo nginx + dist)

# ── Etapa 1: instalar dependencias (cacheada) ───────────────────
FROM node:22-alpine AS deps
WORKDIR /app

# Copiar solo manifiestos primero (layer cache)
COPY package.json package-lock.json ./

# ci es mas rapido y reproducible que install
RUN npm ci --prefer-offline --no-audit --no-fund

# ── Etapa 2: compilar TypeScript + Vite ─────────────────────────
FROM node:22-alpine AS builder
WORKDIR /app

# Reutilizar node_modules de la etapa anterior
COPY --from=deps /app/node_modules ./node_modules

# Copiar todo el codigo fuente
COPY . .

ARG VITE_API_URL=""
ENV VITE_API_URL=$VITE_API_URL \
    NODE_ENV=production

# Vite build (ignora errores TS, solo transpila)
RUN npx vite build

# ── Etapa 3: servir con nginx Alpine (~6MB base) ─────────────────
FROM nginx:1.27-alpine

# curl para healthcheck
RUN apk add --no-cache curl

# Assets del build
COPY --from=builder /app/dist /usr/share/nginx/html

# Config nginx (proxy + gzip + SPA)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost/health || exit 1

CMD ["nginx", "-g", "daemon off;"]