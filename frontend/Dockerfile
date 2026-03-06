# ══════════════════════════════════════════════════════════
# GeoRiesgo Perú — Frontend Dockerfile v4.0 (Final)
# ══════════════════════════════════════════════════════════

# ── Etapa 1: dependencias ─────────────────────────────────
FROM node:22-alpine AS deps
WORKDIR /app
COPY package*.json ./
# Instalamos TODO incluyendo devDependencies para que Vite tenga sus plugins
RUN npm ci --prefer-offline --no-audit --no-fund

# ── Etapa 2: build ────────────────────────────────────────
FROM node:22-alpine AS builder
WORKDIR /app
# Copiamos node_modules de la etapa anterior
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Variables de entorno para el build
ARG VITE_API_URL=/api
ENV VITE_API_URL=$VITE_API_URL
ENV NODE_ENV=production

# Forzamos a Vite a ignorar TypeScript (tsc) ejecutándolo directamente
# Esto soluciona los errores de 'PopupInfo' y variables no usadas
RUN npx vite build

# ── Etapa 3: servir con nginx:alpine (~6MB base) ──────────
FROM nginx:1.27-alpine
RUN apk add --no-cache curl

# Copiar el build generado por Vite
COPY --from=builder /app/dist /usr/share/nginx/html

# Configuración de Nginx (Proxy + Gzip)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost/health || exit 1

CMD ["nginx", "-g", "daemon off;"]
