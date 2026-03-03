# ── Etapa 1: compilar la app React ──────────────────────────────────
FROM node:22-alpine AS builder

WORKDIR /app

# Instalar dependencias primero (caché de capas Docker)
COPY package*.json ./
RUN npm ci --prefer-offline --no-audit --no-fund

# Build arg para URL de API (valor por defecto /api para que nginx haga proxy)
ARG VITE_API_URL=/api
ENV VITE_API_URL=$VITE_API_URL

COPY . .
RUN npm run build

# ── Etapa 2: servir con nginx ───────────────────────────────────
FROM nginx:1.27-alpine

RUN apk add --no-cache curl

# Copiar los assets compilados
COPY --from=builder /app/dist /usr/share/nginx/html

# Copiar configuración nginx (con proxy /api → backend)
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -f http://localhost/health || exit 1

CMD ["nginx", "-g", "daemon off;"]