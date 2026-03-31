# 🌋 GeoRiesgo Perú v9.0 — Plataforma Multi-Amenaza con ML y EWS

> Plataforma geoespacial multi-amenaza para el análisis integral de riesgos naturales en Perú. Integra sismicidad, vulcanismo, deslizamientos, inundaciones, tsunamis, precipitaciones FEN, modelos de susceptibilidad ML (XGBoost) y sistema de alerta temprana (EWS) en tiempo real.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178C6?logo=typescript&logoColor=white)](https://typescriptlang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16_PostGIS-4169E1?logo=postgresql&logoColor=white)](https://postgis.net)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![XGBoost](https://img.shields.io/badge/XGBoost-ML-FF6600?logo=xgboost&logoColor=white)](https://xgboost.readthedocs.io)

---

## 📑 Contenidos

1. [Descripción General](#-descripción-general)
2. [Arquitectura del Sistema](#-arquitectura-del-sistema)
3. [Stack Tecnológico](#-stack-tecnológico)
4. [Estructura del Proyecto](#-estructura-del-proyecto)
5. [Inicio Rápido](#-inicio-rápido-con-docker)
6. [API REST — Endpoints](#-api-rest--endpoints)
7. [Motor ML — Susceptibilidad](#-motor-ml--susceptibilidad)
8. [Early Warning System (EWS)](#-early-warning-system-ews)
9. [Frontend](#-frontend)
10. [Desarrollo Local](#-desarrollo-local)

---

## 📌 Descripción General

**GeoRiesgo Perú** es una plataforma web full-stack de evaluación integral de riesgos por amenazas naturales. Combina datos geoespaciales de múltiples fuentes (USGS, IGP, INGEMMET, SENAMHI, NOAA-CPC, GEM, CENEPRED, INDECI, UNDRR) en una interfaz unificada con análisis en tiempo real.

### Capacidades Principales

| Módulo                       | Descripción |
|------------------------------|-------------|
| 🗺️ Mapa interactivo          | MapLibre GL + deck.gl con 15+ capas WebGL aceleradas por GPU |
| 🌐 Vista 2D / 3D             | Perspectiva aérea con extrusión ColumnLayer por nivel de riesgo |
| 🔥 Heatmap sísmico           | Mapa de calor con ponderación por magnitud (ScreenGridLayer) |
| 🌋 Volcanes                  | Ubicación, estado, radios de peligro, historial eruptivo |
| ⛰️ Deslizamientos            | Zonas activas con clasificación                              |
| 🌊 Tsunamis e inundaciones   | Zonas de riesgo, altura de ola, zona costera                 |
| 🌧️ Precipitaciones + FEN     | Zonas climáticas, índice El Niño, riesgo pluvial              |
| 🤖 ML Susceptibilidad        | XGBoost por amenaza (deslizamiento, inundación, sequía)       |
| ⚡ EWS Alertas               | Early Warning System en tiempo real (SSE + WebSocket)         |
| 🏗️ IRC v9                    | Índice de Riesgo de Construcción con cascada multi-amenaza    |
| 📊 Sendai Framework          | Indicadores alineados con los ODS/Sendai 2015–2030           |
| 🎚️ Escenarios 4DS            | Simulación de daño modelo (magnitud, profundidad, radio)      |
| ⌨️ Atajos de teclado          | `[L]` sidebar · `[F]` filtros · `[G]` gráfica · `[R]` centrar |

---

## 🏗 Arquitectura del Sistema

```
┌──────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE                           │
│                                                                  │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  FRONTEND    │   │   BACKEND         │   │   PostgreSQL   │  │
│  │  React 19    │   │   FastAPI v9.0    │   │   16 + PostGIS │  │
│  │  Vite 7      │   │   ML Engine       │   │   3.4          │  │
│  │  deck.gl 9.2 │──►│   EWS Worker      │──►│   TimescaleDB  │  │
│  │  nginx :80   │   │   uvicorn :8000   │   │   :5432        │  │
│  └──────────────┘   └────────┬─────────┘   └────────────────┘  │
│                              │                                   │
│                     ┌────────▼─────────┐   ┌────────────────┐   │
│                     │   Redis 7        │   │  models_data   │   │
│                     │   Cache + Pub    │   │  (XGBoost .pkl)│   │
│                     │   :6379          │   └────────────────┘   │
│                     └──────────────────┘                         │
└──────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos

1. **Primer arranque**: ETL (`procesar_datos.py`) descarga datos de USGS FDSNWS, INGEMMET, IGP, ANA y los carga en PostgreSQL/PostGIS
2. **PostGIS**: Toda la data reside en tablas geoespaciales con índices GIST para consultas espaciales eficientes
3. **Redis**: Cache LRU de 128 MB para respuestas GeoJSON frecuentes (TTL configurable)
4. **ML Engine**: Entrena modelos XGBoost por amenaza tras el ETL (si `ML_AUTO_TRAIN=1`)
5. **EWS Worker**: Monitorea USGS en tiempo real, emite alertas vía SSE/WebSocket con cascada (tsunami/deslizamiento)
6. **Frontend**: React consume la API REST y renderiza 15+ capas deck.gl sobre MapLibre GL

---

## 🛠 Stack Tecnológico

### Backend
| Tecnología       | Versión   | Uso |
|-----------------|-----------|-----|
| Python           | 3.11      | Runtime |
| FastAPI          | 0.115     | Framework API REST asíncrono |
| Uvicorn          | 0.31      | Servidor ASGI |
| asyncpg          | —         | Driver PostgreSQL asíncrono |
| XGBoost          | —         | Modelos de susceptibilidad ML |
| scikit-learn     | —         | Pipeline ML + métricas |
| NumPy / Pandas   | 2.x       | Procesamiento numérico |
| Redis (aioredis) | —         | Cache distribuida |
| httpx / tenacity | —         | HTTP asíncrono + reintentos |
| slowapi          | —         | Rate limiting |
| orjson           | —         | Serialización JSON rápida |

### Frontend
| Tecnología        | Versión   | Uso |
|------------------|-----------|-----|
| React             | 19        | UI Framework |
| TypeScript        | 5.9       | Tipado estático |
| Vite              | 7         | Build tool + HMR |
| MapLibre GL JS    | 5.19      | Motor de mapas WebGL |
| deck.gl           | 9.2       | Capas geoespaciales (Scatterplot, GeoJson, Column, ScreenGrid) |
| Recharts          | 3.7       | Gráficos estadísticos |
| Tailwind CSS      | 3.4       | Utilidades CSS (dev) |

### Infraestructura
| Tecnología           | Uso |
|---------------------|-----|
| Docker Compose       | Orquestación de 4 servicios |
| PostgreSQL 16        | Base de datos principal |
| PostGIS 3.4          | Extensión geoespacial |
| Redis 7 Alpine       | Cache LRU + pub/sub |
| nginx 1.27           | Servidor HTTP + proxy reverso /api/ → backend |

---

## 📁 Estructura del Proyecto

```
georiesgo-peru/
├── docker-compose.yml              # 4 servicios: db, redis, backend, frontend
├── README.md
├── backend/
│   ├── Dockerfile                  # Python 3.11-slim + GDAL + deps
│   ├── entrypoint.sh               # ETL + arranque uvicorn
│   ├── init.sql                    # DDL PostgreSQL (20+ tablas + PostGIS)
│   ├── main.py                     # API FastAPI — 30+ endpoints REST + WS
│   ├── ml_engine.py                # Motor ML XGBoost (susceptibilidad multi-amenaza)
│   ├── alert_worker.py             # EWS Worker — monitoreo USGS en tiempo real
│   ├── damage_model.py             # Modelo de daño 4DS (Sendai)
│   ├── procesar_datos.py           # ETL: descarga y carga a PostGIS
│   ├── cache.py                    # Decorador cache Redis con TTL
│   ├── stac_catalog.py             # Catálogo STAC (opcional, requiere MinIO)
│   └── requirements.txt
├── frontend/
│   ├── Dockerfile                  # Build multi-stage React → nginx
│   ├── nginx.conf                  # Proxy /api/ → backend:8000
│   ├── vite.config.ts              # Config Vite + proxy dev
│   └── src/
│       ├── App.tsx                 # Layout principal + header + sidebar
│       ├── components/
│       │   ├── MapView.tsx         # Mapa MapLibre + 15 capas deck.gl + 3D extrusion
│       │   ├── LayerPanel.tsx      # Panel de capas con toggles
│       │   ├── FilterPanel.tsx     # Filtros sismos + precipitación + ML
│       │   ├── StatsChart.tsx      # Gráficos: histograma, IRC ranking, FEN, Sendai
│       │   ├── Landingpage.tsx     # Página de bienvenida
│       │   ├── InfoPopup.tsx       # Popup detallado por entidad (12+ tipos)
│       │   ├── HoverTooltip.tsx    # Tooltip de hover rápido
│       │   ├── RiesgoPanel.tsx     # Panel lateral de riesgo puntual
│       │   ├── Loader.tsx          # Pantalla de carga con progreso
│       │   ├── ToastList.tsx       # Notificaciones toast
│       │   └── ui/
│       │       ├── constants.ts    # Colores, etiquetas de riesgo
│       │       ├── Icons.tsx       # Iconos SVG del sistema
│       │       ├── Btn.tsx         # Componente botón reutilizable
│       │       ├── Row.tsx         # Fila clave-valor
│       │       └── Badges.tsx      # ZonaBadge (sísmica) + FENBadge
│       ├── hooks/
│       │   └── useMapData.ts       # Hook central de carga de datos
│       ├── services/
│       │   └── api.ts              # Cliente HTTP tipado → FastAPI
│       └── types/
│           └── index.ts            # 50+ interfaces TypeScript
```

---

## 🐳 Inicio Rápido con Docker

### Requisitos
- Docker >= 24.0
- Docker Compose >= 2.20
- 4 GB de RAM disponibles (PostGIS + XGBoost requieren ~2.5 GB en total)

### Levantar la plataforma completa

```bash
git clone <repo-url>
cd georiesgo-peru

# Construir y levantar (primer arranque incluye ETL ~5-10 min)
docker compose up --build

# En background
docker compose up --build -d
```

> ⏱️ **Primer arranque**: El backend ejecutará el ETL completo: descarga de sismos USGS, datos IGP/INGEMMET/ANA, carga en PostGIS y entrenamiento ML automático. Puede tardar 5–10 minutos. Los modelos ML se persisten en el volumen `models_data`.

### Variables de Entorno

| Variable          | Default              | Descripción |
|-------------------|----------------------|-------------|
| `DB_PASSWORD`     | `georiesgo_secret`   | Contraseña PostgreSQL |
| `ML_AUTO_TRAIN`   | `1`                  | Entrenar ML tras ETL |
| `ML_MIN_SAMPLES`  | `10`                 | Muestras mín. para entrenar |
| `LOG_LEVEL`       | `INFO`               | Nivel de logging |
| `WORKERS`         | `2`                  | Workers uvicorn |
| `FORCE_SYNC`      | `0`                  | Forzar re-ETL |

### Accesos

| Servicio          | URL                                    |
|------------------|----------------------------------------|
| 🗺️ Frontend       | http://localhost (puerto 80)           |
| ⚡ API REST       | http://localhost:8000                  |
| 📖 API Docs       | http://localhost:8000/docs (Swagger)   |
| ❤️ Health         | http://localhost:8000/health           |
| 🐘 PostgreSQL     | localhost:5432 (georiesgo/georiesgo_secret) |

### Ver logs

```bash
docker compose logs -f backend    # Logs del backend + ML
docker compose logs -f db         # Logs PostgreSQL
```

### Parar y limpiar

```bash
docker compose down          # Para servicios
docker compose down -v       # Para servicios + elimina volúmenes (BORRA datos)
```

---

## 🌐 API REST — Endpoints

La API expone 30+ endpoints REST + WebSocket bajo `/api/v1/`. Documentación interactiva en `/docs` (Swagger) y `/redoc`.

### Sistema

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/`  | Estado general del sistema v9.0 |
| `GET`  | `/health` | Healthcheck Docker/k8s |
| `GET`  | `/api/v1/resumen` | Resumen completo de datos cargados |
| `GET`  | `/api/v1/diagnostico/regiones` | Diagnóstico de regiones disponibles |

### Sismos

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/sismos` | GeoJSON FeatureCollection filtrable (mag, año, región, profundidad) |
| `GET`  | `/api/v1/sismos/recientes` | Últimos sismos detectados |
| `GET`  | `/api/v1/sismos/estadisticas` | Estadísticas agregadas por año |
| `GET`  | `/api/v1/sismos/heatmap` | Datos para mapa de calor |
| `GET`  | `/api/v1/sismos/cercanos` | Sismos cercanos a un punto (lon, lat, radio_km) |
| `GET`  | `/api/v1/sismos/{usgs_id}` | Detalle de un sismo por ID USGS |
| `WS`   | `/ws/sismos` | WebSocket tiempo real de sismos nuevos |

### Administrativo

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/departamentos` | GeoJSON departamentos con zona sísmica |
| `GET`  | `/api/v1/distritos` | GeoJSON distritos con nivel_riesgo 1–5, peligro_*, IRC |
| `GET`  | `/api/v1/distritos/resumen` | Resumen estadístico de distritos |

### Geología

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/fallas` | Fallas geológicas (activa, longitud_km, magnitud_max) |
| `GET`  | `/api/v1/deslizamientos` | Zonas de deslizamiento activas |
| `GET`  | `/api/v1/zonas-sismicas` | Zonificación sísmica NTE E.030 |
| `GET`  | `/api/v1/zonas-sismicas/referencia` | Tabla de referencia Z1–Z4 |

### Hidrometeorología

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/inundaciones` | Zonas inundables |
| `GET`  | `/api/v1/tsunamis` | Zonas de riesgo de tsunami |
| `GET`  | `/api/v1/precipitaciones` | Zonas de precipitación con índice FEN |
| `GET`  | `/api/v1/precipitaciones/cercanas` | Zonas cercanas a un punto |

### Precipitaciones y Fenómeno El Niño (FEN)

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/fen` | Eventos El Niño / La Niña históricos |
| `GET`  | `/api/v1/fen/estadisticas` | Estadísticas FEN agregadas |
| `GET`  | `/api/v1/riesgo/lluvia` | Riesgo pluvial puntual (lon, lat) |

### Infraestructura

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/infraestructura` | GeoJSON infraestructura filtrable (tipo, criticidad, fuente) |
| `GET`  | `/api/v1/infraestructura/cobertura` | Estadísticas de cobertura oficial vs OSM |
| `GET`  | `/api/v1/estaciones` | Estaciones de monitoreo sísmico |

### Riesgo y ML

| Método  | Ruta | Descripción |
|---------|------|-------------|
| `GET`   | `/api/v1/riesgo` | Riesgo integral puntual (lon, lat) |
| `GET`   | `/api/v1/riesgo/construccion/mapa` | GeoJSON IRC por distrito |
| `GET`   | `/api/v1/bbox` | Bounding box de los datos |

### Espacial

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/bbox` | Bounding box general de los datos |

---

## 🤖 Motor ML — Susceptibilidad

El módulo `ml_engine.py` implementa modelos XGBoost de susceptibilidad por amenaza natural.

### Amenazas Soportadas

| Amenaza         | Features (8 por modelo) |
|-----------------|-------------------------|
| `deslizamiento` | peligro_sismo, peligro_inundacion, peligro_deslizamiento, peligro_sequia, pendiente_media, altitud_media, precipitacion_anual, densidad_poblacional |
| `inundacion`    | Los mismos 8 features extraídos de la tabla `distritos` |
| `sequia`        | Los mismos 8 features extraídos de la tabla `distritos` |

### Pipeline ML

1. **Extracción**: Consulta tabla `distritos` con campos `peligro_*` vía asyncpg
2. **Target binario**: `1` si `peligro_{amenaza} ≥ 3`, `0` si no
3. **Balanceo**: `scale_pos_weight` automático para clases desbalanceadas
4. **Modelo**: `XGBClassifier` (max_depth=5, n_estimators=150, learning_rate=0.08)
5. **Evaluación**: Accuracy, F1, AUC-ROC, Classification Report
6. **Persistencia**: Modelo `.pkl` en volumen Docker `models_data`
7. **Metadata**: UPSERT en tabla `modelo_metadata` (accuracy, features, timestamp)

### Predicción

- `predict_point(lon, lat, amenaza, conn)` → `{score, nivel, score_p10, score_p90}`
- **Bootstrap CI**: 100 iteraciones con ruido gaussiano para intervalos de confianza al 80%
- **Fallback heurístico**: Si no hay modelo entrenado, calcula score desde datos del distrito

### Configuración

```bash
ML_AUTO_TRAIN=1       # Entrenar automáticamente tras ETL
ML_MIN_SAMPLES=10     # Mínimo de muestras (10 funciona para datos init.sql)
```

---

## ⚡ Early Warning System (EWS)

El módulo `alert_worker.py` implementa un sistema de alerta temprana en tiempo real.

### Características

- **Polling**: Consulta USGS cada 30s para sismos recientes (últimos 5 min)
- **Cascada**: Evalúa posible tsunami (profundidad < 50 km + magnitud ≥ 7.0 + costera) y deslizamiento
- **Niveles**: `emergency` (M≥7.0), `warning` (M≥5.5), `watch` (M≥4.0)
- **Difusión**: SSE (Server-Sent Events) + WebSocket simultáneo
- **Alineación**: Protocolo CAP v1.2 · INDECI 2020 · EW4All UNDRR 2022

---

## 🎨 Frontend — Funcionalidades

### Capas del Mapa (15+)

| Capa | deck.gl Layer | Descripción |
|------|--------------|-------------|
| Sismos | ScatterplotLayer | Puntos coloreados por profundidad, radio ∝ magnitud |
| Heatmap | ScreenGridLayer | Mapa de calor ponderado por magnitud |
| Departamentos | GeoJsonLayer | Polígonos con zona sísmica |
| Distritos | GeoJsonLayer | Nivel de riesgo 1–5 (coropleta) |
| IRC Mapa | GeoJsonLayer | Índice de Riesgo Construcción |
| Fallas | GeoJsonLayer | Líneas con buffer visual |
| Inundaciones | GeoJsonLayer | Zonas inundables |
| Tsunamis | GeoJsonLayer | Franja costera de riesgo |
| Deslizamientos | GeoJsonLayer | Zonas activas |
| Precipitaciones | GeoJsonLayer | Zonas climáticas + FEN |
| Volcanes | ScatterplotLayer | Con estado y radio de peligro |
| Susceptibilidad ML | ScatterplotLayer | Scores de modelo ML con IC |
| Alertas EWS | ScatterplotLayer | Alertas en tiempo real con pulsación |
| Estaciones | ScatterplotLayer | Estaciones de monitoreo |
| Extrusión 3D | ColumnLayer | Centroides de distritos extruidos por nivel_riesgo |

### Panel de Filtros

- Presets rápidos: Todo · Recientes · Fuertes ≥6 · Pisco 2007 · Grandes ≥7
- Filtros de magnitud, año, región, profundidad
- Filtros ML: score mínimo, amenaza seleccionada
- Filtros de precipitación: riesgo de inundación mínimo
- Fuente de datos: todos / oficial / OSM

### Gráficos (StatsChart)

- Histograma sísmico por año (barras/línea)
- Ranking IRC por distrito
- Eventos FEN históricos
- Escenario de daño 4DS
- Reporte Sendai

---

## 💻 Desarrollo Local

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt

# Necesita PostgreSQL + PostGIS + Redis corriendo
# Opción 1: levantar solo db y redis con Docker
docker compose up -d db redis

# Variables de entorno
set DATABASE_URL=postgresql://georiesgo:georiesgo_secret@localhost:5432/georiesgo
set REDIS_URL=redis://localhost:6379/0

# Iniciar con hot-reload
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev     # http://localhost:5173 con proxy a :8000
```

> El `vite.config.ts` incluye proxy `/api/*` → `http://localhost:8000`.

### Verificación

```bash
cd frontend
npm run build   # Compila TypeScript + verifica tipos
npm run lint    # ESLint
```

---

## 📡 Fuentes de Datos

| Capa                | Fuente |
|--------------------|--------|
| Sismos históricos   | USGS FDSN Web Services |
| Fallas geológicas   | INGEMMET GeoCATMIN |
| Volcanes            | INGEMMET 2021 |
| Zonas inundables    | ANA — Autoridad Nacional del Agua |
| Riesgo distrital    | IGP — Instituto Geofísico del Perú |
| Precipitaciones     | SENAMHI |
| Eventos FEN         | NOAA-CPC |
| Infraestructura     | GEM + OSM |
| Marco Sendai        | UNDRR / CENEPRED / INDECI |

---

## 📄 Licencia

Proyecto académico — noveno ciclo. Datos de fuentes públicas gubernamentales.

### Atajos de Teclado
| Tecla    | Acción           |
|---------|-----------------|
| `L`     | Toggle sidebar  |
| `F`     | Abrir filtros   |
| `G`     | Toggle gráfica  |
| `Esc`   | Cerrar popup    |

---

## ⚙️ Variables de Entorno

### Backend
| Variable    | Default          | Descripción |
|------------|-----------------|-------------|
| `DATA_DIR`  | `/data/processed` | Directorio de GeoJSON procesados |
| `APP_ENV`   | `production`     | `development` activa hot-reload |

### Frontend (build-time)
| Variable       | Default | Descripción |
|---------------|---------|-------------|
| `VITE_API_URL` | `/api`  | URL base de la API (en Docker usa nginx proxy) |

---

## 🗺️ Notas sobre la Región Ica

La Región Ica se encuentra en la Costa Sur de Perú, directamente sobre la **Zona de Subducción de Nazca**, donde la Placa de Nazca se subduce bajo la Placa Sudamericana a ~7 cm/año. Esto la convierte en una de las regiones con mayor actividad sísmica del planeta.

**Eventos históricos catastróficos incluidos en el dataset:**

| Año  | Evento          | Magnitud | Fallecidos |
|------|----------------|----------|-----------|
| 1942 | Terremoto Ica   | 8.2 Mw   | ~30        |
| 1996 | Terremoto Nazca | 7.7 Mw   | 14         |
| 2007 | Terremoto Pisco | 8.0 Mw   | 519        |

El **terremoto de Pisco 2007** (15 de agosto, 18:40 UTC-5) fue el más destructivo del siglo XXI en Perú, con epicentro a 40 km al oeste de Pisco. Destruyó ~80% de Pisco y ~60% de Ica, generando un tsunami local con olas de hasta 10 metros.

---

## 📦 Producción y Despliegue Avanzado

Para un despliegue en producción se recomiendan las siguientes mejoras:

1. **Base de datos PostGIS**: Descomentar el servicio `postgres` en `docker-compose.yml` y migrar los GeoJSON a tablas con índices espaciales GIST
2. **SSL/TLS**: Agregar nginx como reverse proxy externo con Certbot (Let's Encrypt)
3. **Datos IGN oficiales**: Reemplazar polígonos aproximados con shapefiles oficiales del IGN
4. **Monitoreo**: Agregar Prometheus + Grafana (el proyecto incluye carpeta `/infra` como referencia)
5. **CDN**: Servir assets estáticos desde un CDN (CloudFront, Cloudflare)

---

## 📄 Licencia

Uso académico y educativo. Datos sísmicos © USGS (dominio público). Datos geoespaciales © IGP, INGEMMET, ANA (sujetos a términos de uso de cada institución).

---

*Desarrollado como proyecto de visualización geoespacial para la región Ica, Perú.*
#   S I S T E M A S - D E - I N F O R M A C I - N - g e o g r a f i c o 
 
 