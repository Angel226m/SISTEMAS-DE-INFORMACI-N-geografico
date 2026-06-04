# 🌋 GeoRiesgo Perú — Plataforma Multi-Amenaza con ML y Sistema de Alerta Temprana

> **Plataforma geoespacial full-stack para la evaluación integral de riesgos por amenazas naturales en Perú.** Integra 15+ fuentes de datos oficiales (USGS, IGP, INGEMMET, SENAMHI, NOAA-CPC, GEM, CENEPRED, INDECI, ANA, CAPECO, MIDIS, INEI, OVI-IGP, DHN, UNDRR), modelos de susceptibilidad con Machine Learning (XGBoost + SHAP + Optuna), un Índice de Riesgo Compuesto (IRC v9) con bootstrap de incertidumbre, modelo de daño sísmico, y un sistema de alerta temprana (EWS) en tiempo real con alineación al Marco Sendai 2015–2030.

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178C6?logo=typescript&logoColor=white)](https://typescriptlang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL_17_+_PostGIS_3.6-4169E1?logo=postgresql&logoColor=white)](https://postgis.net)
[![Docker](https://img.shields.io/badge/Docker_Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![XGBoost](https://img.shields.io/badge/XGBoost_ML-FF6600?logo=xgboost&logoColor=white)](https://xgboost.readthedocs.io)
[![Redis](https://img.shields.io/badge/Redis_7_LRU-DC382D?logo=redis&logoColor=white)](https://redis.io)
[![MapLibre](https://img.shields.io/badge/MapLibre_GL_5.19-35495E?logo=maplibre&logoColor=white)](https://maplibre.org)
[![deck.gl](https://img.shields.io/badge/deck.gl_9.2-00BFFF?logo=webgl&logoColor=white)](https://deck.gl)
[![Sendai](https://img.shields.io/badge/Sendai_2015--2030-00A1E0)](https://www.undrr.org/implementing-sendai-framework)

</div>

---

## 🖼️ Capturas de la Plataforma

<div align="center">

### 📊 Registro Histórico Sísmico
*Catálogo USGS 1900–hoy con 22 bloques paralelos, heatmap ponderado por magnitud y filtros por año/magnitud/región/profundidad.*
![Registro Histórico de Sismos](imagenes/registrohistoricosismo.png)

### 🌋 Riesgo por Distrito (IRC v9) + Clasificación de Suelos
*Índice de Riesgo Compuesto con bootstrap de incertidumbre (p10/p90), clasificación NTE E.031-2020 (S0–S4) y factor de amplificación sísmica.*
![Riesgo por Distrito IRC](imagenes/riesgodistrito-irc.png)

### ⛰️🌊🏥 Deslizamientos · Tsunamis · Infraestructura Crítica
*10 zonas activas de deslizamiento (CENEPRED/INGEMMET), 9 zonas de tsunami con altura de ola (PREDES/IGP/DHN) y red de infraestructura estratégica (hospitales, aeropuertos, puertos, bomberos).*
![Deslizamientos, Tsunami e Infraestructura](imagenes/dezlisaminetosunamieinfra.png)

</div>

---

## 📑 Tabla de Contenidos

0. [🖼️ Capturas de la Plataforma](#️-capturas-de-la-plataforma)
1. [Descripción General](#-descripción-general)
2. [Arquitectura del Sistema](#-arquitectura-del-sistema)
3. [Stack Tecnológico](#-stack-tecnológico)
4. [Estructura del Proyecto](#-estructura-del-proyecto)
5. [Inicio Rápido con Docker](#-inicio-rápido-con-docker)
6. [API REST — Endpoints](#-api-rest--endpoints)
7. [Motor ML — Susceptibilidad](#-motor-ml--susceptibilidad)
8. [Modelo de Daño Sísmico](#-modelo-de-daño-sísmico)
9. [Early Warning System (EWS)](#-early-warning-system-ews)
10. [ETL Pipeline](#-etl-pipeline)
11. [IRC v9 — Índice de Riesgo Compuesto](#-irc-v9--índice-de-riesgo-compuesto)
12. [Frontend — Mapa Interactivo](#-frontend--mapa-interactivo)
13. [Seguridad y Buenas Prácticas](#-seguridad-y-buenas-prácticas)
14. [Desarrollo Local](#-desarrollo-local)
15. [Fuentes de Datos](#-fuentes-de-datos)
16. [Referencias Científicas](#-referencias-científicas)
17. [Changelog](#-changelog)
18. [Atajos de Teclado](#️-atajos-de-teclado)
19. [Licencia](#-licencia)

---

## 📌 Descripción General

**GeoRiesgo Perú** es una plataforma web empresarial para la gestión integral del riesgo de desastres naturales en el territorio peruano. Combina datos geoespaciales de **15+ fuentes oficiales**, modelos de **Machine Learning explicables (XGBoost + SHAP)**, un **sistema de alerta temprana multi-amenaza en tiempo real**, y un **mapa interactivo 2D/3D** con más de 15 capas visualizables.

### Capacidades Principales

| Módulo | Descripción |
|---|---|
| 🗺️ **Mapa Interactivo** | MapLibre GL + deck.gl con 15+ capas WebGL aceleradas por GPU, soporte 2D y 3D con extrusión ColumnLayer |
| 🌐 **Vista 3D** | Perspectiva aérea con extrusión por nivel de riesgo, rotación automática y controles de inclinación |
| 🔥 **Heatmap Sísmico** | Mapa de calor con ponderación por magnitud (ScreenGridLayer) con actualización en tiempo real |
| 🌋 **Volcanes** | 20 volcanes catalogados por INGEMMET/OVI-IGP 2021 con radios de peligro por estado |
| ⛰️ **Deslizamientos** | 10 zonas activas con clasificación por tipo y causa (CENEPRED/INGEMMET) |
| 🌊 **Tsunamis e Inundaciones** | 9 zonas de tsunami + 12 zonas inundables con períodos de retorno |
| 🌧️ **Precipitaciones + FEN** | 22 zonas climáticas CHIRPS, índice El Niño, 22 eventos FEN históricos (1957–2024) |
| 🏜️ **Sequía SPI-12** | Standardized Precipitation Index (McKee et al. 1993 / CHIRPS 1981–2020) |
| 🤖 **ML Susceptibilidad** | XGBoost con pipeline VIF + SMOTE-Tomek + Optuna (20 trials) + SHAP TreeExplainer |
| ⚡ **EWS Alertas** | Sistema de Alerta Temprana en tiempo real (SSE + WebSocket + protocolo CAP v1.2) |
| 🏗️ **IRC v9** | Índice de Riesgo Compuesto: 7 amenazas ponderadas + factor cascada + bootstrap de incertidumbre |
| 💥 **Modelo de Daño** | Atenuación Youngs et al. 1997 + fragilidad lognormal multi-taxonomía + factor hora |
| 📊 **Sendai Framework** | 7 targets proxy alineados con SFDRR/EW4All (UNDRR 2022) |
| 🎚️ **Escenarios Sísmicos** | Simulación de pérdidas por magnitud, profundidad, hora del día y mix constructivo |
| ⌨️ **Atajos de Teclado** | `L` sidebar · `F` filtros · `G` gráfica · `R` centrar mapa · `Esc` cerrar popup |

---

## 🏗 Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                DOCKER COMPOSE                                       │
│                                                                                     │
│  ┌──────────────────────┐    ┌─────────────────────────┐    ┌─────────────────────┐ │
│  │      FRONTEND        │    │        BACKEND          │    │     POSTGRESQL      │ │
│  │   React 19 + TS 5.9  │    │   FastAPI 0.115         │    │      17 + PostGIS   │ │
│  │   Vite 7 + HMR       │    │   Uvicorn ASGI          │    │      3.6            │ │
│  │   deck.gl 9.2        │◄──►│   ML Engine (XGBoost)   │◄──►│  20+ tablas         │ │
│  │   MapLibre 5.19      │    │   EWS Alert Worker      │    │  Índices GIST/SP-   │ │
│  │   Recharts 3.7       │    │   Damage Model          │    │  GiST              │ │
│  │   Tailwind CSS 3.4   │    │   Cache Layer           │    │  MV vistas material.│ │
│  │   Nginx 1.27 :80     │    │   STAC Catalog          │    │  :5432              │ │
│  └──────────────────────┘    └───────────┬─────────────┘    └─────────────────────┘ │
│                                          │                                          │
│                                 ┌────────▼─────────┐    ┌─────────────────────┐    │
│                                 │    Redis 7 Alpine  │    │   models_data       │    │
│                                 │  Cache LRU 128 MB │    │  Modelos ML .pkl    │    │
│                                 │  Pub/Sub + SSE     │    │  Resultados SHAP    │    │
│                                 │  :6379             │    │  Scalers / Metadata │    │
│                                 └───────────────────┘    └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos

1. **ETL Inicial** (`procesar_datos.py`): Descarga datos de USGS FDSNWS, GADM 4.1, INGEMMET, IGP, ANA, INEI, SENAMHI y los procesa en 20 pasos secuenciales con dependencias, cargando todo en PostgreSQL/PostGIS
2. **PostGIS**: Datos geoespaciales almacenados con índices GIST/SP-GiST para consultas espaciales eficientes (+ vistas materializadas para agregaciones)
3. **Redis Cache**: Capa de caché LRU de 128 MB para respuestas GeoJSON frecuentes (TTL configurable por endpoint, decorador Python)
4. **ML Engine**: Entrenamiento de modelos XGBoost por amenaza tras el ETL con pipeline completo: VIF → SMOTE-Tomek → Optuna Bayesian HP Search → SHAP TreeExplainer
5. **EWS Worker**: Monitoreo de USGS/IGP cada 30s, emisión de alertas vía SSE + WebSocket con efecto cascada (tsunami/deslizamiento), protocolo CAP v1.2
6. **Damage Model**: Simulación de escenarios sísmicos con atenuación Youngs 1997, fragilidad multi-taxonomía y factor hora del día
7. **Frontend**: React con deck.gl renderiza 15+ capas sobre MapLibre GL, con lazy loading, ErrorBoundary por sección y ARIA landmarks

---

## 🛠 Stack Tecnológico

### Backend

| Tecnología | Versión | Propósito |
|---|---|---|
| Python | 3.12 | Runtime principal |
| FastAPI | 0.115 | Framework API REST asíncrono con OpenAPI/Swagger automático |
| Uvicorn | 0.31 | Servidor ASGI de alto rendimiento |
| asyncpg | ≥0.29 | Driver PostgreSQL asíncrono con prepared statements |
| XGBoost | ≥2.0 | Modelos gradient boosting para susceptibilidad |
| scikit-learn | ≥1.4 | Pipeline ML, métricas, StratifiedKFold CV |
| SHAP | ≥0.45 | TreeExplainer para explicabilidad de modelos |
| Optuna | ≥3.5 | Optimización bayesiana de hiperparámetros |
| imbalanced-learn | ≥0.12 | SMOTE-Tomek para balanceo de clases |
| Redis (aioredis) | ≥5.0 | Caché distribuida LRU + pub/sub para alertas |
| httpx + tenacity | — | HTTP asíncrono con reintentos exponenciales y jitter |
| slowapi | 0.1.9 | Rate limiting por IP |
| orjson | 3.10 | Serialización JSON optimizada (3-5x más rápida que json) |
| NumPy / Pandas | — | Procesamiento numérico y data frames |
| Shapely / PyProj | — | Operaciones geoespaciales y proyecciones cartográficas |
| Rasterio / rio-cogeo | — | Procesamiento de rasters y nubes de puntos COG |
| pystac / boto3 | — | Catálogo STAC opcional con MinIO |

### Frontend

| Tecnología | Versión | Propósito |
|---|---|---|
| React | 19 | UI Framework con ErrorBoundary y Concurrent Features |
| TypeScript | 5.9 | Tipado estático estricto |
| Vite | 7 | Build tool con HMR ultrarrápido y optimización de bundles |
| MapLibre GL JS | 5.19 | Motor de mapas WebGL open-source |
| deck.gl | 9.2 | Capas geoespaciales WebGL (Scatterplot, GeoJson, Column, ScreenGrid) |
| react-map-gl | 8.1 | Binding React para MapLibre |
| Recharts | 3.7 | Gráficos estadísticos (histogramas, rankings, barras) |
| Tailwind CSS | 3.4 | Framework de utilidades CSS |
| lucide-react | latest | Iconos SVG livianos |
| ESLint | 9 | Linting con reglas React + TypeScript |

### Infraestructura

| Tecnología | Propósito |
|---|---|
| Docker Compose | Orquestación de 4+ servicios |
| PostgreSQL 17 + PostGIS 3.6 | Base de datos relacional con extensión geoespacial |
| Redis 7 Alpine | Caché LRU + pub/sub para alertas en tiempo real |
| Nginx 1.27 | Servidor HTTP + proxy reverso (frontend estático + `/api/` → backend) |
| MinIO (opcional) | Almacenamiento de objetos para rasters COG y STAC |

---

## 📁 Estructura del Proyecto

```
georiesgo-peru/
├── docker-compose.yml          # Orquestación: db, redis, backend, frontend
├── .env                        # Variables de entorno (plantilla)
├── README.md
├── ARCHITECTURE.md             # Documentación técnica de arquitectura
│
├── backend/
│   ├── Dockerfile              # Multi-stage: builder (wheels C++) + runtime 3.12-slim
│   ├── entrypoint.sh           # Entrypoint: ETL + arranque uvicorn
│   ├── init.sql                # DDL completo: 20+ tablas, PostGIS, vistas materializadas
│   ├── main.py                 # API FastAPI — 30+ endpoints REST + WebSocket + SSE
│   ├── ml_engine.py            # Motor ML: XGBoost + VIF + SMOTE-Tomek + Optuna + SHAP
│   ├── alert_worker.py         # EWS Worker — polling USGS/IGP + alertas CAP v1.2
│   ├── damage_model.py         # Modelo de daño sísmico: Youngs 1997 + fragilidad
│   ├── procesar_datos.py       # ETL v9.1: 20 pasos con dependencias + bootstrap IRC
│   ├── cache.py                # Decorador de caché Redis con TTL y estadísticas
│   ├── stac_catalog.py         # Catálogo STAC opcional (requiere MinIO)
│   └── requirements.txt        # Dependencias Python
│
├── frontend/
│   ├── Dockerfile              # Multi-stage: build React → nginx estático
│   ├── nginx.conf              # Config nginx: proxy /api/ → backend:8000
│   ├── vite.config.ts          # Configuración Vite + proxy de desarrollo
│   ├── tailwind.config.js      # Configuración Tailwind CSS
│   ├── tsconfig.json           # TypeScript configuración
│   ├── eslint.config.js        # ESLint flat config
│   ├── index.html              # Entry point HTML
│   └── src/
│       ├── App.tsx             # Layout principal + ErrorBoundary + ARIA
│       ├── main.tsx            # Entry point React
│       ├── components/
│       │   ├── MapView.tsx     # Mapa MapLibre + 15 capas deck.gl + 3D
│       │   ├── LayerPanel.tsx  # Panel de capas con toggles y leyendas
│       │   ├── FilterPanel.tsx # Filtros: sismos, precipitación, ML, EWS
│       │   ├── StatsChart.tsx  # Gráficos: histograma, IRC ranking, FEN, Sendai
│       │   ├── Landingpage.tsx # Página de bienvenida con estadísticas
│       │   ├── InfoPopup.tsx   # Popup detallado por entidad (12+ tipos)
│       │   ├── HoverTooltip.tsx # Tooltip rápido de hover
│       │   ├── RiesgoPanel.tsx # Panel lateral de riesgo puntual
│       │   ├── Loader.tsx      # Pantalla de carga con progreso
│       │   ├── ToastList.tsx   # Notificaciones toast
│       │   ├── ErrorBoundary.tsx # ErrorBoundary con reset y logging
│       │   └── ui/             # Componentes atómicos: Badges, Btn, Icons, Row
│       ├── hooks/
│       │   └── useMapData.ts   # Hook central: carga lazy de datos (14+ estados)
│       ├── services/
│       │   └── api.ts          # Cliente HTTP tipado con cache, ETag, retry
│       └── types/
│           └── index.ts        # 50+ interfaces TypeScript
```

---

## 🐳 Inicio Rápido con Docker

### Requisitos

- Docker ≥ 24.0
- Docker Compose ≥ 2.20
- 4 GB RAM disponibles (PostGIS + XGBoost + Optuna requieren ~2.5 GB pico)

### Levantar la Plataforma Completa

```bash
git clone <repo-url>
cd georiesgo-peru

# Construir y levantar (primer arranque incluye ETL ~5-15 min)
docker compose up --build

# En background
docker compose up --build -d
```

> ⏱️ **Primer arranque**: El backend ejecuta el ETL completo (20 pasos): descarga de sismos USGS (1900–hoy en 22 bloques paralelos), datos IGP/INGEMMET/ANA/GADM, cálculo de IRC v9 con bootstrap 500 iteraciones, y entrenamiento ML automático. Los modelos ML se persisten en el volumen `models_data`.

### Variables de Entorno

| Variable | Default | Descripción |
|---|---|---|
| `DB_PASSWORD` | `georiesgo_secret` | Contraseña PostgreSQL |
| `CORS_ORIGINS` | `http://localhost:5173,http://localhost:3000,http://localhost` | Orígenes CORS (separados por coma) |
| `ML_AUTO_TRAIN` | `1` | Entrenar ML automáticamente tras ETL |
| `ML_MIN_SAMPLES` | `10` | Muestras mínimas para entrenar |
| `LOG_LEVEL` | `INFO` | Nivel de logging |
| `WORKERS` | `2` | Workers de uvicorn |
| `FORCE_SYNC` | `0` | Forzar re-ETL completo |
| `ETL_WORKERS` | `4` | Workers paralelos del ETL |
| `ETL_BOOTSTRAP_N` | `500` | Iteraciones bootstrap IRC v9 |
| `APP_ENV` | `production` | Entorno de la aplicación |
| `BACKEND_PORT` | `8000` | Puerto del backend |
| `FRONTEND_PORT` | `80` | Puerto del frontend |
| `DB_PORT_EXPOSE` | `5432` | Puerto expuesto de PostgreSQL |

### Accesos

| Servicio | URL |
|---|---|
| 🗺️ Frontend | `http://localhost` (puerto 80) |
| ⚡ API REST | `http://localhost:8000` |
| 📖 API Docs | `http://localhost:8000/docs` (Swagger UI) |
| 📖 API Docs (ReDoc) | `http://localhost:8000/redoc` (ReDoc) |
| ❤️ Health Endpoint | `http://localhost:8000/health` |
| 🐘 PostgreSQL | `localhost:5432` (`georiesgo` / `georiesgo_secret`) |

### Comandos Útiles

```bash
# Ver logs en tiempo real
docker compose logs -f backend       # Backend + ML + ETL
docker compose logs -f db            # PostgreSQL
docker compose logs -f frontend      # Frontend (nginx)

# Parar servicios
docker compose down

# Parar y eliminar volúmenes (borra TODOS los datos)
docker compose down -v

# Reconstruir sin caché
docker compose build --no-cache
```

---

## 🌐 API REST — Endpoints

La API expone **30+ endpoints REST** + WebSocket bajo `/api/v1/`. Documentación interactiva completa en `/docs` (Swagger) y `/redoc`.

### Sistema

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/` | Estado general del sistema v9.1 |
| `GET` | `/health` | Healthcheck para Docker/k8s |
| `GET` | `/api/v1/resumen` | Resumen completo de datos cargados |
| `GET` | `/api/v1/diagnostico/regiones` | Diagnóstico de regiones disponibles |

### Sismos

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/sismos` | GeoJSON FeatureCollection filtrable (mag, año, región, profundidad) |
| `GET` | `/api/v1/sismos/recientes` | Últimos sismos detectados |
| `GET` | `/api/v1/sismos/estadisticas` | Estadísticas agregadas por año |
| `GET` | `/api/v1/sismos/heatmap` | Datos para mapa de calor (ScreenGridLayer) |
| `GET` | `/api/v1/sismos/cercanos` | Sismos cercanos a un punto (lon, lat, radio_km) |
| `GET` | `/api/v1/sismos/{usgs_id}` | Detalle de un sismo por ID USGS |
| `WS` | `/ws/sismos` | WebSocket en tiempo real para nuevos sismos |

### Administrativo (Geografía)

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/departamentos` | GeoJSON departamentos con zona sísmica (GADM 4.1) |
| `GET` | `/api/v1/distritos` | GeoJSON distritos con nivel_riesgo, peligro_*, IRC v9 |
| `GET` | `/api/v1/distritos/resumen` | Resumen estadístico de distritos |

### Geología

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/fallas` | 19 fallas geológicas IGP/Audin et al. 2008 |
| `GET` | `/api/v1/deslizamientos` | 10 zonas de deslizamiento activas (CENEPRED/INGEMMET) |
| `GET` | `/api/v1/volcanes` | 20 volcanes INGEMMET/OVI-IGP 2021 con estado y peligro |
| `GET` | `/api/v1/zonas-sismicas` | Zonificación sísmica NTE E.030 |
| `GET` | `/api/v1/zonas-sismicas/referencia` | Tabla de referencia Z1–Z4 |

### Hidrometeorología

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/inundaciones` | 12 zonas inundables (ANA/CENEPRED 2024) |
| `GET` | `/api/v1/tsunamis` | 9 zonas de riesgo de tsunami (PREDES/IGP/DHN 2024) |
| `GET` | `/api/v1/precipitaciones` | 22 zonas de precipitación con índice FEN |
| `GET` | `/api/v1/precipitaciones/cercanas` | Zonas cercanas a un punto |
| `GET` | `/api/v1/fen` | 22 eventos El Niño / La Niña (NOAA-CPC 1957–2024) |
| `GET` | `/api/v1/fen/estadisticas` | Estadísticas FEN agregadas |
| `GET` | `/api/v1/riesgo/lluvia` | Riesgo pluvial puntual (lon, lat) |

### Infraestructura

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/infraestructura` | GeoJSON: aeropuertos, puertos, hospitales, bomberos, centrales + OSM |
| `GET` | `/api/v1/infraestructura/cobertura` | Estadísticas cobertura oficial vs OSM |
| `GET` | `/api/v1/estaciones` | 34 estaciones de monitoreo (IGP, SENAMHI, ANA, DHN, IPEN) |

### Riesgo, ML y Escenarios

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/riesgo` | Riesgo integral puntual (lon, lat) |
| `GET` | `/api/v1/riesgo/punto` | Riesgo detallado con IRC v9 para un punto |
| `GET` | `/api/v1/riesgo/construccion/mapa` | GeoJSON IRC por distrito |
| `POST` | `/api/v1/riesgo/escenario` | Escenario de daño sísmico (magnitud, profundidad, mix constructivo) |
| `GET` | `/api/v1/susceptibilidad/{amenaza}` | Predicción ML puntual con SHAP local |
| `POST` | `/api/v1/susceptibilidad/entrenar` | Entrenar modelo ML para una amenaza |
| `GET` | `/api/v1/susceptibilidad/modelos` | Info de modelos entrenados |

### Sendai Framework

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/sendai/report` | Reporte completo Sendai (7 targets A–G) |
| `GET` | `/api/v1/sendai/mapa` | GeoJSON distritos con score por target Sendai |

### Alertas EWS

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/alertas/stream` | SSE (Server-Sent Events) en tiempo real |
| `GET` | `/api/v1/alertas/recap` | Últimas alertas emitidas |
| `GET` | `/api/v1/ews/stats` | Estadísticas del worker EWS |

### Caché y Rendimiento

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/api/v1/cache/stats` | Estadísticas Redis: hit rate, memoria, keys por endpoint |
| `DELETE` | `/api/v1/cache/flush` | Invalida keys por prefijo `gr:` (parámetro: `prefix`) |

---

## 🤖 Motor ML — Susceptibilidad

El módulo `ml_engine.py` implementa un pipeline completo de aprendizaje automático supervisado para la evaluación de susceptibilidad por amenaza natural.

### Amenazas Soportadas

| Amenaza | Features (8 por modelo) |
|---|---|
| `deslizamiento` | peligro_sismo, peligro_inundacion, peligro_deslizamiento, peligro_sequia, pendiente_media, altitud_media, precipitacion_anual, densidad_poblacional |
| `inundacion` | Mismos 8 features extraídos de la tabla `distritos` |
| `sequia` | Mismos 8 features extraídos de la tabla `distritos` |

### Pipeline ML v9.0

```
Datos (distritos) ──► VIF filter (umbral 10) ──► SMOTE-Tomek ──► Train/Test 80/20
                                                                          │
                                                                Optuna (20 trials)
                                                              Bayesian HP Search
                                                                          │
                                                          XGBClassifier (óptimo)
                                                                          │
                                          ┌───────────────────┬───────────┴───────────┐
                                          │                   │                       │
                                    5-Fold CV            SHAP                  Bootstrap CI
                                    AUC-ROC/PR        TreeExplainer           80% (100 iter)
                                                    Global + Local
```

### Etapas del Pipeline

1. **Extracción**: Consulta a tabla `distritos` con campos `peligro_*` vía asyncpg
2. **Target binario**: `1` si `peligro_{amenaza} ≥ 3`, `0` en caso contrario
3. **VIF Filter**: Eliminación de features con Variance Inflation Factor > 10 (multicolinealidad)
4. **SMOTE-Tomek**: Balanceo de clases con sobremuestreo sintético + limpieza de Tomek links
5. **Optuna**: 20 trials de optimización bayesiana (max_depth, n_estimators, learning_rate, subsample, colsample_bytree, min_child_weight, gamma, reg_alpha, reg_lambda)
6. **Cross-validation**: 5-Fold StratifiedKFold con métricas AUC-ROC y AUC-PR
7. **SHAP**: TreeExplainer para importancia global de features + explicaciones locales por predicción
8. **Persistencia**: Modelo `.pkl` en volumen Docker `models_data`
9. **Metadata**: UPSERT en tabla `modelo_metadata` (AUC, features, hiperparámetros, timestamp)

### Predicción

- `predict_point(lon, lat, amenaza, conn)` → `{score, nivel, score_p10, score_p90, shap_local}`
- **Bootstrap CI**: 100 iteraciones con ruido feature-scaled (5% de magnitud, `np.random.default_rng()`)
- **SHAP Local**: Contribución de cada feature a la predicción individual
- **Validación**: Coordenadas validadas contra bounding box de Perú
- **Fallback heurístico**: Si no hay modelo entrenado, calcula score desde datos del distrito más cercano

### Configuración

```bash
ML_AUTO_TRAIN=1       # Entrenar automáticamente tras ETL
ML_MIN_SAMPLES=10     # Mínimo de muestras para entrenar
```

---

## 💥 Modelo de Daño Sísmico

El módulo `damage_model.py` simula escenarios de pérdidas sísmicas utilizando metodologías reconocidas internacionalmente.

### Metodología

| Componente | Método | Fuente |
|---|---|---|
| **Atenuación PGA** | Youngs et al. 1997 — intraslab (>70 km) / interface (≤70 km) | Youngs et al. (1997) SRL 68(1):58–73 |
| **PGA Range** | Clampeado a [0.001g, 3.0g] | — |
| **Fragilidad** | Curvas lognormales por taxonomía GEM (6 tipologías) | GEM GTX 2023, Tarque et al. 2012, FEMA P-58, HAZUS MR4 |
| **Estados de daño** | Ninguno, Leve, Moderado, Severo, Colapso | — |
| **Multi-taxonomía** | Mix constructivo ponderado por distrito | CAPECO 2023, INEI CPV 2017 |
| **Factor hora** | Mortalidad 1.4× nocturna | Coburn et al. (1992) |
| **Validación** | Coordenadas Perú, mag [3.0, 9.5], profundidad [0, 700 km], mecanismo válido | — |

### Taxonomías de Fragilidad

| Taxonomía | Descripción | Media log PGA (Colapso) | Fuente |
|---|---|---|---|
| ADOBE | Mampostería de adobe 1 piso | 0.35g | Tarque et al. 2012 |
| URM | Mampostería no reforzada | 0.50g | GEM GTX 2023 |
| CM | Mampostería confinada | 0.65g | GEM GTX 2023 |
| RC | Concreto armado | 0.80g | GEM GTX 2023 |
| WOOD | Madera | 0.90g | FEMA P-58 |
| INFRA | Infraestructura crítica | 1.20g | HAZUS MR4 |

### Funciones Principales

| Función | Descripción |
|---|---|
| `pga_youngs97()` | Cálculo de PGA via Youngs et al. 1997 (selección automática interface/intraslab) |
| `fragility_probability()` | Probabilidad P(DS ≥ ds) por taxonomía con distribución lognormal |
| `scenario_losses()` | Escenario completo: PGA sitio, distribución de daño, pérdidas económicas, mortalidad |
| `scenario_from_sismo()` | Escenario automático a partir de un sismo USGS existente |
| `batch_losses()` | Evaluación batch para múltiples distritos |

---

## ⚡ Early Warning System (EWS)

El módulo `alert_worker.py` implementa un sistema de alerta temprana multi-amenaza en tiempo real, alineado con los 4 pilares de EW4All (UNDRR 2022).

### Características

| Feature | Detalle |
|---|---|
| **Polling** | USGS + IGP cada 30s para sismos recientes (últimos 5 min) |
| **Cascada** | Tsunami (M ≥ 6.5 + costa < 50 km), Deslizamiento (M ≥ 5.0 + peligro ≥ 3) |
| **Niveles** | `emergency` (M ≥ 7.0), `warning` (M ≥ 5.5), `watch` (M ≥ 4.0) |
| **Difusión** | SSE + WebSocket simultáneo |
| **Protocolo** | CAP v1.2 (Common Alerting Protocol) estándar OASIS |
| **EW4All** | 4 pilares: Conocimiento del Riesgo, Monitoreo, Difusión, Preparación |
| **Estadísticas** | Contadores polls_ok/err, alertas enviadas, clientes conectados |
| **Memoria** | Set de IDs vistos con poda automática (máx. 10,000 entradas) |

### Flujo de Alerta

```
USGS/IGP API ──► Polling 30s ──► ¿Nuevo sismo? ──► Calcular nivel ──► Evaluar cascada
                                       │                              │
                                       No                             │
                                       ▼                              ▼
                                    Esperar                      ¿Tsunami? ──► Alerta tsunami
                                                                  ¿Desliz? ──► Alerta deslizamiento
                                                                       │
                                                                       ▼
                                                          SSE + WebSocket ──► Frontend
                                                          CAP v1.2 JSON  ──► Toast + Mapa
```

---

## 🔄 ETL Pipeline

El módulo `procesar_datos.py` implementa un pipeline ETL de **20 pasos** con dependencias entre ellos, diseñado para robustez y resiliencia.

### Pasos del Pipeline

| # | Paso | Fuente | Dependencias | Descripción |
|---|---|---|---|---|
| 1 | Departamentos | GADM 4.1 + 25 fallback bboxes | — | Límites departamentales |
| 2 | Sismos | USGS FDSNWS M≥2.5 1900–hoy (22 bloques paralelos) | — | Catálogo sísmico histórico |
| 3 | Distritos | INEI WFS → GADM L3 (75 fallback bboxes) | 1 | Geometría distrital |
| 4 | Fallas | 19 fallas INGEMMET/IGP (Audin et al. 2008) | — | Fallas geológicas activas |
| 5 | Inundaciones | 12 zonas ANA/CENEPRED 2024 | — | Zonas inundables |
| 6 | Tsunamis | 9 zonas PREDES/IGP/DHN 2024 | — | Zonas de tsunami |
| 7 | Deslizamientos | 10 zonas CENEPRED/INGEMMET 2024 | — | Zonas de deslizamiento |
| 8 | Infraestructura | MTC + APN + OSINERGMIN + MINSA + OSM | 1 | Activos estratégicos |
| 9 | Estaciones | 34 estaciones IGP+SENAMHI+ANA+DHN+IPEN | — | Red de monitoreo |
| 10 | Precipitaciones | 22 zonas SENAMHI/CHIRPS 1981–2020 | — | Zonas climáticas |
| 11 | Eventos FEN | 22 eventos ENSO NOAA-CPC 1957–2024 | — | Historia El Niño/La Niña |
| 12 | Volcanes | 20 volcanes INGEMMET/OVI-IGP 2021 | 1 | Catálogo volcánico |
| 13 | Sequía SPI-12 | McKee et al. 1993 / CHIRPS por zona | 10 | Índice de sequía |
| 14 | Cascada | Gill & Malamud 2014, α=0.15 | 4,7 | Factor sismo→deslizamiento |
| 15 | IRC v9 | 7 amenazas + bootstrap 500 iter | 12,13,14 | Índice compuesto |
| 16 | Exposición/IVS | GEM 2023 + INEI 2017 + MIDIS 2022 | 15 | Exposición y vulnerabilidad |
| 17 | Heatmap | REFRESH MV heatmap | 2 | Mapa de calor sísmico |
| 18 | Regiones | f_actualizar_regiones() | 1,3 | Zonificación sísmica |
| 19 | Riesgo construcción | REFRESH MV riesgo_construccion | 18 | Riesgo constructivo NTE E.031 |
| 20 | Sendai | Snapshot 7 targets SFDRR | 15 | Alineación Sendai |

### Robustez del ETL

- **Retry con jitter exponencial**: Reintentos con backoff exponencial y jitter aleatorio para requests HTTP (tenacity)
- **Circuit breaker Overpass**: 3 endpoints con rotación automática por fallos
- **COPY FROM buffer**: Inserciones masivas vía PostgreSQL COPY con auto-flush cada 10,000 filas
- **Coordenadas validadas**: Sismos verificados contra rango [-180, 180] × [-90, 90] antes de inserción
- **Materialized view allowlist**: REFRESH solo permitido contra vistas en whitelist (`_ALLOWED_MATVIEWS`)
- **Progreso por paso**: Logging estructurado con porcentaje de avance y warnings por errores
- **Modo validación**: Flag `--validate` para diagnóstico de integridad sin ejecutar ETL

### Opciones CLI

```bash
# Ejecutar ETL completo
python procesar_datos.py

# Solo pasos específicos
python procesar_datos.py --solo volcanes sequia_spi irc_v9

# Omitir pasos lentos
python procesar_datos.py --skip infraestructura sismos

# Modo simulación (sin escritura a BD)
python procesar_datos.py --dry-run

# Ajustar paralelismo e iteraciones
python procesar_datos.py --workers 8 --bootstrap-n 1000 --verbose

# Validar integridad de datos
python procesar_datos.py --validate
```

---

## 🏗️ IRC v9 — Índice de Riesgo Compuesto

<div align="center">

![Riesgo por Distrito IRC](imagenes/riesgodistrito-irc.png)
*Mapa coroplético distrital del IRC v9 con clasificación de suelo NTE E.031-2020 (S0–S4) y MMI estimada por distrito.*

</div>

El IRC v9 combina **7 amenazas naturales** con un **factor de cascada** e **incertidumbre por bootstrapping**, proporcionando una métrica única y robusta del riesgo compuesto a nivel distrital.

### Fórmula General

$$IRC_{v9} = \left( \sum_{i=1}^{7} w_i \cdot P_i \right) \times F_{\text{cascada}}$$

Donde:
- \(P_i\): Peligro normalizado [0–5] de la amenaza \(i\)
- \(w_i\): Peso específico de cada amenaza
- \(F_{\text{cascada}}\): Factor multiplicador por interacción entre amenazas

### Pesos (CENEPRED 2014 + SENCICO E.030 2018)

| Amenaza | Peso \(w_i\) | Justificación |
|---|---|---|
| Sismo (S) | 35% | Amenaza dominante — Cinturón de Fuego del Pacífico |
| Inundación (I) | 20% | FEN + variabilidad climática andina |
| Deslizamiento (D) | 18% | Segunda causa de víctimas históricas en Perú |
| Tsunami (T) | 10% | Costa de alta densidad poblacional |
| Volcán (V) | 8% | Sur peruano (Ubinas, Sabancaya, Misti) |
| Sequía (Q) | 5% | Altiplano y sierra sur |
| Fallas activas (F) | 4% | Proxy de intensidad local (capped a 5) |

### Factor de Cascada (Gill & Malamud 2014)

$$F_{\text{cascada}} = 1.0 + 0.15 \times \frac{P_S}{5} \times \frac{P_D}{5}$$

Calibrado con inventario CENEPRED post-sismo Pisco M8.0 2007. Modela la interacción sismo → deslizamiento.

### Bandas de Incertidumbre (Li et al. 2023)

- 500 iteraciones bootstrap con perturbación ±10% en los pesos (normalización a suma=1)
- Percentiles p10 y p90 almacenados en `irc_v9_p10` / `irc_v9_p90`
- Intervalos de confianza del 80% para cada distrito

### Clasificación de Suelos NTE E.031-2020 (v9.1)

La vista `mv_riesgo_construccion` incluye clasificación de perfil de suelo por distrito conforme a NTE E.031-2020 (equivalente a ASCE 7-22 / EN 1998):

| Clase | Descripción | Vs30 (m/s) | Factor Fa |
|---|---|---|---|
| S0 | Roca dura | > 1500 | 0.80 |
| S1 | Roca o suelo muy rígido | 500–1500 | 1.00 |
| S2 | Suelos intermedios | 180–500 | 1.20 |
| S3 | Suelos blandos | < 180 | 1.40 |
| S4 | Condiciones especiales | — | 1.60 |

Campos incluidos en `/api/v1/riesgo/construccion/mapa`: `clasificacion_suelo`, `factor_suelo_s`, `tp_suelo`, `tl_suelo`, `mmi_estimada`, `mag_max_cercana_50km`, `dist_epicentro_km`.

---

## 🎨 Frontend — Mapa Interactivo

<div align="center">

![Deslizamientos, Tsunami e Infraestructura](imagenes/dezlisaminetosunamieinfra.png)
*Mapa interactivo con capas de deslizamientos, tsunamis e infraestructura crítica (hospitales, aeropuertos, puertos, bomberos) sobre MapLibre GL + deck.gl.*

</div>

### Capas del Mapa (15+)

| Capa | deck.gl Layer | Descripción | Carga |
|---|---|---|---|
| Sismos | ScatterplotLayer | Puntos coloreados por profundidad, radio ∝ magnitud | Inicial |
| Heatmap | ScreenGridLayer | Mapa de calor ponderado por magnitud | Inicial |

<div align="center">

![Registro Histórico Sísmico](imagenes/registrohistoricosismo.png)
*Visualización de sismos históricos (USGS 1900–hoy) con ScreenGridLayer ponderado por magnitud — actualizado en tiempo real vía WebSocket `/ws/sismos`.*

</div>
| Departamentos | GeoJsonLayer | Polígonos con zona sísmica E.030 | Lazy |
| Distritos | GeoJsonLayer | Nivel de riesgo 1–5 (coropleta) | Inicial |
| IRC Mapa | GeoJsonLayer | Índice de Riesgo Construcción v9 | Inicial |
| Fallas | GeoJsonLayer | Líneas con buffer visual | Inicial |
| Inundaciones | GeoJsonLayer | Zonas inundables con tipo y período retorno | Lazy |
| Tsunamis | GeoJsonLayer | Franja costera con altura de ola y tiempo de arribo | Lazy |
| Deslizamientos | GeoJsonLayer | Zonas activas con tipo y causa | Lazy |
| Precipitaciones | GeoJsonLayer | 22 zonas climáticas + índice FEN | Inicial |
| Volcanes | ScatterplotLayer | 20 volcanes con estado y radio de peligro | Inicial |
| Susceptibilidad ML | ScatterplotLayer | Scores de modelo ML con IC bootstrap | Inicial |
| Alertas EWS | ScatterplotLayer | Alertas en tiempo real con efecto de pulsación | Inicial |
| Estaciones | ScatterplotLayer | 34 estaciones de monitoreo | Lazy |
| Extrusión 3D | ColumnLayer | Centroides de distritos extruidos por nivel_riesgo | Inicial |
| Infraestructura | GeoJsonLayer | Aeropuertos, puertos, hospitales, bomberos | Lazy |

### Panel de Filtros

- **Presets rápidos**: Todo · Recientes · Fuertes ≥6 · Pisco 2007 · Grandes ≥7
- **Filtros sísmicos**: Magnitud, año, región, profundidad
- **Filtros ML**: Score mínimo, amenaza seleccionada
- **Filtros precipitación**: Riesgo de inundación mínimo
- **Fuente de datos**: Todos / Oficial / OSM

### Gráficos (StatsChart)

- Histograma sísmico por año (barras/línea)
- Ranking IRC por distrito (top N)
- Eventos FEN históricos con intensidad
- Escenario de daño sísmico (distribución de estados de daño, pérdidas)
- Reporte Sendai (7 targets con indicadores proxy)

### Lazy Loading de Capas

| Capas iniciales | Capas lazy (carga al activar) |
|---|---|
| Sismos, Distritos, Fallas, Estadísticas, Volcanes, Precipitaciones, ML, Alertas, 3D | Departamentos, Inundaciones, Tsunamis, Deslizamientos, Infraestructura, Estaciones |

Esto reduce la carga inicial en ~60% de ancho de banda y mejora significativamente el tiempo de primer renderizado.

### Error Handling

- **ErrorBoundary**: Componente React class-based que envuelve MapView y StatsChart con UI de recuperación y botón de reset
- **API Service**: Reintentos automáticos (3 intentos, backoff exponencial), deduplicación de requests inflight, caché con ETag, parsing estructurado de errores

### Accesibilidad

- ARIA landmarks: `role="banner"` en header, `role="complementary"` en sidebar
- `aria-label` descriptivos en controles principales
- `role="alert"` en errores de ErrorBoundary y notificaciones

---

## 🔒 Seguridad y Buenas Prácticas

| Medida | Implementación |
|---|---|
| **CORS** | Orígenes configurados desde variable `CORS_ORIGINS` (no wildcard en producción) |
| **Security Headers** | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: strict-origin-when-cross-origin` |
| **SQL Injection** | Consultas parametrizadas en todos los endpoints; tablas de diagnóstico validadas contra `frozenset` |
| **Materialized Views** | Allowlist (`_ALLOWED_MATVIEWS`) para prevenir inyección en operaciones REFRESH |
| **Rate Limiting** | slowapi con límites por IP en endpoints sensibles (100 requests/min) |
| **Exception Handling** | Manejador global que no expone stack traces al cliente en producción |
| **Input Validation** | Coordenadas, magnitudes, profundidades y mecanismos validados en todos los modelos |
| **Docker Security** | Contenedores sin root, imágenes slim, capas minimizadas |
| **Secrets** | Variables de entorno para credenciales, no hardcoded |

---

## 💻 Desarrollo Local

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate      # Windows
pip install -r requirements.txt

# Necesita PostgreSQL + PostGIS + Redis corriendo
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
npm run dev    # http://localhost:5173 con proxy a :8000
```

> El `vite.config.ts` incluye proxy `/api/*` → `http://localhost:8000` para desarrollo.

### Verificación

```bash
cd frontend
npm run build    # Compila TypeScript + verifica tipos
npm run lint     # ESLint
```

---

## 📡 Fuentes de Datos

| Capa | Fuente | Tipo de Acceso |
|---|---|---|
| Sismos históricos | USGS FDSN Web Services (earthquake.usgs.gov) | API REST |
| Departamentos | GADM 4.1 (geodata.ucdavis.edu) | GeoJSON |
| Distritos | INEI WFS + GADM 4.1 L3 | WFS / GeoJSON |
| Fallas geológicas | INGEMMET/IGP (Audin et al. 2008) | Catálogo hardcoded |
| Volcanes (20) | INGEMMET "Mapa de Peligros Volcánicos" 2da ed. 2021 / OVI-IGP | Catálogo |
| Zonas inundables | ANA + CENEPRED 2024 | Hardcoded |
| Zonas tsunami | PREDES/IGP/DHN 2024 | Hardcoded |
| Deslizamientos | CENEPRED/INGEMMET 2024 | Hardcoded |
| Precipitaciones | SENAMHI + CHIRPS v2.0 (1981–2020) | Hardcoded |
| Eventos FEN/ENSO | NOAA-CPC / ENFEN | Catálogo |
| SPI-12 | McKee et al. 1993 / CHIRPS por zona climática | Calculado |
| Infraestructura | MTC/CORPAC + APN + OSINERGMIN/MINEM + MINSA/SUSALUD + CGBVP + OSM Overpass | API + WFS |
| Estaciones | IGP (RSN) + SENAMHI (RMN) + ANA (RHN) + DHN (DART) + IPEN + INDECI (COEN) | Catálogo |
| Exposición | GEM Global Exposure Model 2023 (Yepes-Estrada et al.) | Académico |
| Vulnerabilidad | INEI CPV 2017 + MIDIS SISFOH 2022 | Censo / Registro |
| Costos reposición | CAPECO 2023 | Sectorial |
| Zonificación sísmica | NTE E.030 (SENCICO 2018) | Norma técnica |
| Marco Sendai | UNDRR Sendai Framework Monitor 2015–2030 | Metodología |

---

## 📚 Referencias Científicas

| Referencia | Aplicación en el Proyecto |
|---|---|
| Youngs, R.R. et al. (1997) "Strong Ground Motion Attenuation Relationships for Subduction Zone Earthquakes". *SRL* 68(1):58–73 | Modelo de atenuación PGA en `damage_model.py` |
| McKee, T.B. et al. (1993) "The Relationship of Drought Frequency and Duration to Time Scales". 8th AMS Conference on Applied Climatology | SPI-12 sequía en `procesar_datos.py` |
| Gill, J.C. & Malamud, B.D. (2014) "Reviewing and visualizing the interactions of natural hazards". *Reviews of Geophysics* 52(4):680–722 | Factor cascada sismo→deslizamiento en IRC v9 |
| Li, Z. et al. (2023) "Uncertainty in multi-hazard risk index". *Nat. Hazards Earth Syst. Sci.* | Bootstrap IC para IRC v9 |
| Yepes-Estrada, C. et al. (2023) GEM Global Exposure Model. *Earthquake Spectra* | Taxonomía constructiva por región |
| Tarque, N. et al. (2012) Fragilidad sísmica de edificaciones de adobe en Perú | Curvas de fragilidad adobe |
| Kumar, S. et al. (2023) Seismic PML estimation techniques | Estimación de pérdida monetaria probable |
| Coburn, A. et al. (1992) Factors determining casualties in earthquakes | Factor mortalidad nocturna (1.4×) |
| CENEPRED (2014) Manual para la Evaluación de Riesgos | Pesos base del IRC |
| SENCICO (2018) Norma Técnica de Edificación E.030 | Zonificación sísmica Z1–Z4 |
| UNDRR (2022) Early Warnings for All (EW4All) | 4 pilares del sistema EWS |
| WMO (2012) SPI User Guide (WMO-No. 1090) | Clasificación de sequía por SPI |
| Wald, D.J. et al. (1999) "Relationships between PGA, PGV, and MMI". *Earthquake Spectra* 15(3):557–564 | Conversión PGA→MMI en InfoPopup y MV |
| Toppozada, T. (1975) "Earthquake Magnitude as a Function of Intensity Data". *BSSA* 65(5):1223–1238 | Lookup de intensidad percibida por magnitud |
| Gutenberg, B. & Richter, C.F. (1956) Magnitude and Energy of Earthquakes. *Annali di Geofisica* 9:1–15 | Cálculo de energía sísmica en popup |
| SENCICO (2020) Norma Técnica de Edificación E.031 — Suelos con Condiciones Especiales | Clasificación S0–S4 y factores Fa/Fv |

---

## 📋 Changelog v9.1

| Componente | Mejora |
|---|---|
| `backend/init.sql` | Vista `mv_riesgo_construccion` reescrita con CTEs NTE E.031-2020: nuevas columnas de suelo S0–S4 + `mmi_estimada` |
| `backend/Dockerfile` | Build multi-stage: etapa `builder` compila wheels C/C++, etapa `runtime` usa 3.12-slim (~100 MB de ahorro) |
| `backend/main.py` | Cache warming en `lifespan` (volcanes + IRC mapa). Nuevos endpoints `/api/v1/cache/stats` y `DELETE /api/v1/cache/flush` |
| `backend/cache.py` | Método `stats()` con métricas Redis (hit rate, memoria, keys por endpoint). Método `mset_pipeline()` para escrituras batch |
| `backend/procesar_datos.py` | Flag `--validate` para diagnóstico de integridad. Barra de progreso visual. Cobertura IRC en resumen |
| `frontend/useMapData.ts` | Lazy loading: 6 capas deferred. Nuevos callbacks exportados por capa |
| `frontend/App.tsx` | 6 `useEffect` que disparan carga lazy al activar cada capa |
| `frontend/InfoPopup.tsx` | Sismos: MMI coloreado, energía J, radio percepción. IRC/suelo: card S0–S4 + Fs + MMI estimada |
| `frontend/constants.ts` | Sistema de diseño completo: `SUELO_COLORS`, `mmiColor()`, escala tipográfica `T`, escala espaciado `S` |

---

## ⌨️ Atajos de Teclado

| Tecla | Acción |
|---|---|
| `L` | Toggle sidebar (panel lateral) |
| `F` | Abrir/cerrar panel de filtros |
| `G` | Toggle gráfica (StatsChart) |
| `R` | Centrar/restablecer vista del mapa |
| `Esc` | Cerrar popup activo |

---

## 📄 Licencia

**Proyecto académico** — Noveno ciclo de Ingeniería. Datos provenientes de fuentes públicas gubernamentales.

Datos sísmicos © USGS (dominio público). Datos geoespaciales © IGP, INGEMMET, ANA, INEI, SENAMHI (sujetos a términos de uso institucionales). GEM Global Exposure Model © GEM Foundation 2023.

---

*Plataforma de evaluación de riesgos geoespaciales multi-amenaza para el territorio peruano — alineada con el Marco de Sendai para la Reducción del Riesgo de Desastres 2015–2030.*
