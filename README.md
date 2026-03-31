# 🌋 GeoRiesgo Perú v9.0 — Plataforma Multi-Amenaza con ML y EWS

> Plataforma geoespacial multi-amenaza para el análisis integral de riesgos naturales en Perú. Integra sismicidad, vulcanismo, deslizamientos, inundaciones, tsunamis, precipitaciones FEN, sequía SPI-12, modelos de susceptibilidad ML (XGBoost + SHAP + Optuna) y sistema de alerta temprana (EWS) en tiempo real con alineación Sendai 2015–2030.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178C6?logo=typescript&logoColor=white)](https://typescriptlang.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17_PostGIS-4169E1?logo=postgresql&logoColor=white)](https://postgis.net)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![XGBoost](https://img.shields.io/badge/XGBoost-ML-FF6600?logo=xgboost&logoColor=white)](https://xgboost.readthedocs.io)
[![Sendai](https://img.shields.io/badge/Sendai-2015--2030-00A1E0)](https://www.undrr.org/implementing-sendai-framework)

---

## 📑 Contenidos

1. [Descripción General](#-descripción-general)
2. [Arquitectura del Sistema](#-arquitectura-del-sistema)
3. [Stack Tecnológico](#-stack-tecnológico)
4. [Estructura del Proyecto](#-estructura-del-proyecto)
5. [Inicio Rápido](#-inicio-rápido-con-docker)
6. [API REST — Endpoints](#-api-rest--endpoints)
7. [Motor ML — Susceptibilidad](#-motor-ml--susceptibilidad)
8. [Modelo de Daño Sísmico](#-modelo-de-daño-sísmico)
9. [Early Warning System (EWS)](#-early-warning-system-ews)
10. [ETL Pipeline](#-etl-pipeline)
11. [IRC v9 — Índice de Riesgo Compuesto](#-irc-v9--índice-de-riesgo-compuesto)
12. [Frontend](#-frontend)
13. [Seguridad](#-seguridad)
14. [Desarrollo Local](#-desarrollo-local)
15. [Fuentes de Datos](#-fuentes-de-datos)
16. [Referencias Científicas](#-referencias-científicas)

---

## 📌 Descripción General

**GeoRiesgo Perú** es una plataforma web full-stack de evaluación integral de riesgos por amenazas naturales. Combina datos geoespaciales de 15+ fuentes oficiales (USGS, IGP, INGEMMET, SENAMHI, NOAA-CPC, GEM, CENEPRED, INDECI, ANA, CAPECO, MIDIS, INEI, OVI-IGP, DHN, UNDRR) en una interfaz unificada con análisis en tiempo real.

### Capacidades Principales

| Módulo                       | Descripción |
|------------------------------|-------------|
| 🗺️ Mapa interactivo          | MapLibre GL + deck.gl con 15+ capas WebGL aceleradas por GPU |
| 🌐 Vista 2D / 3D             | Perspectiva aérea con extrusión ColumnLayer por nivel de riesgo |
| 🔥 Heatmap sísmico           | Mapa de calor con ponderación por magnitud (ScreenGridLayer) |
| 🌋 Volcanes                  | 20 volcanes INGEMMET/OVI-IGP 2021 con radios de peligro por estado |
| ⛰️ Deslizamientos            | 10 zonas activas con clasificación tipo/causa (CENEPRED/INGEMMET) |
| 🌊 Tsunamis e inundaciones   | 9 zonas tsunami + 12 zonas inundables con períodos de retorno |
| 🌧️ Precipitaciones + FEN     | 22 zonas climáticas CHIRPS, índice El Niño, 22 eventos FEN 1957–2024 |
| 🏜️ Sequía SPI-12             | Standardized Precipitation Index (McKee et al. 1993 / CHIRPS 1981–2020) |
| 🤖 ML Susceptibilidad        | XGBoost + VIF + SMOTE-Tomek + Optuna (20 trials) + SHAP TreeExplainer |
| ⚡ EWS Alertas               | Early Warning System tiempo real (SSE + WebSocket + CAP v1.2) |
| 🏗️ IRC v9                    | Índice de Riesgo Compuesto: 7 amenazas + factor cascada + bootstrap IC |
| 💥 Modelo de Daño            | Atenuación Youngs et al. 1997 + fragilidad lognormal multi-taxonomía |
| 📊 Sendai Framework          | 7 targets proxy alineados con SFDRR/EW4All (UNDRR 2022) |
| 🎚️ Escenarios sísmicos       | Simulación pérdidas por magnitud, profundidad, hora, mix constructivo |
| ⌨️ Atajos de teclado          | `[L]` sidebar · `[F]` filtros · `[G]` gráfica · `[R]` centrar |

---

## 🏗 Arquitectura del Sistema

```
┌──────────────────────────────────────────────────────────────────┐
│                         DOCKER COMPOSE                           │
│                                                                  │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │  FRONTEND    │   │   BACKEND         │   │   PostgreSQL   │  │
│  │  React 19    │   │   FastAPI v9.0    │   │   17 + PostGIS │  │
│  │  Vite 7      │   │   ML Engine       │   │   3.6          │  │
│  │  deck.gl 9.2 │──►│   EWS Worker      │──►│   20+ tablas   │  │
│  │  nginx :80   │   │   Damage Model    │   │   :5432        │  │
│  └──────────────┘   │   uvicorn :8000   │   └────────────────┘  │
│                     └────────┬─────────┘                         │
│                              │                                   │
│                     ┌────────▼─────────┐   ┌────────────────┐   │
│                     │   Redis 7        │   │  models_data   │   │
│                     │   Cache LRU 128MB│   │  (XGBoost .pkl)│   │
│                     │   :6379          │   └────────────────┘   │
│                     └──────────────────┘                         │
└──────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos

1. **Primer arranque**: ETL (`procesar_datos.py`) descarga datos de USGS FDSNWS, GADM 4.1, INGEMMET, IGP, ANA y los carga en PostgreSQL/PostGIS (20 pasos secuenciales con dependencias)
2. **PostGIS**: Toda la data reside en tablas geoespaciales con índices GIST/SP-GiST para consultas espaciales eficientes
3. **Redis**: Cache LRU de 128 MB para respuestas GeoJSON frecuentes (TTL configurable por endpoint)
4. **ML Engine**: Entrena modelos XGBoost por amenaza tras el ETL (si `ML_AUTO_TRAIN=1`) con pipeline VIF → SMOTE-Tomek → Optuna → SHAP
5. **EWS Worker**: Monitorea USGS/IGP en tiempo real, emite alertas vía SSE/WebSocket con cascada (tsunami/deslizamiento), protocolo CAP v1.2
6. **Damage Model**: Simula escenarios sísmicos con atenuación Youngs 1997, fragilidad multi-taxonomía y factor hora del día
7. **Frontend**: React consume la API REST y renderiza 15+ capas deck.gl sobre MapLibre GL con ErrorBoundary por sección

---

## 🛠 Stack Tecnológico

### Backend
| Tecnología       | Versión   | Uso |
|-----------------|-----------|-----|
| Python           | 3.11      | Runtime |
| FastAPI          | 0.115     | Framework API REST asíncrono |
| Uvicorn          | 0.31      | Servidor ASGI |
| asyncpg          | —         | Driver PostgreSQL asíncrono (prepared statements para 5 queries frecuentes) |
| XGBoost          | —         | Modelos de susceptibilidad ML |
| scikit-learn     | —         | Pipeline ML + métricas + StratifiedKFold |
| SHAP             | —         | TreeExplainer para explicabilidad local/global |
| Optuna           | —         | Optimización bayesiana de hiperparámetros (20 trials) |
| imbalanced-learn | —         | SMOTE-Tomek para balanceo de clases |
| NumPy            | 2.x       | Procesamiento numérico + bootstrap IC |
| Redis (aioredis) | —         | Cache distribuida LRU 128 MB |
| httpx / tenacity | —         | HTTP asíncrono + reintentos exponenciales |
| slowapi          | —         | Rate limiting por IP |
| orjson           | —         | Serialización JSON rápida |

### Frontend
| Tecnología        | Versión   | Uso |
|------------------|-----------|-----|
| React             | 19        | UI Framework con ErrorBoundary |
| TypeScript        | 5.9       | Tipado estático |
| Vite              | 7         | Build tool + HMR |
| MapLibre GL JS    | 5.19      | Motor de mapas WebGL |
| deck.gl           | 9.2       | Capas geoespaciales (Scatterplot, GeoJson, Column, ScreenGrid) |
| Recharts          | 3.7       | Gráficos estadísticos |
| Tailwind CSS      | 3.4       | Utilidades CSS |

### Infraestructura
| Tecnología           | Uso |
|---------------------|-----|
| Docker Compose       | Orquestación de 4 servicios |
| PostgreSQL 17        | Base de datos principal |
| PostGIS 3.6          | Extensión geoespacial con índices GIST/SP-GiST |
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
│   ├── init.sql                    # DDL PostgreSQL (20+ tablas + PostGIS + vistas materializadas)
│   ├── main.py                     # API FastAPI — 30+ endpoints REST + WS + SSE
│   ├── ml_engine.py                # Motor ML: XGBoost + VIF + SMOTE-Tomek + Optuna + SHAP
│   ├── alert_worker.py             # EWS Worker — USGS/IGP tiempo real + CAP v1.2
│   ├── damage_model.py             # Modelo de daño: Youngs 1997 + fragilidad multi-taxonomía
│   ├── procesar_datos.py           # ETL v9.0: 20 pasos con dependencias + bootstrap IRC
│   ├── cache.py                    # Decorador cache Redis con TTL
│   ├── stac_catalog.py             # Catálogo STAC (opcional, requiere MinIO)
│   └── requirements.txt
├── frontend/
│   ├── Dockerfile                  # Build multi-stage React → nginx
│   ├── nginx.conf                  # Proxy /api/ → backend:8000
│   ├── vite.config.ts              # Config Vite + proxy dev
│   └── src/
│       ├── App.tsx                 # Layout principal + ErrorBoundary + aria landmarks
│       ├── components/
│       │   ├── ErrorBoundary.tsx   # React ErrorBoundary con reset y logging
│       │   ├── MapView.tsx         # Mapa MapLibre + 15 capas deck.gl + 3D extrusion
│       │   ├── LayerPanel.tsx      # Panel de capas con toggles y leyendas
│       │   ├── FilterPanel.tsx     # Filtros sismos + precipitación + ML + EWS
│       │   ├── StatsChart.tsx      # Gráficos: histograma, IRC ranking, FEN, Sendai
│       │   ├── Landingpage.tsx     # Página de bienvenida con estadísticas y fuentes
│       │   ├── InfoPopup.tsx       # Popup detallado por entidad (12+ tipos)
│       │   ├── HoverTooltip.tsx    # Tooltip de hover rápido
│       │   ├── RiesgoPanel.tsx     # Panel lateral de riesgo puntual
│       │   ├── Loader.tsx          # Pantalla de carga con progreso
│       │   ├── ToastList.tsx       # Notificaciones toast
│       │   └── ui/                 # Badges, Btn, Icons, Row, constants
│       ├── hooks/
│       │   └── useMapData.ts       # Hook central de carga de datos (14+ estados)
│       ├── services/
│       │   └── api.ts              # Cliente HTTP tipado con cache, ETag, retry, deduplicación
│       └── types/
│           └── index.ts            # 50+ interfaces TypeScript
```

---

## 🐳 Inicio Rápido con Docker

### Requisitos
- Docker >= 24.0
- Docker Compose >= 2.20
- 4 GB de RAM disponibles (PostGIS + XGBoost + Optuna requieren ~2.5 GB en total)

### Levantar la plataforma completa

```bash
git clone <repo-url>
cd georiesgo-peru

# Construir y levantar (primer arranque incluye ETL ~5-10 min)
docker compose up --build

# En background
docker compose up --build -d
```

> ⏱️ **Primer arranque**: El backend ejecutará el ETL completo (20 pasos): descarga de sismos USGS (1900–hoy en 22 bloques paralelos), datos IGP/INGEMMET/ANA/GADM, cálculo de IRC v9 con bootstrap 500 iteraciones, y entrenamiento ML automático. Puede tardar 5–15 minutos. Los modelos ML se persisten en el volumen `models_data`.

### Variables de Entorno

| Variable          | Default              | Descripción |
|-------------------|----------------------|-------------|
| `DB_PASSWORD`     | `georiesgo_secret`   | Contraseña PostgreSQL |
| `CORS_ORIGINS`    | `http://localhost:5173,http://localhost:3000,http://localhost` | Orígenes CORS permitidos (separados por coma) |
| `ML_AUTO_TRAIN`   | `1`                  | Entrenar ML tras ETL |
| `ML_MIN_SAMPLES`  | `10`                 | Muestras mín. para entrenar |
| `LOG_LEVEL`       | `INFO`               | Nivel de logging |
| `WORKERS`         | `2`                  | Workers uvicorn |
| `FORCE_SYNC`      | `0`                  | Forzar re-ETL completo |
| `ETL_WORKERS`     | `4`                  | Workers paralelos del ETL |
| `ETL_BOOTSTRAP_N` | `500`                | Iteraciones bootstrap IRC v9 |

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
docker compose logs -f backend    # Logs del backend + ML + ETL
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
| `GET`  | `/api/v1/sismos/heatmap` | Datos para mapa de calor (ScreenGridLayer) |
| `GET`  | `/api/v1/sismos/cercanos` | Sismos cercanos a un punto (lon, lat, radio_km) |
| `GET`  | `/api/v1/sismos/{usgs_id}` | Detalle de un sismo por ID USGS |
| `WS`   | `/ws/sismos` | WebSocket tiempo real de sismos nuevos |

### Administrativo

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/departamentos` | GeoJSON departamentos con zona sísmica (GADM 4.1) |
| `GET`  | `/api/v1/distritos` | GeoJSON distritos con nivel_riesgo, peligro_*, IRC v9 |
| `GET`  | `/api/v1/distritos/resumen` | Resumen estadístico de distritos |

### Geología

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/fallas` | 19 fallas geológicas IGP/Audin et al. 2008 |
| `GET`  | `/api/v1/deslizamientos` | 10 zonas de deslizamiento activas (CENEPRED/INGEMMET) |
| `GET`  | `/api/v1/volcanes` | 20 volcanes INGEMMET/OVI-IGP 2021 con estado y peligro |
| `GET`  | `/api/v1/zonas-sismicas` | Zonificación sísmica NTE E.030 |
| `GET`  | `/api/v1/zonas-sismicas/referencia` | Tabla de referencia Z1–Z4 |

### Hidrometeorología

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/inundaciones` | 12 zonas inundables (ANA/CENEPRED 2024) |
| `GET`  | `/api/v1/tsunamis` | 9 zonas de riesgo de tsunami (PREDES/IGP/DHN 2024) |
| `GET`  | `/api/v1/precipitaciones` | 22 zonas de precipitación con índice FEN |
| `GET`  | `/api/v1/precipitaciones/cercanas` | Zonas cercanas a un punto |
| `GET`  | `/api/v1/fen` | 22 eventos El Niño / La Niña (NOAA-CPC 1957–2024) |
| `GET`  | `/api/v1/fen/estadisticas` | Estadísticas FEN agregadas |
| `GET`  | `/api/v1/riesgo/lluvia` | Riesgo pluvial puntual (lon, lat) |

### Infraestructura

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/infraestructura` | GeoJSON: aeropuertos, puertos, hospitales, bomberos, centrales + OSM |
| `GET`  | `/api/v1/infraestructura/cobertura` | Estadísticas cobertura oficial vs OSM |
| `GET`  | `/api/v1/estaciones` | 34 estaciones de monitoreo (IGP, SENAMHI, ANA, DHN, IPEN) |

### Riesgo, ML y Escenarios

| Método  | Ruta | Descripción |
|---------|------|-------------|
| `GET`   | `/api/v1/riesgo` | Riesgo integral puntual (lon, lat) |
| `GET`   | `/api/v1/riesgo/punto` | Riesgo detallado con IRC v9 para un punto |
| `GET`   | `/api/v1/riesgo/construccion/mapa` | GeoJSON IRC por distrito |
| `POST`  | `/api/v1/riesgo/escenario` | Escenario de daño sísmico (magnitud, profundidad, mix constructivo) |
| `GET`   | `/api/v1/susceptibilidad/{amenaza}` | Predicción ML puntual con SHAP local |
| `POST`  | `/api/v1/susceptibilidad/entrenar` | Entrenar modelo ML para una amenaza |
| `GET`   | `/api/v1/susceptibilidad/modelos` | Info de modelos entrenados |

### Sendai Framework

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/sendai/report` | Reporte completo Sendai (7 targets A–G) |
| `GET`  | `/api/v1/sendai/mapa` | GeoJSON distritos con score por target Sendai |

### Alertas EWS

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET`  | `/api/v1/alertas/stream` | SSE (Server-Sent Events) en tiempo real |
| `GET`  | `/api/v1/alertas/recap` | Últimas alertas emitidas |
| `GET`  | `/api/v1/ews/stats` | Estadísticas del worker EWS |

---

## 🤖 Motor ML — Susceptibilidad

El módulo `ml_engine.py` implementa un pipeline completo de aprendizaje automático para susceptibilidad por amenaza natural.

### Amenazas Soportadas

| Amenaza         | Features (8 por modelo) |
|-----------------|-------------------------|
| `deslizamiento` | peligro_sismo, peligro_inundacion, peligro_deslizamiento, peligro_sequia, pendiente_media, altitud_media, precipitacion_anual, densidad_poblacional |
| `inundacion`    | Los mismos 8 features extraídos de la tabla `distritos` |
| `sequia`        | Los mismos 8 features extraídos de la tabla `distritos` |

### Pipeline ML v9.0

```
Datos (distritos) → VIF filter (umbral 10) → SMOTE-Tomek → Train/Test 80/20
                                                              │
                                                    Optuna (20 trials)
                                                    Bayesian HP search
                                                              │
                                              XGBClassifier (parámetros óptimos)
                                                              │
                                              ┌───────────────┼───────────────┐
                                              │               │               │
                                        5-Fold CV       SHAP TreeExpl.   Bootstrap CI
                                        AUC-ROC/PR      Global + Local   80% (100 iter)
```

1. **Extracción**: Consulta tabla `distritos` con campos `peligro_*` vía asyncpg
2. **Target binario**: `1` si `peligro_{amenaza} ≥ 3`, `0` si no
3. **VIF**: Eliminación de features con Variance Inflation Factor > 10 (multicolinealidad)
4. **SMOTE-Tomek**: Balanceo de clases con sobremuestreo sintético + limpieza Tomek links
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
- **Fallback heurístico**: Si no hay modelo entrenado, calcula score desde datos del distrito

### Configuración

```bash
ML_AUTO_TRAIN=1       # Entrenar automáticamente tras ETL
ML_MIN_SAMPLES=10     # Mínimo de muestras (10 funciona para datos init.sql)
```

---

## 💥 Modelo de Daño Sísmico

El módulo `damage_model.py` simula escenarios de pérdidas sísmicas.

### Metodología

- **Atenuación**: Youngs et al. 1997 — PGA en función de magnitud, distancia y profundidad (intraslab si profundidad > 70 km, interface en caso contrario)
- **PGA**: Clampeado al rango [0.001g, 3.0g]
- **Fragilidad**: Curvas lognormales por taxonomía GEM (Adobe, URM, CM, RC, Wood, Infra)
- **Estados de daño**: Ninguno, Leve, Moderado, Severo, Colapso
- **Multi-taxonomía**: Parámetro `mix_construccion` permite mezclar tipologías constructivas
- **Factor hora**: `hora_del_dia` ("dia"/"noche") con factor mortalidad 1.4× nocturno (Coburn et al. 1992)
- **Validación de entrada**: Coordenadas Perú, magnitud [3.0, 9.5], profundidad [0, 700 km], mecanismo válido

### Taxonomías de Fragilidad

| Taxonomía | Descripción | Fuente |
|-----------|-------------|--------|
| ADOBE | Mampostería de adobe 1 piso | Tarque et al. 2012 |
| URM | Mampostería no reforzada | GEM GTX 2023 |
| CM | Mampostería confinada | GEM GTX 2023 |
| RC | Concreto armado | GEM GTX 2023 |
| WOOD | Madera | FEMA P-58 |
| INFRA | Infraestructura crítica | HAZUS MR4 |

### Funciones Principales

| Función | Descripción |
|---------|-------------|
| `pga_youngs97()` | PGA via Youngs et al. 1997 (interface / intraslab) |
| `fragility_probability()` | P(DS ≥ ds) por taxonomía con curva lognormal |
| `scenario_losses()` | Escenario completo: PGA, distribución de daño, pérdidas, mortalidad |
| `scenario_from_sismo()` | Escenario automático a partir de un sismo USGS existente |
| `batch_losses()` | Evaluación batch para múltiples distritos |

---

## ⚡ Early Warning System (EWS)

El módulo `alert_worker.py` implementa un sistema de alerta temprana multi-amenaza en tiempo real.

### Características

| Feature | Detalle |
|---------|---------|
| **Polling** | USGS + IGP cada 30s para sismos recientes (últimos 5 min) |
| **Cascada** | Tsunami (M ≥ 6.5 + costa < 50 km), Deslizamiento (M ≥ 5.0 + peligro ≥ 3) |
| **Niveles** | `emergency` (M ≥ 7.0), `warning` (M ≥ 5.5), `watch` (M ≥ 4.0) |
| **Difusión** | SSE + WebSocket simultáneo |
| **CAP v1.2** | Protocolo Common Alerting Protocol estándar |
| **EW4All** | 4 pilares: Conocimiento, Monitoreo, Difusión, Preparación (UNDRR 2022) |
| **Estadísticas** | Contadores polls_ok/err, alertas enviadas, clientes conectados |
| **Memoria** | Set de IDs vistos con poda automática (máx. 10,000 entradas) |

---

## 🔄 ETL Pipeline

El módulo `procesar_datos.py` implementa un pipeline ETL de 20 pasos con dependencias.

### Pasos

| # | Paso | Fuente | Dependencias |
|---|------|--------|-------------|
| 1 | Departamentos | GADM 4.1 + 25 fallback bboxes | — |
| 2 | Sismos | USGS FDSNWS M≥2.5 1900–hoy (22 bloques paralelos) | — |
| 3 | Distritos | INEI WFS → GADM L3 → 75 fallback bboxes | departamentos |
| 4 | Fallas | 19 fallas INGEMMET/IGP (Audin et al. 2008) | — |
| 5 | Inundaciones | 12 zonas ANA/CENEPRED 2024 | — |
| 6 | Tsunamis | 9 zonas PREDES/IGP/DHN 2024 | — |
| 7 | Deslizamientos | 10 zonas CENEPRED/INGEMMET 2024 | — |
| 8 | Infraestructura | MTC + APN + OSINERGMIN + MINSA + CGBVP + OSM Overpass | departamentos |
| 9 | Estaciones | 34 estaciones IGP + SENAMHI + ANA + DHN + IPEN | — |
| 10 | Precipitaciones | 22 zonas climáticas SENAMHI/CHIRPS 2024 | — |
| 11 | Eventos FEN | 22 eventos ENSO NOAA-CPC 1957–2024 | — |
| 12 | Volcanes | 20 volcanes INGEMMET/OVI-IGP 2021 + peligro_volcan por distrito | departamentos |
| 13 | Sequía SPI-12 | SPI (McKee et al. 1993) vs CHIRPS 1981–2020 | precipitaciones |
| 14 | Cascada | factor_cascada sismo→deslizamiento (Gill & Malamud 2014, α=0.15) | fallas, deslizamientos |
| 15 | IRC v9 | 7 amenazas ponderadas + bootstrap 500 iter (Li et al. 2023) | volcanes, sequía, cascada |
| 16 | Exposición/IVS | GEM 2023 + INEI CPV 2017 + MIDIS SISFOH 2022 | irc_v9 |
| 17 | Heatmap | REFRESH mv_heatmap_sismos | sismos |
| 18 | Regiones | f_actualizar_regiones() + zona_sismica | departamentos, distritos |
| 19 | Riesgo construcción | REFRESH mv_riesgo_construccion | regiones |
| 20 | Sendai | Snapshot 7 targets proxy SFDRR 2015–2030 | irc_v9 |

### Robustez del ETL

- **Retry con jitter**: Reintentos exponenciales con jitter aleatorio para requests HTTP (tenacity)
- **Circuit breaker Overpass**: 3 endpoints con rotación automática por fallos
- **COPY FROM buffer**: Inserciones masivas vía PostgreSQL COPY con auto-flush cada 10,000 filas
- **Coordenadas validadas**: Sismos verificados contra rango [-180, 180] × [-90, 90] antes de inserción
- **Materialized view allowlist**: REFRESH solo permitido contra vistas en la whitelist (_ALLOWED_MATVIEWS)
- **Progreso por paso**: Logging estructurado con porcentaje de avance y warnings por errores

### Opciones CLI

```bash
# Ejecutar todo
python procesar_datos.py

# Solo pasos específicos
python procesar_datos.py --solo volcanes sequia_spi irc_v9

# Omitir pasos lentos
python procesar_datos.py --skip infraestructura sismos

# Modo simulación (sin escritura a BD)
python procesar_datos.py --dry-run

# Ajustar workers y bootstrap
python procesar_datos.py --workers 8 --bootstrap-n 1000 --verbose
```

---

## 🏗️ IRC v9 — Índice de Riesgo Compuesto

El IRC v9 combina 7 amenazas con factor de cascada e incertidumbre por bootstrapping.

### Fórmula

$$\text{IRC}_{v9} = \left( \sum_{i=1}^{7} w_i \cdot P_i \right) \times F_{\text{cascada}}$$

### Pesos (CENEPRED 2014 + SENCICO E.030 2018)

| Amenaza | Peso | Justificación |
|---------|------|---------------|
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

---

## 🎨 Frontend — Funcionalidades

### Capas del Mapa (15+)

| Capa | deck.gl Layer | Descripción |
|------|--------------|-------------|
| Sismos | ScatterplotLayer | Puntos coloreados por profundidad, radio ∝ magnitud |
| Heatmap | ScreenGridLayer | Mapa de calor ponderado por magnitud |
| Departamentos | GeoJsonLayer | Polígonos con zona sísmica E.030 |
| Distritos | GeoJsonLayer | Nivel de riesgo 1–5 (coropleta) |
| IRC Mapa | GeoJsonLayer | Índice de Riesgo Construcción v9 |
| Fallas | GeoJsonLayer | Líneas con buffer visual |
| Inundaciones | GeoJsonLayer | Zonas inundables con tipo y período retorno |
| Tsunamis | GeoJsonLayer | Franja costera con altura ola y tiempo arribo |
| Deslizamientos | GeoJsonLayer | Zonas activas con tipo y causa |
| Precipitaciones | GeoJsonLayer | 22 zonas climáticas + índice FEN |
| Volcanes | ScatterplotLayer | 20 volcanes con estado y radio de peligro |
| Susceptibilidad ML | ScatterplotLayer | Scores de modelo ML con IC bootstrap |
| Alertas EWS | ScatterplotLayer | Alertas en tiempo real con pulsación |
| Estaciones | ScatterplotLayer | 34 estaciones de monitoreo |
| Extrusión 3D | ColumnLayer | Centroides de distritos extruidos por nivel_riesgo |

### Panel de Filtros

- Presets rápidos: Todo · Recientes · Fuertes ≥6 · Pisco 2007 · Grandes ≥7
- Filtros de magnitud, año, región, profundidad
- Filtros ML: score mínimo, amenaza seleccionada
- Filtros de precipitación: riesgo de inundación mínimo
- Fuente de datos: todos / oficial / OSM

### Gráficos (StatsChart)

- Histograma sísmico por año (barras/línea)
- Ranking IRC por distrito (top N)
- Eventos FEN históricos con intensidad
- Escenario de daño sísmico (distribución estados de daño, pérdidas estimadas)
- Reporte Sendai (7 targets con indicadores)

### Error Handling

- **ErrorBoundary**: Componente React class-based que envuelve MapView y StatsChart, capturando errores de renderizado con UI de recuperación y botón de reset
- **API Service**: Reintentos automáticos (3 intentos, backoff exponencial), deduplicación de requests inflight, cache con ETag, parsing estructurado de errores del backend

### Accesibilidad

- ARIA landmarks: `role="banner"` en header, `role="complementary"` en sidebar
- `aria-label` descriptivos en controles principales
- `role="alert"` en errores de ErrorBoundary

---

## 🔒 Seguridad

| Medida | Implementación |
|--------|----------------|
| CORS | Orígenes desde variable `CORS_ORIGINS` (no wildcard en producción) |
| Headers | `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy: strict-origin-when-cross-origin` |
| SQL Injection | Consultas parametrizadas en todos los endpoints; tablas de diagnóstico validadas contra `frozenset` |
| Materialized Views | Allowlist (`_ALLOWED_MATVIEWS`) para evitar inyección en `REFRESH` |
| Rate Limiting | slowapi con límites por IP en endpoints sensibles |
| Exception Handler | Global exception handler que no expone stack traces al cliente |
| Input Validation | Coordenadas, magnitudes, profundidades y mecanismos validados en `damage_model.py` y `ml_engine.py` |

---

## 💻 Desarrollo Local

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate   # Windows
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

| Capa                | Fuente | Tipo |
|--------------------|--------|------|
| Sismos históricos   | USGS FDSN Web Services (earthquake.usgs.gov) | API REST |
| Departamentos       | GADM 4.1 (geodata.ucdavis.edu) | GeoJSON |
| Distritos           | INEI WFS + GADM 4.1 L3 | WFS / GeoJSON |
| Fallas geológicas   | INGEMMET/IGP (Audin et al. 2008) | Hardcoded |
| Volcanes (20)       | INGEMMET "Mapa de Peligros Volcánicos" 2da ed. 2021 / OVI-IGP | Catálogo |
| Zonas inundables    | ANA + CENEPRED 2024 | Hardcoded |
| Zonas tsunami       | PREDES/IGP/DHN 2024 | Hardcoded |
| Deslizamientos      | CENEPRED/INGEMMET 2024 | Hardcoded |
| Precipitaciones     | SENAMHI + CHIRPS v2.0 (1981–2020 climatología) | Hardcoded |
| Eventos FEN/ENSO    | NOAA-CPC / ENFEN | Hardcoded |
| SPI-12 histórico    | McKee et al. 1993 / CHIRPS por zona climática | Calculado |
| Infraestructura     | MTC/CORPAC + APN + OSINERGMIN/MINEM + MINSA/SUSALUD + CGBVP + OSM Overpass | Oficial + OSM |
| Estaciones          | IGP (RSN) + SENAMHI (RMN) + ANA (RHN) + DHN (DART) + IPEN + INDECI (COEN) | Oficial |
| Exposición          | GEM Global Exposure Model 2023 (Yepes-Estrada et al., Earthquake Spectra) | Académico |
| Vulnerabilidad      | INEI CPV 2017 + MIDIS SISFOH 2022 | Censo / Registro |
| Costos reposición   | CAPECO 2023 | Sectorial |
| Zonificación sísmica| NTE E.030 (SENCICO 2018) | Norma técnica |
| Marco Sendai        | UNDRR Sendai Framework Monitor 2015–2030 | Metodología |

---

## 📚 Referencias Científicas

| Referencia | Uso en el proyecto |
|------------|-------------------|
| Youngs, R.R. et al. (1997) "Strong Ground Motion Attenuation Relationships for Subduction Zone Earthquakes". *Seismological Research Letters* 68(1):58–73 | Modelo de atenuación PGA en `damage_model.py` |
| McKee, T.B. et al. (1993) "The Relationship of Drought Frequency and Duration to Time Scales". 8th AMS Conference on Applied Climatology | SPI-12 sequía en `procesar_datos.py` |
| Gill, J.C. & Malamud, B.D. (2014) "Reviewing and visualizing the interactions of natural hazards". *Reviews of Geophysics* 52(4):680–722 | Factor cascada sismo→deslizamiento |
| Li, Z. et al. (2023) "Uncertainty in multi-hazard risk index". *Nat. Hazards Earth Syst. Sci.* | Bootstrap IC para IRC v9 |
| Yepes-Estrada, C. et al. (2023) GEM Global Exposure Model. *Earthquake Spectra* | Taxonomía constructiva por región |
| Tarque, N. et al. (2012) Fragilidad sísmica de edificaciones de adobe en Perú | Curvas de fragilidad adobe |
| Kumar, S. et al. (2023) Seismic PML estimation techniques | Estimación pérdida monetaria |
| Coburn, A. et al. (1992) Factors determining casualties in earthquakes | Factor mortalidad nocturna (1.4×) |
| CENEPRED (2014) Manual para la Evaluación de Riesgos | Pesos IRC base |
| SENCICO (2018) Norma Técnica de Edificación E.030 | Zonificación sísmica Z1–Z4 |
| UNDRR (2022) Early Warnings for All (EW4All) | 4 pilares del EWS |
| WMO (2012) Standardized Precipitation Index User Guide (WMO-No. 1090) | Clasificación SPI |

---

## ⌨️ Atajos de Teclado

| Tecla    | Acción           |
|---------|-----------------|
| `L`     | Toggle sidebar  |
| `F`     | Abrir filtros   |
| `G`     | Toggle gráfica  |
| `R`     | Centrar mapa    |
| `Esc`   | Cerrar popup    |

---

## 📄 Licencia

Proyecto académico — noveno ciclo. Datos de fuentes públicas gubernamentales.

Datos sísmicos © USGS (dominio público). Datos geoespaciales © IGP, INGEMMET, ANA, INEI, SENAMHI (sujetos a términos de uso de cada institución). GEM Global Exposure Model © GEM Foundation 2023.

---

*Plataforma de evaluación de riesgos geoespaciales multi-amenaza para el territorio peruano.*
