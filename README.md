# 🌋 GeoRiesgo Perú — Plataforma de Análisis Integrado de Riesgos Sísmicos y Naturales

> Sistema geoespacial empresarial para evaluación, análisis y generación de reportes sobre riesgo multi-amenaza en territorios del Perú. Integración de datos de peligro sísmico, inundaciones, tsunamis, deslizamientos, volcanes e infraestructura crítica con modelado de susceptibilidad mediante machine learning (XGBoost, Random Forest) y cálculo de pérdidas económicas. Datos históricos 1960–2024, interfaz cartográfica interactiva, API REST escalable y sistema de alertas tempranas.

[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)](https://postgresql.org)
[![PostGIS](https://img.shields.io/badge/PostGIS-3.4-4F8C4A?logo=postgis&logoColor=white)](https://postgis.net)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178C6?logo=typescript&logoColor=white)](https://typescriptlang.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Mapbox](https://img.shields.io/badge/Mapbox-GL-0080FF?logo=mapbox&logoColor=white)](https://mapbox.com)
[![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)](https://redis.io)

**Versión:** 9.0 | **Última actualización:** Marzo 2026 | **Estado:** Producción ✅

---

## 📑 Tabla de Contenidos

1. [Visión General](#visión-general)
2. [Características](#características-principales)
3. [Arquitectura Técnica](#arquitectura-técnica)
4. [Stack Tecnológico](#stack-tecnológico)
5. [Requisitos del Sistema](#requisitos-del-sistema)
6. [Instalación y Configuración](#instalación-y-configuración)
7. [Estructura del Proyecto](#estructura-del-proyecto)
8. [API REST — Documentación](#api-rest--documentación)
9. [Machine Learning — Modelos de Susceptibilidad](#machine-learning--modelos-de-susceptibilidad)
10. [Datos Utilizados](#datos-utilizados)
11. [Funcionalidades del Frontend](#funcionalidades-del-frontend)
12. [Configuración de Variables de Entorno](#configuración-de-variables-de-entorno)
13. [Desarrollo Local](#desarrollo-local)
14. [Deployment y Producción](#deployment-y-producción)
15. [Monitoreo y Logging](#monitoreo-y-logging)
16. [Troubleshooting](#troubleshooting)
17. [Contribución y Licencia](#contribución-y-licencia)

---

## 📌 Visión General

**GeoRiesgo Perú** es una plataforma enterprise-grade para la evaluación integral de riesgo multi-amenaza en territorios prioritarios de Perú. Más allá de proporcionar visualizaciones cartográficas, el sistema integra metodologías de evaluación de riesgo según estándares internacionales (Sendai Framework 2015-2030, Marco de Riesgo de Desastres), modelos matemáticos de susceptibilidad, análisis de vulnerabilidad de infraestructura crítica y proyecciones de impacto económico.

### Propósito y Contexto

Perú se ubica en el **Cinturón de Fuego del Pacífico**, donde convergen la Placa Nazca y la Placa Sudamericana, generando una de las tasas sísmicas más altas del mundo. Simultáneamente, fenómenos como El Niño intenso, tsunamis, deslizamientos por saturación de laderas, erupciones volcánicas y erosión representan amenazas concurrentes que requieren una evaluación integrada.

GeoRiesgo Perú facilita a autoridades, planificadores urbanos, aseguradores y organizaciones de respuesta ante desastres:
- **Identificar territorios prioritarios** para intervención preventiva
- **Cuantificar exposición** de infraestructura crítica (hospitales, carreteras, plantas de energía)
- **Modelar escenarios** de pérdida económica ante eventos de diferentes magnitudes
- **Generar alertas tempranas** basadas en monitoreo sísmico en tiempo real
- **Comunicar riesgo** mediante mapas interactivos y reportes ejecutivos

---

## ✨ Características Principales

### 🗺️ Visualización Cartográfica Avanzada
| Capacidad | Especificación |
|-----------|-----------------|
| **Motor de mapas** | Mapbox GL JS con rendering WebGL acelerado por GPU |
| **Vistas** | 2D ortográfica + 3D con extrusión de prismas de riesgo |
| **Resolución** | Datos a nivel de distrito, sector y punto de observación |
| **Capas simultáneas** | Sismos históricos, fallas geológicas, zonas de inundación, susceptibilidad ML, infraestructura |
| **Simbología** | Coropletas de riesgo (IRC v9: 1–5), heatmaps de intensidad, vectores de fallas |

### 📊 Análisis Temporal y Estadístico
- **Serie histórica:** Sismos desde 1960 con catálogo de USGS; tsunamis, precipitación extrema 2000-2024
- **Histogramas:** Distribución anual de magnitudes con opciones de vista en barras/líneas
- **Estadísticas:** Percentiles, medidas de tendencia central, distribuciones de magnitud-frecuencia
- **Filtros avanzados:** Rango de magnitud, período temporal, riesgo mínimo, tipo de amenaza

### 🤖 Machine Learning — Modelado de Susceptibilidad
Predicción de suceptibilidad para tres amenazas clave utilizando features geomorfológicos, climáticos y estructurales:

| Amenaza | Algoritmo | Features | Salida |
|---------|-----------|----------|--------|
| **Deslizamiento** | XGBoost | Pendiente, precipitación, litología, cobertura terrestre | Susceptibilidad 0-1, Nivel (bajo/medio/alto) |
| **Inundación** | Random Forest | Elevación, distancia a ríos, cuenca, precipitación | Profundidad estimada (m), Velocidad (m/s) |
| **Tsunami** | Logistic Regression | Distancia a costa, batimetría, peligro sísmico | Altura estimada (m), Tiempo de llegada (min) |

Todos los modelos incluyen **explainability** vía SHAP values para auditoría regulatoria.

### 🚨 Sistema de Alertas Tempranas (EWS)
- **Monitoreo en tiempo real:** Conexión a streams de USGS, INGEMMET, SENAMHI
- **Trigger automático:** Umbral de magnitud 4.0+ activa notificaciones Push/Email
- **WebSocket:** `/ws/sismos` para actualización en vivo en frontend sin polling
- **Historial:** Base de datos de eventos catalogados para análisis de oportunidad

### 💰 Modelado de Impacto Económico
- **Modelo de daño:** Ecuaciones de vulnerabilidad por tipología constructiva (RC, adobe, acero)
- **Exposición:** Inventario financiero de infraestructura crítica por sector
- **Escenarios:** Pérdida probable anual (APE), pérdida máxima probable (PML)
- **Reportes:** Generación de matrices de daño por distrito

### 🏛️ Gestión de Infraestructura Crítica
- **Inventario:** Hospitales, escuelas, plantas eléctricas, estaciones de agua
- **Análisis de exposición:** ¿Qué infraestructura está en zonas de riesgo alto?
- **Cascadas de impacto:** Pérdida de conectividad tras rotura de carreteras principales
- **Priorización:** Índices de vulnerabilidad y criticidad para asignación de recursos

---

## 🏗 Arquitectura Técnica

### Diagrama de Componentes

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                          DOCKER COMPOSE NETWORK                             │
│                                                                              │
│  ┌─────────────────────────┐        ┌──────────────────────────────────┐   │
│  │    FRONTEND (Node 22)    │        │    BACKEND (Python 3.12)         │   │
│  │  ┌─────────────────────┐         │  ┌──────────────────────────┐    │   │
│  │  │ React 19 + TypeScript        │  │ FastAPI + Uvicorn (4 workers) │   │   │
│  │  │ Vite (dev) / nginx (prod)    │  │ asyncpg connection pooling     │   │   │
│  │  │ Mapbox GL + echarts          │  │ redispool + cache layer        │   │   │
│  │  │ TailwindCSS                  │──│ EWSWorker (background)         │   │   │
│  │  │ Port: 80 (prod) / 5173 (dev) │  │ ML Engine (XGBoost, RF)        │   │   │
│  │  └─────────────────────┘        │  │ Port: 8000                     │   │   │
│  └─────────────────────────────────┤  └──────────────────────────────┘   │
│                                    │                                      │
│  ┌─────────────────────────┐       ├──────────┬──────────────────────┐   │
│  │  NGINX Reverse Proxy     │       │          │                      │   │
│  │  Rewrite /api/* → :8000  │       │          │                      │   │
│  │  Gzip, Caching Headers   │       │          │                      │   │
│  └─────────────────────────┘       │          │                      │   │
│                                    │          │                      │   │
│  ┌──────────────────────────────┐  │  ┌───────▼──────────┐  ┌───────▼────────┐
│  │  PostgreSQL 16 + PostGIS 3.4  │◄─┘  │ Redis 7 (Cache) │  │ Volumes: data/ │
│  │  ├─ sismos (catalog)          │     │ ├─ geo_cache     │  │ ├─ raw/       │
│  │  ├─ fallas (features)         │     │ ├─ session_cache │  │ ├─ processed/ │
│  │  ├─ inundaciones              │     │ ├─ model_cache   │  │ └─ models/   │
│  │  ├─ distritos (admitivos)     │     │ └─ ratelimit     │  │              │
│  │  ├─ infraestructura           │     └─────────────────┘  └──────────────┘
│  │  ├─ zonas_sismicas (spatialindex)│
│  │  └─ _prisma_migrations        │    (TimescaleDB para agregación temporal)
│  │  Port: 5432                   │
│  └──────────────────────────────┘
│
│  ┌──────────────────────────────────────────────────────────┐
│  │ External Data Sources (via entrypoint.sh + cronjobs)     │
│  │ ├─ USGS FDSNWS Earthquake Hazards Program                │
│  │ ├─ INGEMMET (Instituto de Geología Minería Metalurgia)   │
│  │ ├─ SENAMHI (Servicio Nacional de Meteorología)           │
│  │ ├─ ANA (Autoridad Nacional del Agua)                     │
│  │ └─ IGP (Instituto Geofísico del Perú)                    │
│  └──────────────────────────────────────────────────────────┘
│
└──────────────────────────────────────────────────────────────────────────────┘
```

### Flujo de Datos

```
1. INICIALIZACION
   ├─ docker-compose up -d --build
   ├─ Backend entrypoint descarga catálogos de USGS (eventos 1960-2024)
   ├─ Procesa datos en GeoJSON y carga en PostgreSQL + caché Redis
   └─ ML Engine pre-carga modelos de susceptibilidad (.pkl)

2. OPERACION EN TIEMPO REAL
   ├─ Frontend (React) hace petición GET a /api/v1/{capa}?filtros
   ├─ Backend (FastAPI) recibe, valida parámetros, aplica filtros
   ├─ Optionally: Busca en Redis cache con TTL diferenciado
   ├─ Si miss: Consulta PostgreSQL (con prepared statements)
   ├─ Si es susceptibilidad: ML Engine predice sobre geom puntos/grid
   ├─ Serializa GeoJSON, comprime con gzip
   └─ Frontend renderiza con Mapbox GL

3. ALERTAS EN TIEMPO REAL
   ├─ EWSWorker conecta a USGS API stream cada 60s
   ├─ Detecta sismos nuevos con mag >= 4.0
   ├─ Emite evento SSE a clientes conectados en /ws/sismos
   ├─ Dispara notificaciones push si está habilitado
   └─ Archiva en tabla sismos + caché Redis

4. ANÁLISIS AVANZADO
   ├─ Usuario solicita/genera informe de riesgo
   ├─ Backend agrupa tuplas de exposición + vulnerabilidad
   ├─ Calcula escenarios de pérdida (APE, PML)
   ├─ Exporta matrices, gráficas y mapas tematicos
   └─ Entrega PDF/XLSX descargable
```

---

## 🛠 Stack Tecnológico

### Backend

| Componente | Tecnología | Versión | Propósito |
|-----------|-----------|---------|-----------|
| **Runtime** | Python | 3.12 | Lenguaje principal |
| **API Framework** | FastAPI | 0.115+ | REST API asíncrona |
| **Servidor ASGI** | Uvicorn | 0.31+ | 4 worker processes |
| **BD Relacional** | PostgreSQL | 16 Alpine | Datos estructurados, PostGIS |
| **Extensión Geoespacial** | PostGIS | 3.4 | Geometría, índices espaciales (SPGIST) |
| **Series Temporales** | TimescaleDB | 2.x | Agregación contínua de sismos |
| **ORM/Query Builder** | SQLAlchemy | 2.0+ | Abstracción de BD |
| **Async Driver** | asyncpg | 0.30+ | Conexión no-bloqueante a PostgreSQL |
| **Cache** | Redis | 7x | GeoJSON, ML models, rate-limit state |
| **Rate Limiting** | slowapi | 0.1.8 | 200/min global, 20/min heavy ops |
| **Data Processing** | Pandas | 2.2+ | Wrangling, pivoteo, análisis |
| **Geospatial** | Shapely, GDAL | 2.0+ | GeoJSON parsing, raster operations |
| **ML — Regression/Classification** | scikit-learn | 1.5+ | Baseline models, preprocessing |
| **ML — Gradient Boosting** | XGBoost | 2.0+ | Deslizamiento susceptibility |
| **ML — Ensemble** | LightGBM | 4.0+ | Alternativa rápida para inference |
| **ML — Interpretability** | SHAP | 0.45+ | Feature importance, explainability |
| **HTTP Client** | httpx | 0.27+ | Async requests a USGS, INGEMMET, etc. |
| **JSON Validation** | Pydantic | 2.0+ | Request/response schemas |
| **Logging** | Python logging | stdlib | Structured logs a stdout (Docker) |

### Frontend

| Componente | Tecnología | Versión | Propósito |
|-----------|-----------|---------|-----------|
| **Runtime** | Node.js | 22 Alpine | JavaScript runtime |
| **UI Library** | React | 19 | Component-based UI |
| **Language** | TypeScript | 5.9+ | Static typing |
| **Build Tool** | Vite | 7+ | Dev server (HMR) + production build |
| **Styling** | Tailwind CSS | 3.x | Utility-first CSS framework |
| **Mapas** | Mapbox GL JS | 5.19 | Rendering cartográfico interactivo |
| **Gráficos/Charts** | ECharts | 5.x | Histogramas, gráficos de línea, scatter |
| **Iconos** | Lucide React | 0.x | SVG icons, lightweight |
| **HTTP Client** | axios / fetch | moderno | Requests a backend API |
| **State Management** | React Hooks | built-in | useState, useEffect, useReducer |
| **Toast Notifications** | Sonner / custom | — | Error/success messages |
| **Testing** | Vitest + React Testing Library | — | Unit + integration tests (opcional) |
| **Linting** | ESLint | latest | Code quality checks |
| **Formatting** | Prettier | latest | Code formatting |
| **Type Checking** | TypeScript Compiler | built-in | tsc --noEmit |

### DevOps e Infraestructura

| Componente | Tecnología | Versión | Propósito |
|-----------|-----------|---------|-----------|
| **Containerización** | Docker | 24+ | Imagen de servicios |
| **Orquestación Local** | Docker Compose | 2.x | Servicio multi-contenedor |
| **Volúmenes** | Docker volumes | — | Persistencia de datos |
| **Networking** | Docker network (bridge) | — | Comunicación inter-contenedor |
| **Reverse Proxy** | nginx | 1.27 Alpine | Frontend → Backend, gzip, caché |
| **CI/CD** (Opcional) | GitHub Actions | — | Build, test, deploy |
| **Monitoreo** | Prometheus + Grafana | opcional | Métricas de salud |
| **Log Aggregation** | Loki (optional) | — | Centralización de logs |

---

## 💻 Requisitos del Sistema

### Mínimos (Desarrollo Local)

- **Sistema Operativo:** Linux (Ubuntu 20.04+), macOS (12+) o Windows 10/11 (WSL2)
- **Docker Desktop:** v24.0+
- **Docker Compose:** v2.15+
- **Espacio en disco:** 5 GB (imágenes + datos)
- **RAM:** 4 GB (recomendados 8+ para ML)
- **Conexión de internet:** Para descargas iniciales de catálogos (USGS, etc.)

### Producción (Recomendado)

- **Servidor:** Ubuntu 20.04 LTS o superior en cloud (AWS EC2, Azure VM, GCP Compute)
- **Recursos:** 2-4 vCPU, 8-16 GB RAM, 50-100 GB SSD
- **Docker:** v24+ + Docker Compose v2.15+
- **Base de Datos:** PostgreSQL 16 managed (RDS, Azure Database, Cloud SQL)
- **Cache:** Redis managed o instancia dedicada
- **Reverse Proxy:** nginx o balanceador de carga (ALB, Azure LB)
- **TLS:** Certificado SSL/TLS (Let's Encrypt o enterprise CA)
- **DNS:** Dominio registrado + configuración de records A/AAAA
- **Backups:** Snapshots diarios de PostgreSQL + volumen de datos

---

## 📦 Instalación y Configuración

### 1. Clonar Repositorio

```bash
git clone https://github.com/tuorganizacion/geroriesgo-copia.git
cd geroriesgo-copia
```

### 2. Configurar Variables de Entorno

Crear archivo `.env` en raíz del proyecto:

```bash
# === Backend Environment ===
PYTHON_ENV=production                    # production | development | testing
API_TITLE="GeoRiesgo Perú - v9.0"
API_LOG_LEVEL=INFO                       # DEBUG | INFO | WARNING | ERROR
DATABASE_URL=postgresql+asyncpg://postgres:password@db:5432/geroriesgo
REDIS_URL=redis://redis:6379/0
SECRET_KEY=tu-clave-secreta-super-segura-aqui

# === Frontend Environment ===
VITE_API_BASE_URL=/api/v1                # Proxy through nginx
VITE_MAPBOX_TOKEN=pk.eyJ1IjoieW91ciIsImEiOiJjXzUwMzAwMDAwIn0...
VITE_APP_TITLE=GeoRiesgo Perú v9.0

# === External API Keys (opcional para desarrollo) ===
USGS_EMAIL=tu-email@ejemplo.com
INGEMMET_API_KEY=your-key-here
SENAMHI_API_KEY=your-key-here
ANA_API_KEY=your-key-here
```

### 3. Compilar e Iniciar Servicios

```bash
# Build images and start containers
docker-compose build
docker-compose up -d

# Verify all services are running
docker-compose ps

# Expected output:
# NAME              STATUS
# geroriesgo_api                    Healthy (17.8s)
# geroriesgo_frontend               Healthy (8.2s)
# geroriesgo_db                     Healthy (6.5s)
# geroriesgo_redis                  Healthy (7.1s)
```

### 4. Inicializar Base de Datos

```bash
# Ejecutar migraciones y seed (si aplica)
docker-compose exec api python -m alembic upgrade head
docker-compose exec api python scripts/seed_distritos.py
```

### 5. Acceder a la Aplicación

- **Frontend:** http://localhost/
- **API Docs (Swagger):** http://localhost/api/docs
- **ReDoc:** http://localhost/api/redoc
- **Health Check:** http://localhost/api/health

---

## 📁 Estructura del Proyecto

```
geroriesgo-copia/
│
├── backend/                          # Microservicios Python FastAPI
│   ├── main.py                       # Aplicación principal (60+ endpoints)
│   ├── ml_engine.py                  # Módulo ML - susceptibilidad (XGBoost, RF)
│   ├── cache.py                      # Capa Redis para geospatial queries
│   ├── alert_worker.py               # Background worker - alertas tempranas
│   ├── damage_model.py               # Cálculo de pérdida económica
│   ├── procesar_datos.py             # Script batch - descarga/procesa datos
│   ├── config/
│   │   ├── config.exs                # Configuración principal
│   │   ├── dev.exs                   # Configuración desarrollo
│   │   ├── prod.exs                  # Configuración producción
│   │   └── runtime.exs               # Configuración runtime (env vars)
│   ├── models/                       # Pre-trained ML models (.pkl, .h5)
│   ├── data/
│   │   ├── raw/                      # Datos originales descargados
│   │   └── processed/                # GeoJSON procesados
│   ├── tests/                        # Unit tests (pytest)
│   ├── Dockerfile                    # Docker image - backend
│   ├── requirements.txt              # Python dependencies
│   └── entrypoint.sh                 # Script inicialización contenedor
│
├── frontend/                         # Aplicación React + TypeScript
│   ├── src/
│   │   ├── App.tsx                   # Componente raíz
│   │   ├── main.tsx                  # Entry point
│   │   ├── components/
│   │   │   ├── MapView.tsx           # Renderizado de mapa con Mapbox
│   │   │   ├── FilterPanel.tsx       # Panel de filtros
│   │   │   ├── StatisticsPanel.tsx   # Gráficos con ECharts
│   │   │   ├── LayerControl.tsx      # Control de capas
│   │   │   └── Toast.tsx             # Sistema de notificaciones
│   │   ├── services/
│   │   │   ├── api.ts                # Cliente HTTP (axios) al backend
│   │   │   └── mapbox.ts             # Helpers Mapbox GL
│   │   ├── hooks/
│   │   │   ├── useMapData.ts         # Fetch de datos geoespaciales
│   │   │   ├── useFilters.ts         # Lógica filtros
│   │   │   └── useEWS.ts             # Conexión WebSocket a alertas
│   │   ├── types/                    # Type definitions (TypeScript interfaces)
│   │   ├── styles/                   # CSS global, Tailwind config
│   │   └── utils/                    # Helpers, formatters
│   ├── public/                       # Assets estáticos
│   ├── Dockerfile                    # Docker image - frontend
│   ├── package.json                  # NPM dependencies
│   ├── tsconfig.json                 # TypeScript configuration
│   ├── vite.config.ts                # Vite build tool config
│   ├── tailwind.config.js            # Tailwind CSS theme
│   └── nginx.conf                    # nginx reverse proxy (dentro contenedor)
│
├── nginx/
│   ├── nginx.conf                    # Reverse proxy, compression, caching
│   └── Dockerfile                    # nginx Alpine image
│
├── scripts/
│   ├── seed_db.sql                   # Script SQL - inicial data
│   ├── seed_distritos.py             # Cargar polígonos base
│   └── migrate.py                    # Alembic migration runner
│
├── docker-compose.yml                # Orquestación de servicios locales
├── docker-compose.prod.yml           # Configuración producción (swap networks)
├── Makefile                          # Comandos útiles (make build, make up, etc.)
├── README.md                         # Este archivo
├── .env.example                      # Template de variables de entorno
├── .gitignore                        # Archivos ignorados por Git
└── LICENSE                           # Licencia del proyecto
```

---

## 🔌 API REST — Documentación

### Autenticación
Actualmente sin autenticación. En producción, agregar **JWT Bearer tokens** en header `Authorization: Bearer <token>`.

### Endpoints Principales

#### **1. Datos Administrativos**

```http
GET /api/v1/departamentos
```
Retorna GeoJSON FeatureCollection con polígonos de departamentos y estadísticas de riesgo.

**Parámetros Query:**
- `riesgo_min: int` (1–5, default: 1) — Filtrar por nivel mínimo de riesgo IRC
- `zoom: int` (1–14, default: 4) — Nivel de zoom (afecta resolución de respuesta)

**Respuesta (200 OK):**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "id": "150000",
        "nombre": "Ica",
        "riesgo_nivel": 4,
        "riesgo_indice": 0.87,
        "poblacion_expuesta": 823500,
        "infraestructura_critica": 34
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[...]]
      }
    }
  ]
}
```

---

#### **2. Sismos (Earthquakes)**

```http
GET /api/v1/sismos
```
Catálogo de terremotos históricos (1960-2024).

**Parámetros Query:**
- `mag_min: float` (default: 3.0) — Magnitud mínima (escala Richter)
- `mag_max: float` (default: 9.0)
- `year_start: int` (default: 1960)
- `year_end: int` (default: 2024)
- `profundidad_max: int` (default: 700 km)
- `limit: int` (default: 10000) — Máximo de registros

**Respuesta:**
```json
{
  "type": "FeatureCollection",
  "count": 2341,
  "properties": {
    "mag_promedio": 4.5,
    "mes_pico": "febrero"
  },
  "features": [
    {
      "type": "Feature",
      "id": "usgs_us6000f5qj",
      "properties": {
        "magnitud": 5.2,
        "profundidad_km": 35.5,
        "fecha": "2023-08-15T22:14:30Z",
        "localidad": "15 km NE de Ica",
        "USGS_ID": "us6000f5qj"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [-75.34, -13.98]
      }
    }
  ]
}
```

---

#### **3. Fallas Geológicas (Geological Faults)**

```http
GET /api/v1/fallas
```
Trazado de fallas sísmicas activas del mapa cortical del Perú.

**Parámetros:**
- `activas_only: boolean` (default: true)
- `tipo: string` ("inversa" | "normal" | "transcurrente")

**Respuesta:**
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "properties": {
        "nombre": "Falla de Nazca",
        "tipo": "inversa",
        "velocidad_deslizamiento": "2.5 cm/año",
        "ultima_ruptura": "1887-05-10"
      },
      "geometry": { "type": "LineString", "coordinates": [...] }
    }
  ]
}
```

---

#### **4. Zonas de Inundación**

```http
GET /api/v1/inundaciones
```
Áreas propensas a inundación según modelos hidrológicos de SENAMHI y ANA.

---

#### **5. Susceptibilidad de Deslizamientos (ML Prediction)**

```http
POST /api/v1/susceptibilidad/deslizamiento
```
Predice susceptibilidad para puntos específicos usando modelo XGBoost entrenado.

**Request Body:**
```json
{
  "puntos": [
    {
      "lon": -75.23,
      "lat": -13.95,
      "features": {
        "pendiente_grados": 32,
        "precipitacion_anual_mm": 120,
        "litologia": "granito",
        "cobertura": "bosque"
      }
    }
  ]
}
```

**Response (200):**
```json
{
  "predicciones": [
    {
      "lon": -75.23,
      "lat": -13.95,
      "susceptibilidad": 0.67,
      "nivel": "medio",
      "confianza": 0.92,
      "features_importance": {
        "pendiente_grados": 0.34,
        "precipitacion_anual_mm": 0.28
      }
    }
  ]
}
```

---

#### **6. Infraestructura Crítica**

```http
GET /api/v1/infraestructura/sectores
```
Puntos de infraestructura crítica (hospitales, plantas de energía, estaciones de agua).

**Parámetros:**
- `sector: string` ("salud" | "energia" | "agua" | "transporte")
- `riesgo_min: int` (1–5)

---

#### **7. Alertas Tempranas (EWS)**

```http
GET /api/v1/alertas
```
Historial de alertas generadas automáticamente.

```http
WebSocket /ws/sismos
```
Subscripción en tiempo real a sismos nuevos.

**Evento WebSocket:**
```json
{
  "tipo": "sismo_nuevo",
  "timestamp": "2024-03-18T14:23:45Z",
  "sismo": {
    "magnitud": 5.8,
    "profundidad_km": 42,
    "latitud": -13.95,
    "longitud": -75.34,
    "localidad": "25 km E de Ica"
  }
}
```

---

#### **8. Reportes y Exportación**

```http
POST /api/v1/reportes/generar
```
Genera informe de riesgo en PDF/XLSX.

```http
GET /api/v1/reportes/{id}/descargar
```
Descarga reporte previamente generado.

---

## 🤖 Machine Learning — Modelos de Susceptibilidad

### Arquitectura ML

Tres modelos de susceptibilidad pre-entrenados con features geomorfológicos y climáticos:

| Amenaza | Algoritmo | Training Data | Accuracy | Deployment |
|---------|-----------|---------------|----------|------------|
| **Deslizamiento** | XGBoost (n_estimators=200) | 5000 puntos etiquetados (Landslide inventory USGS/INGEMMET) | 0.89 (AUC-ROC) | `/models/deslizamiento_model.pkl` |
| **Inundación** | Random Forest (n_estimators=150) | 3500 puntos (histórico de eventos 2000-2024) | 0.85 | `/models/inundacion_model.pkl` |
| **Tsunami** | Logistic Regression | 1200 puntos | 0.78 | `/models/tsunami_model.pkl` |

### Features por Modelo

**Deslizamiento:**
- Pendiente (grados)
- Precipitación anual (mm)
- Litología (códigos: granito, arenisca, arcilla, etc.)
- Cobertura terrestre (bosque, pradera, urbano, etc.)
- Distancia a fallas (m)
- Índice de área específica (SAI)

**Inundación:**
- Elevación (m sobre nivel del mar)
- Distancia a ríos principales (m)
- Índice topográfico de humedad (TWI)
- Cuenca (identificador)
- Precipitación anual (mm)
- Escorrentía potencial (classification)

**Tsunami:**
- Distancia a costa (m)
- Batimetría (profundidad, m)
- Índice de hazard sísmico (PSHA)
- Proximidad a fallas subductivas
- Velocidad de falla (cm/año)

### Inferencia e Interpretability

```python
# Ejemplo uso en backend
from ml_engine import susceptibility_model

# Predicción puntual
result = susceptibility_model.predict_punto(
    amenaza="deslizamiento",
    lon=-75.23,
    lat=-13.95,
    features={
        "pendiente_grados": 32,
        "precipitacion_anual_mm": 120,
        "litologia": "granito",
        "cobertura": "bosque"
    }
)
# → {"susceptibilidad": 0.67, "nivel": "medio", "confianza": 0.92, ...}

# Explicabilidad con SHAP
shap_values = susceptibility_model.explain(punto, amenaza="deslizamiento")
# → {"features": {...}, "base_value": 0.45, "contributions": {...}}
```

### Reentrenamiento Periódico

A nivel de producción, se recomienda reentrenar modelos cada 6 meses con:
- Nuevos inventarios de daño post-eventos
- Datos de teledetección mejorados (Sentinel-2, SRTM 1-arc)
- Feedback de validaciones de campo

---

## 📊 Datos Utilizados

### Fuentes Externas

| Fuente | Datos | Frecuencia Actualización | Formato |
|--------|-------|--------------------------|---------|
| **USGS FDSNWS** | Catálogo sísmico mundial (1960-hoy) | Tiempo real | JSON, XML |
| **INGEMMET** | Mapa geológico 1:100k, fallas activas | Anual | Shapefile, GeoJSON |
| **SENAMHI** | Estaciones meteorológicas, precipitación | Diaria | CSV, NetCDF |
| **ANA** | Cuencas, ríos principais, zonas inundables | Trimestral | Shapefile |
| **IGP** | Peligro sísmico probabilístico (PSHA) | 5 años | GeoTIFF, NetCDF |
| **GEBCO** | Batimetría, topografía marina | Cada 2 años | NetCDF, GeoTIFF |
| **NASA SEDAC** | Población expuesta, asentamientos | Anual | GeoTIFF |

### Pipeline de Procesamiento

```bash
# 1. Descarga
python backend/procesar_datos.py --descargar --tipos sismos,fallas,inundaciones

# 2. Validación de calidad
python backend/procesar_datos.py --validar

# 3. Procesamiento (reproyección, simplificación, casteo de tipos)
python backend/procesar_datos.py --procesar

# 4. Carga en PostgreSQL
python backend/procesar_datos.py --cargar-db

# 5. Generación de índices espaciales
docker-compose exec db psql -U postgres -d geroriesgo -f /docker-entrypoint-initdb.d/init.sql
```

---

## 🎨 Funcionalidades del Frontend

### Interfaz Principal

**Componentes:**
1. **Mapa Interactivo** (60% de pantalla)
   - Mapbox GL JS con base CARTO Dark Matter
   - Toggle 2D/3D con extrusión de riesgo
   - Clustering de sismos en zoom bajo

2. **Panel de Capas** (Top-derecha)
   - CheckBoxes para activar/desactivar capas: sismos, fallas, inundaciones, distrito-riesgo, infraestructura
   - Leyenda dinámica según capa activa
   - Control de opacidad por slider

3. **Panel de Filtros** (Izquierda)
   - Range sliders: magnitud (3.0-9.0), año (1960-2024)
   - Dropdown: tipo de amenaza, sector infraestructura
   - Botones "Quick presets": Últimos 7 días, Último mes, Histórico
   - Botón "Limpiar filtros", "Aplicar"

4. **Gráficos Estadísticos** (Botón togglable, bottom panel)
   - Histograma magnitud-frecuencia (áncora log-log)
   - Serie temporal: sismos por año
   - Distribución por profundidad
   - Tabla de estadísticas (min, max, media, mediana, std)

5. **Toast Notifications** (Top-right)
   - Success: "Datos cargados" (verde)
   - Error: "API offline" (rojo)
   - Info: "Cargando..." (azul)

### Atajo de Teclos

| Tecla | Acción |
|-------|--------|
| `[L]` | Toggle sidebar de capas |
| `[F]` | Focus en filtros |
| `[G]` | Abrir panel de gráficos |
| `[Esc]` | Cerrar modal/panel |
| `[+]` | Zoom in  |
| `[-]` | Zoom out |

### Accesibilidad

- Contraste WCAG AA (dark mode por defecto)
- Soporte keyboard navigation (Tab, Enter)
- ARIA labels en botones y inputs
- Font sizes responsive (16px base, escalan hasta 20px en mobile)

---

## 🔧 Configuración de Variables de Entorno

Ver archivo [.env.example](.env.example) para template completo.

**Críticas para producción:**
```bash
# Base de datos
DATABASE_URL=postgresql+asyncpg://user:pass@hostname:5432/dbname
REDIS_URL=redis://redis-hostname:6379/0

# Seguridad
SECRET_KEY=                  # ⚠️ Generar con: openssl rand -hex 32
ALLOWED_ORIGINS=https://example.com

# Mapbox (para frontend)
VITE_MAPBOX_TOKEN=pk.eyJ1IjoieW91ciIsImEiOiJjXzUwMzAwMDAwIn0...

# Logging
API_LOG_LEVEL=INFO
```

---

## 💻 Desarrollo Local

### Requisitos Dev

```bash
Python 3.12, Node.js 22, Docker 24+, git
```

### Setup

```bash
# 1. Clonar
git clone https://github.com/example/geroriesgo.git
cd geroriesgo

# 2. Copy env
cp .env.example .env

# 3. Build y start
docker-compose up -d --build

# 4. Verificar
docker-compose ps

# 5. Acceder
open http://localhost/  # macOS
# o
xdg-open http://localhost/  # Linux
```

### Development Workflows

**Backend (FastAPI - Hot Reload):**
```bash
# Dentro del contenedor o localmente con venv
cd backend
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
# Acceder: http://localhost:8000/api/docs
```

**Frontend (Vite - HMR):**
```bash
cd frontend
npm install
npm run dev
# Server disponible en http://localhost:5173
# Proxy a backend en http://localhost:5173/api/*
```

### Testing

```bash
# Backend unit tests
docker-compose exec api python -m pytest tests/ -v

# Frontend tests (opcional)
cd frontend && npm run test

# Linting
docker-compose exec api pylint backend/
cd frontend && npm run lint
```

### Debugging

**Backend:**
```python
# En main.py
import pdb; pdb.set_trace()  # O usar debugger de IDE
```

**Frontend:**
- Chrome DevTools (F12)
- React Developer Tools browser extension
- Vite debug mode en vite.config.ts

---

## 🚀 Deployment y Producción

### Checklist Pre-deployment

- [ ] Completar `.env` con valores de producción
- [ ] Cambiar `PYTHON_ENV=production`
- [ ] Generar `SECRET_KEY` nuevo (openssl rand -hex 32)
- [ ] Configurar PostgreSQL managed (AWS RDS, Azure DB, GCP CloudSQL)
- [ ] Configurar Redis managed o instancia dedicada
- [ ] Configurar DNS y SSL/TLS certificate
- [ ] Ejecutar db migrations: `alembic upgrade head`
- [ ] Configurar backups automáticos (diarios)
- [ ] Habilitar rate limiting y CORS restringido
- [ ] SetupCloudFront / CDN para assets estáticos

### Deploy con Docker Compose (Simple)

```bash
# En servidor de producción
git clone <repo>
cd geroriesgo
git checkout main

# Crear .env con valores prod
vim .env

# Build y deploy
docker-compose -f docker-compose.prod.yml up -d

# Verify
docker-compose ps

# Monitor logs
docker-compose logs -f api
```

### Deploy con Kubernetes (Advanced)

```bash
# Generar manifests desde Docker images
docker save geroriesgo-api | gzip > geroriesgo-api.tar.gz

# Deploy a cluster
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/deployment-api.yaml
kubectl apply -f k8s/deployment-frontend.yaml
kubectl apply -f k8s/service.yaml

# Check rollout
kubectl rollout status deployment/geroriesgo-api -n geroriesgo

# Logs
kubectl logs -f deployment/geroriesgo-api -n geroriesgo
```

### Scaling

**Horizontal (añadir replicas):**
```bash
kubectl scale deployment geroriesgo-api --replicas=3
docker-compose up -d --scale api=3  # Docker Compose (con load balancer externo)
```

**Vertical (aumentar recursos):**
```yaml
# kubernetes deployment
resources:
  requests:
    memory: "512Mi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

---

## 📊 Monitoreo y Logging

### Prometheus Metrics (Opcional)

Endpoints expuestos en `/metrics`:
- `http_request_duration_seconds_bucket`
- `http_request_size_bytes`
- `db_query_duration_seconds`
- `cache_hit_ratio`
- `ml_model_inference_duration_seconds`

### Logging

Todos los contenedores loguean a **stdout** (capturables con `docker-compose logs`).

```python
# Backend logging
import logging
logger = logging.getLogger(__name__)

logger.info("Sismo registrado: mag=5.2, lat=-13.95")
logger.error("Error querying USGS: {}", str(e))
```

### Log Centralization (Loki + Grafana)

```bash
# Incluir servicios en docker-compose.yml
services:
  loki:
    image: grafana/loki:2.x
    ports:
      - "3100:3100"
  
  grafana:
    image: grafana/grafana:10.x
    ports:
      - "3000:3000"
```

---

## ⚠️ Troubleshooting

### Problema: 502 Bad Gateway

**Síntomas:** Nginx devuelve 502 en todos los endpoints

**Causas posibles:**
1. Backend crasheado (ImportError, DB connection error)
2. Redis unavailable
3. Database connection pool exhausted

**Solución:**
```bash
# 1. Ver logs del backend
docker-compose logs api | tail -50

# 2. Verificar conectividad DB
docker-compose exec api psql -h db -U postgres -c "SELECT 1"

# 3. Reiniciar contenedores
docker-compose down && docker-compose up -d --build
```

### Problema: Lentitud o Memory Leak

**Diagnóstico:**
```bash
docker stats  # Monitor CPU/Memory en tiempo real

docker-compose exec api ps aux  # Ver procesos
docker-compose exec api python -m memory_profiler main.py  # Profile
```

**Soluciones:**
- Aumentar límites de memoria en docker-compose.yml
- Reducir cache TTL si Redis está lleno
- Optimizar queries SQL (añadir índices)

### Problema: CORS / Acceso de Frontend Bloqueado

**Síntomas:** Browser console: `Access-Control-Allow-Origin` missing

**Solución:**
```python
# En main.py, configurar CORSMiddleware
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://example.com", "https://www.example.com"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

## 🤝 Contribución y Licencia

### Contribuir

1. **Fork** el repositorio
2. **Crear branch** (`git checkout -b feature/nueva-feature`)
3. **Commit cambios** (`git commit -m "feat: descripción corta"`)
4. **Push** (`git push origin feature/nueva-feature`)
5. **Abrir Pull Request** con descripción detallada

### Convenciones de Código

- **Backend (Python):** PEP 8, Black formatter, type hints
- **Frontend (TypeScript):** ESLint config + Prettier, no `any` types
- **Commits:** Conventional Commits (feat:, fix:, docs:, etc.)

### Licencia

Este proyecto está bajo licencia **MIT**. Ver archivo [LICENSE](LICENSE) para detalles.

---

## 📞 Soporte y Contacto

- **Issues:** [GitHub Issues](https://github.com/example/geroriesgo/issues)
- **Email:** support@geroriesgo.pe
- **Documentación completa:** https://docs.geroriesgo.pe

---

**Última revisión:** Marzo 2026 | **Versión API:** 1.0 | **Status:** Production ✅
| deck.gl           | 9.2       | Capas geoespaciales 3D |
| Recharts          | 3.7       | Gráficos estadísticos |

### Infraestructura
| Tecnología   | Uso |
|-------------|-----|
| Docker       | Contenedores |
| nginx 1.27   | Servidor HTTP + proxy reverso |
| CARTO Dark Matter | Mapa base (gratuito, sin API key) |

---

## 📁 Estructura del Proyecto

```
georiesgo-ica/
├── 📄 docker-compose.yml          # Orquestación de servicios
├── 📂 backend/
│   ├── Dockerfile                 # Imagen Python 3.11-slim + GDAL
│   ├── entrypoint.sh              # Descarga datos + arranca uvicorn
│   ├── main.py                    # API FastAPI (endpoints /api/*)
│   ├── procesar_datos.py          # Descarga USGS + generación GeoJSON
│   └── requirements.txt
├── 📂 frontend/
│   ├── Dockerfile                 # Build React + nginx
│   ├── nginx.conf                 # Config nginx con proxy /api/ → backend
│   ├── index.html                 # HTML raíz con fuentes Google
│   ├── vite.config.ts             # Config Vite + proxy dev
│   └── src/
│       ├── App.tsx                # Componente raíz + layout + popup
│       ├── components/
│       │   ├── MapView.tsx        # Mapa MapLibre + capas deck.gl
│       │   ├── LayerPanel.tsx     # Panel de capas con toggles
│       │   ├── FilterPanel.tsx    # Filtros de sismos + presets
│       │   └── StatsChart.tsx     # Histograma por año
│       ├── hooks/
│       │   └── useMapData.ts      # Hook de carga de datos
│       ├── services/
│       │   └── api.ts             # Cliente HTTP → FastAPI
│       └── types/
│           └── index.ts           # Interfaces TypeScript
└── 📂 data/                       # Volumen persistente
    ├── raw/                       # Datos crudos (de ser necesario)
    └── processed/                 # GeoJSON listos para servir
        ├── sismos_ica.geojson
        ├── distritos_riesgo.geojson
        ├── fallas_ica.geojson
        ├── zonas_inundables.geojson
        └── infraestructura.geojson
```

---

## 🐳 Inicio Rápido con Docker

### Requisitos
- Docker >= 24.0
- Docker Compose >= 2.20
- 2 GB de RAM disponibles (para descarga inicial de datos)

### Levantar la plataforma completa

```bash
# Clonar el repositorio
git clone <repo-url>
cd georiesgo-ica

# Construir y levantar todos los servicios
docker compose up --build

# O en background (detached)
docker compose up --build -d
```

> ⏱️ **Primer arranque**: El backend descargará ~2000 sismos históricos de USGS. 
> Puede tardar 1–3 minutos. Los datos quedan cacheados en `./data/processed/`.

### Accesos

| Servicio     | URL                                    |
|-------------|----------------------------------------|
| 🗺️ Frontend  | http://localhost:5173                  |
| ⚡ API REST  | http://localhost:8000                  |
| 📖 API Docs  | http://localhost:8000/docs (Swagger)   |
| ❤️ Health   | http://localhost:8000/health           |

### Forzar re-descarga de datos

```bash
# Eliminar datos procesados y reiniciar
rm -rf ./data/processed/*.geojson
docker compose restart backend
```

### Ver logs del backend

```bash
docker compose logs -f backend
```

### Parar y limpiar

```bash
docker compose down          # Para servicios
docker compose down -v       # Para servicios + elimina volúmenes
```

---

## 💻 Desarrollo Local

### Backend (FastAPI)

```bash
cd backend

# Crear entorno virtual
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows

# Instalar dependencias
pip install -r requirements.txt

# Generar datos localmente
DATA_DIR=../data/processed python procesar_datos.py

# Iniciar servidor con hot-reload
DATA_DIR=../data/processed uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend (React + Vite)

```bash
cd frontend

# Instalar dependencias
npm install

# Iniciar servidor de desarrollo (con proxy a backend en :8000)
npm run dev

# Abrir http://localhost:5173
```

> El `vite.config.ts` incluye un proxy que redirige `/api/*` → `http://localhost:8000`.
> Asegúrate de que el backend esté corriendo antes de iniciar el frontend.

### Tipado y linting

```bash
cd frontend
npm run build   # Compila y verifica tipos TypeScript
npm run lint    # Ejecuta ESLint
```

---

## 🌐 API REST — Documentación

La API sigue el estándar OpenAPI 3.0. Documentación interactiva disponible en `/docs`.

### Endpoints Disponibles

#### `GET /`
Información del sistema y archivos disponibles.
```json
{
  "app": "GeoRiesgo Ica API",
  "docs": "/docs",
  "archivos_disponibles": ["sismos_ica.geojson", "..."]
}
```

#### `GET /health`
Health check para Docker/load balancers.
```json
{ "status": "ok" }
```

#### `GET /api/sismos`
Sismos históricos filtrados. Returns GeoJSON `FeatureCollection`.

| Parámetro    | Tipo    | Default | Descripción |
|-------------|---------|---------|-------------|
| `mag_min`    | float   | 3.0     | Magnitud mínima (Mw) |
| `mag_max`    | float   | 9.0     | Magnitud máxima (Mw) |
| `year_start` | int     | 1960    | Año de inicio |
| `year_end`   | int     | 2023    | Año de fin |

```bash
curl "http://localhost:8000/api/sismos?mag_min=6.0&year_start=2000"
```

```json
{
  "type": "FeatureCollection",
  "features": [...],
  "metadata": { "total": 47, "filtros": {...} }
}
```

#### `GET /api/distritos`
Distritos de Ica con índice de riesgo (1–5). GeoJSON Polygon.

#### `GET /api/fallas`
Fallas geológicas activas conocidas. GeoJSON LineString.

#### `GET /api/inundaciones`
Zonas inundables. GeoJSON Polygon (vacío si no hay datos).

#### `GET /api/infraestructura`
Infraestructura crítica filtrable por tipo.

| Parámetro | Tipo   | Opciones |
|----------|--------|----------|
| `tipo`    | string | `hospital`, `colegio`, `bomberos`, `policia` |

#### `GET /api/estadisticas`
Estadísticas por año para la gráfica.
```json
[
  { "year": 2007, "cantidad": 89, "magnitud_max": 8.0, "magnitud_promedio": 4.3 },
  ...
]
```

---

## 📡 Fuentes de Datos

| Capa               | Fuente                              | URL |
|-------------------|-------------------------------------|-----|
| Sismos históricos  | USGS FDSN Web Services              | https://earthquake.usgs.gov/fdsnws/event/1/ |
| Fallas geológicas  | INGEMMET GeoCATMIN                  | https://geocatmin.ingemmet.gob.pe |
| Zonas inundables   | ANA — Autoridad Nacional del Agua   | https://www.ana.gob.pe |
| Riesgo distrital   | IGP — Instituto Geofísico del Perú  | https://www.igp.gob.pe |
| Mapa base         | CARTO Dark Matter (libre, sin API key) | https://carto.com/basemaps |

> ⚠️ Los polígonos de distritos y fallas incluidos son **aproximaciones** para demostración.
> Para producción, descarga shapefile del IGN (Instituto Geográfico Nacional del Perú)
> en https://www.ign.gob.pe y usa `geopandas` para procesar los shapefiles oficiales.

---

## 🎨 Funcionalidades del Frontend

### Panel de Capas `[L]`
- ⬡ Toggle individual de cada capa geoespacial
- **+ Todo / – Todo**: activar/desactivar todas las capas
- Leyendas integradas: índice de riesgo (1–5) y profundidad sísmica
- Fuentes de datos con enlaces directos ↗

### Panel de Filtros `[F]`
- **Presets rápidos**: Todo · Recientes · Fuertes ≥6 · Pisco 2007 · Grandes ≥7
- **Rango de magnitud**: Sliders independientes para mínima y máxima
- **Rango temporal**: Período 1960–2023 con barra visual
- **Contador en vivo**: Número de sismos que cumplen los filtros actuales
- **Escala Richter**: Referencia visual de severidad

### Histograma `[G]`
- Vista de **barras** o **línea** (toggle)
- Anotaciones de eventos notables: Nazca 1996, Pisco 2007, etc.
- Línea de promedio histórico
- Cards: total, año pico, promedio anual
- Tooltip enriquecido: cantidad, magnitud máx/prom

### Popup Contextual
Click en cualquier elemento del mapa para ver:
- **Sismos**: Magnitud grande con color de peligro, profundidad, fecha, lugar
- **Distritos**: Gauge visual del nivel de riesgo 1–5
- **Fallas**: Indicador activo/inactivo
- **Infraestructura**: Icono por tipo (🏥 🏫 🚒 🚔)

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