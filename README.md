# 🌋 GeoRiesgo Ica — Plataforma de Visualización de Riesgo Sísmico

> Plataforma geoespacial interactiva para el análisis y visualización del riesgo sísmico en la Región Ica, Perú. Datos históricos 1960–2023 con renderizado 3D, filtros en tiempo real y estadísticas avanzadas.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-19-61DAFB?logo=react&logoColor=black)](https://react.dev)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.9-3178C6?logo=typescript&logoColor=white)](https://typescriptlang.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://docs.docker.com/compose/)

---

## 📋 Índice

- [Descripción del Proyecto](#-descripción-del-proyecto)
- [Arquitectura del Sistema](#-arquitectura-del-sistema)
- [Stack Tecnológico](#-stack-tecnológico)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Inicio Rápido con Docker](#-inicio-rápido-con-docker)
- [Desarrollo Local](#-desarrollo-local)
- [API REST — Documentación](#-api-rest--documentación)
- [Fuentes de Datos](#-fuentes-de-datos)
- [Funcionalidades del Frontend](#-funcionalidades-del-frontend)
- [Variables de Entorno](#-variables-de-entorno)
- [Notas sobre la Región Ica](#-notas-sobre-la-región-ica)

---

## 📌 Descripción del Proyecto

**GeoRiesgo Ica** es una aplicación web full-stack orientada a la visualización geoespacial del riesgo sísmico en la Región Ica, Perú — una de las zonas de mayor actividad sísmica de América del Sur, ubicada sobre el cinturón de fuego del Pacífico.

La plataforma integra datos sísmicos históricos de **USGS** (United States Geological Survey), fallas geológicas del **INGEMMET**, zonas de inundación de la **ANA**, e indicadores de riesgo por distrito del **IGP**.

### Características Principales

| Característica              | Descripción |
|-----------------------------|-------------|
| 🗺️ Mapa interactivo         | MapLibre GL + deck.gl con renderizado WebGL acelerado por GPU |
| 🌐 Vista 2D / 3D            | Perspectiva aérea y extrusión tridimensional |
| 🔥 Heatmap de intensidad    | Mapa de calor sísmico con ponderación por magnitud |
| ⌇ Fallas geológicas         | Trazado de fallas activas del sistema cortical |
| ▦ Índice de riesgo           | Coropleta por distrito con escala 1–5 |
| 📊 Histograma histórico      | Distribución anual 1960–2023 con vista de barras/línea |
| ⚡ Filtros en tiempo real    | Magnitud (min/max), rango temporal, presets rápidos |
| 🔔 Notificaciones            | Toast system para errores de API y acciones |
| ⌨️ Atajos de teclado         | `[L]` sidebar · `[F]` filtros · `[G]` gráfica · `[Esc]` cerrar |

---

## 🏗 Arquitectura del Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                        DOCKER COMPOSE                       │
│                                                             │
│  ┌──────────────────┐         ┌─────────────────────────┐  │
│  │   FRONTEND       │         │   BACKEND                │  │
│  │  React + Vite    │         │   FastAPI + Python        │  │
│  │  nginx:1.27      │──/api/──│   uvicorn                │  │
│  │  port 5173:80    │ proxy   │   port 8000:8000         │  │
│  └──────────────────┘         └────────────┬────────────-┘  │
│                                            │                │
│                               ┌────────────▼───────────┐   │
│                               │   VOLUMEN: ./data       │   │
│                               │   /data/raw/           │   │
│                               │   /data/processed/     │   │
│                               │   *.geojson            │   │
│                               └────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Flujo de Datos

1. **Primer arranque**: El entrypoint del backend descarga sismos históricos de la API USGS FDSNWS y genera datos de fallas/distritos
2. **Datos en caché**: Una vez descargados, los GeoJSON se guardan en el volumen `./data/processed/` para evitar re-descargas
3. **Backend sirve GeoJSON**: FastAPI lee los archivos y aplica filtros en memoria
4. **Frontend consume API**: React + hooks llaman al backend vía `/api/*` (proxy nginx en Docker, proxy Vite en desarrollo)
5. **Renderizado**: deck.gl dibuja las capas sobre un mapa base de CARTO Dark Matter

---

## 🛠 Stack Tecnológico

### Backend
| Tecnología      | Versión   | Uso |
|----------------|-----------|-----|
| Python          | 3.11      | Runtime |
| FastAPI         | 0.115     | Framework API REST |
| Uvicorn         | 0.31      | Servidor ASGI |
| Pandas          | 2.2.3     | Procesamiento de datos |
| Requests        | 2.32.3    | Llamadas a USGS API |

### Frontend
| Tecnología        | Versión   | Uso |
|------------------|-----------|-----|
| React             | 19        | UI Framework |
| TypeScript        | 5.9       | Tipado estático |
| Vite              | 7         | Build tool |
| MapLibre GL JS    | 5.19      | Motor de mapas |
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
#   S I S T E M A S - D E - I N F O R M A C I - N - g e o g r a f i c o  
 