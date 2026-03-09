# ══════════════════════════════════════════════════════════
# GeoRiesgo Perú — API FastAPI v7.0
# CORRECCIONES:
#   ✅ GET /api/v1/zonas-sismicas            (nuevo)
#   ✅ GET /api/v1/infraestructura/cobertura  (nuevo — usa v_infraestructura_cobertura)
#   ✅ GET /api/v1/riesgo/construccion/ranking (nuevo — usa mv_riesgo_construccion)
#   ✅ GET /api/v1/riesgo/construccion/mapa   (nuevo — GeoJSON distritos con índice)
#   ✅ Todos los endpoints v6.0 mantenidos
# ══════════════════════════════════════════════════════════

from __future__ import annotations

import hashlib
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
import orjson
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

# ── Conexión ──────────────────────────────────────────────
DB_DSN = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql://georiesgo:georiesgo_secret@db:5432/georiesgo",
).replace("postgresql+asyncpg://", "postgresql://")

_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await asyncpg.create_pool(
        DB_DSN,
        min_size=2,
        max_size=10,
        command_timeout=60,
        server_settings={"application_name": "georiesgo_api_v7"},
    )
    yield
    if _pool:
        await _pool.close()


app = FastAPI(
    title="GeoRiesgo Perú API",
    description="""
## API de Riesgo Geoespacial — Perú v7.0

### Nuevos endpoints v7.0
- **GET /api/v1/zonas-sismicas** — Departamentos coloreados por zona NTE E.030-2018
- **GET /api/v1/infraestructura/cobertura** — Diagnóstico de cobertura por tipo y fuente
- **GET /api/v1/riesgo/construccion/ranking** — Top distritos por índice de riesgo de construcción
- **GET /api/v1/riesgo/construccion/mapa** — GeoJSON distritos con índice CENEPRED

### Mejoras v6.0 mantenidas
- **ST_Covers** en lugar de ST_Within
- **KNN fallback** — `region` nunca es NULL
- **f_asignar_region()** PostGIS con 3 niveles

### Fuentes
| Capa | Fuente | Cobertura |
|------|--------|-----------|
| Sismos | USGS FDSNWS + IGP | Nacional (M≥2.5, desde 1900) |
| Fallas | INGEMMET + IGP/Audin et al. | Nacional |
| Inundaciones | ANA + CENEPRED | Nacional |
| Tsunamis | PREDES + IGP + INDECI | Costa peruana |
| Deslizamientos | CENEPRED + INGEMMET | Nacional |
| Infraestructura | SUSALUD/MINEDU/MTC/APN/OSINERGMIN + OSM | Nacional |
| Estaciones | IGP + SENAMHI + ANA + DHN | Nacional |
| Distritos | INEI + GADM | Nacional (1,874 distritos) |
| Departamentos | INEI + GADM | Nacional (25 regiones) |
| Zonas sísmicas | NTE E.030-2018 (DS N°003-2016-VIVIENDA) | Nacional |
    """,
    version="7.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Cache", "ETag"],
    max_age=3600,
)
app.add_middleware(GZipMiddleware, minimum_size=512)


# ══════════════════════════════════════════════════════════
#  UTILIDADES
# ══════════════════════════════════════════════════════════

async def db() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(503, detail={
            "error": "database_unavailable",
            "mensaje": "Base de datos no disponible temporalmente",
        })
    return _pool


def _simplify_tolerance(zoom: Optional[int]) -> float:
    if zoom is None or zoom >= 13:
        return 0.0
    if zoom <= 5:
        return 0.05
    if zoom <= 9:
        return 0.01
    return 0.001


def _geom_expr(zoom: Optional[int], col: str = "geom", decimals: int = 6) -> str:
    tol = _simplify_tolerance(zoom)
    if tol > 0:
        return f"ST_AsGeoJSON(ST_SimplifyPreserveTopology({col}, {tol}), {decimals})::TEXT"
    return f"ST_AsGeoJSON({col}, {decimals})::TEXT"


def geojson_response(
    features: list,
    metadata: dict | None = None,
    cache_seconds: int = 300,
) -> Response:
    fc = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total":  len(features),
            "crs":    "EPSG:4326",
            "api":    "GeoRiesgo Perú v7.0",
            **(metadata or {}),
        },
    }
    content = orjson.dumps(fc, option=orjson.OPT_NON_STR_KEYS)
    etag    = f'"{hashlib.md5(content).hexdigest()[:12]}"'  # noqa: S324
    return Response(
        content=content,
        media_type="application/geo+json",
        headers={
            "Cache-Control": f"public, max-age={cache_seconds}",
            "ETag":          etag,
            "X-Total-Count": str(len(features)),
        },
    )


def row_to_feature(row: asyncpg.Record, props_keys: list[str]) -> dict | None:
    geom_str = row.get("geom_json")
    if not geom_str:
        return None
    try:
        geom = json.loads(geom_str)
    except Exception:
        return None
    props: dict = {}
    for k in props_keys:
        try:
            v = row[k]
            if v is None:
                props[k] = None
            elif hasattr(v, "isoformat"):
                props[k] = v.isoformat()
            elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
                props[k] = float(v)
            else:
                props[k] = v
        except (KeyError, IndexError):
            pass
    return {"type": "Feature", "geometry": geom, "properties": props}


def rows_to_features(rows, props_keys: list[str]) -> list[dict]:
    return [f for row in rows if (f := row_to_feature(row, props_keys)) is not None]


# ══════════════════════════════════════════════════════════
#  ROOT / HEALTH
# ══════════════════════════════════════════════════════════

@app.get("/", summary="Estado general de la API", tags=["Sistema"])
async def root():
    pool = await db()
    row  = await pool.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM sismos)            AS sismos,
            (SELECT COUNT(*) FROM departamentos)     AS departamentos,
            (SELECT COUNT(*) FROM distritos)         AS distritos,
            (SELECT COUNT(*) FROM fallas)            AS fallas,
            (SELECT COUNT(*) FROM zonas_inundables)  AS inundaciones,
            (SELECT COUNT(*) FROM zonas_tsunami)     AS tsunamis,
            (SELECT COUNT(*) FROM deslizamientos)    AS deslizamientos,
            (SELECT COUNT(*) FROM infraestructura)   AS infraestructura,
            (SELECT COUNT(*) FROM estaciones)        AS estaciones,
            (SELECT MAX(fecha)::TEXT FROM sismos)    AS ultimo_sismo,
            (SELECT MIN(fecha)::TEXT FROM sismos)    AS primer_sismo,
            (SELECT COUNT(*) FROM sismos WHERE region IS NULL)          AS sismos_sin_region,
            (SELECT COUNT(*) FROM infraestructura WHERE region IS NULL) AS infra_sin_region
    """)
    return {
        "api":     "GeoRiesgo Perú v7.0",
        "docs":    "/docs",
        "redoc":   "/redoc",
        "capas":   dict(row),
        "spatial": {
            "metodo_region": "ST_Covers + KNN fallback (PostGIS)",
            "null_regions":  {
                "sismos":          row["sismos_sin_region"],
                "infraestructura": row["infra_sin_region"],
            },
        },
        "fuentes": [
            "USGS FDSNWS", "IGP", "INGEMMET", "ANA", "CENEPRED",
            "PREDES", "INDECI", "SENAMHI", "OpenStreetMap", "INEI/GADM",
            "SUSALUD/RENIPRESS", "MINEDU/ESCALE", "MTC/CORPAC", "APN", "OSINERGMIN",
        ],
    }


@app.get("/health", summary="Healthcheck Docker", tags=["Sistema"])
async def health():
    pool = await db()
    await pool.fetchval("SELECT 1")
    return {"status": "ok", "ts": time.time(), "version": "7.0"}


# ══════════════════════════════════════════════════════════
#  DIAGNÓSTICO DE REGIONES
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/diagnostico/regiones", summary="Cobertura de asignación de regiones", tags=["Sistema"])
async def diagnostico_regiones():
    pool = await db()
    tablas = ["sismos", "infraestructura", "estaciones", "fallas"]
    resultado = {}
    for tabla in tablas:
        row = await pool.fetchrow(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE region IS NOT NULL) AS con_region,
                COUNT(*) FILTER (WHERE region IS NULL) AS sin_region,
                COUNT(DISTINCT region) AS regiones_distintas
            FROM {tabla}
        """)
        resultado[tabla] = dict(row)
    return resultado


# ══════════════════════════════════════════════════════════
#  ZONAS SÍSMICAS NTE E.030-2018  ← NUEVO v7.0
#  GET /api/v1/zonas-sismicas
# ══════════════════════════════════════════════════════════

@app.get(
    "/api/v1/zonas-sismicas",
    summary="Zonificación sísmica NTE E.030-2018 por departamento",
    tags=["Sismicidad"],
    response_class=Response,
)
async def get_zonas_sismicas(
    zona:  Optional[int] = Query(None, ge=1, le=4, description="Filtrar por zona (1-4)"),
    zoom:  Optional[int] = Query(None, ge=1, le=20),
):
    """
    Devuelve los polígonos de departamentos coloreados por zona sísmica
    según la Norma Técnica de Edificación E.030-2018.

    - **Zona 4** (Z=0.45g): Costa — Tumbes, Piura, Lambayeque, La Libertad,
      Ancash, Lima, Callao, Ica, Arequipa, Moquegua, Tacna
    - **Zona 3** (Z=0.35g): Sierra central/sur
    - **Zona 2** (Z=0.25g): Sierra norte, Selva central
    - **Zona 1** (Z=0.10g): Amazonia — Loreto, Madre de Dios

    Referencia: DS N°003-2016-VIVIENDA (actualizado 2018)
    """
    pool     = await db()
    geom_col = _geom_expr(zoom)
    rows     = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id,
            ubigeo,
            nombre,
            COALESCE(zona_sismica, 2)                    AS zona_sismica,
            COALESCE(factor_z, 0.25)                     AS factor_z,
            nivel_riesgo,
            area_km2,
            capital,
            CASE COALESCE(zona_sismica, 2)
                WHEN 4 THEN 'Muy Alta — Costa'
                WHEN 3 THEN 'Alta — Sierra Central/Sur'
                WHEN 2 THEN 'Media — Sierra Norte/Selva Central'
                WHEN 1 THEN 'Baja — Amazonia'
                ELSE   'No clasificado'
            END AS descripcion_zona,
            CASE COALESCE(zona_sismica, 2)
                WHEN 4 THEN '#d32f2f'
                WHEN 3 THEN '#f57c00'
                WHEN 2 THEN '#fbc02d'
                WHEN 1 THEN '#388e3c'
                ELSE        '#9e9e9e'
            END AS color
        FROM departamentos
        WHERE ($1::INT IS NULL OR zona_sismica = $1)
        ORDER BY COALESCE(zona_sismica, 2) DESC, nombre
    """, zona)

    props_keys = [
        "id", "ubigeo", "nombre", "zona_sismica", "factor_z",
        "nivel_riesgo", "area_km2", "capital",
        "descripcion_zona", "color",
    ]

    # Estadísticas de resumen
    stats: dict[int, dict] = {}
    for row in rows:
        z = row["zona_sismica"] or 2
        if z not in stats:
            stats[z] = {"zona": z, "factor_z": float(row["factor_z"] or 0.25), "departamentos": 0}
        stats[z]["departamentos"] += 1

    return geojson_response(
        rows_to_features(rows, props_keys),
        {
            "norma":    "NTE E.030-2018 — DS N°003-2016-VIVIENDA",
            "fuente":   "INEI/GADM + Reglamento Nacional de Edificaciones",
            "zonas":    sorted(stats.values(), key=lambda x: x["zona"], reverse=True),
            "zoom":     zoom,
        },
        cache_seconds=86400,  # 24h — datos estáticos normativos
    )


# ══════════════════════════════════════════════════════════
#  INFRAESTRUCTURA COBERTURA  ← NUEVO v7.0
#  GET /api/v1/infraestructura/cobertura
# ══════════════════════════════════════════════════════════

@app.get(
    "/api/v1/infraestructura/cobertura",
    summary="Diagnóstico de cobertura de infraestructura por tipo y fuente",
    tags=["Infraestructura"],
)
async def get_infraestructura_cobertura(
    tipo:        Optional[str] = Query(None, description="Filtrar por tipo (hospital, escuela, …)"),
    fuente_tipo: Optional[str] = Query(None, description="'oficial' o 'osm'"),
):
    """
    Retorna estadísticas de cobertura por tipo de infraestructura y fuente de datos.
    Usa la vista `v_infraestructura_cobertura` del esquema v7.0.

    Útil para verificar cuántos elementos tienen región asignada,
    zona sísmica, y la distribución por fuente oficial vs OSM.
    """
    pool = await db()

    # Intentar usar la vista materializada; si no existe, calcular en línea
    try:
        rows = await pool.fetch("""
            SELECT
                tipo,
                fuente_tipo,
                total,
                con_region,
                con_zona_sismica,
                regiones_distintas,
                criticidad_max,
                criticidad_prom
            FROM v_infraestructura_cobertura
            WHERE ($1::TEXT IS NULL OR tipo        = $1)
              AND ($2::TEXT IS NULL OR fuente_tipo = $2)
            ORDER BY tipo, fuente_tipo
        """, tipo, fuente_tipo)
    except Exception:
        # Fallback: calcular directamente si la vista no existe
        rows = await pool.fetch("""
            SELECT
                tipo,
                fuente_tipo,
                COUNT(*)                                         AS total,
                COUNT(*) FILTER (WHERE region IS NOT NULL)       AS con_region,
                COUNT(*) FILTER (WHERE zona_sismica IS NOT NULL) AS con_zona_sismica,
                COUNT(DISTINCT region)                           AS regiones_distintas,
                MAX(criticidad)                                  AS criticidad_max,
                ROUND(AVG(criticidad)::NUMERIC, 2)               AS criticidad_prom
            FROM infraestructura
            WHERE ($1::TEXT IS NULL OR tipo        = $1)
              AND ($2::TEXT IS NULL OR fuente_tipo = $2)
            GROUP BY tipo, fuente_tipo
            ORDER BY tipo, fuente_tipo
        """, tipo, fuente_tipo)

    # Totales globales
    total_global     = sum(r["total"]          for r in rows)
    total_oficial    = sum(r["total"]          for r in rows if r["fuente_tipo"] == "oficial")
    total_osm        = sum(r["total"]          for r in rows if r["fuente_tipo"] == "osm")
    total_con_region = sum(r["con_region"]     for r in rows)
    total_con_zona   = sum(r["con_zona_sismica"] for r in rows)

    return {
        "cobertura": [dict(r) for r in rows],
        "resumen": {
            "total_elementos":        total_global,
            "total_oficial":          total_oficial,
            "total_osm":              total_osm,
            "pct_oficial":            round(total_oficial / total_global * 100, 1) if total_global else 0,
            "pct_con_region":         round(total_con_region / total_global * 100, 1) if total_global else 0,
            "pct_con_zona_sismica":   round(total_con_zona / total_global * 100, 1) if total_global else 0,
        },
        "tipos_disponibles": sorted({r["tipo"] for r in rows}),
    }


# ══════════════════════════════════════════════════════════
#  RIESGO CONSTRUCCIÓN — RANKING  ← NUEVO v7.0
#  GET /api/v1/riesgo/construccion/ranking
# ══════════════════════════════════════════════════════════

@app.get(
    "/api/v1/riesgo/construccion/ranking",
    summary="Top distritos con mayor índice de riesgo de construcción",
    tags=["Riesgo de Construcción"],
)
async def get_riesgo_construccion_ranking(
    limit:        int            = Query(20, ge=1, le=200),
    departamento: Optional[str] = Query(None, description="Filtrar por departamento"),
    zona_sismica: Optional[int] = Query(None, ge=1, le=4, description="Filtrar por zona NTE E.030"),
    indice_min:   float          = Query(1.0, ge=1.0, le=5.0, description="Índice mínimo"),
):
    """
    Devuelve los distritos ordenados por índice de riesgo de construcción (mayor a menor).

    **Metodología** (CENEPRED 2014 + NTE E.030-2018):
    - 40% Peligro Sísmico (zona NTE E.030)
    - 25% Peligro por Inundación
    - 20% Peligro por Deslizamiento
    - 10% Peligro por Tsunami
    - 5%  Fallas activas en radio 50km

    Fuente: `mv_riesgo_construccion` (vista materializada PostGIS)
    """
    pool = await db()

    # Verificar si la vista materializada existe y tiene datos
    exists = await pool.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'mv_riesgo_construccion'
              AND table_type = 'BASE TABLE'
        )
        OR EXISTS (
            SELECT 1 FROM pg_matviews WHERE matviewname = 'mv_riesgo_construccion'
        )
    """)

    if not exists:
        raise HTTPException(503, detail={
            "error":   "vista_no_disponible",
            "mensaje": "La vista mv_riesgo_construccion no está disponible. "
                       "Ejecuta: docker exec georiesgo_api python procesar_datos.py --solo riesgo_construccion",
        })

    rows = await pool.fetch("""
        SELECT
            id,
            ubigeo,
            distrito,
            provincia,
            departamento,
            zona_sismica,
            COALESCE(factor_z, 0.25)              AS factor_z,
            COALESCE(poblacion, 0)                AS poblacion,
            COALESCE(area_km2, 0)                 AS area_km2,
            peligro_sismico,
            peligro_inundacion,
            peligro_deslizamiento,
            peligro_tsunami,
            fallas_activas_50km,
            sismos_m4_30a_50km,
            ROUND(indice_riesgo_construccion::NUMERIC, 2) AS indice_riesgo_construccion,
            CASE
                WHEN indice_riesgo_construccion >= 4.5 THEN 'MUY ALTO'
                WHEN indice_riesgo_construccion >= 3.5 THEN 'ALTO'
                WHEN indice_riesgo_construccion >= 2.5 THEN 'MEDIO'
                WHEN indice_riesgo_construccion >= 1.5 THEN 'BAJO'
                ELSE 'MUY BAJO'
            END AS nivel_riesgo
        FROM mv_riesgo_construccion
        WHERE indice_riesgo_construccion >= $1
          AND ($2::TEXT IS NULL OR LOWER(departamento) ILIKE '%' || LOWER($2) || '%')
          AND ($3::INT  IS NULL OR zona_sismica = $3)
        ORDER BY indice_riesgo_construccion DESC
        LIMIT $4
    """, indice_min, departamento, zona_sismica, limit)

    resultado = [dict(r) for r in rows]
    # Convertir Decimal a float para serialización JSON
    for r in resultado:
        for k, v in r.items():
            if hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
                r[k] = float(v)

    return {
        "ranking":      resultado,
        "total":        len(resultado),
        "metodologia":  "CENEPRED 2014 + NTE E.030-2018",
        "ponderacion":  "40% sísmico + 25% inundación + 20% deslizamiento + 10% tsunami + 5% fallas",
        "filtros": {
            "departamento": departamento,
            "zona_sismica": zona_sismica,
            "indice_min":   indice_min,
            "limit":        limit,
        },
    }


# ══════════════════════════════════════════════════════════
#  RIESGO CONSTRUCCIÓN — MAPA GEOJSON  ← NUEVO v7.0
#  GET /api/v1/riesgo/construccion/mapa
# ══════════════════════════════════════════════════════════

@app.get(
    "/api/v1/riesgo/construccion/mapa",
    summary="GeoJSON de distritos coloreados por índice de riesgo de construcción",
    tags=["Riesgo de Construcción"],
    response_class=Response,
)
async def get_riesgo_construccion_mapa(
    departamento: Optional[str] = Query(None),
    zona_sismica: Optional[int] = Query(None, ge=1, le=4),
    indice_min:   float          = Query(1.0, ge=1.0, le=5.0),
    zoom:         Optional[int]  = Query(None, ge=1, le=20),
    limit:        int            = Query(500, ge=1, le=2000),
):
    """
    Devuelve GeoJSON con los polígonos de distritos, enriquecidos con:
    - `indice_riesgo_construccion` (1.0 – 5.0)
    - `nivel_riesgo` (MUY BAJO / BAJO / MEDIO / ALTO / MUY ALTO)
    - `color` HEX sugerido para renderizado en Leaflet / MapLibre
    - Todos los componentes de peligro (sísmico, inundación, desliz., tsunami, fallas)

    Requiere que `mv_riesgo_construccion` esté actualizado.
    """
    pool     = await db()
    geom_col = _geom_expr(zoom, col="d.geom")

    rows = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            mv.id,
            mv.ubigeo,
            mv.distrito,
            mv.provincia,
            mv.departamento,
            mv.zona_sismica,
            COALESCE(mv.factor_z, 0.25)                    AS factor_z,
            COALESCE(mv.poblacion, 0)                      AS poblacion,
            mv.peligro_sismico,
            mv.peligro_inundacion,
            mv.peligro_deslizamiento,
            mv.peligro_tsunami,
            mv.fallas_activas_50km,
            mv.sismos_m4_30a_50km,
            ROUND(mv.indice_riesgo_construccion::NUMERIC, 2) AS indice_riesgo_construccion,
            CASE
                WHEN mv.indice_riesgo_construccion >= 4.5 THEN 'MUY ALTO'
                WHEN mv.indice_riesgo_construccion >= 3.5 THEN 'ALTO'
                WHEN mv.indice_riesgo_construccion >= 2.5 THEN 'MEDIO'
                WHEN mv.indice_riesgo_construccion >= 1.5 THEN 'BAJO'
                ELSE 'MUY BAJO'
            END AS nivel_riesgo,
            CASE
                WHEN mv.indice_riesgo_construccion >= 4.5 THEN '#b71c1c'
                WHEN mv.indice_riesgo_construccion >= 3.5 THEN '#e53935'
                WHEN mv.indice_riesgo_construccion >= 2.5 THEN '#fb8c00'
                WHEN mv.indice_riesgo_construccion >= 1.5 THEN '#fdd835'
                ELSE '#43a047'
            END AS color
        FROM mv_riesgo_construccion mv
        JOIN distritos d ON mv.id = d.id
        WHERE mv.indice_riesgo_construccion >= $1
          AND ($2::TEXT IS NULL OR LOWER(mv.departamento) ILIKE '%' || LOWER($2) || '%')
          AND ($3::INT  IS NULL OR mv.zona_sismica = $3)
          AND d.geom IS NOT NULL
        ORDER BY mv.indice_riesgo_construccion DESC
        LIMIT $4
    """, indice_min, departamento, zona_sismica, limit)

    props_keys = [
        "id", "ubigeo", "distrito", "provincia", "departamento",
        "zona_sismica", "factor_z", "poblacion",
        "peligro_sismico", "peligro_inundacion", "peligro_deslizamiento",
        "peligro_tsunami", "fallas_activas_50km", "sismos_m4_30a_50km",
        "indice_riesgo_construccion", "nivel_riesgo", "color",
    ]

    return geojson_response(
        rows_to_features(rows, props_keys),
        {
            "metodologia":  "CENEPRED 2014 + NTE E.030-2018",
            "ponderacion":  "40% sísmico + 25% inundación + 20% deslizamiento + 10% tsunami + 5% fallas",
            "escala_color": {
                "MUY ALTO": "#b71c1c",
                "ALTO":     "#e53935",
                "MEDIO":    "#fb8c00",
                "BAJO":     "#fdd835",
                "MUY BAJO": "#43a047",
            },
            "zoom":   zoom,
            "filtros": {
                "departamento": departamento,
                "zona_sismica": zona_sismica,
                "indice_min":   indice_min,
            },
        },
        cache_seconds=1800,
    )


# ══════════════════════════════════════════════════════════
#  DEPARTAMENTOS  /api/v1/departamentos
# ══════════════════════════════════════════════════════════

DEPT_PROPS = ["id", "ubigeo", "nombre", "nivel_riesgo", "zona_sismica", "factor_z", "area_km2", "capital", "fuente"]


@app.get(
    "/api/v1/departamentos",
    summary="Polígonos de departamentos/regiones",
    tags=["Administrativo"],
    response_class=Response,
)
async def get_departamentos(
    riesgo_min: int            = Query(1, ge=1, le=5),
    nombre:     Optional[str]  = Query(None),
    zoom:       Optional[int]  = Query(None, ge=1, le=20),
):
    pool     = await db()
    geom_col = _geom_expr(zoom)
    rows     = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id, ubigeo, nombre, nivel_riesgo,
            COALESCE(zona_sismica, 2) AS zona_sismica,
            COALESCE(factor_z, 0.25) AS factor_z,
            area_km2, capital, fuente
        FROM departamentos
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT IS NULL OR nombre ILIKE '%' || $2 || '%')
        ORDER BY nombre
    """, riesgo_min, nombre)
    return geojson_response(
        rows_to_features(rows, DEPT_PROPS),
        {"zoom": zoom, "simplificacion_grados": _simplify_tolerance(zoom)},
        cache_seconds=3600,
    )


# ══════════════════════════════════════════════════════════
#  SISMOS  /api/v1/sismos
# ══════════════════════════════════════════════════════════

SISMOS_PROPS = [
    "usgs_id", "magnitud", "profundidad_km", "tipo_profundidad",
    "fecha", "lugar", "region", "tipo_magnitud", "estado",
]


@app.get(
    "/api/v1/sismos",
    summary="Catálogo sísmico completo con filtros",
    tags=["Sismos"],
    response_class=Response,
)
async def get_sismos(
    mag_min:    float          = Query(3.0,  ge=0,    le=10),
    mag_max:    float          = Query(9.9,  ge=0,    le=10),
    year_start: int            = Query(1960, ge=1900, le=2100),
    year_end:   int            = Query(2030, ge=1900, le=2100),
    prof_tipo:  Optional[str]  = Query(None),
    region:     Optional[str]  = Query(None),
    limit:      int            = Query(5000, ge=1,    le=20000),
    offset:     int            = Query(0,    ge=0),
):
    if mag_min > mag_max:
        raise HTTPException(400, detail={"error": "parametro_invalido",
                                          "mensaje": "mag_min no puede ser mayor que mag_max"})
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, lugar,
            COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
            tipo_magnitud, estado
        FROM sismos
        WHERE magnitud      BETWEEN $1 AND $2
          AND EXTRACT(YEAR FROM fecha) BETWEEN $3 AND $4
          AND ($5::TEXT IS NULL OR tipo_profundidad = $5)
          AND ($6::TEXT IS NULL OR region ILIKE '%' || $6 || '%')
        ORDER BY fecha DESC, magnitud DESC
        LIMIT $7 OFFSET $8
    """, mag_min, mag_max, year_start, year_end, prof_tipo, region, limit, offset)

    return geojson_response(
        rows_to_features(rows, SISMOS_PROPS),
        {
            "filtros": {"mag_min": mag_min, "mag_max": mag_max,
                        "year_start": year_start, "year_end": year_end,
                        "prof_tipo": prof_tipo, "region": region},
            "paginacion": {"limit": limit, "offset": offset},
        },
    )


@app.get(
    "/api/v1/sismos/recientes",
    summary="Sismos de los últimos N días",
    tags=["Sismos"],
    response_class=Response,
)
async def get_sismos_recientes(
    dias:    int   = Query(30,  ge=1,  le=365),
    mag_min: float = Query(2.5, ge=0,  le=10),
    limit:   int   = Query(500, ge=1,  le=2000),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, lugar,
            COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
            tipo_magnitud, estado
        FROM sismos
        WHERE fecha    >= CURRENT_DATE - ($1 * INTERVAL '1 day')
          AND magnitud >= $2
        ORDER BY fecha DESC, magnitud DESC
        LIMIT $3
    """, dias, mag_min, limit)
    return geojson_response(
        rows_to_features(rows, SISMOS_PROPS),
        {"dias": dias, "mag_min": mag_min},
        cache_seconds=120,
    )


@app.get("/api/v1/sismos/estadisticas", summary="Estadísticas sísmicas por año", tags=["Sismos"])
async def get_estadisticas(
    year_start: int   = Query(1960, ge=1900, le=2100),
    year_end:   int   = Query(2030, ge=1900, le=2100),
    mag_min:    float = Query(2.5,  ge=0,    le=10),
    region:     Optional[str] = Query(None),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            EXTRACT(YEAR FROM fecha)::INTEGER         AS anio,
            COUNT(*)                                   AS cantidad,
            ROUND(MAX(magnitud)::NUMERIC, 1)           AS magnitud_max,
            ROUND(AVG(magnitud)::NUMERIC, 2)           AS magnitud_prom,
            COUNT(*) FILTER (WHERE tipo_profundidad='superficial')  AS superficiales,
            COUNT(*) FILTER (WHERE tipo_profundidad='intermedio')   AS intermedios,
            COUNT(*) FILTER (WHERE tipo_profundidad='profundo')     AS profundos,
            COUNT(*) FILTER (WHERE magnitud >= 5.0)   AS m5_plus,
            COUNT(*) FILTER (WHERE magnitud >= 6.0)   AS m6_plus,
            COUNT(*) FILTER (WHERE magnitud >= 7.0)   AS m7_plus
        FROM sismos
        WHERE EXTRACT(YEAR FROM fecha) BETWEEN $1 AND $2
          AND magnitud >= $3
          AND ($4::TEXT IS NULL OR region ILIKE '%' || $4 || '%')
        GROUP BY EXTRACT(YEAR FROM fecha)
        ORDER BY anio
    """, year_start, year_end, mag_min, region)
    return [dict(r) for r in rows]


@app.get(
    "/api/v1/sismos/heatmap",
    summary="Grid de densidad sísmica para mapas de calor",
    tags=["Sismos"],
    response_class=Response,
)
async def get_heatmap_sismos(
    resolucion: float = Query(0.1, ge=0.05, le=1.0),
    mag_min:    float = Query(3.0, ge=0,    le=10),
    year_start: Optional[int] = Query(None, ge=1900),
    year_end:   Optional[int] = Query(None, le=2100),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(ST_Centroid(ST_SnapToGrid(geom, $1)), 6)::TEXT AS geom_json,
            COUNT(*)                                AS cantidad,
            ROUND(AVG(magnitud)::NUMERIC, 2)        AS magnitud_prom,
            ROUND(MAX(magnitud)::NUMERIC, 1)        AS magnitud_max,
            ROUND(AVG(profundidad_km)::NUMERIC, 1)  AS prof_prom
        FROM sismos
        WHERE magnitud >= $2
          AND ($3::INT IS NULL OR EXTRACT(YEAR FROM fecha) >= $3)
          AND ($4::INT IS NULL OR EXTRACT(YEAR FROM fecha) <= $4)
        GROUP BY ST_SnapToGrid(geom, $1)
        HAVING COUNT(*) > 0
        ORDER BY cantidad DESC
    """, resolucion, mag_min, year_start, year_end)

    features = []
    for row in rows:
        if row["geom_json"]:
            features.append({
                "type": "Feature",
                "geometry": json.loads(row["geom_json"]),
                "properties": {
                    "cantidad":      row["cantidad"],
                    "magnitud_prom": float(row["magnitud_prom"]),
                    "magnitud_max":  float(row["magnitud_max"]),
                    "prof_prom":     float(row["prof_prom"]),
                },
            })
    return geojson_response(features, {"resolucion_grados": resolucion})


@app.get(
    "/api/v1/sismos/cercanos",
    summary="Sismos cercanos a un punto (KNN + DWithin)",
    tags=["Sismos"],
)
async def get_sismos_cercanos(
    lon:      float = Query(..., ge=-82,   le=-68),
    lat:      float = Query(..., ge=-18.5, le=0),
    radio_km: int   = Query(50,  ge=1,  le=500),
    mag_min:  float = Query(3.0, ge=0,  le=10),
    limit:    int   = Query(100, ge=1,  le=1000),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, lugar,
            COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
            ROUND(
                (ST_Distance(geom::GEOGRAPHY,
                 ST_SetSRID(ST_MakePoint($1, $2), 4326)::GEOGRAPHY) / 1000)::NUMERIC, 1
            ) AS distancia_km
        FROM sismos
        WHERE magnitud >= $3
          AND ST_DWithin(
              geom::GEOGRAPHY,
              ST_SetSRID(ST_MakePoint($1, $2), 4326)::GEOGRAPHY,
              $4 * 1000
          )
        ORDER BY distancia_km ASC
        LIMIT $5
    """, lon, lat, mag_min, radio_km, limit)
    return [dict(r) for r in rows]


@app.get("/api/v1/sismos/{usgs_id}", summary="Detalle de un sismo por ID USGS", tags=["Sismos"])
async def get_sismo_detalle(usgs_id: str):
    pool = await db()
    row  = await pool.fetchrow("""
        SELECT
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, hora_utc::TEXT AS hora_utc,
            lugar,
            COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
            tipo_magnitud, estado, fuente,
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM sismos WHERE usgs_id = $1
    """, usgs_id)
    if not row:
        raise HTTPException(404, detail={"error": "not_found", "usgs_id": usgs_id})
    d = dict(row)
    d["geom"] = json.loads(d.pop("geom_json"))
    return d


# ══════════════════════════════════════════════════════════
#  DISTRITOS  /api/v1/distritos
# ══════════════════════════════════════════════════════════

DIST_PROPS = ["id", "ubigeo", "nombre", "provincia", "departamento",
              "nivel_riesgo", "poblacion", "area_km2", "zona_sismica", "fuente"]


@app.get(
    "/api/v1/distritos",
    summary="Polígonos de distritos con nivel de riesgo",
    tags=["Administrativo"],
    response_class=Response,
)
async def get_distritos(
    provincia:    Optional[str] = Query(None),
    departamento: Optional[str] = Query(None),
    riesgo_min:   int           = Query(1, ge=1, le=5),
    zoom:         Optional[int] = Query(None, ge=1, le=20),
    limit:        int           = Query(500, ge=1, le=2000),
):
    pool     = await db()
    geom_col = _geom_expr(zoom)
    rows     = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id, ubigeo, nombre, provincia, departamento,
            nivel_riesgo, poblacion, area_km2,
            COALESCE(zona_sismica, 2) AS zona_sismica,
            fuente
        FROM distritos
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT IS NULL OR LOWER(provincia)    ILIKE '%' || LOWER($2) || '%')
          AND ($3::TEXT IS NULL OR LOWER(departamento) ILIKE '%' || LOWER($3) || '%')
        ORDER BY nivel_riesgo DESC, nombre
        LIMIT $4
    """, riesgo_min, provincia, departamento, limit)
    return geojson_response(
        rows_to_features(rows, DIST_PROPS),
        {"zoom": zoom, "simplificacion_grados": _simplify_tolerance(zoom)},
        cache_seconds=3600,
    )


@app.get("/api/v1/distritos/resumen", summary="Estadísticas sísmicas por distrito", tags=["Administrativo"])
async def get_distritos_resumen():
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            d.nombre, d.provincia, d.departamento, d.nivel_riesgo,
            COUNT(s.id)                                   AS total_sismos,
            ROUND(MAX(s.magnitud)::NUMERIC, 1)            AS max_magnitud,
            ROUND(AVG(s.magnitud)::NUMERIC, 2)            AS avg_magnitud,
            COUNT(s.id) FILTER (WHERE s.magnitud >= 5.0)  AS m5_plus
        FROM distritos d
        LEFT JOIN sismos s ON ST_Covers(d.geom, s.geom)
        GROUP BY d.nombre, d.provincia, d.departamento, d.nivel_riesgo
        ORDER BY total_sismos DESC
        LIMIT 100
    """)
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════
#  FALLAS GEOLÓGICAS  /api/v1/fallas
# ══════════════════════════════════════════════════════════

FALLAS_PROPS = [
    "id", "ingemmet_id", "nombre", "nombre_alt", "activa", "tipo",
    "mecanismo", "longitud_km", "magnitud_max", "region", "fuente", "referencia",
]


@app.get(
    "/api/v1/fallas",
    summary="Fallas geológicas — cobertura nacional",
    tags=["Geología"],
    response_class=Response,
)
async def get_fallas(
    activas_only: bool            = Query(False),
    tipo:         Optional[str]   = Query(None),
    mecanismo:    Optional[str]   = Query(None),
    region:       Optional[str]   = Query(None),
    mag_min:      Optional[float] = Query(None, ge=0, le=10),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, ingemmet_id, nombre, nombre_alt, activa, tipo,
            mecanismo, longitud_km, magnitud_max,
            COALESCE(region, f_asignar_region(
                ST_X(ST_Centroid(geom)), ST_Y(ST_Centroid(geom))
            )) AS region,
            fuente, referencia
        FROM fallas
        WHERE ($1 = FALSE OR activa = TRUE)
          AND ($2::TEXT IS NULL OR tipo      ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR mecanismo ILIKE '%' || $3 || '%')
          AND ($4::TEXT IS NULL OR region    ILIKE '%' || $4 || '%')
          AND ($5::FLOAT IS NULL OR magnitud_max >= $5)
        ORDER BY activa DESC, longitud_km DESC NULLS LAST, nombre
    """, activas_only, tipo, mecanismo, region, mag_min)
    return geojson_response(rows_to_features(rows, FALLAS_PROPS), cache_seconds=3600)


# ══════════════════════════════════════════════════════════
#  INUNDACIONES  /api/v1/inundaciones
# ══════════════════════════════════════════════════════════

INUND_PROPS = [
    "id", "nombre", "nivel_riesgo", "tipo_inundacion",
    "periodo_retorno", "profundidad_max_m", "cuenca", "region", "fuente",
]


@app.get(
    "/api/v1/inundaciones",
    summary="Zonas de inundación",
    tags=["Hidrometeorología"],
    response_class=Response,
)
async def get_inundaciones(
    riesgo_min:  int            = Query(1, ge=1, le=5),
    tipo:        Optional[str]  = Query(None),
    region:      Optional[str]  = Query(None),
    cuenca:      Optional[str]  = Query(None),
    periodo_max: Optional[int]  = Query(None, ge=1),
    zoom:        Optional[int]  = Query(None, ge=1, le=20),
):
    pool     = await db()
    geom_col = _geom_expr(zoom)
    rows     = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id, nombre, nivel_riesgo, tipo_inundacion,
            periodo_retorno, profundidad_max_m, cuenca, region, fuente
        FROM zonas_inundables
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT IS NULL OR tipo_inundacion ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR region          ILIKE '%' || $3 || '%')
          AND ($4::TEXT IS NULL OR cuenca          ILIKE '%' || $4 || '%')
          AND ($5::INT  IS NULL OR periodo_retorno <= $5)
        ORDER BY nivel_riesgo DESC, periodo_retorno ASC NULLS LAST
    """, riesgo_min, tipo, region, cuenca, periodo_max)
    return geojson_response(rows_to_features(rows, INUND_PROPS), {"zoom": zoom}, cache_seconds=1800)


# ══════════════════════════════════════════════════════════
#  TSUNAMIS  /api/v1/tsunamis
# ══════════════════════════════════════════════════════════

TSUN_PROPS = [
    "id", "nombre", "nivel_riesgo", "altura_ola_m",
    "tiempo_arribo_min", "periodo_retorno", "region", "fuente",
]


@app.get(
    "/api/v1/tsunamis",
    summary="Zonas de inundación por tsunami",
    tags=["Hidrometeorología"],
    response_class=Response,
)
async def get_tsunamis(
    riesgo_min:  int            = Query(1, ge=1, le=5),
    region:      Optional[str]  = Query(None),
    altura_min:  Optional[float]= Query(None, ge=0),
    zoom:        Optional[int]  = Query(None, ge=1, le=20),
):
    pool     = await db()
    geom_col = _geom_expr(zoom)
    rows     = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id, nombre, nivel_riesgo, altura_ola_m,
            tiempo_arribo_min, periodo_retorno, region, fuente
        FROM zonas_tsunami
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT  IS NULL OR region      ILIKE '%' || $2 || '%')
          AND ($3::FLOAT IS NULL OR altura_ola_m >= $3)
        ORDER BY nivel_riesgo DESC, altura_ola_m DESC NULLS LAST
    """, riesgo_min, region, altura_min)
    return geojson_response(rows_to_features(rows, TSUN_PROPS), cache_seconds=3600)


# ══════════════════════════════════════════════════════════
#  DESLIZAMIENTOS  /api/v1/deslizamientos
# ══════════════════════════════════════════════════════════

DESL_PROPS = ["id", "nombre", "tipo", "nivel_riesgo", "area_km2", "region", "activo", "fuente"]


@app.get(
    "/api/v1/deslizamientos",
    summary="Deslizamientos, huaycos y remoción en masa",
    tags=["Geología"],
    response_class=Response,
)
async def get_deslizamientos(
    riesgo_min: int            = Query(1, ge=1, le=5),
    tipo:       Optional[str]  = Query(None),
    region:     Optional[str]  = Query(None),
    activos:    Optional[bool] = Query(None),
    zoom:       Optional[int]  = Query(None, ge=1, le=20),
):
    pool     = await db()
    geom_col = _geom_expr(zoom)
    rows     = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id, nombre, tipo, nivel_riesgo, area_km2, region, activo, fuente
        FROM deslizamientos
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT IS NULL OR tipo   ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR region ILIKE '%' || $3 || '%')
          AND ($4::BOOL IS NULL OR activo = $4)
        ORDER BY nivel_riesgo DESC, area_km2 DESC NULLS LAST
    """, riesgo_min, tipo, region, activos)
    return geojson_response(rows_to_features(rows, DESL_PROPS), {"zoom": zoom}, cache_seconds=1800)


# ══════════════════════════════════════════════════════════
#  INFRAESTRUCTURA CRÍTICA  /api/v1/infraestructura
# ══════════════════════════════════════════════════════════

INFRA_PROPS = [
    "id", "osm_id", "nombre", "tipo", "criticidad",
    "estado", "region", "distrito", "fuente", "fuente_tipo",
]


@app.get(
    "/api/v1/infraestructura",
    summary="Infraestructura crítica (hospitales, escuelas, puertos…)",
    tags=["Infraestructura"],
    response_class=Response,
)
async def get_infraestructura(
    tipo:           Optional[str]  = Query(None),
    criticidad_min: int            = Query(1, ge=1, le=5),
    region:         Optional[str]  = Query(None),
    fuente_tipo:    Optional[str]  = Query(None, description="'oficial' o 'osm'"),
    radio_km:       Optional[int]  = Query(None, ge=1, le=500),
    lon:            Optional[float]= Query(None, ge=-82, le=-68),
    lat:            Optional[float]= Query(None, ge=-18.5, le=0),
    limit:          int            = Query(500, ge=1, le=2000),
):
    pool = await db()

    if radio_km is not None and (lon is None or lat is None):
        raise HTTPException(400, detail={
            "error": "parametros_faltantes",
            "mensaje": "radio_km requiere los parámetros lon y lat",
        })

    spatial_enabled = bool(radio_km and lon is not None and lat is not None)
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, osm_id, nombre, tipo, criticidad,
            estado,
            COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
            distrito, fuente,
            COALESCE(fuente_tipo, 'osm') AS fuente_tipo
        FROM infraestructura
        WHERE criticidad >= $1
          AND ($2::TEXT IS NULL OR tipo        ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR region      ILIKE '%' || $3 || '%')
          AND ($9::TEXT IS NULL OR fuente_tipo = $9)
          AND (
              NOT $5::BOOLEAN
              OR ST_DWithin(
                  geom::GEOGRAPHY,
                  ST_SetSRID(ST_MakePoint($6::FLOAT, $7::FLOAT), 4326)::GEOGRAPHY,
                  $8::FLOAT * 1000
              )
          )
        ORDER BY criticidad DESC, nombre
        LIMIT $4
    """,
        criticidad_min, tipo, region, limit,
        spatial_enabled,
        lon if lon is not None else 0.0,
        lat if lat is not None else 0.0,
        float(radio_km) if radio_km else 0.0,
        fuente_tipo,
    )
    return geojson_response(rows_to_features(rows, INFRA_PROPS))


# ══════════════════════════════════════════════════════════
#  ESTACIONES  /api/v1/estaciones
# ══════════════════════════════════════════════════════════

EST_PROPS = ["id", "codigo", "nombre", "tipo", "altitud_m",
             "activa", "institucion", "region", "red"]


@app.get(
    "/api/v1/estaciones",
    summary="Estaciones sísmicas, meteorológicas e hidrométricas",
    tags=["Monitoreo"],
    response_class=Response,
)
async def get_estaciones(
    tipo:        Optional[str] = Query(None),
    institucion: Optional[str] = Query(None),
    region:      Optional[str] = Query(None),
    activas:     bool          = Query(True),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, codigo, nombre, tipo, altitud_m,
            activa, institucion,
            COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
            red
        FROM estaciones
        WHERE ($1 = FALSE OR activa = TRUE)
          AND ($2::TEXT IS NULL OR tipo        ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR institucion ILIKE '%' || $3 || '%')
          AND ($4::TEXT IS NULL OR region      ILIKE '%' || $4 || '%')
        ORDER BY institucion, tipo, nombre
    """, activas, tipo, institucion, region)
    return geojson_response(rows_to_features(rows, EST_PROPS), cache_seconds=3600)


# ══════════════════════════════════════════════════════════
#  BÚSQUEDA ESPACIAL POR BBOX  /api/v1/bbox
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/bbox", summary="Consulta todas las capas dentro de un bounding box", tags=["Espacial"])
async def get_por_bbox(
    min_lon: float = Query(..., ge=-82,   le=-68),
    min_lat: float = Query(..., ge=-18.5, le=0),
    max_lon: float = Query(..., ge=-82,   le=-68),
    max_lat: float = Query(..., ge=-18.5, le=0),
    capas:   str   = Query("sismos,fallas,inundaciones"),
    mag_min: float = Query(3.0, ge=0, le=10),
    zoom:    Optional[int] = Query(None, ge=1, le=20),
):
    if min_lon >= max_lon or min_lat >= max_lat:
        raise HTTPException(400, detail={"error": "bbox_invalido",
                                          "mensaje": "min_lon < max_lon y min_lat < max_lat"})

    pool       = await db()
    capas_list = [c.strip().lower() for c in capas.split(",")]
    bbox       = f"ST_MakeEnvelope({min_lon},{min_lat},{max_lon},{max_lat},4326)"
    resultado  = {}
    geom_poly  = _geom_expr(zoom)

    if "sismos" in capas_list:
        rows = await pool.fetch(f"""
            SELECT ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
                   usgs_id, magnitud, profundidad_km, tipo_profundidad,
                   fecha::TEXT AS fecha, lugar,
                   COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region
            FROM sismos
            WHERE magnitud >= $1 AND geom && {bbox}
            ORDER BY magnitud DESC LIMIT 2000
        """, mag_min)
        resultado["sismos"] = {"type": "FeatureCollection",
                                "features": rows_to_features(rows, ["usgs_id","magnitud","profundidad_km","tipo_profundidad","fecha","lugar","region"])}

    if "fallas" in capas_list:
        rows = await pool.fetch(f"""
            SELECT ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
                   nombre, activa, tipo, longitud_km,
                   COALESCE(region, f_asignar_region(ST_X(ST_Centroid(geom)), ST_Y(ST_Centroid(geom)))) AS region
            FROM fallas WHERE geom && {bbox}
        """)
        resultado["fallas"] = {"type": "FeatureCollection",
                                "features": rows_to_features(rows, ["nombre","activa","tipo","longitud_km","region"])}

    if "inundaciones" in capas_list:
        rows = await pool.fetch(f"""
            SELECT {geom_poly} AS geom_json, nombre, nivel_riesgo, tipo_inundacion, region
            FROM zonas_inundables WHERE geom && {bbox}
        """)
        resultado["inundaciones"] = {"type": "FeatureCollection",
                                      "features": rows_to_features(rows, ["nombre","nivel_riesgo","tipo_inundacion","region"])}

    if "tsunamis" in capas_list:
        rows = await pool.fetch(f"""
            SELECT {geom_poly} AS geom_json, nombre, nivel_riesgo, altura_ola_m, region
            FROM zonas_tsunami WHERE geom && {bbox}
        """)
        resultado["tsunamis"] = {"type": "FeatureCollection",
                                  "features": rows_to_features(rows, ["nombre","nivel_riesgo","altura_ola_m","region"])}

    if "deslizamientos" in capas_list:
        rows = await pool.fetch(f"""
            SELECT {geom_poly} AS geom_json, nombre, tipo, nivel_riesgo, region
            FROM deslizamientos WHERE geom && {bbox}
        """)
        resultado["deslizamientos"] = {"type": "FeatureCollection",
                                        "features": rows_to_features(rows, ["nombre","tipo","nivel_riesgo","region"])}

    if "infraestructura" in capas_list:
        rows = await pool.fetch(f"""
            SELECT ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
                   nombre, tipo, criticidad,
                   COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region
            FROM infraestructura WHERE geom && {bbox}
            ORDER BY criticidad DESC LIMIT 500
        """)
        resultado["infraestructura"] = {"type": "FeatureCollection",
                                         "features": rows_to_features(rows, ["nombre","tipo","criticidad","region"])}

    if "departamentos" in capas_list:
        rows = await pool.fetch(f"""
            SELECT {geom_poly} AS geom_json, nombre, nivel_riesgo,
                   COALESCE(zona_sismica, 2) AS zona_sismica
            FROM departamentos WHERE geom && {bbox}
        """)
        resultado["departamentos"] = {"type": "FeatureCollection",
                                       "features": rows_to_features(rows, ["nombre","nivel_riesgo","zona_sismica"])}

    if "distritos" in capas_list:
        rows = await pool.fetch(f"""
            SELECT {geom_poly} AS geom_json, nombre, provincia, departamento, nivel_riesgo
            FROM distritos WHERE geom && {bbox}
            LIMIT 200
        """)
        resultado["distritos"] = {"type": "FeatureCollection",
                                   "features": rows_to_features(rows, ["nombre","provincia","departamento","nivel_riesgo"])}

    resultado["_meta"] = {
        "bbox": [min_lon, min_lat, max_lon, max_lat],
        "capas_solicitadas": capas_list,
        "capas_devueltas":   [k for k in resultado if not k.startswith("_")],
        "zoom": zoom,
    }
    return resultado


# ══════════════════════════════════════════════════════════
#  RESUMEN  /api/v1/resumen
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/resumen", summary="Panel de control — resumen general", tags=["Sistema"])
async def get_resumen():
    pool = await db()
    stats = await pool.fetchrow("""
        SELECT
            COUNT(*)                                          AS total_sismos,
            ROUND(MAX(magnitud)::NUMERIC, 1)                  AS max_magnitud,
            ROUND(AVG(magnitud)::NUMERIC, 2)                  AS avg_magnitud,
            COUNT(*) FILTER (WHERE magnitud >= 7.0)           AS m7_plus,
            COUNT(*) FILTER (WHERE fecha >= CURRENT_DATE - INTERVAL '30 days') AS ultimos_30d,
            COUNT(*) FILTER (WHERE fecha >= CURRENT_DATE - INTERVAL '7 days')  AS ultimos_7d,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial') AS superficiales,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'intermedio')  AS intermedios,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'profundo')    AS profundos,
            MIN(fecha)::TEXT AS desde,
            MAX(fecha)::TEXT AS hasta
        FROM sismos
    """)
    ultimos = await pool.fetch("""
        SELECT usgs_id, magnitud, fecha::TEXT, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
               profundidad_km, tipo_profundidad
        FROM sismos
        WHERE magnitud >= 4.0
        ORDER BY fecha DESC, magnitud DESC
        LIMIT 10
    """)
    fallas_res = await pool.fetch("""
        SELECT tipo, COUNT(*) AS cantidad, BOOL_OR(activa) AS hay_activas
        FROM fallas GROUP BY tipo ORDER BY cantidad DESC
    """)
    capas_counts = await pool.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM departamentos)    AS departamentos,
            (SELECT COUNT(*) FROM distritos)        AS distritos,
            (SELECT COUNT(*) FROM fallas)           AS fallas,
            (SELECT COUNT(*) FROM zonas_inundables) AS inundaciones,
            (SELECT COUNT(*) FROM zonas_tsunami)    AS tsunamis,
            (SELECT COUNT(*) FROM deslizamientos)   AS deslizamientos,
            (SELECT COUNT(*) FROM infraestructura)  AS infraestructura,
            (SELECT COUNT(*) FROM estaciones)       AS estaciones
    """)
    return {
        "sismos":                 dict(stats),
        "ultimos_significativos": [dict(r) for r in ultimos],
        "fallas": {
            "total":    sum(r["cantidad"] for r in fallas_res),
            "por_tipo": [dict(r) for r in fallas_res],
        },
        "capas": dict(capas_counts),
    }


# ══════════════════════════════════════════════════════════
#  RIESGO PUNTO  /api/v1/riesgo
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/riesgo", summary="Resumen de riesgo para un punto geográfico", tags=["Espacial"])
async def get_riesgo_punto(
    lon: float = Query(..., ge=-82,   le=-68,   description="Longitud WGS84"),
    lat: float = Query(..., ge=-18.5, le=0,     description="Latitud WGS84"),
):
    pool = await db()
    row  = await pool.fetchrow("SELECT f_riesgo_punto($1, $2) AS resultado", lon, lat)
    return row["resultado"]


# ══════════════════════════════════════════════════════════
#  ADMIN / SYNC LOG
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/sync/log", summary="Historial de sincronizaciones ETL", tags=["Sistema"])
async def get_sync_log(limit: int = Query(20, ge=1, le=100)):
    pool = await db()
    rows = await pool.fetch("""
        SELECT fuente, tabla, registros, estado, detalle,
               duracion_s, inicio::TEXT, fin::TEXT
        FROM sync_log
        ORDER BY fin DESC NULLS FIRST
        LIMIT $1
    """, limit)
    return [dict(r) for r in rows]


@app.get("/api/v1/sync/status", summary="Estado actual de todas las tablas", tags=["Sistema"])
async def get_sync_status():
    pool = await db()
    rows = await pool.fetch("""
        SELECT DISTINCT ON (tabla) fuente, tabla, registros, estado, fin::TEXT AS ultima_sync
        FROM sync_log
        WHERE fin IS NOT NULL
        ORDER BY tabla, fin DESC
    """)
    return [dict(r) for r in rows]