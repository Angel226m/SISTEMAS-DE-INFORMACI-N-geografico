# ══════════════════════════════════════════════════════════
# GeoRiesgo Perú — API FastAPI v4.0
# Endpoints organizados por capa temática
# Respuestas GeoJSON RFC 7946 con coordenadas precisas WGS84
# ══════════════════════════════════════════════════════════

from __future__ import annotations

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
        server_settings={"application_name": "georiesgo_api"},
    )
    yield
    if _pool:
        await _pool.close()


# ── App ───────────────────────────────────────────────────
app = FastAPI(
    title="GeoRiesgo Perú API",
    description="""
API de datos geoespaciales de riesgo sísmico y geológico para Perú.

**Fuentes de datos:**
- Sismos: USGS FDSNWS (catalogo desde 1900)
- Fallas: INGEMMET + dataset científico nacional (Audin et al. 2008 / IGP)
- Inundaciones: ANA + CENEPRED + SENAMHI
- Tsunamis: PREDES / IGP / INDECI
- Infraestructura: OpenStreetMap
- Estaciones: IGP (Red Sísmica Nacional) + SENAMHI

**Coordenadas:** WGS84 (EPSG:4326) — formato GeoJSON RFC 7946
    """,
    version="4.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS abierto (restringir en producción con dominios específicos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
    max_age=3600,
)

# Compresión automática para respuestas GeoJSON grandes
app.add_middleware(GZipMiddleware, minimum_size=1024)


# ── Utilidades ────────────────────────────────────────────

async def db() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(503, "Base de datos no disponible")
    return _pool


def geojson_response(features: list, metadata: dict | None = None) -> Response:
    """Respuesta GeoJSON con orjson (más rápido que json estándar)."""
    fc = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total": len(features),
            "crs": "EPSG:4326",
            **(metadata or {}),
        },
    }
    return Response(
        content=orjson.dumps(fc),
        media_type="application/geo+json",
        headers={"Cache-Control": "public, max-age=300"},
    )


def row_to_feature(row: asyncpg.Record, props_keys: list[str]) -> dict | None:
    geom_str = row.get("geom_json")
    if not geom_str:
        return None
    try:
        geom = json.loads(geom_str)
    except Exception:
        return None
    props = {}
    for k in props_keys:
        try:
            v = row[k]
            # asyncpg puede devolver Decimal o date — convertir
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            elif hasattr(v, "__float__"):
                v = float(v)
            props[k] = v
        except (KeyError, IndexError):
            pass
    return {"type": "Feature", "geometry": geom, "properties": props}


def rows_to_features(rows, props_keys: list[str]) -> list[dict]:
    return [f for row in rows if (f := row_to_feature(row, props_keys)) is not None]


# ══════════════════════════════════════════════════════════
#  ROOT / HEALTH
# ══════════════════════════════════════════════════════════

@app.get("/", summary="Estado general de la API y conteo de registros")
async def root():
    pool = await db()
    row = await pool.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM sismos)           AS sismos,
            (SELECT COUNT(*) FROM distritos)        AS distritos,
            (SELECT COUNT(*) FROM fallas)           AS fallas,
            (SELECT COUNT(*) FROM zonas_inundables) AS inundaciones,
            (SELECT COUNT(*) FROM zonas_tsunami)    AS tsunamis,
            (SELECT COUNT(*) FROM infraestructura)  AS infraestructura,
            (SELECT COUNT(*) FROM estaciones)       AS estaciones,
            (SELECT MAX(fecha)::TEXT FROM sismos)   AS ultimo_sismo
    """)
    return {
        "api":     "GeoRiesgo Perú v4.0",
        "docs":    "/docs",
        "redoc":   "/redoc",
        "capas":   dict(row),
        "fuentes": ["USGS", "IGP", "INGEMMET", "ANA", "CENEPRED",
                    "PREDES", "INDECI", "SENAMHI", "OpenStreetMap"],
    }


@app.get("/health", summary="Healthcheck para Docker")
async def health():
    pool = await db()
    await pool.fetchval("SELECT 1")
    return {"status": "ok", "ts": time.time()}


# ══════════════════════════════════════════════════════════
#  SISMOS  /api/v1/sismos
# ══════════════════════════════════════════════════════════

SISMOS_PROPS = [
    "usgs_id", "magnitud", "profundidad_km", "tipo_profundidad",
    "fecha", "lugar", "region", "tipo_magnitud", "estado",
]


@app.get("/api/v1/sismos",
         summary="Catálogo sísmico completo con filtros",
         response_class=Response,
         responses={200: {"content": {"application/geo+json": {}}}})
async def get_sismos(
    mag_min:    float = Query(3.0,  ge=0,    le=10,  description="Magnitud mínima"),
    mag_max:    float = Query(9.9,  ge=0,    le=10,  description="Magnitud máxima"),
    year_start: int   = Query(1960, ge=1900, le=2100, description="Año inicial"),
    year_end:   int   = Query(2030, ge=1900, le=2100, description="Año final"),
    prof_tipo:  Optional[str] = Query(None, description="superficial|intermedio|profundo"),
    region:     Optional[str] = Query(None, description="Región/departamento del Perú"),
    limit:      int   = Query(5000, ge=1,   le=20000, description="Límite de registros"),
    offset:     int   = Query(0,    ge=0,             description="Paginación"),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT  AS geom_json,
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, lugar, region, tipo_magnitud, estado
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
        {"filtros": {"mag_min": mag_min, "mag_max": mag_max,
                     "year_start": year_start, "year_end": year_end,
                     "prof_tipo": prof_tipo, "region": region},
         "paginacion": {"limit": limit, "offset": offset}},
    )


@app.get("/api/v1/sismos/recientes",
         summary="Sismos de los últimos N días",
         response_class=Response)
async def get_sismos_recientes(
    dias:    int   = Query(30,  ge=1,  le=365, description="Días hacia atrás"),
    mag_min: float = Query(2.5, ge=0,  le=10),
    limit:   int   = Query(500, ge=1,  le=2000),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, lugar, region, tipo_magnitud, estado
        FROM sismos
        WHERE fecha    >= CURRENT_DATE - ($1 * INTERVAL '1 day')
          AND magnitud >= $2
        ORDER BY fecha DESC, magnitud DESC
        LIMIT $3
    """, dias, mag_min, limit)
    return geojson_response(
        rows_to_features(rows, SISMOS_PROPS),
        {"dias": dias, "mag_min": mag_min},
    )


@app.get("/api/v1/sismos/estadisticas",
         summary="Estadísticas sísmicas agrupadas por año")
async def get_estadisticas(
    year_start: int   = Query(1960, ge=1900, le=2100),
    year_end:   int   = Query(2030, ge=1900, le=2100),
    mag_min:    float = Query(2.5,  ge=0,    le=10),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            EXTRACT(YEAR FROM fecha)::INTEGER          AS anio,
            COUNT(*)                                    AS cantidad,
            ROUND(MAX(magnitud)::NUMERIC, 1)            AS magnitud_max,
            ROUND(AVG(magnitud)::NUMERIC, 2)            AS magnitud_prom,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial')  AS superficiales,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'intermedio')   AS intermedios,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'profundo')     AS profundos,
            COUNT(*) FILTER (WHERE magnitud >= 5.0)    AS m5_plus,
            COUNT(*) FILTER (WHERE magnitud >= 6.0)    AS m6_plus,
            COUNT(*) FILTER (WHERE magnitud >= 7.0)    AS m7_plus
        FROM sismos
        WHERE EXTRACT(YEAR FROM fecha) BETWEEN $1 AND $2
          AND magnitud >= $3
        GROUP BY EXTRACT(YEAR FROM fecha)
        ORDER BY anio
    """, year_start, year_end, mag_min)
    return [dict(r) for r in rows]


@app.get("/api/v1/sismos/heatmap",
         summary="Grid de densidad sísmica para mapas de calor",
         response_class=Response)
async def get_heatmap_sismos(
    resolucion: float = Query(0.1, ge=0.05, le=1.0,
                              description="Tamaño de celda en grados (0.05–1.0)"),
    mag_min: float = Query(3.0, ge=0, le=10),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(ST_Centroid(ST_SnapToGrid(geom, $1)), 6)::TEXT AS geom_json,
            COUNT(*) AS cantidad,
            ROUND(AVG(magnitud)::NUMERIC, 2) AS magnitud_prom,
            ROUND(MAX(magnitud)::NUMERIC, 1) AS magnitud_max
        FROM sismos
        WHERE magnitud >= $2
        GROUP BY ST_SnapToGrid(geom, $1)
        HAVING COUNT(*) > 0
        ORDER BY cantidad DESC
    """, resolucion, mag_min)

    features = []
    for row in rows:
        if row["geom_json"]:
            features.append({
                "type": "Feature",
                "geometry": json.loads(row["geom_json"]),
                "properties": {
                    "cantidad": row["cantidad"],
                    "magnitud_prom": float(row["magnitud_prom"]),
                    "magnitud_max": float(row["magnitud_max"]),
                },
            })
    return geojson_response(features, {"resolucion_grados": resolucion})


@app.get("/api/v1/sismos/cercanos",
         summary="Sismos cercanos a un punto (búsqueda espacial)")
async def get_sismos_cercanos(
    lon:      float = Query(..., ge=-82, le=-68, description="Longitud WGS84"),
    lat:      float = Query(..., ge=-18.5, le=0, description="Latitud WGS84"),
    radio_km: int   = Query(50,  ge=1,  le=500),
    mag_min:  float = Query(3.0, ge=0,  le=10),
    limit:    int   = Query(100, ge=1,  le=1000),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, lugar, region,
            ROUND(
                (ST_Distance(geom::GEOGRAPHY,
                 ST_SetSRID(ST_MakePoint($1, $2), 4326)::GEOGRAPHY) / 1000)::NUMERIC, 1
            ) AS distancia_km
        FROM sismos
        WHERE magnitud >= $3
          AND ST_DWithin(geom::GEOGRAPHY,
                         ST_SetSRID(ST_MakePoint($1, $2), 4326)::GEOGRAPHY,
                         $4 * 1000)
        ORDER BY distancia_km ASC
        LIMIT $5
    """, lon, lat, mag_min, radio_km, limit)
    return [dict(r) for r in rows]


@app.get("/api/v1/sismos/{usgs_id}",
         summary="Detalle de un sismo por ID USGS")
async def get_sismo_detalle(usgs_id: str):
    pool = await db()
    row = await pool.fetchrow("""
        SELECT
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::TEXT AS fecha, hora_utc::TEXT AS hora_utc,
            lugar, region, tipo_magnitud, estado, fuente,
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM sismos
        WHERE usgs_id = $1
    """, usgs_id)
    if not row:
        raise HTTPException(404, f"Sismo {usgs_id} no encontrado")
    d = dict(row)
    d["geom"] = json.loads(d.pop("geom_json"))
    return d


# ══════════════════════════════════════════════════════════
#  DISTRITOS  /api/v1/distritos
# ══════════════════════════════════════════════════════════

DIST_PROPS = ["id", "ubigeo", "nombre", "provincia", "departamento",
              "nivel_riesgo", "poblacion", "area_km2", "fuente"]


@app.get("/api/v1/distritos",
         summary="Polígonos de distritos con nivel de riesgo",
         response_class=Response)
async def get_distritos(
    provincia:    Optional[str] = Query(None),
    departamento: Optional[str] = Query(None),
    riesgo_min:   int = Query(1, ge=1, le=5),
    simplify:     float = Query(0.001, ge=0, le=0.1,
                                description="Tolerancia de simplificación en grados (0=sin simplificar)"),
):
    pool = await db()
    _tol = str(simplify) if simplify > 0 else "0"
    _geom = f"ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, {_tol}), 6)::TEXT" if simplify > 0 else "ST_AsGeoJSON(geom, 6)::TEXT"
    rows = await pool.fetch(f"""
        SELECT
            {_geom} AS geom_json,
            id, ubigeo, nombre, provincia, departamento,
            nivel_riesgo, poblacion, area_km2, fuente
        FROM distritos
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT IS NULL OR LOWER(provincia)    ILIKE '%' || LOWER($2) || '%')
          AND ($3::TEXT IS NULL OR LOWER(departamento) ILIKE '%' || LOWER($3) || '%')
        ORDER BY nivel_riesgo DESC, nombre
        LIMIT 500
    """, riesgo_min, provincia, departamento)
    return geojson_response(rows_to_features(rows, DIST_PROPS))


@app.get("/api/v1/distritos/resumen",
         summary="Estadísticas sísmicas por distrito (join espacial PostGIS)")
async def get_distritos_resumen():
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            d.nombre, d.provincia, d.departamento, d.nivel_riesgo,
            COUNT(s.id)                                  AS total_sismos,
            ROUND(MAX(s.magnitud)::NUMERIC, 1)           AS max_magnitud,
            ROUND(AVG(s.magnitud)::NUMERIC, 2)           AS avg_magnitud,
            COUNT(s.id) FILTER (WHERE s.magnitud >= 5.0) AS m5_plus
        FROM distritos d
        LEFT JOIN sismos s ON ST_Within(s.geom, d.geom)
        GROUP BY d.nombre, d.provincia, d.departamento, d.nivel_riesgo
        ORDER BY total_sismos DESC
        LIMIT 100
    """)
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════
#  FALLAS GEOLÓGICAS  /api/v1/fallas
# ══════════════════════════════════════════════════════════

FALLAS_PROPS = ["id", "ingemmet_id", "nombre", "nombre_alt", "activa", "tipo",
                "mecanismo", "longitud_km", "magnitud_max", "region", "fuente", "referencia"]


@app.get("/api/v1/fallas",
         summary="Fallas geológicas — cobertura nacional",
         response_class=Response)
async def get_fallas(
    activas_only: bool           = Query(False),
    tipo:         Optional[str]  = Query(None, description="neotectonica|subduccion|inferida|normal|inversa"),
    mecanismo:    Optional[str]  = Query(None, description="compresivo|extensional|transcurrente|inverso"),
    region:       Optional[str]  = Query(None),
    mag_min:      Optional[float]= Query(None, ge=0, le=10,
                                         description="Magnitud máxima histórica mínima"),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, ingemmet_id, nombre, nombre_alt, activa, tipo,
            mecanismo, longitud_km, magnitud_max, region, fuente, referencia
        FROM fallas
        WHERE ($1 = FALSE OR activa = TRUE)
          AND ($2::TEXT IS NULL OR tipo    ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR mecanismo ILIKE '%' || $3 || '%')
          AND ($4::TEXT IS NULL OR region  ILIKE '%' || $4 || '%')
          AND ($5::FLOAT IS NULL OR magnitud_max >= $5)
        ORDER BY activa DESC, longitud_km DESC NULLS LAST, nombre
    """, activas_only, tipo, mecanismo, region, mag_min)
    return geojson_response(rows_to_features(rows, FALLAS_PROPS))


# ══════════════════════════════════════════════════════════
#  INUNDACIONES  /api/v1/inundaciones
# ══════════════════════════════════════════════════════════

INUND_PROPS = ["id", "nombre", "nivel_riesgo", "tipo_inundacion",
               "periodo_retorno", "profundidad_max_m", "cuenca", "region", "fuente"]


@app.get("/api/v1/inundaciones",
         summary="Zonas de inundación (fluvial, costero, tsunami)",
         response_class=Response)
async def get_inundaciones(
    riesgo_min:  int           = Query(1, ge=1, le=5),
    tipo:        Optional[str] = Query(None, description="fluvial|costero|pluvial|tsunami|aluvion"),
    region:      Optional[str] = Query(None),
    cuenca:      Optional[str] = Query(None),
    periodo_max: Optional[int] = Query(None, ge=1, description="Período de retorno máximo en años"),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom, 0.001), 6)::TEXT AS geom_json,
            id, nombre, nivel_riesgo, tipo_inundacion,
            periodo_retorno, profundidad_max_m, cuenca, region, fuente
        FROM zonas_inundables
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT IS NULL OR tipo_inundacion ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR region ILIKE '%' || $3 || '%')
          AND ($4::TEXT IS NULL OR cuenca ILIKE '%' || $4 || '%')
          AND ($5::INT  IS NULL OR periodo_retorno <= $5)
        ORDER BY nivel_riesgo DESC, periodo_retorno ASC NULLS LAST
    """, riesgo_min, tipo, region, cuenca, periodo_max)
    return geojson_response(rows_to_features(rows, INUND_PROPS))


# ══════════════════════════════════════════════════════════
#  TSUNAMIS  /api/v1/tsunamis
# ══════════════════════════════════════════════════════════

TSUN_PROPS = ["id", "nombre", "nivel_riesgo", "altura_ola_m",
              "tiempo_arribo_min", "periodo_retorno", "region", "fuente"]


@app.get("/api/v1/tsunamis",
         summary="Zonas de inundación por tsunami",
         response_class=Response)
async def get_tsunamis(
    riesgo_min: int           = Query(1, ge=1, le=5),
    region:     Optional[str] = Query(None),
    altura_min: Optional[float] = Query(None, ge=0, description="Altura ola mínima en metros"),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, nombre, nivel_riesgo, altura_ola_m,
            tiempo_arribo_min, periodo_retorno, region, fuente
        FROM zonas_tsunami
        WHERE nivel_riesgo >= $1
          AND ($2::TEXT  IS NULL OR region      ILIKE '%' || $2 || '%')
          AND ($3::FLOAT IS NULL OR altura_ola_m >= $3)
        ORDER BY nivel_riesgo DESC, altura_ola_m DESC NULLS LAST
    """, riesgo_min, region, altura_min)
    return geojson_response(rows_to_features(rows, TSUN_PROPS))


# ══════════════════════════════════════════════════════════
#  INFRAESTRUCTURA CRÍTICA  /api/v1/infraestructura
# ══════════════════════════════════════════════════════════

INFRA_PROPS = ["id", "osm_id", "nombre", "tipo", "criticidad",
               "estado", "region", "distrito", "fuente"]


@app.get("/api/v1/infraestructura",
         summary="Infraestructura crítica (hospitales, escuelas, puertos…)",
         response_class=Response)
async def get_infraestructura(
    tipo:            Optional[str] = Query(None,
                                           description="hospital|clinica|escuela|aeropuerto|"
                                                       "puerto|bomberos|policia|puente|"
                                                       "central_electrica|planta_agua"),
    criticidad_min:  int           = Query(1, ge=1, le=5),
    region:          Optional[str] = Query(None),
    radio_km:        Optional[int] = Query(None, ge=1, le=500,
                                           description="Radio en km desde lon/lat"),
    lon:             Optional[float] = Query(None, ge=-82, le=-68),
    lat:             Optional[float] = Query(None, ge=-18.5, le=0),
    limit:           int           = Query(500, ge=1, le=2000),
):
    pool = await db()
    # Filtro espacial opcional
    spatial_clause = ""
    if radio_km and lon is not None and lat is not None:
        spatial_clause = f"""
          AND ST_DWithin(geom::GEOGRAPHY,
              ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::GEOGRAPHY,
              {radio_km * 1000})
        """
    rows = await pool.fetch(f"""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, osm_id, nombre, tipo, criticidad,
            estado, region, distrito, fuente
        FROM infraestructura
        WHERE criticidad >= $1
          AND ($2::TEXT IS NULL OR tipo   ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR region ILIKE '%' || $3 || '%')
          {spatial_clause}
        ORDER BY criticidad DESC, nombre
        LIMIT $4
    """, criticidad_min, tipo, region, limit)
    return geojson_response(rows_to_features(rows, INFRA_PROPS))


# ══════════════════════════════════════════════════════════
#  ESTACIONES DE MONITOREO  /api/v1/estaciones
# ══════════════════════════════════════════════════════════

EST_PROPS = ["id", "codigo", "nombre", "tipo", "altitud_m",
             "activa", "institucion", "region", "red"]


@app.get("/api/v1/estaciones",
         summary="Estaciones sísmicas, meteorológicas e hidrométricas",
         response_class=Response)
async def get_estaciones(
    tipo:        Optional[str] = Query(None,
                                       description="sismica|meteorologica|hidrometrica|mareografica"),
    institucion: Optional[str] = Query(None, description="IGP|SENAMHI|ANA"),
    region:      Optional[str] = Query(None),
    activas:     bool          = Query(True),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, codigo, nombre, tipo, altitud_m,
            activa, institucion, region, red
        FROM estaciones
        WHERE ($1 = FALSE OR activa = TRUE)
          AND ($2::TEXT IS NULL OR tipo        ILIKE '%' || $2 || '%')
          AND ($3::TEXT IS NULL OR institucion ILIKE '%' || $3 || '%')
          AND ($4::TEXT IS NULL OR region      ILIKE '%' || $4 || '%')
        ORDER BY institucion, tipo, nombre
    """, activas, tipo, institucion, region)
    return geojson_response(rows_to_features(rows, EST_PROPS))


# ══════════════════════════════════════════════════════════
#  BÚSQUEDA ESPACIAL  /api/v1/bbox
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/bbox",
         summary="Consulta todas las capas dentro de un bounding box (para mapas)")
async def get_por_bbox(
    min_lon: float = Query(..., ge=-82, le=-68),
    min_lat: float = Query(..., ge=-18.5, le=0),
    max_lon: float = Query(..., ge=-82, le=-68),
    max_lat: float = Query(..., ge=-18.5, le=0),
    capas:   str   = Query("sismos,fallas,inundaciones",
                            description="Capas separadas por coma"),
    mag_min: float = Query(3.0, ge=0, le=10),
):
    pool = await db()
    bbox_wkt = (f"ST_MakeEnvelope({min_lon},{min_lat},{max_lon},{max_lat},4326)")
    resultado = {}
    capas_list = [c.strip() for c in capas.split(",")]

    if "sismos" in capas_list:
        rows = await pool.fetch(f"""
            SELECT ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
                   usgs_id, magnitud, profundidad_km, tipo_profundidad,
                   fecha::TEXT AS fecha, lugar
            FROM sismos
            WHERE magnitud >= $1
              AND geom && {bbox_wkt}
            ORDER BY magnitud DESC LIMIT 2000
        """, mag_min)
        resultado["sismos"] = {
            "type": "FeatureCollection",
            "features": rows_to_features(
                rows, ["usgs_id", "magnitud", "profundidad_km",
                       "tipo_profundidad", "fecha", "lugar"]),
        }

    if "fallas" in capas_list:
        rows = await pool.fetch(f"""
            SELECT ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
                   nombre, activa, tipo, longitud_km
            FROM fallas
            WHERE geom && {bbox_wkt}
        """)
        resultado["fallas"] = {
            "type": "FeatureCollection",
            "features": rows_to_features(rows, ["nombre", "activa", "tipo", "longitud_km"]),
        }

    if "inundaciones" in capas_list:
        rows = await pool.fetch(f"""
            SELECT ST_AsGeoJSON(ST_SimplifyPreserveTopology(geom,0.001), 6)::TEXT AS geom_json,
                   nombre, nivel_riesgo, tipo_inundacion
            FROM zonas_inundables
            WHERE geom && {bbox_wkt}
        """)
        resultado["inundaciones"] = {
            "type": "FeatureCollection",
            "features": rows_to_features(rows, ["nombre", "nivel_riesgo", "tipo_inundacion"]),
        }

    return resultado


# ══════════════════════════════════════════════════════════
#  RESUMEN  /api/v1/resumen
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/resumen",
         summary="Panel de control — resumen general de datos")
async def get_resumen():
    pool = await db()

    # Stats sísmicas generales
    stats = await pool.fetchrow("""
        SELECT
            COUNT(*)                                         AS total_sismos,
            ROUND(MAX(magnitud)::NUMERIC, 1)                AS max_magnitud,
            ROUND(AVG(magnitud)::NUMERIC, 2)                AS avg_magnitud,
            COUNT(*) FILTER (WHERE magnitud >= 7.0)         AS m7_plus,
            COUNT(*) FILTER (WHERE fecha >= CURRENT_DATE - INTERVAL '30 days') AS ultimos_30d,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial') AS superficiales,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'intermedio')  AS intermedios,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'profundo')    AS profundos,
            MIN(fecha)::TEXT AS desde,
            MAX(fecha)::TEXT AS hasta
        FROM sismos
    """)

    # Últimos 5 sismos significativos
    ultimos = await pool.fetch("""
        SELECT usgs_id, magnitud, fecha::TEXT, lugar, region,
               profundidad_km, tipo_profundidad
        FROM sismos
        WHERE magnitud >= 4.0
        ORDER BY fecha DESC, magnitud DESC
        LIMIT 5
    """)

    # Fallas por tipo
    fallas_resumen = await pool.fetch("""
        SELECT tipo, COUNT(*) AS cantidad, BOOL_OR(activa) AS hay_activas
        FROM fallas
        GROUP BY tipo ORDER BY cantidad DESC
    """)

    return {
        "sismos": dict(stats),
        "ultimos_significativos": [dict(r) for r in ultimos],
        "fallas": {
            "total": sum(r["cantidad"] for r in fallas_resumen),
            "por_tipo": [dict(r) for r in fallas_resumen],
        },
    }


# ══════════════════════════════════════════════════════════
#  ADMIN  /api/v1/sync
# ══════════════════════════════════════════════════════════

@app.get("/api/v1/sync/log",
         summary="Historial de sincronizaciones ETL")
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