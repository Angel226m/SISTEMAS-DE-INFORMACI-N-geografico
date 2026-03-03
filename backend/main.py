# ══════════════════════════════════════════════════════════
# GeoRiesgo Ica — Backend FastAPI v3.1
# Lee directamente de PostgreSQL/PostGIS (sin archivos GeoJSON)
# ══════════════════════════════════════════════════════════

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ── Conexión asyncpg ──────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://georiesgo:georiesgo_secret@db:5432/georiesgo",
).replace("postgresql+asyncpg://", "postgresql://").replace("postgresql://", "postgresql://")

# asyncpg quiere el esquema postgresql://, no postgresql+asyncpg://
DB_DSN = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql://georiesgo:georiesgo_secret@db:5432/georiesgo",
)

_pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    _pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
    yield
    if _pool:
        await _pool.close()


app = FastAPI(
    title="GeoRiesgo Ica API",
    description="Datos sísmicos y de riesgo geológico — Región Ica, Perú",
    version="3.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


async def pool() -> asyncpg.Pool:
    if _pool is None:
        raise HTTPException(status_code=503, detail="Base de datos no disponible")
    return _pool


# ── Utilidad: filas → GeoJSON FeatureCollection ───────────
def rows_to_geojson(rows: list[asyncpg.Record], props_keys: list[str]) -> dict:
    features = []
    for row in rows:
        geom = row["geom_json"]
        if geom is None:
            continue
        import json
        props = {k: row[k] for k in props_keys if k in row.keys()}
        features.append({
            "type": "Feature",
            "geometry": json.loads(geom),
            "properties": props,
        })
    return {"type": "FeatureCollection", "features": features, "metadata": {"total": len(features)}}


# ─────────────────────────────────────────────────────────
#  Endpoints
# ─────────────────────────────────────────────────────────

@app.get("/")
async def root():
    db = await pool()
    counts = await db.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM sismos)      AS sismos,
            (SELECT COUNT(*) FROM distritos)   AS distritos,
            (SELECT COUNT(*) FROM fallas)      AS fallas,
            (SELECT COUNT(*) FROM zonas_inundables) AS inundables,
            (SELECT COUNT(*) FROM infraestructura)  AS infraestructura
    """)
    return {
        "app":     "GeoRiesgo Ica API v3.1",
        "docs":    "/docs",
        "datos":   dict(counts),
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


# ── Sismos ────────────────────────────────────────────────
@app.get("/api/sismos")
async def get_sismos(
    mag_min:    float = Query(3.0,  ge=0,    le=10),
    mag_max:    float = Query(9.9,  ge=0,    le=10),
    year_start: int   = Query(1960, ge=1900, le=2100),
    year_end:   int   = Query(2030, ge=1900, le=2100),
    prof_tipo:  Optional[str] = Query(None, description="superficial|intermedio|profundo"),
    limit:      int   = Query(5000, ge=1, le=20000),
):
    db = await pool()
    sql = """
        SELECT
            ST_AsGeoJSON(geom)::text  AS geom_json,
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::text AS fecha, lugar, tipo_magnitud, estado
        FROM sismos
        WHERE magnitud      BETWEEN $1 AND $2
          AND EXTRACT(YEAR FROM fecha) BETWEEN $3 AND $4
          AND ($5::text IS NULL OR tipo_profundidad = $5)
        ORDER BY fecha DESC
        LIMIT $6
    """
    rows = await db.fetch(sql, mag_min, mag_max, year_start, year_end, prof_tipo, limit)
    props = ["usgs_id", "magnitud", "profundidad_km", "tipo_profundidad",
             "fecha", "lugar", "tipo_magnitud", "estado"]
    result = rows_to_geojson(rows, props)
    result["metadata"].update({
        "filtros": {
            "mag_min": mag_min, "mag_max": mag_max,
            "year_start": year_start, "year_end": year_end,
            "prof_tipo": prof_tipo,
        }
    })
    return result


# ── Distritos ─────────────────────────────────────────────
@app.get("/api/distritos")
async def get_distritos(
    provincia: Optional[str] = Query(None),
    riesgo_min: int = Query(1, ge=1, le=5),
):
    db = await pool()
    sql = """
        SELECT
            ST_AsGeoJSON(geom)::text AS geom_json,
            id, ubigeo, nombre, provincia, departamento,
            nivel_riesgo, poblacion, area_km2, fuente
        FROM distritos
        WHERE nivel_riesgo >= $1
          AND ($2::text IS NULL OR LOWER(provincia) = LOWER($2))
        ORDER BY nivel_riesgo DESC, nombre
    """
    rows = await db.fetch(sql, riesgo_min, provincia)
    props = ["id", "ubigeo", "nombre", "provincia", "departamento",
             "nivel_riesgo", "poblacion", "area_km2", "fuente"]
    return rows_to_geojson(rows, props)


# ── Fallas geológicas ─────────────────────────────────────
@app.get("/api/fallas")
async def get_fallas(
    activas_only: bool = Query(False),
    tipo: Optional[str] = Query(None),
):
    db = await pool()
    sql = """
        SELECT
            ST_AsGeoJSON(geom)::text AS geom_json,
            id, ingemmet_id, nombre, activa, tipo, longitud_km, fuente
        FROM fallas
        WHERE ($1 = FALSE OR activa = TRUE)
          AND ($2::text IS NULL OR LOWER(tipo) ILIKE '%' || LOWER($2) || '%')
        ORDER BY nombre
    """
    rows = await db.fetch(sql, activas_only, tipo)
    props = ["id", "ingemmet_id", "nombre", "activa", "tipo", "longitud_km", "fuente"]
    return rows_to_geojson(rows, props)


# ── Zonas inundables ──────────────────────────────────────
@app.get("/api/inundaciones")
async def get_inundaciones(
    riesgo_min: int = Query(1, ge=1, le=5),
):
    db = await pool()
    sql = """
        SELECT
            ST_AsGeoJSON(geom)::text AS geom_json,
            id, nombre, nivel_riesgo, periodo_retorno, fuente
        FROM zonas_inundables
        WHERE nivel_riesgo >= $1
        ORDER BY nivel_riesgo DESC
    """
    rows = await db.fetch(sql, riesgo_min)
    props = ["id", "nombre", "nivel_riesgo", "periodo_retorno", "fuente"]
    return rows_to_geojson(rows, props)


# ── Infraestructura crítica ───────────────────────────────
@app.get("/api/infraestructura")
async def get_infraestructura(
    tipo: Optional[str] = Query(None),
    criticidad_min: int = Query(1, ge=1, le=5),
):
    db = await pool()
    sql = """
        SELECT
            ST_AsGeoJSON(geom)::text AS geom_json,
            id, nombre, tipo, criticidad, fuente
        FROM infraestructura
        WHERE criticidad >= $1
          AND ($2::text IS NULL OR LOWER(tipo) = LOWER($2))
        ORDER BY criticidad DESC, nombre
    """
    rows = await db.fetch(sql, criticidad_min, tipo)
    props = ["id", "nombre", "tipo", "criticidad", "fuente"]
    return rows_to_geojson(rows, props)


# ── Estadísticas por año ──────────────────────────────────
@app.get("/api/estadisticas")
async def get_estadisticas(
    year_start: int = Query(1900, ge=1900, le=2100),
    year_end:   int = Query(2030, ge=1900, le=2100),
    mag_min:    float = Query(0.0, ge=0, le=10),
):
    db = await pool()
    rows = await db.fetch("""
        SELECT
            EXTRACT(YEAR FROM fecha)::INTEGER       AS year,
            COUNT(*)                                 AS cantidad,
            ROUND(MAX(magnitud)::NUMERIC, 1)         AS magnitud_max,
            ROUND(AVG(magnitud)::NUMERIC, 2)         AS magnitud_promedio,
            ROUND(MIN(magnitud)::NUMERIC, 1)         AS magnitud_min,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial')  AS superficiales,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'intermedio')   AS intermedios,
            COUNT(*) FILTER (WHERE tipo_profundidad = 'profundo')     AS profundos
        FROM sismos
        WHERE EXTRACT(YEAR FROM fecha) BETWEEN $1 AND $2
          AND magnitud >= $3
        GROUP BY EXTRACT(YEAR FROM fecha)
        ORDER BY year
    """, year_start, year_end, mag_min)
    return [dict(r) for r in rows]


# ── Resumen por distrito (join espacial PostGIS) ──────────
@app.get("/api/distritos/resumen")
async def get_distritos_resumen():
    db = await pool()
    rows = await db.fetch("""
        SELECT
            d.nombre                                    AS distrito,
            d.provincia,
            d.nivel_riesgo,
            COUNT(s.id)                                 AS total_sismos,
            ROUND(MAX(s.magnitud)::NUMERIC, 1)          AS max_magnitud,
            ROUND(AVG(s.magnitud)::NUMERIC, 2)          AS avg_magnitud
        FROM distritos d
        LEFT JOIN sismos s ON ST_Within(s.geom, d.geom)
        GROUP BY d.nombre, d.provincia, d.nivel_riesgo
        ORDER BY total_sismos DESC
        LIMIT 50
    """)
    return [dict(r) for r in rows]


# ── Sismos recientes (últimos N días) ─────────────────────
@app.get("/api/sismos/recientes")
async def get_sismos_recientes(
    dias: int = Query(30, ge=1, le=365),
    mag_min: float = Query(2.5, ge=0, le=10),
):
    db = await pool()
    rows = await db.fetch("""
        SELECT
            ST_AsGeoJSON(geom)::text  AS geom_json,
            usgs_id, magnitud, profundidad_km, tipo_profundidad,
            fecha::text AS fecha, lugar, tipo_magnitud
        FROM sismos
        WHERE fecha >= CURRENT_DATE - ($1 * INTERVAL '1 day')
          AND magnitud >= $2
        ORDER BY fecha DESC, magnitud DESC
        LIMIT 500
    """, dias, mag_min)
    props = ["usgs_id", "magnitud", "profundidad_km", "tipo_profundidad", "fecha", "lugar", "tipo_magnitud"]
    return rows_to_geojson(rows, props)