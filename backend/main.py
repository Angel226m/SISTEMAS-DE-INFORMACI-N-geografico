#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════
# GeoRiesgo Perú — API FastAPI v9.0  ENTERPRISE
#
# NUEVOS ENDPOINTS v9.0:
#   🆕 GET  /api/v1/volcanes                   — 20 volcanes INGEMMET/OVI-IGP 2021
#   🆕 GET  /api/v1/susceptibilidad/{amenaza}  — score ML punto + SHAP + IC
#   🆕 GET  /api/v1/susceptibilidad/{amenaza}/mapa — grilla 0.05° departamento
#   🆕 GET  /api/v1/susceptibilidad/modelo/info — metadata XGBoost + métricas
#   🆕 POST /api/v1/susceptibilidad/modelo/entrenar — train async background
#   🆕 GET  /api/v1/alertas/stream             — SSE EWS multi-hazard
#   🆕 GET  /ws/sismos                         — WebSocket EWS
#   🆕 GET  /api/v1/alertas/recientes          — alertas EWS últimas N horas
#   🆕 GET  /api/v1/exposicion/{ubigeo}        — GEM 2023 + INEI 2017 + MIDIS 2022
#   🆕 GET  /api/v1/sismos/tendencia           — TimescaleDB CAG + fallback
#   🆕 GET  /api/v1/estaciones/{codigo}/lecturas — series temporales
#   🆕 GET  /api/v1/riesgo/escenario           — 4DS + Youngs 1997 + GEM
#   🆕 GET  /api/v1/sendai/report              — Sendai Framework 7 targets
#   🆕 GET  /api/v1/sendai/mapa               — GeoJSON distritos por target
#   🆕 GET  /api/v1/raster/precipitacion       — window read COG MinIO
#   🆕 GET  /api/v1/raster/catalogo            — metadata STAC
#
# ACTUALIZADOS v9.0:
#   ✅ /api/v1/riesgo/construccion/ranking — +IRC v9, IVS, cascada, volcán
#   ✅ /health — incluye EWS stats + Redis + modelos ML
#
# MANTENIDOS v8.0 (100% backward compatible):
#   GET /api/v1/precipitaciones · /precipitaciones/cercanas
#   GET /api/v1/fen · /fen/estadisticas
#   GET /api/v1/riesgo/lluvia
#   GET /api/v1/zonas-sismicas · /zonas-sismicas/referencia
#   GET /api/v1/infraestructura/cobertura
#   GET /api/v1/riesgo/construccion/mapa
#   Todos los endpoints de sismos, distritos, fallas, inundaciones,
#   tsunamis, deslizamientos, infraestructura, estaciones, bbox, resumen
#
# Fuentes: USGS·IGP·INGEMMET·INEI·GADM·ANA·CENEPRED·PREDES·INDECI
#          SENAMHI·CHIRPS·NOAA-CPC·GEM 2023·MIDIS 2022·CAPECO 2023
# ══════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
import orjson
from fastapi import (
    BackgroundTasks, FastAPI, HTTPException,
    Query, Request, Response, WebSocket, WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from alert_worker import EWSWorker
from cache import GeoCache, geo_cache, CACHE_VOLCANES, CACHE_IRC_MAPA
from cache import (
    CACHE_SISMOS_RECIENTES, CACHE_SISMOS_HISTORICOS,
    CACHE_SUSCEPTIBILIDAD, CACHE_ESCENARIO, CACHE_TENDENCIA,
    CACHE_SENDAI_REPORT, CACHE_EXPOSICION, CACHE_FEN,
    CACHE_RIESGO_RANKING, CACHE_RIESGO_PUNTO,
)
from damage_model import scenario_losses
from ml_engine import SusceptibilityModel, susceptibility_model, AMENAZAS_VALIDAS

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════

DB_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://georiesgo:georiesgo_secret@db:5432/georiesgo",
).replace("postgresql+asyncpg://", "postgresql://")

# Rate limiter — 200/min global, 20/min rutas pesadas, 1/hour entrenar
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["200/minute"],
    # Docker interna sin límite
    headers_enabled=True,
)

# Whitelist para redes Docker internas (sin rate limit efectivo)
_WHITELIST_NETS = ["172.28.0.0/16", "172.17.0.0/16", "127.0.0.1"]


# ══════════════════════════════════════════════════════════════════
#  LIFESPAN — pool + cache + EWS worker
# ══════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Pool asyncpg ──────────────────────────────────────────────
    # Little's Law: ~50 req/s × 0.2s p95 = 10 conns mínimo
    # Fuente: FastAPI benchmarks + asyncpg best practices 2025
    pool: asyncpg.Pool = await asyncpg.create_pool(
        DB_DSN,
        min_size=5,
        max_size=20,
        max_inactive_connection_lifetime=300.0,
        command_timeout=30.0,
        statement_cache_size=200,
        server_settings={"application_name": "georiesgo_api_v9"},
    )
    app.state.pool = pool

    # ── Prepared statements (5 queries más frecuentes) ────────────
    # Reduce overhead de parse/plan en PostgreSQL por conexión
    try:
        async with pool.acquire() as conn:
            app.state.stmt_sismos_recientes = await conn.prepare(
                """SELECT usgs_id, magnitud, profundidad_km, tipo_profundidad,
                          fecha::TEXT, lugar,
                          COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region
                   FROM sismos
                   WHERE fecha >= $1 AND magnitud >= $2
                   ORDER BY fecha DESC, magnitud DESC LIMIT $3"""
            )
            app.state.stmt_distrito_ubigeo = await conn.prepare(
                """SELECT id, ubigeo, nombre, provincia, departamento,
                          nivel_riesgo, zona_sismica, indice_riesgo_v9,
                          factor_cascada
                   FROM distritos WHERE ubigeo = $1"""
            )
            app.state.stmt_infra_radio = await conn.prepare(
                """SELECT nombre, tipo, criticidad,
                          ROUND((ST_Distance(geom::geography,
                              ST_SetSRID(ST_MakePoint($1,$2),4326)::geography)/1000)::NUMERIC,1)
                              AS distancia_km
                   FROM infraestructura
                   WHERE ST_DWithin(geom::geography,
                       ST_SetSRID(ST_MakePoint($1,$2),4326)::geography, $3*1000)
                   ORDER BY distancia_km LIMIT $4"""
            )
            app.state.stmt_zona_precip = await conn.prepare(
                """SELECT nombre, tipo, region,
                          ROUND(precipitacion_anual_mm::NUMERIC,1) AS precipitacion_anual_mm,
                          ROUND(indice_fen::NUMERIC,2) AS indice_fen,
                          nivel_riesgo_inundacion
                   FROM zonas_precipitacion
                   ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1,$2),4326) LIMIT 1"""
            )
            app.state.stmt_irc_ranking = await conn.prepare(
                """SELECT id, ubigeo, distrito, provincia, departamento,
                          zona_sismica, COALESCE(factor_z,0.25) AS factor_z,
                          COALESCE(poblacion,0) AS poblacion,
                          peligro_sismico, peligro_inundacion, peligro_deslizamiento,
                          peligro_tsunami, fallas_activas_50km, sismos_m4_30a_50km,
                          ROUND(indice_riesgo_construccion::NUMERIC,2) AS indice_riesgo_construccion
                   FROM mv_riesgo_construccion
                   WHERE indice_riesgo_construccion >= $1
                     AND ($2::TEXT IS NULL OR LOWER(departamento) ILIKE '%'||LOWER($2)||'%')
                     AND ($3::INT  IS NULL OR zona_sismica = $3)
                   ORDER BY indice_riesgo_construccion DESC LIMIT $4"""
            )
        logger.info("main: prepared statements registrados (5)")
    except Exception as exc:
        logger.warning("main: prepared statements fallaron (%s) — modo normal", exc)

    # ── Redis cache ───────────────────────────────────────────────
    await geo_cache.connect()
    app.state.cache = geo_cache

    # ── EWS Worker ────────────────────────────────────────────────
    ews = EWSWorker(pool)
    app.state.ews = ews
    await ews.start()
    logger.info("main: EWSWorker iniciado (poll cada 60s)")

    # ── ML models — precargar si existen ─────────────────────────
    app.state.ml_model = susceptibility_model
    for amenaza in AMENAZAS_VALIDAS:
        if susceptibility_model.is_trained(amenaza):
            try:
                susceptibility_model.load_model(amenaza)
                logger.info("main: modelo ML '%s' precargado", amenaza)
            except Exception as exc:
                logger.warning("main: modelo '%s' no cargado (%s)", amenaza, exc)

    yield

    # ── Shutdown ──────────────────────────────────────────────────
    await ews.stop()
    await geo_cache.close()
    await pool.close()
    logger.info("main: shutdown completo")


# ══════════════════════════════════════════════════════════════════
#  APP
# ══════════════════════════════════════════════════════════════════

app = FastAPI(
    title="GeoRiesgo Perú API",
    description="""
## API de Riesgo Geoespacial — Perú v9.0  ENTERPRISE

### 🆕 Nuevos endpoints v9.0
| Endpoint | Descripción |
|----------|-------------|
| `GET /api/v1/volcanes` | 20 volcanes INGEMMET/OVI-IGP 2021 + radios peligro |
| `GET /api/v1/susceptibilidad/{amenaza}` | Score ML punto + SHAP + IC 80% |
| `GET /api/v1/susceptibilidad/{amenaza}/mapa` | Grilla 0.05° por departamento |
| `POST /api/v1/susceptibilidad/modelo/entrenar` | Entrena XGBoost (background) |
| `GET /api/v1/alertas/stream` | SSE — alertas EWS tiempo real |
| `GET /ws/sismos` | WebSocket — alertas EWS + CAP v1.2 |
| `GET /api/v1/alertas/recientes` | Alertas EWS últimas N horas |
| `GET /api/v1/exposicion/{ubigeo}` | Exposición GEM 2023 + IVS |
| `GET /api/v1/riesgo/escenario` | Pérdidas 4DS + Youngs 1997 + GEM |
| `GET /api/v1/sismos/tendencia` | Series temporales CAG TimescaleDB |
| `GET /api/v1/sendai/report` | Sendai Framework 7 targets (proxy) |
| `GET /api/v1/raster/precipitacion` | Window read COG MinIO |

### 📐 IRC v9 — 7 amenazas
`35%S + 20%I + 18%D + 10%T + 8%V + 5%Q + 4%F × factor_cascada`

### Fuentes científicas
| Módulo | Fuente |
|--------|--------|
| Fragilidad adobe | Tarque et al. 2012 PUCP |
| Susceptibilidad ML | Kumar et al. 2023 Remote Sensing |
| Cascada sismo→desl | Gill & Malamud 2014 Rev. Geophys. |
| EWS | INDECI 2020 + EW4All UNDRR 2022 |
| GEM taxonomy | Yepes-Estrada et al. 2023 Earthquake Spectra |
    """,
    version="9.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS", "HEAD"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Cache", "ETag", "X-RateLimit-Limit",
                    "X-RateLimit-Remaining", "Retry-After"],
    max_age=3600,
)
app.add_middleware(GZipMiddleware, minimum_size=512)


# ══════════════════════════════════════════════════════════════════
#  UTILIDADES
# ══════════════════════════════════════════════════════════════════

async def db() -> asyncpg.Pool:
    pool = getattr(app.state, "pool", None)
    if pool is None:
        raise HTTPException(503, detail={
            "error": "database_unavailable",
            "mensaje": "Base de datos no disponible temporalmente",
        })
    return pool


def _simplify_tolerance(zoom: Optional[int]) -> float:
    if zoom is None or zoom >= 13: return 0.0
    if zoom <= 5:  return 0.05
    if zoom <= 9:  return 0.01
    return 0.001


def _geom_expr(zoom: Optional[int], col: str = "geom", decimals: int = 6) -> str:
    tol = _simplify_tolerance(zoom)
    if tol > 0:
        return f"ST_AsGeoJSON(ST_SimplifyPreserveTopology({col},{tol}),{decimals})::TEXT"
    return f"ST_AsGeoJSON({col},{decimals})::TEXT"


def geojson_response(
    features: list,
    metadata: dict | None = None,
    cache_seconds: int = 300,
) -> Response:
    fc = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total": len(features),
            "crs": "EPSG:4326",
            "api": "GeoRiesgo Perú v9.0",
            **(metadata or {}),
        },
    }
    content = orjson.dumps(fc, option=orjson.OPT_NON_STR_KEYS)
    etag = f'"{hashlib.md5(content).hexdigest()[:12]}"'  # noqa: S324
    return Response(
        content=content,
        media_type="application/geo+json",
        headers={
            "Cache-Control": f"public, max-age={cache_seconds}",
            "ETag": etag,
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


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _serialize_row(row) -> dict:
    result = {}
    for k, v in dict(row).items():
        if v is None:
            result[k] = None
        elif hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
            result[k] = float(v)
        else:
            result[k] = v
    return result


async def _cache_get(key: str) -> bytes | None:
    cache: GeoCache = getattr(app.state, "cache", None)
    if cache and cache.available:
        return await cache.get(key)
    return None


async def _cache_set(key: str, value: bytes, ttl: int) -> None:
    cache: GeoCache = getattr(app.state, "cache", None)
    if cache and cache.available:
        await cache.set(key, value, ttl)


def _cache_key(path: str, **kwargs) -> str:
    params = ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()) if v is not None)
    return f"gr:GET:{path}:{params}"


# ══════════════════════════════════════════════════════════════════
#  ROOT / HEALTH
# ══════════════════════════════════════════════════════════════════

@app.get("/", summary="Estado general de la API v9.0", tags=["Sistema"])
async def root():
    pool = await db()
    row = await pool.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM sismos)               AS sismos,
            (SELECT COUNT(*) FROM departamentos)        AS departamentos,
            (SELECT COUNT(*) FROM distritos)            AS distritos,
            (SELECT COUNT(*) FROM fallas)               AS fallas,
            (SELECT COUNT(*) FROM zonas_inundables)     AS inundaciones,
            (SELECT COUNT(*) FROM zonas_tsunami)        AS tsunamis,
            (SELECT COUNT(*) FROM deslizamientos)       AS deslizamientos,
            (SELECT COUNT(*) FROM infraestructura)      AS infraestructura,
            (SELECT COUNT(*) FROM estaciones)           AS estaciones,
            (SELECT COUNT(*) FROM zonas_precipitacion)  AS zonas_precipitacion,
            (SELECT COUNT(*) FROM eventos_fen)          AS eventos_fen,
            (SELECT COUNT(*) FROM volcanes)             AS volcanes,
            (SELECT COUNT(*) FROM alertas_rt)           AS alertas_rt,
            (SELECT COUNT(*) FROM susceptibilidad_ml)   AS susceptibilidad_ml,
            (SELECT COUNT(*) FROM exposicion_distritos) AS exposicion_distritos,
            (SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 IS NOT NULL) AS distritos_con_irc_v9,
            (SELECT MAX(fecha)::TEXT FROM sismos)       AS ultimo_sismo
    """)
    ews_stats = getattr(app.state, "ews", None)
    cache_ok  = geo_cache.available if geo_cache else False
    ml_info   = susceptibility_model.get_model_info()
    return {
        "api": "GeoRiesgo Perú v9.0 ENTERPRISE",
        "docs": "/docs", "redoc": "/redoc",
        "capas": _serialize_row(row),
        "servicios": {
            "redis_cache": "activo" if cache_ok else "no disponible",
            "ews_worker":  ews_stats.stats if ews_stats else "inactivo",
            "ml_modelos": {a: "entrenado" if susceptibility_model.is_trained(a)
                           else "no entrenado" for a in AMENAZAS_VALIDAS},
        },
        "irc_v9": {
            "pesos": "35%S + 20%I + 18%D + 10%T + 8%V + 5%Q + 4%F × cascada",
            "fuente": "CENEPRED 2014 + SENCICO E.030 2018",
        },
        "nuevo_v9": [
            "volcanes — INGEMMET/OVI-IGP 2021 (20 volcanes)",
            "susceptibilidad ML — XGBoost+SMOTE-Tomek+Optuna+SHAP",
            "alertas EWS — SSE + WebSocket + CAP v1.2 (EW4All)",
            "exposicion — GEM 2023 + INEI 2017 + MIDIS SISFOH 2022",
            "damage model — 4 estados DS + Youngs 1997 + GEM",
            "sendai — Marco Sendai 2015-2030 proxy metrics",
            "raster STAC — COG MinIO + STAC 1.0",
        ],
    }


@app.get("/health", summary="Healthcheck Docker/k8s", tags=["Sistema"])
async def health():
    pool = await db()
    await pool.fetchval("SELECT 1")
    ews_stats = getattr(app.state, "ews", None)
    return {
        "status": "ok",
        "ts": time.time(),
        "version": "9.0",
        "db": "ok",
        "redis": "ok" if geo_cache.available else "degraded",
        "ews": ews_stats.stats if ews_stats else {},
    }


# ══════════════════════════════════════════════════════════════════
#  DIAGNÓSTICO DE REGIONES
# ══════════════════════════════════════════════════════════════════

@app.get("/api/v1/diagnostico/regiones", tags=["Sistema"])
async def diagnostico_regiones():
    pool = await db()
    resultado = {}
    for tabla in ["sismos", "infraestructura", "estaciones", "fallas", "volcanes"]:
        try:
            row = await pool.fetchrow(f"""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE region IS NOT NULL) AS con_region,
                       COUNT(*) FILTER (WHERE region IS NULL)     AS sin_region,
                       COUNT(DISTINCT region)                     AS regiones_distintas
                FROM {tabla}
            """)
            resultado[tabla] = _serialize_row(row)
        except Exception:
            resultado[tabla] = {"error": "tabla no disponible"}
    return resultado


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — VOLCANES
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/volcanes",
    summary="Volcanes activos y potencialmente activos — INGEMMET/OVI-IGP 2021",
    tags=["Volcanes"],
    response_class=Response,
)
@limiter.limit("200/minute")
async def get_volcanes(
    request: Request,
    estado: Optional[str] = Query(
        None,
        description="activo_critico | activo | potencialmente_activo | inactivo",
    ),
    region: Optional[str] = Query(None),
):
    """
    Catálogo de volcanes del Perú según INGEMMET Mapa de Peligros Volcánicos 2021.

    Incluye radios de peligro por nivel según estado del volcán:
    - `activo_critico`: nivel 5 <30km, nivel 4 <60km, nivel 3 <100km
    - `activo`:         nivel 4 <30km, nivel 3 <60km, nivel 2 <100km

    **Fuente**: INGEMMET "Mapa de Peligros Volcánicos del Perú" 2da ed. 2021
    """
    ck = _cache_key("/api/v1/volcanes", estado=estado, region=region)
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/geo+json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    rows = await pool.fetch("""
        SELECT
            ST_AsGeoJSON(geom, 6)::TEXT AS geom_json,
            id, nombre, estado, altitud_m, region,
            tipo_erupcion, ultima_erupcion, fuente,
            CASE estado
                WHEN 'activo_critico' THEN
                    '{"nivel5":30,"nivel4":60,"nivel3":100,"nivel2":200}'::jsonb
                WHEN 'activo' THEN
                    '{"nivel4":30,"nivel3":60,"nivel2":100}'::jsonb
                WHEN 'potencialmente_activo' THEN
                    '{"nivel3":30,"nivel2":60}'::jsonb
                ELSE '{}'::jsonb
            END AS radio_peligro_km,
            CASE estado
                WHEN 'activo_critico' THEN '#b71c1c'
                WHEN 'activo'         THEN '#e53935'
                WHEN 'potencialmente_activo' THEN '#fb8c00'
                ELSE '#9e9e9e'
            END AS color
        FROM volcanes
        WHERE ($1::TEXT IS NULL OR estado ILIKE '%' || $1 || '%')
          AND ($2::TEXT IS NULL OR region ILIKE '%' || $2 || '%')
        ORDER BY
            CASE estado
                WHEN 'activo_critico' THEN 1
                WHEN 'activo' THEN 2
                WHEN 'potencialmente_activo' THEN 3
                ELSE 4
            END, nombre
    """, estado, region)

    features = []
    for row in rows:
        feat = row_to_feature(
            row,
            ["id","nombre","estado","altitud_m","region",
             "tipo_erupcion","ultima_erupcion","fuente","color"],
        )
        if feat:
            feat["properties"]["radio_peligro_km"] = (
                json.loads(row["radio_peligro_km"])
                if row["radio_peligro_km"] else {}
            )
            features.append(feat)

    content = orjson.dumps({
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total": len(features),
            "fuente": "INGEMMET/OVI-IGP 2021",
            "metodologia": "Mapa de Peligros Volcánicos del Perú 2da ed.",
            "api": "GeoRiesgo Perú v9.0",
        },
    }, option=orjson.OPT_NON_STR_KEYS)

    await _cache_set(ck, content, CACHE_VOLCANES)
    return Response(content=content, media_type="application/geo+json",
                    headers={"X-Cache": "MISS", "Cache-Control": f"public, max-age={CACHE_VOLCANES}"})


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — IRC v9 RANKING (actualizado)
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/riesgo/construccion/ranking",
    summary="Ranking IRC v8/v9 por distrito (backward compatible)",
    tags=["Riesgo de Construcción"],
)
@limiter.limit("20/minute")
async def get_riesgo_construccion_ranking(
    request: Request,
    limit:        int            = Query(50,  ge=1, le=200),
    departamento: Optional[str]  = Query(None),
    zona_sismica: Optional[int]  = Query(None, ge=1, le=4),
    indice_min:   float          = Query(1.0, ge=1.0, le=5.0),
    order:        str            = Query("v9",
                    description="v9 = IRC v9 | v8 = IRC v8 con FEN"),
    incluir_ivs:  bool           = Query(False),
):
    """
    Ranking de distritos por IRC. Backward compatible con v8.

    **IRC v9** (nuevos campos añadidos):
    - `indice_riesgo_v9` — 7 amenazas × factor_cascada
    - `irc_v9_p10` / `irc_v9_p90` — IC 80% (Li et al. 2023)
    - `peligro_volcan` — 1-5 según distancia a volcán activo
    - `peligro_sequia` — SPI-12 (McKee et al. 1993)
    - `factor_cascada` — amplificación sismo→deslizamiento (Gill & Malamud 2014)
    """
    ck = _cache_key(
        "/api/v1/riesgo/construccion/ranking",
        limit=limit, departamento=departamento,
        zona_sismica=zona_sismica, indice_min=indice_min,
        order=order, incluir_ivs=incluir_ivs,
    )
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    exists = await pool.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_matviews WHERE matviewname='mv_riesgo_construccion')"
    )
    if not exists:
        raise HTTPException(503, detail={
            "error": "vista_no_disponible",
            "mensaje": "Ejecuta: python procesar_datos.py --solo riesgo_construccion",
        })

    order_col = (
        "COALESCE(d.indice_riesgo_v9, mv.indice_riesgo_construccion)"
        if order == "v9" else "mv.indice_riesgo_construccion"
    )

    ivs_cols = ", e.ivs, e.nivel_ivs, e.indice_riesgo_total" if incluir_ivs else ""
    ivs_join = """
        LEFT JOIN exposicion_distritos e ON mv.ubigeo = e.ubigeo
    """ if incluir_ivs else ""

    rows = await pool.fetch(f"""
        SELECT
            mv.id, mv.ubigeo, mv.distrito, mv.provincia, mv.departamento,
            mv.zona_sismica,
            COALESCE(mv.factor_z, 0.25) AS factor_z,
            COALESCE(mv.poblacion, 0)   AS poblacion,
            COALESCE(mv.area_km2, 0)    AS area_km2,
            mv.peligro_sismico, mv.peligro_inundacion,
            mv.peligro_deslizamiento, mv.peligro_tsunami,
            mv.fallas_activas_50km, mv.sismos_m4_30a_50km,
            ROUND(mv.indice_riesgo_construccion::NUMERIC, 2) AS indice_riesgo_construccion,
            CASE
                WHEN mv.indice_riesgo_construccion >= 4.5 THEN 'MUY ALTO'
                WHEN mv.indice_riesgo_construccion >= 3.5 THEN 'ALTO'
                WHEN mv.indice_riesgo_construccion >= 2.5 THEN 'MEDIO'
                WHEN mv.indice_riesgo_construccion >= 1.5 THEN 'BAJO'
                ELSE 'MUY BAJO'
            END AS nivel_riesgo,
            -- v9 nuevos campos
            d.indice_riesgo_v9,
            d.irc_v9_p10,
            d.irc_v9_p90,
            COALESCE(d.peligro_volcan, 1)        AS peligro_volcan,
            COALESCE(d.peligro_sequia, 1)        AS peligro_sequia,
            COALESCE(d.factor_cascada, 1.0)      AS factor_cascada
            {ivs_cols}
        FROM mv_riesgo_construccion mv
        JOIN distritos d ON mv.id = d.id
        {ivs_join}
        WHERE mv.indice_riesgo_construccion >= $1
          AND ($2::TEXT IS NULL OR LOWER(mv.departamento) ILIKE '%'||LOWER($2)||'%')
          AND ($3::INT  IS NULL OR mv.zona_sismica = $3)
        ORDER BY {order_col} DESC NULLS LAST
        LIMIT $4
    """, indice_min, departamento, zona_sismica, limit)

    result = {
        "ranking": [_serialize_row(r) for r in rows],
        "total": len(rows),
        "metodologia_v8": "CENEPRED 2014 + NTE E.030-2018 (40%S+25%I+20%D+10%T+5%F)",
        "metodologia_v9": "CENEPRED 2014 + 7 amenazas (35%S+20%I+18%D+10%T+8%V+5%Q+4%F × cascada)",
        "filtros": {
            "departamento": departamento, "zona_sismica": zona_sismica,
            "indice_min": indice_min, "order": order, "limit": limit,
        },
    }

    content = orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS)
    await _cache_set(ck, content, CACHE_RIESGO_RANKING)
    return Response(content=content, media_type="application/json",
                    headers={"X-Cache": "MISS"})


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — SUSCEPTIBILIDAD ML
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/susceptibilidad/{amenaza}",
    summary="Score de susceptibilidad ML para un punto (XGBoost + SHAP)",
    tags=["Susceptibilidad ML"],
)
async def get_susceptibilidad_punto(
    amenaza: str,
    lon:     float = Query(..., ge=-82,   le=-68),
    lat:     float = Query(..., ge=-18.5, le=0),
):
    """
    Calcula la susceptibilidad a deslizamiento, inundación o sequía
    en un punto usando el modelo XGBoost entrenado.

    Retorna:
    - `score` — probabilidad [0,1]
    - `score_p10` / `score_p90` — IC 80% via bootstrapping 100 iter.
    - `nivel` — MUY_BAJO | BAJO | MEDIO | ALTO | MUY_ALTO
    - `shap_values` — top-5 features explicativas
    - `modelo_info` — algoritmo, AUC-PR, fecha entrenamiento

    **Fuente**: Kumar et al. 2023 Remote Sensing 15(5):1376 (AUC > 0.95)
    """
    if amenaza not in AMENAZAS_VALIDAS:
        raise HTTPException(422, detail={
            "error": "amenaza_invalida",
            "validas": list(AMENAZAS_VALIDAS),
        })

    if not susceptibility_model.is_trained(amenaza):
        raise HTTPException(503, detail={
            "error": "modelo_no_entrenado",
            "instruccion": f"POST /api/v1/susceptibilidad/modelo/entrenar?amenaza={amenaza}",
        })

    ck = _cache_key(
        f"/api/v1/susceptibilidad/{amenaza}",
        lon=round(lon, 3), lat=round(lat, 3),
    )
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    try:
        async with pool.acquire() as conn:
            result = await susceptibility_model.predict_point(lon, lat, amenaza, conn)
    except FileNotFoundError:
        raise HTTPException(503, detail={
            "error": "modelo_no_entrenado",
            "instruccion": f"POST /api/v1/susceptibilidad/modelo/entrenar?amenaza={amenaza}",
        })
    except Exception as exc:
        logger.error("susceptibilidad punto: %s", exc)
        raise HTTPException(500, detail={"error": str(exc)})

    content = orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS)
    await _cache_set(ck, content, CACHE_SUSCEPTIBILIDAD)
    return Response(content=content, media_type="application/json",
                    headers={"X-Cache": "MISS"})


@app.get(
    "/api/v1/susceptibilidad/{amenaza}/mapa",
    summary="Grilla de susceptibilidad 0.05° por departamento",
    tags=["Susceptibilidad ML"],
    response_class=Response,
)
@limiter.limit("20/minute")
async def get_susceptibilidad_mapa(
    request: Request,
    amenaza:      str,
    departamento: str   = Query(..., description="Nombre del departamento"),
    zoom:         int   = Query(6, ge=4, le=12),
):
    """
    Genera una grilla regular de 0.05° sobre el departamento indicado
    con el score de susceptibilidad. Rate limit: 20/min.
    """
    if amenaza not in AMENAZAS_VALIDAS:
        raise HTTPException(422, detail={"error": "amenaza_invalida",
                                         "validas": list(AMENAZAS_VALIDAS)})

    if not susceptibility_model.is_trained(amenaza):
        raise HTTPException(503, detail={
            "error": "modelo_no_entrenado",
            "instruccion": f"POST /api/v1/susceptibilidad/modelo/entrenar?amenaza={amenaza}",
        })

    ck = _cache_key(f"/api/v1/susceptibilidad/{amenaza}/mapa",
                    departamento=departamento, zoom=zoom)
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/geo+json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    # Obtener bbox del departamento
    bbox_row = await pool.fetchrow("""
        SELECT
            ST_XMin(geom) AS lon_min, ST_YMin(geom) AS lat_min,
            ST_XMax(geom) AS lon_max, ST_YMax(geom) AS lat_max
        FROM departamentos
        WHERE nombre ILIKE '%' || $1 || '%'
        LIMIT 1
    """, departamento)

    if not bbox_row:
        raise HTTPException(404, detail={
            "error": "departamento_no_encontrado",
            "departamento": departamento,
        })

    import numpy as np
    lon_min = float(bbox_row["lon_min"])
    lat_min = float(bbox_row["lat_min"])
    lon_max = float(bbox_row["lon_max"])
    lat_max = float(bbox_row["lat_max"])

    # Grilla 0.05°
    res = 0.05
    lons = np.arange(lon_min, lon_max + res, res)
    lats = np.arange(lat_min, lat_max + res, res)

    features = []
    async with pool.acquire() as conn:
        for lat_p in lats[::max(1, len(lats) // 20)]:
            for lon_p in lons[::max(1, len(lons) // 20)]:
                try:
                    result = await susceptibility_model.predict_point(
                        float(lon_p), float(lat_p), amenaza, conn
                    )
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lon_p), float(lat_p)]},
                        "properties": {
                            "score":    result["score"],
                            "score_p10": result["score_p10"],
                            "score_p90": result["score_p90"],
                            "nivel":    result["nivel"],
                        },
                    })
                except Exception:
                    pass

    content = orjson.dumps({
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "total": len(features), "amenaza": amenaza,
            "departamento": departamento, "resolucion_grados": res,
            "api": "GeoRiesgo Perú v9.0",
        },
    }, option=orjson.OPT_NON_STR_KEYS)

    await _cache_set(ck, content, CACHE_SUSCEPTIBILIDAD)
    return Response(content=content, media_type="application/geo+json",
                    headers={"X-Cache": "MISS",
                             "Cache-Control": f"public, max-age={CACHE_SUSCEPTIBILIDAD}"})


@app.get(
    "/api/v1/susceptibilidad/modelo/info",
    summary="Metadata de modelos ML (métricas, features, hiperparámetros)",
    tags=["Susceptibilidad ML"],
)
async def get_modelo_info(
    amenaza: Optional[str] = Query(None, description="deslizamiento|inundacion|sequia"),
):
    """
    Retorna métricas de entrenamiento y configuración de los modelos.
    - `auc_pr` — métrica primaria (más informativa con desbalance >10:1)
    - `features_usadas` — post-VIF y post-RFE
    - `tecnica_balance` — smote_tomek | smote_enn | scale_pos_weight
    - `hiperparametros` — mejores params Optuna (50 trials)
    """
    pool = await db()
    if amenaza:
        if amenaza not in AMENAZAS_VALIDAS:
            raise HTTPException(422, detail={"error": "amenaza_invalida"})
        row = await pool.fetchrow(
            "SELECT * FROM modelo_metadata WHERE amenaza = $1", amenaza
        )
        if row:
            return _serialize_row(row)
        return susceptibility_model.get_model_info(amenaza)

    rows = await pool.fetch("SELECT * FROM modelo_metadata ORDER BY amenaza")
    bd_info = {r["amenaza"]: _serialize_row(r) for r in rows}
    mem_info = susceptibility_model.get_model_info()
    # Merges BD y memoria
    for am in AMENAZAS_VALIDAS:
        if am not in bd_info:
            bd_info[am] = mem_info.get(am, {"entrenado": False})
    return bd_info


@app.post(
    "/api/v1/susceptibilidad/modelo/entrenar",
    summary="Entrena el modelo ML para una amenaza (background, hasta 120s)",
    tags=["Susceptibilidad ML"],
)
@limiter.limit("1/hour")
async def entrenar_modelo(
    request: Request,
    background_tasks: BackgroundTasks,
    amenaza: str = Query("deslizamiento",
                         description="deslizamiento | inundacion | sequia"),
):
    """
    Inicia el entrenamiento en background.
    Rate limit: **1 por hora por IP** (proceso costoso ~90s).

    Pipeline: VIF → RFE → SMOTE-Tomek → Optuna 50 trials → XGBoost → SHAP
    """
    if amenaza not in AMENAZAS_VALIDAS:
        raise HTTPException(422, detail={"error": "amenaza_invalida",
                                         "validas": list(AMENAZAS_VALIDAS)})

    async def _train_task():
        try:
            pool = app.state.pool
            async with pool.acquire() as conn:
                await susceptibility_model.train(amenaza, conn)
            logger.info("Modelo '%s' entrenado exitosamente", amenaza)
        except Exception as exc:
            logger.error("Error entrenando '%s': %s", amenaza, exc)

    background_tasks.add_task(_train_task)
    return {
        "status": "entrenando",
        "amenaza": amenaza,
        "estimado_segundos": 90,
        "timestamp_inicio": time.time(),
        "nota": "Pipeline: VIF→RFE→SMOTE-Tomek→Optuna 50 trials→XGBoost→SHAP",
        "verificar": f"GET /api/v1/susceptibilidad/modelo/info?amenaza={amenaza}",
    }


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — EWS: SSE + WebSocket + Alertas recientes
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/alertas/stream",
    summary="Stream de alertas sísmicas en tiempo real (SSE)",
    tags=["EWS Alertas"],
)
async def alertas_stream(request: Request):
    """
    Server-Sent Events (SSE) para alertas EWS en tiempo real.

    Eventos:
    - `ping`: heartbeat cada 30s `{"ts":"...","server":"georiesgo-v9"}`
    - `alerta`: nueva alerta `{"nivel":"warning","magnitud":6.2,...,"cascade":{...}}`

    Al conectar recibe backfill de las últimas 3 alertas.
    Fuente: INDECI Protocolo Alertas Sísmicas 2020 + EW4All UNDRR 2022
    """
    ews: EWSWorker = app.state.ews
    queue = ews.register_sse_client()

    # Backfill: últimas 3 alertas al conectar
    pool = await db()
    async with pool.acquire() as conn:
        recientes = await ews.get_recent_alerts(conn, horas=24, limit=3)

    async def event_generator():
        # Enviar ping inicial
        import json as _json
        ts = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat()
        yield f'event: ping\ndata: {{"ts":"{ts}","server":"georiesgo-v9"}}\n\n'

        # Backfill
        for alerta in recientes:
            yield f'event: alerta\ndata: {_json.dumps(alerta, default=str)}\n\n'

        # Stream en tiempo real
        ping_interval = 30
        last_ping = time.time()
        try:
            while True:
                # Ping heartbeat
                if time.time() - last_ping >= ping_interval:
                    ts_now = __import__("datetime").datetime.now(
                        __import__("datetime").timezone.utc
                    ).isoformat()
                    yield f'event: ping\ndata: {{"ts":"{ts_now}","server":"georiesgo-v9"}}\n\n'
                    last_ping = time.time()

                try:
                    msg = queue.get_nowait()
                    yield msg
                except asyncio.QueueEmpty:
                    await asyncio.sleep(1)

                if await request.is_disconnected():
                    break
        finally:
            ews.unregister_sse_client(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream; charset=utf-8",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.websocket("/ws/sismos")
async def ws_sismos(websocket: WebSocket):
    """
    WebSocket para alertas EWS en tiempo real.

    Mensajes enviados por el servidor:
    - `{"type":"ping","ts":"...","server":"georiesgo-v9"}`
    - `{"type":"alerta","data":{...alerta_completa...},"cascade":{...}}`
    - `{"type":"backfill","data":[...ultimas_3...],"total":3}`
    """
    ews: EWSWorker = app.state.ews
    await websocket.accept()
    ews.register_ws_client(websocket)

    try:
        # Backfill
        pool = await db()
        async with pool.acquire() as conn:
            recientes = await ews.get_recent_alerts(conn, horas=24, limit=3)

        await websocket.send_json({
            "type": "backfill",
            "data": recientes,
            "total": len(recientes),
        })

        # Heartbeat
        while True:
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                ts = __import__("datetime").datetime.now(
                    __import__("datetime").timezone.utc
                ).isoformat()
                await websocket.send_json({
                    "type": "ping",
                    "ts": ts,
                    "server": "georiesgo-v9",
                })
            except WebSocketDisconnect:
                break
    finally:
        ews.unregister_ws_client(websocket)


@app.get(
    "/api/v1/alertas/recientes",
    summary="Alertas EWS recientes (últimas N horas)",
    tags=["EWS Alertas"],
)
async def get_alertas_recientes(
    horas:       int            = Query(24,  ge=1, le=168),
    nivel:       Optional[str]  = Query(None,
                     description="watch | warning | emergency"),
    incluir_cap: bool           = Query(False,
                     description="Incluir XML CAP v1.2 en cada alerta"),
    limit:       int            = Query(50, ge=1, le=500),
):
    """
    Retorna alertas del sistema EWS.
    `incluir_cap=true` añade el mensaje CAP v1.2 completo (ITU-T X.1303bis).
    """
    pool = await db()
    ews: EWSWorker = app.state.ews
    async with pool.acquire() as conn:
        alertas = await ews.get_recent_alerts(
            conn, horas=horas, nivel=nivel,
            incluir_cap=incluir_cap, limit=limit,
        )
    return {
        "alertas": alertas,
        "total": len(alertas),
        "horas": horas,
        "nivel_filtro": nivel,
        "fuente": "INDECI Protocolo Alertas Sísmicas 2020 + EW4All UNDRR 2022",
    }


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — EXPOSICIÓN / IVS
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/exposicion/{ubigeo}",
    summary="Exposición física y vulnerabilidad social por distrito (GEM 2023)",
    tags=["Exposición / IVS"],
)
async def get_exposicion(ubigeo: str):
    """
    Datos de exposición del distrito incluyendo:
    - Taxonomía GEM (Yepes-Estrada et al. 2023 Earthquake Spectra)
    - IVS = 0.30×adobe + 0.25×pobreza + 0.20×sin_agua + 0.15×analfabetismo
             + 0.10×sin_desague (MIDIS SISFOH 2022)
    - `indice_riesgo_total` = IRC_v9 × (1 + IVS) × factor_cascada
    - Percentiles nacionales y departamentales
    """
    ck = _cache_key(f"/api/v1/exposicion/{ubigeo}")
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    row = await pool.fetchrow("""
        SELECT
            e.*,
            d.nombre AS distrito_nombre,
            d.departamento,
            d.provincia,
            d.indice_riesgo_v9,
            d.irc_v9_p10,
            d.irc_v9_p90,
            COALESCE(d.factor_cascada, 1.0) AS factor_cascada,
            (SELECT ROUND(PERCENT_RANK() OVER (ORDER BY ivs)::NUMERIC, 4)
             FROM exposicion_distritos e2 WHERE e2.ubigeo = e.ubigeo) AS percentil_ivs,
            (SELECT ROUND(PERCENT_RANK() OVER (ORDER BY indice_riesgo_total)::NUMERIC, 4)
             FROM exposicion_distritos e2 WHERE e2.ubigeo = e.ubigeo) AS percentil_riesgo_total
        FROM exposicion_distritos e
        JOIN distritos d ON e.ubigeo = d.ubigeo
        WHERE e.ubigeo = $1
    """, ubigeo)

    if not row:
        raise HTTPException(404, detail={
            "error": "ubigeo_no_encontrado",
            "ubigeo": ubigeo,
            "nota": "Ejecuta: python procesar_datos.py --solo exposicion_ivs",
        })

    result = _serialize_row(row)
    # Añadir nivel de riesgo total y comparación nacional
    irt = result.get("indice_riesgo_total")
    if irt:
        nivel = (
            "muy_alto" if irt >= 5.0 else
            "alto"     if irt >= 3.5 else
            "medio"    if irt >= 2.5 else
            "bajo"
        )
        result["nivel_riesgo_total"] = nivel
    result["comparacion_nacional"] = {
        "percentil_ivs": result.pop("percentil_ivs", None),
        "percentil_riesgo_total": result.pop("percentil_riesgo_total", None),
    }
    result["metodologia"] = (
        "IVS: MIDIS SISFOH 2022. IRC v9: CENEPRED 2014. "
        "GEM: Yepes-Estrada et al. 2023 Earthquake Spectra. "
        "indice_riesgo_total = IRC_v9 × (1+IVS) × factor_cascada"
    )

    content = orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS)
    await _cache_set(ck, content, CACHE_EXPOSICION)
    return Response(content=content, media_type="application/json",
                    headers={"X-Cache": "MISS"})


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — TENDENCIA SÍSMICA (TimescaleDB CAG)
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/sismos/tendencia",
    summary="Tendencia sísmica mensual (TimescaleDB CAG + fallback GROUP BY)",
    tags=["Sismos"],
)
async def get_sismos_tendencia(
    año_inicio: int            = Query(1980, ge=1900, le=2100),
    año_fin:    int            = Query(2030, ge=1900, le=2100),
    region:     Optional[str]  = Query(None),
    mag_min:    float          = Query(2.5, ge=0, le=10),
):
    """
    Serie temporal mensual de actividad sísmica.
    Usa `sismos_mensual` (TimescaleDB CAG) si está disponible — consulta en ms.
    Fallback automático a `GROUP BY` si TimescaleDB no está activo.

    Incluye `mag_mediana` (nuevo en v9) además de mag_prom y mag_max.
    """
    ck = _cache_key("/api/v1/sismos/tendencia",
                    año_inicio=año_inicio, año_fin=año_fin,
                    region=region, mag_min=mag_min)
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/json",
                        headers={"X-Cache": "HIT"})

    pool = await db()

    # Intentar CAG TimescaleDB primero
    try:
        rows = await pool.fetch("""
            SELECT
                TO_CHAR(mes, 'YYYY-MM') AS periodo,
                region,
                cantidad,
                mag_max,
                mag_prom,
                mag_mediana,
                m5_plus, m6_plus,
                superficiales
            FROM sismos_mensual
            WHERE EXTRACT(YEAR FROM mes) BETWEEN $1 AND $2
              AND ($3::TEXT IS NULL OR region ILIKE '%'||$3||'%')
            ORDER BY mes
        """, año_inicio, año_fin, region)
        fuente = "TimescaleDB CAG sismos_mensual"
    except Exception:
        # Fallback GROUP BY PostgreSQL estándar
        rows = await pool.fetch("""
            SELECT
                TO_CHAR(DATE_TRUNC('month', fecha), 'YYYY-MM') AS periodo,
                COALESCE(region, 'Sin región') AS region,
                COUNT(*) AS cantidad,
                ROUND(MAX(magnitud)::NUMERIC, 1) AS mag_max,
                ROUND(AVG(magnitud)::NUMERIC, 2) AS mag_prom,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY magnitud) AS mag_mediana,
                COUNT(*) FILTER (WHERE magnitud >= 5.0) AS m5_plus,
                COUNT(*) FILTER (WHERE magnitud >= 6.0) AS m6_plus,
                COUNT(*) FILTER (WHERE tipo_profundidad='superficial') AS superficiales
            FROM sismos
            WHERE EXTRACT(YEAR FROM fecha) BETWEEN $1 AND $2
              AND magnitud >= $3
              AND ($4::TEXT IS NULL OR region ILIKE '%'||$4||'%')
            GROUP BY DATE_TRUNC('month', fecha), region
            ORDER BY periodo
        """, año_inicio, año_fin, mag_min, region)
        fuente = "PostgreSQL GROUP BY (fallback)"

    result = {
        "tendencia": [_serialize_row(r) for r in rows],
        "total_periodos": len(rows),
        "fuente_query": fuente,
        "filtros": {
            "año_inicio": año_inicio, "año_fin": año_fin,
            "region": region, "mag_min": mag_min,
        },
    }

    content = orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS)
    await _cache_set(ck, content, CACHE_TENDENCIA)
    return Response(content=content, media_type="application/json",
                    headers={"X-Cache": "MISS"})


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — LECTURAS DE ESTACIONES
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/estaciones/{codigo}/lecturas",
    summary="Series temporales de una estación de monitoreo",
    tags=["Monitoreo"],
)
async def get_lecturas_estacion(
    codigo:   str,
    variable: Optional[str] = Query(
        None,
        description="temperatura | precipitacion | humedad | "
                    "velocidad_viento | aceleracion_pga | nivel_rio | nivel_mar",
    ),
    desde:    Optional[str] = Query(None, description="ISO 8601 inicio"),
    hasta:    Optional[str] = Query(None, description="ISO 8601 fin"),
    limit:    int           = Query(1000, ge=1, le=5000),
):
    """
    Lee series temporales desde `lecturas_estaciones` (hypertable TimescaleDB).
    """
    pool = await db()

    # Verificar que la estación existe
    est = await pool.fetchrow(
        "SELECT codigo, nombre, tipo, institucion FROM estaciones WHERE codigo = $1",
        codigo,
    )
    if not est:
        raise HTTPException(404, detail={"error": "estacion_no_encontrada", "codigo": codigo})

    rows = await pool.fetch("""
        SELECT time::TEXT AS time, variable, valor, calidad
        FROM lecturas_estaciones
        WHERE estacion_codigo = $1
          AND ($2::TEXT IS NULL OR variable = $2)
          AND ($3::TEXT IS NULL OR time >= $3::TIMESTAMPTZ)
          AND ($4::TEXT IS NULL OR time <= $4::TIMESTAMPTZ)
        ORDER BY time DESC
        LIMIT $5
    """, codigo, variable, desde, hasta, limit)

    return {
        "estacion": _serialize_row(est),
        "lecturas": [_serialize_row(r) for r in rows],
        "total": len(rows),
        "filtros": {"variable": variable, "desde": desde, "hasta": hasta},
    }


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — ESCENARIO SÍSMICO (4 estados DS + GEM)
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/riesgo/escenario",
    summary="Modelo de pérdidas sísmicas 4 estados DS (Youngs 1997 + GEM 2023)",
    tags=["Riesgo de Construcción"],
)
@limiter.limit("20/minute")
async def get_riesgo_escenario(
    request:        Request,
    lon:            float = Query(..., ge=-82,   le=-68),
    lat:            float = Query(..., ge=-18.5, le=0),
    magnitud:       float = Query(7.0, ge=4.0, le=9.5),
    profundidad_km: float = Query(30.0, ge=0, le=600),
    n_viviendas:    int   = Query(1000, ge=1, le=100000),
    hora_del_dia:   str   = Query("dia", description="dia | noche"),
):
    """
    Calcula pérdidas sísmicas usando curvas de fragilidad lognormales.

    Modelos de atenuación:
    - `pga_from_youngs1997` — subducción Nazca-SA (Youngs et al. 1997 BSSA)

    Estados de daño (DS1–DS4):
    - DS1 leve, DS2 moderado, DS3 extenso, DS4 colapso
    - θ_DS adobe calibrado con sismo Pisco M8.0 2007 (Tarque et al. 2012 PUCP)
    - MDR por estado desde GEM Global Vulnerability Model 2023

    Siempre incluye `advertencia` y `metodologia` en la respuesta.
    """
    ck = _cache_key(
        "/api/v1/riesgo/escenario",
        lon=round(lon, 2), lat=round(lat, 2),
        magnitud=round(magnitud, 1),
        profundidad_km=round(profundidad_km, 0),
    )
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    # Obtener mix de construcción del distrito más cercano si disponible
    mix_construccion = None
    try:
        async with pool.acquire() as conn:
            dist_row = await conn.fetchrow("""
                SELECT e.ubigeo, e.pct_adobe, e.pct_ladrillo_conf, e.pct_concreto,
                       e.pct_quincha
                FROM exposicion_distritos e
                JOIN distritos d ON e.ubigeo = d.ubigeo
                WHERE d.geom IS NOT NULL
                ORDER BY d.geom <-> ST_SetSRID(ST_MakePoint($1,$2),4326) LIMIT 1
            """, lon, lat)
            if dist_row:
                total = (float(dist_row["pct_adobe"] or 0) +
                         float(dist_row["pct_ladrillo_conf"] or 0) +
                         float(dist_row["pct_concreto"] or 0) +
                         float(dist_row["pct_quincha"] or 0))
                if total > 0:
                    mix_construccion = {
                        "adobe":         float(dist_row["pct_adobe"] or 0) / 100,
                        "ladrillo_conf": float(dist_row["pct_ladrillo_conf"] or 0) / 100,
                        "concreto_armado": float(dist_row["pct_concreto"] or 0) / 100,
                        "quincha":       float(dist_row["pct_quincha"] or 0) / 100,
                    }
    except Exception:
        pass

    result = scenario_losses(
        lon=lon, lat=lat,
        magnitude=magnitud,
        n_viviendas=n_viviendas,
        mix_construccion=mix_construccion,
        profundidad_km=profundidad_km,
        hora_del_dia=hora_del_dia,
    )

    content = orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS)
    await _cache_set(ck, content, CACHE_ESCENARIO)
    return Response(content=content, media_type="application/json",
                    headers={"X-Cache": "MISS"})


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — SENDAI FRAMEWORK
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/sendai/report",
    summary="Reporte Sendai Framework 2015-2030 (métricas proxy)",
    tags=["Sendai Framework"],
)
@limiter.limit("20/minute")
async def get_sendai_report(
    request: Request,
    año:     int = Query(2024, ge=2015, le=2030),
):
    """
    Métricas proxy para el Marco de Sendai 2015-2030 (7 targets, UNDRR).

    Las métricas se calculan automáticamente desde datos GeoRiesgo v9.
    **NOTA**: Proxy generado automáticamente. NO sustituye el reporte
    oficial INDECI/CENEPRED al UNDRR Sendai Monitor.

    - Target A: mortalidad por desastres
    - Target B: personas afectadas (proxy: IRC v9 ≥ 4)
    - Target D: infraestructura crítica en zonas de alto riesgo
    - Target G: acceso a MHEWS — 4 pilares EW4All (UNDRR 2022)
    """
    ck = _cache_key("/api/v1/sendai/report", año=año)
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    row = await pool.fetchrow(
        "SELECT * FROM sendai_snapshots WHERE año = $1", año
    )
    if not row:
        raise HTTPException(404, detail={
            "error": "snapshot_no_disponible",
            "año": año,
            "nota": "Ejecuta: python procesar_datos.py --solo sendai",
        })

    result = _serialize_row(row)
    result["advertencia"] = (
        "Métricas proxy generadas automáticamente desde GeoRiesgo v9. "
        "NO sustituyen el reporte oficial INDECI/CENEPRED al UNDRR Sendai Monitor."
    )
    result["marco_referencia"] = "Sendai Framework for Disaster Risk Reduction 2015-2030 — UNDRR"

    content = orjson.dumps(result, option=orjson.OPT_NON_STR_KEYS)
    await _cache_set(ck, content, CACHE_SENDAI_REPORT)
    return Response(content=content, media_type="application/json",
                    headers={"X-Cache": "MISS",
                             "Cache-Control": f"public, max-age={CACHE_SENDAI_REPORT}"})


@app.get(
    "/api/v1/sendai/mapa",
    summary="GeoJSON distritos coloreados por contribución a un Target Sendai",
    tags=["Sendai Framework"],
    response_class=Response,
)
async def get_sendai_mapa(
    target: str = Query("b",
                        description="a | b | c | d | g — Target Sendai"),
    zoom:   Optional[int] = Query(None, ge=1, le=20),
):
    """
    Mapa de distritos coloreados según su contribución al Target indicado.
    - Target B: IRC v9 (distritos con mayor población expuesta)
    - Target D: infraestructura crítica en zona de alto riesgo
    - Target G: cobertura MHEWS (% distritos con IRC v9 calculado)
    """
    ck = _cache_key("/api/v1/sendai/mapa", target=target, zoom=zoom)
    cached = await _cache_get(ck)
    if cached:
        return Response(content=cached, media_type="application/geo+json",
                        headers={"X-Cache": "HIT"})

    pool = await db()
    geom_col = _geom_expr(zoom)

    # Coloreado según target
    score_expr = {
        "a": "COALESCE(indice_riesgo_v9, nivel_riesgo::NUMERIC)",
        "b": "COALESCE(indice_riesgo_v9, nivel_riesgo::NUMERIC)",
        "c": "COALESCE(indice_riesgo_v9, nivel_riesgo::NUMERIC)",
        "d": "nivel_riesgo::NUMERIC",
        "g": "CASE WHEN indice_riesgo_v9 IS NOT NULL THEN 5 ELSE 1 END",
    }.get(target.lower(), "COALESCE(indice_riesgo_v9, nivel_riesgo::NUMERIC)")

    rows = await pool.fetch(f"""
        SELECT
            {geom_col} AS geom_json,
            id, ubigeo, nombre, provincia, departamento,
            nivel_riesgo, indice_riesgo_v9, factor_cascada,
            ROUND(({score_expr})::NUMERIC, 2) AS score_sendai,
            CASE ROUND(({score_expr})::NUMERIC, 0)::INT
                WHEN 5 THEN '#b71c1c'
                WHEN 4 THEN '#e53935'
                WHEN 3 THEN '#fb8c00'
                WHEN 2 THEN '#fdd835'
                ELSE       '#43a047'
            END AS color
        FROM distritos
        WHERE geom IS NOT NULL
        ORDER BY {score_expr} DESC NULLS LAST
        LIMIT 1874
    """)

    content = orjson.dumps({
        "type": "FeatureCollection",
        "features": rows_to_features(
            rows,
            ["id","ubigeo","nombre","provincia","departamento",
             "nivel_riesgo","indice_riesgo_v9","factor_cascada",
             "score_sendai","color"],
        ),
        "metadata": {
            "target_sendai": target.upper(),
            "marco": "Sendai Framework 2015-2030 — UNDRR",
            "total": len(rows),
            "api": "GeoRiesgo Perú v9.0",
        },
    }, option=orjson.OPT_NON_STR_KEYS)

    await _cache_set(ck, content, CACHE_IRC_MAPA)
    return Response(content=content, media_type="application/geo+json",
                    headers={"X-Cache": "MISS",
                             "Cache-Control": f"public, max-age={CACHE_IRC_MAPA}"})


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 — RASTER / STAC
# ══════════════════════════════════════════════════════════════════

@app.get(
    "/api/v1/raster/precipitacion",
    summary="Precipitación anual CHIRPS en un punto (window read COG)",
    tags=["Raster / STAC"],
)
async def get_raster_precipitacion(
    lon: float = Query(..., ge=-82,   le=-68),
    lat: float = Query(..., ge=-18.5, le=0),
):
    """
    Lee el valor de precipitación anual (mm) desde el COG almacenado en MinIO.
    Si MinIO no está disponible retorna HTTP 503.

    **Fuente**: CHIRPS v2.0 climatología 1981-2020
    """
    try:
        from stac_catalog import read_raster_point, _get_s3_client
        s3_client = _get_s3_client()
        result = await read_raster_point(lon, lat, s3_client)
        return result
    except RuntimeError as exc:
        raise HTTPException(503, detail={
            "error": "minio_no_disponible",
            "mensaje": str(exc),
        })
    except ImportError:
        raise HTTPException(503, detail={
            "error": "rasterio_no_disponible",
            "mensaje": "Módulo rasterio/stac_catalog no disponible",
        })


@app.get(
    "/api/v1/raster/catalogo",
    summary="Metadata del catálogo STAC 1.0 (colecciones raster)",
    tags=["Raster / STAC"],
)
async def get_raster_catalogo():
    """
    Retorna la metadata del catálogo STAC sin acceder a MinIO.
    Incluye colecciones disponibles, bbox, temporalidad y proveedores.

    **STAC spec**: 1.0.0 — https://stacspec.org
    """
    try:
        from stac_catalog import get_catalog_metadata
        return await get_catalog_metadata()
    except ImportError:
        raise HTTPException(503, detail={
            "error": "stac_catalog_no_disponible",
            "mensaje": "Módulo pystac/stac_catalog no disponible",
        })


# ══════════════════════════════════════════════════════════════════
#  ENDPOINTS v8.0 MANTENIDOS INTACTOS
# ══════════════════════════════════════════════════════════════════

# ── Precipitaciones ──────────────────────────────────────────────

PRECIP_PROPS = [
    "id","nombre","tipo","region","precipitacion_anual_mm",
    "precipitacion_dic_mar_mm","precipitacion_jun_ago_mm",
    "indice_fen","nivel_riesgo_inundacion","fuente",
]


@app.get("/api/v1/precipitaciones", tags=["Precipitaciones / FEN"],
         response_class=Response)
async def get_precipitaciones(
    tipo:             Optional[str]   = Query(None),
    region:           Optional[str]   = Query(None),
    riesgo_inund_min: int             = Query(1, ge=1, le=5),
    fen_min:          Optional[float] = Query(None, ge=0.0),
    zoom:             Optional[int]   = Query(None, ge=1, le=20),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, nombre, tipo, region,
               ROUND(precipitacion_anual_mm::NUMERIC,1) AS precipitacion_anual_mm,
               ROUND(precipitacion_dic_mar_mm::NUMERIC,1) AS precipitacion_dic_mar_mm,
               ROUND(precipitacion_jun_ago_mm::NUMERIC,1) AS precipitacion_jun_ago_mm,
               ROUND(indice_fen::NUMERIC,2) AS indice_fen,
               nivel_riesgo_inundacion, fuente,
               CASE WHEN indice_fen>=3.5 THEN 'Amplificación catastrófica en FEN'
                    WHEN indice_fen>=2.0 THEN 'Amplificación alta en FEN'
                    WHEN indice_fen>=1.3 THEN 'Amplificación moderada en FEN'
                    WHEN indice_fen>=0.9 THEN 'Sin cambio significativo'
                    ELSE 'Reducción (sequía en FEN)' END AS descripcion_fen,
               CASE nivel_riesgo_inundacion
                    WHEN 5 THEN '#b71c1c' WHEN 4 THEN '#e53935'
                    WHEN 3 THEN '#fb8c00' WHEN 2 THEN '#fdd835'
                    ELSE '#43a047' END AS color_riesgo
        FROM zonas_precipitacion
        WHERE nivel_riesgo_inundacion >= $1
          AND ($2::TEXT IS NULL OR tipo   ILIKE '%'||$2||'%')
          AND ($3::TEXT IS NULL OR region ILIKE '%'||$3||'%')
          AND ($4::FLOAT IS NULL OR indice_fen >= $4)
        ORDER BY nivel_riesgo_inundacion DESC, indice_fen DESC, nombre
    """, riesgo_inund_min, tipo, region, fen_min)
    return geojson_response(
        rows_to_features(rows, PRECIP_PROPS + ["descripcion_fen","color_riesgo"]),
        {"fuente": "SENAMHI Atlas Climático 2021 + CHIRPS v2.0 (1981-2020)", "zoom": zoom},
        cache_seconds=43200,
    )


@app.get("/api/v1/precipitaciones/cercanas", tags=["Precipitaciones / FEN"])
async def get_precipitaciones_cercanas(
    lon:      float = Query(..., ge=-82,   le=-68),
    lat:      float = Query(..., ge=-18.5, le=0),
    radio_km: int   = Query(100, ge=1,    le=500),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT id, nombre, tipo, region,
               ROUND(precipitacion_anual_mm::NUMERIC,1) AS precipitacion_anual_mm,
               ROUND(precipitacion_dic_mar_mm::NUMERIC,1) AS precipitacion_dic_mar_mm,
               ROUND(precipitacion_jun_ago_mm::NUMERIC,1) AS precipitacion_jun_ago_mm,
               ROUND(indice_fen::NUMERIC,2) AS indice_fen,
               nivel_riesgo_inundacion, fuente,
               ROUND((ST_Distance(geom::GEOGRAPHY,
                   ST_SetSRID(ST_MakePoint($1,$2),4326)::GEOGRAPHY)/1000)::NUMERIC,1)
                   AS distancia_km
        FROM zonas_precipitacion
        WHERE ST_DWithin(geom::GEOGRAPHY,
            ST_SetSRID(ST_MakePoint($1,$2),4326)::GEOGRAPHY, $3*1000)
        ORDER BY distancia_km ASC
    """, lon, lat, radio_km)
    return {"punto": {"lon": lon, "lat": lat}, "radio_km": radio_km,
            "zonas": [_serialize_row(r) for r in rows], "total": len(rows)}


# ── FEN ──────────────────────────────────────────────────────────

@app.get("/api/v1/fen", tags=["Precipitaciones / FEN"])
async def get_eventos_fen(
    tipo:       Optional[str]   = Query(None),
    intensidad: Optional[str]   = Query(None),
    año_desde:  int             = Query(1957, ge=1950, le=2030),
    año_hasta:  int             = Query(2030, ge=1950, le=2030),
    oni_min:    Optional[float] = Query(None, ge=-3.0, le=3.0),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT id, año_inicio, mes_inicio, año_fin, mes_fin, tipo, intensidad,
               ROUND(oni_peak::NUMERIC,2) AS oni_peak, impacto_peru, fuente,
               (año_fin-año_inicio)*12+(mes_fin-mes_inicio) AS duracion_meses
        FROM eventos_fen
        WHERE año_inicio >= $1 AND año_inicio <= $2
          AND ($3::TEXT IS NULL OR tipo = $3)
          AND ($4::TEXT IS NULL OR intensidad = $4)
          AND ($5::FLOAT IS NULL OR ABS(oni_peak) >= $5)
        ORDER BY año_inicio DESC
    """, año_desde, año_hasta, tipo, intensidad, oni_min)
    return {"eventos": [_serialize_row(r) for r in rows], "total": len(rows),
            "fuentes": ["NOAA-CPC ONI", "ENFEN-SENAMHI Perú"]}


@app.get("/api/v1/fen/estadisticas", tags=["Precipitaciones / FEN"])
async def get_fen_estadisticas():
    pool = await db()
    dist = await pool.fetch("""
        SELECT tipo, intensidad, COUNT(*) AS cantidad,
               ROUND(AVG(ABS(oni_peak))::NUMERIC,2) AS oni_prom,
               ROUND(MAX(ABS(oni_peak))::NUMERIC,2) AS oni_max,
               ROUND(AVG((año_fin-año_inicio)*12+(mes_fin-mes_inicio))::NUMERIC,1)
                   AS duracion_prom_meses
        FROM eventos_fen GROUP BY tipo, intensidad ORDER BY tipo, oni_prom DESC
    """)
    extremos = await pool.fetch("""
        SELECT año_inicio, tipo, intensidad,
               ROUND(oni_peak::NUMERIC,2) AS oni_peak, impacto_peru
        FROM eventos_fen WHERE intensidad IN ('fuerte','extraordinario')
        ORDER BY ABS(oni_peak) DESC LIMIT 5
    """)
    return {"distribucion_tipo_intensidad": [_serialize_row(r) for r in dist],
            "eventos_mas_intensos": [_serialize_row(r) for r in extremos]}


# ── Riesgo lluvia ─────────────────────────────────────────────────

@app.get("/api/v1/riesgo/lluvia", tags=["Precipitaciones / FEN"])
async def get_riesgo_lluvia(
    lon: float = Query(..., ge=-82,   le=-68),
    lat: float = Query(..., ge=-18.5, le=0),
):
    pool = await db()
    zona = await pool.fetchrow("""
        SELECT nombre, tipo, region,
               ROUND(precipitacion_anual_mm::NUMERIC,1) AS precipitacion_anual_mm,
               ROUND(precipitacion_dic_mar_mm::NUMERIC,1) AS precipitacion_dic_mar_mm,
               ROUND(precipitacion_jun_ago_mm::NUMERIC,1) AS precipitacion_jun_ago_mm,
               ROUND(indice_fen::NUMERIC,2) AS indice_fen,
               nivel_riesgo_inundacion
        FROM zonas_precipitacion
        ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1,$2),4326) LIMIT 1
    """, lon, lat)
    inundaciones = await pool.fetch("""
        SELECT nombre, nivel_riesgo, tipo_inundacion, periodo_retorno
        FROM zonas_inundables
        WHERE ST_Covers(geom, ST_SetSRID(ST_MakePoint($1,$2),4326))
        ORDER BY nivel_riesgo DESC LIMIT 3
    """, lon, lat)
    desliz = await pool.fetchval("""
        SELECT COUNT(*) FROM deslizamientos
        WHERE activo=TRUE AND ST_DWithin(geom::GEOGRAPHY,
            ST_SetSRID(ST_MakePoint($1,$2),4326)::GEOGRAPHY, 20000)
    """, lon, lat)
    fen_reciente = await pool.fetchrow("""
        SELECT año_inicio, tipo, intensidad, ROUND(oni_peak::NUMERIC,2) AS oni_peak
        FROM eventos_fen WHERE intensidad IN ('fuerte','extraordinario')
        ORDER BY año_inicio DESC LIMIT 1
    """)
    nivel_base = int(zona["nivel_riesgo_inundacion"] or 2) if zona else 2
    indice_fen = float(zona["indice_fen"] or 1.0) if zona else 1.0
    indice_pluvial = min(5.0, round(
        nivel_base*0.5 + min(indice_fen,4.5)*0.25
        + min(len(inundaciones),3)*0.15 + min(int(desliz or 0),5)*0.10, 2
    ))
    return {
        "punto": {"lon": lon, "lat": lat},
        "zona_climatica": _serialize_row(zona) if zona else None,
        "inundaciones": [_serialize_row(r) for r in inundaciones],
        "deslizamientos_20km": int(desliz or 0),
        "fen_reciente": _serialize_row(fen_reciente) if fen_reciente else None,
        "indice_pluvial": indice_pluvial,
        "nivel_riesgo": (
            "MUY ALTO" if indice_pluvial>=4.5 else "ALTO" if indice_pluvial>=3.5
            else "MEDIO" if indice_pluvial>=2.5 else "BAJO" if indice_pluvial>=1.5
            else "MUY BAJO"
        ),
        "metodologia": {
            "formula": "0.5×zona + 0.25×FEN + 0.15×inundaciones + 0.10×deslizamientos",
        },
    }


# ── Zonas sísmicas ────────────────────────────────────────────────

@app.get("/api/v1/zonas-sismicas", tags=["Sismicidad"], response_class=Response)
async def get_zonas_sismicas(
    zona: Optional[int] = Query(None, ge=1, le=4),
    zoom: Optional[int] = Query(None, ge=1, le=20),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, ubigeo, nombre,
               COALESCE(zona_sismica,2) AS zona_sismica,
               COALESCE(factor_z,0.25) AS factor_z,
               nivel_riesgo, area_km2, capital,
               CASE COALESCE(zona_sismica,2)
                   WHEN 4 THEN 'Muy Alta — Costa'
                   WHEN 3 THEN 'Alta — Sierra Central/Sur'
                   WHEN 2 THEN 'Media — Sierra Norte/Selva Central'
                   WHEN 1 THEN 'Baja — Amazonia' ELSE 'No clasificado' END AS descripcion_zona,
               CASE COALESCE(zona_sismica,2)
                   WHEN 4 THEN '#d32f2f' WHEN 3 THEN '#f57c00'
                   WHEN 2 THEN '#fbc02d' WHEN 1 THEN '#388e3c'
                   ELSE '#9e9e9e' END AS color
        FROM departamentos
        WHERE ($1::INT IS NULL OR zona_sismica=$1)
        ORDER BY COALESCE(zona_sismica,2) DESC, nombre
    """, zona)
    return geojson_response(
        rows_to_features(rows, ["id","ubigeo","nombre","zona_sismica","factor_z",
                                "nivel_riesgo","area_km2","capital","descripcion_zona","color"]),
        {"norma": "NTE E.030-2018 — DS N°003-2016-VIVIENDA", "zoom": zoom},
        cache_seconds=86400,
    )


@app.get("/api/v1/zonas-sismicas/referencia", tags=["Sismicidad"])
async def get_zonas_sismicas_referencia():
    pool = await db()
    rows = await pool.fetch("""
        SELECT departamento, zona_sismica, factor_z, descripcion, referencia,
               actualizado_en::TEXT AS actualizado_en
        FROM zona_sismica_departamento ORDER BY zona_sismica DESC, departamento
    """)
    return {"referencia": [_serialize_row(r) for r in rows], "total": len(rows),
            "norma": "NTE E.030-2018 — DS N°003-2016-VIVIENDA"}


# ── Infraestructura cobertura ─────────────────────────────────────

@app.get("/api/v1/infraestructura/cobertura", tags=["Infraestructura"])
async def get_infraestructura_cobertura(
    tipo:        Optional[str] = Query(None),
    fuente_tipo: Optional[str] = Query(None),
):
    pool = await db()
    try:
        rows = await pool.fetch("""
            SELECT tipo, fuente_tipo, total, con_region, con_zona_sismica,
                   regiones_distintas, criticidad_max, criticidad_prom
            FROM v_infraestructura_cobertura
            WHERE ($1::TEXT IS NULL OR tipo=$1) AND ($2::TEXT IS NULL OR fuente_tipo=$2)
            ORDER BY tipo, fuente_tipo
        """, tipo, fuente_tipo)
    except Exception:
        rows = await pool.fetch("""
            SELECT tipo, fuente_tipo,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE region IS NOT NULL) AS con_region,
                   COUNT(*) FILTER (WHERE zona_sismica IS NOT NULL) AS con_zona_sismica,
                   COUNT(DISTINCT region) AS regiones_distintas,
                   MAX(criticidad) AS criticidad_max,
                   ROUND(AVG(criticidad)::NUMERIC,2) AS criticidad_prom
            FROM infraestructura
            WHERE ($1::TEXT IS NULL OR tipo=$1) AND ($2::TEXT IS NULL OR fuente_tipo=$2)
            GROUP BY tipo, fuente_tipo ORDER BY tipo, fuente_tipo
        """, tipo, fuente_tipo)
    total_global = sum(r["total"] for r in rows)
    return {
        "cobertura": [_serialize_row(r) for r in rows],
        "resumen": {
            "total_elementos": total_global,
            "total_oficial": sum(r["total"] for r in rows if r["fuente_tipo"]=="oficial"),
            "total_osm":     sum(r["total"] for r in rows if r["fuente_tipo"]=="osm"),
        },
    }


# ── Riesgo construcción mapa ──────────────────────────────────────

@app.get("/api/v1/riesgo/construccion/mapa", tags=["Riesgo de Construcción"],
         response_class=Response)
async def get_riesgo_construccion_mapa(
    departamento: Optional[str]  = Query(None),
    zona_sismica: Optional[int]  = Query(None, ge=1, le=4),
    indice_min:   float          = Query(1.0, ge=1.0, le=5.0),
    zoom:         Optional[int]  = Query(None, ge=1, le=20),
    limit:        int            = Query(500, ge=1, le=2000),
):
    pool = await db()
    geom_col = _geom_expr(zoom, col="d.geom")
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json,
               mv.id, mv.ubigeo, mv.distrito, mv.provincia, mv.departamento,
               mv.zona_sismica, COALESCE(mv.factor_z,0.25) AS factor_z,
               COALESCE(mv.clasificacion_suelo,'S2') AS clasificacion_suelo,
               COALESCE(mv.poblacion,0) AS poblacion,
               mv.peligro_sismico, mv.peligro_inundacion, mv.peligro_deslizamiento,
               mv.peligro_tsunami, mv.fallas_activas_50km, mv.sismos_m4_30a_50km,
               ROUND(mv.indice_riesgo_construccion::NUMERIC,2) AS indice_riesgo_construccion,
               CASE WHEN mv.indice_riesgo_construccion>=4.5 THEN 'MUY ALTO'
                    WHEN mv.indice_riesgo_construccion>=3.5 THEN 'ALTO'
                    WHEN mv.indice_riesgo_construccion>=2.5 THEN 'MEDIO'
                    WHEN mv.indice_riesgo_construccion>=1.5 THEN 'BAJO'
                    ELSE 'MUY BAJO' END AS nivel_riesgo,
               CASE WHEN mv.indice_riesgo_construccion>=4.5 THEN '#b71c1c'
                    WHEN mv.indice_riesgo_construccion>=3.5 THEN '#e53935'
                    WHEN mv.indice_riesgo_construccion>=2.5 THEN '#fb8c00'
                    WHEN mv.indice_riesgo_construccion>=1.5 THEN '#fdd835'
                    ELSE '#43a047' END AS color
        FROM mv_riesgo_construccion mv JOIN distritos d ON mv.id=d.id
        WHERE mv.indice_riesgo_construccion>=$1
          AND ($2::TEXT IS NULL OR LOWER(mv.departamento) ILIKE '%'||LOWER($2)||'%')
          AND ($3::INT IS NULL OR mv.zona_sismica=$3)
          AND d.geom IS NOT NULL
        ORDER BY mv.indice_riesgo_construccion DESC LIMIT $4
    """, indice_min, departamento, zona_sismica, limit)
    return geojson_response(
        rows_to_features(rows, ["id","ubigeo","distrito","provincia","departamento",
                                "zona_sismica","factor_z","clasificacion_suelo","poblacion",
                                "peligro_sismico","peligro_inundacion","peligro_deslizamiento",
                                "peligro_tsunami","fallas_activas_50km","sismos_m4_30a_50km",
                                "indice_riesgo_construccion","nivel_riesgo","color"]),
        {"metodologia": "CENEPRED 2014 + NTE E.030-2018 + NTE E.031-2020", "zoom": zoom},
        cache_seconds=1800,
    )


# ── Departamentos ─────────────────────────────────────────────────

@app.get("/api/v1/departamentos", tags=["Administrativo"], response_class=Response)
async def get_departamentos(
    riesgo_min: int           = Query(1, ge=1, le=5),
    nombre:     Optional[str] = Query(None),
    zoom:       Optional[int] = Query(None, ge=1, le=20),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, ubigeo, nombre, nivel_riesgo,
               COALESCE(zona_sismica,2) AS zona_sismica,
               COALESCE(factor_z,0.25) AS factor_z, area_km2, capital, fuente
        FROM departamentos
        WHERE nivel_riesgo>=$1 AND ($2::TEXT IS NULL OR nombre ILIKE '%'||$2||'%')
        ORDER BY nombre
    """, riesgo_min, nombre)
    return geojson_response(
        rows_to_features(rows, ["id","ubigeo","nombre","nivel_riesgo","zona_sismica",
                                "factor_z","area_km2","capital","fuente"]),
        {"zoom": zoom}, cache_seconds=3600,
    )


# ── Sismos ────────────────────────────────────────────────────────

_SISMOS_PROPS = ["usgs_id","magnitud","profundidad_km","tipo_profundidad",
                 "fecha","lugar","region","tipo_magnitud","estado"]


@app.get("/api/v1/sismos", tags=["Sismos"], response_class=Response)
async def get_sismos(
    mag_min:    float         = Query(3.0,  ge=0,    le=10),
    mag_max:    float         = Query(9.9,  ge=0,    le=10),
    year_start: int           = Query(1960, ge=1900, le=2100),
    year_end:   int           = Query(2030, ge=1900, le=2100),
    prof_tipo:  Optional[str] = Query(None),
    region:     Optional[str] = Query(None),
    limit:      int           = Query(5000, ge=1,    le=20000),
    offset:     int           = Query(0,    ge=0),
):
    if mag_min > mag_max:
        raise HTTPException(400, detail={"error": "mag_min > mag_max"})
    pool = await db()
    rows = await pool.fetch("""
        SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               usgs_id, magnitud, profundidad_km, tipo_profundidad,
               fecha::TEXT AS fecha, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region,
               tipo_magnitud, estado
        FROM sismos
        WHERE magnitud BETWEEN $1 AND $2
          AND EXTRACT(YEAR FROM fecha) BETWEEN $3 AND $4
          AND ($5::TEXT IS NULL OR tipo_profundidad=$5)
          AND ($6::TEXT IS NULL OR region ILIKE '%'||$6||'%')
        ORDER BY fecha DESC, magnitud DESC
        LIMIT $7 OFFSET $8
    """, mag_min, mag_max, year_start, year_end, prof_tipo, region, limit, offset)
    return geojson_response(rows_to_features(rows, _SISMOS_PROPS),
                            {"filtros": {"mag_min": mag_min, "mag_max": mag_max,
                                         "year_start": year_start, "year_end": year_end}})


@app.get("/api/v1/sismos/recientes", tags=["Sismos"], response_class=Response)
async def get_sismos_recientes(
    dias:    int   = Query(30,  ge=1,  le=365),
    mag_min: float = Query(2.5, ge=0,  le=10),
    limit:   int   = Query(500, ge=1,  le=2000),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               usgs_id, magnitud, profundidad_km, tipo_profundidad,
               fecha::TEXT AS fecha, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region,
               tipo_magnitud, estado
        FROM sismos
        WHERE fecha >= CURRENT_DATE-($1*INTERVAL '1 day') AND magnitud>=$2
        ORDER BY fecha DESC, magnitud DESC LIMIT $3
    """, dias, mag_min, limit)
    return geojson_response(rows_to_features(rows, _SISMOS_PROPS),
                            {"dias": dias, "mag_min": mag_min}, cache_seconds=120)


@app.get("/api/v1/sismos/estadisticas", tags=["Sismos"])
async def get_estadisticas_sismos(
    year_start: int           = Query(1960, ge=1900, le=2100),
    year_end:   int           = Query(2030, ge=1900, le=2100),
    mag_min:    float         = Query(2.5,  ge=0,    le=10),
    region:     Optional[str] = Query(None),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT EXTRACT(YEAR FROM fecha)::INTEGER AS anio,
               COUNT(*) AS cantidad,
               ROUND(MAX(magnitud)::NUMERIC,1) AS magnitud_max,
               ROUND(AVG(magnitud)::NUMERIC,2) AS magnitud_prom,
               COUNT(*) FILTER (WHERE tipo_profundidad='superficial') AS superficiales,
               COUNT(*) FILTER (WHERE tipo_profundidad='intermedio')  AS intermedios,
               COUNT(*) FILTER (WHERE tipo_profundidad='profundo')    AS profundos,
               COUNT(*) FILTER (WHERE magnitud>=5.0) AS m5_plus,
               COUNT(*) FILTER (WHERE magnitud>=6.0) AS m6_plus,
               COUNT(*) FILTER (WHERE magnitud>=7.0) AS m7_plus
        FROM sismos
        WHERE EXTRACT(YEAR FROM fecha) BETWEEN $1 AND $2
          AND magnitud>=$3
          AND ($4::TEXT IS NULL OR region ILIKE '%'||$4||'%')
        GROUP BY EXTRACT(YEAR FROM fecha) ORDER BY anio
    """, year_start, year_end, mag_min, region)
    return [_serialize_row(r) for r in rows]


@app.get("/api/v1/sismos/heatmap", tags=["Sismos"], response_class=Response)
async def get_heatmap_sismos(
    resolucion: float         = Query(0.1, ge=0.05, le=1.0),
    mag_min:    float         = Query(3.0, ge=0,    le=10),
    year_start: Optional[int] = Query(None, ge=1900),
    year_end:   Optional[int] = Query(None, le=2100),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT ST_AsGeoJSON(ST_Centroid(ST_SnapToGrid(geom,$1)),6)::TEXT AS geom_json,
               COUNT(*) AS cantidad,
               ROUND(AVG(magnitud)::NUMERIC,2) AS magnitud_prom,
               ROUND(MAX(magnitud)::NUMERIC,1) AS magnitud_max,
               ROUND(AVG(profundidad_km)::NUMERIC,1) AS prof_prom
        FROM sismos
        WHERE magnitud>=$2
          AND ($3::INT IS NULL OR EXTRACT(YEAR FROM fecha)>=$3)
          AND ($4::INT IS NULL OR EXTRACT(YEAR FROM fecha)<=$4)
        GROUP BY ST_SnapToGrid(geom,$1) HAVING COUNT(*)>0
        ORDER BY cantidad DESC
    """, resolucion, mag_min, year_start, year_end)
    features = [
        {"type":"Feature","geometry": json.loads(r["geom_json"]),
         "properties": {"cantidad": r["cantidad"], "magnitud_prom": _safe_float(r["magnitud_prom"]),
                        "magnitud_max": _safe_float(r["magnitud_max"]), "prof_prom": _safe_float(r["prof_prom"])}}
        for r in rows if r["geom_json"]
    ]
    return geojson_response(features, {"resolucion_grados": resolucion})


@app.get("/api/v1/sismos/cercanos", tags=["Sismos"])
async def get_sismos_cercanos(
    lon:      float = Query(..., ge=-82,   le=-68),
    lat:      float = Query(..., ge=-18.5, le=0),
    radio_km: int   = Query(50,  ge=1,    le=500),
    mag_min:  float = Query(3.0, ge=0,    le=10),
    limit:    int   = Query(100, ge=1,    le=1000),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT usgs_id, magnitud, profundidad_km, tipo_profundidad,
               fecha::TEXT AS fecha, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region,
               ROUND((ST_Distance(geom::GEOGRAPHY,
                   ST_SetSRID(ST_MakePoint($1,$2),4326)::GEOGRAPHY)/1000)::NUMERIC,1)
                   AS distancia_km
        FROM sismos
        WHERE magnitud>=$3
          AND ST_DWithin(geom::GEOGRAPHY,
              ST_SetSRID(ST_MakePoint($1,$2),4326)::GEOGRAPHY, $4*1000)
        ORDER BY distancia_km ASC LIMIT $5
    """, lon, lat, mag_min, radio_km, limit)
    return [_serialize_row(r) for r in rows]


@app.get("/api/v1/sismos/{usgs_id}", tags=["Sismos"])
async def get_sismo_detalle(usgs_id: str):
    pool = await db()
    row = await pool.fetchrow("""
        SELECT usgs_id, magnitud, profundidad_km, tipo_profundidad,
               fecha::TEXT, hora_utc::TEXT AS hora_utc, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region,
               tipo_magnitud, estado, fuente,
               ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM sismos WHERE usgs_id=$1
    """, usgs_id)
    if not row:
        raise HTTPException(404, detail={"error":"not_found","usgs_id":usgs_id})
    d = _serialize_row(row)
    d["geom"] = json.loads(d.pop("geom_json"))
    return d


# ── Distritos ─────────────────────────────────────────────────────

@app.get("/api/v1/distritos", tags=["Administrativo"], response_class=Response)
async def get_distritos(
    provincia:    Optional[str] = Query(None),
    departamento: Optional[str] = Query(None),
    riesgo_min:   int           = Query(1, ge=1, le=5),
    zoom:         Optional[int] = Query(None, ge=1, le=20),
    limit:        int           = Query(500, ge=1, le=2000),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, ubigeo, nombre, provincia, departamento,
               nivel_riesgo, poblacion, area_km2, COALESCE(zona_sismica,2) AS zona_sismica, fuente
        FROM distritos
        WHERE nivel_riesgo>=$1
          AND ($2::TEXT IS NULL OR LOWER(provincia)    ILIKE '%'||LOWER($2)||'%')
          AND ($3::TEXT IS NULL OR LOWER(departamento) ILIKE '%'||LOWER($3)||'%')
        ORDER BY nivel_riesgo DESC, nombre LIMIT $4
    """, riesgo_min, provincia, departamento, limit)
    return geojson_response(
        rows_to_features(rows, ["id","ubigeo","nombre","provincia","departamento",
                                "nivel_riesgo","poblacion","area_km2","zona_sismica","fuente"]),
        {"zoom": zoom}, cache_seconds=3600,
    )


@app.get("/api/v1/distritos/resumen", tags=["Administrativo"])
async def get_distritos_resumen():
    pool = await db()
    rows = await pool.fetch("""
        SELECT d.nombre, d.provincia, d.departamento, d.nivel_riesgo,
               COUNT(s.id) AS total_sismos,
               ROUND(MAX(s.magnitud)::NUMERIC,1) AS max_magnitud,
               ROUND(AVG(s.magnitud)::NUMERIC,2) AS avg_magnitud,
               COUNT(s.id) FILTER (WHERE s.magnitud>=5.0) AS m5_plus
        FROM distritos d LEFT JOIN sismos s ON ST_Covers(d.geom,s.geom)
        GROUP BY d.nombre, d.provincia, d.departamento, d.nivel_riesgo
        ORDER BY total_sismos DESC LIMIT 100
    """)
    return [_serialize_row(r) for r in rows]


# ── Fallas ────────────────────────────────────────────────────────

@app.get("/api/v1/fallas", tags=["Geología"], response_class=Response)
async def get_fallas(
    activas_only: bool            = Query(False),
    tipo:         Optional[str]   = Query(None),
    mecanismo:    Optional[str]   = Query(None),
    region:       Optional[str]   = Query(None),
    mag_min:      Optional[float] = Query(None, ge=0, le=10),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               id, ingemmet_id, nombre, nombre_alt, activa, tipo, mecanismo,
               longitud_km, magnitud_max,
               COALESCE(region, f_asignar_region(ST_X(ST_Centroid(geom)),ST_Y(ST_Centroid(geom)))) AS region,
               fuente, referencia
        FROM fallas
        WHERE ($1=FALSE OR activa=TRUE)
          AND ($2::TEXT IS NULL OR tipo      ILIKE '%'||$2||'%')
          AND ($3::TEXT IS NULL OR mecanismo ILIKE '%'||$3||'%')
          AND ($4::TEXT IS NULL OR region    ILIKE '%'||$4||'%')
          AND ($5::FLOAT IS NULL OR magnitud_max>=$5)
        ORDER BY activa DESC, longitud_km DESC NULLS LAST
    """, activas_only, tipo, mecanismo, region, mag_min)
    return geojson_response(
        rows_to_features(rows, ["id","ingemmet_id","nombre","nombre_alt","activa","tipo",
                                "mecanismo","longitud_km","magnitud_max","region","fuente","referencia"]),
        cache_seconds=3600,
    )


# ── Inundaciones / Tsunamis / Deslizamientos ──────────────────────

@app.get("/api/v1/inundaciones", tags=["Hidrometeorología"], response_class=Response)
async def get_inundaciones(
    riesgo_min:  int            = Query(1, ge=1, le=5),
    tipo:        Optional[str]  = Query(None),
    region:      Optional[str]  = Query(None),
    cuenca:      Optional[str]  = Query(None),
    periodo_max: Optional[int]  = Query(None, ge=1),
    zoom:        Optional[int]  = Query(None, ge=1, le=20),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, nombre, nivel_riesgo, tipo_inundacion,
               periodo_retorno, profundidad_max_m, cuenca, region, fuente
        FROM zonas_inundables
        WHERE nivel_riesgo>=$1
          AND ($2::TEXT IS NULL OR tipo_inundacion ILIKE '%'||$2||'%')
          AND ($3::TEXT IS NULL OR region          ILIKE '%'||$3||'%')
          AND ($4::TEXT IS NULL OR cuenca          ILIKE '%'||$4||'%')
          AND ($5::INT IS NULL OR periodo_retorno<=$5)
        ORDER BY nivel_riesgo DESC
    """, riesgo_min, tipo, region, cuenca, periodo_max)
    return geojson_response(
        rows_to_features(rows, ["id","nombre","nivel_riesgo","tipo_inundacion",
                                "periodo_retorno","profundidad_max_m","cuenca","region","fuente"]),
        {"zoom": zoom}, cache_seconds=1800,
    )


@app.get("/api/v1/tsunamis", tags=["Hidrometeorología"], response_class=Response)
async def get_tsunamis(
    riesgo_min:  int             = Query(1, ge=1, le=5),
    region:      Optional[str]   = Query(None),
    altura_min:  Optional[float] = Query(None, ge=0),
    zoom:        Optional[int]   = Query(None, ge=1, le=20),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, nombre, nivel_riesgo,
               altura_ola_m, tiempo_arribo_min, periodo_retorno, region, fuente
        FROM zonas_tsunami
        WHERE nivel_riesgo>=$1
          AND ($2::TEXT IS NULL OR region ILIKE '%'||$2||'%')
          AND ($3::FLOAT IS NULL OR altura_ola_m>=$3)
        ORDER BY nivel_riesgo DESC
    """, riesgo_min, region, altura_min)
    return geojson_response(
        rows_to_features(rows, ["id","nombre","nivel_riesgo","altura_ola_m",
                                "tiempo_arribo_min","periodo_retorno","region","fuente"]),
        cache_seconds=3600,
    )


@app.get("/api/v1/deslizamientos", tags=["Geología"], response_class=Response)
async def get_deslizamientos(
    riesgo_min: int            = Query(1, ge=1, le=5),
    tipo:       Optional[str]  = Query(None),
    region:     Optional[str]  = Query(None),
    activos:    Optional[bool] = Query(None),
    zoom:       Optional[int]  = Query(None, ge=1, le=20),
):
    pool = await db()
    geom_col = _geom_expr(zoom)
    rows = await pool.fetch(f"""
        SELECT {geom_col} AS geom_json, id, nombre, tipo, nivel_riesgo,
               area_km2, region, activo, fuente
        FROM deslizamientos
        WHERE nivel_riesgo>=$1
          AND ($2::TEXT IS NULL OR tipo   ILIKE '%'||$2||'%')
          AND ($3::TEXT IS NULL OR region ILIKE '%'||$3||'%')
          AND ($4::BOOL IS NULL OR activo=$4)
        ORDER BY nivel_riesgo DESC
    """, riesgo_min, tipo, region, activos)
    return geojson_response(
        rows_to_features(rows, ["id","nombre","tipo","nivel_riesgo","area_km2",
                                "region","activo","fuente"]),
        {"zoom": zoom}, cache_seconds=1800,
    )


# ── Infraestructura ───────────────────────────────────────────────

@app.get("/api/v1/infraestructura", tags=["Infraestructura"], response_class=Response)
async def get_infraestructura(
    tipo:           Optional[str]   = Query(None),
    criticidad_min: int             = Query(1, ge=1, le=5),
    region:         Optional[str]   = Query(None),
    fuente_tipo:    Optional[str]   = Query(None),
    radio_km:       Optional[int]   = Query(None, ge=1, le=500),
    lon:            Optional[float] = Query(None, ge=-82,   le=-68),
    lat:            Optional[float] = Query(None, ge=-18.5, le=0),
    limit:          int             = Query(500, ge=1, le=2000),
):
    pool = await db()
    if radio_km and (lon is None or lat is None):
        raise HTTPException(400, detail={"error": "radio_km requiere lon y lat"})
    spatial = bool(radio_km and lon is not None and lat is not None)
    rows = await pool.fetch("""
        SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               id, osm_id, nombre, tipo, criticidad, estado,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region,
               distrito, fuente, COALESCE(fuente_tipo,'osm') AS fuente_tipo
        FROM infraestructura
        WHERE criticidad>=$1
          AND ($2::TEXT IS NULL OR tipo        ILIKE '%'||$2||'%')
          AND ($3::TEXT IS NULL OR region      ILIKE '%'||$3||'%')
          AND ($9::TEXT IS NULL OR fuente_tipo=$9)
          AND (NOT $5::BOOLEAN OR ST_DWithin(geom::GEOGRAPHY,
              ST_SetSRID(ST_MakePoint($6::FLOAT,$7::FLOAT),4326)::GEOGRAPHY, $8::FLOAT*1000))
        ORDER BY criticidad DESC, nombre LIMIT $4
    """, criticidad_min, tipo, region, limit,
         spatial, lon or 0.0, lat or 0.0, float(radio_km) if radio_km else 0.0, fuente_tipo)
    return geojson_response(
        rows_to_features(rows, ["id","osm_id","nombre","tipo","criticidad",
                                "estado","region","distrito","fuente","fuente_tipo"])
    )


# ── Estaciones ────────────────────────────────────────────────────

@app.get("/api/v1/estaciones", tags=["Monitoreo"], response_class=Response)
async def get_estaciones(
    tipo:        Optional[str] = Query(None),
    institucion: Optional[str] = Query(None),
    region:      Optional[str] = Query(None),
    activas:     bool          = Query(True),
):
    pool = await db()
    rows = await pool.fetch("""
        SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               id, codigo, nombre, tipo, altitud_m, activa, institucion,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region, red
        FROM estaciones
        WHERE ($1=FALSE OR activa=TRUE)
          AND ($2::TEXT IS NULL OR tipo        ILIKE '%'||$2||'%')
          AND ($3::TEXT IS NULL OR institucion ILIKE '%'||$3||'%')
          AND ($4::TEXT IS NULL OR region      ILIKE '%'||$4||'%')
        ORDER BY institucion, tipo, nombre
    """, activas, tipo, institucion, region)
    return geojson_response(
        rows_to_features(rows, ["id","codigo","nombre","tipo","altitud_m",
                                "activa","institucion","region","red"]),
        cache_seconds=3600,
    )


# ── BBox ──────────────────────────────────────────────────────────

@app.get("/api/v1/bbox", tags=["Espacial"])
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
        raise HTTPException(400, detail={"error": "bbox_invalido"})
    pool = await db()
    capas_list = [c.strip().lower() for c in capas.split(",")]
    bbox = f"ST_MakeEnvelope({min_lon},{min_lat},{max_lon},{max_lat},4326)"
    geom_poly = _geom_expr(zoom)
    resultado: dict = {}

    queries = {
        "sismos": (f"""SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               usgs_id, magnitud, profundidad_km, tipo_profundidad,
               fecha::TEXT AS fecha, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region
               FROM sismos WHERE magnitud>=$1 AND geom&&{bbox}
               ORDER BY magnitud DESC LIMIT 2000""",
               [mag_min], ["usgs_id","magnitud","profundidad_km","tipo_profundidad","fecha","lugar","region"]),
        "fallas": (f"""SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               nombre, activa, tipo, longitud_km,
               COALESCE(region, f_asignar_region(ST_X(ST_Centroid(geom)),ST_Y(ST_Centroid(geom)))) AS region
               FROM fallas WHERE geom&&{bbox}""", [], ["nombre","activa","tipo","longitud_km","region"]),
        "inundaciones": (f"""SELECT {geom_poly} AS geom_json, nombre, nivel_riesgo, tipo_inundacion, region
               FROM zonas_inundables WHERE geom&&{bbox}""", [], ["nombre","nivel_riesgo","tipo_inundacion","region"]),
        "tsunamis": (f"""SELECT {geom_poly} AS geom_json, nombre, nivel_riesgo, altura_ola_m, region
               FROM zonas_tsunami WHERE geom&&{bbox}""", [], ["nombre","nivel_riesgo","altura_ola_m","region"]),
        "deslizamientos": (f"""SELECT {geom_poly} AS geom_json, nombre, tipo, nivel_riesgo, region
               FROM deslizamientos WHERE geom&&{bbox}""", [], ["nombre","tipo","nivel_riesgo","region"]),
        "infraestructura": (f"""SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json, nombre, tipo, criticidad,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region
               FROM infraestructura WHERE geom&&{bbox} ORDER BY criticidad DESC LIMIT 500""",
               [], ["nombre","tipo","criticidad","region"]),
        "departamentos": (f"""SELECT {geom_poly} AS geom_json, nombre, nivel_riesgo,
               COALESCE(zona_sismica,2) AS zona_sismica
               FROM departamentos WHERE geom&&{bbox}""", [], ["nombre","nivel_riesgo","zona_sismica"]),
        "distritos": (f"""SELECT {geom_poly} AS geom_json, nombre, provincia, departamento, nivel_riesgo
               FROM distritos WHERE geom&&{bbox} LIMIT 200""", [], ["nombre","provincia","departamento","nivel_riesgo"]),
        "precipitaciones": (f"""SELECT {geom_poly} AS geom_json, nombre, tipo, nivel_riesgo_inundacion,
               ROUND(precipitacion_anual_mm::NUMERIC,1) AS precipitacion_anual_mm,
               ROUND(indice_fen::NUMERIC,2) AS indice_fen
               FROM zonas_precipitacion WHERE geom&&{bbox}""", [],
               ["nombre","tipo","nivel_riesgo_inundacion","precipitacion_anual_mm","indice_fen"]),
        "volcanes": (f"""SELECT ST_AsGeoJSON(geom,6)::TEXT AS geom_json,
               nombre, estado, altitud_m, region
               FROM volcanes WHERE geom&&{bbox}""", [], ["nombre","estado","altitud_m","region"]),
    }

    for capa in capas_list:
        if capa in queries:
            sql, params, props = queries[capa]
            rows = await pool.fetch(sql, *params)
            resultado[capa] = {"type":"FeatureCollection","features":rows_to_features(rows,props)}

    resultado["_meta"] = {
        "bbox": [min_lon, min_lat, max_lon, max_lat],
        "capas_solicitadas": capas_list,
        "capas_devueltas": [k for k in resultado if not k.startswith("_")],
        "zoom": zoom,
    }
    return resultado


# ── Resumen / Riesgo punto / Sync ─────────────────────────────────

@app.get("/api/v1/resumen", tags=["Sistema"])
async def get_resumen():
    pool = await db()
    stats = await pool.fetchrow("""
        SELECT COUNT(*) AS total_sismos,
               ROUND(MAX(magnitud)::NUMERIC,1) AS max_magnitud,
               ROUND(AVG(magnitud)::NUMERIC,2) AS avg_magnitud,
               COUNT(*) FILTER (WHERE magnitud>=7.0) AS m7_plus,
               COUNT(*) FILTER (WHERE fecha>=CURRENT_DATE-INTERVAL '30 days') AS ultimos_30d,
               COUNT(*) FILTER (WHERE fecha>=CURRENT_DATE-INTERVAL '7 days')  AS ultimos_7d,
               MIN(fecha)::TEXT AS desde, MAX(fecha)::TEXT AS hasta
        FROM sismos
    """)
    ultimos = await pool.fetch("""
        SELECT usgs_id, magnitud, fecha::TEXT, lugar,
               COALESCE(region, f_asignar_region(ST_X(geom),ST_Y(geom))) AS region,
               profundidad_km, tipo_profundidad
        FROM sismos WHERE magnitud>=4.0
        ORDER BY fecha DESC, magnitud DESC LIMIT 10
    """)
    capas = await pool.fetchrow("""
        SELECT (SELECT COUNT(*) FROM departamentos) AS departamentos,
               (SELECT COUNT(*) FROM distritos)     AS distritos,
               (SELECT COUNT(*) FROM fallas)        AS fallas,
               (SELECT COUNT(*) FROM zonas_inundables) AS inundaciones,
               (SELECT COUNT(*) FROM zonas_tsunami)    AS tsunamis,
               (SELECT COUNT(*) FROM deslizamientos)   AS deslizamientos,
               (SELECT COUNT(*) FROM infraestructura)  AS infraestructura,
               (SELECT COUNT(*) FROM estaciones)       AS estaciones,
               (SELECT COUNT(*) FROM zonas_precipitacion) AS precipitaciones,
               (SELECT COUNT(*) FROM eventos_fen)      AS eventos_fen,
               (SELECT COUNT(*) FROM volcanes)         AS volcanes,
               (SELECT COUNT(*) FROM alertas_rt)       AS alertas_rt
    """)
    return {
        "sismos": _serialize_row(stats),
        "ultimos_significativos": [_serialize_row(r) for r in ultimos],
        "capas": _serialize_row(capas),
    }


@app.get("/api/v1/riesgo", tags=["Espacial"])
async def get_riesgo_punto(
    lon: float = Query(..., ge=-82,   le=-68),
    lat: float = Query(..., ge=-18.5, le=0),
):
    """Evaluación completa de riesgo para un punto (f_riesgo_punto v8.0)."""
    pool = await db()
    row = await pool.fetchrow("SELECT f_riesgo_punto($1,$2) AS resultado", lon, lat)
    return row["resultado"]


@app.get("/api/v1/diagnostico/regiones", tags=["Sistema"])
async def diagnostico_regiones_v2():
    pool = await db()
    resultado = {}
    for tabla in ["sismos","infraestructura","estaciones","fallas","volcanes"]:
        try:
            row = await pool.fetchrow(f"""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE region IS NOT NULL) AS con_region,
                       COUNT(*) FILTER (WHERE region IS NULL) AS sin_region,
                       COUNT(DISTINCT region) AS regiones_distintas
                FROM {tabla}
            """)
            resultado[tabla] = _serialize_row(row)
        except Exception:
            resultado[tabla] = {"error": "tabla no disponible"}
    return resultado


@app.get("/api/v1/sync/log", tags=["Sistema"])
async def get_sync_log(limit: int = Query(20, ge=1, le=100)):
    pool = await db()
    rows = await pool.fetch("""
        SELECT fuente, tabla, registros, estado, detalle,
               duracion_s, inicio::TEXT, fin::TEXT
        FROM sync_log ORDER BY fin DESC NULLS FIRST LIMIT $1
    """, limit)
    return [_serialize_row(r) for r in rows]


@app.get("/api/v1/sync/status", tags=["Sistema"])
async def get_sync_status():
    pool = await db()
    rows = await pool.fetch("""
        SELECT DISTINCT ON (tabla)
               fuente, tabla, registros, estado, fin::TEXT AS ultima_sync
        FROM sync_log WHERE fin IS NOT NULL ORDER BY tabla, fin DESC
    """)
    return [_serialize_row(r) for r in rows]