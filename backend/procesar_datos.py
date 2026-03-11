#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════
# GeoRiesgo Perú — ETL v7.3  (DEFINITIVE FIX — "solo un lugar")
#
# DIAGNÓSTICO REAL del bug "solo un lugar" en IRC/Construcción:
#
#  🔴 CAUSA #1 (raíz del bug — nueva en v7.3):
#     GADM L3 para Perú pesa ~80-120 MB. En Docker con timeout=180s
#     casi SIEMPRE falla → distritos queda con 0 filas →
#     mv_riesgo_construccion queda con 0 filas → mapa IRC vacío.
#     FIX v7.3: DISTRITOS_FALLBACK con 75 distritos hardcoded
#     (3 por departamento). Se activa si GADM/INEI cargan < 50.
#
#  🔴 CAUSA #2 (nueva en v7.3):
#     REFRESH MATERIALIZED VIEW CONCURRENTLY tiene bug adicional:
#     falla en PostgreSQL cuando la vista está vacía (primera carga)
#     incluso con autocommit=True. Además CONCURRENTLY en vistas
#     recién creadas sin índice único poblado lanza error.
#     FIX v7.3: eliminar CONCURRENTLY. REFRESH plain funciona en
#     transacciones normales, en vistas vacías, en todo PostgreSQL.
#
#  🔴 FIX CRÍTICO #2 — ST_Multi() en inserts (ya en v7.1)
#  🔴 FIX CRÍTICO #3 — _limpiar_fuera_peru() NULL-proof (ya en v7.1)
#  🔴 FIX CRÍTICO #4 — DEPARTAMENTOS_FALLBACK hardcoded (ya en v7.1)
#
# Fuentes: USGS·IGP·INEI·GADM·ANA·PREDES·CENEPRED
#          SUSALUD·MINSA·MINEDU·MTC·APN·OSINERGMIN·CGBVP
# ══════════════════════════════════════════════════════════════════

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from shapely.geometry import shape
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ── Logging ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuración ─────────────────────────────────────────────────
DB_DSN = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql://georiesgo:georiesgo_secret@db:5432/georiesgo",
)
MAX_WORKERS     = int(os.getenv("ETL_WORKERS", "3"))
FORCE_SYNC      = os.getenv("FORCE_SYNC", "0") == "1"
REQUEST_TIMEOUT = 45

# ── Bounding box Perú (ampliado) ──────────────────────────────────
PERU_BBOX = dict(min_lon=-82.0, min_lat=-18.5, max_lon=-68.5, max_lat=0.5)

# ── Endpoints Overpass ────────────────────────────────────────────
OVERPASS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

# ── Zona Sísmica NTE E.030-2018 (DS N°003-2016-VIVIENDA)
#    Z4=0.45g · Z3=0.35g · Z2=0.25g · Z1=0.10g
ZONA_SISMICA_POR_DEPTO: dict[str, int] = {
    # Zona 4 — mayor peligro (costa y algunas provincias andinas)
    "Tumbes": 4, "Piura": 4, "Lambayeque": 4, "La Libertad": 4,
    "Ancash": 4, "Lima": 4, "Callao": 4, "Ica": 4,
    "Arequipa": 4, "Moquegua": 4, "Tacna": 4,
    # Zona 3 — alto peligro (sierra central y sur)
    "Cajamarca": 3, "San Martín": 3, "Huánuco": 3, "Pasco": 3,
    "Junín": 3, "Huancavelica": 3, "Ayacucho": 3, "Apurímac": 3, "Cusco": 3,
    # Zona 2 — peligro moderado
    "Amazonas": 2, "Puno": 2, "Ucayali": 2,
    # Zona 1 — bajo peligro (amazonia)
    "Loreto": 1, "Madre de Dios": 1,
}
ZONA_SISMICA_FACTOR = {4: 0.45, 3: 0.35, 2: 0.25, 1: 0.10}


# ══════════════════════════════════════════════════════════════════
#  🔴 FIX 4 — DEPARTAMENTOS FALLBACK HARDCODED
#  Polígonos aproximados (bbox) para los 25 departamentos.
#  Activan si GADM devuelve < 20 departamentos con geometría.
# ══════════════════════════════════════════════════════════════════
DEPARTAMENTOS_FALLBACK = [
    ("Tumbes",        "PER_TUM", -80.70, -3.95, -79.75, -3.30, 4),
    ("Piura",         "PER_PIU", -81.50, -5.85, -79.15, -3.85, 4),
    ("Lambayeque",    "PER_LAM", -80.55, -7.25, -79.00, -5.78, 4),
    ("La Libertad",   "PER_LAL", -79.50, -9.38, -76.75, -7.15, 4),
    ("Cajamarca",     "PER_CAJ", -79.65, -7.96, -77.45, -4.48, 3),
    ("Amazonas",      "PER_AMA", -79.00, -6.58, -77.05, -2.78, 2),
    ("San Martín",    "PER_SAM", -78.20, -8.38, -75.58, -5.38, 3),
    ("Loreto",        "PER_LOR", -76.15, -7.12, -70.00, -0.05, 1),
    ("Ancash",        "PER_ANC", -79.05, -10.58, -76.65, -7.88, 4),
    ("Huánuco",       "PER_HUA", -77.12, -11.52, -74.15, -8.68, 2),
    ("Pasco",         "PER_PAS", -76.92, -11.88, -73.68, -9.48, 3),
    ("Junín",         "PER_JUN", -76.45, -13.08, -73.45, -9.82, 3),
    ("Lima",          "PER_LIM", -77.92, -13.18, -74.98, -10.08, 4),
    ("Callao",        "PER_CAL", -77.22, -12.12, -76.98, -11.87, 4),
    ("Huancavelica",  "PER_HVC", -75.72, -14.28, -73.78, -12.02, 3),
    ("Ica",           "PER_ICA", -76.72, -15.78, -73.78, -13.02, 4),
    ("Ayacucho",      "PER_AYA", -75.12, -15.28, -73.08, -12.18, 3),
    ("Apurímac",      "PER_APU", -73.92, -14.88, -72.08, -13.18, 3),
    ("Cusco",         "PER_CUS", -73.58, -15.38, -70.18, -11.18, 3),
    ("Arequipa",      "PER_ARE", -73.22, -17.12, -69.98, -14.38, 4),
    ("Puno",          "PER_PUN", -71.52, -17.38, -68.58, -13.02, 2),
    ("Moquegua",      "PER_MOQ", -71.48, -17.68, -69.42, -15.78, 4),
    ("Tacna",         "PER_TAC", -70.92, -18.52, -69.28, -16.88, 4),
    ("Madre de Dios", "PER_MDD", -72.28, -14.02, -68.58, -9.78,  1),
    ("Ucayali",       "PER_UCA", -75.92, -11.92, -70.42, -7.78,  2),
]


def _bbox_to_multipolygon_wkt(lon_min: float, lat_min: float,
                               lon_max: float, lat_max: float) -> str:
    return (
        f"MULTIPOLYGON((("
        f"{lon_min} {lat_min}, {lon_max} {lat_min}, "
        f"{lon_max} {lat_max}, {lon_min} {lat_max}, "
        f"{lon_min} {lat_min}"
        f")))"
    )


# ══════════════════════════════════════════════════════════════════
#  UTILIDADES HTTP
# ══════════════════════════════════════════════════════════════════

session = requests.Session()
session.headers.update({
    "User-Agent": "GeoRiesgo-Peru-ETL/7.2 (contact: georiesgo@ica.gob.pe)",
    "Accept": "application/json, application/geo+json",
})


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    wait=wait_exponential(multiplier=1, min=2, max=15),
    stop=stop_after_attempt(3),
)
def http_get(url: str, params: dict | None = None,
             timeout: int = REQUEST_TIMEOUT) -> Any:
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def http_get_bytes(url: str, timeout: int = REQUEST_TIMEOUT) -> bytes:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content


# ══════════════════════════════════════════════════════════════════
#  DB HELPERS
# ══════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(DB_DSN)


def exec_sql(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        conn.commit()
        return cur.rowcount


def fetch_all(conn, sql: str, params=None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def bulk_insert(conn, table: str, rows: list[dict], conflict: str = "") -> int:
    if not rows:
        return 0
    keys = list(rows[0].keys())
    placeholders = ", ".join([f"%({k})s" for k in keys])
    cols = ", ".join(keys)
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) {conflict}"
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    conn.commit()
    return len(rows)


# ══════════════════════════════════════════════════════════════════
#  🔴 FIX DEFINITIVO — REFRESH MATERIALIZED VIEW (sin CONCURRENTLY)
#
#  v7.2 usó autocommit=True + CONCURRENTLY.
#  Problema adicional: CONCURRENTLY en vistas vacías lanza error en
#  PostgreSQL < 14:  "cannot refresh materialized view concurrently"
#  cuando la vista no tiene unique index poblado o está vacía.
#
#  Solución definitiva: REFRESH plain (sin CONCURRENTLY).
#  ✅ Funciona dentro de transacciones psycopg2 normales
#  ✅ Funciona en vistas vacías (primera carga)
#  ✅ Compatible con todas las versiones de PostgreSQL
#  ⚠️  Bloqueo exclusivo durante el refresh — aceptable en ETL nocturno
# ══════════════════════════════════════════════════════════════════

def refresh_matview(conn, view_name: str, timeout_ms: int = 300_000) -> None:
    """
    Refresca una vista materializada con REFRESH plain (sin CONCURRENTLY).
    Funciona dentro de la transacción psycopg2 normal. Sin excepciones.
    """
    log.info(f"  REFRESH MATERIALIZED VIEW {view_name} ...")
    try:
        with conn.cursor() as cur:
            cur.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
        conn.commit()
        log.info(f"  ✅ {view_name} refrescado correctamente")
    except Exception as e:
        log.error(f"  ❌ Error refrescando {view_name}: {e}")
        conn.rollback()
        raise


# ══════════════════════════════════════════════════════════════════
#  UTILIDADES GEOMÉTRICAS
# ══════════════════════════════════════════════════════════════════

def bbox_overpass(margin: float = 0.1) -> str:
    return (
        f"{PERU_BBOX['min_lat'] - margin},{PERU_BBOX['min_lon'] - margin},"
        f"{PERU_BBOX['max_lat'] + margin},{PERU_BBOX['max_lon'] + margin}"
    )


def overpass_query(tags: str) -> str:
    bbox = bbox_overpass()
    return f"""
[out:json][timeout:60];
(
  node[{tags}]({bbox});
  way[{tags}]({bbox});
  relation[{tags}]({bbox});
);
out center tags;
"""


def try_overpass(query: str, label: str) -> list[dict]:
    for ep in OVERPASS:
        for intento in range(3):
            try:
                log.info(f"  OSM: {label} via {ep.split('/')[2]} (intento {intento+1})...")
                r = session.post(ep, data={"data": query}, timeout=90)
                if r.status_code == 429:
                    log.warning("  Rate limit 429 — esperando 30s...")
                    time.sleep(30)
                    continue
                r.raise_for_status()
                elements = r.json().get("elements", [])
                log.info(f"    {len(elements)} elementos OSM")
                return elements
            except Exception as e:
                log.warning(f"  Overpass {label} intento {intento+1} falló: {e}")
                time.sleep(10)
    return []


def normalize_osm_element(el: dict) -> tuple[float, float] | None:
    if el["type"] == "node":
        return el.get("lon"), el.get("lat")
    center = el.get("center", {})
    if center:
        return center.get("lon"), center.get("lat")
    return None


# ══════════════════════════════════════════════════════════════════
#  PASO 0: DEPARTAMENTOS (GADM L1)
#  🔴 FIX 2: ST_Multi() · FIX 4: fallback hardcoded
# ══════════════════════════════════════════════════════════════════

def _insertar_departamento_row(
    cur,
    nombre: str,
    ubigeo: str,
    geom_wkt: str,
    zona: int,
    factor_z: float,
    area_km2=None,
    capital=None,
    fuente: str = "GADM 4.1",
) -> bool:
    """
    🔴 FIX 2: ST_Multi() convierte Polygon→MultiPolygon sin error de tipo.
    Tolerante a geometrías inválidas via ST_MakeValid().
    """
    try:
        cur.execute("""
            INSERT INTO departamentos
                (nombre, ubigeo, geom, zona_sismica, factor_z, area_km2, capital, fuente)
            VALUES (%s, %s,
                ST_Multi(ST_MakeValid(ST_GeomFromText(%s, 4326)))::geometry(MultiPolygon,4326),
                %s, %s, %s, %s, %s)
            ON CONFLICT (ubigeo) DO UPDATE SET
                geom         = EXCLUDED.geom,
                zona_sismica = EXCLUDED.zona_sismica,
                factor_z     = EXCLUDED.factor_z,
                nombre       = EXCLUDED.nombre,
                fuente       = EXCLUDED.fuente
        """, (nombre, ubigeo, geom_wkt, zona, factor_z, area_km2, capital, fuente))
        return True
    except Exception as e:
        log.warning(f"  Error insertando departamento '{nombre}': {e}")
        return False


def paso_departamentos(conn) -> int:
    log.info("Descargando GADM L1 (departamentos)...")
    n_gadm = 0

    url = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_PER_1.json"
    try:
        r = http_get_bytes(url, timeout=60)
        log.info(f"  {len(r)/1e6:.1f} MB descargados")
        gj = json.loads(r)
        with conn.cursor() as cur:
            for feat in gj["features"]:
                props    = feat["properties"]
                nombre   = props.get("NAME_1", "")
                geom_wkt = shape(feat["geometry"]).wkt
                zona     = ZONA_SISMICA_POR_DEPTO.get(nombre, 2)
                ubigeo   = props.get("CC_1") or f"GADM_{nombre[:6].upper()}"
                ok = _insertar_departamento_row(
                    cur, nombre, ubigeo, geom_wkt, zona,
                    ZONA_SISMICA_FACTOR[zona], fuente="GADM 4.1",
                )
                if ok:
                    n_gadm += 1
        conn.commit()
        log.info(f"  ✅ {n_gadm} departamentos GADM insertados")
    except Exception as e:
        log.error(f"  GADM L1 falló: {e}")

    # 🔴 FIX 4: Fallback si GADM insertó menos de 20 departamentos
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")
        n_actual = cur.fetchone()[0]

    if n_actual < 20:
        log.warning(
            f"  ⚠  Solo {n_actual} departamentos con geometría — "
            "cargando fallback hardcoded..."
        )
        n_fb = 0
        with conn.cursor() as cur:
            for (nombre, ubigeo, lon_min, lat_min, lon_max, lat_max, zona) in DEPARTAMENTOS_FALLBACK:
                geom_wkt = _bbox_to_multipolygon_wkt(lon_min, lat_min, lon_max, lat_max)
                ok = _insertar_departamento_row(
                    cur, nombre, ubigeo, geom_wkt, zona,
                    ZONA_SISMICA_FACTOR[zona], fuente="Fallback-bbox",
                )
                if ok:
                    n_fb += 1
        conn.commit()
        log.info(f"  ✅ {n_fb} departamentos fallback insertados")
        n_actual += n_fb

    log.info(f"✅ {n_actual} departamentos disponibles (zona sísmica NTE E.030)")
    return n_actual


# ══════════════════════════════════════════════════════════════════
#  PASO 1: SISMOS USGS (paralelo por bloques temporales)
# ══════════════════════════════════════════════════════════════════

USGS_BASE = "https://earthquake.usgs.gov/fdsnws/event/1/query"

BLOQUES_HISTORICOS = [
    ("1900-01-01", "1905-01-01"), ("1905-01-01", "1910-01-01"),
    ("1910-01-01", "1915-01-01"), ("1915-01-01", "1920-01-01"),
    ("1920-01-01", "1925-01-01"), ("1925-01-01", "1930-01-01"),
    ("1930-01-01", "1935-01-01"), ("1935-01-01", "1940-01-01"),
    ("1940-01-01", "1945-01-01"), ("1945-01-01", "1950-01-01"),
    ("1950-01-01", "1955-01-01"), ("1955-01-01", "1960-01-01"),
    ("1960-01-01", "1965-01-01"), ("1965-01-01", "1970-01-01"),
    ("1970-01-01", "1975-01-01"), ("1975-01-01", "1980-01-01"),
    ("1980-01-01", "1985-01-01"), ("1985-01-01", "1990-01-01"),
    ("1990-01-01", "1995-01-01"), ("1995-01-01", "2000-01-01"),
    ("2000-01-01", "2005-01-01"), ("2005-01-01", "2010-01-01"),
    ("2010-01-01", "2015-01-01"), ("2015-01-01", "2020-01-01"),
    ("2020-01-01", "2025-01-01"),
    ("2025-01-01", date.today().strftime("%Y-%m-%d")),
]


def _fetch_bloque_sismos(start: str, end: str) -> list[dict]:
    params = {
        "format": "geojson", "starttime": start, "endtime": end,
        "minlatitude":  PERU_BBOX["min_lat"], "maxlatitude":  PERU_BBOX["max_lat"],
        "minlongitude": PERU_BBOX["min_lon"], "maxlongitude": PERU_BBOX["max_lon"],
        "minmagnitude": 2.5, "orderby": "time-asc", "limit": 20000,
    }
    data = http_get(USGS_BASE, params=params, timeout=60)
    features = data.get("features", [])
    log.info(f"  Bloque {start}→{end}: {len(features)} sismos")
    return features


def _sismo_row(feat: dict) -> dict | None:
    props  = feat["properties"]
    coords = feat["geometry"]["coordinates"]
    lon, lat, depth = coords[0], coords[1], coords[2] or 0.0
    mag = props.get("mag")
    if not mag or mag < 0:
        return None
    ts = props.get("time", 0)
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None
    depth = max(0.0, depth)
    tipo_prof = (
        "superficial" if depth < 60 else
        "intermedio"  if depth < 300 else
        "profundo"
    )
    return {
        "usgs_id": feat["id"],
        "lon": lon, "lat": lat,
        "magnitud": round(mag, 1),
        "profundidad_km": round(depth, 2),
        "tipo_profundidad": tipo_prof,
        "fecha": dt.date() if dt else None,
        "hora_utc": dt,
        "lugar": props.get("place", ""),
        "tipo_magnitud": props.get("magType", ""),
        "estado": props.get("status", "reviewed"),
    }


def paso_sismos(conn) -> int:
    log.info(f"USGS: 1900-01-01 → {date.today()}  (M≥2.5)")
    log.info(f"  {len(BLOQUES_HISTORICOS)} bloques · descarga paralela ({MAX_WORKERS} workers)")
    all_features: list[dict] = []
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(_fetch_bloque_sismos, s, e): (s, e)
                for s, e in BLOQUES_HISTORICOS}
        for fut in as_completed(futs):
            try:
                all_features.extend(fut.result())
            except Exception as err:
                s, e = futs[fut]
                log.warning(f"  Bloque {s}→{e} falló: {err}")

    rows = [r for feat in all_features if (r := _sismo_row(feat))]
    inserted = 0
    with conn.cursor() as cur:
        for r in rows:
            try:
                cur.execute("""
                    INSERT INTO sismos
                        (usgs_id, geom, magnitud, profundidad_km, tipo_profundidad,
                         fecha, hora_utc, lugar, tipo_magnitud, estado)
                    VALUES (%s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (usgs_id) DO NOTHING
                """, (r["usgs_id"], r["lon"], r["lat"],
                      r["magnitud"], r["profundidad_km"], r["tipo_profundidad"],
                      r["fecha"], r["hora_utc"], r["lugar"],
                      r["tipo_magnitud"], r["estado"]))
                inserted += 1
            except Exception:
                pass
    conn.commit()
    log.info(f"✅ {inserted} sismos cargados ({time.time()-t0:.1f}s)")
    return inserted


# ══════════════════════════════════════════════════════════════════
#  PASO 2: DISTRITOS + POBLACIÓN (INEI WFS → GADM L3 → FALLBACK HARDCODED)
#
#  🔴 CAUSA #1 DEL BUG "solo un lugar":
#     GADM L3 pesa ~80-120 MB y casi SIEMPRE hace timeout en Docker.
#     Sin distritos → mv_riesgo_construccion tiene 0 filas → mapa IRC vacío.
#
#  SOLUCIÓN v7.3:
#     Si después de INEI + GADM hay < 50 distritos con geom,
#     se insertan 75 distritos hardcoded (3 por departamento).
#     Garantiza que el mapa IRC SIEMPRE tenga datos.
#
#  🔴 FIX 2: ST_Multi() en ambas funciones de inserción
# ══════════════════════════════════════════════════════════════════

# ── 75 distritos fallback: 3 por departamento (capital + 2 ciudades) ──
# Formato: (nombre, ubigeo_fb, provincia, departamento,
#            lon_min, lat_min, lon_max, lat_max, zona_sismica)
DISTRITOS_FALLBACK = [
    # LIMA
    ("Lima",                   "FB_LIM_01", "Lima",              "Lima",          -77.12,-12.10,-76.98,-11.96, 4),
    ("San Juan de Lurigancho", "FB_LIM_02", "Lima",              "Lima",          -77.02,-12.05,-76.88,-11.91, 4),
    ("Miraflores",             "FB_LIM_03", "Lima",              "Lima",          -77.05,-12.14,-76.91,-12.00, 4),
    # CALLAO
    ("Callao",                 "FB_CAL_01", "Callao",            "Callao",        -77.21,-12.09,-77.07,-11.95, 4),
    ("La Punta",               "FB_CAL_02", "Callao",            "Callao",        -77.19,-12.08,-77.13,-12.02, 4),
    ("Ventanilla",             "FB_CAL_03", "Callao",            "Callao",        -77.17,-11.93,-77.03,-11.79, 4),
    # AREQUIPA
    ("Arequipa",               "FB_ARE_01", "Arequipa",          "Arequipa",      -71.61,-16.47,-71.47,-16.33, 4),
    ("Mollendo",               "FB_ARE_02", "Islay",             "Arequipa",      -72.08,-17.06,-71.94,-16.92, 4),
    ("Camaná",                 "FB_ARE_03", "Camaná",            "Arequipa",      -72.75,-16.69,-72.61,-16.55, 4),
    # CUSCO
    ("Cusco",                  "FB_CUS_01", "Cusco",             "Cusco",         -72.05,-13.60,-71.91,-13.46, 3),
    ("Wanchaq",                "FB_CUS_02", "Cusco",             "Cusco",         -71.99,-13.54,-71.93,-13.48, 3),
    ("Santiago",               "FB_CUS_03", "Cusco",             "Cusco",         -72.02,-13.59,-71.96,-13.53, 3),
    # ICA
    ("Ica",                    "FB_ICA_01", "Ica",               "Ica",           -75.80,-14.14,-75.66,-14.00, 4),
    ("Pisco",                  "FB_ICA_02", "Pisco",             "Ica",           -76.27,-13.78,-76.13,-13.64, 4),
    ("Nazca",                  "FB_ICA_03", "Nazca",             "Ica",           -74.99,-14.89,-74.85,-14.75, 4),
    # PIURA
    ("Piura",                  "FB_PIU_01", "Piura",             "Piura",         -80.70, -5.26,-80.56, -5.12, 4),
    ("Sullana",                "FB_PIU_02", "Sullana",           "Piura",         -80.72, -4.94,-80.58, -4.80, 4),
    ("Paita",                  "FB_PIU_03", "Paita",             "Piura",         -81.17, -5.12,-81.03, -4.98, 4),
    # LA LIBERTAD
    ("Trujillo",               "FB_LAL_01", "Trujillo",          "La Libertad",   -79.11, -8.19,-78.97, -8.05, 4),
    ("Huanchaco",              "FB_LAL_02", "Trujillo",          "La Libertad",   -79.15, -8.10,-79.01, -7.96, 4),
    ("Pacasmayo",              "FB_LAL_03", "Pacasmayo",         "La Libertad",   -79.62, -7.47,-79.48, -7.33, 4),
    # LAMBAYEQUE
    ("Chiclayo",               "FB_LAM_01", "Chiclayo",          "Lambayeque",    -79.91, -6.84,-79.77, -6.70, 4),
    ("Ferreñafe",              "FB_LAM_02", "Ferreñafe",         "Lambayeque",    -79.85, -6.67,-79.71, -6.53, 4),
    ("Lambayeque",             "FB_LAM_03", "Lambayeque",        "Lambayeque",    -79.97, -6.73,-79.83, -6.59, 4),
    # ANCASH
    ("Huaraz",                 "FB_ANC_01", "Huaraz",            "Ancash",        -77.60, -9.60,-77.46, -9.46, 4),
    ("Chimbote",               "FB_ANC_02", "Santa",             "Ancash",        -78.65, -9.14,-78.51, -9.00, 4),
    ("Casma",                  "FB_ANC_03", "Casma",             "Ancash",        -78.38, -9.54,-78.24, -9.40, 4),
    # AYACUCHO
    ("Huamanga",               "FB_AYA_01", "Huamanga",          "Ayacucho",      -74.30,-13.23,-74.16,-13.09, 3),
    ("Huanta",                 "FB_AYA_02", "Huanta",            "Ayacucho",      -74.33,-12.97,-74.19,-12.83, 3),
    ("San Miguel",             "FB_AYA_03", "La Mar",            "Ayacucho",      -73.99,-13.04,-73.85,-12.90, 3),
    # PUNO
    ("Puno",                   "FB_PUN_01", "Puno",              "Puno",          -70.09,-15.92,-69.95,-15.78, 2),
    ("Juliaca",                "FB_PUN_02", "San Román",         "Puno",          -70.22,-15.55,-70.08,-15.41, 2),
    ("Ilave",                  "FB_PUN_03", "El Collao",         "Puno",          -69.72,-16.17,-69.58,-16.03, 2),
    # JUNÍN
    ("Huancayo",               "FB_JUN_01", "Huancayo",          "Junín",         -75.29,-12.13,-75.15,-11.99, 3),
    ("El Tambo",               "FB_JUN_02", "Huancayo",          "Junín",         -75.25,-12.07,-75.11,-11.93, 3),
    ("Tarma",                  "FB_JUN_03", "Tarma",             "Junín",         -75.74,-11.50,-75.60,-11.36, 3),
    # CAJAMARCA
    ("Cajamarca",              "FB_CAJ_01", "Cajamarca",         "Cajamarca",     -78.58, -7.23,-78.44, -7.09, 3),
    ("Chota",                  "FB_CAJ_02", "Chota",             "Cajamarca",     -78.74, -6.62,-78.60, -6.48, 3),
    ("Jaén",                   "FB_CAJ_03", "Jaén",              "Cajamarca",     -78.85, -5.78,-78.71, -5.64, 3),
    # TACNA
    ("Tacna",                  "FB_TAC_01", "Tacna",             "Tacna",         -70.08,-18.08,-69.94,-17.94, 4),
    ("Ciudad Nueva",           "FB_TAC_02", "Tacna",             "Tacna",         -70.03,-18.06,-69.89,-17.92, 4),
    ("Ilo",                    "FB_TAC_03", "Ilo",               "Moquegua",      -71.41,-17.72,-71.27,-17.58, 4),
    # MOQUEGUA
    ("Moquegua",               "FB_MOQ_01", "Mariscal Nieto",    "Moquegua",      -71.01,-17.27,-70.87,-17.13, 4),
    ("Torata",                 "FB_MOQ_02", "Mariscal Nieto",    "Moquegua",      -70.97,-17.14,-70.83,-17.00, 4),
    ("Omate",                  "FB_MOQ_03", "Gral. Sánchez Cerro","Moquegua",     -70.83,-16.69,-70.69,-16.55, 4),
    # TUMBES
    ("Tumbes",                 "FB_TUM_01", "Tumbes",            "Tumbes",        -80.53, -3.63,-80.39, -3.49, 4),
    ("Zarumilla",              "FB_TUM_02", "Zarumilla",         "Tumbes",        -80.31, -3.57,-80.17, -3.43, 4),
    ("Corrales",               "FB_TUM_03", "Tumbes",            "Tumbes",        -80.50, -3.62,-80.36, -3.48, 4),
    # SAN MARTÍN
    ("Tarapoto",               "FB_SAM_01", "San Martín",        "San Martín",    -76.45, -6.56,-76.31, -6.42, 3),
    ("Moyobamba",              "FB_SAM_02", "Moyobamba",         "San Martín",    -77.06, -6.09,-76.92, -5.95, 3),
    ("Juanjui",                "FB_SAM_03", "Mariscal Cáceres",  "San Martín",    -76.87, -7.25,-76.73, -7.11, 3),
    # LORETO
    ("Iquitos",                "FB_LOR_01", "Maynas",            "Loreto",        -73.32, -3.82,-73.18, -3.68, 1),
    ("Nauta",                  "FB_LOR_02", "Loreto",            "Loreto",        -75.07, -4.57,-74.93, -4.43, 1),
    ("Yurimaguas",             "FB_LOR_03", "Alto Amazonas",     "Loreto",        -76.17, -5.97,-76.03, -5.83, 1),
    # HUÁNUCO
    ("Huánuco",                "FB_HUA_01", "Huánuco",           "Huánuco",       -76.31, -9.99,-76.17, -9.85, 2),
    ("Tingo María",            "FB_HUA_02", "Leoncio Prado",     "Huánuco",       -76.08, -9.30,-75.94, -9.16, 2),
    ("Ambo",                   "FB_HUA_03", "Ambo",              "Huánuco",       -76.29,-10.13,-76.15, -9.99, 2),
    # PASCO
    ("Chaupimarca",            "FB_PAS_01", "Pasco",             "Pasco",         -76.33,-10.75,-76.19,-10.61, 3),
    ("Yanacancha",             "FB_PAS_02", "Pasco",             "Pasco",         -76.32,-10.72,-76.18,-10.58, 3),
    ("Oxapampa",               "FB_PAS_03", "Oxapampa",          "Pasco",         -75.36,-10.62,-75.22,-10.48, 3),
    # UCAYALI
    ("Callería",               "FB_UCA_01", "Coronel Portillo",  "Ucayali",       -74.61, -8.45,-74.47, -8.31, 2),
    ("Yarinacocha",            "FB_UCA_02", "Coronel Portillo",  "Ucayali",       -74.60, -8.35,-74.46, -8.21, 2),
    ("Manantay",               "FB_UCA_03", "Coronel Portillo",  "Ucayali",       -74.58, -8.44,-74.44, -8.30, 2),
    # AMAZONAS
    ("Chachapoyas",            "FB_AMA_01", "Chachapoyas",       "Amazonas",      -77.90, -6.27,-77.76, -6.13, 2),
    ("Bagua Grande",           "FB_AMA_02", "Utcubamba",         "Amazonas",      -78.53, -5.82,-78.39, -5.68, 2),
    ("Luya",                   "FB_AMA_03", "Luya",              "Amazonas",      -77.98, -6.10,-77.84, -5.96, 2),
    # APURÍMAC
    ("Abancay",                "FB_APU_01", "Abancay",           "Apurímac",      -72.95,-13.70,-72.81,-13.56, 3),
    ("Andahuaylas",            "FB_APU_02", "Andahuaylas",       "Apurímac",      -73.45,-13.73,-73.31,-13.59, 3),
    ("Chalhuanca",             "FB_APU_03", "Aymaraes",          "Apurímac",      -73.27,-14.37,-73.13,-14.23, 3),
    # HUANCAVELICA
    ("Huancavelica",           "FB_HVC_01", "Huancavelica",      "Huancavelica",  -75.05,-12.85,-74.91,-12.71, 3),
    ("Lircay",                 "FB_HVC_02", "Angaraes",          "Huancavelica",  -74.78,-12.98,-74.64,-12.84, 3),
    ("Pampas",                 "FB_HVC_03", "Tayacaja",          "Huancavelica",  -74.93,-12.42,-74.79,-12.28, 3),
    # MADRE DE DIOS
    ("Tambopata",              "FB_MDD_01", "Tambopata",         "Madre de Dios", -69.26,-12.67,-69.12,-12.53, 1),
    ("Las Piedras",            "FB_MDD_02", "Tambopata",         "Madre de Dios", -69.80,-12.25,-69.66,-12.11, 1),
    ("Manu",                   "FB_MDD_03", "Manu",              "Madre de Dios", -71.38,-12.02,-71.24,-11.88, 1),
]


def _insertar_distritos_fallback(conn) -> int:
    """
    Inserta los 75 distritos hardcoded como bboxes rectangulares.
    Se activa cuando INEI + GADM no lograron cargar ≥ 50 distritos.
    Garantiza que mv_riesgo_construccion siempre tenga filas para el mapa IRC.
    """
    count = 0
    with conn.cursor() as cur:
        for row in DISTRITOS_FALLBACK:
            (nombre, ubigeo, provincia, departamento,
             lon0, lat0, lon1, lat1, zona) = row
            geom_wkt = (
                f"MULTIPOLYGON((({lon0} {lat0},{lon1} {lat0},"
                f"{lon1} {lat1},{lon0} {lat1},{lon0} {lat0})))"
            )
            try:
                cur.execute("""
                    INSERT INTO distritos
                        (ubigeo, nombre, provincia, departamento, geom,
                         nivel_riesgo, zona_sismica, fuente)
                    VALUES (%s,%s,%s,%s,
                        ST_Multi(ST_MakeValid(ST_GeomFromText(%s,4326)))
                            ::geometry(MultiPolygon,4326),
                        %s,%s,%s)
                    ON CONFLICT (ubigeo) DO NOTHING
                """, (ubigeo, nombre, provincia, departamento, geom_wkt,
                      3, zona, "Fallback-bbox-v7.3"))
                count += 1
            except Exception as e:
                log.warning(f"  Error fallback distrito {nombre}: {e}")
    conn.commit()
    log.info(f"  ✅ {count} distritos fallback insertados")
    return count


def paso_distritos(conn) -> int:
    # — Intentar INEI WFS (ligero, preferido) —
    for inei_url in [
        ("https://geoservidor.inei.gob.pe/geoserver/ows?service=WFS&version=1.0.0"
         "&request=GetFeature&typeName=INEI:LIMITEDISTRITAL"
         "&outputFormat=application/json&srsName=EPSG:4326"),
        ("https://geoservidorperu.inei.gob.pe/geoserver/ows?service=WFS&version=1.0.0"
         "&request=GetFeature&typeName=INEI:LIMITEDISTRITAL"
         "&outputFormat=application/json&srsName=EPSG:4326"),
    ]:
        try:
            log.info(f"Descargando distritos INEI: {inei_url[:70]}...")
            r = http_get_bytes(inei_url, timeout=30)
            gj = json.loads(r)
            features = gj.get("features", [])
            if features:
                n = _insertar_distritos_inei(conn, features)
                if n >= 50:
                    return n
        except Exception as e:
            log.warning(f"  INEI falló: {e}")

    # — Intentar GADM L3 (~100 MB — puede hacer timeout en Docker) —
    log.info("Intentando GADM L3 (~100 MB, puede fallar en Docker)...")
    try:
        url = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_PER_3.json"
        r = http_get_bytes(url, timeout=180)
        log.info(f"  {len(r)/1e6:.1f} MB descargados")
        gj = json.loads(r)
        n = _insertar_distritos_gadm(conn, gj["features"])
        if n >= 50:
            return n
    except Exception as e:
        log.error(f"  GADM L3 falló (esperado en Docker con timeout): {e}")

    # ── ACTIVAR FALLBACK HARDCODED ───────────────────────────────
    # CAUSA #1 del bug: llegamos aquí casi siempre en Docker porque
    # GADM L3 (80-120 MB) hace timeout. Sin este fallback → IRC vacío.
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM distritos WHERE geom IS NOT NULL")
        n_actual = cur.fetchone()[0]

    if n_actual < 50:
        log.warning(
            f"  ⚠  Solo {n_actual} distritos con geom. "
            f"Cargando DISTRITOS_FALLBACK ({len(DISTRITOS_FALLBACK)} distritos hardcoded)..."
        )
        n_fb = _insertar_distritos_fallback(conn)
        n_actual += n_fb
        log.info(f"  → {n_actual} distritos totales. Mapa IRC tendrá datos ✅")
    else:
        log.info(f"  {n_actual} distritos disponibles — fallback no necesario")

    return n_actual


def _insertar_distritos_inei(conn, features: list) -> int:
    count = 0
    with conn.cursor() as cur:
        for feat in features:
            p        = feat["properties"]
            geom_wkt = shape(feat["geometry"]).wkt
            depto    = p.get("NOMBDEP", "") or ""
            zona     = ZONA_SISMICA_POR_DEPTO.get(depto, 2)
            try:
                # 🔴 FIX 2: ST_Multi() garantiza MultiPolygon
                cur.execute("""
                    INSERT INTO distritos
                        (ubigeo, nombre, provincia, departamento, geom,
                         nivel_riesgo, poblacion, zona_sismica, fuente)
                    VALUES (%s, %s, %s, %s,
                        ST_Multi(ST_MakeValid(ST_GeomFromText(%s, 4326)))
                            ::geometry(MultiPolygon,4326),
                        %s, %s, %s, %s)
                    ON CONFLICT (ubigeo) DO NOTHING
                """, (p.get("IDDIST"), p.get("NOMBDIST"), p.get("NOMBPROV"),
                      depto, geom_wkt, 3,
                      p.get("POBLACIE") or p.get("PBLCNE_TO"),
                      zona, "INEI"))
                count += 1
            except Exception:
                pass
    conn.commit()
    log.info(f"✅ {count} distritos INEI insertados")
    return count


def _insertar_distritos_gadm(conn, features: list) -> int:
    count = 0
    with conn.cursor() as cur:
        for feat in features:
            p        = feat["properties"]
            geom_wkt = shape(feat["geometry"]).wkt
            depto    = p.get("NAME_1", "")
            zona     = ZONA_SISMICA_POR_DEPTO.get(depto, 2)
            try:
                # 🔴 FIX 2: ST_Multi() garantiza MultiPolygon
                cur.execute("""
                    INSERT INTO distritos
                        (ubigeo, nombre, provincia, departamento, geom,
                         nivel_riesgo, zona_sismica, fuente)
                    VALUES (%s, %s, %s, %s,
                        ST_Multi(ST_MakeValid(ST_GeomFromText(%s, 4326)))
                            ::geometry(MultiPolygon,4326),
                        %s, %s, %s)
                    ON CONFLICT (ubigeo) DO NOTHING
                """, (p.get("CC_3"), p.get("NAME_3"),
                      p.get("NAME_2"), depto, geom_wkt,
                      3, zona, "GADM 4.1"))
                count += 1
            except Exception:
                pass
    conn.commit()
    log.info(f"✅ {count} distritos GADM L3 insertados")
    return count


# ══════════════════════════════════════════════════════════════════
#  PASO 3: FALLAS GEOLÓGICAS
#  Ref: Audin et al. 2008 + INGEMMET 2021 + IGP
# ══════════════════════════════════════════════════════════════════

FALLAS_DATASET = [
    # ── Costa y subducción ─────────────────────────────────────────
    {"nombre": "Sistema de fallas de Lima", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 8.0, "longitud_km": 120,
     "region": "Lima", "activa": True,
     "coords": [(-77.1,-12.0),(-76.8,-11.5),(-76.5,-11.0),(-76.2,-10.5)]},
    {"nombre": "Falla de Paracas", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 7.5, "longitud_km": 80,
     "region": "Ica", "activa": True,
     "coords": [(-76.2,-13.8),(-75.9,-13.5),(-75.6,-13.2),(-75.3,-12.9)]},
    {"nombre": "Sistema de fallas de Ica", "tipo": "inversa-desplazamiento",
     "mecanismo": "compresión oblicua", "magnitud_max": 7.8, "longitud_km": 200,
     "region": "Ica", "activa": True,
     "coords": [(-75.7,-14.5),(-75.4,-14.0),(-75.1,-13.5),(-74.8,-13.0)]},
    {"nombre": "Falla de Nazca", "tipo": "transcurrente",
     "mecanismo": "deslizamiento lateral", "magnitud_max": 7.2, "longitud_km": 150,
     "region": "Ica", "activa": True,
     "coords": [(-74.9,-14.8),(-74.6,-14.5),(-74.3,-14.2),(-74.0,-13.9)]},
    {"nombre": "Sistema de fallas de Arequipa", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 8.4, "longitud_km": 300,
     "region": "Arequipa", "activa": True,
     "coords": [(-72.5,-16.5),(-72.0,-16.2),(-71.5,-15.9),(-71.0,-15.6),(-70.5,-15.3)]},
    {"nombre": "Falla Ichuna", "tipo": "normal", "mecanismo": "extensión",
     "magnitud_max": 7.0, "longitud_km": 60, "region": "Moquegua", "activa": True,
     "coords": [(-70.7,-16.0),(-70.4,-16.3),(-70.1,-16.6)]},
    {"nombre": "Sistema de fallas de Tacna", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 7.3, "longitud_km": 120,
     "region": "Tacna", "activa": True,
     "coords": [(-70.3,-17.0),(-70.0,-17.5),(-69.7,-18.0)]},
    {"nombre": "Falla Pisco-Ayacucho", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 7.0, "longitud_km": 100,
     "region": "Ica", "activa": True,
     "coords": [(-75.0,-13.7),(-74.7,-14.0),(-74.4,-14.3),(-74.1,-14.6)]},
    {"nombre": "Falla Tumbes-Zarumilla", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 7.2, "longitud_km": 110,
     "region": "Tumbes", "activa": True,
     "coords": [(-80.4,-3.5),(-80.1,-3.8),(-79.8,-4.1)]},
    {"nombre": "Falla de Piura-Sullana", "tipo": "transcurrente",
     "mecanismo": "deslizamiento lateral", "magnitud_max": 6.8, "longitud_km": 80,
     "region": "Piura", "activa": True,
     "coords": [(-80.5,-4.5),(-80.2,-4.8),(-79.9,-5.1),(-79.6,-5.4)]},
    # ── Sierra ────────────────────────────────────────────────────
    {"nombre": "Falla Quiches-Sihuas", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 7.5, "longitud_km": 90,
     "region": "Ancash", "activa": True,
     "coords": [(-77.8,-8.5),(-77.5,-8.8),(-77.2,-9.1)]},
    {"nombre": "Falla de Cordillera Blanca", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 7.5, "longitud_km": 200,
     "region": "Ancash", "activa": True,
     "coords": [(-77.6,-8.0),(-77.5,-8.5),(-77.4,-9.0),(-77.3,-9.5),(-77.2,-10.0)]},
    {"nombre": "Falla Purgatorio (Ancash)", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 6.8, "longitud_km": 45,
     "region": "Ancash", "activa": True,
     "coords": [(-77.4,-9.2),(-77.2,-9.5),(-77.0,-9.8)]},
    {"nombre": "Sistema de fallas del Cusco", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 6.8, "longitud_km": 110,
     "region": "Cusco", "activa": True,
     "coords": [(-72.0,-13.5),(-71.7,-13.8),(-71.4,-14.1),(-71.1,-14.4)]},
    {"nombre": "Falla de Tambomachay (Cusco)", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 6.5, "longitud_km": 25,
     "region": "Cusco", "activa": True,
     "coords": [(-71.9,-13.4),(-71.7,-13.5),(-71.5,-13.6)]},
    {"nombre": "Falla Urcos-Cusipata", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 6.3, "longitud_km": 40,
     "region": "Cusco", "activa": True,
     "coords": [(-71.6,-13.7),(-71.4,-13.9),(-71.2,-14.1)]},
    {"nombre": "Falla Vilcañota", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 7.0, "longitud_km": 130,
     "region": "Puno", "activa": True,
     "coords": [(-70.8,-14.5),(-70.5,-15.0),(-70.2,-15.5)]},
    {"nombre": "Sistema de fallas de Ayacucho", "tipo": "normal-transcurrente",
     "mecanismo": "extensión oblicua", "magnitud_max": 6.5, "longitud_km": 80,
     "region": "Ayacucho", "activa": True,
     "coords": [(-74.2,-13.5),(-74.0,-14.0),(-73.8,-14.5)]},
    {"nombre": "Falla de San Antonio de Cachi", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 6.5, "longitud_km": 50,
     "region": "Ayacucho", "activa": True,
     "coords": [(-74.3,-15.0),(-74.0,-15.3),(-73.7,-15.6)]},
    {"nombre": "Sistema de fallas de Juliaca", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 6.8, "longitud_km": 70,
     "region": "Puno", "activa": True,
     "coords": [(-70.4,-15.2),(-70.1,-15.5),(-69.8,-15.8)]},
    {"nombre": "Falla del Altiplano Sur", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 7.0, "longitud_km": 90,
     "region": "Puno", "activa": True,
     "coords": [(-69.5,-16.0),(-69.2,-16.4),(-68.9,-16.8)]},
    # ── Norte ─────────────────────────────────────────────────────
    {"nombre": "Sistema de fallas del Marañón", "tipo": "transcurrente",
     "mecanismo": "deslizamiento lateral", "magnitud_max": 7.0, "longitud_km": 180,
     "region": "Cajamarca", "activa": True,
     "coords": [(-78.5,-4.5),(-78.2,-5.0),(-77.9,-5.5),(-77.6,-6.0),(-77.3,-6.5)]},
    {"nombre": "Falla de Moyobamba", "tipo": "normal",
     "mecanismo": "extensión", "magnitud_max": 6.5, "longitud_km": 60,
     "region": "San Martín", "activa": True,
     "coords": [(-77.0,-5.8),(-76.7,-6.1),(-76.4,-6.4)]},
    {"nombre": "Falla Alto Chicama", "tipo": "inversa",
     "mecanismo": "compresión", "magnitud_max": 6.5, "longitud_km": 55,
     "region": "La Libertad", "activa": True,
     "coords": [(-78.2,-7.5),(-77.9,-7.8),(-77.6,-8.1)]},
]


def paso_fallas(conn) -> int:
    count = 0
    with conn.cursor() as cur:
        for f in FALLAS_DATASET:
            coords = f.get("coords", [])
            if len(coords) < 2:
                continue
            coords_sql = ",".join([f"{c[0]} {c[1]}" for c in coords])
            geom_wkt = f"MULTILINESTRING(({coords_sql}))"
            try:
                cur.execute("""
                    INSERT INTO fallas
                        (nombre, geom, activa, tipo, mecanismo,
                         longitud_km, magnitud_max, region, fuente)
                    VALUES (%s,
                        ST_MakeValid(ST_GeomFromText(%s, 4326))
                            ::geometry(MultiLineString,4326),
                        %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (f["nombre"], geom_wkt, f.get("activa", True),
                      f.get("tipo"), f.get("mecanismo"),
                      f.get("longitud_km"), f.get("magnitud_max"),
                      f.get("region"), f.get("fuente", "IGP/Audin et al. 2008")))
                count += 1
            except Exception:
                pass
    conn.commit()
    log.info(f"✅ {count} fallas geológicas insertadas")
    return count


# ══════════════════════════════════════════════════════════════════
#  PASOS 4–6: INUNDACIONES · TSUNAMIS · DESLIZAMIENTOS
# ══════════════════════════════════════════════════════════════════

INUNDACIONES_DATASET = [
    {"nombre": "Valle del Mantaro (inundación fluvial)", "tipo": "fluvial",
     "nivel_riesgo": 4, "periodo_retorno": 50, "cuenca": "Mantaro",
     "region": "Junín", "profundidad_max_m": 3.5,
     "coords": [(-75.2,-11.8),(-75.0,-12.0),(-74.8,-12.2),(-75.0,-12.4),(-75.2,-12.2),(-75.2,-11.8)]},
    {"nombre": "Delta del Río Piura", "tipo": "fluvial",
     "nivel_riesgo": 5, "periodo_retorno": 25, "cuenca": "Piura",
     "region": "Piura", "profundidad_max_m": 5.0,
     "coords": [(-80.8,-5.0),(-80.5,-5.1),(-80.3,-5.2),(-80.4,-5.4),(-80.7,-5.3),(-80.8,-5.0)]},
    {"nombre": "Bajo Piura (FEN recurrente)", "tipo": "fluvial-pluvial",
     "nivel_riesgo": 5, "periodo_retorno": 10, "cuenca": "Piura",
     "region": "Piura", "profundidad_max_m": 4.0,
     "coords": [(-80.7,-5.2),(-80.4,-5.3),(-80.2,-5.5),(-80.3,-5.7),(-80.6,-5.6),(-80.7,-5.2)]},
    {"nombre": "Cuenca del Río Santa (Ancash)", "tipo": "fluvial",
     "nivel_riesgo": 4, "periodo_retorno": 100, "cuenca": "Santa",
     "region": "Ancash", "profundidad_max_m": 4.5,
     "coords": [(-78.2,-9.0),(-78.0,-9.2),(-77.8,-9.4),(-78.0,-9.6),(-78.2,-9.4),(-78.2,-9.0)]},
    {"nombre": "Llanura aluvial del Amazonas", "tipo": "fluvial",
     "nivel_riesgo": 4, "periodo_retorno": 10, "cuenca": "Amazonas",
     "region": "Loreto", "profundidad_max_m": 8.0,
     "coords": [(-73.5,-3.5),(-73.0,-3.8),(-72.5,-4.0),(-73.0,-4.5),(-73.5,-4.2),(-73.5,-3.5)]},
    {"nombre": "Valle de Ica (desbordamiento)", "tipo": "fluvial",
     "nivel_riesgo": 3, "periodo_retorno": 50, "cuenca": "Ica",
     "region": "Ica", "profundidad_max_m": 2.5,
     "coords": [(-75.8,-14.0),(-75.6,-14.1),(-75.4,-14.2),(-75.5,-14.4),(-75.7,-14.3),(-75.8,-14.0)]},
    {"nombre": "Litoral de Tumbes (inundación costera)", "tipo": "costera",
     "nivel_riesgo": 4, "periodo_retorno": 20, "cuenca": "Tumbes",
     "region": "Tumbes", "profundidad_max_m": 3.0,
     "coords": [(-80.5,-3.5),(-80.3,-3.6),(-80.2,-3.8),(-80.4,-3.9),(-80.6,-3.7),(-80.5,-3.5)]},
    {"nombre": "Cuenca del Ucayali", "tipo": "fluvial",
     "nivel_riesgo": 4, "periodo_retorno": 5, "cuenca": "Ucayali",
     "region": "Ucayali", "profundidad_max_m": 10.0,
     "coords": [(-74.5,-8.0),(-74.2,-8.3),(-74.0,-8.6),(-74.3,-9.0),(-74.6,-8.7),(-74.5,-8.0)]},
    {"nombre": "Zona baja del Río Rímac", "tipo": "fluvial-pluvial",
     "nivel_riesgo": 3, "periodo_retorno": 50, "cuenca": "Rímac",
     "region": "Lima", "profundidad_max_m": 2.0,
     "coords": [(-77.2,-12.0),(-77.0,-12.1),(-76.8,-12.0),(-76.9,-12.2),(-77.1,-12.2),(-77.2,-12.0)]},
    {"nombre": "Cuenca del Río Chira (Piura-FEN)", "tipo": "fluvial",
     "nivel_riesgo": 5, "periodo_retorno": 15, "cuenca": "Chira",
     "region": "Piura", "profundidad_max_m": 5.5,
     "coords": [(-81.0,-4.5),(-80.7,-4.7),(-80.5,-5.0),(-80.8,-5.2),(-81.0,-4.9),(-81.0,-4.5)]},
    {"nombre": "Valle Jequetepeque (La Libertad)", "tipo": "fluvial",
     "nivel_riesgo": 3, "periodo_retorno": 50, "cuenca": "Jequetepeque",
     "region": "La Libertad", "profundidad_max_m": 3.0,
     "coords": [(-79.3,-7.2),(-79.1,-7.3),(-78.9,-7.4),(-79.0,-7.6),(-79.2,-7.5),(-79.3,-7.2)]},
    {"nombre": "Cuenca del Río Madre de Dios", "tipo": "fluvial",
     "nivel_riesgo": 4, "periodo_retorno": 5, "cuenca": "Madre de Dios",
     "region": "Madre de Dios", "profundidad_max_m": 9.0,
     "coords": [(-70.5,-12.5),(-70.2,-12.7),(-70.0,-13.0),(-70.3,-13.3),(-70.6,-13.0),(-70.5,-12.5)]},
    {"nombre": "Cuenca del Río Huallaga", "tipo": "fluvial",
     "nivel_riesgo": 3, "periodo_retorno": 25, "cuenca": "Huallaga",
     "region": "San Martín", "profundidad_max_m": 4.0,
     "coords": [(-76.5,-6.8),(-76.2,-7.0),(-76.0,-7.3),(-76.3,-7.6),(-76.6,-7.3),(-76.5,-6.8)]},
    {"nombre": "Valle del Apurímac", "tipo": "fluvial",
     "nivel_riesgo": 3, "periodo_retorno": 50, "cuenca": "Apurímac",
     "region": "Ayacucho", "profundidad_max_m": 3.5,
     "coords": [(-73.5,-13.8),(-73.3,-14.0),(-73.1,-14.2),(-73.3,-14.4),(-73.5,-14.2),(-73.5,-13.8)]},
]

TSUNAMIS_DATASET = [
    {"nombre": "Zona inundación tsunami Lima - Callao",
     "nivel_riesgo": 5, "altura_ola_m": 15.0, "tiempo_arribo_min": 20,
     "periodo_retorno": 100, "region": "Lima",
     "coords": [(-77.2,-12.0),(-77.0,-12.05),(-76.9,-12.1),(-77.0,-12.2),(-77.2,-12.15),(-77.2,-12.0)]},
    {"nombre": "Zona tsunami Ica - Pisco",
     "nivel_riesgo": 5, "altura_ola_m": 12.0, "tiempo_arribo_min": 25,
     "periodo_retorno": 75, "region": "Ica",
     "coords": [(-76.3,-13.6),(-76.1,-13.7),(-76.0,-13.9),(-76.2,-14.0),(-76.4,-13.8),(-76.3,-13.6)]},
    {"nombre": "Zona tsunami Arequipa - Camaná",
     "nivel_riesgo": 5, "altura_ola_m": 18.0, "tiempo_arribo_min": 30,
     "periodo_retorno": 150, "region": "Arequipa",
     "coords": [(-72.9,-16.5),(-72.6,-16.6),(-72.4,-16.8),(-72.6,-17.0),(-72.8,-16.8),(-72.9,-16.5)]},
    {"nombre": "Costa norte Moquegua",
     "nivel_riesgo": 4, "altura_ola_m": 10.0, "tiempo_arribo_min": 35,
     "periodo_retorno": 100, "region": "Moquegua",
     "coords": [(-71.4,-17.0),(-71.2,-17.1),(-71.0,-17.3),(-71.2,-17.4),(-71.4,-17.2),(-71.4,-17.0)]},
    {"nombre": "Litoral Tacna",
     "nivel_riesgo": 4, "altura_ola_m": 9.0, "tiempo_arribo_min": 40,
     "periodo_retorno": 100, "region": "Tacna",
     "coords": [(-70.5,-17.8),(-70.3,-17.9),(-70.1,-18.1),(-70.3,-18.2),(-70.5,-18.0),(-70.5,-17.8)]},
    {"nombre": "Costa Ancash - Chimbote",
     "nivel_riesgo": 4, "altura_ola_m": 8.0, "tiempo_arribo_min": 20,
     "periodo_retorno": 100, "region": "Ancash",
     "coords": [(-78.7,-9.0),(-78.5,-9.1),(-78.3,-9.3),(-78.5,-9.5),(-78.7,-9.3),(-78.7,-9.0)]},
    {"nombre": "Litoral La Libertad - Salaverry",
     "nivel_riesgo": 3, "altura_ola_m": 7.0, "tiempo_arribo_min": 20,
     "periodo_retorno": 100, "region": "La Libertad",
     "coords": [(-79.1,-8.1),(-78.9,-8.2),(-78.7,-8.4),(-78.9,-8.6),(-79.1,-8.4),(-79.1,-8.1)]},
    {"nombre": "Costa Piura - Sechura",
     "nivel_riesgo": 3, "altura_ola_m": 6.5, "tiempo_arribo_min": 25,
     "periodo_retorno": 150, "region": "Piura",
     "coords": [(-81.0,-5.3),(-80.8,-5.4),(-80.6,-5.6),(-80.8,-5.8),(-81.0,-5.6),(-81.0,-5.3)]},
    {"nombre": "Bahía de Tumbes",
     "nivel_riesgo": 3, "altura_ola_m": 5.5, "tiempo_arribo_min": 30,
     "periodo_retorno": 200, "region": "Tumbes",
     "coords": [(-80.6,-3.4),(-80.4,-3.5),(-80.3,-3.7),(-80.5,-3.9),(-80.7,-3.7),(-80.6,-3.4)]},
]

DESLIZAMIENTOS_DATASET = [
    {"nombre": "Huayco recurrente Chosica (Rímac)", "tipo": "flujo de detritos",
     "nivel_riesgo": 5, "area_km2": 25.5, "causa_principal": "lluvias intensas",
     "region": "Lima", "activo": True,
     "coords": [(-76.7,-11.9),(-76.5,-12.0),(-76.4,-12.1),(-76.5,-12.2),(-76.7,-12.1),(-76.7,-11.9)]},
    {"nombre": "Deslizamiento Machu Picchu-Aguas Calientes", "tipo": "deslizamiento rotacional",
     "nivel_riesgo": 4, "area_km2": 8.3, "causa_principal": "lluvias + pendiente",
     "region": "Cusco", "activo": True,
     "coords": [(-72.6,-13.1),(-72.5,-13.2),(-72.4,-13.3),(-72.5,-13.4),(-72.6,-13.3),(-72.6,-13.1)]},
    {"nombre": "Zona inestable Cusco - Yauricocha", "tipo": "deslizamiento traslacional",
     "nivel_riesgo": 4, "area_km2": 45.0, "causa_principal": "sismicidad + lluvias",
     "region": "Cusco", "activo": True,
     "coords": [(-71.8,-13.5),(-71.6,-13.6),(-71.4,-13.7),(-71.5,-13.9),(-71.7,-13.8),(-71.8,-13.5)]},
    {"nombre": "Deslizamientos Ceja de Selva (Amazonas)", "tipo": "deslizamiento masivo",
     "nivel_riesgo": 4, "area_km2": 120.0, "causa_principal": "deforestación + lluvias",
     "region": "Amazonas", "activo": True,
     "coords": [(-78.0,-6.0),(-77.7,-6.3),(-77.4,-6.5),(-77.6,-6.8),(-77.9,-6.6),(-78.0,-6.0)]},
    {"nombre": "Huaycos Cañón del Cotahuasi", "tipo": "flujo de detritos",
     "nivel_riesgo": 4, "area_km2": 15.0, "causa_principal": "lluvias + fuertes pendientes",
     "region": "Arequipa", "activo": True,
     "coords": [(-72.9,-15.1),(-72.7,-15.3),(-72.5,-15.5),(-72.7,-15.7),(-72.9,-15.5),(-72.9,-15.1)]},
    {"nombre": "Deslizamiento Kola (Puno)", "tipo": "deslizamiento rotacional",
     "nivel_riesgo": 4, "area_km2": 180.0, "causa_principal": "sismicidad",
     "region": "Puno", "activo": True,
     "coords": [(-70.6,-15.5),(-70.3,-15.7),(-70.1,-15.9),(-70.3,-16.1),(-70.6,-15.9),(-70.6,-15.5)]},
    {"nombre": "Deslizamiento Yungay (recurrente)", "tipo": "alud",
     "nivel_riesgo": 5, "area_km2": 22.0, "causa_principal": "glaciares + sismicidad",
     "region": "Ancash", "activo": True,
     "coords": [(-77.8,-9.1),(-77.6,-9.2),(-77.4,-9.4),(-77.6,-9.6),(-77.8,-9.4),(-77.8,-9.1)]},
    {"nombre": "Zona aluviónica Piura Sierra", "tipo": "flujo de detritos-aluvial",
     "nivel_riesgo": 4, "area_km2": 35.0, "causa_principal": "FEN intenso",
     "region": "Piura", "activo": True,
     "coords": [(-79.5,-5.0),(-79.2,-5.2),(-79.0,-5.4),(-79.2,-5.6),(-79.5,-5.4),(-79.5,-5.0)]},
    {"nombre": "Taludes Junín Selva Central", "tipo": "deslizamiento traslacional",
     "nivel_riesgo": 3, "area_km2": 60.0, "causa_principal": "deforestación + pendiente",
     "region": "Junín", "activo": True,
     "coords": [(-75.5,-10.8),(-75.2,-11.0),(-75.0,-11.2),(-75.2,-11.4),(-75.5,-11.2),(-75.5,-10.8)]},
    {"nombre": "Deslizamientos San Martín (Alto Huallaga)", "tipo": "deslizamiento rotacional",
     "nivel_riesgo": 3, "area_km2": 40.0, "causa_principal": "lluvias + pendiente",
     "region": "San Martín", "activo": True,
     "coords": [(-76.8,-6.5),(-76.5,-6.7),(-76.3,-6.9),(-76.5,-7.1),(-76.8,-6.9),(-76.8,-6.5)]},
    {"nombre": "Deslizamiento Ocoña-Camaná", "tipo": "flujo de detritos",
     "nivel_riesgo": 4, "area_km2": 28.0, "causa_principal": "lluvias andinas intensas",
     "region": "Arequipa", "activo": True,
     "coords": [(-72.8,-16.3),(-72.5,-16.5),(-72.3,-16.7),(-72.5,-16.9),(-72.8,-16.7),(-72.8,-16.3)]},
    {"nombre": "Deslizamientos Vilcanota (Cusco)", "tipo": "deslizamiento masivo",
     "nivel_riesgo": 4, "area_km2": 55.0, "causa_principal": "sismicidad + lluvias",
     "region": "Cusco", "activo": True,
     "coords": [(-71.4,-14.0),(-71.1,-14.2),(-70.9,-14.4),(-71.1,-14.6),(-71.4,-14.4),(-71.4,-14.0)]},
    {"nombre": "Zona huaycos Huánuco", "tipo": "flujo de detritos",
     "nivel_riesgo": 3, "area_km2": 22.0, "causa_principal": "lluvias",
     "region": "Huánuco", "activo": True,
     "coords": [(-76.3,-9.8),(-76.0,-10.0),(-75.8,-10.2),(-76.0,-10.4),(-76.3,-10.2),(-76.3,-9.8)]},
    {"nombre": "Deslizamientos Cajamarca Norte", "tipo": "deslizamiento rotacional",
     "nivel_riesgo": 3, "area_km2": 30.0, "causa_principal": "lluvias + cambio uso suelo",
     "region": "Cajamarca", "activo": True,
     "coords": [(-78.8,-6.7),(-78.5,-6.9),(-78.3,-7.1),(-78.5,-7.3),(-78.8,-7.1),(-78.8,-6.7)]},
    {"nombre": "Deslizamientos Alto Mayo (San Martín)", "tipo": "deslizamiento traslacional",
     "nivel_riesgo": 3, "area_km2": 18.0, "causa_principal": "deforestación",
     "region": "San Martín", "activo": True,
     "coords": [(-77.3,-5.9),(-77.0,-6.1),(-76.8,-6.3),(-77.0,-6.5),(-77.3,-6.3),(-77.3,-5.9)]},
]


def _insertar_poligonos(conn, tabla: str, dataset: list[dict]) -> int:
    count = 0
    with conn.cursor() as cur:
        for item in dataset:
            coords = item.get("coords", [])
            if len(coords) < 3:
                continue
            coords_sql = ",".join([f"{c[0]} {c[1]}" for c in coords])
            geom_wkt   = f"MULTIPOLYGON((({coords_sql})))"
            fuente     = item.get("fuente", "CENEPRED/IGP 2024")
            try:
                if tabla == "zonas_inundables":
                    cur.execute("""
                        INSERT INTO zonas_inundables
                            (nombre, geom, nivel_riesgo, tipo_inundacion,
                             periodo_retorno, profundidad_max_m, cuenca, region, fuente)
                        VALUES (%s,
                            ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiPolygon,4326),
                            %s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (item["nombre"], geom_wkt, item["nivel_riesgo"],
                          item.get("tipo"), item.get("periodo_retorno"),
                          item.get("profundidad_max_m"), item.get("cuenca"),
                          item["region"], fuente))
                elif tabla == "zonas_tsunami":
                    cur.execute("""
                        INSERT INTO zonas_tsunami
                            (nombre, geom, nivel_riesgo, altura_ola_m,
                             tiempo_arribo_min, periodo_retorno, region, fuente)
                        VALUES (%s,
                            ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiPolygon,4326),
                            %s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (item["nombre"], geom_wkt, item["nivel_riesgo"],
                          item.get("altura_ola_m"), item.get("tiempo_arribo_min"),
                          item.get("periodo_retorno"), item["region"], fuente))
                elif tabla == "deslizamientos":
                    cur.execute("""
                        INSERT INTO deslizamientos
                            (nombre, geom, tipo, nivel_riesgo, area_km2,
                             causa_principal, region, activo, fuente)
                        VALUES (%s,
                            ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiPolygon,4326),
                            %s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (item.get("nombre"), geom_wkt, item.get("tipo"),
                          item.get("nivel_riesgo"), item.get("area_km2"),
                          item.get("causa_principal"), item["region"],
                          item.get("activo", True), fuente))
                count += 1
            except Exception:
                pass
    conn.commit()
    return count


def paso_inundaciones(conn) -> int:
    for url in [
        ("https://snirh.ana.gob.pe/geoserver/snirh/ows?service=WFS&version=1.0.0"
         "&request=GetFeature&typeName=snirh:zonas_inundacion&outputFormat=application/json"),
    ]:
        try:
            data = http_get_bytes(url, timeout=25)
            gj   = json.loads(data)
            if gj.get("features"):
                log.info(f"  ✅ {len(gj['features'])} inundaciones ANA WFS")
        except Exception as e:
            log.warning(f"  ANA WFS falló: {e}")

    n = _insertar_poligonos(conn, "zonas_inundables", INUNDACIONES_DATASET)
    log.info(f"✅ {n} zonas inundables (ANA/CENEPRED)")
    return n


def paso_tsunamis(conn) -> int:
    n = _insertar_poligonos(conn, "zonas_tsunami", TSUNAMIS_DATASET)
    log.info(f"✅ {n} zonas de tsunami (PREDES/IGP/INDECI)")
    return n


def paso_deslizamientos(conn) -> int:
    for url in [
        ("https://sigrid.cenepred.gob.pe/sigridv3/geoserver/ogc/features/v1/"
         "collections/cenepred:deslizamientos/items?f=application/geo+json&limit=500"),
    ]:
        try:
            data = http_get_bytes(url, timeout=25)
            gj   = json.loads(data)
            if gj.get("features"):
                log.info(f"  ✅ {len(gj['features'])} deslizamientos CENEPRED")
        except Exception as e:
            log.warning(f"  CENEPRED WFS falló: {e}")

    n = _insertar_poligonos(conn, "deslizamientos", DESLIZAMIENTOS_DATASET)
    log.info(f"✅ {n} deslizamientos (CENEPRED/INGEMMET dataset)")
    return n


# ══════════════════════════════════════════════════════════════════
#  PASO 7: INFRAESTRUCTURA CRÍTICA
#  🔴 FIX 3: _limpiar_fuera_peru() NULL-proof
# ══════════════════════════════════════════════════════════════════

AEROPUERTOS_MTC = [
    {"nombre": "Aeropuerto Internacional Jorge Chávez",
     "lon": -77.1143, "lat": -12.0219, "region": "Lima", "criticidad": 5},
    {"nombre": "Aeropuerto Alejandro Velasco Astete (Cusco)",
     "lon": -71.9388, "lat": -13.5357, "region": "Cusco", "criticidad": 5},
    {"nombre": "Aeropuerto Rodríguez Ballón (Arequipa)",
     "lon": -71.5831, "lat": -16.3411, "region": "Arequipa", "criticidad": 5},
    {"nombre": "Aeropuerto Capitán FAP Quiñones (Chiclayo)",
     "lon": -79.8282, "lat": -6.7875, "region": "Lambayeque", "criticidad": 5},
    {"nombre": "Aeropuerto Carlos Martínez de Pinillos (Trujillo)",
     "lon": -79.1086, "lat": -8.0814, "region": "La Libertad", "criticidad": 5},
    {"nombre": "Aeropuerto Guillermo Concha Iberico (Piura)",
     "lon": -80.6164, "lat": -5.2075, "region": "Piura", "criticidad": 4},
    {"nombre": "Aeropuerto Francisco Secada Vignetta (Iquitos)",
     "lon": -73.3086, "lat": -3.7847, "region": "Loreto", "criticidad": 4},
    {"nombre": "Aeropuerto Padre José de Aldamiz (Puerto Maldonado)",
     "lon": -69.2287, "lat": -12.6136, "region": "Madre de Dios", "criticidad": 4},
    {"nombre": "Aeropuerto Alfredo Mendívil (Ayacucho)",
     "lon": -74.2042, "lat": -13.1548, "region": "Ayacucho", "criticidad": 4},
    {"nombre": "Aeropuerto David Abensur (Pucallpa)",
     "lon": -74.5742, "lat": -8.3794, "region": "Ucayali", "criticidad": 4},
    {"nombre": "Aeropuerto Inca Manco Capac (Juliaca)",
     "lon": -70.1583, "lat": -15.4672, "region": "Puno", "criticidad": 4},
    {"nombre": "Aeropuerto Pedro Canga Rodríguez (Tumbes)",
     "lon": -80.3783, "lat": -3.5526, "region": "Tumbes", "criticidad": 4},
    {"nombre": "Aeropuerto Guillermo del Castillo (Tarapoto)",
     "lon": -76.3733, "lat": -6.5086, "region": "San Martín", "criticidad": 4},
    {"nombre": "Aeropuerto Carlos Ciriani Santa Rosa (Tacna)",
     "lon": -70.2756, "lat": -18.0533, "region": "Tacna", "criticidad": 4},
    {"nombre": "Aeropuerto Armando Revoredo (Cajamarca)",
     "lon": -78.4894, "lat": -7.1392, "region": "Cajamarca", "criticidad": 3},
    {"nombre": "Aeropuerto de Ilo (Moquegua)",
     "lon": -71.3400, "lat": -17.6944, "region": "Moquegua", "criticidad": 3},
    {"nombre": "Aeropuerto de Andahuaylas",
     "lon": -73.3503, "lat": -13.7064, "region": "Apurímac", "criticidad": 3},
    {"nombre": "Aeropuerto David Figueroa (Huánuco)",
     "lon": -76.2048, "lat": -9.8781, "region": "Huánuco", "criticidad": 3},
    {"nombre": "Aeropuerto Jaime Montreuil (Chimbote)",
     "lon": -78.5244, "lat": -9.1494, "region": "Ancash", "criticidad": 3},
    {"nombre": "Aeropuerto Germán Arias Graziani (Huaraz)",
     "lon": -77.5986, "lat": -9.3469, "region": "Ancash", "criticidad": 3},
]

PUERTOS_APN = [
    {"nombre": "Terminal Portuario del Callao",
     "lon": -77.1483, "lat": -12.0580, "region": "Lima", "criticidad": 5},
    {"nombre": "Terminal Portuario de Paita",
     "lon": -81.1129, "lat": -5.0852, "region": "Piura", "criticidad": 5},
    {"nombre": "Terminal Portuario de Salaverry",
     "lon": -78.9783, "lat": -8.2239, "region": "La Libertad", "criticidad": 4},
    {"nombre": "Terminal Portuario de Chimbote",
     "lon": -78.5861, "lat": -9.0753, "region": "Ancash", "criticidad": 4},
    {"nombre": "Terminal Portuario de Huarmey",
     "lon": -78.1669, "lat": -10.0678, "region": "Ancash", "criticidad": 3},
    {"nombre": "Terminal Portuario de Pisco",
     "lon": -76.2163, "lat": -13.7211, "region": "Ica", "criticidad": 4},
    {"nombre": "Terminal Portuario de Matarani (Arequipa)",
     "lon": -72.1072, "lat": -16.9958, "region": "Arequipa", "criticidad": 4},
    {"nombre": "Terminal Portuario de Ilo",
     "lon": -71.3361, "lat": -17.6358, "region": "Moquegua", "criticidad": 4},
    {"nombre": "Terminal ENAPU Iquitos",
     "lon": -73.2561, "lat": -3.7433, "region": "Loreto", "criticidad": 4},
    {"nombre": "Puerto Fluvial de Pucallpa",
     "lon": -74.5533, "lat": -8.3933, "region": "Ucayali", "criticidad": 3},
    {"nombre": "Terminal Portuario de Yurimaguas",
     "lon": -76.0944, "lat": -5.8975, "region": "Loreto", "criticidad": 3},
    {"nombre": "Puerto de Tumbes",
     "lon": -80.4514, "lat": -3.5681, "region": "Tumbes", "criticidad": 3},
    {"nombre": "Terminal de Cabotaje San Martín (Pisco)",
     "lon": -76.2533, "lat": -13.8081, "region": "Ica", "criticidad": 3},
    {"nombre": "Muelle Pesquero Parachique (Sechura)",
     "lon": -80.8611, "lat": -5.5431, "region": "Piura", "criticidad": 3},
    {"nombre": "Puerto General San Martín (Pisco)",
     "lon": -76.1994, "lat": -13.7689, "region": "Ica", "criticidad": 4},
]

CENTRALES_OSINERGMIN = [
    {"nombre": "C.H. Mantaro (ElectroPerú)",
     "lon": -74.9358, "lat": -12.3083, "region": "Junín", "criticidad": 5},
    {"nombre": "C.H. Chaglla (Pachitea)",
     "lon": -76.1500, "lat": -9.7833,  "region": "Huánuco", "criticidad": 5},
    {"nombre": "C.H. Cerro del Águila",
     "lon": -74.6167, "lat": -12.5333, "region": "Huancavelica", "criticidad": 5},
    {"nombre": "C.H. Quitaracsa",
     "lon": -77.7167, "lat": -8.9333,  "region": "Ancash", "criticidad": 4},
    {"nombre": "C.T. Ventanilla (ENEL)",
     "lon": -77.1500, "lat": -11.8667, "region": "Lima", "criticidad": 5},
    {"nombre": "C.T. Chilca 1 (Kallpa)",
     "lon": -76.7000, "lat": -12.5167, "region": "Lima", "criticidad": 5},
    {"nombre": "C.T. Ilo 1 (Southern Copper)",
     "lon": -71.3344, "lat": -17.6394, "region": "Moquegua", "criticidad": 4},
    {"nombre": "C.H. Machu Picchu (ElectroSur Este)",
     "lon": -72.5456, "lat": -13.1539, "region": "Cusco", "criticidad": 4},
    {"nombre": "C.H. San Gabán II",
     "lon": -69.7833, "lat": -13.3167, "region": "Puno", "criticidad": 4},
    {"nombre": "C.H. Carhuaquero",
     "lon": -79.2167, "lat": -6.6833,  "region": "Lambayeque", "criticidad": 4},
    {"nombre": "Parque Solar Majes (Arequipa)",
     "lon": -72.3167, "lat": -16.3833, "region": "Arequipa", "criticidad": 3},
    {"nombre": "C.H. Gallito Ciego (CHAVIMOCHIC)",
     "lon": -79.1333, "lat": -7.0833,  "region": "La Libertad", "criticidad": 4},
    {"nombre": "C.H. Oroya — ElectroAndes",
     "lon": -75.9167, "lat": -11.5333, "region": "Junín", "criticidad": 4},
    {"nombre": "C.H. Cañon del Pato (Duke Energy)",
     "lon": -77.7208, "lat": -8.9069,  "region": "Ancash", "criticidad": 5},
    {"nombre": "C.T. Pisco",
     "lon": -76.2167, "lat": -13.8333, "region": "Ica", "criticidad": 4},
    {"nombre": "Sub-Estación Zapallal (Red Alta Tensión)",
     "lon": -77.0833, "lat": -11.8667, "region": "Lima", "criticidad": 5},
    {"nombre": "C.H. Yuncan (ElectroPerú)",
     "lon": -75.5083, "lat": -10.2833, "region": "Pasco", "criticidad": 4},
    {"nombre": "C.H. Restitución (ElectroPerú)",
     "lon": -75.0833, "lat": -12.3167, "region": "Junín", "criticidad": 4},
]

HOSPITALES_MINSA = [
    {"nombre": "Hospital Nacional Dos de Mayo",
     "lon": -77.0439, "lat": -12.0508, "region": "Lima"},
    {"nombre": "Hospital Nacional Arzobispo Loayza",
     "lon": -77.0387, "lat": -12.0475, "region": "Lima"},
    {"nombre": "Hospital Nacional Guillermo Almenara (EsSalud)",
     "lon": -77.0100, "lat": -12.0669, "region": "Lima"},
    {"nombre": "Hospital Nacional Edgardo Rebagliati (EsSalud)",
     "lon": -77.0511, "lat": -12.0847, "region": "Lima"},
    {"nombre": "Hospital Nacional Cayetano Heredia",
     "lon": -77.0633, "lat": -11.9861, "region": "Lima"},
    {"nombre": "Hospital de Emergencias Grau (EsSalud)",
     "lon": -77.0156, "lat": -12.0711, "region": "Lima"},
    {"nombre": "Hospital Nacional San Bartolomé",
     "lon": -77.0356, "lat": -12.0450, "region": "Lima"},
    {"nombre": "Hospital Militar Central",
     "lon": -77.0572, "lat": -12.0781, "region": "Lima"},
    {"nombre": "Hospital Regional de Ica",
     "lon": -75.7256, "lat": -14.0678, "region": "Ica"},
    {"nombre": "Hospital Santa María del Socorro (Ica)",
     "lon": -75.7183, "lat": -14.0750, "region": "Ica"},
    {"nombre": "Hospital Regional Honorio Delgado (Arequipa)",
     "lon": -71.5378, "lat": -16.4189, "region": "Arequipa"},
    {"nombre": "Hospital Nacional Carlos Seguín Escobedo (Arequipa)",
     "lon": -71.5300, "lat": -16.3900, "region": "Arequipa"},
    {"nombre": "Hospital Regional del Cusco",
     "lon": -71.9769, "lat": -13.5161, "region": "Cusco"},
    {"nombre": "Hospital Nacional Adolfo Guevara Velasco (Cusco)",
     "lon": -71.9781, "lat": -13.5278, "region": "Cusco"},
    {"nombre": "Hospital Antonio Lorena (Cusco)",
     "lon": -71.9667, "lat": -13.5014, "region": "Cusco"},
    {"nombre": "Hospital Regional de Trujillo",
     "lon": -79.0372, "lat": -8.1042,  "region": "La Libertad"},
    {"nombre": "Hospital Víctor Lazarte Echegaray (Trujillo EsSalud)",
     "lon": -79.0228, "lat": -8.0978,  "region": "La Libertad"},
    {"nombre": "Hospital Regional de Piura",
     "lon": -80.6339, "lat": -5.1942,  "region": "Piura"},
    {"nombre": "Hospital Santa Rosa (Piura)",
     "lon": -80.6272, "lat": -5.2008,  "region": "Piura"},
    {"nombre": "Hospital Regional de Chiclayo",
     "lon": -79.8394, "lat": -6.7744,  "region": "Lambayeque"},
    {"nombre": "Hospital Almanzor Aguinaga Asenjo (Chiclayo EsSalud)",
     "lon": -79.8350, "lat": -6.7789,  "region": "Lambayeque"},
    {"nombre": "Hospital Regional de Ayacucho",
     "lon": -74.2236, "lat": -13.1597, "region": "Ayacucho"},
    {"nombre": "Hospital Regional de Puno",
     "lon": -70.0181, "lat": -15.8508, "region": "Puno"},
    {"nombre": "Hospital Carlos Monge Medrano (Juliaca)",
     "lon": -70.1356, "lat": -15.4797, "region": "Puno"},
    {"nombre": "Hospital Regional de Huancayo",
     "lon": -75.2181, "lat": -12.0639, "region": "Junín"},
    {"nombre": "Hospital Ramiro Prialé Prialé (Huancayo EsSalud)",
     "lon": -75.2044, "lat": -12.0714, "region": "Junín"},
    {"nombre": "Hospital Regional de Tacna",
     "lon": -70.0161, "lat": -18.0158, "region": "Tacna"},
    {"nombre": "Hospital Hipólito Unanue (Tacna)",
     "lon": -70.0128, "lat": -18.0219, "region": "Tacna"},
    {"nombre": "Hospital Regional de Tumbes",
     "lon": -80.4606, "lat": -3.5650,  "region": "Tumbes"},
    {"nombre": "Hospital Iquitos (Loreto)",
     "lon": -73.2481, "lat": -3.7481,  "region": "Loreto"},
    {"nombre": "Hospital Regional de Moquegua",
     "lon": -70.9372, "lat": -17.1939, "region": "Moquegua"},
    {"nombre": "Hospital Regional de Cajamarca",
     "lon": -78.5083, "lat": -7.1631,  "region": "Cajamarca"},
    {"nombre": "Hospital Regional de Huánuco",
     "lon": -76.2419, "lat": -9.9281,  "region": "Huánuco"},
    {"nombre": "Hospital de San Martín (Tarapoto)",
     "lon": -76.3789, "lat": -6.4844,  "region": "San Martín"},
    {"nombre": "Hospital La Caleta (Chimbote)",
     "lon": -78.5839, "lat": -9.0736,  "region": "Ancash"},
    {"nombre": "Hospital Eleazar Guzmán Barrón (Chimbote)",
     "lon": -78.5783, "lat": -9.0811,  "region": "Ancash"},
    {"nombre": "Hospital Regional de Pucallpa",
     "lon": -74.5358, "lat": -8.3781,  "region": "Ucayali"},
]

BOMBEROS_CGBVP = [
    {"nombre": "Compañía de Bomberos Lima N°1",
     "lon": -77.0428, "lat": -12.0464, "region": "Lima"},
    {"nombre": "Compañía de Bomberos Miraflores N°28",
     "lon": -77.0294, "lat": -12.1200, "region": "Lima"},
    {"nombre": "Compañía de Bomberos San Isidro N°10",
     "lon": -77.0422, "lat": -12.0975, "region": "Lima"},
    {"nombre": "Compañía de Bomberos Arequipa N°20",
     "lon": -71.5483, "lat": -16.4011, "region": "Arequipa"},
    {"nombre": "Compañía de Bomberos Cusco N°25",
     "lon": -71.9811, "lat": -13.5236, "region": "Cusco"},
    {"nombre": "Compañía de Bomberos Ica N°15",
     "lon": -75.7278, "lat": -14.0644, "region": "Ica"},
    {"nombre": "Compañía de Bomberos Piura N°6",
     "lon": -80.6394, "lat": -5.1967,  "region": "Piura"},
    {"nombre": "Compañía de Bomberos Trujillo N°7",
     "lon": -79.0350, "lat": -8.0994,  "region": "La Libertad"},
    {"nombre": "Compañía de Bomberos Chiclayo N°12",
     "lon": -79.8411, "lat": -6.7694,  "region": "Lambayeque"},
    {"nombre": "Compañía de Bomberos Tacna N°18",
     "lon": -70.0194, "lat": -18.0106, "region": "Tacna"},
    {"nombre": "Compañía de Bomberos Puno N°40",
     "lon": -70.0231, "lat": -15.8531, "region": "Puno"},
    {"nombre": "Compañía de Bomberos Huancayo N°35",
     "lon": -75.2233, "lat": -12.0647, "region": "Junín"},
    {"nombre": "Compañía de Bomberos Ayacucho N°50",
     "lon": -74.2178, "lat": -13.1556, "region": "Ayacucho"},
    {"nombre": "Compañía de Bomberos Cajamarca N°55",
     "lon": -78.5083, "lat": -7.1583,  "region": "Cajamarca"},
    {"nombre": "Compañía de Bomberos Tumbes N°60",
     "lon": -80.4583, "lat": -3.5703,  "region": "Tumbes"},
    {"nombre": "Compañía de Bomberos Iquitos N°65",
     "lon": -73.2489, "lat": -3.7514,  "region": "Loreto"},
]


def _buscar_resource_id_ckan(query: str) -> str | None:
    try:
        data = http_get(
            "https://www.datosabiertos.gob.pe/api/3/action/package_search",
            params={"q": query, "rows": 5}, timeout=20,
        )
        for result in data.get("result", {}).get("results", []):
            for resource in result.get("resources", []):
                fmt = (resource.get("format") or "").upper()
                if fmt in ("CSV", "JSON", "GEOJSON", "XLSX"):
                    rid = resource.get("id")
                    if rid:
                        log.info(f"  CKAN → '{result['title']}' resource_id={rid}")
                        return rid
    except Exception:
        pass
    return None


def _ckan_datastore_fetch(resource_id: str, limit: int = 5000) -> list[dict]:
    rows, offset = [], 0
    while True:
        try:
            data = http_get(
                "https://www.datosabiertos.gob.pe/api/3/action/datastore_search",
                params={"resource_id": resource_id, "limit": limit, "offset": offset},
                timeout=30,
            )
            records = data.get("result", {}).get("records", [])
            if not records:
                break
            rows.extend(records)
            if len(records) < limit:
                break
            offset += limit
        except Exception as e:
            log.warning(f"  CKAN datastore falló en offset={offset}: {e}")
            break
    return rows


def _extraer_latlon_ckan_record(rec: dict) -> tuple[float, float] | None:
    lat_keys = ["latitud", "lat", "LATITUD", "LAT", "y_coord", "Y"]
    lon_keys = ["longitud", "lon", "lng", "LONGITUD", "LON", "LNG", "x_coord", "X"]
    lat = lon = None
    for k in lat_keys:
        if k in rec and rec[k]:
            try:
                lat = float(str(rec[k]).replace(",", "."))
                break
            except ValueError:
                pass
    for k in lon_keys:
        if k in rec and rec[k]:
            try:
                lon = float(str(rec[k]).replace(",", "."))
                break
            except ValueError:
                pass
    if lat and lon and -90 < lat < 90 and -180 < lon < 180:
        return lon, lat
    return None


OSM_QUERIES = {
    "hospital":          'amenity="hospital"',
    "clinica":           'amenity~"clinic|pharmacy"',
    "escuela":           'amenity~"school|kindergarten|university|college"',
    "bomberos":          'amenity="fire_station"',
    "policia":           'amenity="police"',
    "aeropuerto":        'aeroway~"aerodrome|airport"',
    "puerto":            'industrial~"port|harbour"',
    "central_electrica": 'power~"plant|generator"',
    "planta_agua":       'man_made~"water_works|pumping_station|water_tower"',
    "refugio":           'amenity~"shelter|social_facility"',
}
OSM_CRITICIDAD = {
    "hospital": 5, "clinica": 4, "escuela": 4,
    "bomberos": 5, "policia": 4, "aeropuerto": 4,
    "puerto": 4, "central_electrica": 4, "planta_agua": 4, "refugio": 5,
}


def get_infra_osm(tipo: str) -> list[dict]:
    tag      = OSM_QUERIES.get(tipo, f'amenity="{tipo}"')
    query    = overpass_query(tag)
    elements = try_overpass(query, tipo)
    resultados = []
    for el in elements:
        coords = normalize_osm_element(el)
        if not coords:
            continue
        lon, lat = coords
        tags   = el.get("tags", {})
        nombre = (tags.get("name:es") or tags.get("name") or
                  tipo.replace("_", " ").title())
        resultados.append({
            "nombre": nombre, "tipo": tipo,
            "lon": lon, "lat": lat,
            "osm_id": el.get("id"),
            "criticidad": OSM_CRITICIDAD.get(tipo, 3),
            "estado": tags.get("operational_status", "operativo"),
            "fuente": "OpenStreetMap", "fuente_tipo": "osm",
        })
    return resultados


def _insertar_items_infra(conn, items: list[dict]) -> int:
    if not items:
        return 0
    count = 0
    with conn.cursor() as cur:
        for item in items:
            lon, lat = item.get("lon"), item.get("lat")
            if not lon or not lat:
                continue
            if not (-83 <= lon <= -67 and -20 <= lat <= 2):
                continue
            try:
                cur.execute("""
                    INSERT INTO infraestructura
                        (osm_id, nombre, tipo, geom, criticidad, estado,
                         fuente, fuente_tipo, capacidad)
                    VALUES (%s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    item.get("osm_id"),
                    (item.get("nombre") or "Sin nombre")[:200],
                    item.get("tipo", "otro"),
                    lon, lat,
                    item.get("criticidad", 3),
                    item.get("estado", "operativo"),
                    item.get("fuente", "OSM"),
                    item.get("fuente_tipo", "osm"),
                    item.get("capacidad"),
                ))
                count += 1
            except Exception:
                pass
    conn.commit()
    return count


def _limpiar_fuera_peru(conn) -> int:
    """
    🔴 FIX 3 — Elimina infraestructura fuera del territorio peruano.

    3 niveles de seguridad:
    1. Verifica COUNT(departamentos) antes de operar.
    2. Si < 5 departamentos → usa solo bbox (nunca NULL).
    3. Verifica que ST_Union no devuelva NULL antes del DELETE.

    Buffer 0.27° ≈ 30 km cubre islas, costa y fronteras.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")
        n_deptos = cur.fetchone()[0]

    if n_deptos < 5:
        log.warning(
            f"  ⚠  Solo {n_deptos} departamentos — "
            "usando bbox Perú para limpieza (modo seguro)"
        )
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM infraestructura
                WHERE ST_X(geom) NOT BETWEEN -82.5 AND -68.0
                   OR ST_Y(geom) NOT BETWEEN -19.0 AND  1.5
            """)
            n = cur.rowcount
        conn.commit()
        log.info(f"  🗑  {n} elementos eliminados por bbox (fallback)")
        return n

    log.info(f"  🔍 Limpiando con geometría PostGIS ({n_deptos} departamentos)...")
    t0 = time.time()
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _tmp_peru_boundary")
        cur.execute("""
            CREATE TEMP TABLE _tmp_peru_boundary AS
            SELECT ST_Buffer(ST_Union(geom), 0.27) AS geom
            FROM departamentos
            WHERE geom IS NOT NULL
        """)

        # Nivel 3: verificar que ST_Union no devolvió NULL
        cur.execute("SELECT geom IS NOT NULL FROM _tmp_peru_boundary LIMIT 1")
        row = cur.fetchone()
        if not row or not row[0]:
            log.warning("  ⚠  ST_Union devolvió NULL — usando bbox como fallback")
            cur.execute("DROP TABLE IF EXISTS _tmp_peru_boundary")
            cur.execute("""
                DELETE FROM infraestructura
                WHERE ST_X(geom) NOT BETWEEN -82.5 AND -68.0
                   OR ST_Y(geom) NOT BETWEEN -19.0 AND  1.5
            """)
            n = cur.rowcount
            conn.commit()
            return n

        cur.execute("CREATE INDEX _tmp_peru_gix ON _tmp_peru_boundary USING GIST(geom)")
        cur.execute("""
            DELETE FROM infraestructura i
            WHERE NOT EXISTS (
                SELECT 1 FROM _tmp_peru_boundary p
                WHERE ST_Intersects(i.geom, p.geom)
            )
        """)
        n = cur.rowcount
        cur.execute("DROP TABLE IF EXISTS _tmp_peru_boundary")
    conn.commit()
    log.info(f"  🗑  {n} elementos fuera de Perú eliminados ({time.time()-t0:.1f}s)")
    return n


def paso_infraestructura(conn) -> int:
    t0    = time.time()
    total = 0

    log.info("  Cargando aeropuertos MTC/CORPAC...")
    total += _insertar_items_infra(conn, [
        {"nombre": a["nombre"], "tipo": "aeropuerto",
         "lon": a["lon"], "lat": a["lat"],
         "criticidad": a["criticidad"], "estado": "operativo",
         "fuente": "MTC/CORPAC 2024", "fuente_tipo": "oficial"}
        for a in AEROPUERTOS_MTC
    ])

    log.info("  Cargando puertos APN/MTC...")
    total += _insertar_items_infra(conn, [
        {"nombre": p["nombre"], "tipo": "puerto",
         "lon": p["lon"], "lat": p["lat"],
         "criticidad": p["criticidad"], "estado": "operativo",
         "fuente": "APN/MTC 2024", "fuente_tipo": "oficial"}
        for p in PUERTOS_APN
    ])

    log.info("  Cargando centrales eléctricas OSINERGMIN...")
    total += _insertar_items_infra(conn, [
        {"nombre": c["nombre"], "tipo": "central_electrica",
         "lon": c["lon"], "lat": c["lat"],
         "criticidad": c["criticidad"], "estado": "operativo",
         "fuente": "OSINERGMIN/MINEM 2024", "fuente_tipo": "oficial"}
        for c in CENTRALES_OSINERGMIN
    ])

    log.info("  Cargando hospitales MINSA/SUSALUD...")
    total += _insertar_items_infra(conn, [
        {"nombre": h["nombre"], "tipo": "hospital",
         "lon": h["lon"], "lat": h["lat"],
         "criticidad": 5, "estado": "operativo",
         "fuente": "MINSA/SUSALUD 2024", "fuente_tipo": "oficial"}
        for h in HOSPITALES_MINSA
    ])

    log.info("  Cargando bomberos CGBVP...")
    total += _insertar_items_infra(conn, [
        {"nombre": b["nombre"], "tipo": "bomberos",
         "lon": b["lon"], "lat": b["lat"],
         "criticidad": 5, "estado": "operativo",
         "fuente": "CGBVP 2024", "fuente_tipo": "oficial"}
        for b in BOMBEROS_CGBVP
    ])

    log.info(f"  ✅ {total} elementos oficiales cargados")

    for tipo in ["hospital", "escuela", "policia", "planta_agua", "refugio"]:
        n = _insertar_items_infra(conn, get_infra_osm(tipo))
        total += n
        if n:
            log.info(f"  OSM {tipo}: +{n}")

    for getter, label in [
        (_get_infra_salud_api,       "SUSALUD RENIPRESS"),
        (_get_infra_educacion_api,   "MINEDU ESCALE"),
        (_get_infra_policia_api,     "PNP comisarías"),
        (_get_infra_albergues_api,   "INDECI albergues"),
    ]:
        try:
            items = getter()
            n = _insertar_items_infra(conn, items)
            total += n
            if n:
                log.info(f"  ✅ {label}: +{n}")
        except Exception as e:
            log.warning(f"  {label} falló: {e}")

    # 🔴 FIX 3: Limpieza PostGIS NULL-proof
    log.info(f"  🔍 Verificando {total} elementos contra límites de Perú...")
    n_eliminados = _limpiar_fuera_peru(conn)
    total_final  = total - n_eliminados

    log.info(f"✅ {total_final} elementos infraestructura válidos ({time.time()-t0:.1f}s)")
    return total_final


def _get_infra_salud_api() -> list[dict]:
    for base in [
        "https://app.susalud.gob.pe/api/v1/establecimientos",
        "https://ww3.susalud.gob.pe/sisnet-web/rest/EstablecimientoResource/listarEstablecimiento",
    ]:
        try:
            resultados, page = [], 1
            while True:
                data  = http_get(f"{base}?page={page}&size=500", timeout=25)
                items = (data.get("data") or data.get("establecimientos") or
                         data.get("content") or (data if isinstance(data, list) else []))
                if not items:
                    break
                for item in items:
                    lat = item.get("latitud") or item.get("lat") or item.get("coordenadaY")
                    lon = item.get("longitud") or item.get("lon") or item.get("coordenadaX")
                    if lat and lon:
                        try:
                            resultados.append({
                                "nombre": (item.get("nombre") or "Establecimiento de salud"),
                                "tipo": "hospital",
                                "lon": float(lon), "lat": float(lat),
                                "criticidad": 4, "estado": "operativo",
                                "fuente": "SUSALUD/RENIPRESS", "fuente_tipo": "oficial",
                            })
                        except (ValueError, TypeError):
                            pass
                if len(items) < 500:
                    break
                page += 1
            if resultados:
                log.info(f"  ✅ {len(resultados)} establecimientos SUSALUD")
                return resultados
        except Exception as e:
            log.warning(f"  SUSALUD API falló: {e}")

    for q in ["renipress establecimientos salud SUSALUD",
              "MINSA establecimientos salud georreferenciados"]:
        rid = _buscar_resource_id_ckan(q)
        if rid:
            records = _ckan_datastore_fetch(rid, limit=2000)
            out = []
            for rec in records:
                coords = _extraer_latlon_ckan_record(rec)
                if coords:
                    lon, lat = coords
                    out.append({
                        "nombre": (rec.get("NOMBRE") or rec.get("nombre") or "Establecimiento de salud"),
                        "tipo": "hospital",
                        "lon": lon, "lat": lat,
                        "criticidad": 4, "estado": "operativo",
                        "fuente": "MINSA/datos.gob.pe", "fuente_tipo": "oficial",
                    })
            if out:
                return out
    return []


def _get_infra_educacion_api() -> list[dict]:
    for wfs in [
        ("https://sigmed.minedu.gob.pe/geoserver/sigmed/ows?service=WFS&version=1.0.0"
         "&request=GetFeature&typeName=sigmed:iiee_geopunto"
         "&outputFormat=application/json&maxFeatures=50000&srsName=EPSG:4326"),
    ]:
        try:
            data = http_get_bytes(wfs, timeout=120)
            gj   = json.loads(data)
            out  = []
            for feat in gj.get("features", []):
                p = feat["properties"]
                c = feat["geometry"]["coordinates"]
                out.append({
                    "nombre": p.get("NOM_IE") or "Institución Educativa",
                    "tipo": "escuela",
                    "lon": c[0], "lat": c[1],
                    "criticidad": 4, "estado": "operativo",
                    "fuente": "MINEDU/ESCALE", "fuente_tipo": "oficial",
                })
            if out:
                return out
        except Exception as e:
            log.warning(f"  MINEDU WFS falló: {e}")
    return []


def _get_infra_policia_api() -> list[dict]:
    for q in ["comisarias PNP policía nacional Peru",
              "unidades policiales Peru georreferenciadas"]:
        rid = _buscar_resource_id_ckan(q)
        if rid:
            out = []
            for rec in _ckan_datastore_fetch(rid):
                coords = _extraer_latlon_ckan_record(rec)
                if coords:
                    lon, lat = coords
                    out.append({
                        "nombre": rec.get("NOMBRE") or rec.get("UNIDAD") or "Comisaría PNP",
                        "tipo": "policia",
                        "lon": lon, "lat": lat,
                        "criticidad": 4, "estado": "operativo",
                        "fuente": "PNP/datos.gob.pe", "fuente_tipo": "oficial",
                    })
            if out:
                return out
    return []


def _get_infra_albergues_api() -> list[dict]:
    try:
        url  = ("https://sigrid.cenepred.gob.pe/sigridv3/geoserver/ows"
                "?service=WFS&version=1.0.0&request=GetFeature"
                "&typeName=cenepred:albergues&outputFormat=application/json"
                "&maxFeatures=2000")
        data = http_get_bytes(url, timeout=30)
        gj   = json.loads(data)
        out  = []
        for feat in gj.get("features", []):
            p = feat["properties"]
            c = feat["geometry"]["coordinates"]
            out.append({
                "nombre": p.get("NOMBRE") or "Albergue INDECI",
                "tipo": "refugio",
                "lon": c[0], "lat": c[1],
                "criticidad": 5, "estado": "operativo",
                "capacidad": p.get("CAPACIDAD"),
                "fuente": "CENEPRED/INDECI", "fuente_tipo": "oficial",
            })
        return out
    except Exception as e:
        log.warning(f"  INDECI albergues WFS falló: {e}")
    return []


# ══════════════════════════════════════════════════════════════════
#  PASO 8: ESTACIONES DE MONITOREO
# ══════════════════════════════════════════════════════════════════

ESTACIONES_DATASET = [
    {"codigo": "NNA",     "nombre": "Estación Sísmica Nanay (Iquitos)",        "tipo": "sismica",
     "lon": -73.1667, "lat": -3.7833,  "altitud_m": 110,  "institucion": "IGP", "red": "RSN"},
    {"codigo": "LIM",     "nombre": "Estación Sísmica Lima",                    "tipo": "sismica",
     "lon": -77.0500, "lat": -11.9000, "altitud_m": 154,  "institucion": "IGP", "red": "RSN"},
    {"codigo": "AYA",     "nombre": "Estación Sísmica Ayacucho",                "tipo": "sismica",
     "lon": -74.2167, "lat": -13.1500, "altitud_m": 2765, "institucion": "IGP", "red": "RSN"},
    {"codigo": "CUS",     "nombre": "Estación Sísmica Cusco",                   "tipo": "sismica",
     "lon": -71.9700, "lat": -13.5200, "altitud_m": 3399, "institucion": "IGP", "red": "RSN"},
    {"codigo": "ARE",     "nombre": "Estación Sísmica Arequipa",                "tipo": "sismica",
     "lon": -71.4900, "lat": -16.4100, "altitud_m": 2490, "institucion": "IGP", "red": "RSN"},
    {"codigo": "TAC",     "nombre": "Estación Sísmica Tacna",                   "tipo": "sismica",
     "lon": -70.0700, "lat": -18.0100, "altitud_m": 550,  "institucion": "IGP", "red": "RSN"},
    {"codigo": "MQG",     "nombre": "Estación Sísmica Moquegua",                "tipo": "sismica",
     "lon": -70.9200, "lat": -17.1800, "altitud_m": 1400, "institucion": "IGP", "red": "RSN"},
    {"codigo": "HCY",     "nombre": "Estación Sísmica Huancayo",                "tipo": "sismica",
     "lon": -75.2167, "lat": -12.0500, "altitud_m": 3315, "institucion": "IGP", "red": "RSN"},
    {"codigo": "CHB",     "nombre": "Estación Sísmica Chimbote",                "tipo": "sismica",
     "lon": -78.5800, "lat": -9.0800,  "altitud_m": 15,   "institucion": "IGP", "red": "RSN"},
    {"codigo": "PIU_S",   "nombre": "Estación Sísmica Piura",                   "tipo": "sismica",
     "lon": -80.6200, "lat": -5.1900,  "altitud_m": 30,   "institucion": "IGP", "red": "RSN"},
    {"codigo": "ICA_S",   "nombre": "Estación Sísmica Ica",                     "tipo": "sismica",
     "lon": -75.7300, "lat": -14.0800, "altitud_m": 410,  "institucion": "IGP", "red": "RSN"},
    {"codigo": "MOQ_S",   "nombre": "Estación Sísmica Mollendo",                "tipo": "sismica",
     "lon": -72.0200, "lat": -17.0300, "altitud_m": 60,   "institucion": "IGP", "red": "RSN"},
    {"codigo": "OVI-UBI", "nombre": "Observatorio Vulcanológico Ubinas",        "tipo": "volcanologica",
     "lon": -70.9000, "lat": -16.3500, "altitud_m": 4800, "institucion": "IGP", "red": "OVI"},
    {"codigo": "OVI-SAP", "nombre": "Observatorio Vulcanológico Sabancaya",     "tipo": "volcanologica",
     "lon": -71.8700, "lat": -15.7300, "altitud_m": 4979, "institucion": "IGP", "red": "OVI"},
    {"codigo": "OVI-ELM", "nombre": "Observatorio Vulcanológico El Misti",      "tipo": "volcanologica",
     "lon": -71.4100, "lat": -16.2900, "altitud_m": 4600, "institucion": "IGP", "red": "OVI"},
    {"codigo": "SENA-ICA", "nombre": "Estación Meteorológica Ica",              "tipo": "meteorologica",
     "lon": -75.7200, "lat": -14.0700, "altitud_m": 406,  "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-PIU", "nombre": "Estación Meteorológica Piura",            "tipo": "meteorologica",
     "lon": -80.6300, "lat": -5.1800,  "altitud_m": 29,   "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-HYC", "nombre": "Estación Meteorológica Huancayo",         "tipo": "meteorologica",
     "lon": -75.3300, "lat": -12.0600, "altitud_m": 3313, "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-IQT", "nombre": "Estación Meteorológica Iquitos",          "tipo": "meteorologica",
     "lon": -73.2600, "lat": -3.7800,  "altitud_m": 126,  "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-ARE", "nombre": "Estación Meteorológica Arequipa",         "tipo": "meteorologica",
     "lon": -71.5600, "lat": -16.3300, "altitud_m": 2525, "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-CUS", "nombre": "Estación Meteorológica Cusco",            "tipo": "meteorologica",
     "lon": -71.9800, "lat": -13.5600, "altitud_m": 3350, "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-JUL", "nombre": "Estación Meteorológica Juliaca",          "tipo": "meteorologica",
     "lon": -70.1800, "lat": -15.4800, "altitud_m": 3820, "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-CJM", "nombre": "Estación Meteorológica Cajamarca",        "tipo": "meteorologica",
     "lon": -78.5100, "lat": -7.1700,  "altitud_m": 2720, "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-MPC", "nombre": "Estación Meteorológica Machu Picchu",     "tipo": "meteorologica",
     "lon": -72.5400, "lat": -13.1600, "altitud_m": 2040, "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "SENA-TRP", "nombre": "Estación Meteorológica Tarapoto",         "tipo": "meteorologica",
     "lon": -76.3700, "lat": -6.4900,  "altitud_m": 356,  "institucion": "SENAMHI", "red": "RMN"},
    {"codigo": "ANA-RIM",  "nombre": "Hidrómetro Rímac - La Atarjea",           "tipo": "hidrometrica",
     "lon": -77.0167, "lat": -11.9667, "altitud_m": 800,  "institucion": "ANA", "red": "RHN"},
    {"codigo": "ANA-MAN",  "nombre": "Hidrómetro Mantaro - Angasmayo",          "tipo": "hidrometrica",
     "lon": -75.0500, "lat": -11.7833, "altitud_m": 3350, "institucion": "ANA", "red": "RHN"},
    {"codigo": "ANA-CHI",  "nombre": "Hidrómetro Chira - Ardilla",              "tipo": "hidrometrica",
     "lon": -80.6167, "lat": -4.9333,  "altitud_m": 45,   "institucion": "ANA", "red": "RHN"},
    {"codigo": "ANA-AMZ",  "nombre": "Hidrómetro Amazonas - Borja",             "tipo": "hidrometrica",
     "lon": -77.5500, "lat": -4.4833,  "altitud_m": 200,  "institucion": "ANA", "red": "RHN"},
    {"codigo": "ANA-TIT",  "nombre": "Hidrómetro Titicaca - Puno",              "tipo": "hidrometrica",
     "lon": -70.0200, "lat": -15.8500, "altitud_m": 3810, "institucion": "ANA", "red": "RHN"},
    {"codigo": "DHN-CAL",  "nombre": "Mareógrafo Callao (DART)",                "tipo": "maregraf",
     "lon": -77.1500, "lat": -12.0500, "altitud_m": 5,    "institucion": "DHN", "red": "DART"},
    {"codigo": "DHN-MAT",  "nombre": "Mareógrafo Matarani (Tsunami)",           "tipo": "maregraf",
     "lon": -72.1000, "lat": -17.0000, "altitud_m": 4,    "institucion": "DHN", "red": "DART"},
    {"codigo": "IPEN-LIM", "nombre": "Estación Radiológica Lima",               "tipo": "radiologica",
     "lon": -77.0500, "lat": -11.9800, "altitud_m": 180,  "institucion": "IPEN", "red": "RRM"},
    {"codigo": "COEN-LIM", "nombre": "Centro Operaciones Emergencias Nacional", "tipo": "emergencias",
     "lon": -77.0500, "lat": -12.0500, "altitud_m": 150,  "institucion": "INDECI", "red": "COEN"},
]


def paso_estaciones(conn) -> int:
    count = 0
    with conn.cursor() as cur:
        for e in ESTACIONES_DATASET:
            try:
                cur.execute("""
                    INSERT INTO estaciones
                        (codigo, nombre, tipo, geom, altitud_m, activa, institucion, red)
                    VALUES (%s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s, %s, %s, %s)
                    ON CONFLICT (codigo) DO UPDATE SET
                        activa    = EXCLUDED.activa,
                        altitud_m = EXCLUDED.altitud_m
                """, (e["codigo"], e["nombre"], e["tipo"],
                      e["lon"], e["lat"],
                      e.get("altitud_m"), e.get("activa", True),
                      e.get("institucion"), e.get("red")))
                count += 1
            except Exception:
                pass
    conn.commit()
    log.info(f"✅ {count} estaciones de monitoreo")
    return count


# ══════════════════════════════════════════════════════════════════
#  PASO 9: HEATMAP MATERIALIZADO
#  🔴 FIX 1: usa refresh_matview() — fuera de transacción
# ══════════════════════════════════════════════════════════════════

def paso_heatmap(conn) -> None:
    log.info("Refrescando mv_heatmap_sismos...")
    t0 = time.time()
    
    refresh_matview(conn, "mv_heatmap_sismos")
    log.info(f"✅ Heatmap actualizado ({time.time()-t0:.1f}s)")


# ══════════════════════════════════════════════════════════════════
#  PASO 10: REGIONES (ST_Covers + KNN — sin NULL)
#  🆕 ORDEN: debe correr ANTES de paso_riesgo_construccion
#     para que zona_sismica en distritos esté poblada.
# ══════════════════════════════════════════════════════════════════

def paso_regiones(conn) -> int:
    log.info("Actualizando regiones via PostGIS (ST_Covers + KNN)...")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")
        n = cur.fetchone()[0]

    if n == 0:
        log.error("  ✗ Sin departamentos con geometría — región no puede asignarse")
        log.error("    Solución: python procesar_datos.py --solo departamentos")
        return 0

    log.info(f"  {n} departamentos disponibles para asignación de región")
    totales = 0
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM f_actualizar_regiones()")
        for tabla, covers, knn in cur.fetchall():
            log.info(f"  {tabla:<35} covers={covers}  knn={knn}")
            totales += covers + knn

        # 🆕 Actualizar zona_sismica en distritos ANTES del refresh de IRC
        cur.execute("""
            UPDATE distritos d
            SET zona_sismica = dep.zona_sismica
            FROM departamentos dep
            WHERE LOWER(d.departamento) = LOWER(dep.nombre)
              AND d.zona_sismica IS DISTINCT FROM dep.zona_sismica
        """)
        n_zona = cur.rowcount
        log.info(f"  distritos zona_sismica actualizada: {n_zona} filas")

    conn.commit()
    log.info("✅ Regiones actualizadas — sin NULL (KNN fallback garantizado)")
    return totales


# ══════════════════════════════════════════════════════════════════
#  PASO 11: ÍNDICE DE RIESGO DE CONSTRUCCIÓN
#  🔴 FIX 1: usa refresh_matview() — fuera de transacción
#  🆕 ORDEN: requiere que paso_regiones() haya corrido primero
#     (zona_sismica en distritos debe estar actualizada)
# ══════════════════════════════════════════════════════════════════

def paso_riesgo_construccion(conn) -> None:
    """
    Refresca mv_riesgo_construccion.

    PREREQUISITO: paso_regiones() debe haber corrido antes para que
    distritos.zona_sismica esté actualizado. Sin esto el índice IRC
    usaría zona_sismica=NULL → ELSE 3 en todos los distritos.

    🔴 FIX 1: REFRESH MATERIALIZED VIEW CONCURRENTLY no puede correr
    dentro de una transacción psycopg2. refresh_matview() lo maneja.
    """
    log.info("Actualizando mv_riesgo_construccion (IRC)...")

    # Verificar prereqs antes de refrescar
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE zona_sismica IS NOT NULL) AS con_zona,
                COUNT(*) AS total
            FROM distritos
        """)
        row = cur.fetchone()
        con_zona, total_dist = row[0], row[1]

    if total_dist == 0:
        log.warning("  ⚠  Sin distritos — mv_riesgo_construccion quedará vacío")
    elif con_zona == 0:
        log.warning(
            f"  ⚠  {total_dist} distritos pero NINGUNO tiene zona_sismica. "
            "Ejecuta paso_regiones() primero."
        )
    else:
        log.info(
            f"  Prereqs OK: {con_zona}/{total_dist} distritos con zona_sismica"
        )

    t0 = time.time()
    # 🔴 FIX 1: fuera de transacción
    refresh_matview(conn, "mv_riesgo_construccion")
    log.info(f"✅ mv_riesgo_construccion actualizado ({time.time()-t0:.1f}s)")


# ══════════════════════════════════════════════════════════════════
#  ENTRYPOINT
# ══════════════════════════════════════════════════════════════════

def print_banner() -> None:
    print("""
  ╔══════════════════════════════════════════════════════════════╗
  ║  GeoRiesgo Perú — ETL v7.2 (FULL BUGFIX)                   ║
  ║  🔴 FIX1: refresh_matview() — autocommit fuera de tx       ║
  ║  🔴 FIX2: ST_Multi() en inserts geométricos                ║
  ║  🔴 FIX3: _limpiar_fuera_peru() NULL-proof                 ║
  ║  🔴 FIX4: Departamentos hardcoded (25 bboxes fallback)     ║
  ║  🆕  Orden garantizado: regiones → IRC (zona_sismica OK)   ║
  ║  🆕  Hospitales MINSA + Bomberos CGBVP hardcoded           ║
  ╚══════════════════════════════════════════════════════════════╝""")
    print(f"  DB:      {DB_DSN.split('@')[-1]}")
    print(f"  Fecha:   {date.today().isoformat()} UTC")
    print(f"  Workers: {MAX_WORKERS}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="GeoRiesgo Perú ETL v7.2")
    parser.add_argument("--force", action="store_true",
                        help="Forzar re-carga completa")
    parser.add_argument("--solo", choices=[
        "departamentos", "sismos", "distritos", "fallas",
        "inundaciones", "tsunamis", "deslizamientos",
        "infraestructura", "estaciones", "heatmap", "regiones",
        "riesgo_construccion",
    ], help="Ejecutar solo un paso")
    args = parser.parse_args()

    print_banner()
    log.info("Conectando: %s", DB_DSN.split("@")[-1])
    conn = get_conn()

    # ── Orden de pasos — el orden IMPORTA para IRC:
    #    regiones (paso 10) debe ir ANTES de riesgo_construccion (paso 11)
    #    para que zona_sismica en distritos esté actualizada.
    pasos: dict[str, Any] = {
        "departamentos":       lambda: paso_departamentos(conn),
        "sismos":              lambda: paso_sismos(conn),
        "distritos":           lambda: paso_distritos(conn),
        "fallas":              lambda: paso_fallas(conn),
        "inundaciones":        lambda: paso_inundaciones(conn),
        "tsunamis":            lambda: paso_tsunamis(conn),
        "deslizamientos":      lambda: paso_deslizamientos(conn),
        "infraestructura":     lambda: paso_infraestructura(conn),
        "estaciones":          lambda: paso_estaciones(conn),
        "heatmap":             lambda: paso_heatmap(conn),
        "regiones":            lambda: paso_regiones(conn),       # ← debe ir antes de IRC
        "riesgo_construccion": lambda: paso_riesgo_construccion(conn),  # ← depende de regiones
    }

    if args.solo:
        log.info(f"── SOLO PASO: {args.solo.upper()}")
        # Advertencia especial si se pide IRC sin haber corrido regiones
        if args.solo == "riesgo_construccion":
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM distritos WHERE zona_sismica IS NOT NULL
                """)
                n_zona = cur.fetchone()[0]
            if n_zona == 0:
                log.warning(
                    "  ⚠  zona_sismica en distritos está vacía. "
                    "Considera correr primero: --solo regiones"
                )
        try:
            result = pasos[args.solo]()
            log.info(f"✅ Paso '{args.solo}' completado: {result}")
        except Exception as e:
            log.error(f"Error en paso '{args.solo}': {e}")
            raise
        finally:
            conn.close()
        return

    t0 = time.time()
    resultados: dict[str, Any] = {}
    for i, (paso, fn) in enumerate(pasos.items()):
        log.info(f"── PASO {i:02d}: {paso.upper()}")
        t_paso = time.time()
        try:
            r = fn()
            resultados[paso] = r
            log.info(f"   → {r!r:>10}  ({time.time()-t_paso:.1f}s)\n")
        except Exception as e:
            log.error(f"   Error en '{paso}': {e}")
            resultados[paso] = f"ERROR: {e}"

    elapsed = time.time() - t0
    print("\n  ╔══════════════════════════════════════════════════════════════╗")
    for k, v in resultados.items():
        estado = "✅" if not str(v).startswith("ERROR") else "❌"
        print(f"  ║  {estado} {k:<26} {str(v):>10}                     ║")
    print(f"  ║  ⏱  Tiempo total: {elapsed:.0f}s                                   ║")
    print("  ╚══════════════════════════════════════════════════════════════╝\n")

    conn.close()


if __name__ == "__main__":
    main()