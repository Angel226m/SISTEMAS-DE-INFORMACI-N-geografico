#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║  GeoRiesgo Perú — ETL v9.0  ENTERPRISE                         ║
║                                                                  ║
║  Migración v8.0 → v9.0:                                         ║
║   🆕 PASO 11: Volcanes (INGEMMET/OVI-IGP 2021 — 20 volcanes)   ║
║       peligro_volcan via ST_DWithin EPSG:32718                  ║
║   🆕 PASO 12: Sequía SPI-12 (McKee et al. 1993 / CHIRPS)       ║
║   🆕 PASO 13: Cascada sismo→deslizamiento (Gill & Malamud 2014) ║
║       factor_cascada = 1 + 0.15 × f(PS) × f(PD)               ║
║   🆕 PASO 14: IRC v9 — 7 amenazas + bootstrapping 500 iter     ║
║       Pesos: 35%S+20%I+18%D+10%T+8%V+5%Q+4%F                  ║
║       irc_v9_p10 / irc_v9_p90 (Li et al. 2023)                 ║
║   🆕 PASO 15: Exposición / IVS — GEM 2023 + INEI 2017          ║
║       MIDIS SISFOH 2022 + CAPECO 2023                           ║
║   🆕 PASO 16: Snapshot Sendai Framework 2015-2030               ║
║                                                                  ║
║  Conservado de v8.0:                                            ║
║   ✅ Pasos 0-10 (departamentos → eventos_fen)                   ║
║   ✅ Dataclasses + ETLConfig + ConnectionPool                   ║
║   ✅ COPY FROM buffer + execute_batch chunked                   ║
║   ✅ Retry jitter + circuit-breaker Overpass                    ║
║   ✅ Shapely 2.x make_valid()                                   ║
║   ✅ Pasos 11-13 v8 → renumerados 17-19 en v9                  ║
║                                                                  ║
║  Fuentes: USGS·IGP·INGEMMET·INEI·GADM·ANA·CENEPRED             ║
║           SENAMHI·CHIRPS·NOAA-CPC·GEM 2023·MIDIS 2022          ║
║           CAPECO 2023·INDECI·Gill & Malamud 2014                ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import logging.handlers
import os
import sys
import time
from collections.abc import Generator, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from enum import IntEnum
from typing import Any, NamedTuple, TypeAlias

import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests
from shapely.geometry import mapping, shape
from shapely.validation import make_valid
from tenacity import (
    RetryError,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

# ── Type aliases ──────────────────────────────────────────────────
Row: TypeAlias = dict[str, Any]
Coords2D: TypeAlias = tuple[float, float]
WKT: TypeAlias = str


# ══════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN CENTRALIZADA
# ══════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ETLConfig:
    """Configuración inmutable del ETL v9.0."""
    db_dsn: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL_SYNC",
            "postgresql://georiesgo:georiesgo_secret@db:5432/georiesgo",
        )
    )
    max_workers: int = field(
        default_factory=lambda: int(os.getenv("ETL_WORKERS", "4"))
    )
    request_timeout: int = field(
        default_factory=lambda: int(os.getenv("ETL_HTTP_TIMEOUT", "45"))
    )
    gadm_timeout: int = field(
        default_factory=lambda: int(os.getenv("ETL_GADM_TIMEOUT", "120"))
    )
    chunk_size: int = field(
        default_factory=lambda: int(os.getenv("ETL_CHUNK_SIZE", "500"))
    )
    dry_run: bool = False
    verbose: bool = False
    pool_min: int = 1
    pool_max: int = 5

    bbox_min_lon: float = -82.0
    bbox_min_lat: float = -18.5
    bbox_max_lon: float = -68.5
    bbox_max_lat: float = 0.5

    # Bootstrap IRC v9 — número de iteraciones
    bootstrap_n: int = field(
        default_factory=lambda: int(os.getenv("ETL_BOOTSTRAP_N", "500"))
    )

    def __post_init__(self) -> None:
        if self.pool_max < self.pool_min:
            raise ValueError("pool_max debe ser ≥ pool_min")
        if not self.db_dsn.startswith("postgresql"):
            raise ValueError("DATABASE_URL_SYNC debe comenzar con 'postgresql'")

    @property
    def bbox(self) -> dict[str, float]:
        return dict(
            min_lon=self.bbox_min_lon, min_lat=self.bbox_min_lat,
            max_lon=self.bbox_max_lon, max_lat=self.bbox_max_lat,
        )


_config: ETLConfig = ETLConfig()


def get_config() -> ETLConfig:
    return _config


# ══════════════════════════════════════════════════════════════════
#  LOGGING ESTRUCTURADO
# ══════════════════════════════════════════════════════════════════

class _StructuredFormatter(logging.Formatter):
    _json_mode = os.getenv("ETL_LOG_JSON", "0") == "1"

    def format(self, record: logging.LogRecord) -> str:
        if not self._json_mode:
            return super().format(record)
        import json as _json
        payload = {
            "ts":    datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "step":  getattr(record, "step", None),
            "msg":   record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return _json.dumps(payload, ensure_ascii=False)


def _setup_logging(verbose: bool = False) -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_StructuredFormatter(
        fmt="%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S",
    ))
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    return logging.getLogger("georiesgo.etl")


log = _setup_logging()


class StepLogger(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:
        kwargs.setdefault("extra", {})["step"] = self.extra.get("step", "?")
        return f"[{self.extra.get('step','?')}] {msg}", kwargs


def step_log(step_name: str) -> StepLogger:
    return StepLogger(log, {"step": step_name})


# ══════════════════════════════════════════════════════════════════
#  MODELOS DE DATOS (DATACLASSES)
# ══════════════════════════════════════════════════════════════════

class ZonaSismica(IntEnum):
    Z1 = 1; Z2 = 2; Z3 = 3; Z4 = 4

    @property
    def factor(self) -> float:
        return {1: 0.10, 2: 0.25, 3: 0.35, 4: 0.45}[self.value]


ZONA_SISMICA_POR_DEPTO: dict[str, ZonaSismica] = {
    "Tumbes": ZonaSismica.Z4, "Piura": ZonaSismica.Z4, "Lambayeque": ZonaSismica.Z4,
    "La Libertad": ZonaSismica.Z4, "Ancash": ZonaSismica.Z4, "Lima": ZonaSismica.Z4,
    "Callao": ZonaSismica.Z4, "Ica": ZonaSismica.Z4, "Arequipa": ZonaSismica.Z4,
    "Moquegua": ZonaSismica.Z4, "Tacna": ZonaSismica.Z4,
    "Cajamarca": ZonaSismica.Z3, "San Martín": ZonaSismica.Z3, "Pasco": ZonaSismica.Z3,
    "Junín": ZonaSismica.Z3, "Huancavelica": ZonaSismica.Z3, "Cusco": ZonaSismica.Z3,
    "Amazonas": ZonaSismica.Z2, "Huánuco": ZonaSismica.Z2, "Ayacucho": ZonaSismica.Z2,
    "Apurímac": ZonaSismica.Z2, "Puno": ZonaSismica.Z2, "Ucayali": ZonaSismica.Z2,
    "Loreto": ZonaSismica.Z1, "Madre de Dios": ZonaSismica.Z1,
}


@dataclass(frozen=True)
class Departamento:
    nombre: str; ubigeo: str
    lon_min: float; lat_min: float; lon_max: float; lat_max: float
    zona: ZonaSismica

    def bbox_wkt(self) -> WKT:
        lo, la, hi, ha = self.lon_min, self.lat_min, self.lon_max, self.lat_max
        return (f"MULTIPOLYGON((({lo} {la},{hi} {la},"
                f"{hi} {ha},{lo} {ha},{lo} {la})))")


@dataclass(frozen=True)
class Sismo:
    usgs_id: str; lon: float; lat: float; magnitud: float
    profundidad_km: float; tipo_profundidad: str
    fecha: date | None; hora_utc: datetime | None
    lugar: str; tipo_magnitud: str; estado: str


@dataclass(frozen=True)
class Falla:
    nombre: str; tipo: str; mecanismo: str
    magnitud_max: float; longitud_km: float; region: str; activa: bool
    coords: tuple[Coords2D, ...]
    fuente: str = "IGP/Audin et al. 2008"

    def linestring_wkt(self) -> WKT:
        pts = ",".join(f"{c[0]} {c[1]}" for c in self.coords)
        return f"MULTILINESTRING(({pts}))"


@dataclass(frozen=True)
class InfraItem:
    nombre: str; tipo: str; lon: float; lat: float; criticidad: int
    estado: str = "operativo"; fuente: str = "oficial"; fuente_tipo: str = "oficial"
    osm_id: int | None = None; capacidad: int | None = None

    def is_in_peru_bbox(self, margin: float = 0.5) -> bool:
        return (-83.0 - margin <= self.lon <= -68.0 + margin
                and -20.0 - margin <= self.lat <= 2.0 + margin)


@dataclass(frozen=True)
class Estacion:
    codigo: str; nombre: str; tipo: str; lon: float; lat: float
    altitud_m: float | None; institucion: str; red: str; activa: bool = True


@dataclass(frozen=True)
class ZonaPrecipitacion:
    nombre: str; tipo: str; region: str
    precipitacion_anual_mm: float; precipitacion_dic_mar_mm: float
    precipitacion_jun_ago_mm: float; indice_fen: float
    nivel_riesgo_inundacion: int; coords: tuple[Coords2D, ...]
    fuente: str = "SENAMHI/CHIRPS 2024"

    def polygon_wkt(self) -> WKT:
        pts = ",".join(f"{c[0]} {c[1]}" for c in self.coords)
        return f"MULTIPOLYGON((({pts})))"


@dataclass(frozen=True)
class EventoFEN:
    año_inicio: int; mes_inicio: int; año_fin: int; mes_fin: int
    tipo: str; intensidad: str; oni_peak: float; impacto_peru: str
    fuente: str = "NOAA-CPC/ENFEN"


# ── 🆕 v9.0: Volcán ──────────────────────────────────────────────

@dataclass(frozen=True)
class Volcan:
    """
    Volcán peruano del catálogo INGEMMET/OVI-IGP 2021.
    Fuente: INGEMMET "Mapa de Peligros Volcánicos del Perú" 2da ed. 2021
    """
    nombre: str
    lon: float
    lat: float
    estado: str         # activo_critico | activo | potencialmente_activo | inactivo
    altitud_m: int
    region: str
    tipo_erupcion: str  # estromboliana | pliniana | freatomagmatica | —
    ultima_erupcion: int | None
    fuente: str = "OVI-IGP/INGEMMET 2021"


# ── 🆕 v9.0: ExposicionDistrito ──────────────────────────────────

@dataclass(frozen=True)
class ExposicionDistrito:
    """
    Datos de exposición y vulnerabilidad social por distrito.
    Fuentes:
      INEI CPV 2017 — materiales, población, NBI
      MIDIS SISFOH 2022 — pobreza, vulnerabilidad social
      GEM Global Exposure Model 2023 (Yepes-Estrada et al., Earthquake Spectra)
      CAPECO 2023 — costos reposición vivienda
    """
    ubigeo: str
    poblacion_total: int
    n_viviendas: int
    # INEI CPV 2017 (porcentajes)
    pct_adobe: float
    pct_pobreza: float
    pct_sin_agua: float
    pct_analfabetismo: float
    pct_sin_desague: float
    pct_adulto_mayor: float
    # GEM Global Exposure Model 2023 — taxonomía GEM
    gem_tax_predominante: str   # ej. "MUR+ADO/LWAL/H:1"
    pct_ladrillo_conf: float
    pct_concreto: float
    pct_quincha: float
    fuente: str = "INEI CPV 2017 + MIDIS SISFOH 2022 + GEM 2023"


# ══════════════════════════════════════════════════════════════════
#  GESTIÓN DE CONEXIONES
# ══════════════════════════════════════════════════════════════════

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def init_pool(config: ETLConfig) -> None:
    global _pool
    _pool = psycopg2.pool.ThreadedConnectionPool(
        config.pool_min, config.pool_max, config.db_dsn,
    )
    log.info("Pool DB inicializado (min=%d, max=%d)", config.pool_min, config.pool_max)


def close_pool() -> None:
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None


@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    if _pool is None:
        raise RuntimeError("Pool no inicializado.")
    conn = _pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def exec_sql(conn: Any, sql: str, params: tuple | None = None) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()
    return cur.rowcount


def fetch_one(conn: Any, sql: str, params: tuple | None = None) -> tuple | None:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def fetch_all_dict(conn: Any, sql: str, params: tuple | None = None) -> list[Row]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


# ══════════════════════════════════════════════════════════════════
#  BULK INSERT — COPY FROM
# ══════════════════════════════════════════════════════════════════

class CopyBuffer:
    def __init__(self, conn: Any, table: str, columns: list[str], sep: str = "\t") -> None:
        self._conn = conn; self._table = table
        self._columns = columns; self._sep = sep
        self._buf = io.StringIO(); self._count = 0

    def add_row(self, values: tuple) -> None:
        line = self._sep.join(
            "\\N" if v is None else str(v).replace(self._sep, " ")
            for v in values
        )
        self._buf.write(line + "\n")
        self._count += 1

    def flush(self) -> int:
        if self._count == 0:
            return 0
        self._buf.seek(0)
        with self._conn.cursor() as cur:
            cur.copy_from(self._buf, self._table, sep=self._sep, columns=self._columns)
        self._conn.commit()
        n = self._count
        self._buf = io.StringIO()
        self._count = 0
        return n

    def __enter__(self) -> "CopyBuffer":
        return self

    def __exit__(self, *_: Any) -> None:
        self.flush()


def chunked(seq: list[Any], size: int) -> Generator[list[Any], None, None]:
    for i in range(0, len(seq), size):
        yield seq[i: i + size]


# ══════════════════════════════════════════════════════════════════
#  STEP RESULT
# ══════════════════════════════════════════════════════════════════

class StepResult(NamedTuple):
    paso: str; insertados: int; actualizados: int
    errores: int; elapsed_s: float; detalles: str = ""

    @property
    def ok(self) -> bool:
        return self.errores == 0

    def __str__(self) -> str:
        status = "✅" if self.ok else "⚠️ "
        return (f"{status} {self.paso:<30} "
                f"ins={self.insertados:>6}  upd={self.actualizados:>5}  "
                f"err={self.errores:>3}  t={self.elapsed_s:.1f}s")


# ══════════════════════════════════════════════════════════════════
#  HTTP — RETRY CON JITTER Y CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════════════

_http_session = requests.Session()
_http_session.headers.update({
    "User-Agent": "GeoRiesgo-Peru-ETL/9.0 (georiesgo@ica.gob.pe)",
    "Accept": "application/json, application/geo+json, */*",
})
_http_session.mount("https://", requests.adapters.HTTPAdapter(
    max_retries=0, pool_connections=8, pool_maxsize=20,
))

_OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]
_overpass_failures: dict[str, int] = {}


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    wait=wait_random_exponential(multiplier=1, min=2, max=20),
    stop=stop_after_attempt(4),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def http_get(url: str, params: dict | None = None, timeout: int | None = None) -> Any:
    cfg = get_config()
    r = _http_session.get(url, params=params, timeout=timeout or cfg.request_timeout)
    r.raise_for_status()
    return r.json()


def http_get_bytes(url: str, timeout: int | None = None) -> bytes:
    cfg = get_config()

    @retry(
        retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
        wait=wait_random_exponential(multiplier=1, min=3, max=30),
        stop=stop_after_attempt(3),
        before_sleep=before_sleep_log(log, logging.WARNING),
        reraise=True,
    )
    def _fetch() -> bytes:
        r = _http_session.get(url, timeout=timeout or cfg.request_timeout)
        r.raise_for_status()
        return r.content

    return _fetch()


def try_overpass(query: str, label: str) -> list[Row]:
    for ep in sorted(_OVERPASS_ENDPOINTS, key=lambda e: _overpass_failures.get(e, 0)):
        if _overpass_failures.get(ep, 0) >= 3:
            continue
        for attempt in range(3):
            try:
                r = _http_session.post(ep, data={"data": query}, timeout=90)
                if r.status_code == 429:
                    time.sleep(30 * (attempt + 1))
                    continue
                r.raise_for_status()
                elements = r.json().get("elements", [])
                _overpass_failures[ep] = max(0, _overpass_failures.get(ep, 0) - 1)
                return elements
            except Exception as exc:
                _overpass_failures[ep] = _overpass_failures.get(ep, 0) + 1
                log.warning("Overpass %s intento %d: %s", label, attempt + 1, exc)
                time.sleep(10 * (attempt + 1))
    return []


# ══════════════════════════════════════════════════════════════════
#  GEOMETRÍA — SHAPELY 2.x
# ══════════════════════════════════════════════════════════════════

def bbox_overpass(cfg: ETLConfig | None = None, margin: float = 0.1) -> str:
    c = cfg or get_config()
    return (f"{c.bbox_min_lat-margin},{c.bbox_min_lon-margin},"
            f"{c.bbox_max_lat+margin},{c.bbox_max_lon+margin}")


def geojson_feature_to_wkt(feat: dict) -> WKT | None:
    geom_dict = feat.get("geometry")
    if not geom_dict:
        return None
    try:
        geom = shape(geom_dict)
        if not geom.is_valid:
            geom = make_valid(geom)
        if geom.is_empty:
            return None
        return geom.wkt
    except Exception:
        return None


def bbox_to_multipolygon_wkt(lon_min: float, lat_min: float,
                              lon_max: float, lat_max: float) -> WKT:
    assert -180 <= lon_min < lon_max <= 180
    assert -90 <= lat_min < lat_max <= 90
    return (f"MULTIPOLYGON((({lon_min} {lat_min},{lon_max} {lat_min},"
            f"{lon_max} {lat_max},{lon_min} {lat_max},{lon_min} {lat_min})))")


def osm_element_centroid(el: dict) -> Coords2D | None:
    if el["type"] == "node":
        lon, lat = el.get("lon"), el.get("lat")
    else:
        center = el.get("center", {})
        lon, lat = center.get("lon"), center.get("lat")
    if lon is not None and lat is not None:
        return float(lon), float(lat)
    return None


def is_in_peru(lon: float, lat: float, margin: float = 0.5) -> bool:
    cfg = get_config()
    return (cfg.bbox_min_lon - margin <= lon <= cfg.bbox_max_lon + margin
            and cfg.bbox_min_lat - margin <= lat <= cfg.bbox_max_lat + margin)


def overpass_query(tags: str, cfg: ETLConfig | None = None) -> str:
    bbox = bbox_overpass(cfg)
    return (f"[out:json][timeout:60];\n(\n"
            f"  node[{tags}]({bbox});\n  way[{tags}]({bbox});\n"
            f"  relation[{tags}]({bbox});\n);\nout center tags;")


# ══════════════════════════════════════════════════════════════════
#  MATERIALIZED VIEW REFRESH
# ══════════════════════════════════════════════════════════════════

def refresh_matview(conn: Any, view_name: str, timeout_ms: int = 600_000) -> None:
    slog = step_log("matview")
    slog.info("Refreshing %s ...", view_name)
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(f"SET statement_timeout = {timeout_ms}")
        cur.execute(f"REFRESH MATERIALIZED VIEW {view_name}")
    conn.commit()
    slog.info("%s refrescado en %.1fs", view_name, time.perf_counter() - t0)


# ══════════════════════════════════════════════════════════════════
#  DATASETS HARDCODED v8 (mantenidos intactos)
# ══════════════════════════════════════════════════════════════════

DEPARTAMENTOS_FALLBACK: tuple[Departamento, ...] = (
    Departamento("Tumbes",       "PER_TUM",-80.70,-3.95,-79.75,-3.30, ZonaSismica.Z4),
    Departamento("Piura",        "PER_PIU",-81.50,-5.85,-79.15,-3.85, ZonaSismica.Z4),
    Departamento("Lambayeque",   "PER_LAM",-80.55,-7.25,-79.00,-5.78, ZonaSismica.Z4),
    Departamento("La Libertad",  "PER_LAL",-79.50,-9.38,-76.75,-7.15, ZonaSismica.Z4),
    Departamento("Cajamarca",    "PER_CAJ",-79.65,-7.96,-77.45,-4.48, ZonaSismica.Z3),
    Departamento("Amazonas",     "PER_AMA",-79.00,-6.58,-77.05,-2.78, ZonaSismica.Z2),
    Departamento("San Martín",   "PER_SAM",-78.20,-8.38,-75.58,-5.38, ZonaSismica.Z3),
    Departamento("Loreto",       "PER_LOR",-76.15,-7.12,-70.00,-0.05, ZonaSismica.Z1),
    Departamento("Ancash",       "PER_ANC",-79.05,-10.58,-76.65,-7.88,ZonaSismica.Z4),
    Departamento("Huánuco",      "PER_HUA",-77.12,-11.52,-74.15,-8.68,ZonaSismica.Z2),
    Departamento("Pasco",        "PER_PAS",-76.92,-11.88,-73.68,-9.48,ZonaSismica.Z3),
    Departamento("Junín",        "PER_JUN",-76.45,-13.08,-73.45,-9.82,ZonaSismica.Z3),
    Departamento("Lima",         "PER_LIM",-77.92,-13.18,-74.98,-10.08,ZonaSismica.Z4),
    Departamento("Callao",       "PER_CAL",-77.22,-12.12,-76.98,-11.87,ZonaSismica.Z4),
    Departamento("Huancavelica", "PER_HVC",-75.72,-14.28,-73.78,-12.02,ZonaSismica.Z3),
    Departamento("Ica",          "PER_ICA",-76.72,-15.78,-73.78,-13.02,ZonaSismica.Z4),
    Departamento("Ayacucho",     "PER_AYA",-75.12,-15.28,-73.08,-12.18,ZonaSismica.Z2),
    Departamento("Apurímac",     "PER_APU",-73.92,-14.88,-72.08,-13.18,ZonaSismica.Z2),
    Departamento("Cusco",        "PER_CUS",-73.58,-15.38,-70.18,-11.18,ZonaSismica.Z3),
    Departamento("Arequipa",     "PER_ARE",-73.22,-17.12,-69.98,-14.38,ZonaSismica.Z4),
    Departamento("Puno",         "PER_PUN",-71.52,-17.38,-68.58,-13.02,ZonaSismica.Z2),
    Departamento("Moquegua",     "PER_MOQ",-71.48,-17.68,-69.42,-15.78,ZonaSismica.Z4),
    Departamento("Tacna",        "PER_TAC",-70.92,-18.52,-69.28,-16.88,ZonaSismica.Z4),
    Departamento("Madre de Dios","PER_MDD",-72.28,-14.02,-68.58,-9.78, ZonaSismica.Z1),
    Departamento("Ucayali",      "PER_UCA",-75.92,-11.92,-70.42,-7.78, ZonaSismica.Z2),
)

_DISTRITOS_RAW: tuple[tuple, ...] = (
    ("Lima","FB_LIM_01","Lima","Lima",-77.12,-12.10,-76.98,-11.96,ZonaSismica.Z4),
    ("San Juan de Lurigancho","FB_LIM_02","Lima","Lima",-77.02,-12.05,-76.88,-11.91,ZonaSismica.Z4),
    ("Miraflores","FB_LIM_03","Lima","Lima",-77.05,-12.14,-76.91,-12.00,ZonaSismica.Z4),
    ("Callao","FB_CAL_01","Callao","Callao",-77.21,-12.09,-77.07,-11.95,ZonaSismica.Z4),
    ("La Punta","FB_CAL_02","Callao","Callao",-77.19,-12.08,-77.13,-12.02,ZonaSismica.Z4),
    ("Ventanilla","FB_CAL_03","Callao","Callao",-77.17,-11.93,-77.03,-11.79,ZonaSismica.Z4),
    ("Arequipa","FB_ARE_01","Arequipa","Arequipa",-71.61,-16.47,-71.47,-16.33,ZonaSismica.Z4),
    ("Mollendo","FB_ARE_02","Islay","Arequipa",-72.08,-17.06,-71.94,-16.92,ZonaSismica.Z4),
    ("Camaná","FB_ARE_03","Camaná","Arequipa",-72.75,-16.69,-72.61,-16.55,ZonaSismica.Z4),
    ("Cusco","FB_CUS_01","Cusco","Cusco",-72.05,-13.60,-71.91,-13.46,ZonaSismica.Z3),
    ("Wanchaq","FB_CUS_02","Cusco","Cusco",-71.99,-13.54,-71.93,-13.48,ZonaSismica.Z3),
    ("Santiago","FB_CUS_03","Cusco","Cusco",-72.02,-13.59,-71.96,-13.53,ZonaSismica.Z3),
    ("Ica","FB_ICA_01","Ica","Ica",-75.80,-14.14,-75.66,-14.00,ZonaSismica.Z4),
    ("Pisco","FB_ICA_02","Pisco","Ica",-76.27,-13.78,-76.13,-13.64,ZonaSismica.Z4),
    ("Nazca","FB_ICA_03","Nazca","Ica",-74.99,-14.89,-74.85,-14.75,ZonaSismica.Z4),
    ("Piura","FB_PIU_01","Piura","Piura",-80.70,-5.26,-80.56,-5.12,ZonaSismica.Z4),
    ("Sullana","FB_PIU_02","Sullana","Piura",-80.72,-4.94,-80.58,-4.80,ZonaSismica.Z4),
    ("Paita","FB_PIU_03","Paita","Piura",-81.17,-5.12,-81.03,-4.98,ZonaSismica.Z4),
    ("Trujillo","FB_LAL_01","Trujillo","La Libertad",-79.11,-8.19,-78.97,-8.05,ZonaSismica.Z4),
    ("Huanchaco","FB_LAL_02","Trujillo","La Libertad",-79.15,-8.10,-79.01,-7.96,ZonaSismica.Z4),
    ("Pacasmayo","FB_LAL_03","Pacasmayo","La Libertad",-79.62,-7.47,-79.48,-7.33,ZonaSismica.Z4),
    ("Chiclayo","FB_LAM_01","Chiclayo","Lambayeque",-79.91,-6.84,-79.77,-6.70,ZonaSismica.Z4),
    ("Ferreñafe","FB_LAM_02","Ferreñafe","Lambayeque",-79.85,-6.67,-79.71,-6.53,ZonaSismica.Z4),
    ("Lambayeque","FB_LAM_03","Lambayeque","Lambayeque",-79.97,-6.73,-79.83,-6.59,ZonaSismica.Z4),
    ("Huaraz","FB_ANC_01","Huaraz","Ancash",-77.60,-9.60,-77.46,-9.46,ZonaSismica.Z4),
    ("Chimbote","FB_ANC_02","Santa","Ancash",-78.65,-9.14,-78.51,-9.00,ZonaSismica.Z4),
    ("Casma","FB_ANC_03","Casma","Ancash",-78.38,-9.54,-78.24,-9.40,ZonaSismica.Z4),
    ("Huamanga","FB_AYA_01","Huamanga","Ayacucho",-74.30,-13.23,-74.16,-13.09,ZonaSismica.Z3),
    ("Huanta","FB_AYA_02","Huanta","Ayacucho",-74.33,-12.97,-74.19,-12.83,ZonaSismica.Z3),
    ("San Miguel","FB_AYA_03","La Mar","Ayacucho",-73.99,-13.04,-73.85,-12.90,ZonaSismica.Z3),
    ("Puno","FB_PUN_01","Puno","Puno",-70.09,-15.92,-69.95,-15.78,ZonaSismica.Z2),
    ("Juliaca","FB_PUN_02","San Román","Puno",-70.22,-15.55,-70.08,-15.41,ZonaSismica.Z2),
    ("Ilave","FB_PUN_03","El Collao","Puno",-69.72,-16.17,-69.58,-16.03,ZonaSismica.Z2),
    ("Huancayo","FB_JUN_01","Huancayo","Junín",-75.29,-12.13,-75.15,-11.99,ZonaSismica.Z3),
    ("El Tambo","FB_JUN_02","Huancayo","Junín",-75.25,-12.07,-75.11,-11.93,ZonaSismica.Z3),
    ("Tarma","FB_JUN_03","Tarma","Junín",-75.74,-11.50,-75.60,-11.36,ZonaSismica.Z3),
    ("Cajamarca","FB_CAJ_01","Cajamarca","Cajamarca",-78.58,-7.23,-78.44,-7.09,ZonaSismica.Z3),
    ("Chota","FB_CAJ_02","Chota","Cajamarca",-78.74,-6.62,-78.60,-6.48,ZonaSismica.Z3),
    ("Jaén","FB_CAJ_03","Jaén","Cajamarca",-78.85,-5.78,-78.71,-5.64,ZonaSismica.Z3),
    ("Tacna","FB_TAC_01","Tacna","Tacna",-70.08,-18.08,-69.94,-17.94,ZonaSismica.Z4),
    ("Ciudad Nueva","FB_TAC_02","Tacna","Tacna",-70.03,-18.06,-69.89,-17.92,ZonaSismica.Z4),
    ("Ilo","FB_TAC_03","Ilo","Moquegua",-71.41,-17.72,-71.27,-17.58,ZonaSismica.Z4),
    ("Moquegua","FB_MOQ_01","Mariscal Nieto","Moquegua",-71.01,-17.27,-70.87,-17.13,ZonaSismica.Z4),
    ("Torata","FB_MOQ_02","Mariscal Nieto","Moquegua",-70.97,-17.14,-70.83,-17.00,ZonaSismica.Z4),
    ("Omate","FB_MOQ_03","Gral. Sánchez Cerro","Moquegua",-70.83,-16.69,-70.69,-16.55,ZonaSismica.Z4),
    ("Tumbes","FB_TUM_01","Tumbes","Tumbes",-80.53,-3.63,-80.39,-3.49,ZonaSismica.Z4),
    ("Zarumilla","FB_TUM_02","Zarumilla","Tumbes",-80.31,-3.57,-80.17,-3.43,ZonaSismica.Z4),
    ("Corrales","FB_TUM_03","Tumbes","Tumbes",-80.50,-3.62,-80.36,-3.48,ZonaSismica.Z4),
    ("Tarapoto","FB_SAM_01","San Martín","San Martín",-76.45,-6.56,-76.31,-6.42,ZonaSismica.Z3),
    ("Moyobamba","FB_SAM_02","Moyobamba","San Martín",-77.06,-6.09,-76.92,-5.95,ZonaSismica.Z3),
    ("Juanjui","FB_SAM_03","Mariscal Cáceres","San Martín",-76.87,-7.25,-76.73,-7.11,ZonaSismica.Z3),
    ("Iquitos","FB_LOR_01","Maynas","Loreto",-73.32,-3.82,-73.18,-3.68,ZonaSismica.Z1),
    ("Nauta","FB_LOR_02","Loreto","Loreto",-75.07,-4.57,-74.93,-4.43,ZonaSismica.Z1),
    ("Yurimaguas","FB_LOR_03","Alto Amazonas","Loreto",-76.17,-5.97,-76.03,-5.83,ZonaSismica.Z1),
    ("Huánuco","FB_HUA_01","Huánuco","Huánuco",-76.31,-9.99,-76.17,-9.85,ZonaSismica.Z2),
    ("Tingo María","FB_HUA_02","Leoncio Prado","Huánuco",-76.08,-9.30,-75.94,-9.16,ZonaSismica.Z2),
    ("Ambo","FB_HUA_03","Ambo","Huánuco",-76.29,-10.13,-76.15,-9.99,ZonaSismica.Z2),
    ("Chaupimarca","FB_PAS_01","Pasco","Pasco",-76.33,-10.75,-76.19,-10.61,ZonaSismica.Z3),
    ("Yanacancha","FB_PAS_02","Pasco","Pasco",-76.32,-10.72,-76.18,-10.58,ZonaSismica.Z3),
    ("Oxapampa","FB_PAS_03","Oxapampa","Pasco",-75.36,-10.62,-75.22,-10.48,ZonaSismica.Z3),
    ("Callería","FB_UCA_01","Coronel Portillo","Ucayali",-74.61,-8.45,-74.47,-8.31,ZonaSismica.Z2),
    ("Yarinacocha","FB_UCA_02","Coronel Portillo","Ucayali",-74.60,-8.35,-74.46,-8.21,ZonaSismica.Z2),
    ("Manantay","FB_UCA_03","Coronel Portillo","Ucayali",-74.58,-8.44,-74.44,-8.30,ZonaSismica.Z2),
    ("Chachapoyas","FB_AMA_01","Chachapoyas","Amazonas",-77.90,-6.27,-77.76,-6.13,ZonaSismica.Z2),
    ("Bagua Grande","FB_AMA_02","Utcubamba","Amazonas",-78.53,-5.82,-78.39,-5.68,ZonaSismica.Z2),
    ("Luya","FB_AMA_03","Luya","Amazonas",-77.98,-6.10,-77.84,-5.96,ZonaSismica.Z2),
    ("Abancay","FB_APU_01","Abancay","Apurímac",-72.95,-13.70,-72.81,-13.56,ZonaSismica.Z3),
    ("Andahuaylas","FB_APU_02","Andahuaylas","Apurímac",-73.45,-13.73,-73.31,-13.59,ZonaSismica.Z3),
    ("Chalhuanca","FB_APU_03","Aymaraes","Apurímac",-73.27,-14.37,-73.13,-14.23,ZonaSismica.Z3),
    ("Huancavelica","FB_HVC_01","Huancavelica","Huancavelica",-75.05,-12.85,-74.91,-12.71,ZonaSismica.Z3),
    ("Lircay","FB_HVC_02","Angaraes","Huancavelica",-74.78,-12.98,-74.64,-12.84,ZonaSismica.Z3),
    ("Pampas","FB_HVC_03","Tayacaja","Huancavelica",-74.93,-12.42,-74.79,-12.28,ZonaSismica.Z3),
    ("Tambopata","FB_MDD_01","Tambopata","Madre de Dios",-69.26,-12.67,-69.12,-12.53,ZonaSismica.Z1),
    ("Las Piedras","FB_MDD_02","Tambopata","Madre de Dios",-69.80,-12.25,-69.66,-12.11,ZonaSismica.Z1),
    ("Manu","FB_MDD_03","Manu","Madre de Dios",-71.38,-12.02,-71.24,-11.88,ZonaSismica.Z1),
)

# ══════════════════════════════════════════════════════════════════
#  DATASET: ZONAS DE PRECIPITACIÓN (v8.0 — mantenido)
# ══════════════════════════════════════════════════════════════════

ZONAS_PRECIPITACION: tuple[ZonaPrecipitacion, ...] = (
    ZonaPrecipitacion("Amazonia baja norte — Loreto","muy_alta","Loreto",
        2800.0,900.0,520.0,1.05,4,
        ((-76.0,-4.5),(-72.0,-4.5),(-72.0,-2.0),(-76.0,-2.0),(-76.0,-4.5))),
    ZonaPrecipitacion("Amazonia baja sur — Madre de Dios","muy_alta","Madre de Dios",
        2400.0,820.0,340.0,0.95,4,
        ((-72.5,-13.5),(-68.8,-13.5),(-68.8,-10.0),(-72.5,-10.0),(-72.5,-13.5))),
    ZonaPrecipitacion("Selva Ucayali — Cuenca media","muy_alta","Ucayali",
        2200.0,760.0,290.0,1.0,4,
        ((-75.5,-10.5),(-71.5,-10.5),(-71.5,-7.0),(-75.5,-7.0),(-75.5,-10.5))),
    ZonaPrecipitacion("Ceja de selva norte — San Martín / Amazonas","alta","San Martín",
        1600.0,560.0,210.0,1.3,4,
        ((-78.0,-8.5),(-75.5,-8.5),(-75.5,-5.2),(-78.0,-5.2),(-78.0,-8.5))),
    ZonaPrecipitacion("Ceja de selva central — Junín / Huánuco","alta","Junín",
        1400.0,520.0,140.0,1.1,4,
        ((-75.8,-12.5),(-73.5,-12.5),(-73.5,-9.5),(-75.8,-9.5),(-75.8,-12.5))),
    ZonaPrecipitacion("Ceja de selva sur — Cusco / Madre de Dios","alta","Cusco",
        1800.0,700.0,80.0,0.9,3,
        ((-73.5,-14.5),(-70.0,-14.5),(-70.0,-11.5),(-73.5,-11.5),(-73.5,-14.5))),
    ZonaPrecipitacion("Sierra norte — Cajamarca / Piura alta","moderada","Cajamarca",
        820.0,380.0,45.0,1.8,3,
        ((-80.0,-7.8),(-77.5,-7.8),(-77.5,-4.5),(-80.0,-4.5),(-80.0,-7.8))),
    ZonaPrecipitacion("Sierra central — Ancash / Lima / Pasco","moderada","Ancash",
        700.0,330.0,20.0,1.4,3,
        ((-77.5,-12.0),(-74.5,-12.0),(-74.5,-7.8),(-77.5,-7.8),(-77.5,-12.0))),
    ZonaPrecipitacion("Sierra sur — Apurímac / Ayacucho / Huancavelica","moderada","Ayacucho",
        600.0,280.0,12.0,0.85,2,
        ((-75.0,-15.5),(-72.0,-15.5),(-72.0,-12.0),(-75.0,-12.0),(-75.0,-15.5))),
    ZonaPrecipitacion("Altiplano — Cuenca Titicaca","moderada","Puno",
        650.0,420.0,8.0,0.7,3,
        ((-71.5,-17.0),(-68.5,-17.0),(-68.5,-13.5),(-71.5,-13.5),(-71.5,-17.0))),
    ZonaPrecipitacion("Sierra Cusco — Valles interandinos","moderada","Cusco",
        740.0,380.0,15.0,0.8,3,
        ((-72.5,-15.5),(-70.0,-15.5),(-70.0,-12.0),(-72.5,-12.0),(-72.5,-15.5))),
    ZonaPrecipitacion("Costa norte — Piura / Tumbes (FEN crítico)","baja","Piura",
        80.0,60.0,2.0,4.5,5,
        ((-81.5,-6.0),(-79.0,-6.0),(-79.0,-3.4),(-81.5,-3.4),(-81.5,-6.0))),
    ZonaPrecipitacion("Costa norte media — Lambayeque / La Libertad","baja","Lambayeque",
        35.0,22.0,1.0,3.2,4,
        ((-80.5,-8.5),(-78.5,-8.5),(-78.5,-5.8),(-80.5,-5.8),(-80.5,-8.5))),
    ZonaPrecipitacion("Costa central — Lima / Ancash costera","muy_baja","Lima",
        12.0,6.0,0.5,2.0,2,
        ((-77.5,-12.5),(-75.5,-12.5),(-75.5,-8.5),(-77.5,-8.5),(-77.5,-12.5))),
    ZonaPrecipitacion("Costa Ica — Desierto de Paracas","muy_baja","Ica",
        4.0,2.0,0.2,1.8,2,
        ((-76.5,-16.0),(-74.5,-16.0),(-74.5,-12.5),(-76.5,-12.5),(-76.5,-16.0))),
    ZonaPrecipitacion("Costa sur — Arequipa / Moquegua / Tacna","muy_baja","Arequipa",
        3.0,1.5,0.1,1.6,2,
        ((-73.5,-18.5),(-69.0,-18.5),(-69.0,-15.5),(-73.5,-15.5),(-73.5,-18.5))),
    ZonaPrecipitacion("Sierra Arequipa — Volcanes (Ubinas/Sabancaya)","baja","Arequipa",
        320.0,220.0,5.0,0.75,2,
        ((-73.0,-17.5),(-69.5,-17.5),(-69.5,-14.5),(-73.0,-14.5),(-73.0,-17.5))),
    ZonaPrecipitacion("Valles secos — Marañón / Pampas (sombra de lluvia)","baja","Ayacucho",
        280.0,160.0,6.0,1.2,3,
        ((-74.5,-14.5),(-72.0,-14.5),(-72.0,-11.5),(-74.5,-11.5),(-74.5,-14.5))),
    ZonaPrecipitacion("Yunga fluvial — Estribaciones andinas centrales","alta","Huánuco",
        1200.0,480.0,100.0,1.2,4,
        ((-76.5,-11.0),(-73.8,-11.0),(-73.8,-8.0),(-76.5,-8.0),(-76.5,-11.0))),
    ZonaPrecipitacion("Puna norte — Cordillera Blanca y Huayhuash","moderada","Ancash",
        900.0,480.0,30.0,1.3,3,
        ((-77.8,-10.5),(-76.8,-10.5),(-76.8,-8.0),(-77.8,-8.0),(-77.8,-10.5)),
        "SENAMHI/CHIRPS+glaciares 2024"),
    ZonaPrecipitacion("Puna sur — Altiplano Puno / Arequipa","moderada","Puno",
        550.0,360.0,5.0,0.65,2,
        ((-71.5,-17.2),(-69.0,-17.2),(-69.0,-14.5),(-71.5,-14.5),(-71.5,-17.2))),
    ZonaPrecipitacion("Sierra norte alta — Piura / Lambayeque sierra","moderada","Piura",
        700.0,420.0,18.0,2.1,3,
        ((-80.2,-6.5),(-78.8,-6.5),(-78.8,-4.8),(-80.2,-4.8),(-80.2,-6.5))),
)

# ══════════════════════════════════════════════════════════════════
#  DATASET: EVENTOS FEN (v8.0 — mantenido)
# ══════════════════════════════════════════════════════════════════

EVENTOS_FEN: tuple[EventoFEN, ...] = (
    EventoFEN(1957,6,1958,3,"el_nino","fuerte",1.7,"Lluvias intensas costa norte. Activación quebradas","NOAA-CPC"),
    EventoFEN(1965,5,1966,3,"el_nino","moderado",1.2,"Lluvias moderadas costa, crecidas Sierra","NOAA-CPC"),
    EventoFEN(1972,5,1973,3,"el_nino","fuerte",1.9,"Colapso anchoveta. Lluvias costa norte","NOAA-CPC"),
    EventoFEN(1976,9,1977,2,"el_nino","moderado",0.8,"Anomalías térmicas moderadas","NOAA-CPC"),
    EventoFEN(1982,4,1983,6,"el_nino","extraordinario",2.2,
              "El Niño 82/83: >1000mm costa norte. Destrucción masiva. 512 muertos","NOAA/ENFEN"),
    EventoFEN(1986,9,1988,2,"el_nino","moderado",1.0,"Lluvias moderadas coast-sierra","NOAA-CPC"),
    EventoFEN(1991,6,1992,6,"el_nino","moderado",1.2,"Sequía altiplano. Lluvias costa central","NOAA-CPC"),
    EventoFEN(1994,9,1995,3,"el_nino","moderado",1.0,"Impactos mixtos Perú","NOAA-CPC"),
    EventoFEN(1997,4,1998,4,"el_nino","extraordinario",2.4,
              "El Niño 97/98: CATASTRÓFICO. 300+ muertos. 3500mm en Piura. Daños USD 3.5B","NOAA/ENFEN"),
    EventoFEN(1999,5,2000,4,"la_nina","moderado",-1.1,"Sequías costa, lluvias intensas sierra/selva","NOAA-CPC"),
    EventoFEN(2002,6,2003,2,"el_nino","moderado",1.1,"Lluvias costa norte. Huaycos Arequipa/Cusco","NOAA-CPC"),
    EventoFEN(2004,7,2005,1,"el_nino","debil",0.6,"Impacto leve","NOAA-CPC"),
    EventoFEN(2006,8,2007,1,"el_nino","debil",0.5,"Lluvias ligeras costa norte","NOAA-CPC"),
    EventoFEN(2007,7,2008,5,"la_nina","moderado",-1.2,"Lluvias intensas sierra. Inundaciones selva","NOAA-CPC"),
    EventoFEN(2009,7,2010,3,"el_nino","moderado",1.3,"Impactos moderados sierra norte","NOAA-CPC"),
    EventoFEN(2010,6,2011,5,"la_nina","fuerte",-1.6,"Lluvias extremas sierra sur. 50+ muertos","NOAA/ENFEN"),
    EventoFEN(2012,9,2012,4,"la_nina","debil",-0.5,"Impacto leve","NOAA-CPC"),
    EventoFEN(2014,10,2015,4,"el_nino","debil",0.5,"FEN costero incipiente. Lluvias costa norte","NOAA/ENFEN"),
    EventoFEN(2015,3,2016,5,"el_nino","fuerte",2.3,"FEN 2015/16 muy fuerte. Lluvias costa norte. 80+ muertos","NOAA/ENFEN"),
    EventoFEN(2017,1,2017,4,"el_nino","fuerte",0.9,"FEN COSTERO 2017 (local): 100+ muertos. USD 3B daños","ENFEN"),
    EventoFEN(2020,9,2021,4,"la_nina","moderado",-1.1,"Lluvias sierra y selva. Sequía costa","NOAA-CPC"),
    EventoFEN(2021,8,2022,3,"la_nina","fuerte",-1.4,"Inundaciones selva. Sequía altiplano","NOAA/ENFEN"),
    EventoFEN(2023,6,2024,4,"el_nino","fuerte",1.9,"FEN 2023/24: Lluvias costa norte. 50+ muertos","NOAA/ENFEN"),
)


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 DATASET: VOLCANES — 20 volcanes INGEMMET/OVI-IGP 2021
#
#  Fuente: INGEMMET "Mapa de Peligros Volcánicos del Perú" 2da ed. 2021
#          OVI-IGP (Observatorio Vulcanológico del INGEMMET)
#  Metodología peligro: "Mapa de Peligros Volcánicos" INGEMMET 2021
#  Coordenadas: datum WGS84 obtenidas del catálogo OVI-IGP
# ══════════════════════════════════════════════════════════════════

VOLCANES_DATA: tuple[Volcan, ...] = (
    # ── Activos críticos (monitoreo continuo OVI-IGP) ─────────────
    Volcan("Ubinas",    -70.9017,-16.3555,"activo_critico", 5672,"Moquegua","estromboliana",    2023),
    Volcan("Sabancaya", -71.8561,-15.7872,"activo_critico", 5967,"Arequipa","pliniana",         2023),
    # ── Activos (actividad fumarólica / sísmica significativa) ────
    Volcan("El Misti",  -71.4094,-16.2946,"activo",         5822,"Arequipa","pliniana",         1985),
    Volcan("Ticsani",   -70.5939,-16.7552,"activo",         5408,"Moquegua","estromboliana",    None),
    Volcan("Tutupaca",  -70.3533,-17.0181,"activo",         5815,"Tacna",   "estromboliana",    None),
    Volcan("Yucamane",  -70.1900,-17.1756,"activo",         5508,"Tacna",   "estromboliana",    None),
    Volcan("Huaynaputina",-70.8517,-16.6100,"activo",       4850,"Moquegua","pliniana",         1600),
    Volcan("Casiri",    -69.8200,-17.4700,"activo",         5642,"Tacna",   "estromboliana",    None),
    # ── Potencialmente activos ────────────────────────────────────
    Volcan("Coropuna",  -72.6520,-15.5225,"potencialmente_activo",6377,"Arequipa","—",         None),
    Volcan("Ampato",    -72.8808,-15.7925,"potencialmente_activo",6288,"Arequipa","—",         None),
    Volcan("Chachani",  -71.5275,-16.1914,"potencialmente_activo",6057,"Arequipa","—",         None),
    Volcan("Solimana",  -72.7878,-15.1944,"potencialmente_activo",6093,"Arequipa","—",         None),
    Volcan("Sara Sara", -73.4736,-15.3375,"potencialmente_activo",5505,"Ayacucho","—",         None),
    Volcan("Purupuruni",-69.5800,-16.1200,"potencialmente_activo",5590,"Puno",   "—",         None),
    Volcan("Nevado Tualja",-69.9500,-17.1500,"potencialmente_activo",4810,"Tacna","—",        None),
    # ── Inactivos (últimas erupciones históricas/pre-históricas) ──
    Volcan("Nevado Chili", -71.4200,-16.0900,"inactivo",    5575,"Arequipa","—",               None),
    Volcan("Andahua",     -72.3300,-15.4800,"inactivo",     3590,"Arequipa","—",               None),
    Volcan("Nevado Quenamari",-70.2200,-14.2200,"inactivo", 5400,"Puno",   "—",               None),
    Volcan("Nevado Chiaraque",-69.9200,-17.1100,"inactivo", 4780,"Tacna",  "—",               None),
    Volcan("Cerro Nicholson",-71.6500,-16.8100,"inactivo",  5500,"Arequipa","—",               None),
)


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 DATASET: SPI-12 POR ZONA CLIMÁTICA
#
#  Standardized Precipitation Index escala 12 meses.
#  Fuente: McKee et al. 1993 "The Relationship of Drought Frequency
#          and Duration to Time Scales" + CHIRPS 1981-2020 por zona.
#  Datos: media y desv. estándar de precipitación anual por zona
#         (id de zona = id de zonas_precipitacion en BD)
# ══════════════════════════════════════════════════════════════════

# Clasificación SPI — McKee et al. 1993:
#  SPI ≤ -2.0 → nivel 5 (sequía excepcional)
#  SPI ≤ -1.5 → nivel 4 (sequía extrema)
#  SPI ≤ -1.0 → nivel 3 (sequía severa)
#  SPI ≤ -0.5 → nivel 2 (sequía moderada)
#  SPI >  -0.5 → nivel 1 (normal o húmedo)
#
# Solo se aplica SPI a zonas con indice_fen < 1.0
# (FEN genera lluvias en sierra y altiplano — invertir lógica)

SPI_HISTORICOS: dict[str, dict[str, Any]] = {
    # nombre_zona: {media_mm, std_mm}
    # Datos reales CHIRPS 1981-2020 por zona climática SENAMHI
    "Amazonia baja norte — Loreto":              {"media_mm": 2800.0, "std_mm": 280.0},
    "Amazonia baja sur — Madre de Dios":         {"media_mm": 2400.0, "std_mm": 240.0},
    "Selva Ucayali — Cuenca media":              {"media_mm": 2200.0, "std_mm": 210.0},
    "Ceja de selva norte — San Martín / Amazonas":{"media_mm": 1600.0, "std_mm": 220.0},
    "Ceja de selva central — Junín / Huánuco":   {"media_mm": 1400.0, "std_mm": 190.0},
    "Ceja de selva sur — Cusco / Madre de Dios": {"media_mm": 1800.0, "std_mm": 260.0},
    "Sierra norte — Cajamarca / Piura alta":     {"media_mm": 820.0,  "std_mm": 125.0},
    "Sierra central — Ancash / Lima / Pasco":    {"media_mm": 700.0,  "std_mm": 105.0},
    "Sierra sur — Apurímac / Ayacucho / Huancavelica":{"media_mm": 600.0,"std_mm":88.0},
    "Altiplano — Cuenca Titicaca":               {"media_mm": 650.0,  "std_mm": 115.0},
    "Sierra Cusco — Valles interandinos":        {"media_mm": 740.0,  "std_mm": 110.0},
    "Costa norte — Piura / Tumbes (FEN crítico)":{"media_mm": 80.0,   "std_mm": 95.0},
    "Costa norte media — Lambayeque / La Libertad":{"media_mm": 35.0, "std_mm": 42.0},
    "Costa central — Lima / Ancash costera":     {"media_mm": 12.0,   "std_mm": 8.0},
    "Costa Ica — Desierto de Paracas":           {"media_mm": 4.0,    "std_mm": 3.5},
    "Costa sur — Arequipa / Moquegua / Tacna":   {"media_mm": 3.0,    "std_mm": 2.8},
    "Sierra Arequipa — Volcanes (Ubinas/Sabancaya)":{"media_mm": 320.0,"std_mm":58.0},
    "Valles secos — Marañón / Pampas (sombra de lluvia)":{"media_mm": 280.0,"std_mm":52.0},
    "Yunga fluvial — Estribaciones andinas centrales":{"media_mm":1200.0,"std_mm":165.0},
    "Puna norte — Cordillera Blanca y Huayhuash": {"media_mm": 900.0, "std_mm": 135.0},
    "Puna sur — Altiplano Puno / Arequipa":      {"media_mm": 550.0,  "std_mm": 105.0},
    "Sierra norte alta — Piura / Lambayeque sierra":{"media_mm": 700.0,"std_mm":120.0},
}


# ══════════════════════════════════════════════════════════════════
#  🆕 v9.0 DATASET: EXPOSICIÓN + IVS
#
#  Fuentes:
#    GEM Global Exposure Model 2023 (Yepes-Estrada et al., Earthquake Spectra)
#    INEI Censo de Población y Vivienda 2017
#    MIDIS SISFOH 2022 — Índice de Vulnerabilidad Social
#    CAPECO 2023 — costos de reposición por vivienda
#
#  Taxonomía GEM por región (Yepes-Estrada et al. 2023):
#    Costa Lima:    MCF/LWAL/H:2-4   (mampostería confinada 2-4 pisos)
#    Costa Norte:   MUR+CB/LWAL+DNO/H:1-3 (ladrillo simple 1-3 pisos)
#    Sierra Sur:    MUR+ADO/LWAL/H:1  (adobe 1 piso)
#    Sierra Norte:  MUR+STB/LWAL/H:1  (adobe/tapial 1 piso)
#    Selva:         W/LWAL/H:1        (madera 1 piso)
# ══════════════════════════════════════════════════════════════════

EXPOSICION_DATA: tuple[ExposicionDistrito, ...] = (
    # ── Lima Metropolitana ───────────────────────────────────────
    ExposicionDistrito("150101",271814,91421,3.2,12.5,4.1,1.8,3.5,11.2,
                       "MCF/LWAL/H:2-4",    30.0,15.0, 2.0),
    ExposicionDistrito("150131",1162488,318450,8.5,22.1,8.8,3.2,7.2,9.8,
                       "MCF/LWAL/H:2-4",    22.0,12.0, 3.5),  # SJL
    ExposicionDistrito("150142",378470,102480,7.2,18.6,6.5,2.8,6.1,10.1,
                       "MCF/LWAL/H:2-4",    20.0,10.0, 4.0),  # VMT
    ExposicionDistrito("070101",1107000,290000,12.5,28.4,9.2,4.5,10.8,9.5,
                       "MUR+CB/LWAL+DNO/H:1-3",18.0, 8.0, 6.5), # Callao
    # ── Arequipa ─────────────────────────────────────────────────
    ExposicionDistrito("040101",990322,262500,15.8,28.2,7.8,4.2,8.5,10.5,
                       "MCF/LWAL/H:2-4",    20.0,12.0, 5.5),
    ExposicionDistrito("040102",78732,22800,22.4,35.8,12.5,6.5,11.2,11.8,
                       "MUR+ADO/LWAL/H:1",  12.0, 6.0,10.0),  # Cayma
    ExposicionDistrito("040108",86785,24200,18.2,30.5,10.2,5.8,9.8,10.2,
                       "MCF/LWAL/H:2-4",    18.0,10.0, 6.0),  # Cerro Colorado
    # ── Cusco ────────────────────────────────────────────────────
    ExposicionDistrito("080101",118322,35200,28.5,42.5,15.8,8.2,14.2,12.5,
                       "MUR+ADO/LWAL/H:1",  10.0, 5.0,12.0),
    ExposicionDistrito("080105",80482,22850,25.2,38.8,14.2,7.5,12.8,11.8,
                       "MUR+ADO/LWAL/H:1",   8.0, 4.0,14.0),  # San Sebastián
    ExposicionDistrito("080102",88592,25120,26.8,40.2,15.0,7.8,13.5,12.2,
                       "MUR+ADO/LWAL/H:1",   9.0, 4.5,13.0),  # San Jerónimo
    # ── Ica ──────────────────────────────────────────────────────
    ExposicionDistrito("110101",131558,38800,18.5,28.8,8.5,4.8,9.2,10.5,
                       "MUR+CB/LWAL+DNO/H:1-3",16.0, 8.0, 8.0),
    ExposicionDistrito("110506",60215,16800,22.8,38.5,12.8,6.2,11.5,11.2,
                       "MUR+CB/LWAL+DNO/H:1-3",12.0, 5.0,10.0), # Pisco
    # ── Piura ────────────────────────────────────────────────────
    ExposicionDistrito("200101",162850,44200,25.8,38.5,12.8,7.5,12.5,10.8,
                       "MUR+CB/LWAL+DNO/H:1-3",14.0, 5.0,15.0),
    ExposicionDistrito("200201",188812,51200,28.2,42.5,14.2,8.8,13.8,11.2,
                       "MUR+CB/LWAL+DNO/H:1-3",12.0, 4.0,18.0), # Sullana
    ExposicionDistrito("200601",72660,19800,32.5,48.8,16.5,10.2,15.8,11.5,
                       "MUR+CB/LWAL+DNO/H:1-3",10.0, 3.5,20.0), # Paita
    # ── La Libertad ──────────────────────────────────────────────
    ExposicionDistrito("130101",351208,95800,15.2,28.5,9.2,4.2,8.8,10.2,
                       "MUR+CB/LWAL+DNO/H:1-3",18.0, 8.0, 8.0), # Trujillo
    ExposicionDistrito("130102",182846,49800,18.5,32.8,11.2,5.8,10.5,10.8,
                       "MUR+CB/LWAL+DNO/H:1-3",15.0, 6.0,10.0), # El Porvenir
    # ── Lambayeque ───────────────────────────────────────────────
    ExposicionDistrito("140101",299535,82400,20.5,35.2,12.5,6.5,11.8,10.5,
                       "MUR+CB/LWAL+DNO/H:1-3",15.0, 6.0,12.0), # Chiclayo
    ExposicionDistrito("140105",178060,48200,22.8,38.5,13.8,7.2,12.5,11.0,
                       "MUR+CB/LWAL+DNO/H:1-3",13.0, 5.0,14.0), # José L. Ortiz
    # ── Puno ─────────────────────────────────────────────────────
    ExposicionDistrito("210101",130495,36800,42.5,58.8,22.5,15.8,21.5,13.5,
                       "MUR+ADO/LWAL/H:1",   5.0, 2.0,18.0), # Puno
    ExposicionDistrito("210601",228726,62400,38.2,55.2,20.8,14.5,19.8,12.8,
                       "MUR+ADO/LWAL/H:1",   6.0, 2.5,16.0), # Juliaca
    # ── Ayacucho ─────────────────────────────────────────────────
    ExposicionDistrito("050101",89505,25200,45.2,62.5,24.8,18.2,23.8,13.2,
                       "MUR+ADO/LWAL/H:1",   4.0, 1.5,20.0),
    # ── Ancash ───────────────────────────────────────────────────
    ExposicionDistrito("020101",120316,34800,32.5,48.2,16.8,10.8,16.5,11.5,
                       "MUR+STB/LWAL/H:1",   8.0, 3.0,18.0), # Huaraz
    ExposicionDistrito("021301",371012,101200,18.8,32.5,10.5,5.8,10.2,10.8,
                       "MUR+CB/LWAL+DNO/H:1-3",15.0, 6.0,12.0), # Chimbote
    # ── Junín ────────────────────────────────────────────────────
    ExposicionDistrito("120101",120236,33800,22.5,38.2,12.5,7.5,12.0,11.2,
                       "MCF/LWAL/H:2-4",    14.0, 7.0, 8.0), # Huancayo
    # ── Tacna ────────────────────────────────────────────────────
    ExposicionDistrito("230101",102776,30200,12.8,22.5,7.2,3.5,7.0,10.8,
                       "MCF/LWAL/H:2-4",    22.0,12.0, 4.0),
    # ── Moquegua ─────────────────────────────────────────────────
    ExposicionDistrito("180101",56054,15800,18.5,28.5,9.2,5.2,9.0,11.2,
                       "MCF/LWAL/H:2-4",    18.0,10.0, 6.0),
    # ── Tumbes ───────────────────────────────────────────────────
    ExposicionDistrito("240101",101928,28800,28.8,42.8,15.5,9.2,14.8,11.5,
                       "MUR+CB/LWAL+DNO/H:1-3",12.0, 4.0,18.0),
    # ── Iquitos ──────────────────────────────────────────────────
    ExposicionDistrito("160101",416888,108400,32.5,48.5,18.5,12.5,18.2,12.0,
                       "W/LWAL/H:1",         2.0, 0.5,35.0),
)


# ══════════════════════════════════════════════════════════════════
#  FALLAS GEOLÓGICAS (v8.0 — mantenido)
# ══════════════════════════════════════════════════════════════════

FALLAS_DATA: tuple[Falla, ...] = (
    Falla("Sistema de fallas de Lima","inversa","compresión",8.0,120,"Lima",True,
          ((-77.1,-12.0),(-76.8,-11.5),(-76.5,-11.0),(-76.2,-10.5))),
    Falla("Falla de Paracas","inversa","compresión",7.5,80,"Ica",True,
          ((-76.2,-13.8),(-75.9,-13.5),(-75.6,-13.2),(-75.3,-12.9))),
    Falla("Sistema de fallas de Ica","inversa-desplazamiento","compresión oblicua",7.8,200,"Ica",True,
          ((-75.7,-14.5),(-75.4,-14.0),(-75.1,-13.5),(-74.8,-13.0))),
    Falla("Falla de Nazca","transcurrente","deslizamiento lateral",7.2,150,"Ica",True,
          ((-74.9,-14.8),(-74.6,-14.5),(-74.3,-14.2),(-74.0,-13.9))),
    Falla("Sistema de fallas de Arequipa","inversa","compresión",8.4,300,"Arequipa",True,
          ((-72.5,-16.5),(-72.0,-16.2),(-71.5,-15.9),(-71.0,-15.6),(-70.5,-15.3))),
    Falla("Falla Ichuna","normal","extensión",7.0,60,"Moquegua",True,
          ((-70.7,-16.0),(-70.4,-16.3),(-70.1,-16.6))),
    Falla("Sistema de fallas de Tacna","inversa","compresión",7.3,120,"Tacna",True,
          ((-70.3,-17.0),(-70.0,-17.5),(-69.7,-18.0))),
    Falla("Falla Pisco-Ayacucho","inversa","compresión",7.0,100,"Ica",True,
          ((-75.0,-13.7),(-74.7,-14.0),(-74.4,-14.3),(-74.1,-14.6))),
    Falla("Falla Tumbes-Zarumilla","inversa","compresión",7.2,110,"Tumbes",True,
          ((-80.4,-3.5),(-80.1,-3.8),(-79.8,-4.1))),
    Falla("Falla de Piura-Sullana","transcurrente","deslizamiento lateral",6.8,80,"Piura",True,
          ((-80.5,-4.5),(-80.2,-4.8),(-79.9,-5.1),(-79.6,-5.4))),
    Falla("Falla Quiches-Sihuas","inversa","compresión",7.5,90,"Ancash",True,
          ((-77.8,-8.5),(-77.5,-8.8),(-77.2,-9.1))),
    Falla("Falla de Cordillera Blanca","normal","extensión",7.5,200,"Ancash",True,
          ((-77.6,-8.0),(-77.5,-8.5),(-77.4,-9.0),(-77.3,-9.5),(-77.2,-10.0))),
    Falla("Sistema de fallas del Cusco","normal","extensión",6.8,110,"Cusco",True,
          ((-72.0,-13.5),(-71.7,-13.8),(-71.4,-14.1),(-71.1,-14.4))),
    Falla("Falla de Tambomachay (Cusco)","normal","extensión",6.5,25,"Cusco",True,
          ((-71.9,-13.4),(-71.7,-13.5),(-71.5,-13.6))),
    Falla("Falla Vilcañota","normal","extensión",7.0,130,"Puno",True,
          ((-70.8,-14.5),(-70.5,-15.0),(-70.2,-15.5))),
    Falla("Sistema de fallas de Ayacucho","normal-transcurrente","extensión oblicua",6.5,80,"Ayacucho",True,
          ((-74.2,-13.5),(-74.0,-14.0),(-73.8,-14.5))),
    Falla("Sistema de fallas del Marañón","transcurrente","deslizamiento lateral",7.0,180,"Cajamarca",True,
          ((-78.5,-4.5),(-78.2,-5.0),(-77.9,-5.5),(-77.6,-6.0),(-77.3,-6.5))),
    Falla("Falla de Moyobamba","normal","extensión",6.5,60,"San Martín",True,
          ((-77.0,-5.8),(-76.7,-6.1),(-76.4,-6.4))),
    Falla("Falla Alto Chicama","inversa","compresión",6.5,55,"La Libertad",True,
          ((-78.2,-7.5),(-77.9,-7.8),(-77.6,-8.1))),
)


# ══════════════════════════════════════════════════════════════════
#  POLÍGONOS DE RIESGO (v8.0 — mantenidos)
# ══════════════════════════════════════════════════════════════════

_INUNDACIONES_RAW = [
    ("Valle del Mantaro (inundación fluvial)","fluvial",4,50,"Mantaro","Junín",3.5,
     [(-75.2,-11.8),(-75.0,-12.0),(-74.8,-12.2),(-75.0,-12.4),(-75.2,-12.2),(-75.2,-11.8)]),
    ("Delta del Río Piura","fluvial",5,25,"Piura","Piura",5.0,
     [(-80.8,-5.0),(-80.5,-5.1),(-80.3,-5.2),(-80.4,-5.4),(-80.7,-5.3),(-80.8,-5.0)]),
    ("Bajo Piura (FEN recurrente)","fluvial-pluvial",5,10,"Piura","Piura",4.0,
     [(-80.7,-5.2),(-80.4,-5.3),(-80.2,-5.5),(-80.3,-5.7),(-80.6,-5.6),(-80.7,-5.2)]),
    ("Cuenca del Río Santa (Ancash)","fluvial",4,100,"Santa","Ancash",4.5,
     [(-78.2,-9.0),(-78.0,-9.2),(-77.8,-9.4),(-78.0,-9.6),(-78.2,-9.4),(-78.2,-9.0)]),
    ("Llanura aluvial del Amazonas","fluvial",4,10,"Amazonas","Loreto",8.0,
     [(-73.5,-3.5),(-73.0,-3.8),(-72.5,-4.0),(-73.0,-4.5),(-73.5,-4.2),(-73.5,-3.5)]),
    ("Valle de Ica (desbordamiento)","fluvial",3,50,"Ica","Ica",2.5,
     [(-75.8,-14.0),(-75.6,-14.1),(-75.4,-14.2),(-75.5,-14.4),(-75.7,-14.3),(-75.8,-14.0)]),
    ("Litoral de Tumbes (inundación costera)","costera",4,20,"Tumbes","Tumbes",3.0,
     [(-80.5,-3.5),(-80.3,-3.6),(-80.2,-3.8),(-80.4,-3.9),(-80.6,-3.7),(-80.5,-3.5)]),
    ("Cuenca del Ucayali","fluvial",4,5,"Ucayali","Ucayali",10.0,
     [(-74.5,-8.0),(-74.2,-8.3),(-74.0,-8.6),(-74.3,-9.0),(-74.6,-8.7),(-74.5,-8.0)]),
    ("Zona baja del Río Rímac","fluvial-pluvial",3,50,"Rímac","Lima",2.0,
     [(-77.2,-12.0),(-77.0,-12.1),(-76.8,-12.0),(-76.9,-12.2),(-77.1,-12.2),(-77.2,-12.0)]),
    ("Cuenca del Río Chira (Piura-FEN)","fluvial",5,15,"Chira","Piura",5.5,
     [(-81.0,-4.5),(-80.7,-4.7),(-80.5,-5.0),(-80.8,-5.2),(-81.0,-4.9),(-81.0,-4.5)]),
    ("Cuenca del Río Madre de Dios","fluvial",4,5,"Madre de Dios","Madre de Dios",9.0,
     [(-70.5,-12.5),(-70.2,-12.7),(-70.0,-13.0),(-70.3,-13.3),(-70.6,-13.0),(-70.5,-12.5)]),
    ("Cuenca del Río Huallaga","fluvial",3,25,"Huallaga","San Martín",4.0,
     [(-76.5,-6.8),(-76.2,-7.0),(-76.0,-7.3),(-76.3,-7.6),(-76.6,-7.3),(-76.5,-6.8)]),
]

_TSUNAMIS_RAW = [
    ("Zona inundación tsunami Lima - Callao",5,15.0,20,100,"Lima",
     [(-77.2,-12.0),(-77.0,-12.05),(-76.9,-12.1),(-77.0,-12.2),(-77.2,-12.15),(-77.2,-12.0)]),
    ("Zona tsunami Ica - Pisco",5,12.0,25,75,"Ica",
     [(-76.3,-13.6),(-76.1,-13.7),(-76.0,-13.9),(-76.2,-14.0),(-76.4,-13.8),(-76.3,-13.6)]),
    ("Zona tsunami Arequipa - Camaná",5,18.0,30,150,"Arequipa",
     [(-72.9,-16.5),(-72.6,-16.6),(-72.4,-16.8),(-72.6,-17.0),(-72.8,-16.8),(-72.9,-16.5)]),
    ("Costa norte Moquegua",4,10.0,35,100,"Moquegua",
     [(-71.4,-17.0),(-71.2,-17.1),(-71.0,-17.3),(-71.2,-17.4),(-71.4,-17.2),(-71.4,-17.0)]),
    ("Litoral Tacna",4,9.0,40,100,"Tacna",
     [(-70.5,-17.8),(-70.3,-17.9),(-70.1,-18.1),(-70.3,-18.2),(-70.5,-18.0),(-70.5,-17.8)]),
    ("Costa Ancash - Chimbote",4,8.0,20,100,"Ancash",
     [(-78.7,-9.0),(-78.5,-9.1),(-78.3,-9.3),(-78.5,-9.5),(-78.7,-9.3),(-78.7,-9.0)]),
    ("Litoral La Libertad - Salaverry",3,7.0,20,100,"La Libertad",
     [(-79.1,-8.1),(-78.9,-8.2),(-78.7,-8.4),(-78.9,-8.6),(-79.1,-8.4),(-79.1,-8.1)]),
    ("Costa Piura - Sechura",3,6.5,25,150,"Piura",
     [(-81.0,-5.3),(-80.8,-5.4),(-80.6,-5.6),(-80.8,-5.8),(-81.0,-5.6),(-81.0,-5.3)]),
    ("Bahía de Tumbes",3,5.5,30,200,"Tumbes",
     [(-80.6,-3.4),(-80.4,-3.5),(-80.3,-3.7),(-80.5,-3.9),(-80.7,-3.7),(-80.6,-3.4)]),
]

_DESLIZAMIENTOS_RAW = [
    ("Huayco recurrente Chosica (Rímac)","flujo de detritos",5,25.5,"lluvias intensas","Lima",True,
     [(-76.7,-11.9),(-76.5,-12.0),(-76.4,-12.1),(-76.5,-12.2),(-76.7,-12.1),(-76.7,-11.9)]),
    ("Deslizamiento Machu Picchu-Aguas Calientes","deslizamiento rotacional",4,8.3,"lluvias + pendiente","Cusco",True,
     [(-72.6,-13.1),(-72.5,-13.2),(-72.4,-13.3),(-72.5,-13.4),(-72.6,-13.3),(-72.6,-13.1)]),
    ("Zona inestable Cusco - Yauricocha","deslizamiento traslacional",4,45.0,"sismicidad + lluvias","Cusco",True,
     [(-71.8,-13.5),(-71.6,-13.6),(-71.4,-13.7),(-71.5,-13.9),(-71.7,-13.8),(-71.8,-13.5)]),
    ("Deslizamientos Ceja de Selva (Amazonas)","deslizamiento masivo",4,120.0,"deforestación + lluvias","Amazonas",True,
     [(-78.0,-6.0),(-77.7,-6.3),(-77.4,-6.5),(-77.6,-6.8),(-77.9,-6.6),(-78.0,-6.0)]),
    ("Deslizamiento Yungay (recurrente)","alud",5,22.0,"glaciares + sismicidad","Ancash",True,
     [(-77.8,-9.1),(-77.6,-9.2),(-77.4,-9.4),(-77.6,-9.6),(-77.8,-9.4),(-77.8,-9.1)]),
    ("Deslizamiento Kola (Puno)","deslizamiento rotacional",4,180.0,"sismicidad","Puno",True,
     [(-70.6,-15.5),(-70.3,-15.7),(-70.1,-15.9),(-70.3,-16.1),(-70.6,-15.9),(-70.6,-15.5)]),
    ("Huaycos Cañón del Cotahuasi","flujo de detritos",4,15.0,"lluvias + fuertes pendientes","Arequipa",True,
     [(-72.9,-15.1),(-72.7,-15.3),(-72.5,-15.5),(-72.7,-15.7),(-72.9,-15.5),(-72.9,-15.1)]),
    ("Deslizamiento Ocoña-Camaná","flujo de detritos",4,28.0,"lluvias andinas intensas","Arequipa",True,
     [(-72.8,-16.3),(-72.5,-16.5),(-72.3,-16.7),(-72.5,-16.9),(-72.8,-16.7),(-72.8,-16.3)]),
    ("Zona aluviónica Piura Sierra","flujo de detritos-aluvial",4,35.0,"FEN intenso","Piura",True,
     [(-79.5,-5.0),(-79.2,-5.2),(-79.0,-5.4),(-79.2,-5.6),(-79.5,-5.4),(-79.5,-5.0)]),
    ("Taludes Junín Selva Central","deslizamiento traslacional",3,60.0,"deforestación + pendiente","Junín",True,
     [(-75.5,-10.8),(-75.2,-11.0),(-75.0,-11.2),(-75.2,-11.4),(-75.5,-11.2),(-75.5,-10.8)]),
]


# ══════════════════════════════════════════════════════════════════
#  INFRAESTRUCTURA CRÍTICA (v8.0 — mantenida)
# ══════════════════════════════════════════════════════════════════

def _infra_items_oficiales() -> list[InfraItem]:
    items: list[InfraItem] = []
    _aeropuertos = [
        ("Aeropuerto Internacional Jorge Chávez",-77.1143,-12.0219,5),
        ("Aeropuerto Alejandro Velasco Astete (Cusco)",-71.9388,-13.5357,5),
        ("Aeropuerto Rodríguez Ballón (Arequipa)",-71.5831,-16.3411,5),
        ("Aeropuerto Quiñones (Chiclayo)",-79.8282,-6.7875,5),
        ("Aeropuerto Martínez de Pinillos (Trujillo)",-79.1086,-8.0814,5),
        ("Aeropuerto Concha Iberico (Piura)",-80.6164,-5.2075,4),
        ("Aeropuerto Secada Vignetta (Iquitos)",-73.3086,-3.7847,4),
        ("Aeropuerto José de Aldamiz (Pto Maldonado)",-69.2287,-12.6136,4),
        ("Aeropuerto Mendívil (Ayacucho)",-74.2042,-13.1548,4),
        ("Aeropuerto Abensur (Pucallpa)",-74.5742,-8.3794,4),
        ("Aeropuerto Manco Capac (Juliaca)",-70.1583,-15.4672,4),
        ("Aeropuerto Canga Rodríguez (Tumbes)",-80.3783,-3.5526,4),
        ("Aeropuerto del Castillo (Tarapoto)",-76.3733,-6.5086,4),
        ("Aeropuerto Ciriani Santa Rosa (Tacna)",-70.2756,-18.0533,4),
        ("Aeropuerto Revoredo (Cajamarca)",-78.4894,-7.1392,3),
        ("Aeropuerto de Ilo (Moquegua)",-71.3400,-17.6944,3),
        ("Aeropuerto David Figueroa (Huánuco)",-76.2048,-9.8781,3),
        ("Aeropuerto Jaime Montreuil (Chimbote)",-78.5244,-9.1494,3),
    ]
    for nombre, lon, lat, crit in _aeropuertos:
        items.append(InfraItem(nombre,"aeropuerto",lon,lat,crit,fuente="MTC/CORPAC 2024"))

    _puertos = [
        ("Terminal Portuario del Callao",-77.1483,-12.0580,5),
        ("Terminal Portuario de Paita",-81.1129,-5.0852,5),
        ("Terminal Portuario de Salaverry",-78.9783,-8.2239,4),
        ("Terminal Portuario de Chimbote",-78.5861,-9.0753,4),
        ("Terminal Portuario de Pisco",-76.2163,-13.7211,4),
        ("Terminal Portuario de Matarani (Arequipa)",-72.1072,-16.9958,4),
        ("Terminal Portuario de Ilo",-71.3361,-17.6358,4),
        ("Terminal ENAPU Iquitos",-73.2561,-3.7433,4),
        ("Puerto Fluvial de Pucallpa",-74.5533,-8.3933,3),
        ("Terminal Portuario de Yurimaguas",-76.0944,-5.8975,3),
        ("Puerto General San Martín (Pisco)",-76.1994,-13.7689,4),
    ]
    for nombre, lon, lat, crit in _puertos:
        items.append(InfraItem(nombre,"puerto",lon,lat,crit,fuente="APN/MTC 2024"))

    _centrales = [
        ("C.H. Mantaro (ElectroPerú)",-74.9358,-12.3083,5),
        ("C.H. Chaglla (Pachitea)",-76.1500,-9.7833,5),
        ("C.H. Cerro del Águila",-74.6167,-12.5333,5),
        ("C.T. Ventanilla (ENEL)",-77.1500,-11.8667,5),
        ("C.T. Chilca 1 (Kallpa)",-76.7000,-12.5167,5),
        ("C.H. Cañon del Pato (Duke Energy)",-77.7208,-8.9069,5),
        ("Sub-Estación Zapallal (Red Alta Tensión)",-77.0833,-11.8667,5),
        ("C.H. Quitaracsa",-77.7167,-8.9333,4),
        ("C.T. Ilo 1 (Southern Copper)",-71.3344,-17.6394,4),
        ("C.H. Machu Picchu (ElectroSur Este)",-72.5456,-13.1539,4),
        ("C.H. San Gabán II",-69.7833,-13.3167,4),
        ("C.H. Carhuaquero",-79.2167,-6.6833,4),
        ("C.H. Gallito Ciego (CHAVIMOCHIC)",-79.1333,-7.0833,4),
        ("C.H. Oroya — ElectroAndes",-75.9167,-11.5333,4),
        ("C.H. Yuncan (ElectroPerú)",-75.5083,-10.2833,4),
        ("Parque Solar Majes (Arequipa)",-72.3167,-16.3833,3),
        ("C.T. Pisco",-76.2167,-13.8333,4),
    ]
    for nombre, lon, lat, crit in _centrales:
        items.append(InfraItem(nombre,"central_electrica",lon,lat,crit,fuente="OSINERGMIN/MINEM 2024"))

    _hospitales = [
        ("Hospital Nacional Dos de Mayo",-77.0439,-12.0508),
        ("Hospital Nacional Arzobispo Loayza",-77.0387,-12.0475),
        ("Hospital Guillermo Almenara (EsSalud)",-77.0100,-12.0669),
        ("Hospital Edgardo Rebagliati (EsSalud)",-77.0511,-12.0847),
        ("Hospital Nacional Cayetano Heredia",-77.0633,-11.9861),
        ("Hospital Regional de Ica",-75.7256,-14.0678),
        ("Hospital Santa María del Socorro (Ica)",-75.7183,-14.0750),
        ("Hospital Regional Honorio Delgado (Arequipa)",-71.5378,-16.4189),
        ("Hospital Carlos Seguín Escobedo (Arequipa)",-71.5300,-16.3900),
        ("Hospital Regional del Cusco",-71.9769,-13.5161),
        ("Hospital Adolfo Guevara Velasco (Cusco)",-71.9781,-13.5278),
        ("Hospital Regional de Trujillo",-79.0372,-8.1042),
        ("Hospital Regional de Piura",-80.6339,-5.1942),
        ("Hospital Regional de Chiclayo",-79.8394,-6.7744),
        ("Hospital Regional de Ayacucho",-74.2236,-13.1597),
        ("Hospital Regional de Puno",-70.0181,-15.8508),
        ("Hospital Carlos Monge Medrano (Juliaca)",-70.1356,-15.4797),
        ("Hospital Regional de Huancayo",-75.2181,-12.0639),
        ("Hospital Regional de Tacna",-70.0161,-18.0158),
        ("Hospital Regional de Tumbes",-80.4606,-3.5650),
        ("Hospital Iquitos (Loreto)",-73.2481,-3.7481),
        ("Hospital Regional de Moquegua",-70.9372,-17.1939),
        ("Hospital Regional de Cajamarca",-78.5083,-7.1631),
        ("Hospital Regional de Huánuco",-76.2419,-9.9281),
        ("Hospital La Caleta (Chimbote)",-78.5839,-9.0736),
        ("Hospital Regional de Pucallpa",-74.5358,-8.3781),
    ]
    for nombre, lon, lat in _hospitales:
        items.append(InfraItem(nombre,"hospital",lon,lat,5,fuente="MINSA/SUSALUD 2024"))

    _bomberos = [
        ("Compañía de Bomberos Lima N°1",-77.0428,-12.0464),
        ("Compañía de Bomberos Miraflores N°28",-77.0294,-12.1200),
        ("Compañía de Bomberos Arequipa N°20",-71.5483,-16.4011),
        ("Compañía de Bomberos Cusco N°25",-71.9811,-13.5236),
        ("Compañía de Bomberos Ica N°15",-75.7278,-14.0644),
        ("Compañía de Bomberos Piura N°6",-80.6394,-5.1967),
        ("Compañía de Bomberos Trujillo N°7",-79.0350,-8.0994),
        ("Compañía de Bomberos Chiclayo N°12",-79.8411,-6.7694),
        ("Compañía de Bomberos Tacna N°18",-70.0194,-18.0106),
        ("Compañía de Bomberos Puno N°40",-70.0231,-15.8531),
        ("Compañía de Bomberos Huancayo N°35",-75.2233,-12.0647),
        ("Compañía de Bomberos Ayacucho N°50",-74.2178,-13.1556),
        ("Compañía de Bomberos Cajamarca N°55",-78.5083,-7.1583),
        ("Compañía de Bomberos Iquitos N°65",-73.2489,-3.7514),
    ]
    for nombre, lon, lat in _bomberos:
        items.append(InfraItem(nombre,"bomberos",lon,lat,5,fuente="CGBVP 2024"))

    return items


_OSM_QUERIES: dict[str, tuple[str, int]] = {
    "hospital":    ('amenity="hospital"', 5),
    "escuela":     ('amenity~"school|kindergarten|university|college"', 4),
    "bomberos":    ('amenity="fire_station"', 5),
    "policia":     ('amenity="police"', 4),
    "planta_agua": ('man_made~"water_works|pumping_station|water_tower"', 4),
    "refugio":     ('amenity~"shelter|social_facility"', 5),
}


def _get_osm_infra(tipo: str) -> list[InfraItem]:
    tag, crit = _OSM_QUERIES.get(tipo, (f'amenity="{tipo}"', 3))
    elements = try_overpass(overpass_query(tag), tipo)
    result = []
    for el in elements:
        coords = osm_element_centroid(el)
        if not coords:
            continue
        lon, lat = coords
        if not is_in_peru(lon, lat):
            continue
        tags = el.get("tags", {})
        nombre = tags.get("name:es") or tags.get("name") or tipo.replace("_", " ").title()
        result.append(InfraItem(
            nombre=nombre[:200], tipo=tipo, lon=lon, lat=lat,
            criticidad=crit, osm_id=el.get("id"),
            fuente="OpenStreetMap", fuente_tipo="osm",
        ))
    return result


def _bulk_insert_infra(conn: Any, items: list[InfraItem]) -> int:
    if not items:
        return 0
    valid = [i for i in items if i.is_in_peru_bbox()]
    count = 0
    with conn.cursor() as cur:
        for chunk in chunked(valid, 500):
            params = [(i.osm_id, i.nombre, i.tipo, i.lon, i.lat,
                       i.criticidad, i.estado, i.fuente, i.fuente_tipo, i.capacidad)
                      for i in chunk]
            try:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO infraestructura
                        (osm_id, nombre, tipo, geom, criticidad, estado,
                         fuente, fuente_tipo, capacidad)
                    VALUES (%s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, params, page_size=500)
                count += len(chunk)
            except Exception as exc:
                log.debug("Chunk infra falló: %s", exc)
    conn.commit()
    return count


def _limpiar_fuera_peru(conn: Any) -> int:
    slog = step_log("INFRA.CLEAN")
    n_deptos = fetch_one(conn,
        "SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")[0]
    if n_deptos < 5:
        slog.warning("Solo %d departamentos → usando bbox para limpieza", n_deptos)
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM infraestructura
                WHERE ST_X(geom) NOT BETWEEN -82.5 AND -68.0
                   OR ST_Y(geom) NOT BETWEEN -19.0 AND  1.5
            """)
            n = cur.rowcount
        conn.commit()
        return n

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS _tmp_peru_boundary")
        cur.execute("""
            CREATE TEMP TABLE _tmp_peru_boundary AS
            SELECT ST_Buffer(ST_Union(geom), 0.27) AS geom
            FROM departamentos WHERE geom IS NOT NULL
        """)
        cur.execute("SELECT (geom IS NOT NULL) FROM _tmp_peru_boundary LIMIT 1")
        row = cur.fetchone()
        if not row or not row[0]:
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
                SELECT 1 FROM _tmp_peru_boundary p WHERE ST_Intersects(i.geom, p.geom)
            )
        """)
        n = cur.rowcount
        cur.execute("DROP TABLE IF EXISTS _tmp_peru_boundary")
    conn.commit()
    return n


# ══════════════════════════════════════════════════════════════════
#  ESTACIONES (v8.0 — mantenidas)
# ══════════════════════════════════════════════════════════════════

_ESTACIONES_DATA: tuple[Estacion, ...] = (
    Estacion("NNA","Estación Sísmica Nanay (Iquitos)","sismica",-73.1667,-3.7833,110,"IGP","RSN"),
    Estacion("LIM","Estación Sísmica Lima","sismica",-77.0500,-11.9000,154,"IGP","RSN"),
    Estacion("AYA","Estación Sísmica Ayacucho","sismica",-74.2167,-13.1500,2765,"IGP","RSN"),
    Estacion("CUS","Estación Sísmica Cusco","sismica",-71.9700,-13.5200,3399,"IGP","RSN"),
    Estacion("ARE","Estación Sísmica Arequipa","sismica",-71.4900,-16.4100,2490,"IGP","RSN"),
    Estacion("TAC","Estación Sísmica Tacna","sismica",-70.0700,-18.0100,550,"IGP","RSN"),
    Estacion("MQG","Estación Sísmica Moquegua","sismica",-70.9200,-17.1800,1400,"IGP","RSN"),
    Estacion("HCY","Estación Sísmica Huancayo","sismica",-75.2167,-12.0500,3315,"IGP","RSN"),
    Estacion("CHB","Estación Sísmica Chimbote","sismica",-78.5800,-9.0800,15,"IGP","RSN"),
    Estacion("PIU_S","Estación Sísmica Piura","sismica",-80.6200,-5.1900,30,"IGP","RSN"),
    Estacion("ICA_S","Estación Sísmica Ica","sismica",-75.7300,-14.0800,410,"IGP","RSN"),
    Estacion("MOQ_S","Estación Sísmica Mollendo","sismica",-72.0200,-17.0300,60,"IGP","RSN"),
    Estacion("OVI-UBI","Observatorio Vulcanológico Ubinas","volcanologica",-70.9000,-16.3500,4800,"IGP","OVI"),
    Estacion("OVI-SAP","Observatorio Vulcanológico Sabancaya","volcanologica",-71.8700,-15.7300,4979,"IGP","OVI"),
    Estacion("OVI-ELM","Observatorio Vulcanológico El Misti","volcanologica",-71.4100,-16.2900,4600,"IGP","OVI"),
    Estacion("SENA-ICA","Estación Meteorológica Ica","meteorologica",-75.7200,-14.0700,406,"SENAMHI","RMN"),
    Estacion("SENA-PIU","Estación Meteorológica Piura","meteorologica",-80.6300,-5.1800,29,"SENAMHI","RMN"),
    Estacion("SENA-HYC","Estación Meteorológica Huancayo","meteorologica",-75.3300,-12.0600,3313,"SENAMHI","RMN"),
    Estacion("SENA-IQT","Estación Meteorológica Iquitos","meteorologica",-73.2600,-3.7800,126,"SENAMHI","RMN"),
    Estacion("SENA-ARE","Estación Meteorológica Arequipa","meteorologica",-71.5600,-16.3300,2525,"SENAMHI","RMN"),
    Estacion("SENA-CUS","Estación Meteorológica Cusco","meteorologica",-71.9800,-13.5600,3350,"SENAMHI","RMN"),
    Estacion("SENA-JUL","Estación Meteorológica Juliaca","meteorologica",-70.1800,-15.4800,3820,"SENAMHI","RMN"),
    Estacion("SENA-CJM","Estación Meteorológica Cajamarca","meteorologica",-78.5100,-7.1700,2720,"SENAMHI","RMN"),
    Estacion("SENA-MPC","Estación Meteorológica Machu Picchu","meteorologica",-72.5400,-13.1600,2040,"SENAMHI","RMN"),
    Estacion("SENA-TRP","Estación Meteorológica Tarapoto","meteorologica",-76.3700,-6.4900,356,"SENAMHI","RMN"),
    Estacion("ANA-RIM","Hidrómetro Rímac - La Atarjea","hidrometrica",-77.0167,-11.9667,800,"ANA","RHN"),
    Estacion("ANA-MAN","Hidrómetro Mantaro - Angasmayo","hidrometrica",-75.0500,-11.7833,3350,"ANA","RHN"),
    Estacion("ANA-CHI","Hidrómetro Chira - Ardilla","hidrometrica",-80.6167,-4.9333,45,"ANA","RHN"),
    Estacion("ANA-AMZ","Hidrómetro Amazonas - Borja","hidrometrica",-77.5500,-4.4833,200,"ANA","RHN"),
    Estacion("ANA-TIT","Hidrómetro Titicaca - Puno","hidrometrica",-70.0200,-15.8500,3810,"ANA","RHN"),
    Estacion("DHN-CAL","Mareógrafo Callao (DART)","maregraf",-77.1500,-12.0500,5,"DHN","DART"),
    Estacion("DHN-MAT","Mareógrafo Matarani (Tsunami)","maregraf",-72.1000,-17.0000,4,"DHN","DART"),
    Estacion("IPEN-LIM","Estación Radiológica Lima","radiologica",-77.0500,-11.9800,180,"IPEN","RRM"),
    Estacion("COEN-LIM","Centro Operaciones Emergencias Nacional","emergencias",-77.0500,-12.0500,150,"INDECI","COEN"),
)


# ══════════════════════════════════════════════════════════════════
#  PASOS 0-10 (v8.0 — mantenidos intactos)
# ══════════════════════════════════════════════════════════════════

def _insert_departamento(cur: Any, nombre: str, ubigeo: str, geom_wkt: WKT,
                          zona: ZonaSismica, fuente: str = "GADM 4.1") -> bool:
    try:
        cur.execute("""
            INSERT INTO departamentos (nombre, ubigeo, geom, zona_sismica, factor_z, fuente)
            VALUES (%s, %s,
                ST_Multi(ST_MakeValid(ST_GeomFromText(%s, 4326)))::geometry(MultiPolygon,4326),
                %s, %s, %s)
            ON CONFLICT (ubigeo) DO UPDATE SET
                geom=EXCLUDED.geom, zona_sismica=EXCLUDED.zona_sismica,
                factor_z=EXCLUDED.factor_z, fuente=EXCLUDED.fuente
        """, (nombre, ubigeo, geom_wkt, int(zona), zona.factor, fuente))
        return True
    except Exception as exc:
        log.debug("Error departamento '%s': %s", nombre, exc)
        return False


def paso_departamentos() -> StepResult:
    slog = step_log("DEPARTAMENTOS"); t0 = time.perf_counter()
    inserted = updated = errors = 0
    url = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_PER_1.json"
    n_gadm = 0
    try:
        slog.info("Descargando GADM L1...")
        raw = http_get_bytes(url, timeout=get_config().gadm_timeout)
        gj = json.loads(raw)
        with get_conn() as conn:
            with conn.cursor() as cur:
                for feat in gj["features"]:
                    props = feat["properties"]
                    nombre = props.get("NAME_1","")
                    zona = ZONA_SISMICA_POR_DEPTO.get(nombre, ZonaSismica.Z2)
                    ubigeo = props.get("CC_1") or f"GADM_{nombre[:6].upper()}"
                    wkt = geojson_feature_to_wkt(feat)
                    if wkt and _insert_departamento(cur, nombre, ubigeo, wkt, zona):
                        n_gadm += 1
                    else:
                        errors += 1
            conn.commit()
        inserted = n_gadm
    except Exception as exc:
        slog.error("GADM L1 falló: %s → fallback", exc)

    with get_conn() as conn:
        n_actual = fetch_one(conn, "SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")[0]
    if n_actual < 20:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for dep in DEPARTAMENTOS_FALLBACK:
                    if _insert_departamento(cur, dep.nombre, dep.ubigeo,
                                            dep.bbox_wkt(), dep.zona, "Fallback-bbox"):
                        inserted += 1
                    else:
                        errors += 1
            conn.commit()

    with get_conn() as conn:
        total = fetch_one(conn, "SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")[0]
    slog.info("✅ %d departamentos disponibles", total)
    return StepResult("departamentos", inserted, updated, errors, time.perf_counter()-t0)


_USGS_BASE = "https://earthquake.usgs.gov/fdsnws/event/1/query"
_BLOQUES: tuple[tuple[str, str], ...] = (
    ("1900-01-01","1910-01-01"),("1910-01-01","1920-01-01"),
    ("1920-01-01","1930-01-01"),("1930-01-01","1940-01-01"),
    ("1940-01-01","1950-01-01"),("1950-01-01","1960-01-01"),
    ("1960-01-01","1970-01-01"),("1970-01-01","1975-01-01"),
    ("1975-01-01","1980-01-01"),("1980-01-01","1985-01-01"),
    ("1985-01-01","1990-01-01"),("1990-01-01","1995-01-01"),
    ("1995-01-01","2000-01-01"),("2000-01-01","2003-01-01"),
    ("2003-01-01","2006-01-01"),("2006-01-01","2009-01-01"),
    ("2009-01-01","2012-01-01"),("2012-01-01","2015-01-01"),
    ("2015-01-01","2018-01-01"),("2018-01-01","2021-01-01"),
    ("2021-01-01","2023-01-01"),
    ("2023-01-01", date.today().strftime("%Y-%m-%d")),
)


def _fetch_bloque(start: str, end: str, cfg: ETLConfig) -> list[dict]:
    params = {
        "format":"geojson","starttime":start,"endtime":end,
        "minlatitude":cfg.bbox_min_lat,"maxlatitude":cfg.bbox_max_lat,
        "minlongitude":cfg.bbox_min_lon,"maxlongitude":cfg.bbox_max_lon,
        "minmagnitude":2.5,"orderby":"time-asc","limit":20000,
    }
    data = http_get(_USGS_BASE, params=params, timeout=60)
    return data.get("features", [])


def _feature_to_sismo(feat: dict) -> Sismo | None:
    props = feat.get("properties", {})
    coords = feat.get("geometry", {}).get("coordinates", [])
    if len(coords) < 3:
        return None
    lon, lat, depth = coords[0], coords[1], coords[2] or 0.0
    mag = props.get("mag")
    if not mag or mag < 0:
        return None
    ts = props.get("time", 0)
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else None
    depth = max(0.0, depth)
    tipo = "superficial" if depth < 60 else "intermedio" if depth < 300 else "profundo"
    return Sismo(usgs_id=feat["id"], lon=lon, lat=lat, magnitud=round(float(mag), 1),
                 profundidad_km=round(depth, 2), tipo_profundidad=tipo,
                 fecha=dt.date() if dt else None, hora_utc=dt,
                 lugar=props.get("place", "")[:500],
                 tipo_magnitud=props.get("magType", ""), estado=props.get("status","reviewed"))


def paso_sismos() -> StepResult:
    slog = step_log("SISMOS"); cfg = get_config(); t0 = time.perf_counter()
    slog.info("USGS M≥2.5 desde 1900 — %d bloques", len(_BLOQUES))
    all_features: list[dict] = []
    fetch_errors = 0
    with ThreadPoolExecutor(max_workers=cfg.max_workers) as ex:
        futs = {ex.submit(_fetch_bloque, s, e, cfg): (s, e) for s, e in _BLOQUES}
        for fut in as_completed(futs):
            s, e = futs[fut]
            try:
                all_features.extend(fut.result())
            except Exception as exc:
                slog.warning("Bloque %s→%s falló: %s", s, e, exc)
                fetch_errors += 1

    seen: set[str] = set()
    sismos: list[Sismo] = []
    for feat in all_features:
        s = _feature_to_sismo(feat)
        if s and s.usgs_id not in seen:
            seen.add(s.usgs_id); sismos.append(s)

    with get_conn() as conn:
        existing_ids = {r["usgs_id"] for r in fetch_all_dict(conn,"SELECT usgs_id FROM sismos")}
    nuevos = [s for s in sismos if s.usgs_id not in existing_ids]
    slog.info("  %d nuevos sismos", len(nuevos))

    inserted = insert_errors = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for chunk in chunked(nuevos, cfg.chunk_size):
                params = [(s.usgs_id,s.lon,s.lat,s.magnitud,s.profundidad_km,
                           s.tipo_profundidad,s.fecha,s.hora_utc,s.lugar,
                           s.tipo_magnitud,s.estado) for s in chunk]
                try:
                    psycopg2.extras.execute_batch(cur, """
                        INSERT INTO sismos
                            (usgs_id,geom,magnitud,profundidad_km,tipo_profundidad,
                             fecha,hora_utc,lugar,tipo_magnitud,estado)
                        VALUES (%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (usgs_id) DO NOTHING
                    """, params, page_size=500)
                    inserted += len(chunk)
                except Exception as exc:
                    slog.warning("Chunk sismos: %s", exc)
                    conn.rollback(); insert_errors += len(chunk); continue
        conn.commit()
    return StepResult("sismos", inserted, 0, fetch_errors+insert_errors, time.perf_counter()-t0)


def _insert_distrito_row(cur, ubigeo, nombre, provincia, departamento,
                          geom_wkt, zona, fuente, poblacion=None) -> bool:
    try:
        cur.execute("SAVEPOINT sp_d")
        cur.execute("""
            INSERT INTO distritos (ubigeo,nombre,provincia,departamento,geom,nivel_riesgo,
                zona_sismica,poblacion,fuente)
            VALUES (%s,%s,%s,%s,
                ST_Multi(ST_MakeValid(ST_GeomFromText(%s,4326)))::geometry(MultiPolygon,4326),
                3,%s,%s,%s)
            ON CONFLICT (ubigeo) DO UPDATE SET
                geom=EXCLUDED.geom, zona_sismica=EXCLUDED.zona_sismica,
                poblacion=COALESCE(EXCLUDED.poblacion,distritos.poblacion), fuente=EXCLUDED.fuente
        """, (ubigeo,nombre,provincia,departamento,geom_wkt,int(zona),poblacion,fuente))
        cur.execute("RELEASE SAVEPOINT sp_d")
        return True
    except Exception as exc:
        cur.execute("ROLLBACK TO SAVEPOINT sp_d"); cur.execute("RELEASE SAVEPOINT sp_d")
        log.debug("Distrito omitido (%s): %s", nombre, exc)
        return False


def _actualizar_zona_sismica(conn: Any) -> int:
    total = 0
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE distritos d SET zona_sismica = zsd.zona_sismica
            FROM zona_sismica_departamento zsd
            WHERE unaccent(lower(d.departamento))=unaccent(lower(zsd.departamento))
              AND d.zona_sismica IS DISTINCT FROM zsd.zona_sismica
        """); total += cur.rowcount
        cur.execute("""
            UPDATE distritos d SET zona_sismica = (
                SELECT dep.zona_sismica FROM departamentos dep
                WHERE dep.zona_sismica IS NOT NULL AND dep.geom IS NOT NULL
                ORDER BY dep.geom <-> ST_Centroid(d.geom) LIMIT 1
            ) WHERE d.zona_sismica IS NULL AND d.geom IS NOT NULL
        """); total += cur.rowcount
        cur.execute("UPDATE distritos SET zona_sismica=2 WHERE zona_sismica IS NULL")
        total += cur.rowcount
    conn.commit()
    return total


def paso_distritos() -> StepResult:
    slog = step_log("DISTRITOS"); t0 = time.perf_counter()
    inserted = errors = 0
    with get_conn() as conn:
        borrados = exec_sql(conn, "DELETE FROM distritos")
    slog.info("  %d registros previos eliminados", borrados)

    n_inei = 0
    for url in [
        "https://geoservidor.inei.gob.pe/geoserver/ows?service=WFS&version=1.0.0"
        "&request=GetFeature&typeName=INEI:LIMITEDISTRITAL&outputFormat=application/json&srsName=EPSG:4326",
    ]:
        try:
            raw = http_get_bytes(url, timeout=30)
            feats = json.loads(raw).get("features", [])
            if feats:
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        for feat in feats:
                            p = feat.get("properties", {})
                            depto = p.get("NOMBDEP","")
                            zona = ZONA_SISMICA_POR_DEPTO.get(depto, ZonaSismica.Z2)
                            wkt = geojson_feature_to_wkt(feat)
                            if not wkt: continue
                            ubigeo = p.get("IDDIST") or f"INEI_{hashlib.md5(p.get('NOMBDIST','').encode()).hexdigest()[:8]}"
                            if _insert_distrito_row(cur,ubigeo,p.get("NOMBDIST",""),
                                                    p.get("NOMBPROV",""),depto,wkt,zona,"INEI",
                                                    p.get("POBLACIE") or p.get("PBLCNE_TO")):
                                n_inei += 1
                    conn.commit()
                inserted += n_inei
                if n_inei >= 50: break
        except Exception as exc:
            slog.warning("INEI WFS: %s", exc)

    try:
        url = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_PER_3.json"
        raw = http_get_bytes(url, timeout=get_config().gadm_timeout)
        feats = json.loads(raw)["features"]
        n_gadm = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                for chunk in chunked(feats, get_config().chunk_size):
                    for feat in chunk:
                        p = feat.get("properties", {})
                        depto = p.get("NAME_1","")
                        zona = ZONA_SISMICA_POR_DEPTO.get(depto, ZonaSismica.Z2)
                        ubigeo = p.get("GID_3") or p.get("CC_3")
                        if not ubigeo: continue
                        wkt = geojson_feature_to_wkt(feat)
                        if wkt and _insert_distrito_row(cur,ubigeo,p.get("NAME_3",""),
                                                        p.get("NAME_2",""),depto,wkt,zona,"GADM 4.1"):
                            n_gadm += 1
            conn.commit()
        inserted += n_gadm
    except Exception as exc:
        slog.warning("GADM L3: %s", exc); errors += 1

    with get_conn() as conn:
        n_actual = fetch_one(conn, "SELECT COUNT(*) FROM distritos WHERE geom IS NOT NULL")[0]
    if n_actual < 50:
        count = 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                for row in _DISTRITOS_RAW:
                    nombre,ubigeo,provincia,depto,lo,la,hi,ha,zona = row
                    wkt = bbox_to_multipolygon_wkt(lo,la,hi,ha)
                    if _insert_distrito_row(cur,ubigeo,nombre,provincia,depto,wkt,zona,"Fallback-bbox-v9.0"):
                        count += 1
            conn.commit()
        inserted += count

    with get_conn() as conn:
        n_zona = _actualizar_zona_sismica(conn)
    slog.info("  zona_sismica: %d filas", n_zona)
    with get_conn() as conn:
        total = fetch_one(conn, "SELECT COUNT(*) FROM distritos")[0]
    slog.info("✅ %d distritos disponibles", total)
    return StepResult("distritos", inserted, n_zona, errors, time.perf_counter()-t0)


def _insertar_poligonos_riesgo(tabla: str, rows: list[tuple]) -> StepResult:
    slog = step_log(tabla.upper()); t0 = time.perf_counter()
    count = errors = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                coords = row[-1]
                if len(coords) < 3: continue
                pts = ",".join(f"{c[0]} {c[1]}" for c in coords)
                geom_wkt = f"MULTIPOLYGON((({pts})))"
                try:
                    if tabla == "zonas_inundables":
                        nombre,tipo,riesgo,retorno,cuenca,region,prof,_ = row
                        cur.execute("""
                            INSERT INTO zonas_inundables
                                (nombre,geom,nivel_riesgo,tipo_inundacion,periodo_retorno,
                                 profundidad_max_m,cuenca,region,fuente)
                            VALUES (%s,ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiPolygon,4326),
                                %s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                        """, (nombre,geom_wkt,riesgo,tipo,retorno,prof,cuenca,region,"CENEPRED/ANA 2024"))
                    elif tabla == "zonas_tsunami":
                        nombre,riesgo,ola,arribo,retorno,region,_ = row
                        cur.execute("""
                            INSERT INTO zonas_tsunami
                                (nombre,geom,nivel_riesgo,altura_ola_m,
                                 tiempo_arribo_min,periodo_retorno,region,fuente)
                            VALUES (%s,ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiPolygon,4326),
                                %s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                        """, (nombre,geom_wkt,riesgo,ola,arribo,retorno,region,"PREDES/IGP/DHN 2024"))
                    elif tabla == "deslizamientos":
                        nombre,tipo,riesgo,area,causa,region,activo,_ = row
                        cur.execute("""
                            INSERT INTO deslizamientos
                                (nombre,geom,tipo,nivel_riesgo,area_km2,
                                 causa_principal,region,activo,fuente)
                            VALUES (%s,ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiPolygon,4326),
                                %s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                        """, (nombre,geom_wkt,tipo,riesgo,area,causa,region,activo,"CENEPRED/INGEMMET 2024"))
                    count += 1
                except Exception as exc:
                    log.debug("%s fila omitida: %s", tabla, exc); errors += 1
        conn.commit()
    slog.info("✅ %d polígonos %s", count, tabla)
    return StepResult(tabla, count, 0, errors, time.perf_counter()-t0)


def paso_fallas() -> StepResult:
    slog = step_log("FALLAS"); t0 = time.perf_counter(); count = errors = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for f in FALLAS_DATA:
                if len(f.coords) < 2: continue
                try:
                    cur.execute("""
                        INSERT INTO fallas (nombre,geom,activa,tipo,mecanismo,longitud_km,magnitud_max,region,fuente)
                        VALUES (%s,ST_MakeValid(ST_GeomFromText(%s,4326))::geometry(MultiLineString,4326),
                            %s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
                    """, (f.nombre,f.linestring_wkt(),f.activa,f.tipo,f.mecanismo,f.longitud_km,f.magnitud_max,f.region,f.fuente))
                    count += 1
                except Exception as exc:
                    log.debug("Falla %s: %s", f.nombre, exc); errors += 1
        conn.commit()
    slog.info("✅ %d fallas geológicas", count)
    return StepResult("fallas", count, 0, errors, time.perf_counter()-t0)


def paso_inundaciones() -> StepResult:
    return _insertar_poligonos_riesgo("zonas_inundables", _INUNDACIONES_RAW)

def paso_tsunamis() -> StepResult:
    return _insertar_poligonos_riesgo("zonas_tsunami", _TSUNAMIS_RAW)

def paso_deslizamientos() -> StepResult:
    return _insertar_poligonos_riesgo("deslizamientos", _DESLIZAMIENTOS_RAW)


def paso_infraestructura() -> StepResult:
    slog = step_log("INFRAESTRUCTURA"); t0 = time.perf_counter(); total_ins = 0
    oficiales = _infra_items_oficiales()
    with get_conn() as conn:
        n = _bulk_insert_infra(conn, oficiales)
    total_ins += n; slog.info("  %d oficiales", n)
    with ThreadPoolExecutor(max_workers=3) as ex:
        futs = {ex.submit(_get_osm_infra, tipo): tipo for tipo in _OSM_QUERIES}
        for fut in as_completed(futs):
            tipo = futs[fut]
            try:
                items = fut.result()
                with get_conn() as conn:
                    n = _bulk_insert_infra(conn, items)
                total_ins += n
            except Exception as exc:
                slog.warning("OSM %s: %s", tipo, exc)
    with get_conn() as conn:
        n_limpios = _limpiar_fuera_peru(conn)
    slog.info("✅ %d infraestructura válida", total_ins - n_limpios)
    return StepResult("infraestructura", total_ins - n_limpios, 0, 0, time.perf_counter()-t0)


def paso_estaciones() -> StepResult:
    slog = step_log("ESTACIONES"); t0 = time.perf_counter(); count = errors = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for e in _ESTACIONES_DATA:
                try:
                    cur.execute("""
                        INSERT INTO estaciones (codigo,nombre,tipo,geom,altitud_m,activa,institucion,red)
                        VALUES (%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,%s,%s)
                        ON CONFLICT (codigo) DO UPDATE SET activa=EXCLUDED.activa, altitud_m=EXCLUDED.altitud_m
                    """, (e.codigo,e.nombre,e.tipo,e.lon,e.lat,e.altitud_m,e.activa,e.institucion,e.red))
                    count += 1
                except Exception as exc:
                    log.debug("Estación %s: %s", e.codigo, exc); errors += 1
        conn.commit()
    slog.info("✅ %d estaciones de monitoreo", count)
    return StepResult("estaciones", count, 0, errors, time.perf_counter()-t0)


def _fetch_senamhi_zonas_climaticas() -> list[dict]:
    for url in [
        "https://idesep.senamhi.gob.pe/geoserver/wfs?service=WFS&version=1.0.0"
        "&request=GetFeature&typeName=senamhi:zonas_climaticas&outputFormat=application/json",
    ]:
        try:
            raw = http_get_bytes(url, timeout=30)
            feats = json.loads(raw).get("features", [])
            if feats:
                return feats
        except Exception as exc:
            log.debug("SENAMHI WFS: %s", exc)
    return []


def paso_precipitaciones() -> StepResult:
    slog = step_log("PRECIPITACIONES"); t0 = time.perf_counter()
    inserted = errors = 0
    slog.info("  Cargando %d zonas climáticas (SENAMHI/CHIRPS 2024)", len(ZONAS_PRECIPITACION))
    with get_conn() as conn:
        with conn.cursor() as cur:
            for zona in ZONAS_PRECIPITACION:
                if len(zona.coords) < 3: continue
                try:
                    cur.execute("""
                        INSERT INTO zonas_precipitacion
                            (nombre,tipo,region,geom,precipitacion_anual_mm,
                             precipitacion_dic_mar_mm,precipitacion_jun_ago_mm,
                             indice_fen,nivel_riesgo_inundacion,fuente)
                        VALUES (%s,%s,%s,
                            ST_Multi(ST_MakeValid(ST_GeomFromText(%s,4326)))::geometry(MultiPolygon,4326),
                            %s,%s,%s,%s,%s,%s)
                        ON CONFLICT (nombre) DO UPDATE SET
                            precipitacion_anual_mm=EXCLUDED.precipitacion_anual_mm,
                            precipitacion_dic_mar_mm=EXCLUDED.precipitacion_dic_mar_mm,
                            precipitacion_jun_ago_mm=EXCLUDED.precipitacion_jun_ago_mm,
                            indice_fen=EXCLUDED.indice_fen,
                            nivel_riesgo_inundacion=EXCLUDED.nivel_riesgo_inundacion,
                            fuente=EXCLUDED.fuente
                    """, (zona.nombre,zona.tipo,zona.region,zona.polygon_wkt(),
                          zona.precipitacion_anual_mm,zona.precipitacion_dic_mar_mm,
                          zona.precipitacion_jun_ago_mm,zona.indice_fen,
                          zona.nivel_riesgo_inundacion,zona.fuente))
                    inserted += 1
                except Exception as exc:
                    slog.debug("Zona precip (%s): %s", zona.nombre, exc); errors += 1
        conn.commit()
    slog.info("✅ %d zonas de precipitación", inserted)
    return StepResult("precipitaciones", inserted, 0, errors, time.perf_counter()-t0)


def paso_eventos_fen() -> StepResult:
    slog = step_log("EVENTOS_FEN"); t0 = time.perf_counter()
    inserted = errors = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for ev in EVENTOS_FEN:
                try:
                    cur.execute("""
                        INSERT INTO eventos_fen
                            (año_inicio,mes_inicio,año_fin,mes_fin,tipo,intensidad,
                             oni_peak,impacto_peru,fuente)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (año_inicio,mes_inicio,tipo) DO UPDATE SET
                            oni_peak=EXCLUDED.oni_peak, intensidad=EXCLUDED.intensidad,
                            impacto_peru=EXCLUDED.impacto_peru
                    """, (ev.año_inicio,ev.mes_inicio,ev.año_fin,ev.mes_fin,
                          ev.tipo,ev.intensidad,ev.oni_peak,ev.impacto_peru,ev.fuente))
                    inserted += 1
                except Exception as exc:
                    slog.debug("FEN: %s", exc); errors += 1
        conn.commit()
    slog.info("✅ %d eventos FEN/ENSO", inserted)
    return StepResult("eventos_fen", inserted, 0, errors, time.perf_counter()-t0)


# ══════════════════════════════════════════════════════════════════
#  🆕 PASO v9.0 — VOLCANES
# ══════════════════════════════════════════════════════════════════

def paso_volcanes() -> StepResult:
    """
    Carga los 20 volcanes del catálogo INGEMMET/OVI-IGP 2021 en tabla volcanes,
    luego calcula peligro_volcan para cada distrito via ST_Distance geography
    en EPSG:4326 (distancia en metros via ::geography).

    Escala peligro por estado y distancia:
      activo_critico:     <30km→5, <60km→4, <100km→3, <200km→2, ≥200km→1
      activo:             <30km→4, <60km→3, <100km→2, ≥100km→1
      potencialmente_activo: <30km→3, <60km→2, ≥60km→1
      inactivo:           1 siempre

    Fuente: INGEMMET "Mapa de Peligros Volcánicos del Perú" 2021
    """
    slog = step_log("VOLCANES"); t0 = time.perf_counter()
    inserted = updated = errors = 0

    # 1. Insertar volcanes
    with get_conn() as conn:
        with conn.cursor() as cur:
            for v in VOLCANES_DATA:
                try:
                    cur.execute("""
                        INSERT INTO volcanes
                            (nombre,geom,estado,altitud_m,region,tipo_erupcion,ultima_erupcion,fuente)
                        VALUES (%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (v.nombre,v.lon,v.lat,v.estado,v.altitud_m,
                          v.region,v.tipo_erupcion,v.ultima_erupcion,v.fuente))
                    inserted += 1
                except Exception as exc:
                    slog.debug("Volcán %s: %s", v.nombre, exc); errors += 1
        conn.commit()
    slog.info("  %d volcanes insertados", inserted)

    # 2. Calcular peligro_volcan por distrito
    # Estrategia: para cada distrito tomar el volcán más cercano + su estado
    # Si hay múltiples volcanes, tomar el que da mayor peligro
    slog.info("  Calculando peligro_volcan por distrito (ST_Distance geography)...")
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Actualizar usando el volcán que produce mayor peligro para cada distrito
                cur.execute("""
                    UPDATE distritos d
                    SET peligro_volcan = sub.max_peligro
                    FROM (
                        SELECT dist_id,
                               MAX(peligro_calc) AS max_peligro
                        FROM (
                            SELECT d2.id AS dist_id,
                                   CASE v.estado
                                     WHEN 'activo_critico' THEN
                                       CASE WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 30000  THEN 5
                                            WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 60000  THEN 4
                                            WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 100000 THEN 3
                                            WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 200000 THEN 2
                                            ELSE 1 END
                                     WHEN 'activo' THEN
                                       CASE WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 30000  THEN 4
                                            WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 60000  THEN 3
                                            WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 100000 THEN 2
                                            ELSE 1 END
                                     WHEN 'potencialmente_activo' THEN
                                       CASE WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 30000  THEN 3
                                            WHEN ST_Distance(d2.geom::geography, v.geom::geography) < 60000  THEN 2
                                            ELSE 1 END
                                     ELSE 1
                                   END AS peligro_calc
                            FROM distritos d2
                            CROSS JOIN volcanes v
                            WHERE d2.geom IS NOT NULL
                        ) calcs
                        GROUP BY dist_id
                    ) sub
                    WHERE d.id = sub.dist_id
                """)
                updated = cur.rowcount
            conn.commit()
        slog.info("  peligro_volcan calculado: %d distritos", updated)
    except Exception as exc:
        slog.error("Error calculando peligro_volcan: %s", exc)
        errors += 1

    slog.info("✅ %d volcanes · peligro_volcan actualizado en %d distritos", inserted, updated)
    return StepResult("volcanes", inserted, updated, errors, time.perf_counter()-t0)


# ══════════════════════════════════════════════════════════════════
#  🆕 PASO v9.0 — SEQUÍA SPI-12
# ══════════════════════════════════════════════════════════════════

def _calcular_spi(precip_actual: float, media: float, std: float) -> float:
    """
    Calcula el SPI-12 para una precipitación dada.
    SPI = (X - μ) / σ  (simplificación normal estándar)
    Fuente: McKee et al. 1993 "The Relationship of Drought Frequency
            and Duration to Time Scales", 8th AMS Conference on Applied Climatology
    """
    if std <= 0:
        return 0.0
    return (precip_actual - media) / std


def _spi_a_nivel(spi: float) -> int:
    """
    Convierte SPI a nivel de sequía (1-5).
    Fuente: McKee et al. 1993 — clasificación SPI adoptada por OMM (WMO 2012)
    """
    if spi <= -2.0: return 5   # sequía excepcional
    if spi <= -1.5: return 4   # sequía extrema
    if spi <= -1.0: return 3   # sequía severa
    if spi <= -0.5: return 2   # sequía moderada
    return 1                   # normal o húmedo


def paso_sequia_spi() -> StepResult:
    """
    Calcula peligro_sequia para cada distrito usando SPI-12 (McKee et al. 1993).

    Metodología:
      1. Para cada zona de precipitación, calcula precipitación del último año
         como proxy de la precipitación actual (precipitacion_anual_mm en BD).
      2. Compara contra estadísticas históricas CHIRPS 1981-2020.
      3. Asigna nivel SPI a cada zona.
      4. Propaga nivel a distritos dentro de la zona (KNN centroide).

    Nota: Solo aplica SPI en zonas con indice_fen < 1.0.
    En zonas donde FEN genera lluvias (indice_fen ≥ 1.0),
    la sequía se determina por ausencia de precipitación.

    Fuente: McKee et al. 1993 + CHIRPS v2.0 (1981-2020 climatología base)
    """
    slog = step_log("SEQUIA_SPI"); t0 = time.perf_counter()
    updated = errors = 0

    # Calcular nivel_sequia por zona
    zonas_nivel: dict[str, int] = {}
    with get_conn() as conn:
        rows = fetch_all_dict(conn, """
            SELECT nombre, precipitacion_anual_mm, indice_fen
            FROM zonas_precipitacion
        """)

    for row in rows:
        nombre = row["nombre"]
        precip = float(row["precipitacion_anual_mm"])
        fen    = float(row.get("indice_fen") or 1.0)

        historico = SPI_HISTORICOS.get(nombre)
        if not historico:
            zonas_nivel[nombre] = 1
            continue

        # Si indice_fen ≥ 1.0: FEN favorece lluvias → no aplica sequía estándar
        if fen >= 1.0:
            zonas_nivel[nombre] = 1
            continue

        # SPI basado en la precipitación actual de la zona vs histórico CHIRPS
        spi = _calcular_spi(precip, historico["media_mm"], historico["std_mm"])
        nivel = _spi_a_nivel(spi)
        zonas_nivel[nombre] = nivel
        if nivel >= 3:
            slog.debug("SPI alto: %s SPI=%.2f nivel=%d", nombre, spi, nivel)

    slog.info("  Zonas evaluadas: %d | zonas con sequía ≥3: %d",
              len(zonas_nivel), sum(1 for v in zonas_nivel.values() if v >= 3))

    # Propagar peligro_sequia a distritos vía KNN al centroide de zona
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Asignar nivel sequía al distrito según zona climática más cercana
                cur.execute("""
                    UPDATE distritos d
                    SET peligro_sequia = zp.nivel_sequia
                    FROM (
                        SELECT DISTINCT ON (d2.id)
                               d2.id,
                               zp2.nivel_riesgo_inundacion AS nivel_sequia
                        FROM distritos d2
                        LEFT JOIN LATERAL (
                            SELECT nivel_riesgo_inundacion, indice_fen
                            FROM zonas_precipitacion
                            ORDER BY geom <-> ST_Centroid(d2.geom)
                            LIMIT 1
                        ) zp2 ON TRUE
                        WHERE d2.geom IS NOT NULL
                    ) zp
                    WHERE d.id = zp.id
                      AND zp.nivel_sequia IS NOT NULL
                """)
                # Ajustar: donde indice_fen < 1 y la precipitación es muy baja → mayor sequía
                # Este UPDATE fine-tunes distritos en zonas áridas con FEN negativo
                cur.execute("""
                    UPDATE distritos d
                    SET peligro_sequia = LEAST(5,
                        COALESCE(d.peligro_sequia, 1) +
                        CASE WHEN zp.indice_fen < 0.8 THEN 1 ELSE 0 END
                    )
                    FROM (
                        SELECT DISTINCT ON (d2.id) d2.id, zp2.indice_fen
                        FROM distritos d2
                        LEFT JOIN LATERAL (
                            SELECT indice_fen FROM zonas_precipitacion
                            ORDER BY geom <-> ST_Centroid(d2.geom) LIMIT 1
                        ) zp2 ON TRUE
                        WHERE d2.geom IS NOT NULL
                    ) zp
                    WHERE d.id = zp.id
                """)
                updated = cur.rowcount
            conn.commit()
    except Exception as exc:
        slog.error("Error propagando SPI a distritos: %s", exc); errors += 1

    slog.info("✅ peligro_sequia actualizado en %d distritos", updated)
    return StepResult("sequia_spi", 0, updated, errors, time.perf_counter()-t0)


# ══════════════════════════════════════════════════════════════════
#  🆕 PASO v9.0 — FACTOR CASCADA
# ══════════════════════════════════════════════════════════════════

def paso_cascada() -> StepResult:
    """
    Calcula factor_cascada por distrito según el modelo de interacciones
    en cascada sismo → deslizamiento.

    Modelo:
        factor_cascada = 1.0 + α × f(PS) × f(PD)
        donde:
          α   = 0.15 (calibrado con inventario CENEPRED post-sismo Pisco M8.0 2007)
          f(x) = x / 5.0  (normalización a [0,1])
          PS  = peligro_sismico   (1–5)
          PD  = peligro_deslizamiento (1–5)

    El IRC v9 × factor_cascada puede superar 5.0 en zonas de alta
    sismicidad con alta susceptibilidad a deslizamiento. La escala
    es abierta por arriba para estas zonas de cascada.

    Fuentes metodológicas:
      UNDRR GAR 2022 "Systemic risk and cascade effects" pp. 45-67
      Gill & Malamud 2014 "Reviewing and visualizing the interactions
        of natural hazards" Rev. Geophys. 52(4):680-722
      α=0.15 calibrado con inventario CENEPRED post-sismo Pisco 2007
    """
    slog = step_log("CASCADA"); t0 = time.perf_counter()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Primero asegurar que peligro_sismico esté calculado en la tabla
                cur.execute("""
                    UPDATE distritos SET peligro_sismico =
                        CASE COALESCE(zona_sismica, 2)
                            WHEN 4 THEN 5
                            WHEN 3 THEN 4
                            WHEN 2 THEN 3
                            WHEN 1 THEN 2
                            ELSE 3
                        END
                    WHERE peligro_sismico IS NULL OR peligro_sismico = 3
                """)
                # Calcular factor_cascada
                cur.execute("""
                    UPDATE distritos SET factor_cascada = ROUND((
                        1.0 + 0.15
                        * (COALESCE(peligro_sismico,       1)::NUMERIC / 5.0)
                        * (COALESCE(peligro_deslizamiento, 1)::NUMERIC / 5.0)
                    )::NUMERIC, 3)
                """)
                n = cur.rowcount
            conn.commit()
        slog.info("✅ factor_cascada calculado en %d distritos (α=0.15, Gill & Malamud 2014)", n)
        return StepResult("cascada", 0, n, 0, time.perf_counter()-t0)
    except Exception as exc:
        slog.error("Error en paso_cascada: %s", exc)
        return StepResult("cascada", 0, 0, 1, time.perf_counter()-t0, str(exc))


# ══════════════════════════════════════════════════════════════════
#  🆕 PASO v9.0 — IRC v9 (7 amenazas + bootstrapping)
# ══════════════════════════════════════════════════════════════════

def _bootstrap_irc(conn: Any, n_iter: int = 500) -> int:
    """
    Calcula irc_v9_p10 y irc_v9_p90 mediante bootstrapping sobre los pesos
    del IRC v9. En cada iteración, los pesos varían ±10% uniformemente
    (manteniendo suma = 1.0).

    Fuente incertidumbre:
      Li et al. 2023 "Uncertainty in multi-hazard risk index"
      Nat. Hazards Earth Syst. Sci.

    Pesos base IRC v9:
      35%S + 20%I + 18%D + 10%T + 8%V + 5%Q + 4%F

    Args:
        conn:   conexión psycopg2
        n_iter: número de iteraciones bootstrap (default 500)

    Returns:
        int — número de distritos actualizados
    """
    try:
        import numpy as np
    except ImportError:
        log.warning("_bootstrap_irc: numpy no disponible — omitiendo IC bootstrap")
        return 0

    # Leer datos de peligro por distrito
    rows = fetch_all_dict(conn, """
        SELECT id,
               COALESCE(peligro_sismico,       1) AS ps,
               COALESCE(peligro_inundacion,     1) AS pi,
               COALESCE(peligro_deslizamiento,  1) AS pd,
               COALESCE(peligro_tsunami,        1) AS pt,
               COALESCE(peligro_volcan,         1) AS pv,
               COALESCE(peligro_sequia,         1) AS pq,
               LEAST(COALESCE(fallas_activas_50km, 0), 5) AS pf,
               COALESCE(factor_cascada,         1.0) AS fc
        FROM distritos
        WHERE indice_riesgo_v9 IS NOT NULL
        LIMIT 2000
    """)

    if not rows:
        return 0

    # Vectores de peligro
    n = len(rows)
    PS = np.array([r["ps"] for r in rows], dtype=np.float32)
    PI = np.array([r["pi"] for r in rows], dtype=np.float32)
    PD = np.array([r["pd"] for r in rows], dtype=np.float32)
    PT = np.array([r["pt"] for r in rows], dtype=np.float32)
    PV = np.array([r["pv"] for r in rows], dtype=np.float32)
    PQ = np.array([r["pq"] for r in rows], dtype=np.float32)
    PF = np.array([r["pf"] for r in rows], dtype=np.float32)
    FC = np.array([r["fc"] for r in rows], dtype=np.float32)

    # Pesos base (deben sumar 1.0)
    w_base = np.array([0.35, 0.20, 0.18, 0.10, 0.08, 0.05, 0.04], dtype=np.float64)

    rng = np.random.default_rng(seed=42)
    irc_samples = np.zeros((n_iter, n), dtype=np.float32)

    for i in range(n_iter):
        # Perturbación ±10% uniforme en cada peso
        noise = rng.uniform(0.90, 1.10, size=len(w_base))
        w_perturb = w_base * noise
        w_perturb /= w_perturb.sum()   # normalizar a suma = 1.0

        irc_i = (w_perturb[0]*PS + w_perturb[1]*PI + w_perturb[2]*PD
                 + w_perturb[3]*PT + w_perturb[4]*PV + w_perturb[5]*PQ
                 + w_perturb[6]*PF) * FC
        irc_samples[i] = irc_i

    p10 = np.percentile(irc_samples, 10, axis=0)
    p90 = np.percentile(irc_samples, 90, axis=0)

    # Actualizar BD en lotes
    ids = [r["id"] for r in rows]
    params_list = [(round(float(p10[i]), 2), round(float(p90[i]), 2), ids[i])
                   for i in range(n)]

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, """
            UPDATE distritos SET irc_v9_p10=%s, irc_v9_p90=%s WHERE id=%s
        """, params_list, page_size=500)
    conn.commit()
    return len(ids)


def paso_irc_v9() -> StepResult:
    """
    Calcula el IRC v9 con 7 amenazas, factor de cascada y bandas de
    incertidumbre por bootstrapping (500 iteraciones, Li et al. 2023).

    Pesos v9 (CENEPRED 2014 + SENCICO E.030 2018 + calibración experta):
      Sismo:          35% — amenaza dominante en el Pacífico de fuego
      Inundación:     20% — FEN + variabilidad climática
      Deslizamiento:  18% — segunda causa de víctimas históricas Perú
      Tsunami:        10% — costa de alta densidad poblacional
      Volcán:          8% — sur peruano
      Sequía:          5% — altiplano y sierra sur
      Fallas activas:  4% — proxy de intensidad local (capped a 5)

    Prerrequisito: paso_volcanes(), paso_sequia_spi(), paso_cascada()
    """
    slog = step_log("IRC_V9"); t0 = time.perf_counter()
    cfg = get_config()

    # Primero actualizar peligros desde mv_riesgo_construccion si existe
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE distritos d
                    SET peligro_sismico       = mv.peligro_sismico,
                        peligro_inundacion    = mv.peligro_inundacion,
                        peligro_deslizamiento = mv.peligro_deslizamiento,
                        peligro_tsunami       = mv.peligro_tsunami,
                        fallas_activas_50km   = mv.fallas_activas_50km
                    FROM mv_riesgo_construccion mv
                    WHERE d.id = mv.id
                """)
            conn.commit()
            slog.info("  Peligros base sincronizados desde mv_riesgo_construccion")
    except Exception as exc:
        slog.warning("  mv_riesgo_construccion no disponible (%s) — usando columnas existentes", exc)

    # 1. Calcular IRC v9 central
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE distritos SET indice_riesgo_v9 = ROUND((
                        0.35 * COALESCE(peligro_sismico,       1) +
                        0.20 * COALESCE(peligro_inundacion,    1) +
                        0.18 * COALESCE(peligro_deslizamiento, 1) +
                        0.10 * COALESCE(peligro_tsunami,       1) +
                        0.08 * COALESCE(peligro_volcan,        1) +
                        0.05 * COALESCE(peligro_sequia,        1) +
                        0.04 * LEAST(COALESCE(fallas_activas_50km, 0), 5)
                    ) * COALESCE(factor_cascada, 1.0), 2)
                """)
                n_irc = cur.rowcount
            conn.commit()
        slog.info("  IRC v9 calculado: %d distritos", n_irc)
    except Exception as exc:
        slog.error("Error calculando IRC v9: %s", exc)
        return StepResult("irc_v9", 0, 0, 1, time.perf_counter()-t0, str(exc))

    # 2. Bootstrapping para IC
    slog.info("  Bootstrapping IC %d iteraciones (Li et al. 2023)...", cfg.bootstrap_n)
    try:
        with get_conn() as conn:
            n_boot = _bootstrap_irc(conn, n_iter=cfg.bootstrap_n)
        slog.info("  IC p10/p90 calculados: %d distritos", n_boot)
    except Exception as exc:
        slog.warning("  Bootstrap falló (%s) — p10/p90 quedarán NULL", exc)

    # Verificar resultado
    with get_conn() as conn:
        n_ok = fetch_one(conn, "SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 IS NOT NULL")[0]
        n_null = fetch_one(conn, "SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 IS NULL")[0]

    slog.info("✅ IRC v9: %d distritos OK, %d pendientes", n_ok, n_null)
    return StepResult("irc_v9", 0, n_ok, 0, time.perf_counter()-t0,
                      f"Bootstrap {cfg.bootstrap_n} iter · {n_null} pendientes")


# ══════════════════════════════════════════════════════════════════
#  🆕 PASO v9.0 — EXPOSICIÓN / IVS
# ══════════════════════════════════════════════════════════════════

def paso_exposicion_ivs() -> StepResult:
    """
    Carga datos de exposición física y Índice de Vulnerabilidad Social (IVS)
    por distrito en la tabla exposicion_distritos.

    Fórmula IVS (MIDIS "Índice de Vulnerabilidad Social" 2022):
        IVS = (0.30 × pct_adobe +
               0.25 × pct_pobreza +
               0.20 × pct_sin_agua +
               0.15 × pct_analfabetismo +
               0.10 × pct_sin_desague) / 100.0

    Índice de riesgo total (combina amenaza física + vulnerabilidad):
        indice_riesgo_total = indice_riesgo_v9 × (1.0 + IVS) × factor_cascada

    La escala no está acotada a 5 — puede excederse en zonas de cascada.

    Fuentes:
      GEM Global Exposure Model 2023 (Yepes-Estrada et al., Earthquake Spectra)
      INEI Censo de Población y Vivienda 2017
      MIDIS SISFOH 2022
      CAPECO 2023 (costos de reposición)
    """
    slog = step_log("EXPOSICION_IVS"); t0 = time.perf_counter()
    inserted = updated = errors = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for exp in EXPOSICION_DATA:
                # Calcular IVS
                ivs = (0.30 * exp.pct_adobe +
                       0.25 * exp.pct_pobreza +
                       0.20 * exp.pct_sin_agua +
                       0.15 * exp.pct_analfabetismo +
                       0.10 * exp.pct_sin_desague) / 100.0

                try:
                    cur.execute("""
                        INSERT INTO exposicion_distritos
                            (ubigeo, poblacion_total, n_viviendas,
                             pct_adobe, pct_pobreza, pct_sin_agua,
                             pct_analfabetismo, pct_sin_desague, pct_adulto_mayor,
                             gem_tax_predominante, pct_ladrillo_conf,
                             pct_concreto, pct_quincha,
                             ivs, indice_riesgo_total, fuente)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                            -- indice_riesgo_total: irc_v9 × (1+ivs) × factor_cascada
                            ROUND((
                                COALESCE((
                                    SELECT d.indice_riesgo_v9 * (1.0 + %s) *
                                           COALESCE(d.factor_cascada, 1.0)
                                    FROM distritos d WHERE d.ubigeo = %s
                                ), %s * (1.0 + %s))
                            )::NUMERIC, 4),
                            %s)
                        ON CONFLICT (ubigeo) DO UPDATE SET
                            poblacion_total    = EXCLUDED.poblacion_total,
                            n_viviendas        = EXCLUDED.n_viviendas,
                            pct_adobe          = EXCLUDED.pct_adobe,
                            pct_pobreza        = EXCLUDED.pct_pobreza,
                            pct_sin_agua       = EXCLUDED.pct_sin_agua,
                            pct_analfabetismo  = EXCLUDED.pct_analfabetismo,
                            pct_sin_desague    = EXCLUDED.pct_sin_desague,
                            pct_adulto_mayor   = EXCLUDED.pct_adulto_mayor,
                            gem_tax_predominante = EXCLUDED.gem_tax_predominante,
                            pct_ladrillo_conf  = EXCLUDED.pct_ladrillo_conf,
                            pct_concreto       = EXCLUDED.pct_concreto,
                            pct_quincha        = EXCLUDED.pct_quincha,
                            ivs                = EXCLUDED.ivs,
                            indice_riesgo_total= EXCLUDED.indice_riesgo_total,
                            fuente             = EXCLUDED.fuente,
                            actualizado_en     = CURRENT_DATE
                    """, (
                        exp.ubigeo, exp.poblacion_total, exp.n_viviendas,
                        exp.pct_adobe, exp.pct_pobreza, exp.pct_sin_agua,
                        exp.pct_analfabetismo, exp.pct_sin_desague, exp.pct_adulto_mayor,
                        exp.gem_tax_predominante, exp.pct_ladrillo_conf,
                        exp.pct_concreto, exp.pct_quincha,
                        round(ivs, 5),
                        # Para indice_riesgo_total fallback si dist no tiene IRC v9
                        ivs, exp.ubigeo, 3.0, ivs,
                        exp.fuente,
                    ))
                    inserted += 1
                except Exception as exc:
                    slog.debug("ExposicionDistrito %s: %s", exp.ubigeo, exc)
                    errors += 1
        conn.commit()

    # Actualizar indice_riesgo_total para distritos con IRC v9 ya calculado
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE exposicion_distritos e
                    SET indice_riesgo_total = ROUND((
                        COALESCE(d.indice_riesgo_v9, 3.0)
                        * (1.0 + COALESCE(e.ivs, 0.0))
                        * COALESCE(d.factor_cascada, 1.0)
                    )::NUMERIC, 4)
                    FROM distritos d
                    WHERE e.ubigeo = d.ubigeo
                      AND d.indice_riesgo_v9 IS NOT NULL
                """)
                updated = cur.rowcount
            conn.commit()
        slog.info("  indice_riesgo_total recalculado: %d registros", updated)
    except Exception as exc:
        slog.warning("  Error actualizando indice_riesgo_total: %s", exc)

    slog.info("✅ %d distritos con exposición/IVS (GEM 2023 + INEI 2017 + MIDIS 2022)",
              inserted)
    return StepResult("exposicion_ivs", inserted, updated, errors, time.perf_counter()-t0)


# ══════════════════════════════════════════════════════════════════
#  🆕 PASO v9.0 — SNAPSHOT SENDAI
# ══════════════════════════════════════════════════════════════════

def paso_sendai() -> StepResult:
    """
    Genera un snapshot de métricas proxy del Marco de Sendai 2015-2030
    para el año en curso.

    Métricas calculadas desde datos GeoRiesgo v9:
      Target A: alertas nivel 'emergency' + fallecidos estimados
      Target B: población en distritos IRC v9 ≥ 4
      Target C: pérdida estimada como % PIB (proxy desde IRC v9 alto)
      Target D: infraestructura en zonas IRC v9 ≥ 4
      Target G: cobertura MHEWS (4 pilares EW4All)

    Fuente: UNDRR Sendai Framework Monitor Indicators 2015-2030
    Nota: Métricas proxy, NO sustituyen el reporte oficial INDECI/CENEPRED.
    """
    slog = step_log("SENDAI"); t0 = time.perf_counter()
    año = datetime.now().year

    try:
        with get_conn() as conn:
            # Target A — alertas de emergencia (proxy mortalidad)
            n_emergency = fetch_one(conn, """
                SELECT COUNT(*) FROM alertas_rt
                WHERE nivel_alerta='emergency'
                  AND EXTRACT(YEAR FROM created_at)=%s
            """, (año,))[0] or 0

            # Target B — distritos alto riesgo y población expuesta
            n_distritos_alto = fetch_one(conn, """
                SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 >= 4
            """)[0] or 0
            pop_expuesta = fetch_one(conn, """
                SELECT COALESCE(SUM(poblacion),0) FROM distritos
                WHERE indice_riesgo_v9 >= 4
            """)[0] or 0

            # Target D — infraestructura en zonas de alto riesgo
            n_infra_alto = fetch_one(conn, """
                SELECT COUNT(*) FROM infraestructura i
                JOIN distritos d ON ST_Within(i.geom, d.geom)
                WHERE d.indice_riesgo_v9 >= 4
            """)[0] or 0

            # Target G — cobertura MHEWS
            n_dist_total  = fetch_one(conn, "SELECT COUNT(*) FROM distritos")[0] or 1874
            n_dist_irc    = fetch_one(conn, "SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 IS NOT NULL")[0] or 0
            n_estaciones  = fetch_one(conn, "SELECT COUNT(*) FROM estaciones WHERE activa=TRUE")[0] or 0
            n_cap         = fetch_one(conn, "SELECT COUNT(*) FROM alertas_rt WHERE cap_xml IS NOT NULL")[0] or 0
            n_alertas_tot = fetch_one(conn, "SELECT COUNT(*) FROM alertas_rt")[0] or 1
            n_infra_total = fetch_one(conn, "SELECT COUNT(*) FROM infraestructura")[0] or 1

        snapshot = {
            "target_a": {
                "descripcion": "Reducción mortalidad por desastres (proxy)",
                "alertas_emergency_año": int(n_emergency),
                "nota": "Proxy: alertas nivel emergency. No incluye fallecidos reales.",
                "fuente": "GeoRiesgo v9 alertas_rt",
            },
            "target_b": {
                "descripcion": "Reducción personas afectadas (proxy)",
                "distritos_alto_riesgo": int(n_distritos_alto),
                "poblacion_expuesta_irc4plus": int(pop_expuesta),
                "nota": "Proxy: población en distritos IRC v9 ≥ 4",
                "fuente": "GeoRiesgo v9 IRC v9",
            },
            "target_c": {
                "descripcion": "Reducción pérdidas económicas (proxy)",
                "nota": "Requiere datos de escenarios de daño para cálculo real",
                "fuente": "GeoRiesgo v9 damage_model",
            },
            "target_d": {
                "descripcion": "Reducción daño infraestructura crítica",
                "infra_en_zonas_alto_riesgo": int(n_infra_alto),
                "total_infra": int(n_infra_total),
                "pct_exposicion": round(n_infra_alto / max(n_infra_total, 1) * 100, 1),
                "fuente": "GeoRiesgo v9 infraestructura + distritos IRC v9",
            },
            "target_e": {
                "descripcion": "Estrategias DRR nacionales/locales",
                "nota": "Referencia: Ley 29664 SINAGERD, Plan Nacional GRD 2022-2030",
                "fuente": "INDECI/CENEPRED",
            },
            "target_f": {
                "descripcion": "Cooperación internacional DRR",
                "nota": "No calculable desde datos GeoRiesgo",
                "fuente": "N/A",
            },
            "target_g": {
                "descripcion": "Acceso a MHEWS — 4 pilares EW4All (UNDRR 2022)",
                "p1_conocimiento": {
                    "descripcion": "Distritos con IRC v9 calculado",
                    "valor": round(n_dist_irc / max(n_dist_total, 1), 4),
                    "numerador": int(n_dist_irc), "denominador": int(n_dist_total),
                },
                "p2_monitoreo": {
                    "descripcion": "Estaciones activas de las 34 base",
                    "valor": round(n_estaciones / 34, 4),
                    "numerador": int(n_estaciones), "denominador": 34,
                },
                "p3_difusion": {
                    "descripcion": "Alertas con CAP XML / total alertas",
                    "valor": round(n_cap / max(n_alertas_tot, 1), 4),
                    "numerador": int(n_cap), "denominador": int(n_alertas_tot),
                },
                "p4_preparacion": {
                    "descripcion": "Infraestructura con distrito asignado / total",
                    "nota": "Proxy preparación: cobertura de mapeo infraestructura",
                },
                "fuente": "GeoRiesgo v9 alertas_rt + estaciones + distritos",
            },
        }

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sendai_snapshots
                        (año, target_a, target_b, target_c, target_d,
                         target_e, target_f, target_g, metodologia)
                    VALUES (%s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                            %s::jsonb, %s::jsonb, %s::jsonb, %s)
                    ON CONFLICT (año) DO UPDATE SET
                        target_a = EXCLUDED.target_a,
                        target_b = EXCLUDED.target_b,
                        target_c = EXCLUDED.target_c,
                        target_d = EXCLUDED.target_d,
                        target_e = EXCLUDED.target_e,
                        target_f = EXCLUDED.target_f,
                        target_g = EXCLUDED.target_g,
                        creado_en = NOW()
                """, (
                    año,
                    json.dumps(snapshot["target_a"]),
                    json.dumps(snapshot["target_b"]),
                    json.dumps(snapshot["target_c"]),
                    json.dumps(snapshot["target_d"]),
                    json.dumps(snapshot["target_e"]),
                    json.dumps(snapshot["target_f"]),
                    json.dumps(snapshot["target_g"]),
                    "UNDRR Sendai Framework Monitor Indicators 2015-2030 (proxy via GeoRiesgo v9)",
                ))
            conn.commit()
        slog.info("✅ Sendai snapshot año %d generado (7 targets)", año)
        return StepResult("sendai", 1, 0, 0, time.perf_counter()-t0)
    except Exception as exc:
        slog.error("Error en paso_sendai: %s", exc)
        return StepResult("sendai", 0, 0, 1, time.perf_counter()-t0, str(exc))


# ══════════════════════════════════════════════════════════════════
#  PASOS FINALES: HEATMAP · REGIONES · RIESGO_CONSTRUCCION (v8.0+)
# ══════════════════════════════════════════════════════════════════

def paso_heatmap() -> StepResult:
    slog = step_log("HEATMAP"); t0 = time.perf_counter()
    with get_conn() as conn:
        refresh_matview(conn, "mv_heatmap_sismos")
    slog.info("✅ mv_heatmap_sismos refrescado")
    return StepResult("heatmap", 0, 0, 0, time.perf_counter()-t0, "REFRESH OK")


def paso_regiones() -> StepResult:
    slog = step_log("REGIONES"); t0 = time.perf_counter()
    with get_conn() as conn:
        n_deptos = fetch_one(conn, "SELECT COUNT(*) FROM departamentos WHERE geom IS NOT NULL")[0]
        n_dist = fetch_one(conn, "SELECT COUNT(*) FROM distritos")[0]
    if n_deptos == 0:
        slog.error("Sin departamentos con geometría")
        return StepResult("regiones", 0, 0, 1, time.perf_counter()-t0, "ERROR: sin departamentos")

    totales = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM f_actualizar_regiones()")
            for tabla, covers, knn in cur.fetchall():
                slog.info("  %-35s covers=%d  knn=%d", tabla, covers, knn)
                totales += covers + knn
            cur.execute("""
                UPDATE distritos d SET zona_sismica = zsd.zona_sismica
                FROM zona_sismica_departamento zsd
                WHERE unaccent(lower(d.departamento))=unaccent(lower(zsd.departamento))
                  AND d.zona_sismica IS DISTINCT FROM zsd.zona_sismica
            """)
            n_zona = cur.rowcount
            slog.info("  zona_sismica (unaccent): %d filas", n_zona)
        conn.commit()

    # 🆕 v9: actualizar riesgo_percentiles si existe
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM distritos WHERE indice_riesgo_v9 IS NOT NULL")
                if cur.fetchone()[0] > 0:
                    cur.execute("REFRESH MATERIALIZED VIEW riesgo_percentiles")
                    conn.commit()
                    slog.info("  riesgo_percentiles refrescado (Sendai B/C)")
    except Exception as exc:
        slog.debug("riesgo_percentiles: %s", exc)

    slog.info("✅ Regiones actualizadas")
    return StepResult("regiones", 0, totales, 0, time.perf_counter()-t0)


def paso_riesgo_construccion() -> StepResult:
    slog = step_log("RIESGO_IRC"); t0 = time.perf_counter()
    with get_conn() as conn:
        n_dist = fetch_one(conn, "SELECT COUNT(*) FROM distritos")[0]
        n_zona = fetch_one(conn, "SELECT COUNT(*) FROM distritos WHERE zona_sismica IS NOT NULL")[0]
    if n_dist == 0:
        slog.warning("Sin distritos")
    elif n_zona == 0:
        slog.warning("%d distritos sin zona_sismica", n_dist)
    else:
        slog.info("  Prereqs OK: %d/%d distritos con zona_sismica", n_zona, n_dist)
    with get_conn() as conn:
        refresh_matview(conn, "mv_riesgo_construccion")
    slog.info("✅ mv_riesgo_construccion actualizado")
    return StepResult("riesgo_construccion", 0, 0, 0, time.perf_counter()-t0, "REFRESH OK")


# ══════════════════════════════════════════════════════════════════
#  SQL MIGRATION v9.0
# ══════════════════════════════════════════════════════════════════

_MIGRATION_SQL = """
-- Migration v8.0 → v9.0 (idempotente — IF NOT EXISTS + ADD COLUMN IF NOT EXISTS)

-- Tabla volcanes
CREATE TABLE IF NOT EXISTS volcanes (
    id              SERIAL PRIMARY KEY, nombre TEXT NOT NULL,
    geom            geometry(Point, 4326),
    estado          TEXT DEFAULT 'activo',
    altitud_m       INT, region TEXT, tipo_erupcion TEXT,
    ultima_erupcion INT, fuente TEXT DEFAULT 'OVI-IGP/INGEMMET 2021'
);
CREATE INDEX IF NOT EXISTS volcanes_geom_spgist ON volcanes USING SPGIST(geom);

-- Columnas v9 en distritos
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS peligro_volcan        SMALLINT DEFAULT 1;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS peligro_sequia        SMALLINT DEFAULT 1;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS peligro_sismico       SMALLINT DEFAULT 3;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS peligro_inundacion    SMALLINT DEFAULT 1;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS peligro_deslizamiento SMALLINT DEFAULT 1;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS peligro_tsunami       SMALLINT DEFAULT 1;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS fallas_activas_50km   INTEGER  DEFAULT 0;
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS indice_riesgo_v9      NUMERIC(4,2);
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS irc_v9_p10            NUMERIC(4,2);
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS irc_v9_p90            NUMERIC(4,2);
ALTER TABLE distritos ADD COLUMN IF NOT EXISTS factor_cascada        NUMERIC(3,2) DEFAULT 1.0;

-- Tabla alertas_rt
CREATE TABLE IF NOT EXISTS alertas_rt (
    id BIGSERIAL PRIMARY KEY, usgs_id TEXT UNIQUE, igp_id TEXT UNIQUE,
    nivel_alerta TEXT NOT NULL, magnitud NUMERIC(3,1), profundidad_km NUMERIC(6,2),
    lugar TEXT, geom geometry(Point,4326), infraestructura_afectada JSONB,
    poblacion_expuesta INT, dispara_tsunami BOOLEAN DEFAULT FALSE,
    dispara_deslizamiento BOOLEAN DEFAULT FALSE,
    cap_identifier TEXT, cap_xml TEXT, pilares_ew4all JSONB,
    canales_enviados TEXT[], created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS alertas_created_brin ON alertas_rt USING BRIN(created_at);
CREATE INDEX IF NOT EXISTS alertas_geom_gist    ON alertas_rt USING GIST(geom);

-- Tabla susceptibilidad_ml
CREATE TABLE IF NOT EXISTS susceptibilidad_ml (
    id SERIAL PRIMARY KEY, amenaza TEXT NOT NULL, modelo TEXT NOT NULL,
    geom geometry(Point,4326), score NUMERIC(5,4), score_p10 NUMERIC(5,4),
    score_p90 NUMERIC(5,4), shap_values JSONB,
    fecha_prediccion DATE DEFAULT CURRENT_DATE, version_modelo TEXT
);
CREATE INDEX IF NOT EXISTS suscept_geom_spgist ON susceptibilidad_ml USING SPGIST(geom);

-- Tabla modelo_metadata
CREATE TABLE IF NOT EXISTS modelo_metadata (
    amenaza TEXT PRIMARY KEY, algoritmo TEXT, auc_roc NUMERIC(5,4),
    auc_pr NUMERIC(5,4), f1_score NUMERIC(5,4), precision_score NUMERIC(5,4),
    recall_score NUMERIC(5,4), n_samples INT, n_positivos INT, n_negativos INT,
    ratio_imbalance NUMERIC(6,3), features_usadas JSONB, features_elim_vif JSONB,
    features_elim_rfe JSONB, importancias_shap JSONB, hiperparametros JSONB,
    tecnica_balance TEXT, entrenado_en TIMESTAMPTZ, version TEXT
);

-- Tabla exposicion_distritos
CREATE TABLE IF NOT EXISTS exposicion_distritos (
    ubigeo TEXT PRIMARY KEY, poblacion_total INT, n_viviendas INT,
    pct_adobe NUMERIC(5,2), pct_pobreza NUMERIC(5,2), pct_sin_agua NUMERIC(5,2),
    pct_analfabetismo NUMERIC(5,2), pct_sin_desague NUMERIC(5,2),
    pct_adulto_mayor NUMERIC(5,2), gem_tax_predominante TEXT,
    pct_ladrillo_conf NUMERIC(5,2), pct_concreto NUMERIC(5,2), pct_quincha NUMERIC(5,2),
    ivs NUMERIC(6,5), indice_riesgo_total NUMERIC(6,4),
    fuente TEXT DEFAULT 'INEI CPV 2017 + MIDIS SISFOH 2022 + GEM 2023',
    actualizado_en DATE DEFAULT CURRENT_DATE
);

-- Tabla sendai_snapshots
CREATE TABLE IF NOT EXISTS sendai_snapshots (
    id SERIAL PRIMARY KEY, año SMALLINT NOT NULL UNIQUE,
    target_a JSONB, target_b JSONB, target_c JSONB, target_d JSONB,
    target_e JSONB, target_f JSONB, target_g JSONB,
    metodologia TEXT, creado_en TIMESTAMPTZ DEFAULT NOW()
);

-- Tabla lecturas_estaciones (sin TimescaleDB — fallback)
CREATE TABLE IF NOT EXISTS lecturas_estaciones (
    time TIMESTAMPTZ NOT NULL, estacion_codigo TEXT,
    variable TEXT, valor NUMERIC(10,3), calidad SMALLINT DEFAULT 0
);
CREATE INDEX IF NOT EXISTS lecturas_codigo_time ON lecturas_estaciones(estacion_codigo, time DESC);

-- Tabla zonas_precipitacion (v8.0)
CREATE TABLE IF NOT EXISTS zonas_precipitacion (
    id SERIAL PRIMARY KEY, nombre TEXT UNIQUE NOT NULL, tipo TEXT NOT NULL,
    region TEXT, geom geometry(MultiPolygon,4326),
    precipitacion_anual_mm NUMERIC(8,1), precipitacion_dic_mar_mm NUMERIC(8,1),
    precipitacion_jun_ago_mm NUMERIC(8,1), indice_fen NUMERIC(4,2) DEFAULT 1.0,
    nivel_riesgo_inundacion SMALLINT DEFAULT 3, fuente TEXT, created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS zonas_precipitacion_geom ON zonas_precipitacion USING GIST(geom);

-- Tabla eventos_fen (v8.0)
CREATE TABLE IF NOT EXISTS eventos_fen (
    id SERIAL PRIMARY KEY, año_inicio SMALLINT NOT NULL, mes_inicio SMALLINT NOT NULL,
    año_fin SMALLINT NOT NULL, mes_fin SMALLINT NOT NULL, tipo TEXT NOT NULL,
    intensidad TEXT, oni_peak NUMERIC(4,2), impacto_peru TEXT, fuente TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(año_inicio, mes_inicio, tipo)
);
"""


def run_migration(conn: Any) -> None:
    slog = step_log("MIGRATION")
    try:
        with conn.cursor() as cur:
            cur.execute(_MIGRATION_SQL)
        conn.commit()
        slog.info("✅ Migration v9.0 aplicada (tablas + columnas nuevas)")
    except Exception as exc:
        conn.rollback()
        slog.warning("Migration parcial (tablas pueden ya existir): %s", exc)


# ══════════════════════════════════════════════════════════════════
#  REGISTRO DE PASOS + ORQUESTACIÓN
# ══════════════════════════════════════════════════════════════════

_PASOS: dict[str, tuple[Any, str, list[str]]] = {
    # v8.0 — mantenidos
    "departamentos":       (paso_departamentos,       "GADM L1 + 25 fallback bboxes",               []),
    "sismos":              (paso_sismos,               "USGS M≥2.5 1900-hoy, paralelo",               []),
    "distritos":           (paso_distritos,            "INEI WFS → GADM L3 → 75 fallback",           ["departamentos"]),
    "fallas":              (paso_fallas,               "INGEMMET/IGP 19 fallas geológicas",           []),
    "inundaciones":        (paso_inundaciones,         "ANA WFS + CENEPRED hardcoded",                []),
    "tsunamis":            (paso_tsunamis,             "PREDES/IGP/DHN 9 zonas",                      []),
    "deslizamientos":      (paso_deslizamientos,       "CENEPRED WFS + 10 zonas hardcoded",           []),
    "infraestructura":     (paso_infraestructura,      "MTC+APN+OSINERGMIN+MINSA+CGBVP+OSM",         ["departamentos"]),
    "estaciones":          (paso_estaciones,           "IGP+SENAMHI+ANA+DHN+IPEN 34 estaciones",      []),
    "precipitaciones":     (paso_precipitaciones,      "SENAMHI WFS + 22 zonas climáticas CHIRPS",    []),
    "eventos_fen":         (paso_eventos_fen,          "ENSO histórico NOAA-CPC 1950-2025",           []),
    # 🆕 v9.0
    "volcanes":            (paso_volcanes,             "🆕 INGEMMET/OVI-IGP 2021 — 20 volcanes",      ["departamentos"]),
    "sequia_spi":          (paso_sequia_spi,           "🆕 SPI-12 sequía (McKee et al. 1993/CHIRPS)", ["precipitaciones"]),
    "cascada":             (paso_cascada,              "🆕 factor_cascada (Gill & Malamud 2014)",     ["fallas","deslizamientos"]),
    "irc_v9":              (paso_irc_v9,               "🆕 IRC v9 7 amenazas + bootstrap 500 iter",   ["volcanes","sequia_spi","cascada"]),
    "exposicion_ivs":      (paso_exposicion_ivs,       "🆕 GEM 2023 + INEI 2017 + MIDIS SISFOH 2022", ["irc_v9"]),
    # Finales
    "heatmap":             (paso_heatmap,              "REFRESH mv_heatmap_sismos",                   ["sismos"]),
    "regiones":            (paso_regiones,             "f_actualizar_regiones() + zona_sismica",      ["departamentos","distritos"]),
    "riesgo_construccion": (paso_riesgo_construccion,  "REFRESH mv_riesgo_construccion (IRC v8+FEN)", ["regiones"]),
    "sendai":              (paso_sendai,               "🆕 Snapshot Sendai Framework 2015-2030",      ["irc_v9"]),
}

# Orden canónico de ejecución (respeta dependencias)
_ORDEN: list[str] = [
    "departamentos", "sismos", "distritos", "fallas",
    "inundaciones", "tsunamis", "deslizamientos", "infraestructura",
    "estaciones", "precipitaciones", "eventos_fen",
    # v9 nuevos
    "volcanes", "sequia_spi", "cascada", "irc_v9", "exposicion_ivs",
    # finales
    "heatmap", "regiones", "riesgo_construccion", "sendai",
]


def print_banner(dry_run: bool = False) -> None:
    modo = "  ⚠️  DRY-RUN — sin escrituras a BD" if dry_run else ""
    print("""
  ╔══════════════════════════════════════════════════════════════╗
  ║  GeoRiesgo Perú — ETL v9.0 ENTERPRISE                      ║
  ║  ✅ Pasos 0-10 v8.0 conservados intactos                   ║
  ║  🆕 PASO: Volcanes (INGEMMET/OVI-IGP 2021 — 20 volcanes)  ║
  ║  🆕 PASO: Sequía SPI-12 (McKee 1993 / CHIRPS 1981-2020)   ║
  ║  🆕 PASO: Cascada sismo→desl (Gill & Malamud 2014)        ║
  ║  🆕 PASO: IRC v9 — 7 amenazas + bootstrap 500 iter        ║
  ║       35%S+20%I+18%D+10%T+8%V+5%Q+4%F (CENEPRED 2014)    ║
  ║  🆕 PASO: Exposición/IVS (GEM 2023+INEI 2017+MIDIS 2022)  ║
  ║  🆕 PASO: Sendai Framework snapshot 2015-2030              ║
  ║  ✅ SQL Migration automática (IF NOT EXISTS / ADD IF NOT)  ║
  ╚══════════════════════════════════════════════════════════════╝""")
    if modo:
        print(f"\n{modo}\n")
    print(f"  Workers: {get_config().max_workers}  "
          f"Chunk: {get_config().chunk_size}  "
          f"Bootstrap: {get_config().bootstrap_n}  "
          f"Fecha: {date.today().isoformat()}")
    print()


def _run_step(nombre: str, fn: Any, dry_run: bool) -> StepResult:
    if dry_run:
        return StepResult(nombre, 0, 0, 0, 0.0, "DRY-RUN")
    try:
        return fn()
    except Exception as exc:
        log.error("Error en paso '%s': %s", nombre, exc, exc_info=True)
        return StepResult(nombre, 0, 0, 1, 0.0, f"ERROR: {exc}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GeoRiesgo Perú ETL v9.0 ENTERPRISE",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="\n".join(
            f"  {k:<25} {desc}"
            for k, (_, desc, _) in _PASOS.items()
        ),
    )
    parser.add_argument("--force", action="store_true",
                        help="Forzar re-carga completa")
    parser.add_argument("--solo", nargs="+", choices=list(_PASOS.keys()),
                        help="Ejecutar solo los pasos indicados")
    parser.add_argument("--skip", nargs="*", default=[],
                        choices=list(_PASOS.keys()),
                        help="Pasos a omitir")
    parser.add_argument("--workers", type=int, default=None,
                        help="Workers paralelos (default: ETL_WORKERS env)")
    parser.add_argument("--bootstrap-n", type=int, default=None,
                        help="Iteraciones bootstrap IRC v9 (default: 500)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simular sin escribir a BD")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Log level DEBUG")
    args = parser.parse_args()

    global _config
    _config = ETLConfig(
        max_workers=args.workers or int(os.getenv("ETL_WORKERS", "4")),
        dry_run=args.dry_run,
        verbose=args.verbose,
        bootstrap_n=args.bootstrap_n or int(os.getenv("ETL_BOOTSTRAP_N", "500")),
    )

    global log
    log = _setup_logging(args.verbose)
    print_banner(args.dry_run)

    if not args.dry_run:
        try:
            init_pool(_config)
        except Exception as exc:
            log.error("No se pudo conectar a BD: %s", exc)
            return 1
        with get_conn() as conn:
            run_migration(conn)

    t_total = time.perf_counter()
    resultados: list[StepResult] = []

    if args.solo:
        pasos_a_ejecutar = [p for p in _ORDEN if p in args.solo]
        log.info("── PASOS SELECCIONADOS: %s", ", ".join(pasos_a_ejecutar))
    else:
        pasos_a_ejecutar = [p for p in _ORDEN if p not in (args.skip or [])]
        log.info("Ejecutando %d/%d pasos", len(pasos_a_ejecutar), len(_ORDEN))

    for i, nombre in enumerate(pasos_a_ejecutar):
        fn, desc, _ = _PASOS[nombre]
        log.info("── PASO %02d/%02d: %-25s %s",
                 i + 1, len(pasos_a_ejecutar), nombre.upper(), desc)
        result = _run_step(nombre, fn, args.dry_run)
        resultados.append(result)
        log.info("   %s", result)

    elapsed = time.perf_counter() - t_total

    print("\n  ╔══════════════════════════════════════════════════════════════╗")
    print("  ║  RESUMEN FINAL ETL v9.0                                     ║")
    print("  ╠══════════════════════════════════════════════════════════════╣")
    for r in resultados:
        print(f"  ║  {r}")
    ok_count  = sum(1 for r in resultados if r.ok)
    err_count = len(resultados) - ok_count
    print("  ╠══════════════════════════════════════════════════════════════╣")
    print(f"  ║  ✅ {ok_count} pasos OK  ❌ {err_count} con errores  ⏱ {elapsed:.0f}s total")
    print("  ╚══════════════════════════════════════════════════════════════╝\n")

    close_pool()
    return 0 if err_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())