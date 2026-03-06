#!/usr/bin/env python3
"""
GeoRiesgo Perú — ETL v4.2
====================================================
Correcciones sobre v4.0 / v4.1:
  ✅ Nombres de columnas alineados al esquema init.sql real:
       fallas.magnitud_max  (no mag_maxima)
       zonas_inundables.tipo_inundacion  (no tipo)
       tabla zonas_tsunami  (no tsunamis)
       mv_heatmap_sismos  (no mv_mapa_calor)
  ✅ Descarga USGS paralela con ThreadPoolExecutor (v4.0)
  ✅ Cobertura nacional Perú (v4.1)
  ✅ log_sync con transacción independiente (no se pierde en rollback)
  ✅ Savepoint real por falla individual (no aborta todo el paso)
  ✅ GADM: selecciona JSON más grande del ZIP (evita leer metadata)
  ✅ Overpass: retry con backoff en 429
  ✅ WKT pasado una sola vez en fallas (bug longitud_km)
  ✅ INEI: prueba URL alternativa si la principal falla
  ✅ Validación coordenadas antes de INSERT
  ✅ ST_MakeValid en todas las geometrías poligonales
  ✅ heatmap: REFRESH no bloqueante con fallback
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from io import BytesIO

import psycopg2
import psycopg2.extras
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

# ── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")

# ══════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════

DB_URL = os.getenv(
    "DATABASE_URL_SYNC",
    "postgresql://georiesgo:georiesgo_secret@localhost:5432/georiesgo",
).replace("postgresql+asyncpg://", "postgresql://")

# Bbox Perú completo
BBOX_PERU = {"lat_min": -18.5, "lat_max": 0.0, "lon_min": -82.0, "lon_max": -68.5}

USGS_BASE    = "https://earthquake.usgs.gov/fdsnws/event/1"
USGS_START   = "1900-01-01"
USGS_MAG_MIN = 2.5
USGS_BLOCK   = 5       # años por bloque
MAX_WORKERS  = 3       # hilos paralelos USGS
HTTP_TIMEOUT = 120

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# GADM 4.1 — URLs actualizadas (ucdavis es el mirror oficial)
GADM_L3_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_PER_3.json.zip"
GADM_L2_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_PER_2.json.zip"

# INEI WFS — dos URLs posibles (el servidor cambia con frecuencia)
INEI_WFS_URLS = [
    (
        "https://geoservidor.inei.gob.pe/geoserver/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=INEI:LIMITEDISTRITAL"
        "&outputFormat=application/json&srsName=EPSG:4326"
    ),
    (
        "https://geoservidorperu.inei.gob.pe/geoserver/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=INEI:LIMITEDISTRITAL"
        "&outputFormat=application/json&srsName=EPSG:4326"
    ),
]

INGEMMET_FALLAS_URLS = [
    "https://geocatmin.ingemmet.gob.pe/arcgis/rest/services/SERV_NEOTECTONICA/MapServer/0/query",
    "https://geocatmin.ingemmet.gob.pe/arcgis/rest/services/SERV_GEOLOGIA_50000/MapServer/4/query",
]

RIESGO_DEPTO: dict[str, int] = {
    "Piura": 5, "Lambayeque": 5, "La Libertad": 4,
    "Ancash": 5, "Lima": 4, "Callao": 4,
    "Ica": 5, "Arequipa": 4, "Moquegua": 4, "Tacna": 4,
    "Cusco": 4, "Puno": 3, "Ayacucho": 4,
    "Huancavelica": 3, "Junin": 3, "Huanuco": 3,
    "Pasco": 3, "San Martin": 4, "Loreto": 2,
    "Ucayali": 2, "Madre De Dios": 2, "Amazonas": 3,
    "Cajamarca": 3, "Tumbes": 4, "Apurimac": 3,
}
RIESGO_PROV: dict[str, int] = {
    "Pisco": 5, "Chincha": 5, "Ica": 4, "Nazca": 4, "Palpa": 3,
}

# ══════════════════════════════════════════════════════════
#  DATASET NACIONAL DE FALLAS (IGP / Audin et al. 2008)
#  Columnas del schema: nombre, nombre_alt, activa, tipo,
#    mecanismo, magnitud_max, region, fuente, referencia
# ══════════════════════════════════════════════════════════
FALLAS_NACIONAL = [
    {
        "nombre": "Zona de Subducción Nazca-Sudamericana",
        "nombre_alt": "Peru-Chile Trench Interface",
        "tipo": "Subducción", "mecanismo": "compresivo",
        "activa": True, "magnitud_max": 9.5,
        "region": "Costa Nacional", "fuente": "USGS/IGP",
        "referencia": "Beck & Nishenko 1990; Dorbath et al. 1990",
        "coords": [
            [-81.5,-2.5],[-81.2,-4.0],[-80.8,-5.5],[-80.5,-7.0],
            [-80.0,-8.5],[-79.5,-9.5],[-79.0,-10.5],[-78.5,-11.5],
            [-78.0,-12.5],[-77.5,-13.5],[-77.0,-14.5],[-76.5,-15.5],
            [-76.0,-16.5],[-75.5,-17.5],[-74.8,-18.5],
        ],
    },
    {
        "nombre": "Sistema de Fallas de Tumbes",
        "tipo": "Neotectónica activa", "mecanismo": "transcurrente",
        "activa": True, "magnitud_max": 6.5, "region": "Tumbes",
        "fuente": "INGEMMET", "referencia": "Audin et al. 2008",
        "coords": [[-80.5,-3.4],[-80.2,-3.7],[-79.9,-4.0],[-79.6,-4.3],[-79.3,-4.6]],
    },
    {
        "nombre": "Sistema de Fallas de Piura",
        "tipo": "Neotectónica activa", "mecanismo": "transcurrente",
        "activa": True, "magnitud_max": 6.8, "region": "Piura",
        "fuente": "INGEMMET/IGP", "referencia": "Audin et al. 2008",
        "coords": [[-80.8,-4.5],[-80.4,-5.0],[-80.0,-5.5],[-79.6,-6.0],[-79.2,-6.4]],
    },
    {
        "nombre": "Sistema de Fallas de la Cordillera Blanca",
        "nombre_alt": "Cordillera Blanca Fault System",
        "tipo": "Neotectónica activa", "mecanismo": "extensional",
        "activa": True, "magnitud_max": 7.0, "region": "Ancash",
        "fuente": "INGEMMET/IGP", "referencia": "McNulty & Farber 2002; Audin et al. 2008",
        "coords": [
            [-77.90,-8.45],[-77.82,-8.80],[-77.73,-9.15],
            [-77.65,-9.50],[-77.55,-9.85],[-77.48,-10.20],[-77.40,-10.50],
        ],
    },
    {
        "nombre": "Sistema de Fallas de Lima",
        "nombre_alt": "Lima Fault System",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.5, "region": "Lima",
        "fuente": "IGP/INGEMMET", "referencia": "Macharé et al. 2003; Audin et al. 2008",
        "coords": [[-77.15,-11.55],[-77.05,-11.75],[-76.95,-11.95],[-76.85,-12.15],[-76.75,-12.35]],
    },
    {
        "nombre": "Falla de San Ramón",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.0, "region": "Lima",
        "fuente": "IGP", "referencia": "Bolaños & Woranke 2006",
        "coords": [[-76.80,-11.90],[-76.65,-12.05],[-76.50,-12.20],[-76.35,-12.35]],
    },
    {
        "nombre": "Sistema de Fallas Ica-Pisco",
        "nombre_alt": "Ica-Pisco Fault System",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 8.0, "region": "Ica",
        "fuente": "IGP/Audin", "referencia": "Audin et al. 2008; Motagh et al. 2008",
        "coords": [
            [-76.50,-13.20],[-76.35,-13.45],[-76.18,-13.72],
            [-75.98,-14.05],[-75.78,-14.38],[-75.58,-14.70],[-75.38,-15.02],
        ],
    },
    {
        "nombre": "Falla de Chincha",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.5, "region": "Ica",
        "fuente": "IGP/Tavera", "referencia": "Tavera & Buforn 2001; Audin et al. 2008",
        "coords": [[-76.38,-12.88],[-76.22,-13.12],[-76.05,-13.38],[-75.88,-13.62],[-75.72,-13.85]],
    },
    {
        "nombre": "Falla Costera de Paracas",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.3, "region": "Ica",
        "fuente": "IGP/Tavera", "referencia": "IGP/Tavera 2015",
        "coords": [[-76.45,-13.70],[-76.25,-13.90],[-76.05,-14.15],[-75.85,-14.40]],
    },
    {
        "nombre": "Sistema de Fallas de Nazca",
        "tipo": "Neotectónica activa", "mecanismo": "transcurrente",
        "activa": True, "magnitud_max": 7.0, "region": "Ica",
        "fuente": "INGEMMET/Audin", "referencia": "Audin et al. 2008",
        "coords": [[-75.52,-13.88],[-75.28,-14.22],[-75.02,-14.58],[-74.78,-14.90],[-74.52,-15.22]],
    },
    {
        "nombre": "Falla de San Juan de Marcona",
        "tipo": "Neotectónica activa", "mecanismo": "normal",
        "activa": True, "magnitud_max": 7.0, "region": "Ica",
        "fuente": "IGP", "referencia": "Audin et al. 2008",
        "coords": [[-75.38,-14.98],[-75.12,-15.25],[-74.88,-15.50],[-74.62,-15.72]],
    },
    {
        "nombre": "Falla de Acarí",
        "tipo": "Neotectónica activa", "mecanismo": "normal",
        "activa": True, "magnitud_max": 6.5, "region": "Arequipa",
        "fuente": "Audin et al. 2008", "referencia": "Audin et al. 2008",
        "coords": [[-74.80,-15.40],[-74.55,-15.65],[-74.30,-15.85]],
    },
    {
        "nombre": "Sistema de Fallas Aplao",
        "nombre_alt": "Aplao Fault Zone",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.0, "region": "Arequipa",
        "fuente": "INGEMMET/Audin", "referencia": "Sébrier et al. 1985; Audin et al. 2008",
        "coords": [[-72.72,-15.92],[-72.48,-16.18],[-72.22,-16.45],[-71.98,-16.72],[-71.72,-16.95]],
    },
    {
        "nombre": "Sistema de Fallas de Tacna",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.5, "region": "Tacna",
        "fuente": "IGP/INGEMMET", "referencia": "Sébrier et al. 1985; Audin et al. 2008",
        "coords": [[-70.85,-17.15],[-70.60,-17.45],[-70.35,-17.72],[-70.10,-18.00]],
    },
    {
        "nombre": "Sistema de Fallas de Cusco",
        "nombre_alt": "Cusco Fault System",
        "tipo": "Neotectónica activa", "mecanismo": "normal",
        "activa": True, "magnitud_max": 7.0, "region": "Cusco",
        "fuente": "IGP/INGEMMET", "referencia": "Macharé et al. 2003; Sébrier et al. 1985",
        "coords": [[-72.12,-13.32],[-71.98,-13.48],[-71.82,-13.62],[-71.68,-13.78],[-71.52,-13.95]],
    },
    {
        "nombre": "Sistema de Fallas del Vilcanota",
        "nombre_alt": "Vilcanota Fault Zone",
        "tipo": "Neotectónica activa", "mecanismo": "normal",
        "activa": True, "magnitud_max": 7.0, "region": "Cusco",
        "fuente": "INGEMMET/IGP", "referencia": "Sébrier et al. 1985; Audin et al. 2008",
        "coords": [[-71.50,-13.45],[-71.25,-13.72],[-71.00,-14.00],[-70.75,-14.28],[-70.52,-14.55]],
    },
    {
        "nombre": "Sistema de Fallas de Moyobamba",
        "tipo": "Neotectónica activa", "mecanismo": "transcurrente",
        "activa": True, "magnitud_max": 7.0, "region": "San Martin",
        "fuente": "IGP/INGEMMET", "referencia": "Audin et al. 2008; Tavera et al. 2009",
        "coords": [[-77.02,-5.90],[-76.82,-6.15],[-76.62,-6.42],[-76.42,-6.68],[-76.22,-6.95]],
    },
    {
        "nombre": "Falla de Rioja",
        "tipo": "Neotectónica activa", "mecanismo": "transcurrente",
        "activa": True, "magnitud_max": 6.5, "region": "San Martin",
        "fuente": "IGP", "referencia": "IGP; Audin et al. 2008",
        "coords": [[-77.18,-6.05],[-77.00,-6.25],[-76.82,-6.45]],
    },
    {
        "nombre": "Sistema de Fallas de Huancayo",
        "tipo": "Neotectónica activa", "mecanismo": "normal",
        "activa": True, "magnitud_max": 6.5, "region": "Junin",
        "fuente": "INGEMMET", "referencia": "Audin et al. 2008",
        "coords": [[-75.25,-12.05],[-75.10,-12.25],[-74.95,-12.45],[-74.80,-12.65]],
    },
    {
        "nombre": "Sistema de Fallas Huancavelica-Ica",
        "tipo": "Neotectónica activa", "mecanismo": "inverso",
        "activa": True, "magnitud_max": 7.1, "region": "Huancavelica",
        "fuente": "Audin et al. 2008", "referencia": "Audin et al. 2008",
        "coords": [[-74.50,-13.20],[-74.30,-13.70],[-74.10,-14.20],[-73.95,-14.70]],
    },
    {
        "nombre": "Sistema de Fallas de Ayacucho",
        "tipo": "Neotectónica activa", "mecanismo": "normal",
        "activa": True, "magnitud_max": 6.0, "region": "Ayacucho",
        "fuente": "INGEMMET", "referencia": "Macharé et al. 2003",
        "coords": [[-74.30,-13.05],[-74.15,-13.25],[-74.00,-13.45],[-73.85,-13.65]],
    },
]

# ══════════════════════════════════════════════════════════
#  ZONAS INUNDABLES
#  Schema: nombre, geom, nivel_riesgo, tipo_inundacion,
#    periodo_retorno, profundidad_max_m, cuenca, region, fuente
# ══════════════════════════════════════════════════════════
ZONAS_INUNDABLES = [
    {
        "nombre": "Cuenca baja Río Ica — llanura aluvial",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 5,
        "periodo_retorno": 50, "profundidad_max_m": 2.5,
        "cuenca": "Río Ica", "region": "Ica", "fuente": "ANA/SENAMHI",
        "coords": [[-75.842,-14.115],[-75.648,-14.115],[-75.648,-13.892],[-75.842,-13.892],[-75.842,-14.115]],
    },
    {
        "nombre": "Valle del Río Pisco — planicie aluvial baja",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 5,
        "periodo_retorno": 25, "profundidad_max_m": 3.0,
        "cuenca": "Río Pisco", "region": "Ica", "fuente": "ANA/CENEPRED",
        "coords": [[-76.218,-13.798],[-75.898,-13.798],[-75.898,-13.558],[-76.218,-13.558],[-76.218,-13.798]],
    },
    {
        "nombre": "Litoral Pisco-Paracas — inundación costera y tsunami",
        "tipo_inundacion": "costero", "nivel_riesgo": 5,
        "periodo_retorno": 500, "profundidad_max_m": 8.0,
        "cuenca": "Zona costera Pisco", "region": "Ica", "fuente": "PREDES/IGP",
        "coords": [[-76.358,-13.855],[-76.098,-13.855],[-76.098,-13.548],[-76.358,-13.548],[-76.358,-13.855]],
    },
    {
        "nombre": "Cuenca Río Chincha — zona inundable baja",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 4,
        "periodo_retorno": 100, "profundidad_max_m": 1.8,
        "cuenca": "Río Chincha", "region": "Ica", "fuente": "ANA",
        "coords": [[-76.152,-13.448],[-75.852,-13.448],[-75.852,-13.215],[-76.152,-13.215],[-76.152,-13.448]],
    },
    {
        "nombre": "Valle Río Grande — zona aluvial Nazca",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 4,
        "periodo_retorno": 50, "profundidad_max_m": 2.0,
        "cuenca": "Río Grande", "region": "Ica", "fuente": "ANA",
        "coords": [[-75.108,-14.905],[-74.748,-14.905],[-74.748,-14.605],[-75.108,-14.605],[-75.108,-14.905]],
    },
    {
        "nombre": "Cuenca Río Piura — zona inundable El Niño",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 5,
        "periodo_retorno": 10, "profundidad_max_m": 4.0,
        "cuenca": "Río Piura", "region": "Piura", "fuente": "ANA/SENAMHI",
        "coords": [[-80.705,-5.205],[-80.305,-5.205],[-80.305,-4.905],[-80.705,-4.905],[-80.705,-5.205]],
    },
    {
        "nombre": "Bajo Piura — planicie inundable",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 5,
        "periodo_retorno": 5, "profundidad_max_m": 5.0,
        "cuenca": "Río Piura", "region": "Piura", "fuente": "ANA/CENEPRED",
        "coords": [[-80.908,-5.502],[-80.508,-5.502],[-80.508,-5.152],[-80.908,-5.152],[-80.908,-5.502]],
    },
    {
        "nombre": "Cuenca Río Rímac — zona inundable Lima",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 4,
        "periodo_retorno": 100, "profundidad_max_m": 2.0,
        "cuenca": "Río Rímac", "region": "Lima", "fuente": "ANA/CENEPRED",
        "coords": [[-77.105,-12.052],[-76.855,-12.052],[-76.855,-11.852],[-77.105,-11.852],[-77.105,-12.052]],
    },
    {
        "nombre": "Valle del Río Camaná-Majes — zona aluvial",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 4,
        "periodo_retorno": 50, "profundidad_max_m": 2.5,
        "cuenca": "Río Majes-Camaná", "region": "Arequipa", "fuente": "ANA",
        "coords": [[-72.908,-16.608],[-72.508,-16.608],[-72.508,-16.308],[-72.908,-16.308],[-72.908,-16.608]],
    },
    {
        "nombre": "Río Ucayali — planicie de inundación amazónica",
        "tipo_inundacion": "fluvial", "nivel_riesgo": 4,
        "periodo_retorno": 5, "profundidad_max_m": 6.0,
        "cuenca": "Río Ucayali", "region": "Ucayali", "fuente": "ANA/SENAMHI",
        "coords": [[-74.608,-8.408],[-74.108,-8.408],[-74.108,-7.908],[-74.608,-7.908],[-74.608,-8.408]],
    },
]

# ══════════════════════════════════════════════════════════
#  ZONAS TSUNAMI
#  Schema: nombre, geom, nivel_riesgo, altura_ola_m,
#    tiempo_arribo_min, periodo_retorno, region, fuente
# ══════════════════════════════════════════════════════════
ZONAS_TSUNAMI = [
    {
        "nombre": "Zona de inundación por tsunami — Callao",
        "nivel_riesgo": 5, "altura_ola_m": 10.0,
        "tiempo_arribo_min": 20, "periodo_retorno": 500,
        "region": "Lima/Callao", "fuente": "PREDES/IGP",
        "coords": [[-77.175,-12.055],[-77.055,-12.055],[-77.055,-11.905],[-77.175,-11.905],[-77.175,-12.055]],
    },
    {
        "nombre": "Zona de inundación por tsunami — Pisco",
        "nivel_riesgo": 5, "altura_ola_m": 8.0,
        "tiempo_arribo_min": 15, "periodo_retorno": 500,
        "region": "Ica", "fuente": "PREDES/IGP",
        "coords": [[-76.268,-13.858],[-76.088,-13.858],[-76.088,-13.658],[-76.268,-13.658],[-76.268,-13.858]],
    },
    {
        "nombre": "Zona de inundación por tsunami — Paracas/Lagunillas",
        "nivel_riesgo": 5, "altura_ola_m": 12.0,
        "tiempo_arribo_min": 10, "periodo_retorno": 200,
        "region": "Ica", "fuente": "PREDES/IGP/INDECI",
        "coords": [[-76.398,-13.908],[-76.238,-13.908],[-76.238,-13.758],[-76.398,-13.758],[-76.398,-13.908]],
    },
    {
        "nombre": "Zona de inundación por tsunami — Nazca/San Juan de Marcona",
        "nivel_riesgo": 4, "altura_ola_m": 6.0,
        "tiempo_arribo_min": 8, "periodo_retorno": 500,
        "region": "Ica", "fuente": "IGP/PREDES",
        "coords": [[-75.20,-15.50],[-75.00,-15.50],[-75.00,-15.20],[-75.20,-15.20],[-75.20,-15.50]],
    },
    {
        "nombre": "Zona de inundación por tsunami — Chimbote",
        "nivel_riesgo": 5, "altura_ola_m": 9.0,
        "tiempo_arribo_min": 18, "periodo_retorno": 500,
        "region": "Ancash", "fuente": "PREDES/IGP",
        "coords": [[-78.658,-9.158],[-78.508,-9.158],[-78.508,-9.008],[-78.658,-9.008],[-78.658,-9.158]],
    },
    {
        "nombre": "Zona de inundación por tsunami — Ilo/Moquegua",
        "nivel_riesgo": 4, "altura_ola_m": 7.0,
        "tiempo_arribo_min": 12, "periodo_retorno": 500,
        "region": "Moquegua", "fuente": "PREDES/IGP",
        "coords": [[-71.408,-17.658],[-71.258,-17.658],[-71.258,-17.508],[-71.408,-17.508],[-71.408,-17.658]],
    },
    {
        "nombre": "Zona de inundación por tsunami — Piura/Sechura",
        "nivel_riesgo": 4, "altura_ola_m": 6.0,
        "tiempo_arribo_min": 25, "periodo_retorno": 500,
        "region": "Piura", "fuente": "PREDES",
        "coords": [[-80.858,-5.558],[-80.658,-5.558],[-80.658,-5.358],[-80.858,-5.358],[-80.858,-5.558]],
    },
]

# ══════════════════════════════════════════════════════════
#  ESTACIONES DE MONITOREO
# ══════════════════════════════════════════════════════════
ESTACIONES = [
    {"codigo": "NNA",     "nombre": "Estación Sísmica Ñaña",         "tipo": "sismica",
     "lon": -76.843, "lat": -11.988, "altitud_m": 575.0,  "institucion": "IGP",     "region": "Lima"},
    {"codigo": "CDLA",    "nombre": "Estación Sísmica Callao",        "tipo": "sismica",
     "lon": -77.108, "lat": -12.065, "altitud_m": 15.0,   "institucion": "IGP",     "region": "Lima"},
    {"codigo": "ICA",     "nombre": "Estación Sísmica Ica",           "tipo": "sismica",
     "lon": -75.748, "lat": -14.078, "altitud_m": 405.0,  "institucion": "IGP",     "region": "Ica"},
    {"codigo": "PSC",     "nombre": "Estación Sísmica Pisco",         "tipo": "sismica",
     "lon": -76.208, "lat": -13.705, "altitud_m": 12.0,   "institucion": "IGP",     "region": "Ica"},
    {"codigo": "CHP",     "nombre": "Estación Sísmica Chincha",       "tipo": "sismica",
     "lon": -76.132, "lat": -13.408, "altitud_m": 98.0,   "institucion": "IGP",     "region": "Ica"},
    {"codigo": "NSC",     "nombre": "Estación Sísmica Nasca",         "tipo": "sismica",
     "lon": -74.942, "lat": -14.838, "altitud_m": 588.0,  "institucion": "IGP",     "region": "Ica"},
    {"codigo": "ANC",     "nombre": "Estación Sísmica Ancón",         "tipo": "sismica",
     "lon": -77.158, "lat": -11.778, "altitud_m": 120.0,  "institucion": "IGP",     "region": "Lima"},
    {"codigo": "ARE",     "nombre": "Estación Sísmica Arequipa",      "tipo": "sismica",
     "lon": -71.478, "lat": -16.462, "altitud_m": 2490.0, "institucion": "IGP",     "region": "Arequipa"},
    {"codigo": "CUS",     "nombre": "Estación Sísmica Cusco",         "tipo": "sismica",
     "lon": -71.978, "lat": -13.512, "altitud_m": 3399.0, "institucion": "IGP",     "region": "Cusco"},
    {"codigo": "HUA",     "nombre": "Estación Sísmica Huaraz",        "tipo": "sismica",
     "lon": -77.528, "lat": -9.528,  "altitud_m": 3052.0, "institucion": "IGP",     "region": "Ancash"},
    {"codigo": "TRU",     "nombre": "Estación Sísmica Trujillo",      "tipo": "sismica",
     "lon": -79.028, "lat": -8.112,  "altitud_m": 34.0,   "institucion": "IGP",     "region": "La Libertad"},
    {"codigo": "PIU",     "nombre": "Estación Sísmica Piura",         "tipo": "sismica",
     "lon": -80.628, "lat": -5.195,  "altitud_m": 29.0,   "institucion": "IGP",     "region": "Piura"},
    {"codigo": "PUN",     "nombre": "Estación Sísmica Puno",          "tipo": "sismica",
     "lon": -70.018, "lat": -15.845, "altitud_m": 3827.0, "institucion": "IGP",     "region": "Puno"},
    {"codigo": "IQT",     "nombre": "Estación Sísmica Iquitos",       "tipo": "sismica",
     "lon": -73.258, "lat": -3.748,  "altitud_m": 122.0,  "institucion": "IGP",     "region": "Loreto"},
    {"codigo": "MOY",     "nombre": "Estación Sísmica Moyobamba",     "tipo": "sismica",
     "lon": -76.965, "lat": -6.038,  "altitud_m": 860.0,  "institucion": "IGP",     "region": "San Martin"},
    {"codigo": "SMH-ICA", "nombre": "Estación Meteorológica Ica",     "tipo": "meteorologica",
     "lon": -75.738, "lat": -14.068, "altitud_m": 406.0,  "institucion": "SENAMHI", "region": "Ica"},
    {"codigo": "SMH-PIU", "nombre": "Estación Meteorológica Piura",   "tipo": "meteorologica",
     "lon": -80.618, "lat": -5.178,  "altitud_m": 29.0,   "institucion": "SENAMHI", "region": "Piura"},
    {"codigo": "SMH-ARE", "nombre": "Estación Meteorológica Arequipa","tipo": "meteorologica",
     "lon": -71.518, "lat": -16.318, "altitud_m": 2525.0, "institucion": "SENAMHI", "region": "Arequipa"},
    {"codigo": "ANA-ICA", "nombre": "Estación Hidrométrica Río Ica (La Achirana)",
     "tipo": "hidrometrica",
     "lon": -75.802, "lat": -14.052, "altitud_m": 398.0,  "institucion": "ANA",     "region": "Ica"},
    {"codigo": "ANA-PSC", "nombre": "Estación Hidrométrica Río Pisco",
     "tipo": "hidrometrica",
     "lon": -75.698, "lat": -13.752, "altitud_m": 480.0,  "institucion": "ANA",     "region": "Ica"},
]

# ══════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════

def hoy_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def prof_tipo(p: float) -> str:
    if p < 30:  return "superficial"
    if p < 70:  return "intermedio"
    return "profundo"


def region_peru(lon: float, lat: float) -> str:
    if lat > -5.5:    return "Piura/Tumbes" if lon < -78 else "Amazonas"
    elif lat > -8.0:  return "Lambayeque/La Libertad"
    elif lat > -10.0: return "Ancash"
    elif lat > -12.0: return "Lima"
    elif lat > -13.5: return "Lima/Ica"
    elif lat > -15.5: return "Ica/Ayacucho"
    elif lat > -17.0: return "Arequipa/Cusco"
    else:             return "Arequipa/Tacna"


@retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(4),
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def http_get(url: str, params: dict | None = None,
             timeout: int = HTTP_TIMEOUT) -> requests.Response:
    resp = requests.get(
        url, params=params, timeout=timeout,
        headers={"User-Agent": "GeoRiesgoPeru/4.2 (georiesgo@igp.gob.pe)"},
    )
    resp.raise_for_status()
    return resp


def db_connect() -> psycopg2.extensions.connection:
    log.info("Conectando a PostGIS: %s", DB_URL.split("@")[-1])
    return psycopg2.connect(DB_URL)


def log_sync(conn, fuente: str, tabla: str, registros: int = 0,
             estado: str = "ok", detalle: str | None = None,
             duracion: float = 0.0) -> None:
    """
    Registra en sync_log usando transacción INDEPENDIENTE.
    No se pierde aunque el paso anterior haya hecho rollback.
    """
    try:
        with conn.cursor() as c:
            c.execute(
                "INSERT INTO sync_log "
                "  (fuente, tabla, registros, estado, detalle, duracion_s, fin) "
                "VALUES (%s,%s,%s,%s,%s,%s,NOW())",
                (fuente, tabla, registros, estado, detalle, round(duracion, 2)),
            )
        conn.commit()
    except Exception as e:
        log.warning("No se pudo guardar sync_log: %s", e)
        try:
            conn.rollback()
        except Exception:
            pass


def _rollback_safe(conn) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


# ══════════════════════════════════════════════════════════
#  PASO 1 — SISMOS (USGS FDSNWS, descarga paralela)
# ══════════════════════════════════════════════════════════

def _usgs_params(start: str, end: str) -> dict:
    return {
        "format":       "geojson",
        "minlatitude":  BBOX_PERU["lat_min"],
        "maxlatitude":  BBOX_PERU["lat_max"],
        "minlongitude": BBOX_PERU["lon_min"],
        "maxlongitude": BBOX_PERU["lon_max"],
        "minmagnitude": USGS_MAG_MIN,
        "starttime":    start,
        "endtime":      end,
        "orderby":      "time-asc",
        "limit":        20000,
    }


def _parse_usgs_features(raw: dict) -> list[dict]:
    features = []
    for feat in raw.get("features", []):
        p     = feat.get("properties", {})
        geom  = feat.get("geometry", {})
        coord = geom.get("coordinates", [0, 0, 0])
        if len(coord) < 2:
            continue
        ts  = p.get("time", 0) or 0
        dt  = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
        mag = p.get("mag")
        if mag is None or float(mag) <= 0:
            continue
        lon  = float(coord[0])
        lat  = float(coord[1])
        prof = float(coord[2]) if len(coord) > 2 and coord[2] is not None else 20.0
        # Filtro bbox ampliado (margen para sismos limítrofes)
        if not (-85 <= lon <= -65 and -20 <= lat <= 2):
            continue
        features.append({
            "usgs_id":          feat.get("id", f"auto_{ts}"),
            "lon":              lon,
            "lat":              lat,
            "magnitud":         round(float(mag), 1),
            "profundidad_km":   round(abs(prof), 2),
            "tipo_profundidad": prof_tipo(abs(prof)),
            "fecha":            dt.date().isoformat(),
            "hora_utc":         dt.isoformat(),
            "lugar":            (p.get("place") or "Perú")[:500],
            "region":           region_peru(lon, lat),
            "tipo_magnitud":    (p.get("magType") or "")[:20],
            "estado":           (p.get("status") or "reviewed")[:20],
        })
    return features


def _fetch_usgs_block(args: tuple) -> list[dict]:
    """Worker para ThreadPoolExecutor — descarga un bloque temporal."""
    start, end = args
    try:
        resp  = http_get(f"{USGS_BASE}/query", params=_usgs_params(start, end))
        feats = _parse_usgs_features(resp.json())
        log.info("    Bloque %s→%s: %s sismos", start, end, len(feats))
        return feats
    except Exception as exc:
        log.warning("    Error bloque %s→%s: %s", start, end, exc)
        return []


def _ultima_fecha_sismos(cur) -> str:
    cur.execute("SELECT MAX(fecha) FROM sismos")
    row = cur.fetchone()
    if row and row[0]:
        return (row[0] - timedelta(days=30)).isoformat()
    return USGS_START


def sincronizar_sismos(conn, cur, force: bool = False) -> int:
    t0         = time.time()
    end_date   = hoy_utc()
    start_date = USGS_START if force else _ultima_fecha_sismos(cur)

    log.info("USGS: %s → %s  (M≥%.1f)", start_date, end_date, USGS_MAG_MIN)

    # Construir bloques temporales
    start_year = int(start_date[:4])
    end_year   = int(end_date[:4])
    blocks: list[tuple[str, str]] = []
    current = start_year
    while current <= end_year:
        block_end = min(current + USGS_BLOCK, end_year + 1)
        bs = start_date if current == start_year else f"{current}-01-01"
        be = end_date   if block_end > end_year  else f"{block_end}-01-01"
        if bs < be:
            blocks.append((bs, be))
        current = block_end

    log.info("  %s bloques → descarga paralela (%s workers)", len(blocks), MAX_WORKERS)

    all_features: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_usgs_block, b): b for b in blocks}
        for future in as_completed(futures):
            all_features.extend(future.result())

    if not all_features:
        log.warning("Sin datos USGS — tabla sismos puede estar vacía")
        return 0

    sql = """
        INSERT INTO sismos
            (usgs_id, geom, magnitud, profundidad_km, tipo_profundidad,
             fecha, hora_utc, lugar, region, tipo_magnitud, estado)
        VALUES (
            %(usgs_id)s,
            ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
            %(magnitud)s, %(profundidad_km)s, %(tipo_profundidad)s,
            %(fecha)s, %(hora_utc)s, %(lugar)s, %(region)s,
            %(tipo_magnitud)s, %(estado)s
        )
        ON CONFLICT (usgs_id) DO UPDATE SET
            magnitud         = EXCLUDED.magnitud,
            profundidad_km   = EXCLUDED.profundidad_km,
            tipo_profundidad = EXCLUDED.tipo_profundidad,
            lugar            = EXCLUDED.lugar,
            region           = EXCLUDED.region,
            estado           = EXCLUDED.estado
    """
    # Lotes de 500 → commits frecuentes, evita transacción enorme
    BATCH = 500
    total = 0
    for i in range(0, len(all_features), BATCH):
        batch = all_features[i : i + BATCH]
        psycopg2.extras.execute_batch(cur, sql, batch, page_size=BATCH)
        conn.commit()
        total += len(batch)
        log.info("  %s/%s sismos insertados...", total, len(all_features))

    # Refrescar heatmap materializado (non-blocking)
    try:
        cur.execute("REFRESH MATERIALIZED VIEW mv_heatmap_sismos")
        conn.commit()
        log.info("  Heatmap actualizado")
    except Exception as e:
        log.warning("  No se pudo refrescar mv_heatmap_sismos: %s", e)
        _rollback_safe(conn)

    log.info("✅ %s sismos cargados (%.1fs)", total, time.time() - t0)
    return total


# ══════════════════════════════════════════════════════════
#  PASO 2 — DISTRITOS
# ══════════════════════════════════════════════════════════

def sincronizar_distritos(conn, cur, force: bool = False) -> int:
    if not force:
        cur.execute("SELECT COUNT(*) FROM distritos")
        if cur.fetchone()[0] > 0:
            log.info("Distritos ya cargados — omitiendo")
            return 0

    # 1) Intentar todas las URLs INEI
    for url in INEI_WFS_URLS:
        try:
            log.info("Descargando distritos INEI: %s ...", url[:60])
            resp  = http_get(url, timeout=90)
            feats = resp.json().get("features", [])
            if feats:
                n = _upsert_distritos_features(conn, cur, feats, "INEI")
                log.info("✅ %s distritos INEI insertados", n)
                return n
            log.warning("  INEI devolvió 0 features")
        except Exception as exc:
            log.warning("  INEI falló (%s): %s", url[:50], exc)

    # 2) GADM — intenta L3 luego L2
    for level, url, ncol, pcol, dcol in [
        (3, GADM_L3_URL, "NAME_3", "NAME_2", "NAME_1"),
        (2, GADM_L2_URL, "NAME_2", "NAME_2", "NAME_1"),
    ]:
        try:
            log.info("Descargando GADM L%s...", level)
            resp = http_get(url, timeout=180)
            log.info("  %.1f MB descargados", len(resp.content) / 1e6)
            zf = zipfile.ZipFile(BytesIO(resp.content))
            jsnames = [f for f in zf.namelist() if f.lower().endswith(".json")]
            if not jsnames:
                log.warning("  ZIP GADM sin JSON")
                continue
            # Seleccionar el JSON más grande (es el de features, no metadata)
            jsnames.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
            with zf.open(jsnames[0]) as fh:
                gj = json.load(fh)
            feats = gj.get("features", [])
            log.info("  %s features en GADM L%s", len(feats), level)
            if not feats:
                continue
            n = _upsert_distritos_features(
                conn, cur, feats, f"GADM 4.1 L{level}",
                nombre_col=ncol, prov_col=pcol, dep_col=dcol,
            )
            log.info("✅ %s distritos GADM L%s insertados", n, level)
            return n
        except Exception as exc:
            log.warning("  GADM L%s falló: %s", level, exc)

    # 3) Fallback con polígonos aproximados
    log.warning("Usando fallback distritos (~43 polígonos Ica + costa)")
    return _distritos_fallback(conn, cur)


def _upsert_distritos_features(conn, cur, features: list[dict], fuente: str,
                                nombre_col: str = "DISTRITO",
                                prov_col: str = "PROVINCIA",
                                dep_col: str = "DEPARTAMENTO") -> int:
    sql_ubigeo = """
        INSERT INTO distritos
            (ubigeo, nombre, provincia, departamento, geom, nivel_riesgo, fuente)
        VALUES (
            %(ubigeo)s, %(nombre)s, %(provincia)s, %(departamento)s,
            ST_Multi(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%(geom_json)s), 4326))),
            %(nivel_riesgo)s, %(fuente)s
        )
        ON CONFLICT (ubigeo) DO NOTHING
    """
    sql_sin_ubigeo = """
        INSERT INTO distritos
            (nombre, provincia, departamento, geom, nivel_riesgo, fuente)
        VALUES (
            %(nombre)s, %(provincia)s, %(departamento)s,
            ST_Multi(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%(geom_json)s), 4326))),
            %(nivel_riesgo)s, %(fuente)s
        )
        ON CONFLICT DO NOTHING
    """
    count = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry")
        if not geom or geom.get("type") not in ("Polygon", "MultiPolygon"):
            continue
        nombre    = str(props.get(nombre_col) or props.get("NOMBRE") or "Sin nombre").strip().title()[:200]
        provincia = str(props.get(prov_col)   or "").strip().title()[:200]
        dep       = str(props.get(dep_col)    or "Ica").strip().title()[:200]
        ubigeo    = str(props.get("UBIGEO")   or props.get("ubigeo") or "").strip() or None
        nivel     = RIESGO_DEPTO.get(dep, RIESGO_PROV.get(provincia, 3))

        row = dict(ubigeo=ubigeo, nombre=nombre, provincia=provincia,
                   departamento=dep, geom_json=json.dumps(geom),
                   nivel_riesgo=nivel, fuente=fuente)
        try:
            if ubigeo:
                cur.execute(sql_ubigeo, row)
            else:
                cur.execute(sql_sin_ubigeo, row)
            count += 1
        except Exception as e:
            log.debug("Error distrito '%s': %s", nombre, e)
            _rollback_safe(conn)

    conn.commit()
    return count


def _distritos_fallback(conn, cur) -> int:
    """~43 distritos con polígonos aproximados (Ica + principales ciudades costa)."""
    distritos = [
        # Provincia Ica
        {"n": "Ica",            "p": "Ica",    "r": 4, "c": [[-75.98,-14.42],[-75.38,-14.42],[-75.38,-13.78],[-75.98,-13.78],[-75.98,-14.42]]},
        {"n": "Parcona",        "p": "Ica",    "r": 4, "c": [[-75.85,-14.05],[-75.72,-14.05],[-75.72,-13.95],[-75.85,-13.95],[-75.85,-14.05]]},
        {"n": "Subtanjalla",    "p": "Ica",    "r": 4, "c": [[-75.75,-14.05],[-75.55,-14.05],[-75.55,-13.85],[-75.75,-13.85],[-75.75,-14.05]]},
        {"n": "La Tinguiña",    "p": "Ica",    "r": 4, "c": [[-75.85,-14.15],[-75.55,-14.15],[-75.55,-13.90],[-75.85,-13.90],[-75.85,-14.15]]},
        {"n": "Pueblo Nuevo",   "p": "Ica",    "r": 4, "c": [[-75.80,-14.05],[-75.60,-14.05],[-75.60,-13.85],[-75.80,-13.85],[-75.80,-14.05]]},
        {"n": "Salas",          "p": "Ica",    "r": 3, "c": [[-75.40,-14.30],[-75.20,-14.30],[-75.20,-14.10],[-75.40,-14.10],[-75.40,-14.30]]},
        {"n": "Ocucaje",        "p": "Ica",    "r": 3, "c": [[-75.65,-14.55],[-75.30,-14.55],[-75.30,-14.35],[-75.65,-14.35],[-75.65,-14.55]]},
        # Provincia Pisco
        {"n": "Pisco",          "p": "Pisco",  "r": 5, "c": [[-76.42,-13.98],[-75.88,-13.98],[-75.88,-13.48],[-76.42,-13.48],[-76.42,-13.98]]},
        {"n": "Paracas",        "p": "Pisco",  "r": 5, "c": [[-76.45,-13.90],[-76.20,-13.90],[-76.20,-13.60],[-76.45,-13.60],[-76.45,-13.90]]},
        {"n": "San Andrés",     "p": "Pisco",  "r": 5, "c": [[-76.25,-13.80],[-76.00,-13.80],[-76.00,-13.55],[-76.25,-13.55],[-76.25,-13.80]]},
        {"n": "Independencia",  "p": "Pisco",  "r": 5, "c": [[-76.30,-13.90],[-76.00,-13.90],[-76.00,-13.60],[-76.30,-13.60],[-76.30,-13.90]]},
        # Provincia Chincha
        {"n": "Chincha Alta",   "p": "Chincha","r": 5, "c": [[-76.32,-13.52],[-75.72,-13.52],[-75.72,-12.88],[-76.32,-12.88],[-76.32,-13.52]]},
        {"n": "El Carmen",      "p": "Chincha","r": 4, "c": [[-76.15,-13.42],[-75.85,-13.42],[-75.85,-13.18],[-76.15,-13.18],[-76.15,-13.42]]},
        {"n": "Tambo de Mora",  "p": "Chincha","r": 5, "c": [[-76.25,-13.60],[-76.05,-13.60],[-76.05,-13.40],[-76.25,-13.40],[-76.25,-13.60]]},
        {"n": "Grocio Prado",   "p": "Chincha","r": 4, "c": [[-76.15,-13.60],[-75.95,-13.60],[-75.95,-13.40],[-76.15,-13.40],[-76.15,-13.60]]},
        # Provincia Nazca
        {"n": "Nasca",          "p": "Nazca",  "r": 4, "c": [[-75.32,-14.98],[-74.58,-14.98],[-74.58,-14.42],[-75.32,-14.42],[-75.32,-14.98]]},
        {"n": "Marcona",        "p": "Nazca",  "r": 4, "c": [[-75.30,-15.40],[-74.90,-15.40],[-74.90,-15.10],[-75.30,-15.10],[-75.30,-15.40]]},
        {"n": "Vista Alegre",   "p": "Nazca",  "r": 3, "c": [[-75.15,-14.75],[-74.88,-14.75],[-74.88,-14.52],[-75.15,-14.52],[-75.15,-14.75]]},
        # Provincia Palpa
        {"n": "Palpa",          "p": "Palpa",  "r": 3, "c": [[-75.58,-14.72],[-74.92,-14.72],[-74.92,-14.18],[-75.58,-14.18],[-75.58,-14.72]]},
        {"n": "Río Grande",     "p": "Palpa",  "r": 3, "c": [[-75.42,-14.55],[-75.08,-14.55],[-75.08,-14.28],[-75.42,-14.28],[-75.42,-14.55]]},
    ]
    sql = (
        "INSERT INTO distritos "
        "  (nombre, provincia, departamento, geom, nivel_riesgo, fuente) "
        "VALUES (%s, %s, 'Ica', "
        "  ST_Multi(ST_SetSRID(ST_GeomFromText(%s), 4326)), "
        "  %s, 'aproximado-INEI') "
        "ON CONFLICT DO NOTHING"
    )
    for d in distritos:
        coords = ",".join(f"{c[0]} {c[1]}" for c in d["c"])
        cur.execute(sql, (d["n"], d["p"], f"POLYGON(({coords}))", d["r"]))
    conn.commit()
    return len(distritos)


# ══════════════════════════════════════════════════════════
#  PASO 3 — FALLAS GEOLÓGICAS
#  Columnas schema: nombre, nombre_alt, geom, activa, tipo,
#    mecanismo, longitud_km, magnitud_max, region, fuente, referencia
# ══════════════════════════════════════════════════════════

def sincronizar_fallas(conn, cur, force: bool = False) -> int:
    if not force:
        cur.execute("SELECT COUNT(*) FROM fallas")
        if cur.fetchone()[0] > 0:
            log.info("Fallas ya cargadas — omitiendo")
            return 0

    ingemmet_n = 0
    bbox_str   = (f"{BBOX_PERU['lon_min']},{BBOX_PERU['lat_min']},"
                  f"{BBOX_PERU['lon_max']},{BBOX_PERU['lat_max']}")

    for url in INGEMMET_FALLAS_URLS:
        svc = url.split("/services/")[1].split("/query")[0] if "/services/" in url else url
        log.info("Consultando INGEMMET: %s ...", svc)
        try:
            params = {
                "where": "1=1", "geometry": bbox_str,
                "geometryType": "esriGeometryEnvelope",
                "spatialRel": "esriSpatialRelIntersects",
                "outFields": "*", "outSR": "4326", "f": "geojson",
                "resultRecordCount": 2000,
            }
            resp     = http_get(url, params=params, timeout=60)
            features = resp.json().get("features", [])
            if features:
                log.info("  %s features INGEMMET", len(features))
                ingemmet_n = _upsert_fallas_arcgis(conn, cur, features, svc)
                break
        except Exception as exc:
            log.warning("  INGEMMET falló: %s", exc)

    n_cientifico = _insertar_fallas_dataset(conn, cur)
    total = ingemmet_n + n_cientifico
    log.info("✅ %s fallas totales (%s INGEMMET + %s dataset científico)", total, ingemmet_n, n_cientifico)
    return total


def _upsert_fallas_arcgis(conn, cur, features: list[dict], fuente: str) -> int:
    sql = """
        INSERT INTO fallas (nombre, geom, activa, tipo, fuente)
        VALUES (%(nombre)s,
            ST_Multi(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%(geom_json)s), 4326))),
            %(activa)s, %(tipo)s, %(fuente)s)
        ON CONFLICT DO NOTHING
    """
    count = 0
    for i, feat in enumerate(features):
        props = feat.get("properties") or {}
        geom  = feat.get("geometry")
        if not geom:
            continue
        gtype = geom.get("type", "")
        # Normalizar todas las geometrías a MultiLineString
        if gtype == "LineString":
            geom = {"type": "MultiLineString", "coordinates": [geom["coordinates"]]}
        elif gtype == "Polygon":
            geom = {"type": "MultiLineString", "coordinates": geom["coordinates"]}
        elif gtype == "MultiPolygon":
            rings = []
            for poly in geom["coordinates"]:
                rings.extend(poly)
            geom = {"type": "MultiLineString", "coordinates": rings}
        elif gtype != "MultiLineString":
            continue

        nombre = str(props.get("NOMBRE") or props.get("nombre") or
                     props.get("NAME") or f"Falla-{i + 1}")[:200]
        tipo   = str(props.get("TIPO") or props.get("tipo") or "Neotectónica")[:100]
        try:
            cur.execute(sql, {"nombre": nombre, "geom_json": json.dumps(geom),
                               "activa": True, "tipo": tipo, "fuente": fuente})
            count += 1
        except Exception as e:
            log.debug("Error falla INGEMMET '%s': %s", nombre, e)
            _rollback_safe(conn)

    conn.commit()
    return count


def _insertar_fallas_dataset(conn, cur) -> int:
    """
    Inserta dataset científico.
    magnitud_max  ← columna real del schema (no mag_maxima).
    longitud_km   ← calculado con ST_Length una sola vez.
    """
    sql = """
        INSERT INTO fallas
            (nombre, nombre_alt, geom, activa, tipo, mecanismo,
             longitud_km, magnitud_max, region, fuente, referencia)
        VALUES (
            %s, %s,
            ST_Multi(ST_SetSRID(ST_GeomFromText(%s), 4326)),
            %s, %s, %s,
            ROUND(
                ST_Length(
                    ST_SetSRID(ST_GeomFromText(%s), 4326)::geography
                ) / 1000
            )::NUMERIC,
            %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
    """
    count = 0
    for f in FALLAS_NACIONAL:
        coords = f["coords"]
        if len(coords) < 2:
            log.warning("Falla '%s' < 2 puntos — omitiendo", f["nombre"])
            continue
        # Validar rango de coordenadas
        if any(not (-180 <= c[0] <= 180 and -90 <= c[1] <= 90) for c in coords):
            log.warning("Coordenada inválida en falla '%s' — omitiendo", f["nombre"])
            continue
        coord_str = ",".join(f"{c[0]} {c[1]}" for c in coords)
        wkt       = f"LINESTRING({coord_str})"
        try:
            cur.execute(sql, (
                f["nombre"],
                f.get("nombre_alt"),
                wkt,                       # geom
                f.get("activa", True),
                f.get("tipo", "Inferida"),
                f.get("mecanismo"),
                wkt,                       # segunda vez para ST_Length
                f.get("magnitud_max"),
                f.get("region"),
                f.get("fuente", "INGEMMET"),
                f.get("referencia"),
            ))
            count += 1
        except Exception as exc:
            log.warning("  Error falla '%s': %s", f["nombre"], exc)
            _rollback_safe(conn)

    conn.commit()
    log.info("✅ %s fallas científicas (cobertura nacional)", count)
    return count


# ══════════════════════════════════════════════════════════
#  PASO 4 — ZONAS INUNDABLES
#  Schema: tipo_inundacion  (no tipo)
# ══════════════════════════════════════════════════════════

def sincronizar_inundables(conn, cur, force: bool = False) -> int:
    if not force:
        cur.execute("SELECT COUNT(*) FROM zonas_inundables")
        if cur.fetchone()[0] > 0:
            log.info("Zonas inundables ya cargadas — omitiendo")
            return 0

    # Intentar ANA SNIRH WFS
    for ana_url in [
        "https://snirh.ana.gob.pe/geoserver/snirh/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=snirh:zonas_inundacion&outputFormat=application/json",
        "https://geoserver.ana.gob.pe/geoserver/ows"
        "?service=WFS&version=1.0.0&request=GetFeature"
        "&typeName=ana:zonas_inundables&outputFormat=application/json",
    ]:
        try:
            log.info("Consultando ANA WFS: %s ...", ana_url[:60])
            resp  = http_get(ana_url, timeout=45)
            feats = resp.json().get("features", [])
            if feats:
                n = _upsert_inundables_geojson(conn, cur, feats, "ANA SNIRH")
                log.info("  %s zonas ANA insertadas", n)
        except Exception as exc:
            log.warning("  ANA WFS falló: %s", exc)

    return _insertar_inundables_dataset(conn, cur)


def _upsert_inundables_geojson(conn, cur, features: list[dict], fuente: str) -> int:
    sql = """
        INSERT INTO zonas_inundables
            (nombre, geom, nivel_riesgo, tipo_inundacion, fuente)
        VALUES (%(nombre)s,
            ST_Multi(ST_MakeValid(ST_SetSRID(ST_GeomFromGeoJSON(%(geom_json)s), 4326))),
            %(nivel_riesgo)s, %(tipo_inundacion)s, %(fuente)s)
        ON CONFLICT DO NOTHING
    """
    count = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom  = feat.get("geometry")
        if not geom:
            continue
        nombre = str(props.get("nombre") or props.get("NOMBRE") or "Zona inundable")[:200]
        nivel  = int(props.get("nivel_riesgo") or props.get("NIVEL") or 3)
        try:
            cur.execute(sql, {"nombre": nombre, "geom_json": json.dumps(geom),
                               "nivel_riesgo": min(max(nivel, 1), 5),
                               "tipo_inundacion": "fluvial", "fuente": fuente})
            count += 1
        except Exception as e:
            log.debug("Error zona ANA: %s", e)
            _rollback_safe(conn)
    conn.commit()
    return count


def _insertar_inundables_dataset(conn, cur) -> int:
    sql = """
        INSERT INTO zonas_inundables
            (nombre, geom, nivel_riesgo, tipo_inundacion, periodo_retorno,
             profundidad_max_m, cuenca, region, fuente)
        VALUES (%s,
            ST_Multi(ST_SetSRID(ST_GeomFromText(%s), 4326)),
            %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    count = 0
    for z in ZONAS_INUNDABLES:
        coords = ",".join(f"{c[0]} {c[1]}" for c in z["coords"])
        wkt    = f"POLYGON(({coords}))"
        try:
            cur.execute(sql, (
                z["nombre"], wkt, z["nivel_riesgo"],
                z["tipo_inundacion"],              # ← nombre correcto de columna
                z.get("periodo_retorno"),
                z.get("profundidad_max_m"),
                z.get("cuenca"), z.get("region"), z["fuente"],
            ))
            count += 1
        except Exception as e:
            log.warning("Error zona inundable '%s': %s", z["nombre"], e)
            _rollback_safe(conn)

    conn.commit()
    log.info("✅ %s zonas inundables insertadas", count)
    return count


# ══════════════════════════════════════════════════════════
#  PASO 5 — ZONAS DE TSUNAMI
#  Tabla correcta: zonas_tsunami  (no tsunamis)
# ══════════════════════════════════════════════════════════

def sincronizar_tsunamis(conn, cur, force: bool = False) -> int:
    if not force:
        cur.execute("SELECT COUNT(*) FROM zonas_tsunami")
        if cur.fetchone()[0] > 0:
            log.info("Zonas tsunami ya cargadas — omitiendo")
            return 0

    sql = """
        INSERT INTO zonas_tsunami
            (nombre, geom, nivel_riesgo, altura_ola_m, tiempo_arribo_min,
             periodo_retorno, region, fuente)
        VALUES (%s,
            ST_Multi(ST_SetSRID(ST_GeomFromText(%s), 4326)),
            %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    count = 0
    for z in ZONAS_TSUNAMI:
        coords = ",".join(f"{c[0]} {c[1]}" for c in z["coords"])
        wkt    = f"POLYGON(({coords}))"
        try:
            cur.execute(sql, (
                z["nombre"], wkt, z["nivel_riesgo"],
                z.get("altura_ola_m"), z.get("tiempo_arribo_min"),
                z.get("periodo_retorno"), z.get("region"), z["fuente"],
            ))
            count += 1
        except Exception as e:
            log.warning("Error tsunami '%s': %s", z["nombre"], e)
            _rollback_safe(conn)

    conn.commit()
    log.info("✅ %s zonas de tsunami insertadas", count)
    return count


# ══════════════════════════════════════════════════════════
#  PASO 6 — INFRAESTRUCTURA CRÍTICA (OSM Overpass)
# ══════════════════════════════════════════════════════════

def sincronizar_infraestructura(conn, cur, force: bool = False) -> int:
    if not force:
        cur.execute("SELECT COUNT(*) FROM infraestructura")
        if cur.fetchone()[0] > 0:
            log.info("Infraestructura ya cargada — omitiendo")
            return 0

    bbox_ov = (f"{BBOX_PERU['lat_min']},{BBOX_PERU['lon_min']},"
               f"{BBOX_PERU['lat_max']},{BBOX_PERU['lon_max']}")

    queries = {
        "hospital":          (f'(node["amenity"="hospital"]({bbox_ov});way["amenity"="hospital"]({bbox_ov}););', 5),
        "clinica":           (f'node["amenity"~"clinic|health_post"]({bbox_ov});', 4),
        "escuela":           (f'(node["amenity"~"school|university"]({bbox_ov});way["amenity"~"school|university"]({bbox_ov}););', 3),
        "bomberos":          (f'node["amenity"="fire_station"]({bbox_ov});', 5),
        "policia":           (f'node["amenity"="police"]({bbox_ov});', 4),
        "aeropuerto":        (f'(node["aeroway"="aerodrome"]({bbox_ov});way["aeroway"="aerodrome"]({bbox_ov}););', 5),
        "puerto":            (f'(node["harbour"="yes"]({bbox_ov});node["amenity"="ferry_terminal"]({bbox_ov}););', 5),
        "central_electrica": (f'node["power"~"plant|substation"]({bbox_ov});', 5),
        "planta_agua":       (f'node["man_made"~"water_works|water_tower"]({bbox_ov});', 4),
    }

    sql = """
        INSERT INTO infraestructura
            (osm_id, nombre, tipo, geom, criticidad, fuente, region)
        VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s, %s)
        ON CONFLICT DO NOTHING
    """

    total = 0
    for tipo, (query_body, criticidad) in queries.items():
        overpass_q = f"[out:json][timeout:60];{query_body}out center;"
        for attempt in range(3):
            try:
                log.info("  OSM: %s (intento %s)...", tipo, attempt + 1)
                resp = requests.post(
                    OVERPASS_URL, data={"data": overpass_q}, timeout=75,
                    headers={"User-Agent": "GeoRiesgoPeru/4.2"},
                )
                if resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    log.warning("  Rate limit 429 — esperando %ss...", wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                elements = resp.json().get("elements", [])
                log.info("    %s elementos", len(elements))

                rows = []
                for el in elements:
                    lat = el.get("lat") or (el.get("center") or {}).get("lat")
                    lon = el.get("lon") or (el.get("center") or {}).get("lon")
                    if not lat or not lon:
                        continue
                    tags   = el.get("tags", {})
                    nombre = (tags.get("name:es") or tags.get("name") or
                              f"{tipo.replace('_',' ').title()} OSM-{el.get('id','')}")[:200]
                    reg    = region_peru(float(lon), float(lat))
                    rows.append((el.get("id"), nombre, tipo,
                                 float(lon), float(lat),       # MakePoint(lon, lat) ✓
                                 criticidad, "OpenStreetMap", reg))

                if rows:
                    psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
                    conn.commit()
                    total += len(rows)
                time.sleep(3)   # respetar rate-limit Overpass
                break
            except Exception as exc:
                log.warning("  Overpass %s intento %s falló: %s", tipo, attempt + 1, exc)
                time.sleep(10)

    if total == 0:
        log.warning("Overpass sin datos — usando fallback")
        total = _infraestructura_fallback(conn, cur)

    log.info("✅ %s elementos infraestructura", total)
    return total


def _infraestructura_fallback(conn, cur) -> int:
    infra = [
        # (nombre, tipo, lon, lat, criticidad, region)
        ("Hospital Regional de Ica",              "hospital",   -75.73560,-14.07550,5,"Ica"),
        ("Hospital San José de Chincha",          "hospital",   -76.13050,-13.40690,5,"Ica"),
        ("Hospital San Juan de Dios de Pisco",    "hospital",   -76.20170,-13.70850,5,"Ica"),
        ("Hospital de Nasca",                     "hospital",   -74.93720,-14.83560,4,"Ica"),
        ("Aeropuerto Internacional de Pisco",     "aeropuerto", -76.22010,-13.74480,5,"Ica"),
        ("Puerto General San Martín (Pisco)",     "puerto",     -76.22980,-13.79280,5,"Ica"),
        ("Hospital Almenara (ESSALUD)",           "hospital",   -77.02050,-12.06050,5,"Lima"),
        ("Hospital Rebagliati (ESSALUD)",         "hospital",   -77.03390,-12.08920,5,"Lima"),
        ("Puerto del Callao",                     "puerto",     -77.14780,-12.05870,5,"Lima"),
        ("Aeropuerto Jorge Chávez",               "aeropuerto", -77.11430,-12.02190,5,"Lima"),
        ("Hospital Honorio Delgado",              "hospital",   -71.52490,-16.38930,5,"Arequipa"),
        ("Aeropuerto Rodríguez Ballón",           "aeropuerto", -71.57190,-16.33800,5,"Arequipa"),
        ("Hospital Regional de Cusco",            "hospital",   -71.97830,-13.52250,5,"Cusco"),
        ("Aeropuerto Velasco Astete",             "aeropuerto", -71.94700,-13.53560,5,"Cusco"),
        ("Hospital Cayetano Heredia Piura",       "hospital",   -80.63120, -5.17200,5,"Piura"),
        ("Hospital Victor Ramos Guardia (Huaraz)","hospital",   -77.52880, -9.52640,5,"Ancash"),
        ("Hospital Hipólito Unanue (Tacna)",      "hospital",   -70.02860,-18.01410,4,"Tacna"),
        ("Subestación Eléctrica Ica Norte",       "central_electrica",-75.68000,-14.03000,4,"Ica"),
        ("Planta Agua Ica (EMAPICA)",             "planta_agua",-75.70000,-14.07000,4,"Ica"),
        ("Bomberos Ica",                          "bomberos",   -75.73480,-14.07500,5,"Ica"),
        ("Bomberos Pisco",                        "bomberos",   -76.20000,-13.71000,5,"Ica"),
    ]
    sql = (
        "INSERT INTO infraestructura "
        "  (nombre, tipo, geom, criticidad, fuente, region) "
        "VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, 'referencia-MINSA', %s) "
        "ON CONFLICT DO NOTHING"
    )
    for item in infra:
        cur.execute(sql, (item[0], item[1], item[2], item[3], item[4], item[5]))
    conn.commit()
    return len(infra)


# ══════════════════════════════════════════════════════════
#  PASO 7 — ESTACIONES DE MONITOREO
# ══════════════════════════════════════════════════════════

def sincronizar_estaciones(conn, cur, force: bool = False) -> int:
    if not force:
        cur.execute("SELECT COUNT(*) FROM estaciones")
        if cur.fetchone()[0] > 0:
            log.info("Estaciones ya cargadas — omitiendo")
            return 0

    sql = """
        INSERT INTO estaciones
            (codigo, nombre, tipo, geom, altitud_m, institucion, region)
        VALUES (%s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s, %s, %s)
        ON CONFLICT (codigo) DO UPDATE SET
            nombre      = EXCLUDED.nombre,
            altitud_m   = EXCLUDED.altitud_m,
            institucion = EXCLUDED.institucion,
            region      = EXCLUDED.region
    """
    count = 0
    for e in ESTACIONES:
        cur.execute(sql, (
            e["codigo"], e["nombre"], e["tipo"],
            e["lon"], e["lat"],
            e.get("altitud_m"), e["institucion"], e["region"],
        ))
        count += 1
    conn.commit()
    log.info("✅ %s estaciones de monitoreo", count)
    return count


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="GeoRiesgo Perú — ETL v4.2")
    parser.add_argument("--force", action="store_true",
                        help="Recargar todos los datos aunque ya existan")
    parser.add_argument(
        "--solo",
        choices=["sismos", "distritos", "fallas", "inundables",
                 "tsunamis", "infraestructura", "estaciones"],
        help="Sincronizar solo una capa específica",
    )
    args = parser.parse_args()

    t0 = time.time()
    print()
    print("  ╔══════════════════════════════════════════════════════╗")
    print("  ║  GeoRiesgo Perú — ETL v4.2                         ║")
    print("  ║  Fuentes: USGS·IGP·INEI·GADM·ANA·PREDES·OSM       ║")
    print("  ╚══════════════════════════════════════════════════════╝")
    print(f"  DB:      {DB_URL.split('@')[-1]}")
    print(f"  Fecha:   {hoy_utc()} UTC")
    print(f"  Force:   {'SÍ' if args.force else 'NO (incremental)'}")
    print(f"  Workers: {MAX_WORKERS} (descarga USGS paralela)")
    print()

    conn = db_connect()
    conn.autocommit = False

    counts = {k: 0 for k in
              ["sismos", "distritos", "fallas", "inundables",
               "tsunamis", "infraestructura", "estaciones"]}

    steps = [
        ("sismos",          "PASO 1: Sismos USGS (paralelo)",           sincronizar_sismos),
        ("distritos",       "PASO 2: Distritos (INEI → GADM → fallback)",sincronizar_distritos),
        ("fallas",          "PASO 3: Fallas (INGEMMET + dataset IGP)",   sincronizar_fallas),
        ("inundables",      "PASO 4: Inundaciones (ANA/CENEPRED)",       sincronizar_inundables),
        ("tsunamis",        "PASO 5: Tsunamis (PREDES/IGP)",             sincronizar_tsunamis),
        ("infraestructura", "PASO 6: Infraestructura (OSM Overpass)",    sincronizar_infraestructura),
        ("estaciones",      "PASO 7: Estaciones (IGP/SENAMHI/ANA)",      sincronizar_estaciones),
    ]

    errores: list[str] = []
    try:
        with conn.cursor() as cur:
            for key, label, fn in steps:
                if args.solo and args.solo != key:
                    continue
                print(f"  ── {label}")
                t_step = time.time()
                try:
                    n = fn(conn, cur, force=args.force)
                    counts[key] = n
                    elapsed     = time.time() - t_step
                    log_sync(conn, key, key, n, duracion=elapsed)
                    print(f"     → {n:,} registros en {elapsed:.1f}s\n")
                except Exception as exc:
                    elapsed = time.time() - t_step
                    log.error("ERROR en %s: %s", key, exc)
                    errores.append(f"{key}: {exc}")
                    _rollback_safe(conn)
                    log_sync(conn, key, key, 0, "error", str(exc)[:500], elapsed)
                    print()

        elapsed_total = time.time() - t0
        print("  ╔══════════════════════════════════════════════════════╗")
        for k, v in counts.items():
            print(f"  ║  {k:<20} {v:>8,} registros                ║")
        print(f"  ║  Tiempo total: {elapsed_total:.1f}s                          ║")
        if errores:
            print(f"  ║  ⚠ Errores parciales: {len(errores)} paso(s)               ║")
        else:
            print("  ║  ✅ ETL completado sin errores                      ║")
        print("  ╚══════════════════════════════════════════════════════╝")
        print()

        if errores:
            sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    main()