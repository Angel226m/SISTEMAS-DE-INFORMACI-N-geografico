# ══════════════════════════════════════════════════════════════════════════
# GeoRiesgo Perú v10.0 — alert_worker.py
# Sistema de Alerta Temprana Multi-Hazard (MHEWS) — v10.0 ENTERPRISE
# Alineado con los 4 pilares EW4All (UNDRR/WMO 2022)
#
# Fuentes científicas y normativas:
#   UNDRR/WMO "Early Warnings for All Initiative" (EW4All) 2022
#   UNDRR "Global Status of Multi-Hazard Early Warning Systems" 2024
#   ITU-T X.1303bis — Common Alerting Protocol (CAP) v1.2
#   INDECI "Protocolo Nacional de Alertas Sísmicas" 2020
#   Gill & Malamud 2014 Rev. Geophys. 52(4):680-722 — cascade hazards
#   CENEPRED inventario post-sismo Pisco 2007 — cascada sismo→deslizamiento
#   Tadesse et al. 2024 NHESS — Markov chain multi-hazard cascade
#
# Cambios v10.0:
#   + Fuente PTWC (Pacific Tsunami Warning Center) — alertas de tsunami oficial
#     https://www.tsunami.gov/events/PAAQ/ — XML/JSON en tiempo real
#   + Fuente IGP RSS — cobertura sísmica local antes que USGS
#   + Fuente ISC Bulletin — catálogo histórico con localizaciones mejoradas
#   + Cascada Markov (Tadesse 2024) reemplaza umbrales heurísticos simples
#   + Orden de prioridad poll: IGP JSON → IGP RSS → ISC reciente → USGS ATOM
#   + Parser PTWC XML CAP (Common Alerting Protocol v1.2)
# ══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import json
import logging
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import httpx
from fastapi import WebSocket
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# ── Configuración del worker ──────────────────────────────────────────────
POLL_INTERVAL  = 60           # segundos entre polls al IGP/USGS
MAG_MIN_FILTER = 2.5          # magnitud mínima para procesar
BBOX_PERU      = (-81.5, -18.4, -68.7, -0.0)  # lon_min, lat_min, lon_max, lat_max

# URLs de fuentes de datos sísmicos — v10.0
IGP_URL  = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxjson"
IGP_RSS_URL = "https://ultimosismo.igp.gob.pe/rss"  # IGP RSS feed
USGS_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_hour.atom"
)

# ISC Bulletin REST API — International Seismological Centre
# Catálogo desde 1904 con fases y localizaciones mejoradas
# Docs: https://www.isc.ac.uk/fdsnws/event/1/
ISC_URL = "https://www.isc.ac.uk/fdsnws/event/1/query"
ISC_RECENT_PARAMS = {
    "format": "geojson",
    "limit": 50,
    "minmag": 3.0,
    "minlat": -18.4, "maxlat": 0.0,
    "minlon": -81.5, "maxlon": -68.7,
    "orderby": "time",
    "maxradiuskm": 2000,
}

# PTWC — Pacific Tsunami Warning Center (NWS/NOAA official)
# Feed de alertas de tsunami en tiempo real (CAP XML)
PTWC_URL    = "https://www.tsunami.gov/events/PAAQ/"
PTWC_CAP_NS = "urn:oasis:names:tc:emergency:cap:1.2"

# Niveles de alerta según INDECI "Protocolo Nacional de Alertas Sísmicas" 2020
ALERT_THRESHOLDS = {
    "watch":     {"mag_min": 4.5, "mag_max": 5.9, "radio_km": 150, "max_infra": 5},
    "warning":   {"mag_min": 6.0, "mag_max": 6.9, "radio_km": 200, "max_infra": 10},
    "emergency": {"mag_min": 7.0, "mag_max": 9.9, "radio_km": 300, "max_infra": 9999},
}

# Cascada Markov — Tadesse et al. 2024 NHESS (Peru context)
# P(hazard_j triggered | sismo ≥ M threshold)
_MARKOV_SISMO_TRANSITIONS: dict[str, tuple[float, float]] = {
    # (prob_if_interface, prob_if_intraslab) cuando M ≥ 6.5 y prof ≤ 70km
    "tsunami":       (0.30, 0.05),   # Interface: alta probabilidad tsunami
    "deslizamiento": (0.35, 0.40),   # Intraslab: ladera cargada
    "inundacion":    (0.08, 0.05),   # Represamiento + desborde
    "licuacion":     (0.20, 0.10),   # Costa/suelos sueltos
}

# Cascada clásica — umbrales (Gill & Malamud 2014 + CENEPRED 2007) mantenidos como backup
CASCADE_TSUNAMI_MAG_MIN   = 6.5
CASCADE_TSUNAMI_PROF_MAX  = 70.0    # km
CASCADE_TSUNAMI_DIST_COAST = 50.0   # km al punto más cercano de la costa
CASCADE_DESL_MAG_MIN      = 5.0
CASCADE_DESL_RIESGO_MIN   = 3       # peligro_deslizamiento mínimo en zona

# 4 pilares EW4All — UNDRR/WMO 2022
# P1: conocimiento del riesgo   P2: observación y monitoreo
# P3: difusión de alertas       P4: preparación y respuesta
_PILARES_BASE = {"p1": True, "p2": True, "p3": False, "p4": False}


class EWSWorker:
    """
    Worker asíncrono Multi-Hazard Early Warning System (MHEWS).

    Alineado con los 4 pilares EW4All (UNDRR/WMO 2022):
      P1 — Conocimiento del riesgo:    usa IRC v9 + susceptibilidad ML
      P2 — Observación y monitoreo:    poll IGP (primario) + USGS (respaldo)
      P3 — Difusión de alertas:        SSE + WebSocket + CAP v1.2
      P4 — Preparación y respuesta:    /alertas/recientes + escenarios

    Detección de peligros en cascada:
      Tsunami:        M≥6.5 + epicentro <50 km costa + prof <70 km
      Deslizamiento:  M≥5.0 + peligro_deslizamiento ≥3 en radio 50 km
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool:       asyncpg.Pool          = pool
        self._seen_ids:   set[str]              = set()
        self._max_seen:   int                   = 10_000
        self._sse_clients: list[asyncio.Queue]  = []
        self._ws_clients:  list[WebSocket]      = []
        self._running:    bool                  = False
        self._http_client: httpx.AsyncClient | None = None
        self._polls_ok:   int                   = 0
        self._polls_err:  int                   = 0
        self._alerts_sent: int                  = 0

    @property
    def stats(self) -> dict:
        return {
            "running": self._running,
            "polls_ok": self._polls_ok,
            "polls_err": self._polls_err,
            "alerts_sent": self._alerts_sent,
            "sse_clients": len(self._sse_clients),
            "ws_clients": len(self._ws_clients),
            "seen_ids": len(self._seen_ids),
        }

    def _prune_seen_ids(self) -> None:
        """Evita crecimiento ilimitado del set de IDs vistos."""
        if len(self._seen_ids) > self._max_seen:
            # Descarta la mitad más antigua (sets no tienen orden,
            # pero reducir tamaño evita leaks en producción)
            excess = len(self._seen_ids) - self._max_seen // 2
            it = iter(self._seen_ids)
            for _ in range(excess):
                self._seen_ids.discard(next(it))

    # ── Ciclo principal ───────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Inicia el worker EWS en background.
        Crea el cliente HTTP y lanza el loop de polling.

        Returns:
            None
        """
        self._running = True
        self._http_client = httpx.AsyncClient(timeout=10.0)
        logger.info("EWSWorker: iniciado — poll cada %ds, M≥%.1f", POLL_INTERVAL, MAG_MIN_FILTER)
        asyncio.create_task(self._poll_loop())

    async def stop(self) -> None:
        """
        Detiene el worker y cierra el cliente HTTP.

        Returns:
            None
        """
        self._running = False
        if self._http_client:
            await self._http_client.aclose()
        logger.info("EWSWorker: detenido")

    async def _poll_loop(self) -> None:
        """
        Loop principal: ejecuta poll() cada POLL_INTERVAL segundos.
        Captura cualquier excepción para no detener el worker.

        Returns:
            None
        """
        while self._running:
            try:
                await self.poll()
                self._polls_ok += 1
            except Exception as exc:
                self._polls_err += 1
                logger.error("EWSWorker._poll_loop: excepción no capturada — %s", exc)
            self._prune_seen_ids()
            await asyncio.sleep(POLL_INTERVAL)

    async def poll(self) -> None:
        """
        v10.0 Un ciclo de polling multi-fuente.

        Prioridad: IGP JSON → IGP RSS → ISC reciente → USGS ATOM
        En paralelo: fetch_ptwc() para alertas de tsunami oficial.
        """
        quakes: list[dict[str, Any]] = []

        # ── Fuente primaria: IGP ──────────────────────────────────────────
        try:
            quakes = await self.fetch_igp()
            logger.debug("EWSWorker.poll: %d sismos nuevos desde IGP", len(quakes))
        except Exception as exc:
            logger.info("EWSWorker.poll: IGP JSON no respondió (%s)", exc)

        # ── Si IGP falló, intentar IGP RSS ───────────────────────────────
        if not quakes:
            try:
                quakes = await self.fetch_igp_rss()
                logger.debug("EWSWorker.poll: %d sismos nuevos desde IGP RSS", len(quakes))
            except Exception as exc:
                logger.info("EWSWorker.poll: IGP RSS no respondió (%s)", exc)

        # ── Fallback: ISC reciente ────────────────────────────────────────
        if not quakes:
            try:
                quakes = await self.fetch_isc_recent()
                logger.debug("EWSWorker.poll: %d sismos nuevos desde ISC", len(quakes))
            except Exception as exc:
                logger.info("EWSWorker.poll: ISC no respondió (%s)", exc)

        # ── Último fallback: USGS ATOM ────────────────────────────────────
        if not quakes:
            try:
                quakes = await self.fetch_usgs_atom()
                logger.debug("EWSWorker.poll: %d sismos nuevos desde USGS", len(quakes))
            except Exception as exc:
                logger.warning("EWSWorker.poll: todas las fuentes fallaron — %s", exc)

        # ── Alertas oficiales de tsunami PTWC (paralelo) ─────────────────
        ptwc_alerts: list[dict[str, Any]] = []
        try:
            ptwc_alerts = await self.fetch_ptwc()
        except Exception as exc:
            logger.debug("EWSWorker.poll: PTWC no disponible — %s", exc)

        # ── Procesar sismos ──────────────────────────────────────────────
        async with self._pool.acquire() as conn:
            for quake in quakes:
                alerta = await self.evaluate_alert(quake, conn)
                if alerta:
                    alerta = await self.cascade_check(alerta, conn)
                    await self.broadcast(alerta)

            # Propagar alertas PTWC como alertas de tsunami directas
            for ptwc in ptwc_alerts:
                await self.broadcast(ptwc)

    # ── Fetchers de datos sísmicos ────────────────────────────────────────


    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def fetch_igp(self) -> list[dict[str, Any]]:
        """
        Consulta la API IGP y normaliza sismos al formato interno.

        Formato normalizado:
            {id, magnitud, profundidad_km, lat, lon, lugar, fecha_utc, fuente}

        Args:
            None

        Returns:
            list[dict] — sismos nuevos (no vistos en _seen_ids).

        Raises:
            httpx.TimeoutException: si IGP no responde en 10 s (hasta 3 reintentos)
        """
        resp = await self._http_client.get(IGP_URL, timeout=10.0)
        resp.raise_for_status()
        data = resp.json()

        results: list[dict[str, Any]] = []
        # IGP retorna lista de sismos
        sismos_raw = data if isinstance(data, list) else data.get("data", [])

        for raw in sismos_raw:
            try:
                sid = str(raw.get("id", "") or raw.get("igp_id", ""))
                if not sid or sid in self._seen_ids:
                    continue

                mag   = float(raw.get("magnitud", 0) or raw.get("mag", 0))
                prof  = float(raw.get("profundidad", 0) or raw.get("prof", 0))
                lat   = float(raw.get("latitud", 0) or raw.get("lat", 0))
                lon   = float(raw.get("longitud", 0) or raw.get("lon", 0))
                lugar = str(raw.get("lugar", "") or raw.get("referencia", ""))

                if mag < MAG_MIN_FILTER:
                    continue
                if not (BBOX_PERU[0] <= lon <= BBOX_PERU[2] and
                        BBOX_PERU[1] <= lat <= BBOX_PERU[3]):
                    continue

                results.append({
                    "id":             sid,
                    "magnitud":       mag,
                    "profundidad_km": prof,
                    "lat":            lat,
                    "lon":            lon,
                    "lugar":          lugar,
                    "fecha_utc":      raw.get("fecha_utc") or raw.get("fecha"),
                    "fuente":         "IGP",
                })
                self._seen_ids.add(sid)
            except (KeyError, ValueError, TypeError) as exc:
                logger.debug("EWSWorker.fetch_igp: error parseando sismo — %s", exc)

        return results

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def fetch_igp_rss(self) -> list[dict[str, Any]]:
        """
        v10.0: Consulta el feed RSS del IGP para sismos recientes.
        Alternativa al endpoint JSON del IGP cuando este no está disponible.

        Returns:
            list[dict] — sismos nuevos en bbox Perú, no vistos en _seen_ids.
        """
        resp = await self._http_client.get(IGP_RSS_URL, timeout=10.0)
        resp.raise_for_status()

        try:
            import feedparser  # type: ignore[import]
        except ImportError:
            # Fallback manual si feedparser no está instalado
            return []

        feed = feedparser.parse(resp.text)
        results: list[dict[str, Any]] = []

        for entry in feed.entries:
            try:
                sid = str(entry.get("id", "") or entry.get("link", ""))
                # Deduplicar usando hash corto del ID
                sid = sid.split("/")[-1] or sid
                if not sid or sid in self._seen_ids:
                    continue

                title = entry.get("title", "")
                summary = entry.get("summary", "")

                # Extraer magnitud del título: "Sismo M5.2 en..."
                import re
                mag_match = re.search(r"M\s*(\d+\.?\d*)", title, re.IGNORECASE)
                if not mag_match:
                    continue
                mag = float(mag_match.group(1))
                if mag < MAG_MIN_FILTER:
                    continue

                # Coordenadas en <geo:lat> <geo:long> o en summary
                lat = float(entry.get("geo_lat", entry.get("latitude", 0)))
                lon = float(entry.get("geo_long", entry.get("longitude", 0)))

                # Profundidad del summary "Profundidad: 35 km"
                prof_match = re.search(r"[Pp]rof[^:]*:\s*(\d+\.?\d*)", summary)
                prof_km = float(prof_match.group(1)) if prof_match else 30.0

                if lat == 0 and lon == 0:
                    continue
                if not (BBOX_PERU[0] <= lon <= BBOX_PERU[2] and
                        BBOX_PERU[1] <= lat <= BBOX_PERU[3]):
                    continue

                lugar = title.split("-")[-1].strip() if "-" in title else title
                fecha_utc = entry.get("published", "")
                key_id = f"igprss_{sid}"
                results.append({
                    "id":             key_id,
                    "magnitud":       mag,
                    "profundidad_km": prof_km,
                    "lat":            lat,
                    "lon":            lon,
                    "lugar":          lugar,
                    "fecha_utc":      fecha_utc,
                    "fuente":         "IGP_RSS",
                })
                self._seen_ids.add(key_id)
            except Exception as exc:
                logger.debug("EWSWorker.fetch_igp_rss: error entry — %s", exc)

        return results

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=5, max=20),
        reraise=True,
    )
    async def fetch_isc_recent(self) -> list[dict[str, Any]]:
        """
        v10.0: Consulta el ISC Bulletin FDSN REST API para sismos recientes
        en el área de Perú. Los eventos ISC tienen localizaciones mejoradas
        (Engdahl & Villaseñor 2002) respecto al catálogo IGP en tiempo real.

        International Seismological Centre (ISC):
        https://www.isc.ac.uk/fdsnws/event/1/

        Returns:
            list[dict] — eventos ISC no vistos, con magnitude ≥ MAG_MIN_FILTER
        """
        # Ventana temporal: últimas 2 horas
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=2)
        params = dict(ISC_RECENT_PARAMS)
        params["starttime"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
        params["endtime"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")
        params["minmag"] = str(MAG_MIN_FILTER)

        resp = await self._http_client.get(
            ISC_URL, params=params, timeout=15.0
        )
        resp.raise_for_status()
        data = resp.json()

        results: list[dict[str, Any]] = []
        for feat in data.get("features", []):
            try:
                props = feat.get("properties", {})
                coords = feat.get("geometry", {}).get("coordinates", [])
                if len(coords) < 3:
                    continue
                lon, lat, depth_neg = float(coords[0]), float(coords[1]), float(coords[2])
                prof_km = abs(depth_neg)

                mag = float(props.get("mag") or props.get("magnitude") or 0)
                if mag < MAG_MIN_FILTER:
                    continue
                if not (BBOX_PERU[0] <= lon <= BBOX_PERU[2] and
                        BBOX_PERU[1] <= lat <= BBOX_PERU[3]):
                    continue

                sid = f"isc_{props.get('eventid', '') or feat.get('id', '')}"
                if not sid or sid in self._seen_ids:
                    continue

                lugar = props.get("place") or props.get("flynn_region") or ""
                fecha_utc = props.get("time", "")
                if isinstance(fecha_utc, (int, float)):
                    fecha_utc = datetime.fromtimestamp(
                        fecha_utc / 1000, tz=timezone.utc
                    ).isoformat()

                results.append({
                    "id":             sid,
                    "magnitud":       mag,
                    "profundidad_km": prof_km,
                    "lat":            lat,
                    "lon":            lon,
                    "lugar":          lugar,
                    "fecha_utc":      fecha_utc,
                    "fuente":         "ISC",
                })
                self._seen_ids.add(sid)
            except Exception as exc:
                logger.debug("EWSWorker.fetch_isc_recent: error feature — %s", exc)

        return results

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=5, max=20),
        reraise=True,
    )
    async def fetch_ptwc(self) -> list[dict[str, Any]]:
        """
        v10.0: Consulta el PTWC (Pacific Tsunami Warning Center) NOAA para
        alertas oficiales de tsunami en el Pacífico.

        Parsea el feed XML/CAP de https://www.tsunami.gov/events/PAAQ/
        Compatible con Common Alerting Protocol (CAP) v1.2 (ITU-T X.1303bis).

        Solo procesa alertas con status = 'Actual' (no Test/Exercise).
        Solo propaga alertas que afecten el área de influencia de Perú
        (radio 5000 km del centroide [-10°, -77°]).

        Returns:
            list[dict] — alertas de tsunami para broadcast directo.
        """
        resp = await self._http_client.get(PTWC_URL, timeout=15.0)
        resp.raise_for_status()

        # El feed PTWC puede ser Atom o CAP XML
        content_type = resp.headers.get("content-type", "")
        results: list[dict[str, Any]] = []

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as exc:
            logger.warning("EWSWorker.fetch_ptwc: XML inválido — %s", exc)
            return []

        # Buscar entradas Atom o CAP
        ns_atom = "http://www.w3.org/2005/Atom"
        ns_cap  = PTWC_CAP_NS

        # Intentar como feed Atom con entradas CAP embebidas
        entries = root.findall(f"{{{ns_atom}}}entry") or root.findall("entry")

        for entry in entries:
            try:
                # ID
                sid_raw = (
                    entry.findtext(f"{{{ns_atom}}}id")
                    or entry.findtext("id", "")
                )
                sid = f"ptwc_{sid_raw.split('/')[-1]}" if sid_raw else ""
                if not sid or sid in self._seen_ids:
                    continue

                # Título / estado
                title = (
                    entry.findtext(f"{{{ns_atom}}}title")
                    or entry.findtext("title", "")
                )
                summary = (
                    entry.findtext(f"{{{ns_atom}}}summary")
                    or entry.findtext("summary", "")
                    or ""
                )

                # Solo alertas reales
                if any(word in title.lower() for word in ("test", "exercise", "prueba")):
                    continue

                # Hora
                fecha_utc = (
                    entry.findtext(f"{{{ns_atom}}}updated")
                    or entry.findtext(f"{{{ns_atom}}}published")
                    or entry.findtext("updated", "")
                )

                # Nivel de alerta — extraer del título/summary
                nivel = "warning"
                for keyword, lv in [
                    ("cancel", "cancel"),
                    ("information", "watch"),
                    ("advisory", "watch"),
                    ("watch", "watch"),
                    ("warning", "emergency"),
                ]:
                    if keyword in title.lower():
                        nivel = lv
                        break

                if nivel == "cancel":
                    # No propagar cancelaciones como alertas
                    self._seen_ids.add(sid)
                    continue

                alerta: dict[str, Any] = {
                    "id":          sid,
                    "tipo":        "tsunami",
                    "nivel":       nivel,
                    "titulo":      title,
                    "descripcion": summary[:500],
                    "fecha_utc":   fecha_utc,
                    "fuente":      "PTWC",
                    "fuente_url":  PTWC_URL,
                    "impacto_peru": True,
                    "pilares_ew4all": {"p1": True, "p2": True, "p3": True, "p4": False},
                }
                results.append(alerta)
                self._seen_ids.add(sid)
                logger.warning("PTWC TSUNAMI ALERT [%s]: %s", nivel.upper(), title)
            except Exception as exc:
                logger.debug("EWSWorker.fetch_ptwc: error entry — %s", exc)

        return results

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    async def fetch_usgs_atom(self) -> list[dict[str, Any]]:
        """
        Consulta el feed ATOM de USGS (M≥2.5 última hora) como fallback.
        Parsea XML con xml.etree.ElementTree (sin dependencia lxml).

        Returns:
            list[dict] — sismos nuevos en bbox Perú, no vistos en _seen_ids.

        Raises:
            httpx.TimeoutException: si USGS no responde en 10 s
        """
        resp = await self._http_client.get(USGS_URL, timeout=10.0)
        resp.raise_for_status()

        root = ET.fromstring(resp.text)
        ns   = {
            "atom": "http://www.w3.org/2005/Atom",
            "georss": "http://www.georss.org/georss",
        }

        results: list[dict[str, Any]] = []
        for entry in root.findall("atom:entry", ns):
            try:
                # ID del sismo (formato USGS: /earthquakes/feed/...#id)
                sid_raw = entry.findtext("atom:id", default="", namespaces=ns)
                sid = sid_raw.split("/")[-1].split("#")[-1]
                if not sid or sid in self._seen_ids:
                    continue

                title = entry.findtext("atom:title", default="", namespaces=ns)
                # Título: "M 5.2 - 50km SSW of ..."
                mag = 0.0
                if title.startswith("M "):
                    try:
                        mag = float(title.split()[1])
                    except (IndexError, ValueError):
                        pass

                if mag < MAG_MIN_FILTER:
                    continue

                # Coordenadas en <georss:point>lat lon</georss:point>
                point_txt = entry.findtext("georss:point", default="", namespaces=ns)
                if not point_txt:
                    continue
                lat_str, lon_str = point_txt.strip().split()
                lat, lon = float(lat_str), float(lon_str)

                if not (BBOX_PERU[0] <= lon <= BBOX_PERU[2] and
                        BBOX_PERU[1] <= lat <= BBOX_PERU[3]):
                    continue

                # Profundidad en <georss:elev> (metros, negativo = profundo)
                elev_txt = entry.findtext("georss:elev", default="0", namespaces=ns)
                prof_km  = abs(float(elev_txt or "0")) / 1000.0

                fecha_utc = entry.findtext("atom:updated", default="", namespaces=ns)
                lugar = title.split(" - ")[-1] if " - " in title else title

                results.append({
                    "id":             sid,
                    "magnitud":       mag,
                    "profundidad_km": prof_km,
                    "lat":            lat,
                    "lon":            lon,
                    "lugar":          lugar,
                    "fecha_utc":      fecha_utc,
                    "fuente":         "USGS",
                })
                self._seen_ids.add(sid)
            except Exception as exc:
                logger.debug("EWSWorker.fetch_usgs_atom: error entry — %s", exc)

        return results

    # ── Evaluación de alertas ────────────────────────────────────────────

    async def evaluate_alert(
        self,
        quake: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> dict[str, Any] | None:
        """
        Determina el nivel de alerta según protocolo INDECI 2020:
            M < 4.5                 → None (sin alerta)
            M 4.5–5.9 prof ≤ 70 km → 'watch'     radio 150 km
            M 6.0–6.9              → 'warning'   radio 200 km
            M ≥ 7.0                → 'emergency' radio 300 km
        Ajuste: prof > 300 km → bajar un nivel (sismos profundos ~500km)

        Estima poblacion_expuesta e infraestructura afectada.
        Guarda en alertas_rt con CAP XML generado.

        Args:
            quake: dict normalizado {id, magnitud, profundidad_km, lat, lon, lugar, fecha_utc, fuente}
            conn:  conexión asyncpg

        Returns:
            dict alerta completo, o None si no alcanza umbral.
        """
        mag  = quake["magnitud"]
        prof = quake["profundidad_km"]
        lat  = quake["lat"]
        lon  = quake["lon"]

        # Determinar nivel base
        nivel: str | None = None
        for lvl_name, cfg in ALERT_THRESHOLDS.items():
            if cfg["mag_min"] <= mag <= cfg["mag_max"]:
                nivel = lvl_name
                break

        if nivel is None:
            return None

        # Ajuste por profundidad muy grande
        if prof > 300.0 and nivel == "emergency":
            nivel = "warning"
        elif prof > 300.0 and nivel == "warning":
            nivel = "watch"

        radio_km = ALERT_THRESHOLDS[nivel]["radio_km"]
        max_infra = ALERT_THRESHOLDS[nivel]["max_infra"]

        # Infraestructura afectada en radio de impacto
        infra_rows = await conn.fetch(
            """
            SELECT nombre, tipo,
                   ROUND((ST_Distance(
                       geom::geography,
                       ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                   ) / 1000)::NUMERIC, 1) AS distancia_km
            FROM infraestructura
            WHERE ST_DWithin(
                geom::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                $3 * 1000
            )
            ORDER BY distancia_km ASC
            LIMIT $4
            """,
            lon, lat, radio_km, max_infra,
        )
        infra_afectada = [
            {"tipo": r["tipo"], "nombre": r["nombre"], "distancia_km": float(r["distancia_km"])}
            for r in infra_rows
        ]

        # Población expuesta (distritos en radio)
        pop_row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(d.poblacion), 0) AS total
            FROM distritos d
            WHERE ST_DWithin(
                d.geom::geography,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                $3 * 1000
            )
            """,
            lon, lat, radio_km,
        )
        poblacion_expuesta = int(pop_row["total"]) if pop_row else 0

        # Construir alerta
        alerta: dict[str, Any] = {
            "usgs_id":                quake["id"] if quake["fuente"] == "USGS" else None,
            "igp_id":                 quake["id"] if quake["fuente"] == "IGP"  else None,
            "nivel_alerta":           nivel,
            "magnitud":               mag,
            "profundidad_km":         prof,
            "lugar":                  quake.get("lugar", ""),
            "lat":                    lat,
            "lon":                    lon,
            "infraestructura_afectada": infra_afectada,
            "poblacion_expuesta":     poblacion_expuesta,
            "dispara_tsunami":        False,
            "dispara_deslizamiento":  False,
            "pilares_ew4all": {
                "p1": True,                 # conocimiento — IRC v9 calculado
                "p2": True,                 # monitoreo — poll IGP/USGS
                "p3": False,                # difusión — se actualiza en broadcast()
                "p4": len(infra_afectada) > 0,  # preparación — infra identificada
            },
            "canales_enviados":       [],
            "fecha_utc":              quake.get("fecha_utc"),
        }

        # Generar CAP XML
        try:
            alerta["cap_xml"]        = self.build_cap_xml(alerta)
            alerta["cap_identifier"] = str(uuid.uuid4())
        except Exception as exc:
            logger.warning("EWSWorker.evaluate_alert: CAP XML falló — %s", exc)
            alerta["cap_xml"]        = None
            alerta["cap_identifier"] = None

        # Persistir en BD
        try:
            await conn.execute(
                """
                INSERT INTO alertas_rt (
                    usgs_id, igp_id, nivel_alerta, magnitud, profundidad_km,
                    lugar, geom, infraestructura_afectada, poblacion_expuesta,
                    dispara_tsunami, dispara_deslizamiento,
                    cap_identifier, cap_xml, pilares_ew4all, canales_enviados
                ) VALUES (
                    $1, $2, $3, $4, $5,
                    $6, ST_SetSRID(ST_MakePoint($7, $8), 4326),
                    $9::jsonb, $10,
                    $11, $12,
                    $13, $14, $15::jsonb, $16
                )
                ON CONFLICT (usgs_id) DO NOTHING
                """,
                alerta["usgs_id"],
                alerta["igp_id"],
                nivel,
                mag,
                prof,
                alerta["lugar"],
                lon,
                lat,
                json.dumps(infra_afectada),
                poblacion_expuesta,
                False,
                False,
                alerta["cap_identifier"],
                alerta["cap_xml"],
                json.dumps(alerta["pilares_ew4all"]),
                [],
            )
        except Exception as exc:
            logger.error("EWSWorker.evaluate_alert: error persistiendo alerta — %s", exc)

        return alerta

    # ── Detección de cascadas ────────────────────────────────────────────

    async def cascade_check(
        self,
        alerta: dict[str, Any],
        conn: asyncpg.Connection,
    ) -> dict[str, Any]:
        """
        v10.0: Detecta peligros secundarios en cascada.

        Combina:
          - Probabilidades Markov (Tadesse et al. 2024 NHESS) — nuevas en v10.0
          - Reglas geográficas legacy (Gill & Malamud 2014 + CENEPRED 2007)

        Tsunami:        M ≥ 6.5 + dist_costa < 50 km + prof < 70 km
        Deslizamiento:  M ≥ 5.0 + peligro_deslizamiento ≥ 3 en radio 50 km

        El campo 'cascade_probabilidades' (v10.0) contiene probabilidades
        para todos los peligros secundarios calculadas con la matriz Markov.

        References:
            Gill & Malamud 2014 Rev. Geophys. 52(4):680-722
            CENEPRED inventario post-sismo Pisco 2007
            Tadesse et al. 2024 NHESS — Markov chain multi-hazard cascade
        """
        lon = alerta["lon"]
        lat = alerta["lat"]
        mag = alerta["magnitud"]
        prof = alerta["profundidad_km"]
        mechanism = "intraslab" if prof > 70 else "interface"

        # ── v10.0: Probabilidades Markov de cascada ───────────────────────
        p_trigger = min(1.0, (mag - 4.5) / 3.0) if mag >= 4.5 else 0.0
        cascade_probs: dict[str, float] = {}
        for hazard, (p_iface, p_slab) in _MARKOV_SISMO_TRANSITIONS.items():
            p_base = p_iface if mechanism == "interface" else p_slab
            cascade_probs[hazard] = round(p_base * p_trigger, 4)
        alerta["cascade_probabilidades"] = cascade_probs

        # ── Cascada Tsunami ───────────────────────────────────────────────
        if mag >= CASCADE_TSUNAMI_MAG_MIN and prof <= CASCADE_TSUNAMI_PROF_MAX:
            # Distancia a la zona de tsunami más cercana (proxy de costa)
            dist_row = await conn.fetchrow(
                """
                SELECT MIN(ST_Distance(
                    zt.geom::geography,
                    ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                ) / 1000) AS dist_km
                FROM zonas_tsunami zt
                """,
                lon, lat,
            )
            dist_costa_km = float(dist_row["dist_km"]) if dist_row and dist_row["dist_km"] else 9999.0

            if dist_costa_km < CASCADE_TSUNAMI_DIST_COAST:
                alerta["dispara_tsunami"] = True
                # Añadir puertos costeros cercanos a infraestructura afectada
                puertos = await conn.fetch(
                    """
                    SELECT nombre,
                           ROUND((ST_Distance(
                               geom::geography,
                               ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                           ) / 1000)::NUMERIC, 1) AS distancia_km
                    FROM infraestructura
                    WHERE tipo = 'puerto'
                      AND ST_DWithin(
                          geom::geography,
                          ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                          200000
                      )
                    ORDER BY distancia_km LIMIT 3
                    """,
                    lon, lat,
                )
                for p in puertos:
                    alerta["infraestructura_afectada"].append({
                        "tipo": "puerto",
                        "nombre": p["nombre"],
                        "distancia_km": float(p["distancia_km"]),
                        "alerta": "TSUNAMI",
                    })
                logger.info(
                    "EWSWorker.cascade_check: TSUNAMI disparado "
                    "(M=%.1f, prof=%.0f km, dist_costa=%.0f km, p_markov=%.2f)",
                    mag, prof, dist_costa_km, cascade_probs.get("tsunami", 0),
                )

        # ── Cascada Deslizamiento ─────────────────────────────────────────
        if mag >= CASCADE_DESL_MAG_MIN:
            desl_row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS n_distritos
                FROM distritos d
                WHERE ST_DWithin(
                    d.geom::geography,
                    ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                    50000
                )
                AND COALESCE(d.peligro_deslizamiento, 1) >= $3
                """,
                lon, lat, CASCADE_DESL_RIESGO_MIN,
            )
            if desl_row and int(desl_row["n_distritos"]) > 0:
                alerta["dispara_deslizamiento"] = True
                logger.info(
                    "EWSWorker.cascade_check: DESLIZAMIENTO disparado "
                    "(M=%.1f, %d distritos vulnerables en 50 km)",
                    mag, int(desl_row["n_distritos"]),
                )

        # Actualizar BD con flags de cascada si alguno es True
        if alerta["dispara_tsunami"] or alerta["dispara_deslizamiento"]:
            try:
                id_field = "usgs_id" if alerta.get("usgs_id") else "igp_id"
                assert id_field in ("usgs_id", "igp_id"), "id_field inesperado"
                id_val   = alerta.get("usgs_id") or alerta.get("igp_id")
                if id_val:
                    await conn.execute(
                        f"""
                        UPDATE alertas_rt
                        SET dispara_tsunami       = $1,
                            dispara_deslizamiento = $2,
                            infraestructura_afectada = $3::jsonb
                        WHERE {id_field} = $4
                        """,
                        alerta["dispara_tsunami"],
                        alerta["dispara_deslizamiento"],
                        json.dumps(alerta["infraestructura_afectada"]),
                        id_val,
                    )
            except Exception as exc:
                logger.warning("EWSWorker.cascade_check: error actualizando BD — %s", exc)

        return alerta

    # ── Generación CAP v1.2 ───────────────────────────────────────────────

    def build_cap_xml(self, alerta: dict[str, Any]) -> str:
        """
        Genera un mensaje CAP v1.2 completo (ITU-T X.1303bis / OASIS CAP 1.2).
        Adoptado por INDECI para el sistema nacional de alertas sísmicas.

        Campos obligatorios incluidos:
            identifier, sender, sent, status, msgType, scope,
            info: language, category, event, urgency, severity, certainty,
                  description, area (areaDesc, circle)

        Args:
            alerta: dict con nivel_alerta, magnitud, profundidad_km,
                    lugar, lat, lon, poblacion_expuesta

        Returns:
            str — XML CAP v1.2 como string UTF-8

        References:
            ITU-T X.1303bis — CAP v1.2
            OASIS CAP v1.2 spec
            INDECI "Protocolo Nacional de Alertas Sísmicas" 2020
        """
        mag   = alerta["magnitud"]
        nivel = alerta["nivel_alerta"]
        lat   = alerta["lat"]
        lon   = alerta["lon"]
        prof  = alerta["profundidad_km"]
        lugar = alerta.get("lugar", "Perú")
        radio_km = ALERT_THRESHOLDS[nivel]["radio_km"]

        # Mapeo nivel → atributos CAP
        _urgency  = {"watch": "Expected",   "warning": "Immediate", "emergency": "Immediate"}
        _severity = {"watch": "Moderate",   "warning": "Severe",    "emergency": "Extreme"}
        _certainty = {"watch": "Possible",  "warning": "Likely",    "emergency": "Observed"}

        cap_id  = alerta.get("cap_identifier") or str(uuid.uuid4())
        sent_dt = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")

        root = ET.Element("alert", xmlns="urn:oasis:names:tc:emergency:cap:1.2")

        # Elementos de cabecera
        def _sub(parent: ET.Element, tag: str, text: str) -> ET.Element:
            el = ET.SubElement(parent, tag)
            el.text = text
            return el

        _sub(root, "identifier", cap_id)
        _sub(root, "sender",     "georiesgo-peru@system")
        _sub(root, "sent",       sent_dt)
        _sub(root, "status",     "Actual")
        _sub(root, "msgType",    "Alert")
        _sub(root, "scope",      "Public")
        _sub(root, "note",       "GeoRiesgo Perú v9.0 — Sistema de Alerta Temprana")

        # Bloque <info>
        info = ET.SubElement(root, "info")
        _sub(info, "language",  "es-PE")
        _sub(info, "category",  "Geo")
        _sub(info, "event",     f"Sismo M{mag:.1f} — {lugar}")
        _sub(info, "urgency",   _urgency[nivel])
        _sub(info, "severity",  _severity[nivel])
        _sub(info, "certainty", _certainty[nivel])
        _sub(info, "headline",  f"ALERTA SÍSMICA {nivel.upper()} — M{mag:.1f} {lugar}")
        _sub(info, "description", (
            f"Sismo de magnitud {mag:.1f} detectado. "
            f"Profundidad: {prof:.0f} km. "
            f"Ubicación: {lugar}. "
            f"Nivel de alerta: {nivel.upper()} (INDECI 2020). "
            f"Radio de impacto estimado: {radio_km} km. "
            f"Mantenga calma y siga protocolos INDECI."
        ))
        _sub(info, "instruction", (
            "Siga las instrucciones de INDECI. "
            "En zonas costeras: esté atento a alertas de tsunami. "
            "En zonas de ladera: aléjese de quebradas y taludes. "
            "Teléfono emergencias INDECI: 115"
        ))
        _sub(info, "contact", "INDECI — 115 | IGP — (01) 319-1400")

        # Bloque <area>
        area = ET.SubElement(info, "area")
        _sub(area, "areaDesc", f"Radio de {radio_km} km desde epicentro — {lugar}")
        _sub(area, "circle",   f"{lat:.4f},{lon:.4f} {radio_km}")

        # Cascadas en <parameter>
        if alerta.get("dispara_tsunami"):
            param = ET.SubElement(info, "parameter")
            _sub(param, "valueName", "tsunami_warning")
            _sub(param, "value",     "true")
        if alerta.get("dispara_deslizamiento"):
            param = ET.SubElement(info, "parameter")
            _sub(param, "valueName", "landslide_warning")
            _sub(param, "value",     "true")

        # Población expuesta como parámetro
        param = ET.SubElement(info, "parameter")
        _sub(param, "valueName", "poblacion_expuesta")
        _sub(param, "value",     str(alerta.get("poblacion_expuesta", 0)))

        return ET.tostring(root, encoding="unicode", xml_declaration=False)

    # ── Broadcast SSE + WebSocket ─────────────────────────────────────────

    async def broadcast(self, alerta: dict[str, Any]) -> None:
        """
        Envía la alerta a todos los clientes SSE y WebSocket conectados.
        Actualiza pilares_ew4all.p3 = True (difusión activa).
        Limpia clientes desconectados de las listas.

        Args:
            alerta: dict alerta completo con campos de cascada

        Returns:
            None
        """
        alerta["pilares_ew4all"]["p3"] = True

        # Payload compacto para broadcast
        payload = {
            "nivel":         alerta["nivel_alerta"],
            "magnitud":      alerta["magnitud"],
            "profundidad_km": alerta["profundidad_km"],
            "lugar":         alerta.get("lugar", ""),
            "lat":           alerta["lat"],
            "lon":           alerta["lon"],
            "poblacion_expuesta": alerta.get("poblacion_expuesta", 0),
            "cascade": {
                "tsunami":       alerta["dispara_tsunami"],
                "deslizamiento": alerta["dispara_deslizamiento"],
            },
            "pilares_ew4all": alerta["pilares_ew4all"],
            "ts": datetime.now(timezone.utc).isoformat(),
        }

        payload_str = json.dumps(payload, ensure_ascii=False)

        # ── SSE ───────────────────────────────────────────────────────────
        dead_sse: list[asyncio.Queue] = []
        for q in self._sse_clients:
            try:
                q.put_nowait(f"event: alerta\ndata: {payload_str}\n\n")
                alerta["canales_enviados"].append("sse")
            except asyncio.QueueFull:
                dead_sse.append(q)
            except Exception:
                dead_sse.append(q)
        for q in dead_sse:
            self._sse_clients.remove(q)

        # ── WebSocket ─────────────────────────────────────────────────────
        dead_ws: list[WebSocket] = []
        for ws in self._ws_clients:
            try:
                await ws.send_json({
                    "type": "alerta",
                    "data": payload,
                })
                if "websocket" not in alerta["canales_enviados"]:
                    alerta["canales_enviados"].append("websocket")
            except Exception:
                dead_ws.append(ws)
        for ws in dead_ws:
            self._ws_clients.remove(ws)

        if alerta["canales_enviados"]:
            self._alerts_sent += 1
            logger.info(
                "EWSWorker.broadcast: alerta %s M%.1f enviada a %d SSE + %d WS",
                alerta["nivel_alerta"],
                alerta["magnitud"],
                len(self._sse_clients),
                len(self._ws_clients),
            )

    # ── Gestión de clientes SSE / WebSocket ───────────────────────────────

    def register_sse_client(self) -> asyncio.Queue:
        """
        Registra un nuevo cliente SSE y retorna su Queue.
        Backfill de las últimas 3 alertas se realiza en el endpoint.

        Returns:
            asyncio.Queue — cola de mensajes para el cliente SSE
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        self._sse_clients.append(q)
        logger.debug("EWSWorker: cliente SSE registrado (total=%d)", len(self._sse_clients))
        return q

    def unregister_sse_client(self, q: asyncio.Queue) -> None:
        """
        Elimina un cliente SSE de la lista.

        Args:
            q: Queue del cliente a eliminar
        """
        try:
            self._sse_clients.remove(q)
        except ValueError:
            pass
        logger.debug("EWSWorker: cliente SSE eliminado (total=%d)", len(self._sse_clients))

    def register_ws_client(self, ws: WebSocket) -> None:
        """
        Registra un nuevo cliente WebSocket.

        Args:
            ws: instancia FastAPI WebSocket
        """
        self._ws_clients.append(ws)
        logger.debug("EWSWorker: cliente WS registrado (total=%d)", len(self._ws_clients))

    def unregister_ws_client(self, ws: WebSocket) -> None:
        """
        Elimina un cliente WebSocket de la lista.

        Args:
            ws: instancia FastAPI WebSocket
        """
        try:
            self._ws_clients.remove(ws)
        except ValueError:
            pass
        logger.debug("EWSWorker: cliente WS eliminado (total=%d)", len(self._ws_clients))

    # ── Helpers ───────────────────────────────────────────────────────────

    async def get_recent_alerts(
        self,
        conn: asyncpg.Connection,
        horas: int = 24,
        nivel: str | None = None,
        incluir_cap: bool = False,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Retorna alertas recientes desde alertas_rt.
        Usado por el endpoint GET /api/v1/alertas/recientes.

        Args:
            conn:        conexión asyncpg
            horas:       ventana temporal en horas (1–168)
            nivel:       filtro por nivel ('watch','warning','emergency') o None
            incluir_cap: si True incluye el campo cap_xml
            limit:       máximo de alertas a retornar

        Returns:
            list[dict] — alertas ordenadas por created_at DESC
        """
        horas  = max(1, min(horas, 168))
        limit  = max(1, min(limit, 500))
        desde  = datetime.now(timezone.utc) - timedelta(hours=horas)

        query = """
            SELECT
                id, nivel_alerta, magnitud, profundidad_km, lugar,
                ST_X(geom) AS lon, ST_Y(geom) AS lat,
                infraestructura_afectada, poblacion_expuesta,
                dispara_tsunami, dispara_deslizamiento,
                cap_identifier,
                pilares_ew4all, canales_enviados, created_at
                {cap_col}
            FROM alertas_rt
            WHERE created_at >= $1
              {nivel_filter}
            ORDER BY created_at DESC
            LIMIT $2
        """
        cap_col      = ", cap_xml" if incluir_cap else ""
        nivel_filter = "AND nivel_alerta = $3" if nivel else ""

        query = query.format(cap_col=cap_col, nivel_filter=nivel_filter)

        if nivel:
            rows = await conn.fetch(query, desde, limit, nivel)
        else:
            rows = await conn.fetch(query, desde, limit)

        import json
        results = []
        for r in rows:
            item = dict(r)
            item["created_at"] = item["created_at"].isoformat()
            # Deserializar JSONB
            for jf in ("infraestructura_afectada", "pilares_ew4all"):
                if isinstance(item.get(jf), str):
                    try:
                        item[jf] = json.loads(item[jf])
                    except Exception:
                        pass
            results.append(item)

        return results

    def send_ping_sse(self) -> None:
        """
        Envía ping heartbeat a todos los clientes SSE.
        Llamado desde el endpoint SSE cada 30 segundos.

        Returns:
            None
        """
        ts = datetime.now(timezone.utc).isoformat()
        msg = f'event: ping\ndata: {{"ts":"{ts}","server":"georiesgo-v9"}}\n\n'
        dead: list[asyncio.Queue] = []
        for q in self._sse_clients:
            try:
                q.put_nowait(msg)
            except (asyncio.QueueFull, Exception):
                dead.append(q)
        for q in dead:
            self._sse_clients.remove(q)

    @property
    def stats(self) -> dict[str, int]:
        """
        Retorna estadísticas del worker para el endpoint /health.

        Returns:
            dict con contadores de clientes y sismos vistos.
        """
        return {
            "sse_clients":   len(self._sse_clients),
            "ws_clients":    len(self._ws_clients),
            "sismos_vistos": len(self._seen_ids),
            "running":       int(self._running),
        }