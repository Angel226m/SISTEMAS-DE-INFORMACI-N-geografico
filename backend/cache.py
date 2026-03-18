# ══════════════════════════════════════════════════════════════════════════
# GeoRiesgo Perú v9.0 — cache.py
# Caché Redis para respuestas GeoJSON con degradación graceful.
# Si Redis no está disponible, la API funciona sin caché (modo bypass).
#
# Fuentes:
#   Redis best practices 2025 — asyncio client patterns
#   RFC 7946 §12 — media_type "application/geo+json"
# ══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import asyncio
import functools
import logging
import os
from collections.abc import Callable
from typing import Any

import orjson
import redis.asyncio as aioredis
from fastapi import Request, Response

logger = logging.getLogger(__name__)

# ── Constantes de TTL (segundos) ──────────────────────────────────────────
# Cada valor está calibrado según la frecuencia de cambio del dato fuente.

CACHE_SISMOS_RECIENTES   = 120       # 2 min  — poll IGP cada 60 s
CACHE_SISMOS_HISTORICOS  = 3_600     # 1 h    — catálogo estable
CACHE_PRECIPITACIONES    = 43_200    # 12 h   — CHIRPS estacional
CACHE_FALLAS             = 3_600     # 1 h    — geometría inmutable
CACHE_IRC_MAPA           = 1_800     # 30 min — IRC cambia con ETL
CACHE_SUSCEPTIBILIDAD    = 43_200    # 12 h   — scores ML estables
CACHE_FEN                = 43_200    # 12 h   — catálogo histórico
CACHE_VOLCANES           = 86_400    # 24 h   — estado volcánico
CACHE_ESCENARIO          = 300       # 5 min  — escenarios dinámicos
CACHE_TENDENCIA          = 3_600     # 1 h    — TimescaleDB CAG
CACHE_SENDAI_REPORT      = 86_400    # 24 h   — reporte anual
CACHE_EXPOSICION         = 3_600     # 1 h    — datos censales
CACHE_ALERTAS_RECIENTES  = 30        # 30 s   — EWS mutable
CACHE_RIESGO_PUNTO       = 600       # 10 min — función PL/pgSQL costosa
CACHE_RIESGO_RANKING     = 1_800     # 30 min — ranking IRC v9

# Prefijo global para todas las keys de GeoRiesgo
_KEY_PREFIX = "gr"


class GeoCache:
    """
    Caché Redis para respuestas GeoJSON de GeoRiesgo Perú v9.

    Degradación graceful: si Redis no está disponible al inicializar,
    `available` se fija en False y todas las operaciones son no-ops.
    Thread-safe vía asyncio — un único cliente async compartido.

    Attributes:
        available: bool — False si Redis no responde al conectar.

    Uso típico (en lifespan de FastAPI):
        cache = GeoCache()
        await cache.connect()
        app.state.cache = cache
        # Al cerrar:
        await cache.close()
    """

    def __init__(self) -> None:
        self._client: aioredis.Redis | None = None
        self.available: bool = False
        self._redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")

    async def connect(self) -> None:
        """
        Crea el cliente Redis y verifica conectividad con PING.
        Si falla, marca available=False (degradación graceful).

        Returns:
            None
        """
        try:
            self._client = aioredis.from_url(
                self._redis_url,
                encoding="utf-8",
                decode_responses=False,   # bytes para GeoJSON serializado
                socket_connect_timeout=3,
                socket_timeout=5,
                max_connections=20,
            )
            await self._client.ping()
            self.available = True
            logger.info("GeoCache: Redis conectado en %s", self._redis_url)
        except Exception as exc:
            self.available = False
            self._client = None
            logger.warning(
                "GeoCache: Redis no disponible (%s) — API funcionará sin caché",
                exc,
            )

    async def close(self) -> None:
        """
        Cierra la conexión Redis limpiamente al apagar la app.

        Returns:
            None
        """
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception:
                pass
            finally:
                self._client = None
                self.available = False
                logger.info("GeoCache: conexión Redis cerrada")

    async def get(self, key: str) -> bytes | None:
        """
        Retorna los bytes cacheados para la key dada, o None en miss/error.
        Silent fail: cualquier excepción Redis → return None.

        Args:
            key: clave Redis (prefijada por cache_geo / build_key)

        Returns:
            bytes con el cuerpo cacheado, o None si miss o Redis caído.
        """
        if not self.available or self._client is None:
            return None
        try:
            return await self._client.get(key)
        except Exception as exc:
            logger.warning("GeoCache.get error [%s]: %s", key, exc)
            return None

    async def set(self, key: str, value: bytes, ttl: int) -> None:
        """
        Guarda value con TTL en segundos.
        Silent fail: cualquier excepción Redis → log WARNING, continúa.

        Args:
            key:   clave Redis
            value: bytes a guardar (JSON/GeoJSON serializado con orjson)
            ttl:   tiempo de vida en segundos (usar constantes CACHE_*)

        Returns:
            None
        """
        if not self.available or self._client is None:
            return
        try:
            await self._client.setex(key, ttl, value)
        except Exception as exc:
            logger.warning("GeoCache.set error [%s]: %s", key, exc)

    async def invalidate_prefix(self, prefix: str) -> None:
        """
        Elimina todas las keys que empiezan con `prefix` via SCAN+DEL en
        batches de 100 para no bloquear el event loop de Redis.

        Args:
            prefix: prefijo de keys a eliminar (ej. "gr:GET:/api/v1/volcanes")

        Returns:
            None
        """
        if not self.available or self._client is None:
            return
        try:
            cursor = 0
            deleted = 0
            pattern = f"{prefix}*"
            while True:
                cursor, keys = await self._client.scan(
                    cursor=cursor, match=pattern, count=100
                )
                if keys:
                    await self._client.delete(*keys)
                    deleted += len(keys)
                if cursor == 0:
                    break
                # yield control al event loop entre batches
                await asyncio.sleep(0)
            if deleted:
                logger.info(
                    "GeoCache.invalidate_prefix: %d keys eliminadas (prefix=%s)",
                    deleted, prefix,
                )
        except Exception as exc:
            logger.warning(
                "GeoCache.invalidate_prefix error [prefix=%s]: %s", prefix, exc
            )

    async def keys_count(self) -> int:
        """
        Retorna el número total de keys en Redis (para diagnóstico).

        Returns:
            int — número de keys, o 0 si Redis no disponible.
        """
        if not self.available or self._client is None:
            return 0
        try:
            return await self._client.dbsize()
        except Exception:
            return 0

    def build_key(self, method: str, path: str, params: dict[str, Any]) -> str:
        """
        Construye una cache key determinista desde método HTTP, path y params.

        Key format:
            gr:<METHOD>:<path>:<k1=v1>:<k2=v2>...  (params ordenados)

        Args:
            method: método HTTP ("GET", "POST", ...)
            path:   URL path (ej. "/api/v1/volcanes")
            params: query params como dict

        Returns:
            str — cache key lista para usar en get/set
        """
        params_str = ":".join(
            f"{k}={v}" for k, v in sorted(params.items()) if v is not None
        )
        return f"{_KEY_PREFIX}:{method}:{path}:{params_str}"


# ── Instancia global ──────────────────────────────────────────────────────
# Se conecta en el lifespan de main.py: await geo_cache.connect()
geo_cache = GeoCache()


def cache_geo(ttl: int = 300) -> Callable:
    """
    Decorador async para endpoints FastAPI que retornan Response GeoJSON.

    Cache key:
        gr:<METHOD>:<path>:<k1=v1>:...  (query params ordenados)

    Comportamiento:
        HIT  → Response(bytes, media_type="application/geo+json",
                        headers={"X-Cache":"HIT","X-Cache-TTL":str(ttl)})
        MISS → ejecuta el endpoint, serializa con orjson, guarda en Redis,
               headers: {"X-Cache":"MISS","X-Cache-TTL":str(ttl)}
        Redis caído → ejecuta endpoint normalmente sin headers de caché.

    Nota: media_type correcto para GeoJSON es "application/geo+json"
    (RFC 7946 §12). "application/json" también es aceptado por clientes.

    Args:
        ttl: tiempo de vida en segundos (usar constantes CACHE_*)

    Returns:
        Callable — decorador para funciones async de endpoint FastAPI.

    Ejemplo:
        @router.get("/api/v1/volcanes")
        @cache_geo(ttl=CACHE_VOLCANES)
        async def get_volcanes(request: Request):
            ...
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Extraer request de args (FastAPI lo inyecta como kwarg o arg)
            request: Request | None = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            # Si no tenemos request o Redis no está disponible → pass-through
            if request is None or not geo_cache.available:
                result = await func(*args, **kwargs)
                return result

            # Construir cache key
            key = geo_cache.build_key(
                method=request.method,
                path=str(request.url.path),
                params=dict(request.query_params),
            )

            # Intento HIT
            cached = await geo_cache.get(key)
            if cached is not None:
                return Response(
                    content=cached,
                    media_type="application/geo+json",
                    headers={
                        "X-Cache":     "HIT",
                        "X-Cache-TTL": str(ttl),
                        "Cache-Control": f"public, max-age={ttl}",
                    },
                )

            # MISS: ejecutar endpoint
            result = await func(*args, **kwargs)

            # Serializar y guardar en Redis
            try:
                if isinstance(result, Response):
                    body = result.body
                elif isinstance(result, (dict, list)):
                    body = orjson.dumps(result)
                else:
                    # Tipo no cacheable → retornar sin caché
                    return result

                await geo_cache.set(key, body, ttl)

                return Response(
                    content=body,
                    media_type="application/geo+json",
                    headers={
                        "X-Cache":     "MISS",
                        "X-Cache-TTL": str(ttl),
                        "Cache-Control": f"public, max-age={ttl}",
                    },
                )
            except Exception as exc:
                logger.warning("cache_geo: error al serializar/guardar [%s]: %s", key, exc)
                return result

        return wrapper
    return decorator


def cache_json(ttl: int = 300) -> Callable:
    """
    Variante de cache_geo para endpoints que retornan JSON estándar
    (no GeoJSON). Media-type: "application/json".

    Args:
        ttl: tiempo de vida en segundos

    Returns:
        Callable — decorador async compatible con FastAPI.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            request: Request | None = kwargs.get("request")
            if request is None:
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

            if request is None or not geo_cache.available:
                return await func(*args, **kwargs)

            key = geo_cache.build_key(
                method=request.method,
                path=str(request.url.path),
                params=dict(request.query_params),
            )

            cached = await geo_cache.get(key)
            if cached is not None:
                return Response(
                    content=cached,
                    media_type="application/json",
                    headers={"X-Cache": "HIT", "X-Cache-TTL": str(ttl)},
                )

            result = await func(*args, **kwargs)

            try:
                if isinstance(result, Response):
                    body = result.body
                elif isinstance(result, (dict, list)):
                    body = orjson.dumps(result)
                else:
                    return result

                await geo_cache.set(key, body, ttl)

                return Response(
                    content=body,
                    media_type="application/json",
                    headers={"X-Cache": "MISS", "X-Cache-TTL": str(ttl)},
                )
            except Exception as exc:
                logger.warning("cache_json: error [%s]: %s", key, exc)
                return result

        return wrapper
    return decorator