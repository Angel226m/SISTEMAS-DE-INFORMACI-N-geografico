# ══════════════════════════════════════════════════════════════════════════
# GeoRiesgo Perú v9.0 — stac_catalog.py  (Bloque 7)
# Generación de rasters Cloud Optimized GeoTIFF (COG) y catálogo STAC
# para la capa de precipitación anual (CHIRPS 1981-2020).
#
# Flujo:
#   1. Lee ZONAS_PRECIPITACION desde BD (o datos hardcoded)
#   2. Interpola a grilla 0.1° con scipy.interpolate.griddata (cubic)
#   3. Genera GeoTIFF uint16 (precipitacion_anual_mm × 10) con rasterio
#   4. Convierte a COG con rio_cogeo.cogeo.cog_translate()
#   5. Sube a MinIO como objeto S3-compatible
#   6. Crea STAC Item con metadatos CHIRPS
#   7. Construye STAC Collection + Catalog con pystac
#
# Dependencias: rasterio, rio-cogeo, pystac, boto3 (MinIO S3-compatible)
#
# Fuentes:
#   CHIRPS v2.0 — Climate Hazards Group UCSB (Funk et al. 2015)
#   STAC spec 1.0.0 — https://stacspec.org
#   COG spec — https://www.cogeo.org
# ══════════════════════════════════════════════════════════════════════════

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# ── Configuración MinIO / S3 ──────────────────────────────────────────────
COLECCION      = "precipitacion_peru_1981_2020"
BUCKET         = os.getenv("MINIO_BUCKET",    "georiesgo-rasters")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "georiesgo")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD", "georiesgo_raster_2024")

# Ruta del objeto en MinIO
COG_OBJECT_KEY = "precipitacion/peru_chirps_1981_2020.tif"

# Bounding box del Perú (lon_min, lat_min, lon_max, lat_max)
BBOX_PERU = [-81.5, -18.5, -68.5, -0.0]

# Resolución de la grilla de interpolación (grados)
GRID_RESOLUTION = 0.10   # ~11 km en el ecuador

# Factor de escala para uint16 (precipitacion_mm × SCALE → uint16)
SCALE_FACTOR = 10        # almacena hasta 6553.5 mm/año

# Datos hardcoded de precipitación por zona climática (SENAMHI + CHIRPS)
# Fuente: CHIRPS v2.0 promedio 1981-2020
# Fuente: SENAMHI Atlas Climático del Perú 2021
# Formato: {nombre, lon_centroide, lat_centroide, precipitacion_anual_mm}
ZONAS_CHIRPS_FALLBACK: list[dict[str, Any]] = [
    # Costa norte — aridez extrema excepto durante FEN
    {"nombre": "Costa Norte Árida",       "lon": -80.6, "lat":  -5.2, "precip": 38.0},
    {"nombre": "Costa Norte Semiárida",   "lon": -79.9, "lat":  -7.5, "precip": 20.0},
    # Costa central y sur
    {"nombre": "Costa Central",           "lon": -77.1, "lat": -12.0, "precip": 10.0},
    {"nombre": "Costa Sur Hiper-árida",   "lon": -75.5, "lat": -15.8, "precip":  2.0},
    {"nombre": "Costa Extremo Sur",       "lon": -70.5, "lat": -17.5, "precip":  4.0},
    # Sierra norte
    {"nombre": "Sierra Norte Húmeda",     "lon": -78.5, "lat":  -5.8, "precip": 850.0},
    {"nombre": "Sierra Norte Alta",       "lon": -77.8, "lat":  -7.2, "precip": 650.0},
    # Sierra central
    {"nombre": "Sierra Central",          "lon": -76.0, "lat": -11.5, "precip": 700.0},
    {"nombre": "Valle Interandino",       "lon": -74.5, "lat": -13.5, "precip": 650.0},
    # Sierra sur
    {"nombre": "Sierra Sur Cusco",        "lon": -71.9, "lat": -13.5, "precip": 750.0},
    {"nombre": "Sierra Sur Puno",         "lon": -70.5, "lat": -15.0, "precip": 620.0},
    {"nombre": "Altiplano",               "lon": -69.8, "lat": -16.2, "precip": 550.0},
    # Vertiente oriental (yunga-selva alta)
    {"nombre": "Yunga Oriental Norte",    "lon": -77.5, "lat":  -6.5, "precip": 1800.0},
    {"nombre": "Yunga Oriental Central",  "lon": -75.5, "lat": -10.5, "precip": 2200.0},
    {"nombre": "Yunga Oriental Sur",      "lon": -73.0, "lat": -13.0, "precip": 1500.0},
    # Selva alta y baja
    {"nombre": "Selva Alta Norte",        "lon": -76.0, "lat":  -5.0, "precip": 2500.0},
    {"nombre": "Selva Alta Central",      "lon": -74.5, "lat":  -9.5, "precip": 2800.0},
    {"nombre": "Selva Baja Norte",        "lon": -74.0, "lat":  -4.0, "precip": 2800.0},
    {"nombre": "Selva Baja Central",      "lon": -73.5, "lat":  -6.5, "precip": 2600.0},
    {"nombre": "Selva Baja Sur",          "lon": -71.5, "lat": -11.5, "precip": 2200.0},
    {"nombre": "Selva Baja Madre de Dios","lon": -70.5, "lat": -12.8, "precip": 2100.0},
    {"nombre": "Ucayali",                 "lon": -73.0, "lat":  -9.5, "precip": 1900.0},
]


def _get_s3_client():
    """
    Crea un cliente boto3 configurado para MinIO S3-compatible.

    Returns:
        boto3.client — cliente S3 apuntando a MinIO

    Raises:
        ImportError: si boto3 no está instalado
        Exception:  si MinIO no responde
    """
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


async def ensure_bucket(s3_client: Any) -> None:
    """
    Verifica que el bucket exista en MinIO; lo crea si no existe.

    Args:
        s3_client: cliente boto3 apuntando a MinIO

    Returns:
        None
    """
    from botocore.exceptions import ClientError

    try:
        s3_client.head_bucket(Bucket=BUCKET)
        logger.info("stac_catalog: bucket '%s' ya existe", BUCKET)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=BUCKET)
            logger.info("stac_catalog: bucket '%s' creado", BUCKET)
        else:
            raise


def zonas_to_grid(
    zonas: list[dict[str, Any]],
    resolution: float = GRID_RESOLUTION,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Interpola puntos de precipitación de zonas climáticas a una grilla regular.

    Usa scipy.interpolate.griddata con método 'cubic' para suavizar
    la transición entre zonas. Para puntos fuera del convex hull,
    hace fallback a 'nearest'.

    Args:
        zonas:      lista de dicts con 'lon', 'lat', 'precip'
        resolution: espaciado de la grilla en grados (default 0.1°)

    Returns:
        tuple (lons_grid, lats_grid, precip_grid) — arrays 2D NumPy.
        precip_grid en mm/año como float32.
    """
    from scipy.interpolate import griddata

    lons   = np.array([z["lon"] for z in zonas], dtype=np.float64)
    lats   = np.array([z["lat"] for z in zonas], dtype=np.float64)
    precip = np.array([z["precip"] for z in zonas], dtype=np.float64)
    points = np.column_stack([lons, lats])

    # Crear grilla regular sobre bbox de Perú
    lon_grid = np.arange(BBOX_PERU[0], BBOX_PERU[2] + resolution, resolution)
    lat_grid = np.arange(BBOX_PERU[1], BBOX_PERU[3] + resolution, resolution)
    lons_mg, lats_mg = np.meshgrid(lon_grid, lat_grid)
    grid_pts = np.column_stack([lons_mg.ravel(), lats_mg.ravel()])

    # Interpolación cúbica (scipy.interpolate.griddata)
    precip_cubic   = griddata(points, precip, grid_pts, method="cubic")
    precip_nearest = griddata(points, precip, grid_pts, method="nearest")

    # Rellenar NaN del cúbico con nearest (áreas fuera del convex hull)
    nan_mask = np.isnan(precip_cubic)
    precip_cubic[nan_mask] = precip_nearest[nan_mask]

    # Clipping: no puede haber valores negativos
    precip_cubic = np.clip(precip_cubic, 0.0, 8000.0).astype(np.float32)
    precip_grid  = precip_cubic.reshape(lats_mg.shape)

    return lons_mg.astype(np.float32), lats_mg.astype(np.float32), precip_grid


def grid_to_cog(
    precip_grid: np.ndarray,
    output_path: str,
    resolution: float = GRID_RESOLUTION,
) -> str:
    """
    Convierte la grilla de precipitación a Cloud Optimized GeoTIFF (COG).

    Pipeline:
        1. Crea GeoTIFF regular uint16 (precip × SCALE_FACTOR) con rasterio
        2. Convierte a COG con rio_cogeo.cogeo.cog_translate()
           Opciones COG: overview levels, predictor=2, compress=deflate

    Args:
        precip_grid: array 2D de precipitación (mm/año, float32)
        output_path: ruta de salida para el archivo COG
        resolution:  resolución en grados de la grilla

    Returns:
        str — ruta al archivo COG generado

    References:
        COG spec — https://www.cogeo.org
        rasterio docs — https://rasterio.readthedocs.io
        rio-cogeo 3.6 — https://cogeotiff.github.io/rio-cogeo/
    """
    import rasterio
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    nrows, ncols = precip_grid.shape

    # Transformación afín desde bbox
    transform = from_bounds(
        west=BBOX_PERU[0], south=BBOX_PERU[1],
        east=BBOX_PERU[2], north=BBOX_PERU[3],
        width=ncols, height=nrows,
    )

    # Escalar a uint16 y crear GeoTIFF temporal
    with tempfile.NamedTemporaryFile(suffix="_precip.tif", delete=False) as tmp:
        tmp_path = tmp.name

    data_u16 = (precip_grid * SCALE_FACTOR).astype(np.uint16)
    # Voltear eje Y: rasterio espera norte arriba (lat descendente en rows)
    data_u16 = np.flipud(data_u16)

    with rasterio.open(
        tmp_path,
        "w",
        driver="GTiff",
        height=nrows,
        width=ncols,
        count=1,
        dtype=rasterio.uint16,
        crs=CRS.from_epsg(4326),
        transform=transform,
        nodata=65535,
    ) as dst:
        dst.write(data_u16, 1)
        dst.update_tags(
            SCALE_FACTOR=str(SCALE_FACTOR),
            UNITS="mm_anio_x10",
            FUENTE="CHIRPS v2.0 1981-2020 + SENAMHI Atlas Climático 2021",
            METODOLOGIA="Interpolación cúbica griddata sobre 22 zonas climáticas",
            FECHA_GENERACION=datetime.now(timezone.utc).isoformat(),
        )

    # Convertir a COG con deflate + predictor 2 (horizontal differencing)
    cog_profile = cog_profiles.get("deflate")
    cog_profile.update({"predictor": 2, "zlevel": 6})

    cog_translate(
        tmp_path,
        output_path,
        cog_profile,
        overview_level=4,
        overview_resampling="average",
        quiet=True,
    )

    # Limpiar temporal
    try:
        os.unlink(tmp_path)
    except OSError:
        pass

    logger.info("stac_catalog: COG generado en %s", output_path)
    return output_path


async def upload_to_minio(local_path: str, s3_client: Any) -> str:
    """
    Sube el archivo COG a MinIO y retorna la URL pública del objeto.

    Args:
        local_path: ruta local del archivo COG a subir
        s3_client:  cliente boto3 apuntando a MinIO

    Returns:
        str — URL del objeto en MinIO (http://minio:9000/bucket/key)
    """
    from botocore.exceptions import ClientError

    try:
        with open(local_path, "rb") as f:
            s3_client.upload_fileobj(
                f,
                BUCKET,
                COG_OBJECT_KEY,
                ExtraArgs={"ContentType": "image/tiff"},
            )
        url = f"{MINIO_ENDPOINT}/{BUCKET}/{COG_OBJECT_KEY}"
        logger.info("stac_catalog: COG subido a %s", url)
        return url
    except ClientError as exc:
        logger.error("stac_catalog: error al subir a MinIO — %s", exc)
        raise


def create_stac_item(
    cog_url: str,
    bbox: list[float] | None = None,
) -> Any:
    """
    Crea un STAC Item para el raster de precipitación CHIRPS 1981-2020.

    Campos STAC 1.0.0 incluidos:
        id, type, bbox, geometry, properties (datetime, title, description),
        assets (data: COG, thumbnail), links.

    Args:
        cog_url: URL del objeto COG en MinIO
        bbox:    [west, south, east, north] (default BBOX_PERU)

    Returns:
        pystac.Item

    References:
        STAC spec 1.0.0 — https://stacspec.org/en/about/stac-spec/
        CHIRPS — Funk et al. 2015 Sci. Data doi:10.1038/sdata.2015.66
    """
    import pystac
    from pystac.extensions.eo import EOExtension
    from shapely.geometry import box, mapping

    if bbox is None:
        bbox = BBOX_PERU

    # Geometría del ítem como GeoJSON Polygon
    geom = mapping(box(bbox[0], bbox[1], bbox[2], bbox[3]))

    item = pystac.Item(
        id=f"chirps_1981_2020_anual_peru",
        geometry=geom,
        bbox=bbox,
        datetime=datetime(2020, 12, 31, tzinfo=timezone.utc),
        properties={
            "title":         "Precipitación Anual Perú — CHIRPS 1981-2020",
            "description":   (
                "Precipitación anual promedio 1981-2020 para el Perú. "
                "Interpolado a grilla 0.1° desde 22 zonas climáticas SENAMHI. "
                "Fuente primaria: CHIRPS v2.0 (Funk et al. 2015)."
            ),
            "start_datetime": "1981-01-01T00:00:00Z",
            "end_datetime":   "2020-12-31T00:00:00Z",
            "platform":       "CHIRPS v2.0",
            "instruments":    ["multi-source"],
            "gsd":            11000,       # ~11 km en el ecuador
            "fuente":         "CHIRPS v2.0 1981-2020 + SENAMHI Atlas Climático 2021",
            "metodologia":    "griddata cubic interpolation — 22 zonas climáticas",
            "unidades":       "mm/año (valor × 10 almacenado como uint16)",
            "scale_factor":   SCALE_FACTOR,
            "nodata":         65535,
            "referencia":     "Funk et al. 2015 Sci. Data doi:10.1038/sdata.2015.66",
        },
    )

    # Asset principal — COG
    item.add_asset(
        "data",
        pystac.Asset(
            href=cog_url,
            media_type=pystac.MediaType.COG,
            title="Precipitación Anual CHIRPS 1981-2020 (COG)",
            roles=["data"],
            extra_fields={
                "type":          "image/tiff; application=geotiff; profile=cloud-optimized",
                "scale_factor":  SCALE_FACTOR,
                "nodata":        65535,
                "description":   "uint16 = precipitacion_mm_anio × 10",
            },
        ),
    )

    return item


def create_stac_collection(item: Any) -> Any:
    """
    Crea una STAC Collection para la colección de precipitación.

    Args:
        item: pystac.Item con el COG de precipitación

    Returns:
        pystac.Collection
    """
    import pystac

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[BBOX_PERU]),
        temporal=pystac.TemporalExtent(
            intervals=[[
                datetime(1981, 1, 1, tzinfo=timezone.utc),
                datetime(2020, 12, 31, tzinfo=timezone.utc),
            ]]
        ),
    )

    collection = pystac.Collection(
        id=COLECCION,
        title="Precipitación Perú 1981-2020 (CHIRPS)",
        description=(
            "Colección de rasters de precipitación anual para el Perú, "
            "derivados de CHIRPS v2.0 y el Atlas Climático SENAMHI 2021. "
            "Resolución espacial: 0.1° (~11 km). "
            "Período: 1981-2020 (climatología base)."
        ),
        extent=extent,
        license="proprietary",
        providers=[
            pystac.Provider(
                name="Climate Hazards Group UCSB (CHIRPS)",
                roles=["producer", "licensor"],
                url="https://www.chc.ucsb.edu/data/chirps",
            ),
            pystac.Provider(
                name="SENAMHI Perú",
                roles=["producer"],
                url="https://www.senamhi.gob.pe",
            ),
            pystac.Provider(
                name="GeoRiesgo Perú v9",
                roles=["processor", "host"],
                url="http://localhost:8000",
            ),
        ],
        extra_fields={
            "scale_factor": SCALE_FACTOR,
            "unidades":     "mm/año",
            "nodata":       65535,
        },
    )
    collection.add_item(item)
    return collection


async def build_catalog(
    conn: Any,
    s3_client: Any,
) -> Any:
    """
    Flujo completo: BD → grilla → COG → MinIO → STAC Catalog.

    Pipeline:
        1. Lee zonas de precipitación desde BD (o fallback hardcoded)
        2. Interpola a grilla 0.1° con zonas_to_grid()
        3. Genera COG con grid_to_cog()
        4. Sube a MinIO con upload_to_minio()
        5. Crea STAC Item → Collection → Catalog

    Args:
        conn:      conexión asyncpg a PostgreSQL (puede ser None para usar fallback)
        s3_client: cliente boto3 apuntando a MinIO

    Returns:
        pystac.Catalog — catálogo STAC con la colección de precipitación

    References:
        pystac 1.9 docs — https://pystac.readthedocs.io
    """
    import pystac

    # 1. Obtener datos de precipitación
    zonas: list[dict[str, Any]] = []
    if conn is not None:
        try:
            rows = await conn.fetch(
                """
                SELECT nombre,
                       ST_X(ST_Centroid(geom)) AS lon,
                       ST_Y(ST_Centroid(geom)) AS lat,
                       precipitacion_anual_mm  AS precip
                FROM zonas_precipitacion
                WHERE precipitacion_anual_mm IS NOT NULL
                  AND geom IS NOT NULL
                ORDER BY nombre
                """
            )
            zonas = [dict(r) for r in rows]
            logger.info("stac_catalog: %d zonas cargadas desde BD", len(zonas))
        except Exception as exc:
            logger.warning("stac_catalog: error leyendo BD (%s) — usando fallback", exc)

    if not zonas:
        logger.info("stac_catalog: usando datos CHIRPS hardcoded (%d zonas)", len(ZONAS_CHIRPS_FALLBACK))
        zonas = ZONAS_CHIRPS_FALLBACK

    # 2. Interpolación a grilla
    logger.info("stac_catalog: interpolando %d zonas a grilla %.2f°", len(zonas), GRID_RESOLUTION)
    _, _, precip_grid = zonas_to_grid(zonas, GRID_RESOLUTION)

    # 3. Generar COG en directorio temporal
    with tempfile.TemporaryDirectory() as tmpdir:
        cog_path = str(Path(tmpdir) / "peru_chirps_1981_2020.tif")
        grid_to_cog(precip_grid, cog_path, GRID_RESOLUTION)

        # 4. Asegurar bucket y subir
        await ensure_bucket(s3_client)
        cog_url = await upload_to_minio(cog_path, s3_client)

    # 5. Construir catálogo STAC
    item       = create_stac_item(cog_url)
    collection = create_stac_collection(item)

    catalog = pystac.Catalog(
        id="georiesgo-peru-v9",
        title="GeoRiesgo Perú v9.0 — Catálogo de Datos Espaciales",
        description=(
            "Catálogo STAC 1.0.0 de GeoRiesgo Perú v9.0. "
            "Contiene capas de precipitación (CHIRPS), susceptibilidad ML, "
            "y amenazas múltiples para el Perú."
        ),
    )
    catalog.add_child(collection)

    logger.info(
        "stac_catalog: catálogo STAC construido — colección '%s', 1 item",
        COLECCION,
    )
    return catalog


async def read_raster_point(
    lon: float,
    lat: float,
    s3_client: Any,
) -> dict[str, Any]:
    """
    Lee el valor de precipitación en un punto específico (lon, lat)
    haciendo window read del COG via rasterio (S3/HTTP range request).

    Args:
        lon:       longitud decimal
        lat:       latitud decimal
        s3_client: cliente boto3 (para construir la URL firmada si necesario)

    Returns:
        dict con lon, lat, precipitacion_anual_mm, fuente, metodologia

    Raises:
        RuntimeError: si MinIO no está disponible o el COG no existe.
    """
    import rasterio
    from rasterio.windows import from_bounds

    cog_url = f"{MINIO_ENDPOINT}/{BUCKET}/{COG_OBJECT_KEY}"

    try:
        with rasterio.Env(AWS_ACCESS_KEY_ID=MINIO_USER,
                         AWS_SECRET_ACCESS_KEY=MINIO_PASS,
                         AWS_S3_ENDPOINT=MINIO_ENDPOINT.replace("http://", ""),
                         GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
                         CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif"):
            with rasterio.open(f"/vsicurl/{cog_url}") as src:
                # Window de 1×1 pixel alrededor del punto
                window = from_bounds(
                    lon - 0.05, lat - 0.05,
                    lon + 0.05, lat + 0.05,
                    src.transform,
                )
                data = src.read(1, window=window)
                if data.size == 0 or data[0, 0] == 65535:
                    return {
                        "lon": lon, "lat": lat,
                        "precipitacion_anual_mm": None,
                        "nota": "Punto fuera del área de cobertura o sin datos",
                        "fuente": "CHIRPS v2.0 1981-2020",
                    }
                valor_mm = float(data.mean()) / SCALE_FACTOR
    except Exception as exc:
        logger.error("stac_catalog.read_raster_point: error — %s", exc)
        raise RuntimeError(f"MinIO/COG no disponible: {exc}") from exc

    return {
        "lon": lon,
        "lat": lat,
        "precipitacion_anual_mm": round(valor_mm, 1),
        "fuente":      "CHIRPS v2.0 1981-2020 + SENAMHI Atlas Climático 2021",
        "metodologia": "Window read COG — grilla 0.1° interpolada",
        "nota":        "Valor climatológico 1981-2020 (no tiempo real)",
    }


async def get_catalog_metadata() -> dict[str, Any]:
    """
    Retorna metadata del catálogo STAC sin acceder a MinIO.
    Útil para el endpoint GET /api/v1/raster/catalogo.

    Returns:
        dict con colecciones, descripción, bbox, temporalidad, items.
    """
    return {
        "id":          "georiesgo-peru-v9",
        "titulo":      "GeoRiesgo Perú v9.0 — Catálogo STAC",
        "version_stac": "1.0.0",
        "colecciones": [
            {
                "id":          COLECCION,
                "titulo":      "Precipitación Anual Perú 1981-2020 (CHIRPS)",
                "descripcion": (
                    "Climatología de precipitación anual 1981-2020. "
                    "Resolución: 0.1° (~11 km). Formato: COG uint16."
                ),
                "bbox":        BBOX_PERU,
                "periodo":     {"inicio": "1981-01-01", "fin": "2020-12-31"},
                "items":       1,
                "fuente":      "CHIRPS v2.0 (Funk et al. 2015) + SENAMHI 2021",
                "object_key":  COG_OBJECT_KEY,
                "bucket":      BUCKET,
                "endpoint":    MINIO_ENDPOINT,
            }
        ],
        "proveedores": [
            "Climate Hazards Group UCSB (CHIRPS v2.0)",
            "SENAMHI Perú — Atlas Climático 2021",
            "GeoRiesgo Perú v9.0",
        ],
        "referencias": [
            "Funk et al. 2015 Sci. Data doi:10.1038/sdata.2015.66",
            "SENAMHI Atlas Climático del Perú 2021",
            "STAC spec 1.0.0 — https://stacspec.org",
        ],
    }