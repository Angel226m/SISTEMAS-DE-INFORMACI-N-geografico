-- ══════════════════════════════════════════════════════════
-- GeoRiesgo Perú — Esquema PostGIS v4.0
-- Cobertura nacional con datos de riesgo multi-amenaza
-- ══════════════════════════════════════════════════════════

-- Extensiones
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- búsqueda de texto

-- ═══════════════════════════════════════════════════════════
--  SISMOS
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sismos (
    id               BIGSERIAL PRIMARY KEY,
    usgs_id          TEXT UNIQUE NOT NULL,
    geom             GEOMETRY(Point, 4326) NOT NULL,
    magnitud         NUMERIC(4,1) NOT NULL CHECK (magnitud >= 0 AND magnitud <= 10),
    profundidad_km   NUMERIC(7,2) NOT NULL CHECK (profundidad_km >= 0),
    tipo_profundidad TEXT NOT NULL CHECK (tipo_profundidad IN ('superficial','intermedio','profundo')),
    fecha            DATE NOT NULL,
    hora_utc         TIMESTAMPTZ,
    lugar            TEXT,
    region           TEXT,          -- departamento aproximado
    tipo_magnitud    TEXT,          -- ML, Mw, mb, etc.
    estado           TEXT DEFAULT 'reviewed',
    fuente           TEXT DEFAULT 'USGS',
    creado_en        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sismos_geom       ON sismos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sismos_fecha      ON sismos(fecha DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_magnitud   ON sismos(magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_profund    ON sismos(profundidad_km);
CREATE INDEX IF NOT EXISTS idx_sismos_tipo_prof  ON sismos(tipo_profundidad);
CREATE INDEX IF NOT EXISTS idx_sismos_fecha_mag  ON sismos(fecha DESC, magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_region     ON sismos(region);

-- ═══════════════════════════════════════════════════════════
--  UNIDADES ADMINISTRATIVAS (departamentos, provincias, distritos)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS departamentos (
    id           SERIAL PRIMARY KEY,
    ubigeo       TEXT UNIQUE,       -- código INEI 2 dígitos
    nombre       TEXT NOT NULL,
    geom         GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo SMALLINT DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    area_km2     NUMERIC(12,3),
    capital      TEXT,
    fuente       TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dep_geom ON departamentos USING GIST(geom);

CREATE TABLE IF NOT EXISTS provincias (
    id           SERIAL PRIMARY KEY,
    ubigeo       TEXT UNIQUE,       -- código INEI 4 dígitos
    nombre       TEXT NOT NULL,
    departamento TEXT,
    geom         GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo SMALLINT DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    area_km2     NUMERIC(12,3),
    fuente       TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prov_geom ON provincias USING GIST(geom);

CREATE TABLE IF NOT EXISTS distritos (
    id           SERIAL PRIMARY KEY,
    ubigeo       TEXT UNIQUE,       -- código INEI 6 dígitos
    nombre       TEXT NOT NULL,
    provincia    TEXT,
    departamento TEXT DEFAULT 'Ica',
    geom         GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo SMALLINT NOT NULL DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    poblacion    INTEGER,
    area_km2     NUMERIC(10,3),
    fuente       TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_distritos_geom    ON distritos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_distritos_nombre  ON distritos USING GIN(nombre gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_distritos_prov    ON distritos(provincia);
CREATE INDEX IF NOT EXISTS idx_distritos_dep     ON distritos(departamento);

-- ═══════════════════════════════════════════════════════════
--  FALLAS GEOLÓGICAS (cobertura nacional)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fallas (
    id             SERIAL PRIMARY KEY,
    ingemmet_id    TEXT,
    nombre         TEXT NOT NULL,
    nombre_alt     TEXT,                -- nombre alternativo o en inglés
    geom           GEOMETRY(MultiLineString, 4326),
    activa         BOOLEAN DEFAULT TRUE,
    tipo           TEXT,                -- Neotectónica, Inversa, Normal, Transcurrente, Subducción
    mecanismo      TEXT,                -- compresivo, extensional, transcurrente
    longitud_km    NUMERIC(10,2),
    magnitud_max   NUMERIC(4,1),        -- sismo máximo histórico asociado
    profundidad_tipo TEXT DEFAULT 'superficial',
    region         TEXT,                -- departamento/región donde se ubica
    fuente         TEXT DEFAULT 'INGEMMET/IGP',
    referencia     TEXT,                -- cita bibliográfica
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_fallas_geom   ON fallas USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_fallas_activa ON fallas(activa);
CREATE INDEX IF NOT EXISTS idx_fallas_tipo   ON fallas(tipo);
CREATE INDEX IF NOT EXISTS idx_fallas_region ON fallas(region);

-- ═══════════════════════════════════════════════════════════
--  ZONAS DE INUNDACIÓN
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS zonas_inundables (
    id               SERIAL PRIMARY KEY,
    nombre           TEXT NOT NULL,
    geom             GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo     SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    tipo_inundacion  TEXT DEFAULT 'fluvial',  -- fluvial, pluvial, costero, tsunami, aluvion
    periodo_retorno  INTEGER,                 -- años
    profundidad_max_m NUMERIC(6,2),           -- lámina de agua máxima estimada
    velocidad_ms     NUMERIC(5,2),            -- velocidad flujo m/s
    cuenca           TEXT,                    -- nombre de cuenca hidrográfica
    region           TEXT,
    fuente           TEXT DEFAULT 'ANA/CENEPRED',
    actualizado_en   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_inundables_geom  ON zonas_inundables USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_inundables_riesgo ON zonas_inundables(nivel_riesgo DESC);
CREATE INDEX IF NOT EXISTS idx_inundables_tipo  ON zonas_inundables(tipo_inundacion);

-- ═══════════════════════════════════════════════════════════
--  DESLIZAMIENTOS Y REMOCIÓN EN MASA
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS deslizamientos (
    id           SERIAL PRIMARY KEY,
    nombre       TEXT,
    geom         GEOMETRY(MultiPolygon, 4326),
    tipo         TEXT,   -- deslizamiento, huayco, derrumbe, flujo_detritico, reptacion
    nivel_riesgo SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    area_km2     NUMERIC(10,4),
    region       TEXT,
    activo       BOOLEAN DEFAULT TRUE,
    fuente       TEXT DEFAULT 'INGEMMET/CENEPRED',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_desl_geom ON deslizamientos USING GIST(geom);

-- ═══════════════════════════════════════════════════════════
--  INFRAESTRUCTURA CRÍTICA
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS infraestructura (
    id          SERIAL PRIMARY KEY,
    osm_id      BIGINT,
    nombre      TEXT NOT NULL,
    tipo        TEXT NOT NULL,    -- hospital, clinica, escuela, bomberos, policia,
                                  -- aeropuerto, puerto, puente, central_electrica,
                                  -- planta_agua, refugio
    geom        GEOMETRY(Point, 4326),
    criticidad  SMALLINT DEFAULT 3 CHECK (criticidad BETWEEN 1 AND 5),
    capacidad   INTEGER,          -- camas, alumnos, etc.
    estado      TEXT DEFAULT 'operativo',
    region      TEXT,
    distrito    TEXT,
    telefono    TEXT,
    fuente      TEXT,
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_infra_geom      ON infraestructura USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_infra_tipo      ON infraestructura(tipo);
CREATE INDEX IF NOT EXISTS idx_infra_critic    ON infraestructura(criticidad DESC);
CREATE INDEX IF NOT EXISTS idx_infra_region    ON infraestructura(region);
CREATE INDEX IF NOT EXISTS idx_infra_nombre    ON infraestructura USING GIN(nombre gin_trgm_ops);

-- ═══════════════════════════════════════════════════════════
--  ZONAS DE TSUNAMI (PREDES/IGP)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS zonas_tsunami (
    id              SERIAL PRIMARY KEY,
    nombre          TEXT NOT NULL,
    geom            GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo    SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    altura_ola_m    NUMERIC(6,2),    -- altura máxima estimada en metros
    tiempo_arribo_min INTEGER,        -- tiempo estimado de arribo en minutos
    periodo_retorno INTEGER,          -- años
    region          TEXT,
    fuente          TEXT DEFAULT 'PREDES/IGP',
    actualizado_en  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tsunami_geom ON zonas_tsunami USING GIST(geom);

-- ═══════════════════════════════════════════════════════════
--  ESTACIONES SÍSMICAS Y METEOROLÓGICAS
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS estaciones (
    id          SERIAL PRIMARY KEY,
    codigo      TEXT UNIQUE,
    nombre      TEXT NOT NULL,
    tipo        TEXT NOT NULL,   -- sismica, meteorologica, hidrometrica, mareografica
    geom        GEOMETRY(Point, 4326),
    altitud_m   NUMERIC(8,2),
    activa      BOOLEAN DEFAULT TRUE,
    institucion TEXT,            -- IGP, SENAMHI, ANA
    region      TEXT,
    red         TEXT,            -- nombre de la red de monitoreo
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_estaciones_geom ON estaciones USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_estaciones_tipo ON estaciones(tipo);

-- ═══════════════════════════════════════════════════════════
--  LOG DE SINCRONIZACIÓN
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sync_log (
    id          SERIAL PRIMARY KEY,
    fuente      TEXT NOT NULL,
    tabla       TEXT NOT NULL,
    registros   INTEGER DEFAULT 0,
    estado      TEXT DEFAULT 'ok',
    detalle     TEXT,
    duracion_s  NUMERIC(10,2),
    inicio      TIMESTAMPTZ DEFAULT NOW(),
    fin         TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_synclog_fuente ON sync_log(fuente, fin DESC);

-- ═══════════════════════════════════════════════════════════
--  VISTAS MATERIALIZADAS (performance para mapas)
-- ═══════════════════════════════════════════════════════════

-- Vista: estadísticas sísmicas por año
CREATE OR REPLACE VIEW v_estadisticas_anio AS
SELECT
    EXTRACT(YEAR FROM fecha)::INTEGER          AS anio,
    COUNT(*)                                    AS cantidad,
    ROUND(MAX(magnitud)::NUMERIC, 1)            AS magnitud_max,
    ROUND(AVG(magnitud)::NUMERIC, 2)            AS magnitud_prom,
    ROUND(MIN(magnitud)::NUMERIC, 1)            AS magnitud_min,
    COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial')  AS superficiales,
    COUNT(*) FILTER (WHERE tipo_profundidad = 'intermedio')   AS intermedios,
    COUNT(*) FILTER (WHERE tipo_profundidad = 'profundo')     AS profundos,
    COUNT(*) FILTER (WHERE magnitud >= 6.0)     AS m6_plus,
    COUNT(*) FILTER (WHERE magnitud >= 7.0)     AS m7_plus
FROM sismos
GROUP BY EXTRACT(YEAR FROM fecha)
ORDER BY anio;

-- Vista: sismos por distrito (join espacial)
CREATE OR REPLACE VIEW v_sismos_por_distrito AS
SELECT
    d.nombre                                     AS distrito,
    d.provincia,
    d.departamento,
    d.nivel_riesgo,
    COUNT(s.id)                                  AS total_sismos,
    ROUND(MAX(s.magnitud)::NUMERIC, 1)           AS max_magnitud,
    ROUND(AVG(s.magnitud)::NUMERIC, 2)           AS avg_magnitud,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 5.0) AS m5_plus
FROM distritos d
LEFT JOIN sismos s ON ST_Within(s.geom, d.geom)
GROUP BY d.nombre, d.provincia, d.departamento, d.nivel_riesgo
ORDER BY total_sismos DESC;

-- Vista materializada: grid de densidad sísmica (para heatmap)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_heatmap_sismos AS
SELECT
    ST_SnapToGrid(geom, 0.1) AS geom_grid,
    COUNT(*) AS cantidad,
    ROUND(AVG(magnitud)::NUMERIC, 2) AS magnitud_prom,
    ROUND(MAX(magnitud)::NUMERIC, 1) AS magnitud_max
FROM sismos
WHERE magnitud >= 3.0
GROUP BY ST_SnapToGrid(geom, 0.1)
HAVING COUNT(*) > 0;

CREATE INDEX IF NOT EXISTS idx_mv_heatmap_geom ON mv_heatmap_sismos USING GIST(geom_grid);

-- ═══════════════════════════════════════════════════════════
--  FUNCIONES UTILITARIAS
-- ═══════════════════════════════════════════════════════════

-- Función: sismos en radio de X km desde un punto
CREATE OR REPLACE FUNCTION f_sismos_cercanos(
    p_lon FLOAT, p_lat FLOAT, p_radio_km INT DEFAULT 50,
    p_mag_min FLOAT DEFAULT 3.0, p_limit INT DEFAULT 100
)
RETURNS TABLE (
    usgs_id TEXT, magnitud NUMERIC, profundidad_km NUMERIC,
    tipo_profundidad TEXT, fecha DATE, lugar TEXT,
    distancia_km NUMERIC
) AS $$
    SELECT
        usgs_id, magnitud, profundidad_km, tipo_profundidad, fecha, lugar,
        ROUND((ST_Distance(geom::geography,
               ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography) / 1000)::NUMERIC, 1)
               AS distancia_km
    FROM sismos
    WHERE magnitud >= p_mag_min
      AND ST_DWithin(geom::geography,
                     ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography,
                     p_radio_km * 1000)
    ORDER BY distancia_km ASC
    LIMIT p_limit;
$$ LANGUAGE SQL STABLE;

-- Confirmación
DO $$ BEGIN
    RAISE NOTICE '✅ Esquema GeoRiesgo Perú v4.0 inicializado — %', NOW();
END $$;