-- ══════════════════════════════════════════════════════════════════════════
-- GeoRiesgo Perú — Esquema PostGIS/TimescaleDB v9.0  ENTERPRISE
--
-- FUENTE ÚNICA DE VERDAD — reescritura completa integrando v8.0 + v9.0
--
-- Changesets v9.0 (sobre v8.0):
--   🆕 volcanes                — 20 volcanes INGEMMET/OVI-IGP 2021
--   🆕 susceptibilidad_ml     — scores XGBoost+SHAP por punto de grilla
--   🆕 modelo_metadata        — métricas de entrenamiento por amenaza
--   🆕 alertas_rt             — EWS multi-hazard + CAP v1.2 + EW4All
--   🆕 exposicion_distritos   — GEM 2023 + INEI 2017 + MIDIS SISFOH 2022
--   🆕 lecturas_estaciones    — hypertable TimescaleDB series temporales
--   🆕 sendai_snapshots       — métricas proxy Sendai Framework 2015-2030
--   🆕 riesgo_percentiles     — MV para Target B/C Sendai
--   🆕 sismos_mensual         — CAG TimescaleDB (fallback a GROUP BY)
--   🆕 distritos.*            — peligro_volcan, peligro_sequia,
--                               indice_riesgo_v9, irc_v9_p10/p90,
--                               factor_cascada
--   🆕 Índices SP-GIST/BRIN   — Crunchy Data 2025 spatial indexes
--   🆕 TimescaleDB setup      — hypertable sismos + compresión
--
-- Changesets v8.0 (mantenidos intactos):
--   ✅ zonas_precipitacion    — 22 zonas climáticas SENAMHI/CHIRPS
--   ✅ eventos_fen            — catálogo ENSO 1957-2024 NOAA-CPC
--   ✅ f_riesgo_construccion  — incluye amplificación PI×FEN
--   ✅ mv_riesgo_construccion — IRC v8 con FEN + clasificación suelo
--   ✅ zona_sismica_departamento — referencia NTE E.030-2018
--
-- Fuentes: USGS·IGP·INGEMMET·INEI·GADM·ANA·CENEPRED·SENAMHI·CHIRPS
--          NOAA-CPC·GEM 2023·MIDIS SISFOH 2022·CAPECO 2023·INDECI
--          TimescaleDB 2.x·PostGIS 3.4·PostgreSQL 16
-- ══════════════════════════════════════════════════════════════════════════

-- ── Extensiones — ORDEN OBLIGATORIO ──────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- TimescaleDB: instalación opcional — envuelto en bloque de excepción
-- Si no está disponible, las tablas funcionan como PostgreSQL estándar
DO $$ BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    RAISE NOTICE 'TimescaleDB disponible — hypertables y CAG activos';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'TimescaleDB no instalado — funcionando en modo PostgreSQL estándar';
END $$;


-- ══════════════════════════════════════════════════════════════════════════
--  SISMOS
--  Tabla principal del catálogo sísmico (1900-presente).
--  Fuente: USGS FDSNWS + IGP catálogo histórico
--  Índices: BRIN en fecha (>500k filas, orden temporal), GIST en geom,
--           partial B-tree para sismos alertables (M≥4.5)
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sismos (
    id               BIGSERIAL PRIMARY KEY,
    usgs_id          TEXT UNIQUE NOT NULL,
    geom             GEOMETRY(Point, 4326) NOT NULL,
    magnitud         NUMERIC(4,1) NOT NULL CHECK (magnitud BETWEEN 0 AND 10),
    profundidad_km   NUMERIC(7,2) NOT NULL CHECK (profundidad_km >= 0),
    tipo_profundidad TEXT NOT NULL
        CHECK (tipo_profundidad IN ('superficial','intermedio','profundo')),
    fecha            DATE NOT NULL,
    hora_utc         TIMESTAMPTZ,
    lugar            TEXT,
    region           TEXT,
    tipo_magnitud    TEXT,
    estado           TEXT DEFAULT 'reviewed',
    fuente           TEXT DEFAULT 'USGS',
    creado_en        TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT ck_sismos_bbox CHECK (
        ST_X(geom) BETWEEN -85 AND -65
        AND ST_Y(geom) BETWEEN -20 AND 2
    )
);

-- BRIN: tabla >500k filas, datos ingresados cronológicamente
-- Fuente: PostGIS docs 4.x — BRIN for spatially correlated time-ordered data
-- Build time: minutos vs horas para GIST; tamaño: ~4 MB vs ~1 GB
CREATE INDEX IF NOT EXISTS idx_sismos_fecha_brin   ON sismos USING BRIN(fecha) WITH (pages_per_range=128);
CREATE INDEX IF NOT EXISTS idx_sismos_geom_gist    ON sismos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sismos_magnitud     ON sismos(magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_profund      ON sismos(profundidad_km);
CREATE INDEX IF NOT EXISTS idx_sismos_tipo_prof    ON sismos(tipo_profundidad);
CREATE INDEX IF NOT EXISTS idx_sismos_region       ON sismos(region);
CREATE INDEX IF NOT EXISTS idx_sismos_fecha_mag    ON sismos(fecha DESC, magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_anio_mag     ON sismos(DATE_PART('year', fecha), magnitud DESC);
-- Partial index para sismos alertables: reduce tamaño y acelera EWS
CREATE INDEX IF NOT EXISTS idx_sismos_alertables   ON sismos(fecha DESC, magnitud DESC)
    WHERE magnitud >= 4.5;
-- Index para geom + magnitud (queries espaciales con filtro de magnitud)
CREATE INDEX IF NOT EXISTS idx_sismos_geom_mag3    ON sismos USING GIST(geom)
    WHERE magnitud >= 3.0;


-- ══════════════════════════════════════════════════════════════════════════
--  DEPARTAMENTOS — zona sísmica NTE E.030-2018
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS departamentos (
    id             SERIAL PRIMARY KEY,
    ubigeo         TEXT UNIQUE,
    nombre         TEXT NOT NULL,
    geom           GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo   SMALLINT DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    factor_z       NUMERIC(4,2),
    area_km2       NUMERIC(12,3),
    capital        TEXT,
    fuente         TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dep_geom        ON departamentos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_dep_nombre      ON departamentos(nombre);
CREATE INDEX IF NOT EXISTS idx_dep_nombre_trgm ON departamentos USING GIN(nombre gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_dep_zona_sis    ON departamentos(zona_sismica);


-- ══════════════════════════════════════════════════════════════════════════
--  TABLA REFERENCIA: ZONA SÍSMICA POR DEPARTAMENTO
--  Fuente: NTE E.030-2018 (DS N°003-2016-VIVIENDA) — INMUTABLE
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS zona_sismica_departamento (
    departamento   TEXT PRIMARY KEY,
    zona_sismica   SMALLINT NOT NULL CHECK (zona_sismica BETWEEN 1 AND 4),
    factor_z       NUMERIC(4,2) NOT NULL CHECK (factor_z > 0),
    descripcion    TEXT,
    referencia     TEXT DEFAULT 'NTE E.030-2018 DS N°003-2016-VIVIENDA',
    actualizado_en TIMESTAMPTZ DEFAULT now() NOT NULL
);

INSERT INTO zona_sismica_departamento (departamento, zona_sismica, factor_z, descripcion)
VALUES
    ('Amazonas',      2, 0.25, 'Selva norte — riesgo sísmico medio'),
    ('Áncash',        4, 0.45, 'Costa norte — muy alto riesgo sísmico'),
    ('Apurímac',      2, 0.25, 'Sierra sur — riesgo sísmico medio'),
    ('Arequipa',      4, 0.45, 'Costa sur — muy alto riesgo sísmico'),
    ('Ayacucho',      2, 0.25, 'Sierra central — riesgo sísmico medio'),
    ('Cajamarca',     3, 0.35, 'Sierra norte — alto riesgo sísmico'),
    ('Callao',        4, 0.45, 'Costa central — muy alto riesgo sísmico'),
    ('Cusco',         3, 0.35, 'Sierra sur — alto riesgo sísmico'),
    ('Huancavelica',  3, 0.35, 'Sierra central — alto riesgo sísmico'),
    ('Huánuco',       2, 0.25, 'Sierra/Selva central — riesgo sísmico medio'),
    ('Ica',           4, 0.45, 'Costa sur — muy alto riesgo sísmico'),
    ('Junín',         3, 0.35, 'Sierra/Selva central — alto riesgo sísmico'),
    ('La Libertad',   4, 0.45, 'Costa norte — muy alto riesgo sísmico'),
    ('Lambayeque',    4, 0.45, 'Costa norte — muy alto riesgo sísmico'),
    ('Lima',          4, 0.45, 'Costa central — muy alto riesgo sísmico'),
    ('Loreto',        1, 0.10, 'Selva norte — bajo riesgo sísmico'),
    ('Madre de Dios', 1, 0.10, 'Selva sur — bajo riesgo sísmico'),
    ('Moquegua',      4, 0.45, 'Costa sur extrema — muy alto riesgo sísmico'),
    ('Pasco',         3, 0.35, 'Sierra central — alto riesgo sísmico'),
    ('Piura',         4, 0.45, 'Costa norte — muy alto riesgo sísmico'),
    ('Puno',          2, 0.25, 'Sierra sur — riesgo sísmico medio'),
    ('San Martín',    3, 0.35, 'Selva norte — alto riesgo sísmico'),
    ('Tacna',         4, 0.45, 'Costa extremo sur — muy alto riesgo sísmico'),
    ('Tumbes',        4, 0.45, 'Costa norte — muy alto riesgo sísmico'),
    ('Ucayali',       2, 0.25, 'Selva central — riesgo sísmico medio')
ON CONFLICT (departamento) DO UPDATE
    SET zona_sismica   = EXCLUDED.zona_sismica,
        factor_z       = EXCLUDED.factor_z,
        descripcion    = EXCLUDED.descripcion,
        actualizado_en = now();

CREATE INDEX IF NOT EXISTS idx_zsd_zona ON zona_sismica_departamento(zona_sismica);


-- ══════════════════════════════════════════════════════════════════════════
--  PROVINCIAS
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS provincias (
    id             SERIAL PRIMARY KEY,
    ubigeo         TEXT UNIQUE,
    nombre         TEXT NOT NULL,
    departamento   TEXT,
    geom           GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo   SMALLINT DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    area_km2       NUMERIC(12,3),
    fuente         TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prov_geom ON provincias USING GIST(geom);


-- ══════════════════════════════════════════════════════════════════════════
--  DISTRITOS — v9.0: +7 columnas nuevas para IRC v9 + cascada
--  Fuentes: INEI GeoServer WFS / GADM 4.1 / fallback 75 capitales
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS distritos (
    id             SERIAL PRIMARY KEY,
    ubigeo         TEXT UNIQUE,
    nombre         TEXT NOT NULL,
    provincia      TEXT,
    departamento   TEXT,
    geom           GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo   SMALLINT NOT NULL DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    poblacion      INTEGER,
    area_km2       NUMERIC(10,3),
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    indice_riesgo_construccion NUMERIC(4,2),
    fuente         TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);

-- ── Columnas v9.0 — aditivas, no rompen v8 ───────────────────────────────
-- peligro_volcan:    1-5 según distancia a volcán activo (INGEMMET 2021)
-- peligro_sequia:    1-5 según SPI-12 (McKee et al. 1993 / CHIRPS)
-- peligro_sismico:   réplica explícita del componente sísmico
-- peligro_inundacion: réplica del componente inundación
-- peligro_deslizamiento: réplica del componente deslizamiento
-- peligro_tsunami:   réplica del componente tsunami
-- fallas_activas_50km: contador de fallas activas en radio 50km
-- indice_riesgo_v9:  IRC v9 central (7 amenazas × factor_cascada)
-- irc_v9_p10/p90:    percentiles 10/90 bootstrapping 500 iter.
-- factor_cascada:    amplificador sismo→deslizamiento (Gill & Malamud 2014)
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

-- Índices distritos — GIST para polígonos complejos
-- Fuente: Crunchy Data 2025 — SP-GIST para puntos, GIST para polígonos
CREATE INDEX IF NOT EXISTS idx_distritos_geom         ON distritos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_distritos_nombre       ON distritos USING GIN(nombre gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_distritos_prov         ON distritos(provincia);
CREATE INDEX IF NOT EXISTS idx_distritos_dep          ON distritos(departamento);
CREATE INDEX IF NOT EXISTS idx_distritos_dep_riesgo   ON distritos(departamento, nivel_riesgo DESC);
CREATE INDEX IF NOT EXISTS idx_distritos_zona_sis     ON distritos(zona_sismica);
CREATE INDEX IF NOT EXISTS idx_distritos_pob          ON distritos(poblacion DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_dist_depto_trgm        ON distritos USING GIN(departamento gin_trgm_ops);
-- v9: índice compuesto para ranking IRC v9 por departamento
CREATE INDEX IF NOT EXISTS idx_distritos_v9_dep_riesgo
    ON distritos(departamento, indice_riesgo_v9 DESC NULLS LAST);
-- v9: índice para filtro de factor_cascada alto
CREATE INDEX IF NOT EXISTS idx_distritos_cascada
    ON distritos(factor_cascada DESC NULLS LAST) WHERE factor_cascada > 1.0;


-- ══════════════════════════════════════════════════════════════════════════
--  FALLAS GEOLÓGICAS ACTIVAS
--  Fuente: INGEMMET GeoCATMIN + Audin et al. 2008 (19 fallas confirmadas)
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fallas (
    id               SERIAL PRIMARY KEY,
    ingemmet_id      TEXT,
    nombre           TEXT NOT NULL,
    nombre_alt       TEXT,
    geom             GEOMETRY(MultiLineString, 4326),
    activa           BOOLEAN DEFAULT TRUE,
    tipo             TEXT,
    mecanismo        TEXT,
    longitud_km      NUMERIC(10,2),
    magnitud_max     NUMERIC(4,1),
    profundidad_tipo TEXT DEFAULT 'superficial',
    region           TEXT,
    fuente           TEXT DEFAULT 'INGEMMET/IGP',
    referencia       TEXT,
    actualizado_en   TIMESTAMPTZ DEFAULT NOW()
);
-- GIST para líneas/polígonos complejos de fallas
CREATE INDEX IF NOT EXISTS idx_fallas_geom    ON fallas USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_fallas_activa  ON fallas(activa);
CREATE INDEX IF NOT EXISTS idx_fallas_tipo    ON fallas(tipo);
CREATE INDEX IF NOT EXISTS idx_fallas_region  ON fallas(region);
CREATE INDEX IF NOT EXISTS idx_fallas_mag_max ON fallas(magnitud_max DESC NULLS LAST)
    WHERE activa = TRUE;


-- ══════════════════════════════════════════════════════════════════════════
--  ZONAS DE INUNDACIÓN
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS zonas_inundables (
    id                SERIAL PRIMARY KEY,
    nombre            TEXT NOT NULL,
    geom              GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo      SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    tipo_inundacion   TEXT DEFAULT 'fluvial',
    periodo_retorno   INTEGER,
    profundidad_max_m NUMERIC(6,2),
    velocidad_ms      NUMERIC(5,2),
    cuenca            TEXT,
    region            TEXT,
    fuente            TEXT DEFAULT 'ANA/CENEPRED',
    actualizado_en    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_inundables_geom   ON zonas_inundables USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_inundables_riesgo ON zonas_inundables(nivel_riesgo DESC);
CREATE INDEX IF NOT EXISTS idx_inundables_tipo   ON zonas_inundables(tipo_inundacion);
CREATE INDEX IF NOT EXISTS idx_inundables_region ON zonas_inundables(region);


-- ══════════════════════════════════════════════════════════════════════════
--  DESLIZAMIENTOS Y REMOCIÓN EN MASA
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS deslizamientos (
    id              SERIAL PRIMARY KEY,
    nombre          TEXT,
    geom            GEOMETRY(MultiPolygon, 4326),
    tipo            TEXT,
    nivel_riesgo    SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    area_km2        NUMERIC(10,4),
    volumen_m3      NUMERIC(14,2),
    velocidad_tipo  TEXT,
    causa_principal TEXT,
    fecha_evento    DATE,
    region          TEXT,
    activo          BOOLEAN DEFAULT TRUE,
    fuente          TEXT DEFAULT 'INGEMMET/CENEPRED',
    actualizado_en  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_desl_geom   ON deslizamientos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_desl_riesgo ON deslizamientos(nivel_riesgo DESC);
CREATE INDEX IF NOT EXISTS idx_desl_tipo   ON deslizamientos(tipo);
CREATE INDEX IF NOT EXISTS idx_desl_region ON deslizamientos(region);
CREATE INDEX IF NOT EXISTS idx_desl_activo ON deslizamientos(activo) WHERE activo = TRUE;


-- ══════════════════════════════════════════════════════════════════════════
--  INFRAESTRUCTURA CRÍTICA
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS infraestructura (
    id             SERIAL PRIMARY KEY,
    osm_id         BIGINT,
    nombre         TEXT NOT NULL,
    tipo           TEXT NOT NULL,
    geom           GEOMETRY(Point, 4326),
    criticidad     SMALLINT DEFAULT 3 CHECK (criticidad BETWEEN 1 AND 5),
    capacidad      INTEGER,
    estado         TEXT DEFAULT 'operativo',
    region         TEXT,
    distrito       TEXT,
    telefono       TEXT,
    fuente         TEXT,
    fuente_tipo    TEXT DEFAULT 'osm'
        CHECK (fuente_tipo IN ('oficial', 'osm')),
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    actualizado_en TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT ck_infra_bbox CHECK (
        ST_X(geom) BETWEEN -83 AND -68
        AND ST_Y(geom) BETWEEN -19 AND 1
    )
);
-- SP-GIST para puntos de infraestructura (uniformemente distribuidos)
CREATE INDEX IF NOT EXISTS idx_infra_geom_spgist ON infraestructura USING SPGIST(geom);
CREATE INDEX IF NOT EXISTS idx_infra_tipo        ON infraestructura(tipo);
CREATE INDEX IF NOT EXISTS idx_infra_critic      ON infraestructura(criticidad DESC);
CREATE INDEX IF NOT EXISTS idx_infra_region      ON infraestructura(region);
CREATE INDEX IF NOT EXISTS idx_infra_nombre      ON infraestructura USING GIN(nombre gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_infra_tipo_region ON infraestructura(tipo, region, criticidad DESC);
CREATE INDEX IF NOT EXISTS idx_infra_fuente_tipo ON infraestructura(fuente_tipo);
CREATE INDEX IF NOT EXISTS idx_infra_oficial     ON infraestructura(tipo, criticidad DESC)
    WHERE fuente_tipo = 'oficial';


-- ══════════════════════════════════════════════════════════════════════════
--  ZONAS DE TSUNAMI
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS zonas_tsunami (
    id                SERIAL PRIMARY KEY,
    nombre            TEXT NOT NULL,
    geom              GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo      SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    altura_ola_m      NUMERIC(6,2),
    tiempo_arribo_min INTEGER,
    periodo_retorno   INTEGER,
    region            TEXT,
    fuente            TEXT DEFAULT 'PREDES/IGP',
    actualizado_en    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tsunami_geom   ON zonas_tsunami USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_tsunami_riesgo ON zonas_tsunami(nivel_riesgo DESC);
CREATE INDEX IF NOT EXISTS idx_tsunami_region ON zonas_tsunami(region);


-- ══════════════════════════════════════════════════════════════════════════
--  ESTACIONES DE MONITOREO
--  SP-GIST para puntos uniformes (estaciones distribuidas en el territorio)
--  Fuente: Crunchy Data 2025 — SP-GIST supera GIST para puntos uniformes
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS estaciones (
    id             SERIAL PRIMARY KEY,
    codigo         TEXT UNIQUE,
    nombre         TEXT NOT NULL,
    tipo           TEXT NOT NULL,
    geom           GEOMETRY(Point, 4326),
    altitud_m      NUMERIC(8,2),
    activa         BOOLEAN DEFAULT TRUE,
    institucion    TEXT,
    region         TEXT,
    red            TEXT,
    actualizado_en TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT ck_estaciones_bbox CHECK (
        ST_X(geom) BETWEEN -83 AND -68
        AND ST_Y(geom) BETWEEN -19 AND 1
    )
);
-- SP-GIST para puntos de estaciones (uniformes en territorio)
CREATE INDEX IF NOT EXISTS idx_estaciones_geom_spgist ON estaciones USING SPGIST(geom);
CREATE INDEX IF NOT EXISTS idx_estaciones_tipo        ON estaciones(tipo);
CREATE INDEX IF NOT EXISTS idx_estaciones_inst        ON estaciones(institucion);


-- ══════════════════════════════════════════════════════════════════════════
--  ZONAS DE PRECIPITACIÓN (v8.0)
--  22 zonas climáticas SENAMHI Atlas Climático 2021 + CHIRPS v2.0 1981-2020
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS zonas_precipitacion (
    id                        SERIAL PRIMARY KEY,
    nombre                    TEXT UNIQUE NOT NULL,
    tipo                      TEXT NOT NULL,
    region                    TEXT,
    geom                      GEOMETRY(MultiPolygon, 4326),
    precipitacion_anual_mm    NUMERIC(8,1) NOT NULL CHECK (precipitacion_anual_mm >= 0),
    precipitacion_dic_mar_mm  NUMERIC(8,1) CHECK (precipitacion_dic_mar_mm >= 0),
    precipitacion_jun_ago_mm  NUMERIC(8,1) CHECK (precipitacion_jun_ago_mm >= 0),
    indice_fen                NUMERIC(4,2) NOT NULL DEFAULT 1.0 CHECK (indice_fen > 0),
    nivel_riesgo_inundacion   SMALLINT NOT NULL DEFAULT 2
        CHECK (nivel_riesgo_inundacion BETWEEN 1 AND 5),
    fuente                    TEXT DEFAULT 'SENAMHI/CHIRPS 2024',
    created_at                TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_precip_geom      ON zonas_precipitacion USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_precip_tipo      ON zonas_precipitacion(tipo);
CREATE INDEX IF NOT EXISTS idx_precip_region    ON zonas_precipitacion(region);
CREATE INDEX IF NOT EXISTS idx_precip_riesgo    ON zonas_precipitacion(nivel_riesgo_inundacion DESC);
CREATE INDEX IF NOT EXISTS idx_precip_fen       ON zonas_precipitacion(indice_fen DESC);
CREATE INDEX IF NOT EXISTS idx_precip_riesgo_fen ON zonas_precipitacion
    (nivel_riesgo_inundacion DESC, indice_fen DESC);


-- ══════════════════════════════════════════════════════════════════════════
--  EVENTOS FEN HISTÓRICOS (v8.0)
--  NOAA-CPC ONI 1957-2024
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS eventos_fen (
    id           SERIAL PRIMARY KEY,
    año_inicio   SMALLINT NOT NULL CHECK (año_inicio BETWEEN 1900 AND 2100),
    mes_inicio   SMALLINT NOT NULL CHECK (mes_inicio BETWEEN 1 AND 12),
    año_fin      SMALLINT NOT NULL CHECK (año_fin >= año_inicio),
    mes_fin      SMALLINT NOT NULL CHECK (mes_fin BETWEEN 1 AND 12),
    tipo         TEXT NOT NULL CHECK (tipo IN ('el_nino','la_nina','neutro')),
    intensidad   TEXT CHECK (intensidad IN ('debil','moderado','fuerte','extraordinario')),
    oni_peak     NUMERIC(4,2),
    impacto_peru TEXT,
    fuente       TEXT DEFAULT 'NOAA-CPC/ENFEN',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (año_inicio, mes_inicio, tipo)
);
CREATE INDEX IF NOT EXISTS idx_fen_anio       ON eventos_fen(año_inicio, año_fin);
CREATE INDEX IF NOT EXISTS idx_fen_tipo       ON eventos_fen(tipo);
CREATE INDEX IF NOT EXISTS idx_fen_intensidad ON eventos_fen(intensidad);
CREATE INDEX IF NOT EXISTS idx_fen_intensos   ON eventos_fen(año_inicio DESC)
    WHERE intensidad IN ('fuerte','extraordinario');


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 VOLCANES (v9.0)
--  Fuente: INGEMMET "Mapa de Peligros Volcánicos del Perú" 2da ed. 2021
--          OVI-IGP (Observatorio Vulcanológico del INGEMMET)
--  20 volcanes del catálogo oficial peruano
--  SP-GIST: puntos uniformes concentrados en el sur del país
--  Fuente índice: Crunchy Data 2025 — SP-GIST benchmarks para puntos
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS volcanes (
    id              SERIAL PRIMARY KEY,
    nombre          TEXT NOT NULL,
    geom            GEOMETRY(Point, 4326),
    estado          TEXT DEFAULT 'activo'
        CHECK (estado IN ('activo_critico','activo','potencialmente_activo','inactivo')),
    altitud_m       INT,
    region          TEXT,
    tipo_erupcion   TEXT,      -- 'estromboliana','pliniana','freatomagmatica'
    ultima_erupcion INT,       -- año de última erupción documentada
    fuente          TEXT DEFAULT 'OVI-IGP/INGEMMET 2021'
);
-- SP-GIST: puntos uniformes (volcanes concentrados en sur de Perú)
-- Fuente: Crunchy Data 2025 spatial indexes benchmarks
CREATE INDEX IF NOT EXISTS idx_volcanes_geom_spgist ON volcanes USING SPGIST(geom);
CREATE INDEX IF NOT EXISTS idx_volcanes_estado      ON volcanes(estado);
CREATE INDEX IF NOT EXISTS idx_volcanes_region      ON volcanes(region);


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 SUSCEPTIBILIDAD ML (v9.0)
--  Scores de susceptibilidad por punto de grilla (0.05° resolución).
--  Modelo: XGBoost + SMOTE-Tomek + Optuna + SHAP
--  Fuentes científicas:
--    Kumar et al. 2023 Remote Sensing 15(5):1376 (AUC > 0.95)
--    Lu et al. 2024 Geomatics Nat. Hazards 15:2314565 (SMOTE-ENN)
--    Scientific Reports 2025 (RFE + meta-classifier AUC 0.987)
--    Tandfonline 2025 XGBoost+SHAP (recall 93.12%)
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS susceptibilidad_ml (
    id               SERIAL PRIMARY KEY,
    amenaza          TEXT NOT NULL
        CHECK (amenaza IN ('deslizamiento','inundacion','sequia')),
    modelo           TEXT NOT NULL,
    geom             GEOMETRY(Point, 4326),
    score            NUMERIC(5,4) CHECK (score BETWEEN 0 AND 1),
    score_p10        NUMERIC(5,4),   -- IC 80% inferior (bootstrapping 100 iter.)
    score_p90        NUMERIC(5,4),   -- IC 80% superior
    -- Columna generada: clasifica automáticamente según score
    nivel            TEXT GENERATED ALWAYS AS (
        CASE
            WHEN score >= 0.8 THEN 'MUY_ALTO'
            WHEN score >= 0.6 THEN 'ALTO'
            WHEN score >= 0.4 THEN 'MEDIO'
            WHEN score >= 0.2 THEN 'BAJO'
            ELSE 'MUY_BAJO'
        END
    ) STORED,
    shap_values      JSONB,           -- top-5 features {feature: shap_val}
    fecha_prediccion DATE DEFAULT CURRENT_DATE,
    version_modelo   TEXT
);
-- SP-GIST: puntos de grilla regular (uniformemente distribuidos)
CREATE INDEX IF NOT EXISTS idx_suscept_geom_spgist  ON susceptibilidad_ml USING SPGIST(geom);
CREATE INDEX IF NOT EXISTS idx_suscept_amenaza_score ON susceptibilidad_ml(amenaza, score DESC);
CREATE INDEX IF NOT EXISTS idx_suscept_nivel        ON susceptibilidad_ml(amenaza, nivel);


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 METADATA DE MODELOS ML (v9.0)
--  Registra métricas, hiperparámetros y configuración por modelo entrenado.
--  AUC-PR como métrica primaria (más informativa con desbalance >10:1)
--  Fuente: Lu et al. 2024 + Acta Geotechnica 2024
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS modelo_metadata (
    amenaza              TEXT PRIMARY KEY,
    algoritmo            TEXT,               -- 'XGBoost','RandomForest','LogisticRegression'
    auc_roc              NUMERIC(5,4),
    auc_pr               NUMERIC(5,4),       -- métrica primaria (desbalance severo)
    f1_score             NUMERIC(5,4),
    precision_score      NUMERIC(5,4),
    recall_score         NUMERIC(5,4),
    n_samples            INT,
    n_positivos          INT,
    n_negativos          INT,
    ratio_imbalance      NUMERIC(6,3),       -- negativos / positivos (original)
    features_usadas      JSONB,              -- lista post-VIF y post-RFE
    features_elim_vif    JSONB,              -- eliminadas por VIF > 10
    features_elim_rfe    JSONB,              -- eliminadas por RFE
    importancias_shap    JSONB,              -- mean |SHAP| por feature
    hiperparametros      JSONB,              -- mejores params Optuna
    tecnica_balance      TEXT,               -- 'smote_tomek','smote_enn','scale_pos_weight'
    entrenado_en         TIMESTAMPTZ,
    version              TEXT
);


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 ALERTAS EN TIEMPO REAL — EWS (v9.0)
--  Sistema de alerta temprana multi-hazard alineado con EW4All (UNDRR 2022)
--  Niveles según INDECI "Protocolo Nacional de Alertas Sísmicas" 2020
--  CAP v1.2 según ITU-T X.1303bis (adoptado por INDECI para sistema nacional)
--  Cascadas: Gill & Malamud 2014 Rev. Geophys. 52(4):680-722
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS alertas_rt (
    id                       BIGSERIAL PRIMARY KEY,
    usgs_id                  TEXT UNIQUE,
    igp_id                   TEXT UNIQUE,
    nivel_alerta             TEXT NOT NULL
        CHECK (nivel_alerta IN ('watch','warning','emergency')),
    magnitud                 NUMERIC(3,1),
    profundidad_km           NUMERIC(6,2),
    lugar                    TEXT,
    geom                     GEOMETRY(Point, 4326),
    infraestructura_afectada JSONB,    -- [{tipo, nombre, distancia_km}]
    poblacion_expuesta       INT,
    -- Detección de peligros en cascada
    -- Tsunami:       M≥6.5 + epicentro <50km costa + prof <70km
    -- Deslizamiento: M≥5.0 + peligro_deslizamiento ≥3 en zona
    dispara_tsunami          BOOLEAN DEFAULT FALSE,
    dispara_deslizamiento    BOOLEAN DEFAULT FALSE,
    -- CAP v1.2 (ITU-T X.1303bis / OASIS CAP 1.2)
    cap_identifier           TEXT,    -- UUID único del mensaje CAP
    cap_xml                  TEXT,    -- mensaje CAP v1.2 completo en XML
    -- 4 pilares EW4All (UNDRR/WMO 2022)
    -- P1: conocimiento riesgo  P2: observación  P3: difusión  P4: preparación
    pilares_ew4all           JSONB,   -- {p1:bool, p2:bool, p3:bool, p4:bool}
    canales_enviados         TEXT[],  -- ['sse','websocket','cap']
    created_at               TIMESTAMPTZ DEFAULT NOW()
);
-- BRIN en created_at: tabla crece cronológicamente (similar a sismos)
CREATE INDEX IF NOT EXISTS idx_alertas_created_brin ON alertas_rt USING BRIN(created_at);
-- GIST en geom: radio de impacto puede ser polígono
CREATE INDEX IF NOT EXISTS idx_alertas_geom_gist    ON alertas_rt USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_alertas_nivel_tiempo ON alertas_rt(nivel_alerta, created_at DESC);
-- Partial index para alertas con peligros en cascada activos
CREATE INDEX IF NOT EXISTS idx_alertas_cascade      ON alertas_rt(dispara_tsunami, dispara_deslizamiento)
    WHERE dispara_tsunami = TRUE OR dispara_deslizamiento = TRUE;


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 EXPOSICIÓN / IVS (v9.0)
--  Datos de exposición por distrito para modelo de daño.
--  Fuentes:
--    GEM Global Exposure Model 2023 (Yepes-Estrada et al., Earthquake Spectra)
--    INEI Censo de Población y Vivienda 2017
--    MIDIS SISFOH 2022 (Índice de Vulnerabilidad Social)
--    CAPECO 2023 (costos de reposición por vivienda)
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS exposicion_distritos (
    ubigeo               TEXT PRIMARY KEY,
    poblacion_total      INT,
    n_viviendas          INT,
    -- INEI CPV 2017 — porcentajes de vulnerabilidad
    pct_adobe            NUMERIC(5,2),
    pct_pobreza          NUMERIC(5,2),
    pct_sin_agua         NUMERIC(5,2),
    pct_analfabetismo    NUMERIC(5,2),
    pct_sin_desague      NUMERIC(5,2),
    pct_adulto_mayor     NUMERIC(5,2),
    -- GEM Global Exposure Model 2023 — taxonomía de edificios
    -- Fuente: Yepes-Estrada et al. 2023 Earthquake Spectra DOI 10.1177/87552930221126842
    gem_tax_predominante TEXT,          -- ej. "MUR+ADO/LWAL/H:1"
    pct_ladrillo_conf    NUMERIC(5,2),  -- mampostería confinada
    pct_concreto         NUMERIC(5,2),  -- concreto armado
    pct_quincha          NUMERIC(5,2),  -- quincha/madera
    -- IVS = 0.30×pct_adobe + 0.25×pct_pobreza + 0.20×pct_sin_agua
    --     + 0.15×pct_analfabetismo + 0.10×pct_sin_desague (÷100)
    -- Fuente: MIDIS "Índice de Vulnerabilidad Social" 2022
    ivs                  NUMERIC(6,5),
    nivel_ivs            TEXT GENERATED ALWAYS AS (
        CASE
            WHEN ivs >= 0.6 THEN 'muy_alto'
            WHEN ivs >= 0.4 THEN 'alto'
            WHEN ivs >= 0.2 THEN 'medio'
            ELSE 'bajo'
        END
    ) STORED,
    -- indice_riesgo_total = indice_riesgo_v9 × (1 + ivs) × factor_cascada
    -- Escala abierta — puede superar 5.0 en zonas de cascada
    indice_riesgo_total  NUMERIC(6,4),
    fuente               TEXT DEFAULT 'INEI CPV 2017 + MIDIS SISFOH 2022 + GEM 2023',
    actualizado_en       DATE DEFAULT CURRENT_DATE
);
CREATE INDEX IF NOT EXISTS idx_exposicion_ivs_desc
    ON exposicion_distritos(ivs DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_exposicion_riesgo_total
    ON exposicion_distritos(indice_riesgo_total DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_exposicion_nivel_ivs
    ON exposicion_distritos(nivel_ivs);


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 LECTURAS DE ESTACIONES — hypertable TimescaleDB (v9.0)
--  Series temporales de variables hidrometeorológicas y sísmicas.
--  TimescaleDB chunk_time_interval = 1 month para optimizar queries recientes.
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS lecturas_estaciones (
    time              TIMESTAMPTZ NOT NULL,
    estacion_codigo   TEXT REFERENCES estaciones(codigo),
    variable          TEXT CHECK (variable IN (
        'temperatura','precipitacion','humedad',
        'velocidad_viento','aceleracion_pga',
        'nivel_rio','nivel_mar'
    )),
    valor             NUMERIC(10,3),
    calidad           SMALLINT DEFAULT 0   -- 0=no_verificado 1=ok 2=dudoso
);

DO $$ BEGIN
    PERFORM create_hypertable(
        'lecturas_estaciones', 'time',
        chunk_time_interval => INTERVAL '1 month',
        if_not_exists       => TRUE
    );
    RAISE NOTICE 'lecturas_estaciones: hypertable creada';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'lecturas_estaciones: TimescaleDB no disponible — tabla normal';
END $$;

-- Índice compuesto para queries de series por estación
-- TimescaleDB gestiona el índice temporal; este es para filtro por estación
CREATE INDEX IF NOT EXISTS idx_lecturas_codigo_time
    ON lecturas_estaciones(estacion_codigo, time DESC);


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 SENDAI FRAMEWORK SNAPSHOTS (v9.0)
--  Métricas proxy del Marco de Sendai 2015-2030 (7 targets, 38 indicadores)
--  Fuente: UNDRR Sendai Framework Monitor — Perú es estado signatario
--  Calculado automáticamente desde datos GeoRiesgo v9
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sendai_snapshots (
    id          SERIAL PRIMARY KEY,
    año         SMALLINT NOT NULL UNIQUE,
    target_a    JSONB,   -- mortalidad por desastres
    target_b    JSONB,   -- personas afectadas
    target_c    JSONB,   -- pérdidas económicas (% PIB)
    target_d    JSONB,   -- daño a infraestructura crítica
    target_e    JSONB,   -- estrategias nacionales/locales DRR
    target_f    JSONB,   -- cooperación internacional (proxy vacío)
    target_g    JSONB,   -- acceso a MHEWS — 4 pilares EW4All
    metodologia TEXT DEFAULT
        'UNDRR Sendai Framework Monitor Indicators 2015-2030 (proxy via GeoRiesgo v9)',
    creado_en   TIMESTAMPTZ DEFAULT NOW()
);


-- ══════════════════════════════════════════════════════════════════════════
--  LOG DE SINCRONIZACIÓN
-- ══════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sync_log (
    id         SERIAL PRIMARY KEY,
    fuente     TEXT NOT NULL,
    tabla      TEXT NOT NULL,
    registros  INTEGER DEFAULT 0,
    estado     TEXT DEFAULT 'ok',
    detalle    TEXT,
    duracion_s NUMERIC(10,2),
    inicio     TIMESTAMPTZ DEFAULT NOW(),
    fin        TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_synclog_fuente ON sync_log(fuente, fin DESC);
CREATE INDEX IF NOT EXISTS idx_synclog_tabla  ON sync_log(tabla, fin DESC);


-- ══════════════════════════════════════════════════════════════════════════
--  TIMESCALEDB — HYPERTABLE SISMOS + COMPRESIÓN + CAG
--  Envuelto en bloque DO con excepción para degradación graceful.
-- ══════════════════════════════════════════════════════════════════════════
DO $$ BEGIN
    PERFORM create_hypertable(
        'sismos', 'fecha',
        migrate_data        => true,
        chunk_time_interval => INTERVAL '2 years',
        if_not_exists       => TRUE
    );
    -- Compresión: datos >5 años se comprimen por región
    ALTER TABLE sismos SET (
        timescaledb.compress,
        timescaledb.compress_orderby   = 'fecha DESC',
        timescaledb.compress_segmentby = 'region'
    );
    PERFORM add_compression_policy(
        'sismos',
        INTERVAL '5 years',
        if_not_exists => true
    );
    RAISE NOTICE 'TimescaleDB: hypertable sismos configurada con compresión';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'TimescaleDB: hypertable sismos no disponible — modo tabla normal';
END $$;


-- ══════════════════════════════════════════════════════════════════════════
--  TIMESCALEDB — CAG SISMOS MENSUAL
--  Continuous Aggregate para /api/v1/sismos/tendencia
--  Fallback: el endpoint usa GROUP BY si la vista no existe
--  Nuevo en v9: incluye mag_mediana (PERCENTILE_CONT)
-- ══════════════════════════════════════════════════════════════════════════
DO $$ BEGIN
    CREATE MATERIALIZED VIEW IF NOT EXISTS sismos_mensual
    WITH (
        timescaledb.continuous,
        timescaledb.materialized_only = false
    ) AS
    SELECT
        time_bucket('1 month', fecha)                                    AS mes,
        region,
        COUNT(*)                                                          AS cantidad,
        MAX(magnitud)                                                     AS mag_max,
        ROUND(AVG(magnitud)::NUMERIC, 2)                                  AS mag_prom,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY magnitud)             AS mag_mediana,
        COUNT(*) FILTER (WHERE magnitud >= 5.0)                           AS m5_plus,
        COUNT(*) FILTER (WHERE magnitud >= 6.0)                           AS m6_plus,
        COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial')          AS superficiales,
        MIN(profundidad_km)                                               AS prof_min,
        AVG(profundidad_km)                                               AS prof_prom
    FROM sismos
    GROUP BY 1, 2;

    PERFORM add_continuous_aggregate_policy(
        'sismos_mensual',
        start_offset      => INTERVAL '3 months',
        end_offset        => INTERVAL '1 hour',
        schedule_interval => INTERVAL '1 day',
        if_not_exists     => TRUE
    );
    RAISE NOTICE 'TimescaleDB: CAG sismos_mensual creada con policy diaria';
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'TimescaleDB: CAG sismos_mensual no disponible — endpoint usará GROUP BY';
END $$;


-- ══════════════════════════════════════════════════════════════════════════
--  FUNCIONES AUXILIARES
-- ══════════════════════════════════════════════════════════════════════════

-- ── f_asignar_region(lon, lat) — 3 niveles de fallback ──────────────────
CREATE OR REPLACE FUNCTION f_asignar_region(p_lon FLOAT, p_lat FLOAT)
RETURNS TEXT AS $$
DECLARE
    v_region TEXT;
    v_pt     GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
BEGIN
    SELECT nombre INTO v_region
    FROM departamentos WHERE ST_Covers(geom, v_pt)
    ORDER BY nivel_riesgo DESC LIMIT 1;
    IF v_region IS NOT NULL THEN RETURN v_region; END IF;

    SELECT nombre INTO v_region
    FROM departamentos
    WHERE ST_DWithin(geom::geography, v_pt::geography, 5000)
    ORDER BY ST_Distance(geom::geography, v_pt::geography) LIMIT 1;
    IF v_region IS NOT NULL THEN RETURN v_region; END IF;

    SELECT nombre INTO v_region
    FROM departamentos ORDER BY geom <-> v_pt LIMIT 1;
    RETURN COALESCE(v_region, 'Perú');
END;
$$ LANGUAGE plpgsql STABLE;


-- ── f_asignar_zona_sismica(lon, lat) ─────────────────────────────────────
CREATE OR REPLACE FUNCTION f_asignar_zona_sismica(p_lon FLOAT, p_lat FLOAT)
RETURNS SMALLINT AS $$
DECLARE
    v_zona SMALLINT;
    v_pt   GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
BEGIN
    SELECT zona_sismica INTO v_zona
    FROM departamentos
    WHERE ST_Covers(geom, v_pt) AND zona_sismica IS NOT NULL
    ORDER BY zona_sismica DESC LIMIT 1;
    IF v_zona IS NOT NULL THEN RETURN v_zona; END IF;

    SELECT zona_sismica INTO v_zona
    FROM departamentos WHERE zona_sismica IS NOT NULL
    ORDER BY geom <-> v_pt LIMIT 1;
    RETURN COALESCE(v_zona, 2);
END;
$$ LANGUAGE plpgsql STABLE;


-- ── f_actualizar_regiones() — v8.0 + v9.0 ────────────────────────────────
CREATE OR REPLACE FUNCTION f_actualizar_regiones()
RETURNS TABLE(tabla TEXT, registros_actualizados BIGINT, via_knn BIGINT) AS $$
DECLARE
    n_covers BIGINT;
    n_knn    BIGINT;
BEGIN
    -- SISMOS
    UPDATE sismos s SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, s.geom) AND (s.region IS NULL OR s.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE sismos s SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> s.geom LIMIT 1
    ) WHERE s.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'sismos'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- INFRAESTRUCTURA
    UPDATE infraestructura i SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, i.geom) AND (i.region IS NULL OR i.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE infraestructura i SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> i.geom LIMIT 1
    ) WHERE i.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    UPDATE infraestructura i SET zona_sismica = d.zona_sismica
    FROM departamentos d
    WHERE ST_Covers(d.geom, i.geom) AND d.zona_sismica IS NOT NULL
      AND i.zona_sismica IS DISTINCT FROM d.zona_sismica;
    tabla := 'infraestructura'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- INFRAESTRUCTURA → DISTRITOS
    UPDATE infraestructura i SET distrito = d.nombre
    FROM distritos d
    WHERE ST_Covers(d.geom, i.geom) AND (i.distrito IS NULL OR i.distrito <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE infraestructura i SET distrito = (
        SELECT d.nombre FROM distritos d ORDER BY d.geom <-> i.geom LIMIT 1
    ) WHERE i.distrito IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'infraestructura.distrito'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- ESTACIONES
    UPDATE estaciones e SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, e.geom) AND (e.region IS NULL OR e.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE estaciones e SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> e.geom LIMIT 1
    ) WHERE e.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'estaciones'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- FALLAS
    UPDATE fallas f SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(f.geom)) AND (f.region IS NULL OR f.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE fallas f SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(f.geom) LIMIT 1
    ) WHERE f.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'fallas'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- ZONAS INUNDABLES
    UPDATE zonas_inundables zi SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(zi.geom)) AND (zi.region IS NULL OR zi.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE zonas_inundables zi SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(zi.geom) LIMIT 1
    ) WHERE zi.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'zonas_inundables'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- DESLIZAMIENTOS
    UPDATE deslizamientos dl SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(dl.geom)) AND (dl.region IS NULL OR dl.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE deslizamientos dl SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(dl.geom) LIMIT 1
    ) WHERE dl.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'deslizamientos'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- TSUNAMIS
    UPDATE zonas_tsunami zt SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(zt.geom)) AND (zt.region IS NULL OR zt.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE zonas_tsunami zt SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(zt.geom) LIMIT 1
    ) WHERE zt.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'zonas_tsunami'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- ZONAS PRECIPITACION (v8.0)
    BEGIN
        UPDATE zonas_precipitacion zp SET region = d.nombre
        FROM departamentos d
        WHERE ST_Covers(d.geom, ST_Centroid(zp.geom))
          AND (zp.region IS NULL OR zp.region <> d.nombre);
        GET DIAGNOSTICS n_covers = ROW_COUNT;
        UPDATE zonas_precipitacion zp SET region = (
            SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(zp.geom) LIMIT 1
        ) WHERE zp.region IS NULL;
        GET DIAGNOSTICS n_knn = ROW_COUNT;
        tabla := 'zonas_precipitacion'; registros_actualizados := n_covers; via_knn := n_knn;
        RETURN NEXT;
    EXCEPTION WHEN undefined_column THEN NULL;
    END;

    -- 🆕 VOLCANES (v9.0)
    BEGIN
        UPDATE volcanes v SET region = d.nombre
        FROM departamentos d
        WHERE ST_Covers(d.geom, v.geom) AND (v.region IS NULL OR v.region <> d.nombre);
        GET DIAGNOSTICS n_covers = ROW_COUNT;
        UPDATE volcanes v SET region = (
            SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> v.geom LIMIT 1
        ) WHERE v.region IS NULL;
        GET DIAGNOSTICS n_knn = ROW_COUNT;
        tabla := 'volcanes'; registros_actualizados := n_covers; via_knn := n_knn;
        RETURN NEXT;
    EXCEPTION WHEN undefined_column THEN NULL;
    END;
END;
$$ LANGUAGE plpgsql;


-- ── f_sismos_cercanos(lon, lat, radio_km, mag_min, limit) ───────────────
CREATE OR REPLACE FUNCTION f_sismos_cercanos(
    p_lon      FLOAT,
    p_lat      FLOAT,
    p_radio_km INT   DEFAULT 50,
    p_mag_min  FLOAT DEFAULT 3.0,
    p_limit    INT   DEFAULT 100
)
RETURNS TABLE (
    usgs_id          TEXT,
    magnitud         NUMERIC,
    profundidad_km   NUMERIC,
    tipo_profundidad TEXT,
    fecha            DATE,
    lugar            TEXT,
    region           TEXT,
    distancia_km     NUMERIC
) AS $$
    SELECT
        usgs_id, magnitud, profundidad_km, tipo_profundidad, fecha, lugar,
        COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))),
        ROUND(
            (ST_Distance(geom::geography,
             ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography) / 1000
            )::NUMERIC, 1
        )
    FROM sismos
    WHERE magnitud >= p_mag_min
      AND ST_DWithin(
          geom::geography,
          ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography,
          p_radio_km * 1000
      )
    ORDER BY 8 ASC
    LIMIT p_limit;
$$ LANGUAGE SQL STABLE;


-- ══════════════════════════════════════════════════════════════════════════
--  FUNCIÓN: f_riesgo_construccion(lon, lat) — v8.0
--  IRC = 0.40×PS + 0.25×PI_fen + 0.20×PD + 0.10×PT + 0.05×PF
--  PI_fen = PI_base × indice_fen (amplificación FEN, capped a 5.0)
--  Fuente: CENEPRED Manual 2014 + NTE E.030-2018 + SENAMHI/CHIRPS v8.0
-- ══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_riesgo_construccion(p_lon FLOAT, p_lat FLOAT)
RETURNS JSONB AS $$
DECLARE
    pt           GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
    v_depto      TEXT;
    v_distrito   TEXT;
    v_zona_sis   SMALLINT;
    v_factor_z   NUMERIC;
    v_ps         NUMERIC;
    v_pi_base    NUMERIC;
    v_pi_fen     NUMERIC;
    v_pd         NUMERIC;
    v_pt_tsun    NUMERIC;
    v_pf         NUMERIC;
    v_indice_fen NUMERIC;
    v_zona_prec  TEXT;
    v_indice     NUMERIC;
    v_nivel_txt  TEXT;
    v_recom      JSONB;
    v_n_sismos   BIGINT;
    v_mag_max    NUMERIC;
    v_n_fallas   INTEGER;
    v_inundacion BOOLEAN;
    v_desl       BOOLEAN;
    v_tsunami    BOOLEAN;
    v_tipo_suelo TEXT;
BEGIN
    SELECT d.nombre, d.zona_sismica, d.factor_z INTO v_depto, v_zona_sis, v_factor_z
    FROM departamentos d WHERE ST_Covers(d.geom, pt) LIMIT 1;

    IF v_zona_sis IS NULL THEN
        SELECT d.zona_sismica, d.factor_z, d.nombre INTO v_zona_sis, v_factor_z, v_depto
        FROM departamentos d ORDER BY d.geom <-> pt LIMIT 1;
    END IF;

    IF v_zona_sis IS NULL AND v_depto IS NOT NULL THEN
        SELECT zsd.zona_sismica, zsd.factor_z INTO v_zona_sis, v_factor_z
        FROM zona_sismica_departamento zsd
        WHERE unaccent(lower(zsd.departamento)) = unaccent(lower(v_depto)) LIMIT 1;
    END IF;

    v_zona_sis := COALESCE(v_zona_sis, 2);
    v_factor_z  := COALESCE(v_factor_z, 0.25);

    SELECT nombre INTO v_distrito
    FROM distritos WHERE ST_Covers(geom, pt) LIMIT 1;

    v_ps := CASE v_zona_sis WHEN 4 THEN 5.0 WHEN 3 THEN 4.0 WHEN 2 THEN 3.0 WHEN 1 THEN 2.0 ELSE 3.0 END;

    SELECT COUNT(*), ROUND(MAX(magnitud)::NUMERIC, 1) INTO v_n_sismos, v_mag_max
    FROM sismos
    WHERE ST_DWithin(geom::geography, pt::geography, 50000)
      AND fecha >= CURRENT_DATE - INTERVAL '30 years' AND magnitud >= 4.0;

    IF v_n_sismos > 500 OR v_mag_max >= 8.0 THEN v_ps := LEAST(5.0, v_ps + 0.5);
    ELSIF v_n_sismos < 10 AND v_mag_max < 5.0 THEN v_ps := GREATEST(1.0, v_ps - 0.3);
    END IF;

    v_tipo_suelo := CASE
        WHEN ST_X(pt) < -78.0 AND ST_Y(pt) BETWEEN -6 AND 0 THEN 'S3 — Depósito aluvial costero norte'
        WHEN ST_X(pt) < -76.5 THEN 'S3 — Depósito costero/aluvial'
        WHEN ST_Y(pt) < -12.0 AND ST_X(pt) BETWEEN -76 AND -70 THEN 'S2 — Suelo intermedio sierra'
        WHEN ST_Y(pt) < -14.0 AND ST_X(pt) > -71.0 THEN 'S1 — Roca/suelo rígido altiplano'
        ELSE 'S2 — Suelo intermedio estimado'
    END;

    SELECT COALESCE(indice_fen, 1.0), nombre INTO v_indice_fen, v_zona_prec
    FROM zonas_precipitacion ORDER BY geom <-> pt LIMIT 1;
    v_indice_fen := COALESCE(v_indice_fen, 1.0);

    SELECT EXISTS(SELECT 1 FROM zonas_inundables WHERE ST_Covers(geom, pt)) INTO v_inundacion;
    SELECT nivel_riesgo::NUMERIC INTO v_pi_base FROM zonas_inundables
    WHERE ST_Covers(geom, pt) ORDER BY nivel_riesgo DESC LIMIT 1;
    v_pi_base := COALESCE(v_pi_base, 1.0);
    v_pi_fen  := LEAST(5.0, v_pi_base * v_indice_fen);

    SELECT EXISTS(SELECT 1 FROM deslizamientos WHERE ST_Covers(geom, pt) AND activo = TRUE) INTO v_desl;
    SELECT nivel_riesgo::NUMERIC INTO v_pd FROM deslizamientos
    WHERE ST_Covers(geom, pt) AND activo = TRUE ORDER BY nivel_riesgo DESC LIMIT 1;
    v_pd := COALESCE(v_pd, 1.0);

    SELECT EXISTS(SELECT 1 FROM zonas_tsunami WHERE ST_Covers(geom, pt)) INTO v_tsunami;
    SELECT nivel_riesgo::NUMERIC INTO v_pt_tsun FROM zonas_tsunami
    WHERE ST_Covers(geom, pt) ORDER BY nivel_riesgo DESC LIMIT 1;
    v_pt_tsun := COALESCE(v_pt_tsun, 1.0);

    SELECT COUNT(*)::INTEGER INTO v_n_fallas FROM fallas
    WHERE activa = TRUE AND ST_DWithin(geom::geography, pt::geography, 50000);
    v_pf := LEAST(5.0, 1.0 + v_n_fallas::NUMERIC * 0.5);

    v_indice := ROUND(LEAST(5.0, GREATEST(1.0,
        0.40 * v_ps + 0.25 * v_pi_fen + 0.20 * v_pd + 0.10 * v_pt_tsun + 0.05 * v_pf
    ))::NUMERIC, 2);

    v_nivel_txt := CASE
        WHEN v_indice >= 4.5 THEN 'MUY ALTO' WHEN v_indice >= 3.5 THEN 'ALTO'
        WHEN v_indice >= 2.5 THEN 'MEDIO'    WHEN v_indice >= 1.5 THEN 'BAJO'
        ELSE 'MUY BAJO' END;

    v_recom := jsonb_build_array();
    IF v_zona_sis >= 4 THEN
        v_recom := v_recom || '["Diseño sismorresistente NTE E.060 ductilidad especial Zona 4"]'::jsonb
                           || '["Estudio microzonificación sísmica CISMID recomendado"]'::jsonb;
    END IF;
    IF v_zona_sis >= 3 THEN
        v_recom := v_recom || '["Refuerzo sísmico obligatorio — NTE E.030 Zona 3/4"]'::jsonb
                           || '["Estudio mecánica de suelos EMS obligatorio — NTE E.050"]'::jsonb;
    END IF;
    IF v_inundacion THEN
        v_recom := v_recom || '["Cota mínima sobre nivel de inundación (ANA/RNE E.060)"]'::jsonb
                           || '["Estudio hidrológico e hidráulico ANA recomendado"]'::jsonb;
    END IF;
    IF v_indice_fen > 2.0 THEN
        v_recom := v_recom || (
            '["Zona alto riesgo FEN (×' || v_indice_fen || ') — diseñar caudales 50 años FEN"]'
        )::jsonb;
    END IF;
    IF v_desl THEN
        v_recom := v_recom || '["Zona remoción en masa — estudio geotécnico INGEMMET obligatorio"]'::jsonb;
    END IF;
    IF v_tsunami THEN
        v_recom := v_recom || '["Zona tsunamigénica — altura mínima 15m snm o construcción resistente"]'::jsonb;
    END IF;
    IF v_n_fallas > 0 THEN
        v_recom := v_recom || (
            '["' || v_n_fallas || ' falla(s) activa(s) 50km — retroceso mínimo 50m (NTE E.030)"]'
        )::jsonb;
    END IF;

    RETURN jsonb_build_object(
        'coordenadas',   jsonb_build_object('lon', p_lon, 'lat', p_lat),
        'departamento',  v_depto,
        'distrito',      v_distrito,
        'zona_sismica',  jsonb_build_object(
            'zona', v_zona_sis, 'factor_z', v_factor_z,
            'tipo_suelo_aprox', v_tipo_suelo,
            'norma', 'NTE E.030-2018 DS N°003-2016-VIVIENDA'
        ),
        'precipitacion', jsonb_build_object(
            'zona_climatica', v_zona_prec, 'indice_fen', v_indice_fen,
            'fuente', 'SENAMHI/CHIRPS 2024 + NOAA-CPC'
        ),
        'peligros', jsonb_build_object(
            'sismico',       jsonb_build_object('valor', v_ps, 'sismos_50km_30a', v_n_sismos, 'mag_max', v_mag_max),
            'inundacion',    jsonb_build_object('valor_base', v_pi_base, 'valor_fen', v_pi_fen, 'en_zona', v_inundacion),
            'deslizamiento', jsonb_build_object('valor', v_pd, 'en_zona', v_desl),
            'tsunami',       jsonb_build_object('valor', v_pt_tsun, 'en_zona', v_tsunami),
            'fallas_activas',jsonb_build_object('valor', v_pf, 'n_fallas_50km', v_n_fallas)
        ),
        'indice_riesgo_construccion', v_indice,
        'nivel_riesgo',   v_nivel_txt,
        'ponderacion',    '40% sísmico + 25% inundación×FEN + 20% deslizamiento + 10% tsunami + 5% fallas',
        'metodologia',    'CENEPRED 2014 + NTE E.030-2018 + NTE E.031-2020 + SENAMHI/NOAA v8.0',
        'recomendaciones', v_recom
    );
END;
$$ LANGUAGE plpgsql STABLE;


-- ══════════════════════════════════════════════════════════════════════════
--  FUNCIÓN: f_riesgo_punto(lon, lat) — v8.0
-- ══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_riesgo_punto(p_lon FLOAT, p_lat FLOAT)
RETURNS JSONB AS $$
DECLARE
    pt            GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
    depto_nombre  TEXT;
    depto_riesgo  SMALLINT;
    zona_sis      SMALLINT;
    factor_z      NUMERIC;
    n_sismos_50   BIGINT;
    max_mag_50    NUMERIC;
    en_tsunami    BOOLEAN;
    en_inund      BOOLEAN;
    en_desl       BOOLEAN;
    infra_cercana JSONB;
    riesgo_constr JSONB;
    zona_precip   JSONB;
    fen_reciente  JSONB;
BEGIN
    SELECT nombre, nivel_riesgo, zona_sismica, factor_z
    INTO depto_nombre, depto_riesgo, zona_sis, factor_z
    FROM departamentos WHERE ST_Covers(geom, pt) ORDER BY nivel_riesgo DESC LIMIT 1;

    IF depto_nombre IS NULL THEN
        SELECT nombre, nivel_riesgo, zona_sismica, factor_z
        INTO depto_nombre, depto_riesgo, zona_sis, factor_z
        FROM departamentos ORDER BY geom <-> pt LIMIT 1;
    END IF;

    SELECT COUNT(*), ROUND(MAX(magnitud)::NUMERIC, 1) INTO n_sismos_50, max_mag_50
    FROM sismos
    WHERE ST_DWithin(geom::geography, pt::geography, 50000)
      AND fecha >= CURRENT_DATE - INTERVAL '10 years';

    SELECT EXISTS(SELECT 1 FROM zonas_tsunami    WHERE ST_Covers(geom, pt)) INTO en_tsunami;
    SELECT EXISTS(SELECT 1 FROM zonas_inundables WHERE ST_Covers(geom, pt)) INTO en_inund;
    SELECT EXISTS(SELECT 1 FROM deslizamientos   WHERE ST_Covers(geom, pt) AND activo = TRUE) INTO en_desl;

    SELECT jsonb_agg(jsonb_build_object(
        'nombre', nombre, 'tipo', tipo, 'fuente_tipo', fuente_tipo,
        'distancia_km', ROUND((ST_Distance(geom::geography, pt::geography) / 1000)::NUMERIC, 2)
    ) ORDER BY geom::geography <-> pt::geography)
    INTO infra_cercana
    FROM (
        SELECT nombre, tipo, fuente_tipo, geom FROM infraestructura
        WHERE ST_DWithin(geom::geography, pt::geography, 10000)
          AND tipo IN ('hospital','bomberos','policia','refugio')
        ORDER BY geom::geography <-> pt::geography LIMIT 5
    ) sub;

    SELECT jsonb_build_object(
        'nombre', nombre, 'tipo', tipo,
        'precipitacion_anual_mm', ROUND(precipitacion_anual_mm::NUMERIC, 1),
        'indice_fen', ROUND(indice_fen::NUMERIC, 2),
        'nivel_riesgo_inundacion', nivel_riesgo_inundacion
    ) INTO zona_precip
    FROM zonas_precipitacion ORDER BY geom <-> pt LIMIT 1;

    SELECT jsonb_build_object(
        'año', año_inicio, 'tipo', tipo, 'intensidad', intensidad,
        'oni_peak', ROUND(oni_peak::NUMERIC, 2), 'impacto', impacto_peru
    ) INTO fen_reciente
    FROM eventos_fen WHERE intensidad IN ('fuerte','extraordinario')
    ORDER BY año_inicio DESC LIMIT 1;

    SELECT f_riesgo_construccion(p_lon, p_lat) INTO riesgo_constr;

    RETURN jsonb_build_object(
        'coordenadas',         jsonb_build_object('lon', p_lon, 'lat', p_lat),
        'departamento',        depto_nombre,
        'nivel_riesgo',        depto_riesgo,
        'zona_sismica',        jsonb_build_object('zona', zona_sis, 'factor_z', factor_z),
        'amenazas',            jsonb_build_object(
            'zona_tsunami', en_tsunami, 'zona_inundable', en_inund, 'zona_desliz', en_desl,
            'sismos_50km_10a', n_sismos_50, 'magnitud_max_50km', max_mag_50
        ),
        'precipitacion',           COALESCE(zona_precip,  '{}'::jsonb),
        'fen_reciente_intenso',    COALESCE(fen_reciente, '{}'::jsonb),
        'riesgo_construccion',     riesgo_constr,
        'infraestructura_cercana', COALESCE(infra_cercana, '[]'::jsonb)
    );
END;
$$ LANGUAGE plpgsql STABLE;


-- ══════════════════════════════════════════════════════════════════════════
--  VISTAS ESTÁTICAS
-- ══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_estadisticas_anio AS
SELECT
    CAST(EXTRACT(YEAR FROM fecha) AS INTEGER)              AS anio,
    COUNT(*)                                               AS cantidad,
    ROUND(MAX(magnitud)::NUMERIC, 1)                       AS magnitud_max,
    ROUND(AVG(magnitud)::NUMERIC, 2)                       AS magnitud_prom,
    ROUND(MIN(magnitud)::NUMERIC, 1)                       AS magnitud_min,
    COUNT(*) FILTER (WHERE tipo_profundidad='superficial') AS superficiales,
    COUNT(*) FILTER (WHERE tipo_profundidad='intermedio')  AS intermedios,
    COUNT(*) FILTER (WHERE tipo_profundidad='profundo')    AS profundos,
    COUNT(*) FILTER (WHERE magnitud >= 5.0)                AS m5_plus,
    COUNT(*) FILTER (WHERE magnitud >= 6.0)                AS m6_plus,
    COUNT(*) FILTER (WHERE magnitud >= 7.0)                AS m7_plus
FROM sismos
GROUP BY EXTRACT(YEAR FROM fecha)
ORDER BY anio;


CREATE OR REPLACE VIEW v_sismos_por_depto AS
SELECT
    dep.nombre AS departamento, dep.nivel_riesgo, dep.zona_sismica, dep.factor_z,
    COUNT(s.id) AS total_sismos,
    ROUND(MAX(s.magnitud)::NUMERIC, 1)                       AS max_magnitud,
    ROUND(AVG(s.magnitud)::NUMERIC, 2)                       AS avg_magnitud,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 5.0)             AS m5_plus,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 6.0)             AS m6_plus,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 7.0)             AS m7_plus,
    COUNT(s.id) FILTER (WHERE s.fecha >= CURRENT_DATE - INTERVAL '365 days') AS ultimo_anio
FROM departamentos dep
LEFT JOIN sismos s ON ST_Covers(dep.geom, s.geom)
GROUP BY dep.nombre, dep.nivel_riesgo, dep.zona_sismica, dep.factor_z
ORDER BY total_sismos DESC;


CREATE OR REPLACE VIEW v_infraestructura_cobertura AS
SELECT
    tipo, fuente_tipo,
    COUNT(*)                                         AS total,
    COUNT(*) FILTER (WHERE region IS NOT NULL)       AS con_region,
    COUNT(*) FILTER (WHERE zona_sismica IS NOT NULL) AS con_zona_sismica,
    COUNT(DISTINCT region)                           AS regiones_distintas,
    MAX(criticidad) AS criticidad_max, ROUND(AVG(criticidad)::NUMERIC, 2) AS criticidad_prom
FROM infraestructura
GROUP BY tipo, fuente_tipo
ORDER BY tipo, fuente_tipo;


CREATE OR REPLACE VIEW v_precipitacion_resumen AS
SELECT
    tipo, COUNT(*) AS zonas,
    ROUND(MIN(precipitacion_anual_mm)::NUMERIC, 0) AS precip_min_mm,
    ROUND(MAX(precipitacion_anual_mm)::NUMERIC, 0) AS precip_max_mm,
    ROUND(AVG(precipitacion_anual_mm)::NUMERIC, 0) AS precip_prom_mm,
    ROUND(MAX(indice_fen)::NUMERIC, 2) AS fen_max,
    ROUND(MIN(indice_fen)::NUMERIC, 2) AS fen_min,
    COUNT(*) FILTER (WHERE nivel_riesgo_inundacion >= 4) AS zonas_riesgo_alto
FROM zonas_precipitacion
GROUP BY tipo ORDER BY precip_prom_mm DESC;


CREATE OR REPLACE VIEW v_fen_periodo_retorno AS
SELECT
    tipo, intensidad, COUNT(*) AS total_eventos,
    ROUND((MAX(año_inicio) - MIN(año_inicio))::NUMERIC / NULLIF(COUNT(*) - 1, 0), 1) AS periodo_retorno_anios,
    ROUND(AVG(ABS(oni_peak))::NUMERIC, 2) AS oni_prom,
    ROUND(MAX(ABS(oni_peak))::NUMERIC, 2) AS oni_max,
    MIN(año_inicio) AS primer_evento, MAX(año_inicio) AS ultimo_evento
FROM eventos_fen
GROUP BY tipo, intensidad ORDER BY tipo, oni_prom DESC;


-- ══════════════════════════════════════════════════════════════════════════
--  VISTA MATERIALIZADA: mv_heatmap_sismos
-- ══════════════════════════════════════════════════════════════════════════
DROP MATERIALIZED VIEW IF EXISTS mv_heatmap_sismos;
CREATE MATERIALIZED VIEW mv_heatmap_sismos AS
SELECT
    ST_AsText(ST_SnapToGrid(geom, 0.1)) AS grid_key,
    ST_SnapToGrid(geom, 0.1)            AS geom_grid,
    COUNT(*)                             AS cantidad,
    ROUND(AVG(magnitud)::NUMERIC, 2)     AS magnitud_prom,
    ROUND(MAX(magnitud)::NUMERIC, 1)     AS magnitud_max,
    ROUND(AVG(profundidad_km)::NUMERIC, 1) AS prof_prom
FROM sismos WHERE magnitud >= 3.0
GROUP BY ST_SnapToGrid(geom, 0.1) HAVING COUNT(*) > 0;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_heatmap_key    ON mv_heatmap_sismos(grid_key);
CREATE INDEX IF NOT EXISTS idx_mv_heatmap_geom          ON mv_heatmap_sismos USING GIST(geom_grid);
CREATE INDEX IF NOT EXISTS idx_mv_heatmap_cantidad      ON mv_heatmap_sismos(cantidad DESC);


-- ══════════════════════════════════════════════════════════════════════════
--  VISTA MATERIALIZADA: mv_riesgo_construccion — v9.1 (IRC con suelo E.031)
--  Ponderación: 40%PS×Fs + 25%PI_fen + 20%PD + 10%PT + 5%PF
--  Fuente: CENEPRED 2014 + NTE E.030-2018 + NTE E.031-2020 + SENAMHI/NOAA
--
--  NUEVO v9.1:
--   · Clasificación suelo NTE E.031-2020 multi-criterio (geología+hidrología)
--   · Factor de amplificación sísmica Fs por perfil de suelo (E.031 §3.4)
--   · Períodos Tp y TL por perfil de suelo
--   · Intensidad MMI estimada (Wald et al. 1999) en sismo más fuerte cercano
--   · factor_suelo_amplif incorporado al IRC
-- ══════════════════════════════════════════════════════════════════════════
DROP MATERIALIZED VIEW IF EXISTS mv_riesgo_construccion;
CREATE MATERIALIZED VIEW mv_riesgo_construccion AS
WITH zona_efectiva AS (
    SELECT d.id,
        COALESCE(d.zona_sismica, zsd.zona_sismica, 2) AS zona_sismica_eff,
        COALESCE(dep.factor_z, zsd.factor_z, 0.25)    AS factor_z_eff
    FROM distritos d
    LEFT JOIN zona_sismica_departamento zsd
        ON unaccent(lower(d.departamento)) = unaccent(lower(zsd.departamento))
    LEFT JOIN departamentos dep
        ON unaccent(lower(dep.nombre)) = unaccent(lower(d.departamento))
),
fen_por_distrito AS (
    SELECT DISTINCT ON (d.id) d.id, COALESCE(zp.indice_fen, 1.0) AS indice_fen
    FROM distritos d
    LEFT JOIN LATERAL (
        SELECT indice_fen FROM zonas_precipitacion ORDER BY geom <-> ST_Centroid(d.geom) LIMIT 1
    ) zp ON TRUE
),
-- ═══ NTE E.031-2020: Clasificación de suelo multi-criterio ═══
-- Criterios jerárquicos:
--   S4 (condiciones excepcionales): zona tsunami alta, suelos licuefactibles, rellenos
--   S3 (suelos blandos): zona inundable alta, selva baja, cuenca aluvial amazónica
--   S2 (suelos intermedios): costa Z4 con depósitos profundos, valles interandinos
--   S1 (roca/suelo muy rígido): sierra alta, zona volcánica rígida, afloramientos
--   S0 (roca dura): base rocosa andina >3500m sin cobertura aluvial
suelo_distrito AS (
    SELECT d.id,
        CASE
            -- S4: condiciones excepcionales (licuefacción, tsunami alto, rellenos)
            WHEN EXISTS(SELECT 1 FROM zonas_tsunami zt
                        WHERE zt.nivel_riesgo >= 4 AND ST_Intersects(zt.geom, d.geom))
            THEN 'S4'
            WHEN EXISTS(SELECT 1 FROM zonas_tsunami zt WHERE zt.nivel_riesgo >= 3
                        AND ST_Intersects(zt.geom, d.geom))
                 AND EXISTS(SELECT 1 FROM zonas_inundables zi WHERE zi.nivel_riesgo >= 3
                            AND ST_Intersects(zi.geom, d.geom))
            THEN 'S4'

            -- S3: suelos blandos — selva baja aluvial, llanuras inundables
            WHEN EXISTS(SELECT 1 FROM zonas_inundables zi
                        WHERE zi.nivel_riesgo >= 4 AND ST_Intersects(zi.geom, d.geom))
            THEN 'S3'
            WHEN d.departamento ILIKE ANY(ARRAY[
                '%loreto%','%ucayali%','%madre de dios%'
            ]) AND ST_Y(ST_Centroid(d.geom)) > -12.0
            THEN 'S3'
            WHEN d.departamento ILIKE ANY(ARRAY[
                '%amazonas%','%san mart%','%huanuco%','%huánuco%'
            ]) AND EXISTS(SELECT 1 FROM zonas_inundables zi
                          WHERE zi.nivel_riesgo >= 2 AND ST_Intersects(zi.geom, d.geom))
            THEN 'S3'

            -- S2: suelos intermedios — costa profunda, valles interandinos
            WHEN ze.zona_sismica_eff = 4 AND NOT EXISTS(
                SELECT 1 FROM deslizamientos dl
                WHERE dl.activo AND dl.nivel_riesgo >= 3 AND ST_Intersects(dl.geom, d.geom))
            THEN 'S2'
            WHEN ze.zona_sismica_eff = 3 AND d.departamento ILIKE ANY(ARRAY[
                '%cajamarca%','%junin%','%junín%','%huancavelica%','%cusco%'
            ])
            THEN 'S2'

            -- S0: roca dura — afloramientos andinos alta montaña (>3500m proxy)
            WHEN ze.zona_sismica_eff IN (1,2)
                 AND NOT EXISTS(SELECT 1 FROM zonas_inundables zi
                                WHERE ST_Intersects(zi.geom, d.geom))
                 AND NOT EXISTS(SELECT 1 FROM deslizamientos dl
                                WHERE dl.activo AND ST_Intersects(dl.geom, d.geom))
                 AND d.departamento ILIKE ANY(ARRAY[
                    '%puno%','%apurímac%','%apurimac%','%ayacucho%'
                 ])
            THEN 'S0'

            -- S1: roca/suelo rígido (default)
            ELSE 'S1'
        END AS clasificacion_suelo,
        -- Factor de amplificación sísmica Fs (NTE E.031-2020 Tabla 3)
        -- Depende de perfil de suelo y zona sísmica
        CASE
            WHEN EXISTS(SELECT 1 FROM zonas_tsunami zt WHERE zt.nivel_riesgo >= 4
                        AND ST_Intersects(zt.geom, d.geom))
            THEN -- S4: requiere estudio especial, usar factor conservador
                CASE ze.zona_sismica_eff WHEN 4 THEN 3.00 WHEN 3 THEN 3.00
                     WHEN 2 THEN 2.80 ELSE 2.40 END
            WHEN EXISTS(SELECT 1 FROM zonas_inundables zi WHERE zi.nivel_riesgo >= 4
                        AND ST_Intersects(zi.geom, d.geom))
                 OR (d.departamento ILIKE ANY(ARRAY['%loreto%','%ucayali%','%madre de dios%'])
                     AND ST_Y(ST_Centroid(d.geom)) > -12.0)
            THEN -- S3
                CASE ze.zona_sismica_eff WHEN 4 THEN 1.10 WHEN 3 THEN 1.20
                     WHEN 2 THEN 1.50 ELSE 1.60 END
            WHEN ze.zona_sismica_eff = 4 THEN 1.05  -- S2 Z4
            WHEN ze.zona_sismica_eff = 3 THEN 1.15  -- S2 Z3
            WHEN ze.zona_sismica_eff IN (1,2) AND NOT EXISTS(
                SELECT 1 FROM zonas_inundables zi WHERE ST_Intersects(zi.geom, d.geom))
                 AND d.departamento ILIKE ANY(ARRAY[
                    '%puno%','%apurímac%','%apurimac%','%ayacucho%'])
            THEN 0.80  -- S0
            ELSE 1.00  -- S1
        END AS factor_suelo_s,
        -- Período predominante Tp (s) — NTE E.031-2020 Tabla 4
        CASE
            WHEN EXISTS(SELECT 1 FROM zonas_tsunami zt WHERE zt.nivel_riesgo >= 4
                        AND ST_Intersects(zt.geom, d.geom)) THEN 1.00  -- S4
            WHEN EXISTS(SELECT 1 FROM zonas_inundables zi WHERE zi.nivel_riesgo >= 4
                        AND ST_Intersects(zi.geom, d.geom)) THEN 1.00  -- S3
            WHEN ze.zona_sismica_eff = 4 THEN 0.60  -- S2
            WHEN ze.zona_sismica_eff IN (1,2) AND d.departamento ILIKE ANY(ARRAY[
                '%puno%','%apurímac%','%apurimac%','%ayacucho%']) THEN 0.30  -- S0
            ELSE 0.40  -- S1
        END AS tp_suelo,
        -- Período largo TL (s) — NTE E.031-2020 Tabla 4
        CASE
            WHEN EXISTS(SELECT 1 FROM zonas_tsunami zt WHERE zt.nivel_riesgo >= 4
                        AND ST_Intersects(zt.geom, d.geom)) THEN 1.60  -- S4
            WHEN EXISTS(SELECT 1 FROM zonas_inundables zi WHERE zi.nivel_riesgo >= 4
                        AND ST_Intersects(zi.geom, d.geom)) THEN 1.60  -- S3
            WHEN ze.zona_sismica_eff = 4 THEN 2.00  -- S2
            WHEN ze.zona_sismica_eff IN (1,2) AND d.departamento ILIKE ANY(ARRAY[
                '%puno%','%apurímac%','%apurimac%','%ayacucho%']) THEN 3.00  -- S0
            ELSE 2.50  -- S1
        END AS tl_suelo
    FROM distritos d
    JOIN zona_efectiva ze ON d.id = ze.id
),
-- Sismo más fuerte cercano en 50km últimos 30 años (para intensidad MMI)
sismo_max_cercano AS (
    SELECT DISTINCT ON (d.id)
        d.id,
        s.magnitud AS mag_max_cercana,
        ROUND((ST_Distance(s.geom::geography, ST_Centroid(d.geom)::geography) / 1000)::NUMERIC, 1)
            AS dist_epicentro_km,
        -- Intensidad MMI estimada (Wald et al. 1999 simplificado)
        -- MMI ≈ 1.0 + 1.47×M - 0.0031×R² (válido para R<200km)
        LEAST(12.0, GREATEST(1.0, ROUND((
            1.0 + 1.47 * s.magnitud
            - 0.0031 * POWER(
                LEAST(200.0, ST_Distance(s.geom::geography, ST_Centroid(d.geom)::geography) / 1000),
                2)
        )::NUMERIC, 1))) AS mmi_estimada
    FROM distritos d
    CROSS JOIN LATERAL (
        SELECT magnitud, geom FROM sismos
        WHERE magnitud >= 4.0
          AND fecha >= CURRENT_DATE - INTERVAL '30 years'
          AND ST_DWithin(geom::geography, ST_Centroid(d.geom)::geography, 50000)
        ORDER BY magnitud DESC
        LIMIT 1
    ) s
)
SELECT
    d.id, d.ubigeo,
    d.nombre AS distrito, d.provincia, d.departamento,
    d.nivel_riesgo,
    ze.zona_sismica_eff AS zona_sismica,
    ze.factor_z_eff     AS factor_z,
    d.poblacion, d.area_km2,
    -- Clasificación suelo NTE E.031-2020 (multi-criterio)
    sd.clasificacion_suelo,
    sd.factor_suelo_s,
    sd.tp_suelo,
    sd.tl_suelo,
    -- Peligros individuales
    CASE ze.zona_sismica_eff WHEN 4 THEN 5 WHEN 3 THEN 4 WHEN 2 THEN 3 WHEN 1 THEN 2 ELSE 3 END AS peligro_sismico,
    COALESCE((SELECT MAX(zi.nivel_riesgo) FROM zonas_inundables zi WHERE ST_Intersects(zi.geom, d.geom)), 1) AS peligro_inundacion,
    LEAST(5.0, COALESCE((SELECT MAX(zi.nivel_riesgo) FROM zonas_inundables zi WHERE ST_Intersects(zi.geom, d.geom)), 1) * fp.indice_fen) AS peligro_inundacion_fen,
    fp.indice_fen,
    COALESCE((SELECT MAX(dl.nivel_riesgo) FROM deslizamientos dl WHERE ST_Intersects(dl.geom, d.geom) AND dl.activo = TRUE), 1) AS peligro_deslizamiento,
    COALESCE((SELECT MAX(zt.nivel_riesgo) FROM zonas_tsunami zt WHERE ST_Intersects(zt.geom, d.geom)), 1) AS peligro_tsunami,
    (SELECT COUNT(*)::INTEGER FROM fallas f WHERE f.activa = TRUE
     AND ST_DWithin(f.geom::geography, ST_Centroid(d.geom)::geography, 50000)) AS fallas_activas_50km,
    (SELECT COUNT(*)::INTEGER FROM sismos s WHERE s.magnitud >= 4.0
     AND s.fecha >= CURRENT_DATE - INTERVAL '30 years'
     AND ST_DWithin(s.geom::geography, ST_Centroid(d.geom)::geography, 50000)) AS sismos_m4_30a_50km,
    -- Intensidad MMI del sismo más fuerte cercano
    COALESCE(smc.mag_max_cercana, 0) AS mag_max_cercana_50km,
    COALESCE(smc.dist_epicentro_km, 0) AS dist_epicentro_km,
    COALESCE(smc.mmi_estimada, 1.0) AS mmi_estimada,
    -- IRC v8 con amplificación de suelo E.031
    LEAST(5.0, GREATEST(1.0, ROUND((
        0.40 * CASE ze.zona_sismica_eff WHEN 4 THEN 5 WHEN 3 THEN 4 WHEN 2 THEN 3 WHEN 1 THEN 2 ELSE 3 END
             * LEAST(1.5, sd.factor_suelo_s / 1.0)  -- amplificación suelo normalizada
      + 0.25 * LEAST(5.0, COALESCE((SELECT MAX(zi.nivel_riesgo) FROM zonas_inundables zi WHERE ST_Intersects(zi.geom, d.geom)), 1) * fp.indice_fen)
      + 0.20 * COALESCE((SELECT MAX(dl.nivel_riesgo) FROM deslizamientos dl WHERE ST_Intersects(dl.geom, d.geom) AND dl.activo = TRUE), 1)
      + 0.10 * COALESCE((SELECT MAX(zt.nivel_riesgo) FROM zonas_tsunami zt WHERE ST_Intersects(zt.geom, d.geom)), 1)
      + 0.05 * LEAST(5.0, 1.0 + (SELECT COUNT(*)::NUMERIC FROM fallas f WHERE f.activa = TRUE
                                  AND ST_DWithin(f.geom::geography, ST_Centroid(d.geom)::geography, 50000)) * 0.5)
    )::NUMERIC, 2))) AS indice_riesgo_construccion,
    ST_AsText(ST_Centroid(d.geom)) AS centroide_wkt
FROM distritos d
JOIN zona_efectiva ze ON d.id = ze.id
JOIN fen_por_distrito fp ON d.id = fp.id
JOIN suelo_distrito sd ON d.id = sd.id
LEFT JOIN sismo_max_cercano smc ON d.id = smc.id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_riesgo_id     ON mv_riesgo_construccion(id);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_indice        ON mv_riesgo_construccion(indice_riesgo_construccion DESC);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_depto         ON mv_riesgo_construccion(departamento, indice_riesgo_construccion DESC);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_zona          ON mv_riesgo_construccion(zona_sismica DESC, indice_riesgo_construccion DESC);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_suelo         ON mv_riesgo_construccion(clasificacion_suelo);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_fen           ON mv_riesgo_construccion(indice_fen DESC, indice_riesgo_construccion DESC);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_mmi           ON mv_riesgo_construccion(mmi_estimada DESC);


-- ══════════════════════════════════════════════════════════════════════════
--  🆕 VISTA MATERIALIZADA: riesgo_percentiles (v9.0)
--  Para módulo Sendai Framework Target B/C
--  Percentil nacional y departamental de IRC v9
-- ══════════════════════════════════════════════════════════════════════════
CREATE MATERIALIZED VIEW IF NOT EXISTS riesgo_percentiles AS
SELECT
    d.ubigeo,
    d.nombre                                                     AS nombre_distrito,
    d.departamento,
    d.indice_riesgo_v9,
    PERCENT_RANK() OVER (ORDER BY d.indice_riesgo_v9)           AS percentil_nacional,
    PERCENT_RANK() OVER (
        PARTITION BY LEFT(d.ubigeo, 2) ORDER BY d.indice_riesgo_v9
    )                                                            AS percentil_departamental
FROM distritos d
WHERE d.indice_riesgo_v9 IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_riesgo_percentiles_ubigeo
    ON riesgo_percentiles(ubigeo);
CREATE INDEX IF NOT EXISTS idx_riesgo_percentiles_dep
    ON riesgo_percentiles(departamento, percentil_nacional DESC);


-- ══════════════════════════════════════════════════════════════════════════
--  CONFIRMACIÓN v9.0
-- ══════════════════════════════════════════════════════════════════════════
DO $$
DECLARE
    v_sismos        BIGINT;
    v_distritos     BIGINT;
    v_dist_v9       BIGINT;
    v_dist_null_v9  BIGINT;
    v_volcanes      BIGINT;
    v_alertas       BIGINT;
    v_suscept       BIGINT;
    v_exposicion    BIGINT;
    v_precip        BIGINT;
    v_fen           BIGINT;
    v_mv_riesgo     BIGINT;
    v_mv_percentil  BIGINT;
    v_ts_ok         BOOLEAN;
BEGIN
    SELECT COUNT(*) INTO v_sismos     FROM sismos;
    SELECT COUNT(*) INTO v_distritos  FROM distritos;
    SELECT COUNT(*) INTO v_dist_v9    FROM distritos WHERE indice_riesgo_v9 IS NOT NULL;
    SELECT COUNT(*) INTO v_dist_null_v9 FROM distritos WHERE indice_riesgo_v9 IS NULL;
    SELECT COUNT(*) INTO v_volcanes   FROM volcanes;
    SELECT COUNT(*) INTO v_alertas    FROM alertas_rt;
    SELECT COUNT(*) INTO v_suscept    FROM susceptibilidad_ml;
    SELECT COUNT(*) INTO v_exposicion FROM exposicion_distritos;
    SELECT COUNT(*) INTO v_precip     FROM zonas_precipitacion;
    SELECT COUNT(*) INTO v_fen        FROM eventos_fen;
    SELECT COUNT(*) INTO v_mv_riesgo  FROM mv_riesgo_construccion;

    BEGIN
        SELECT COUNT(*) INTO v_mv_percentil FROM riesgo_percentiles;
    EXCEPTION WHEN OTHERS THEN v_mv_percentil := 0;
    END;

    SELECT EXISTS(
        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
    ) INTO v_ts_ok;

    RAISE NOTICE '══════════════════════════════════════════════════════════════';
    RAISE NOTICE '✅ GeoRiesgo Perú v9.0 ENTERPRISE — %', NOW();
    RAISE NOTICE '══════════════════════════════════════════════════════════════';
    RAISE NOTICE '  TimescaleDB:              %', CASE WHEN v_ts_ok THEN '✓ activo' ELSE '⚠ no instalado' END;
    RAISE NOTICE '  sismos:                   % registros', v_sismos;
    RAISE NOTICE '  distritos total:          % (% con IRC v9, % pendientes)', v_distritos, v_dist_v9, v_dist_null_v9;
    RAISE NOTICE '  volcanes:                 % (INGEMMET/OVI-IGP 2021)', v_volcanes;
    RAISE NOTICE '  alertas_rt:               % alertas EWS', v_alertas;
    RAISE NOTICE '  susceptibilidad_ml:       % predicciones', v_suscept;
    RAISE NOTICE '  exposicion_distritos:     % distritos', v_exposicion;
    RAISE NOTICE '  zonas_precipitacion:      % zonas SENAMHI/CHIRPS', v_precip;
    RAISE NOTICE '  eventos_fen:              % eventos NOAA-CPC', v_fen;
    RAISE NOTICE '  mv_riesgo_construccion:   % filas (IRC v8 + FEN)', v_mv_riesgo;
    RAISE NOTICE '  riesgo_percentiles:       % filas (Sendai B/C)', v_mv_percentil;
    RAISE NOTICE '══════════════════════════════════════════════════════════════';
    RAISE NOTICE '  Índices estrategia v9: SP-GIST puntos, GIST polígonos, BRIN temporal';
    RAISE NOTICE '  Fuente: Crunchy Data 2025 spatial indexes benchmarks';
    RAISE NOTICE '══════════════════════════════════════════════════════════════';

    IF v_dist_null_v9 > 0 THEN
        RAISE NOTICE '  ℹ  % distritos sin IRC v9 — ejecutar: python procesar_datos.py --solo irc_v9', v_dist_null_v9;
    END IF;
    IF v_volcanes = 0 THEN
        RAISE NOTICE '  ℹ  volcanes vacío — ejecutar: python procesar_datos.py --solo volcanes';
    END IF;
    IF v_precip = 0 THEN
        RAISE NOTICE '  ℹ  precipitaciones vacío — ejecutar: python procesar_datos.py --solo precipitaciones';
    END IF;
    IF v_fen = 0 THEN
        RAISE NOTICE '  ℹ  eventos_fen vacío — ejecutar: python procesar_datos.py --solo eventos_fen';
    END IF;
END $$;