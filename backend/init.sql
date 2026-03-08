-- ══════════════════════════════════════════════════════════
-- GeoRiesgo Perú — Esquema PostGIS v7.0
-- NUEVAS MEJORAS:
--   ✅ zona_sismica (NTE E.030-2018) en departamentos, distritos, infraestructura
--   ✅ factor_z (0.10/0.25/0.35/0.45g) en departamentos
--   ✅ fuente_tipo ('oficial'/'osm') en infraestructura
--   ✅ mv_riesgo_construccion — índice compuesto de riesgo por distrito
--   ✅ f_riesgo_construccion(lon,lat) — riesgo en punto específico
--   ✅ v_infraestructura_cobertura — diagnóstico de cobertura por tipo
--   ✅ Índices mejorados para queries de riesgo
--   ✅ Todos los checks ST_Covers anteriores mantenidos
-- ══════════════════════════════════════════════════════════

-- Extensiones
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

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

CREATE INDEX IF NOT EXISTS idx_sismos_geom      ON sismos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sismos_fecha     ON sismos(fecha DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_magnitud  ON sismos(magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_profund   ON sismos(profundidad_km);
CREATE INDEX IF NOT EXISTS idx_sismos_tipo_prof ON sismos(tipo_profundidad);
CREATE INDEX IF NOT EXISTS idx_sismos_region    ON sismos(region);
CREATE INDEX IF NOT EXISTS idx_sismos_fecha_mag ON sismos(fecha DESC, magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_anio_mag  ON sismos(DATE_PART('year', fecha), magnitud DESC);
CREATE INDEX IF NOT EXISTS idx_sismos_geom_mag  ON sismos USING GIST(geom) WHERE magnitud >= 3.0;

-- ═══════════════════════════════════════════════════════════
--  DEPARTAMENTOS — con zona sísmica NTE E.030-2018
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS departamentos (
    id             SERIAL PRIMARY KEY,
    ubigeo         TEXT UNIQUE,
    nombre         TEXT NOT NULL,
    geom           GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo   SMALLINT DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    -- Zona Sísmica NTE E.030-2018 (DS N°003-2016-VIVIENDA)
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    -- Factor de zona Z (aceleración espectral en g para periodo 0.2s)
    -- Z4=0.45g, Z3=0.35g, Z2=0.25g, Z1=0.10g
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

-- ═══════════════════════════════════════════════════════════
--  PROVINCIAS
-- ═══════════════════════════════════════════════════════════
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

-- ═══════════════════════════════════════════════════════════
--  DISTRITOS — con zona sísmica y población INEI
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS distritos (
    id             SERIAL PRIMARY KEY,
    ubigeo         TEXT UNIQUE,
    nombre         TEXT NOT NULL,
    provincia      TEXT,
    departamento   TEXT,
    geom           GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo   SMALLINT NOT NULL DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    poblacion      INTEGER,         -- INEI Censos 2017
    area_km2       NUMERIC(10,3),
    -- Zona Sísmica NTE E.030-2018
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    -- Índice de riesgo de construcción (calculado por mv_riesgo_construccion)
    indice_riesgo_construccion NUMERIC(4,2),
    fuente         TEXT DEFAULT 'INEI/GADM',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_distritos_geom       ON distritos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_distritos_nombre     ON distritos USING GIN(nombre gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_distritos_prov       ON distritos(provincia);
CREATE INDEX IF NOT EXISTS idx_distritos_dep        ON distritos(departamento);
CREATE INDEX IF NOT EXISTS idx_distritos_dep_riesgo ON distritos(departamento, nivel_riesgo DESC);
CREATE INDEX IF NOT EXISTS idx_distritos_zona_sis   ON distritos(zona_sismica);
CREATE INDEX IF NOT EXISTS idx_distritos_pob        ON distritos(poblacion DESC NULLS LAST);

-- ═══════════════════════════════════════════════════════════
--  FALLAS GEOLÓGICAS
-- ═══════════════════════════════════════════════════════════
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
CREATE INDEX IF NOT EXISTS idx_fallas_geom    ON fallas USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_fallas_activa  ON fallas(activa);
CREATE INDEX IF NOT EXISTS idx_fallas_tipo    ON fallas(tipo);
CREATE INDEX IF NOT EXISTS idx_fallas_region  ON fallas(region);
CREATE INDEX IF NOT EXISTS idx_fallas_mag_max ON fallas(magnitud_max DESC NULLS LAST)
    WHERE activa = TRUE;

-- ═══════════════════════════════════════════════════════════
--  ZONAS DE INUNDACIÓN
-- ═══════════════════════════════════════════════════════════
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

-- ═══════════════════════════════════════════════════════════
--  DESLIZAMIENTOS Y REMOCIÓN EN MASA
-- ═══════════════════════════════════════════════════════════
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

-- ═══════════════════════════════════════════════════════════
--  INFRAESTRUCTURA CRÍTICA — con fuente_tipo oficial/osm
-- ═══════════════════════════════════════════════════════════
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
    -- NUEVO: Identifica si el dato proviene de fuente oficial o OSM
    -- 'oficial' = SUSALUD/MINEDU/MTC/APN/OSINERGMIN/INDECI
    -- 'osm'     = OpenStreetMap (complemento cuando no hay fuente oficial)
    fuente_tipo    TEXT DEFAULT 'osm' CHECK (fuente_tipo IN ('oficial', 'osm')),
    -- NUEVO: zona sísmica NTE E.030 del punto
    zona_sismica   SMALLINT CHECK (zona_sismica BETWEEN 1 AND 4),
    actualizado_en TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT ck_infra_bbox CHECK (
        ST_X(geom) BETWEEN -83 AND -68
        AND ST_Y(geom) BETWEEN -19 AND 1
    )
);
CREATE INDEX IF NOT EXISTS idx_infra_geom        ON infraestructura USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_infra_tipo        ON infraestructura(tipo);
CREATE INDEX IF NOT EXISTS idx_infra_critic      ON infraestructura(criticidad DESC);
CREATE INDEX IF NOT EXISTS idx_infra_region      ON infraestructura(region);
CREATE INDEX IF NOT EXISTS idx_infra_nombre      ON infraestructura USING GIN(nombre gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_infra_tipo_region ON infraestructura(tipo, region, criticidad DESC);
CREATE INDEX IF NOT EXISTS idx_infra_fuente_tipo ON infraestructura(fuente_tipo);
CREATE INDEX IF NOT EXISTS idx_infra_oficial     ON infraestructura(tipo, criticidad DESC)
    WHERE fuente_tipo = 'oficial';

-- ═══════════════════════════════════════════════════════════
--  ZONAS DE TSUNAMI
-- ═══════════════════════════════════════════════════════════
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

-- ═══════════════════════════════════════════════════════════
--  ESTACIONES DE MONITOREO
-- ═══════════════════════════════════════════════════════════
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
CREATE INDEX IF NOT EXISTS idx_estaciones_geom ON estaciones USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_estaciones_tipo ON estaciones(tipo);
CREATE INDEX IF NOT EXISTS idx_estaciones_inst ON estaciones(institucion);

-- ═══════════════════════════════════════════════════════════
--  LOG DE SINCRONIZACIÓN
-- ═══════════════════════════════════════════════════════════
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


-- ═══════════════════════════════════════════════════════════
--  FUNCIÓN: f_asignar_region(lon, lat) — v7.0
--  3 niveles de fallback: ST_Covers → DWithin 5km → KNN
--  Sin NULL garantizado.
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_asignar_region(p_lon FLOAT, p_lat FLOAT)
RETURNS TEXT AS $$
DECLARE
    v_region TEXT;
    v_pt     GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
BEGIN
    SELECT nombre INTO v_region
    FROM departamentos
    WHERE ST_Covers(geom, v_pt)
    ORDER BY nivel_riesgo DESC
    LIMIT 1;
    IF v_region IS NOT NULL THEN RETURN v_region; END IF;

    SELECT nombre INTO v_region
    FROM departamentos
    WHERE ST_DWithin(geom::geography, v_pt::geography, 5000)
    ORDER BY ST_Distance(geom::geography, v_pt::geography)
    LIMIT 1;
    IF v_region IS NOT NULL THEN RETURN v_region; END IF;

    SELECT nombre INTO v_region
    FROM departamentos ORDER BY geom <-> v_pt LIMIT 1;
    RETURN COALESCE(v_region, 'Perú');
END;
$$ LANGUAGE plpgsql STABLE;


-- ═══════════════════════════════════════════════════════════
--  FUNCIÓN: f_asignar_zona_sismica(lon, lat)
--  Retorna la zona sísmica NTE E.030-2018 de un punto.
--  Ref: DS N°003-2016-VIVIENDA, actualizado 2018
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_asignar_zona_sismica(p_lon FLOAT, p_lat FLOAT)
RETURNS SMALLINT AS $$
DECLARE
    v_zona  SMALLINT;
    v_pt    GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
BEGIN
    SELECT zona_sismica INTO v_zona
    FROM departamentos
    WHERE ST_Covers(geom, v_pt) AND zona_sismica IS NOT NULL
    ORDER BY zona_sismica DESC
    LIMIT 1;
    IF v_zona IS NOT NULL THEN RETURN v_zona; END IF;

    SELECT zona_sismica INTO v_zona
    FROM departamentos
    WHERE zona_sismica IS NOT NULL
    ORDER BY geom <-> v_pt
    LIMIT 1;
    RETURN COALESCE(v_zona, 2);
END;
$$ LANGUAGE plpgsql STABLE;


-- ═══════════════════════════════════════════════════════════
--  FUNCIÓN: f_actualizar_regiones() — v7.0
--  ST_Covers + KNN para ALL tablas
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_actualizar_regiones()
RETURNS TABLE(tabla TEXT, registros_actualizados BIGINT, via_knn BIGINT) AS $$
DECLARE
    n_covers BIGINT;
    n_knn    BIGINT;
BEGIN
    -- SISMOS
    UPDATE sismos s SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, s.geom)
      AND (s.region IS NULL OR s.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE sismos s SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> s.geom LIMIT 1
    ) WHERE s.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'sismos'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- INFRAESTRUCTURA (también actualiza zona_sismica)
    UPDATE infraestructura i SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, i.geom)
      AND (i.region IS NULL OR i.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE infraestructura i SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> i.geom LIMIT 1
    ) WHERE i.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    -- Actualizar zona_sismica en infraestructura
    UPDATE infraestructura i
    SET zona_sismica = d.zona_sismica
    FROM departamentos d
    WHERE ST_Covers(d.geom, i.geom)
      AND d.zona_sismica IS NOT NULL
      AND i.zona_sismica IS DISTINCT FROM d.zona_sismica;
    tabla := 'infraestructura'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- INFRAESTRUCTURA DISTRITOS
    UPDATE infraestructura i
    SET distrito = d.nombre
    FROM distritos d
    WHERE ST_Covers(d.geom, i.geom)
      AND (i.distrito IS NULL OR i.distrito <> d.nombre);
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
    WHERE ST_Covers(d.geom, e.geom)
      AND (e.region IS NULL OR e.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE estaciones e SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> e.geom LIMIT 1
    ) WHERE e.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'estaciones'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- FALLAS (centroide)
    UPDATE fallas f SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(f.geom))
      AND (f.region IS NULL OR f.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE fallas f SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(f.geom) LIMIT 1
    ) WHERE f.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'fallas'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- ZONAS INUNDABLES (centroide)
    UPDATE zonas_inundables zi SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(zi.geom))
      AND (zi.region IS NULL OR zi.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE zonas_inundables zi SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(zi.geom) LIMIT 1
    ) WHERE zi.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'zonas_inundables'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- DESLIZAMIENTOS (centroide)
    UPDATE deslizamientos dl SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(dl.geom))
      AND (dl.region IS NULL OR dl.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE deslizamientos dl SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(dl.geom) LIMIT 1
    ) WHERE dl.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'deslizamientos'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;

    -- TSUNAMIS (centroide)
    UPDATE zonas_tsunami zt SET region = d.nombre
    FROM departamentos d
    WHERE ST_Covers(d.geom, ST_Centroid(zt.geom))
      AND (zt.region IS NULL OR zt.region <> d.nombre);
    GET DIAGNOSTICS n_covers = ROW_COUNT;
    UPDATE zonas_tsunami zt SET region = (
        SELECT d.nombre FROM departamentos d ORDER BY d.geom <-> ST_Centroid(zt.geom) LIMIT 1
    ) WHERE zt.region IS NULL;
    GET DIAGNOSTICS n_knn = ROW_COUNT;
    tabla := 'zonas_tsunami'; registros_actualizados := n_covers; via_knn := n_knn;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;


-- ═══════════════════════════════════════════════════════════
--  FUNCIÓN: f_sismos_cercanos
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_sismos_cercanos(
    p_lon     FLOAT,
    p_lat     FLOAT,
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
        COALESCE(region, f_asignar_region(ST_X(geom), ST_Y(geom))) AS region,
        ROUND(
            (ST_Distance(
                geom::geography,
                ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography
            ) / 1000)::NUMERIC, 1
        ) AS distancia_km
    FROM sismos
    WHERE magnitud >= p_mag_min
      AND ST_DWithin(
          geom::geography,
          ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326)::geography,
          p_radio_km * 1000
      )
    ORDER BY distancia_km ASC
    LIMIT p_limit;
$$ LANGUAGE SQL STABLE;


-- ═══════════════════════════════════════════════════════════
--  FUNCIÓN: f_riesgo_construccion(lon, lat) — v7.0
--  Calcula el índice de riesgo de construcción en un punto.
--
--  METODOLOGÍA (CENEPRED 2014 + NTE E.030-2018):
--    Índice_RC = 0.40*PS + 0.25*PI + 0.20*PD + 0.10*PT + 0.05*PF
--    PS = Peligro Sísmico (zona NTE E.030 normalizada 1-5)
--    PI = Peligro por Inundación (si el punto está en zona inundable)
--    PD = Peligro por Deslizamiento
--    PT = Peligro por Tsunami
--    PF = Peligro por Fallas activas en 50km
--
--  Nota sobre tipo de suelo:
--    El RNE NTE E.031 clasifica suelos S1-S4. Los suelos tipo S3/S4
--    (depósitos sedimentarios costeros, rellenos) amplifican la
--    aceleración sísmica hasta 2-3x. Sin mapa Vs30 oficial para todo
--    Perú (CISMID solo cubre Lima Metropolitana), se usa zona costera
--    como proxy de suelo blando.
-- ═══════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION f_riesgo_construccion(p_lon FLOAT, p_lat FLOAT)
RETURNS JSONB AS $$
DECLARE
    pt             GEOMETRY := ST_SetSRID(ST_MakePoint(p_lon, p_lat), 4326);
    v_depto        TEXT;
    v_distrito     TEXT;
    v_zona_sis     SMALLINT;
    v_factor_z     NUMERIC;
    v_ps           NUMERIC;  -- Peligro sísmico normalizado 1-5
    v_pi           NUMERIC;  -- Peligro inundación
    v_pd           NUMERIC;  -- Peligro deslizamiento
    v_pt_tsun      NUMERIC;  -- Peligro tsunami
    v_pf           NUMERIC;  -- Peligro fallas
    v_indice       NUMERIC;  -- Índice compuesto 1-5
    v_nivel_txt    TEXT;
    v_recom        JSONB;
    v_n_sismos     BIGINT;
    v_mag_max      NUMERIC;
    v_n_fallas     INTEGER;
    v_inundacion   BOOLEAN;
    v_desl         BOOLEAN;
    v_tsunami      BOOLEAN;
    v_tipo_suelo   TEXT;
BEGIN
    -- Departamento y zona sísmica
    SELECT d.nombre, d.zona_sismica, d.factor_z
    INTO v_depto, v_zona_sis, v_factor_z
    FROM departamentos d
    WHERE ST_Covers(d.geom, pt)
    LIMIT 1;

    IF v_zona_sis IS NULL THEN
        SELECT d.zona_sismica, d.factor_z, d.nombre
        INTO v_zona_sis, v_factor_z, v_depto
        FROM departamentos d ORDER BY d.geom <-> pt LIMIT 1;
    END IF;

    v_zona_sis := COALESCE(v_zona_sis, 2);
    v_factor_z  := COALESCE(v_factor_z, 0.25);

    -- Distrito
    SELECT nombre INTO v_distrito
    FROM distritos WHERE ST_Covers(geom, pt) LIMIT 1;

    -- Peligro sísmico: zona sísmica → escala 1-5
    -- Zona4=5, Zona3=4, Zona2=3, Zona1=2 (base mínima 2 en todo Perú)
    v_ps := CASE v_zona_sis
        WHEN 4 THEN 5.0
        WHEN 3 THEN 4.0
        WHEN 2 THEN 3.0
        WHEN 1 THEN 2.0
        ELSE 3.0
    END;

    -- Sismicidad histórica 50km (ajuste fino del peligro sísmico)
    SELECT COUNT(*), ROUND(MAX(magnitud)::NUMERIC, 1)
    INTO v_n_sismos, v_mag_max
    FROM sismos
    WHERE ST_DWithin(geom::geography, pt::geography, 50000)
      AND fecha >= CURRENT_DATE - INTERVAL '30 years'
      AND magnitud >= 4.0;

    -- Ajuste por sismicidad histórica intensa (±0.5)
    IF v_n_sismos > 500 OR v_mag_max >= 8.0 THEN
        v_ps := LEAST(5.0, v_ps + 0.5);
    ELSIF v_n_sismos < 10 AND v_mag_max < 5.0 THEN
        v_ps := GREATEST(1.0, v_ps - 0.3);
    END IF;

    -- Tipo de suelo aproximado (proxy: latitud < -15° y longitud < -70° → costa sur)
    -- Nota: Para análisis site-specific usar estudio CISMID/microzonificación
    IF ST_Y(pt) > -8 AND ST_X(pt) < -76 THEN
        v_tipo_suelo := 'S3 (depósito aluvial costero - Perú norte)';
    ELSIF ST_X(pt) < -75 THEN
        v_tipo_suelo := 'S3 (depósito costero/aluvial)';
    ELSIF ST_Y(pt) < -14 AND ST_X(pt) > -74 THEN
        v_tipo_suelo := 'S2 (suelo intermedio - sierra/altiplano)';
    ELSE
        v_tipo_suelo := 'S2 (suelo intermedio - estimado)';
    END IF;

    -- Peligro inundación
    SELECT EXISTS(SELECT 1 FROM zonas_inundables WHERE ST_Covers(geom, pt)) INTO v_inundacion;
    SELECT nivel_riesgo::NUMERIC INTO v_pi
    FROM zonas_inundables WHERE ST_Covers(geom, pt)
    ORDER BY nivel_riesgo DESC LIMIT 1;
    v_pi := COALESCE(v_pi, 1.0);

    -- Peligro deslizamiento
    SELECT EXISTS(SELECT 1 FROM deslizamientos WHERE ST_Covers(geom, pt) AND activo = TRUE) INTO v_desl;
    SELECT nivel_riesgo::NUMERIC INTO v_pd
    FROM deslizamientos WHERE ST_Covers(geom, pt) AND activo = TRUE
    ORDER BY nivel_riesgo DESC LIMIT 1;
    v_pd := COALESCE(v_pd, 1.0);

    -- Peligro tsunami
    SELECT EXISTS(SELECT 1 FROM zonas_tsunami WHERE ST_Covers(geom, pt)) INTO v_tsunami;
    SELECT nivel_riesgo::NUMERIC INTO v_pt_tsun
    FROM zonas_tsunami WHERE ST_Covers(geom, pt)
    ORDER BY nivel_riesgo DESC LIMIT 1;
    v_pt_tsun := COALESCE(v_pt_tsun, 1.0);

    -- Peligro fallas activas en 50km
    SELECT COUNT(*)::INTEGER INTO v_n_fallas
    FROM fallas
    WHERE activa = TRUE
      AND ST_DWithin(geom::geography, pt::geography, 50000);
    v_pf := LEAST(5.0, 1.0 + v_n_fallas::NUMERIC * 0.5);

    -- Índice compuesto (CENEPRED ponderación)
    v_indice := ROUND(
        (0.40 * v_ps + 0.25 * v_pi + 0.20 * v_pd + 0.10 * v_pt_tsun + 0.05 * v_pf)::NUMERIC,
        2
    );
    v_indice := LEAST(5.0, GREATEST(1.0, v_indice));

    -- Clasificación textual
    v_nivel_txt := CASE
        WHEN v_indice >= 4.5 THEN 'MUY ALTO'
        WHEN v_indice >= 3.5 THEN 'ALTO'
        WHEN v_indice >= 2.5 THEN 'MEDIO'
        WHEN v_indice >= 1.5 THEN 'BAJO'
        ELSE 'MUY BAJO'
    END;

    -- Recomendaciones técnicas
    v_recom := jsonb_build_array();
    IF v_zona_sis >= 4 THEN
        v_recom := v_recom || '["Diseño sismorresistente NTE E.060 con ductilidad especial (Zona 4)"]'::jsonb;
        v_recom := v_recom || '["Estudio de microzonificación sísmica CISMID recomendado"]'::jsonb;
    END IF;
    IF v_zona_sis >= 3 THEN
        v_recom := v_recom || '["Refuerzo sísmico obligatorio — NTE E.030 Zona 3/4"]'::jsonb;
        v_recom := v_recom || '["Estudio de mecánica de suelos (EMS) obligatorio — NTE E.050"]'::jsonb;
    END IF;
    IF v_inundacion THEN
        v_recom := v_recom || '["Cota mínima de construcción sobre nivel de inundación (ANA/RNE E.060)"]'::jsonb;
        v_recom := v_recom || '["Estudio hidrológico e hidráulico ANA recomendado"]'::jsonb;
    END IF;
    IF v_desl THEN
        v_recom := v_recom || '["Zona de remoción en masa — estudio geotécnico INGEMMET obligatorio"]'::jsonb;
        v_recom := v_recom || '["Muros de contención y drenaje superficial — NTE E.050"]'::jsonb;
    END IF;
    IF v_tsunami THEN
        v_recom := v_recom || '["Zona de inundación tsunamigénica — altura mínima 15m snm o construcción resistente"]'::jsonb;
        v_recom := v_recom || '["Ruta de evacuación vertical identificada (INDECI/DHN)"]'::jsonb;
    END IF;
    IF v_n_fallas > 0 THEN
        v_recom := v_recom || ('["' || v_n_fallas || ' falla(s) activa(s) en radio 50km — retroceso mínimo 50m de traza NTE E.030"]')::jsonb;
    END IF;

    RETURN jsonb_build_object(
        'coordenadas',     jsonb_build_object('lon', p_lon, 'lat', p_lat),
        'departamento',    v_depto,
        'distrito',        v_distrito,
        'zona_sismica',    jsonb_build_object(
            'zona',         v_zona_sis,
            'factor_z',     v_factor_z,
            'descripcion',  CASE v_zona_sis WHEN 4 THEN 'Muy Alta (Costa)'
                                            WHEN 3 THEN 'Alta (Sierra Central/Sur)'
                                            WHEN 2 THEN 'Media (Sierra Norte/Selva Central)'
                                            WHEN 1 THEN 'Baja (Amazonia)'
                                            ELSE 'Media' END,
            'tipo_suelo_aprox', v_tipo_suelo,
            'norma',        'NTE E.030-2018 (DS N°003-2016-VIVIENDA)'
        ),
        'peligros',        jsonb_build_object(
            'sismico',        jsonb_build_object('valor', v_ps, 'sismos_50km_30a', v_n_sismos, 'mag_max', v_mag_max),
            'inundacion',     jsonb_build_object('valor', v_pi, 'en_zona_inundable', v_inundacion),
            'deslizamiento',  jsonb_build_object('valor', v_pd, 'en_zona_desl', v_desl),
            'tsunami',        jsonb_build_object('valor', v_pt_tsun, 'en_zona_tsunami', v_tsunami),
            'fallas_activas', jsonb_build_object('valor', v_pf, 'n_fallas_50km', v_n_fallas)
        ),
        'indice_riesgo_construccion', v_indice,
        'nivel_riesgo',    v_nivel_txt,
        'ponderacion',     '40% sísmico + 25% inundación + 20% deslizamiento + 10% tsunami + 5% fallas',
        'metodologia',     'CENEPRED 2014 + NTE E.030-2018',
        'recomendaciones', v_recom
    );
END;
$$ LANGUAGE plpgsql STABLE;


-- ═══════════════════════════════════════════════════════════
--  FUNCIÓN: f_riesgo_punto(lon, lat) — v7.0 (incluye construcción)
-- ═══════════════════════════════════════════════════════════
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
BEGIN
    SELECT nombre, nivel_riesgo, zona_sismica, factor_z
    INTO depto_nombre, depto_riesgo, zona_sis, factor_z
    FROM departamentos WHERE ST_Covers(geom, pt)
    ORDER BY nivel_riesgo DESC LIMIT 1;

    IF depto_nombre IS NULL THEN
        SELECT nombre, nivel_riesgo, zona_sismica, factor_z
        INTO depto_nombre, depto_riesgo, zona_sis, factor_z
        FROM departamentos ORDER BY geom <-> pt LIMIT 1;
    END IF;

    SELECT COUNT(*), ROUND(MAX(magnitud)::NUMERIC, 1)
    INTO n_sismos_50, max_mag_50
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
        SELECT nombre, tipo, fuente_tipo, geom
        FROM infraestructura
        WHERE ST_DWithin(geom::geography, pt::geography, 10000)
          AND tipo IN ('hospital','bomberos','policia','refugio')
        ORDER BY geom::geography <-> pt::geography
        LIMIT 5
    ) sub;

    -- Incluir riesgo de construcción
    SELECT f_riesgo_construccion(p_lon, p_lat) INTO riesgo_constr;

    RETURN jsonb_build_object(
        'coordenadas',     jsonb_build_object('lon', p_lon, 'lat', p_lat),
        'departamento',    depto_nombre,
        'nivel_riesgo',    depto_riesgo,
        'zona_sismica',    jsonb_build_object(
            'zona', zona_sis, 'factor_z', factor_z,
            'descripcion', CASE zona_sis WHEN 4 THEN 'Muy Alta'
                                         WHEN 3 THEN 'Alta'
                                         WHEN 2 THEN 'Media'
                                         WHEN 1 THEN 'Baja' ELSE 'Media' END
        ),
        'amenazas',        jsonb_build_object(
            'zona_tsunami',      en_tsunami,
            'zona_inundable',    en_inund,
            'zona_desliz',       en_desl,
            'sismos_50km_10a',   n_sismos_50,
            'magnitud_max_50km', max_mag_50
        ),
        'riesgo_construccion',  riesgo_constr,
        'infraestructura_cercana', COALESCE(infra_cercana, '[]'::jsonb)
    );
END;
$$ LANGUAGE plpgsql STABLE;


-- ═══════════════════════════════════════════════════════════
--  VISTAS
-- ═══════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_estadisticas_anio AS
SELECT
    CAST(EXTRACT(YEAR FROM fecha) AS INTEGER) AS anio,
    COUNT(*)                                   AS cantidad,
    ROUND(MAX(magnitud)::NUMERIC, 1)           AS magnitud_max,
    ROUND(AVG(magnitud)::NUMERIC, 2)           AS magnitud_prom,
    ROUND(MIN(magnitud)::NUMERIC, 1)           AS magnitud_min,
    COUNT(*) FILTER (WHERE tipo_profundidad='superficial') AS superficiales,
    COUNT(*) FILTER (WHERE tipo_profundidad='intermedio')  AS intermedios,
    COUNT(*) FILTER (WHERE tipo_profundidad='profundo')    AS profundos,
    COUNT(*) FILTER (WHERE magnitud >= 5.0) AS m5_plus,
    COUNT(*) FILTER (WHERE magnitud >= 6.0) AS m6_plus,
    COUNT(*) FILTER (WHERE magnitud >= 7.0) AS m7_plus
FROM sismos
GROUP BY EXTRACT(YEAR FROM fecha)
ORDER BY anio;

CREATE OR REPLACE VIEW v_sismos_por_depto AS
SELECT
    dep.nombre AS departamento, dep.nivel_riesgo, dep.zona_sismica,
    dep.factor_z,
    COUNT(s.id)                                    AS total_sismos,
    ROUND(MAX(s.magnitud)::NUMERIC, 1)             AS max_magnitud,
    ROUND(AVG(s.magnitud)::NUMERIC, 2)             AS avg_magnitud,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 5.0)   AS m5_plus,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 6.0)   AS m6_plus,
    COUNT(s.id) FILTER (WHERE s.magnitud >= 7.0)   AS m7_plus,
    COUNT(s.id) FILTER (WHERE s.fecha >= CURRENT_DATE - INTERVAL '365 days') AS ultimo_anio
FROM departamentos dep
LEFT JOIN sismos s ON ST_Covers(dep.geom, s.geom)
GROUP BY dep.nombre, dep.nivel_riesgo, dep.zona_sismica, dep.factor_z
ORDER BY total_sismos DESC;

-- Vista de cobertura de infraestructura por tipo y fuente
CREATE OR REPLACE VIEW v_infraestructura_cobertura AS
SELECT
    tipo,
    fuente_tipo,
    COUNT(*)                                           AS total,
    COUNT(*) FILTER (WHERE region IS NOT NULL)         AS con_region,
    COUNT(*) FILTER (WHERE zona_sismica IS NOT NULL)   AS con_zona_sismica,
    COUNT(DISTINCT region)                             AS regiones_distintas,
    MAX(criticidad)                                    AS criticidad_max,
    ROUND(AVG(criticidad)::NUMERIC, 2)                 AS criticidad_prom
FROM infraestructura
GROUP BY tipo, fuente_tipo
ORDER BY tipo, fuente_tipo;


-- ═══════════════════════════════════════════════════════════
--  VISTA MATERIALIZADA: heatmap sísmico
-- ═══════════════════════════════════════════════════════════
DROP MATERIALIZED VIEW IF EXISTS mv_heatmap_sismos;
CREATE MATERIALIZED VIEW mv_heatmap_sismos AS
SELECT
    ST_AsText(ST_SnapToGrid(geom, 0.1))    AS grid_key,
    ST_SnapToGrid(geom, 0.1)               AS geom_grid,
    COUNT(*)                                AS cantidad,
    ROUND(AVG(magnitud)::NUMERIC, 2)        AS magnitud_prom,
    ROUND(MAX(magnitud)::NUMERIC, 1)        AS magnitud_max,
    ROUND(AVG(profundidad_km)::NUMERIC, 1)  AS prof_prom
FROM sismos
WHERE magnitud >= 3.0
GROUP BY ST_SnapToGrid(geom, 0.1)
HAVING COUNT(*) > 0;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_heatmap_key    ON mv_heatmap_sismos(grid_key);
CREATE INDEX IF NOT EXISTS idx_mv_heatmap_geom          ON mv_heatmap_sismos USING GIST(geom_grid);
CREATE INDEX IF NOT EXISTS idx_mv_heatmap_cantidad      ON mv_heatmap_sismos(cantidad DESC);


-- ═══════════════════════════════════════════════════════════
--  VISTA MATERIALIZADA: índice de riesgo de construcción
--  Ref: CENEPRED — Metodología para elaborar mapas de riesgo (2014)
--       NTE E.030-2018, NTE E.050 (Suelos), NTE E.060 (Concreto)
-- ═══════════════════════════════════════════════════════════
DROP MATERIALIZED VIEW IF EXISTS mv_riesgo_construccion;
CREATE MATERIALIZED VIEW mv_riesgo_construccion AS
SELECT
    d.id,
    d.ubigeo,
    d.nombre           AS distrito,
    d.provincia,
    d.departamento,
    d.nivel_riesgo,
    d.zona_sismica,
    dep.factor_z,
    d.poblacion,
    d.area_km2,
    -- Peligro Sísmico (NTE E.030-2018)
    CASE d.zona_sismica WHEN 4 THEN 5 WHEN 3 THEN 4 WHEN 2 THEN 3 WHEN 1 THEN 2 ELSE 3 END AS peligro_sismico,
    -- Peligro Inundación
    COALESCE((
        SELECT MAX(zi.nivel_riesgo)
        FROM zonas_inundables zi
        WHERE ST_Intersects(zi.geom, d.geom)
    ), 1)              AS peligro_inundacion,
    -- Peligro Deslizamiento
    COALESCE((
        SELECT MAX(dl.nivel_riesgo)
        FROM deslizamientos dl
        WHERE ST_Intersects(dl.geom, d.geom) AND dl.activo = TRUE
    ), 1)              AS peligro_deslizamiento,
    -- Peligro Tsunami
    COALESCE((
        SELECT MAX(zt.nivel_riesgo)
        FROM zonas_tsunami zt
        WHERE ST_Intersects(zt.geom, d.geom)
    ), 1)              AS peligro_tsunami,
    -- Fallas activas en 50km del centroide
    (SELECT COUNT(*)::INTEGER FROM fallas f
     WHERE f.activa = TRUE
       AND ST_DWithin(f.geom::geography, ST_Centroid(d.geom)::geography, 50000)
    )                  AS fallas_activas_50km,
    -- Densidad sísmica (últimos 30 años, M>=4.0)
    (SELECT COUNT(*)::INTEGER FROM sismos s
     WHERE s.magnitud >= 4.0
       AND s.fecha >= CURRENT_DATE - INTERVAL '30 years'
       AND ST_DWithin(s.geom::geography, ST_Centroid(d.geom)::geography, 50000)
    )                  AS sismos_m4_30a_50km,
    -- Índice compuesto (CENEPRED ponderación)
    LEAST(5.0, GREATEST(1.0, ROUND((
        0.40 * CASE d.zona_sismica WHEN 4 THEN 5 WHEN 3 THEN 4 WHEN 2 THEN 3 WHEN 1 THEN 2 ELSE 3 END +
        0.25 * COALESCE((SELECT MAX(zi.nivel_riesgo) FROM zonas_inundables zi WHERE ST_Intersects(zi.geom, d.geom)), 1) +
        0.20 * COALESCE((SELECT MAX(dl.nivel_riesgo) FROM deslizamientos dl WHERE ST_Intersects(dl.geom, d.geom) AND dl.activo = TRUE), 1) +
        0.10 * COALESCE((SELECT MAX(zt.nivel_riesgo) FROM zonas_tsunami zt WHERE ST_Intersects(zt.geom, d.geom)), 1) +
        0.05 * LEAST(5.0, 1 + (SELECT COUNT(*)::NUMERIC FROM fallas f WHERE f.activa = TRUE AND ST_DWithin(f.geom::geography, ST_Centroid(d.geom)::geography, 50000)) * 0.5)
    )::NUMERIC, 2))) AS indice_riesgo_construccion,
    ST_AsText(ST_Centroid(d.geom)) AS centroide_wkt
FROM distritos d
LEFT JOIN departamentos dep ON LOWER(d.departamento) = LOWER(dep.nombre);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_riesgo_id
    ON mv_riesgo_construccion(id);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_indice
    ON mv_riesgo_construccion(indice_riesgo_construccion DESC);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_depto
    ON mv_riesgo_construccion(departamento, indice_riesgo_construccion DESC);
CREATE INDEX IF NOT EXISTS idx_mv_riesgo_zona
    ON mv_riesgo_construccion(zona_sismica DESC, indice_riesgo_construccion DESC);


-- ═══════════════════════════════════════════════════════════
--  Confirmación
-- ═══════════════════════════════════════════════════════════
DO $$ BEGIN
    RAISE NOTICE '✅ Esquema GeoRiesgo Perú v7.0 — %', NOW();
    RAISE NOTICE '   Mejoras: zona_sismica NTE E.030-2018 en departamentos/distritos/infra';
    RAISE NOTICE '   fuente_tipo (oficial/osm) en infraestructura';
    RAISE NOTICE '   f_riesgo_construccion() — CENEPRED 2014 + NTE E.030-2018';
    RAISE NOTICE '   mv_riesgo_construccion — índice compuesto por distrito';
    RAISE NOTICE '   v_infraestructura_cobertura — diagnóstico calidad de datos';
    RAISE NOTICE '   Fix: limpieza PostGIS post-inserción en procesar_datos.py';
END $$;