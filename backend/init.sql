-- ══════════════════════════════════════════════════════════
-- GeoRiesgo Ica — Esquema PostGIS
-- Se ejecuta automáticamente al crear el contenedor DB
-- ══════════════════════════════════════════════════════════

-- Extensiones espaciales
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ── Tabla: sismos ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sismos (
    id              BIGSERIAL PRIMARY KEY,
    usgs_id         TEXT UNIQUE NOT NULL,          -- ID único de USGS
    geom            GEOMETRY(Point, 4326) NOT NULL, -- WGS-84
    magnitud        NUMERIC(4,1) NOT NULL CHECK (magnitud >= 0),
    profundidad_km  NUMERIC(7,2) NOT NULL CHECK (profundidad_km >= 0),
    tipo_profundidad TEXT NOT NULL CHECK (tipo_profundidad IN ('superficial','intermedio','profundo')),
    fecha           DATE NOT NULL,
    hora_utc        TIMESTAMPTZ,
    lugar           TEXT,
    tipo_magnitud   TEXT,                           -- ML, Mw, mb, etc.
    estado          TEXT DEFAULT 'reviewed',
    fuente          TEXT DEFAULT 'USGS',
    creado_en       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sismos_geom      ON sismos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sismos_fecha     ON sismos(fecha);
CREATE INDEX IF NOT EXISTS idx_sismos_magnitud  ON sismos(magnitud);
CREATE INDEX IF NOT EXISTS idx_sismos_profund   ON sismos(profundidad_km);

-- ── Tabla: distritos ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS distritos (
    id              SERIAL PRIMARY KEY,
    ubigeo          TEXT UNIQUE,                    -- Código INEI
    nombre          TEXT NOT NULL,
    provincia       TEXT,
    departamento    TEXT DEFAULT 'Ica',
    geom            GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo    SMALLINT NOT NULL DEFAULT 3 CHECK (nivel_riesgo BETWEEN 1 AND 5),
    poblacion       INTEGER,
    area_km2        NUMERIC(10,3),
    fuente          TEXT DEFAULT 'INEI/GADM',
    actualizado_en  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_distritos_geom ON distritos USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_distritos_nombre ON distritos(nombre);

-- ── Tabla: fallas ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fallas (
    id          SERIAL PRIMARY KEY,
    ingemmet_id TEXT,
    nombre      TEXT NOT NULL,
    geom        GEOMETRY(MultiLineString, 4326),
    activa      BOOLEAN DEFAULT TRUE,
    tipo        TEXT,                               -- Neotectónica, Inferida, etc.
    longitud_km NUMERIC(10,2),
    fuente      TEXT DEFAULT 'INGEMMET',
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fallas_geom ON fallas USING GIST(geom);

-- ── Tabla: zonas_inundables ───────────────────────────────
CREATE TABLE IF NOT EXISTS zonas_inundables (
    id              SERIAL PRIMARY KEY,
    nombre          TEXT,
    geom            GEOMETRY(MultiPolygon, 4326),
    nivel_riesgo    SMALLINT CHECK (nivel_riesgo BETWEEN 1 AND 5),
    periodo_retorno INTEGER,                        -- años
    fuente          TEXT DEFAULT 'ANA/SENAMHI',
    actualizado_en  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_inundables_geom ON zonas_inundables USING GIST(geom);

-- ── Tabla: infraestructura ────────────────────────────────
CREATE TABLE IF NOT EXISTS infraestructura (
    id      SERIAL PRIMARY KEY,
    nombre  TEXT NOT NULL,
    tipo    TEXT NOT NULL,   -- hospital, escuela, puente, etc.
    geom    GEOMETRY(Point, 4326),
    criticidad SMALLINT DEFAULT 3 CHECK (criticidad BETWEEN 1 AND 5),
    fuente  TEXT,
    actualizado_en TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_infra_geom ON infraestructura USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_infra_tipo ON infraestructura(tipo);

-- ── Tabla: sync_log ───────────────────────────────────────
-- Registra cuándo y qué se sincronizó por última vez
CREATE TABLE IF NOT EXISTS sync_log (
    id          SERIAL PRIMARY KEY,
    fuente      TEXT NOT NULL,           -- USGS, GADM, INGEMMET, etc.
    tabla       TEXT NOT NULL,
    registros   INTEGER DEFAULT 0,
    estado      TEXT DEFAULT 'ok',      -- ok / error / parcial
    detalle     TEXT,
    inicio      TIMESTAMPTZ DEFAULT NOW(),
    fin         TIMESTAMPTZ
);

-- ── Vista: resumen estadístico por año ────────────────────
CREATE OR REPLACE VIEW v_estadisticas_anio AS
SELECT
    EXTRACT(YEAR FROM fecha)::INTEGER            AS year,
    COUNT(*)                                      AS cantidad,
    ROUND(MAX(magnitud)::NUMERIC, 1)              AS magnitud_max,
    ROUND(AVG(magnitud)::NUMERIC, 2)              AS magnitud_promedio,
    COUNT(*) FILTER (WHERE tipo_profundidad = 'superficial')  AS superficiales,
    COUNT(*) FILTER (WHERE tipo_profundidad = 'intermedio')   AS intermedios,
    COUNT(*) FILTER (WHERE tipo_profundidad = 'profundo')     AS profundos
FROM sismos
GROUP BY EXTRACT(YEAR FROM fecha)
ORDER BY year;

-- ── Vista: sismos por distrito (join espacial) ────────────
CREATE OR REPLACE VIEW v_sismos_por_distrito AS
SELECT
    d.nombre                        AS distrito,
    d.provincia,
    d.nivel_riesgo,
    COUNT(s.id)                     AS total_sismos,
    ROUND(MAX(s.magnitud)::NUMERIC, 1) AS max_magnitud,
    ROUND(AVG(s.magnitud)::NUMERIC, 2) AS avg_magnitud
FROM distritos d
LEFT JOIN sismos s ON ST_Within(s.geom, d.geom)
GROUP BY d.nombre, d.provincia, d.nivel_riesgo
ORDER BY total_sismos DESC;

-- Confirmar
DO $$ BEGIN
    RAISE NOTICE '✅ Esquema GeoRiesgo Ica inicializado correctamente';
END $$;