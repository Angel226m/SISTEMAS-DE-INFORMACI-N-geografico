// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — Types v7.0
// Alineado con backend v7.0: zona_sismica NTE E.030-2018,
// IRC CENEPRED, fuente_tipo oficial/osm, poblacion INEI
// ══════════════════════════════════════════════════════════

export interface SismoProps {
  usgs_id:          string
  magnitud:         number
  profundidad_km:   number
  tipo_profundidad: 'superficial' | 'intermedio' | 'profundo'
  fecha:            string       // "YYYY-MM-DD"
  hora_utc:         string | null
  lugar:            string
  region:           string | null
  tipo_magnitud:    string
  estado:           string
}

export interface DepartamentoProps {
  id:           number
  ubigeo:       string | null
  nombre:       string
  nivel_riesgo: 1 | 2 | 3 | 4 | 5
  area_km2:     number | null
  capital:      string | null
  fuente:       string
  zona_sismica: 1 | 2 | 3 | 4 | null  // NTE E.030-2018
  factor_z:     number | null           // 0.10, 0.25, 0.35, 0.45
  poblacion:    number | null
}

export interface DistritoProps {
  id:                       number
  ubigeo:                   string | null
  nombre:                   string
  provincia:                string
  departamento:             string
  nivel_riesgo:             1 | 2 | 3 | 4 | 5
  poblacion:                number | null
  area_km2:                 number | null
  fuente:                   string
  zona_sismica:             1 | 2 | 3 | 4 | null
  indice_riesgo_construccion: number | null  // 1.0–5.0 IRC CENEPRED
}

export interface FallaProps {
  id:           number
  ingemmet_id:  string | null
  nombre:       string
  nombre_alt:   string | null
  activa:       boolean
  tipo:         string
  mecanismo:    string | null
  longitud_km:  number | null
  magnitud_max: number | null
  region:       string | null
  fuente:       string
  referencia:   string | null
}

export interface ZonaInundableProps {
  id:               number
  nombre:           string
  nivel_riesgo:     number
  tipo_inundacion:  string
  periodo_retorno:  number | null
  profundidad_max_m:number | null
  cuenca:           string | null
  region:           string | null
  fuente:           string
}

export interface TsunamiProps {
  id:                 number
  nombre:             string
  nivel_riesgo:       number
  altura_ola_m:       number | null
  tiempo_arribo_min:  number | null
  periodo_retorno:    number | null
  region:             string | null
  fuente:             string
}

export interface DeslizamientoProps {
  id:           number
  nombre:       string | null
  tipo:         string | null
  nivel_riesgo: number
  area_km2:     number | null
  region:       string | null
  activo:       boolean
  fuente:       string
}

export interface InfraestructuraProps {
  id:          number
  osm_id:      number | null
  nombre:      string
  tipo:        string
  criticidad:  number
  estado:      string | null
  region:      string | null
  fuente:      string | null
  fuente_tipo: 'oficial' | 'osm' | null   // v7.0
  zona_sismica: 1 | 2 | 3 | 4 | null     // v7.0
}

export interface EstacionProps {
  id:          number
  codigo:      string
  nombre:      string
  tipo:        string
  altitud_m:   number | null
  activa:      boolean
  institucion: string | null
  region:      string | null
}

export interface EstadisticaAnual {
  anio:          number
  cantidad:      number
  magnitud_max:  number
  magnitud_prom: number
  superficiales: number
  intermedios:   number
  profundos:     number
  m5_plus:       number
  m6_plus:       number
  m7_plus:       number
}

export interface HeatmapCell {
  cantidad:      number
  magnitud_prom: number
  magnitud_max:  number
}

export interface SismoDetalle extends SismoProps {
  geom: GeoJSON.Point
  lon:  number
  lat:  number
}

// ── v7.0: Zonas Sísmicas NTE E.030-2018 ─────────────────────

export interface ZonaSismicaInfo {
  zona:              number        // 1–4
  factor_z:          number        // 0.10 / 0.25 / 0.35 / 0.45
  descripcion:       string        // "Muy alto riesgo sísmico"
  departamentos:     string[]
  sismos_historicos: number
  magnitud_max:      number | null
  sismicidad_nivel:  string
}

// ── v7.0: Riesgo de Construcción IRC CENEPRED ────────────────

export interface RiesgoConstruccionPunto {
  lon:             number
  lat:             number
  zona_sismica:    number | null
  factor_z:        number | null
  indice:          number          // 1.0–5.0
  nivel_txt:       string
  peligros: {
    sismico:       number
    inundacion:    number
    deslizamiento: number
    tsunami:       number
    fallas:        number
  }
  recomendaciones: string[]
  norma:           string          // "NTE E.030-2018"
}

export interface RiesgoConstruccionRanking {
  distrito:        string
  departamento:    string
  zona_sismica:    number | null
  factor_z:        number | null
  poblacion:       number | null
  indice_riesgo_construccion: number
  nivel_txt:       string
  peligro_sismico: number
  sismos_m4_50km:  number
}

export interface CoberturaTipo {
  tipo:         string
  total:        number
  oficial:      number
  osm:          number
  pct_oficial:  number
}

export interface PoblacionZona {
  zona_sismica:  number
  factor_z:      number
  descripcion:   string
  departamentos: string[]
  poblacion:     number
  pct_poblacion: number
}

// ── v6.0 (mantenido) ────────────────────────────────────────

/** Resultado del endpoint GET /api/v1/riesgo?lon=&lat= */
export interface RiesgoInfo {
  lon:          number
  lat:          number
  region:       string | null
  distrito:     string | null
  nivel_riesgo: number
  score_sismico:         number
  score_fallas:          number
  score_inundacion:      number
  score_deslizamiento:   number
  sismos_cercanos_5km:   number
  sismos_cercanos_20km:  number
  mag_maxima_cercana:    number | null
  falla_mas_cercana:     string | null
  dist_falla_km:         number | null
  infraestructura_cercana: number
  fuente:       string
  // v7.0: objeto IRC
  riesgo_construccion?: RiesgoConstruccionPunto | null
}

/** Fila del endpoint GET /api/v1/diagnostico/regiones */
export interface DiagnosticoLayer {
  tabla:         string
  total:         number
  con_region:    number
  sin_region:    number
  pct_cobertura: number
  via_knn:       number
}

// ── Filtros y estado ─────────────────────────────────────────

export interface FiltrosSismos {
  mag_min:      number
  mag_max:      number
  year_start:   number
  year_end:     number
  profundidad?: 'superficial' | 'intermedio' | 'profundo' | undefined
  region?:      string | undefined
}

export type FuenteTipo = 'todos' | 'oficial' | 'osm'

export interface CapasActivas {
  sismos:             boolean
  heatmap:            boolean
  departamentos:      boolean
  fallas:             boolean
  inundaciones:       boolean
  tsunamis:           boolean
  deslizamientos:     boolean
  riesgo_distritos:   boolean
  infraestructura:    boolean
  estaciones:         boolean
  riesgo_construccion:boolean   // v7.0 IRC mapa
  extrusion_3d:       boolean
}

export type TipoVista = '2d' | '3d'

export interface TooltipInfo {
  x:      number
  y:      number
  object: GeoJSON.Feature | null
  layer:  string | null
}