// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — Types v8.0
// 🆕 ZonaPrecipitacionProps — 22 zonas climáticas SENAMHI/CHIRPS
// 🆕 EventoFENData          — catálogo ENSO NOAA-CPC 1957-2024
// 🆕 RiesgoLluvia           — índice pluvial + FEN para un punto
// 🆕 FenEstadisticas        — distribución histórica por intensidad
// ✅ CapasActivas            — añade `precipitaciones`
// ══════════════════════════════════════════════════════════

export interface SismoProps {
  usgs_id:          string
  magnitud:         number
  profundidad_km:   number
  tipo_profundidad: 'superficial' | 'intermedio' | 'profundo'
  fecha:            string
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
  zona_sismica: 1 | 2 | 3 | 4 | null
  factor_z:     number | null
  poblacion:    number | null
}

export interface DistritoProps {
  id:                        number
  ubigeo:                    string | null
  nombre:                    string
  provincia:                 string
  departamento:              string
  nivel_riesgo:              1 | 2 | 3 | 4 | 5
  poblacion:                 number | null
  area_km2:                  number | null
  fuente:                    string
  zona_sismica:              1 | 2 | 3 | 4 | null
  indice_riesgo_construccion: number | null
  clasificacion_suelo:        'S1' | 'S2' | 'S3' | 'S4' | null
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
  id:                number
  nombre:            string
  nivel_riesgo:      number
  altura_ola_m:      number | null
  tiempo_arribo_min: number | null
  periodo_retorno:   number | null
  region:            string | null
  fuente:            string
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
  id:           number
  osm_id:       number | null
  nombre:       string
  tipo:         string
  criticidad:   number
  estado:       string | null
  region:       string | null
  fuente:       string | null
  fuente_tipo:  'oficial' | 'osm' | null
  zona_sismica: 1 | 2 | 3 | 4 | null
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

// ── 🆕 v8.0: Precipitaciones ─────────────────────────────

/** Tipo climático de zona de precipitación */
export type TipoPrecipitacion =
  | 'muy_alta' | 'alta' | 'moderada' | 'baja' | 'muy_baja'

/** Propiedades de una zona pluviométrica (GeoJSON Feature) */
export interface ZonaPrecipitacionProps {
  id:                        number
  nombre:                    string
  tipo:                      TipoPrecipitacion
  region:                    string | null
  precipitacion_anual_mm:    number
  precipitacion_dic_mar_mm:  number | null  // estación húmeda
  precipitacion_jun_ago_mm:  number | null  // estación seca
  /** Multiplicador de precipitación durante El Niño fuerte (1.0 = sin cambio) */
  indice_fen:                number
  /** Riesgo integrado lluvia→inundación 1-5 */
  nivel_riesgo_inundacion:   1 | 2 | 3 | 4 | 5
  descripcion_fen:           string   // "Amplificación catastrófica en FEN" etc.
  color_riesgo:              string   // HEX sugerido para render
  fuente:                    string
}

/** Zona de precipitación cercana a un punto (incluye distancia_km) */
export interface ZonaPrecipitacionCercana extends Omit<ZonaPrecipitacionProps, 'descripcion_fen' | 'color_riesgo'> {
  distancia_km: number
}

// ── 🆕 v8.0: Eventos FEN ─────────────────────────────────

export type TipoFEN     = 'el_nino' | 'la_nina' | 'neutro'
export type IntensidadFEN = 'debil' | 'moderado' | 'fuerte' | 'extraordinario'

/** Evento ENSO histórico (NOAA-CPC ONI) */
export interface EventoFENData {
  id:              number
  año_inicio:      number
  mes_inicio:      number
  año_fin:         number
  mes_fin:         number
  tipo:            TipoFEN
  intensidad:      IntensidadFEN | null
  oni_peak:        number | null   // anomalía SST °C en peak
  impacto_peru:    string | null
  fuente:          string
  duracion_meses:  number | null
}

export interface FenDistribucion {
  tipo:                  TipoFEN
  intensidad:            IntensidadFEN | null
  cantidad:              number
  oni_prom:              number | null
  oni_max:               number | null
  duracion_prom_meses:   number | null
}

export interface FenDecadal {
  decada:   number
  el_nino:  number
  la_nina:  number
  intensos: number
}

export interface FenEstadisticas {
  distribucion_tipo_intensidad: FenDistribucion[]
  frecuencia_decadal:           FenDecadal[]
  eventos_mas_intensos:         EventoFENData[]
  nota_metodologica:            string
}

// ── 🆕 v8.0: Riesgo Lluvia ───────────────────────────────

export interface RiesgoLluviaInundacion {
  nombre:           string
  nivel_riesgo:     number
  tipo_inundacion:  string
  periodo_retorno:  number | null
}

/** Respuesta del endpoint /api/v1/riesgo/lluvia */
export interface RiesgoLluvia {
  punto:               { lon: number; lat: number }
  zona_climatica:      ZonaPrecipitacionCercana | null
  inundaciones:        RiesgoLluviaInundacion[]
  deslizamientos_20km: number
  fen_reciente: {
    año:        number
    tipo:       TipoFEN
    intensidad: IntensidadFEN
    oni_peak:   number
    impacto:    string
  } | null
  indice_pluvial: number   // 1.0-5.0
  nivel_riesgo:   string   // MUY BAJO / BAJO / MEDIO / ALTO / MUY ALTO
  metodologia: {
    formula: string
    escala:  string
    nota:    string
  }
}

// ── Existentes v7.x ──────────────────────────────────────

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

export interface ZonaSismicaInfo {
  zona:              number
  factor_z:          number
  descripcion:       string
  departamentos:     string[]
  sismos_historicos: number
  magnitud_max:      number | null
  sismicidad_nivel:  string
}

export interface RiesgoConstruccionPunto {
  lon:             number
  lat:             number
  zona_sismica:    number | null
  factor_z:        number | null
  indice:          number
  nivel_txt:       string
  peligros: {
    sismico:       number
    inundacion:    number
    deslizamiento: number
    tsunami:       number
    fallas:        number
  }
  recomendaciones: string[]
  norma:           string
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
  tipo:        string
  total:       number
  oficial:     number
  osm:         number
  pct_oficial: number
}

export interface PoblacionZona {
  zona_sismica:  number
  factor_z:      number
  descripcion:   string
  departamentos: string[]
  poblacion:     number
  pct_poblacion: number
}

export interface RiesgoInfo {
  lon:                   number
  lat:                   number
  region:                string | null
  distrito:              string | null
  nivel_riesgo:          number
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
  fuente:                string
  riesgo_construccion?:  RiesgoConstruccionPunto | null
}

export interface DiagnosticoLayer {
  tabla:         string
  total:         number
  con_region:    number
  sin_region:    number
  pct_cobertura: number
  via_knn:       number
}

// ── Filtros y estado ─────────────────────────────────────

export interface FiltrosSismos {
  mag_min:      number
  mag_max:      number
  year_start:   number
  year_end:     number
  profundidad?: 'superficial' | 'intermedio' | 'profundo' | undefined
  region?:      string | undefined
}

/** Filtros para la capa de precipitaciones */
export interface FiltrosPrecipitacion {
  tipo?:                 TipoPrecipitacion | undefined
  riesgo_inund_min:      number
  fen_min?:              number | undefined
}

export type FuenteTipo = 'todos' | 'oficial' | 'osm'

export interface CapasActivas {
  sismos:              boolean
  heatmap:             boolean
  departamentos:       boolean
  fallas:              boolean
  inundaciones:        boolean
  tsunamis:            boolean
  deslizamientos:      boolean
  riesgo_distritos:    boolean
  infraestructura:     boolean
  estaciones:          boolean
  riesgo_construccion: boolean
  /** 🆕 v8.0: zonas climáticas coloreadas por indice_fen */
  precipitaciones:     boolean
  extrusion_3d:        boolean
}

export type TipoVista = '2d' | '3d'

export interface TooltipInfo {
  x:      number
  y:      number
  object: GeoJSON.Feature | null
  layer:  string | null
}