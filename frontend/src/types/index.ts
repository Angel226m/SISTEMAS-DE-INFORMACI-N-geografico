// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — Types v4.0
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

export interface DistritoProps {
  id:           number
  ubigeo:       string | null
  nombre:       string
  provincia:    string
  departamento: string
  nivel_riesgo: 1 | 2 | 3 | 4 | 5
  poblacion:    number | null
  area_km2:     number | null
  fuente:       string
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

export interface InfraestructuraProps {
  id:         number
  osm_id:     number | null
  nombre:     string
  tipo:       string
  criticidad: number
  estado:     string | null
  region:     string | null
  fuente:     string | null
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
  anio:              number
  cantidad:          number
  magnitud_max:      number
  magnitud_prom:     number
  superficiales:     number
  intermedios:       number
  profundos:         number
  m5_plus:           number
  m6_plus:           number
  m7_plus:           number
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

export interface FiltrosSismos {
  mag_min:     number
  mag_max:     number
  year_start:  number
  year_end:    number
  profundidad?: 'superficial' | 'intermedio' | 'profundo' | undefined
  region?:      string | undefined
}

export interface CapasActivas {
  sismos:           boolean
  heatmap:          boolean
  fallas:           boolean
  inundaciones:     boolean
  tsunamis:         boolean
  riesgo_distritos: boolean
  infraestructura:  boolean
  estaciones:       boolean
  extrusion_3d:     boolean
}

export type TipoVista = '2d' | '3d'

export interface TooltipInfo {
  x: number
  y: number
  object: GeoJSON.Feature | null
  layer: string | null
}