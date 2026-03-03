// ── GeoRiesgo Ica — Tipos principales ────────────────────

export interface SismoProps {
  id:               number
  usgs_id:          string
  magnitud:         number
  profundidad_km:   number          // ← nombre correcto del backend v3
  tipo_profundidad: 'superficial' | 'intermedio' | 'profundo'
  fecha:            string          // YYYY-MM-DD
  hora_utc:         string | null
  lugar:            string
  tipo_magnitud:    string
  fuente:           string
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
  id:          number
  nombre:      string
  activa:      boolean
  tipo:        string
  longitud_km: number
  fuente:      string
}

export interface ZonaInundableProps {
  id:              number
  nombre:          string | null
  nivel_riesgo:    number | null
  periodo_retorno: number | null
  fuente:          string
}

export interface InfraestructuraProps {
  id:         number
  nombre:     string
  tipo:       string
  criticidad: number
  fuente:     string | null
}

export interface EstadisticaAnual {
  year:               number
  cantidad:           number
  magnitud_max:       number
  magnitud_promedio:  number
  superficiales:      number
  intermedios:        number
  profundos:          number
}

export interface AnalisisPorDistrito {
  distrito:      string
  provincia:     string
  nivel_riesgo:  number
  total_sismos:  number
  max_magnitud:  number
  avg_magnitud:  number
}

export interface FiltrosSismos {
  mag_min:    number
  mag_max:    number
  year_start: number
  year_end:   number
  profundidad?: 'superficial' | 'intermedio' | 'profundo' | undefined
  distrito?:   string | undefined
}

export interface CapasActivas {
  sismos:           boolean
  heatmap:          boolean
  fallas:           boolean
  inundaciones:     boolean
  riesgo_distritos: boolean
  infraestructura:  boolean
  extrusion_3d:     boolean
}

export type TipoVista = '2d' | '3d'