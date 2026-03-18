// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — Types v9.0
// 🆕 VolcanProps             — INGEMMET/OVI-IGP 2021
// 🆕 AlertaRT                — EWS multi-hazard + CAP v1.2
// 🆕 ExposicionDistrito      — GEM 2023 + INEI 2017 + MIDIS 2022
// 🆕 SusceptibilidadPunto    — XGBoost + SHAP + IC 80%
// 🆕 RiesgoEscenario         — 4DS + Youngs 1997 + GEM
// 🆕 SendaiReport            — 7 targets Sendai Framework
// 🆕 RasterPrecipitacion     — COG MinIO window read
// 🆕 CapasActivas            — añade volcanes + susceptibilidad + alertas
// ✅ Todos los tipos v8.0 mantenidos
// ══════════════════════════════════════════════════════════

// ── Tipos v8.0 (mantenidos intactos) ─────────────────────

export interface SismoProps {
  usgs_id: string; magnitud: number; profundidad_km: number
  tipo_profundidad: 'superficial' | 'intermedio' | 'profundo'
  fecha: string; hora_utc: string | null; lugar: string
  region: string | null; tipo_magnitud: string; estado: string
}

export interface DepartamentoProps {
  id: number; ubigeo: string | null; nombre: string
  nivel_riesgo: 1|2|3|4|5; area_km2: number | null; capital: string | null
  fuente: string; zona_sismica: 1|2|3|4|null; factor_z: number | null
  poblacion: number | null
}

export interface DistritoProps {
  id: number; ubigeo: string | null; nombre: string
  provincia: string; departamento: string
  nivel_riesgo: 1|2|3|4|5; poblacion: number | null; area_km2: number | null
  fuente: string; zona_sismica: 1|2|3|4|null
  indice_riesgo_construccion: number | null
  clasificacion_suelo: 'S1'|'S2'|'S3'|'S4'|null
  // 🆕 v9
  indice_riesgo_v9: number | null; irc_v9_p10: number | null
  irc_v9_p90: number | null; factor_cascada: number | null
  peligro_volcan: number | null; peligro_sequia: number | null
}

export interface FallaProps {
  id: number; ingemmet_id: string | null; nombre: string
  nombre_alt: string | null; activa: boolean; tipo: string
  mecanismo: string | null; longitud_km: number | null
  magnitud_max: number | null; region: string | null
  fuente: string; referencia: string | null
}

export interface ZonaInundableProps {
  id: number; nombre: string; nivel_riesgo: number
  tipo_inundacion: string; periodo_retorno: number | null
  profundidad_max_m: number | null; cuenca: string | null
  region: string | null; fuente: string
}

export interface TsunamiProps {
  id: number; nombre: string; nivel_riesgo: number
  altura_ola_m: number | null; tiempo_arribo_min: number | null
  periodo_retorno: number | null; region: string | null; fuente: string
}

export interface DeslizamientoProps {
  id: number; nombre: string | null; tipo: string | null
  nivel_riesgo: number; area_km2: number | null
  region: string | null; activo: boolean; fuente: string
}

export interface InfraestructuraProps {
  id: number; osm_id: number | null; nombre: string; tipo: string
  criticidad: number; estado: string | null; region: string | null
  fuente: string | null; fuente_tipo: 'oficial'|'osm'|null
  zona_sismica: 1|2|3|4|null
}

export interface EstacionProps {
  id: number; codigo: string; nombre: string; tipo: string
  altitud_m: number | null; activa: boolean
  institucion: string | null; region: string | null
}

export type TipoPrecipitacion = 'muy_alta'|'alta'|'moderada'|'baja'|'muy_baja'

export interface ZonaPrecipitacionProps {
  id: number; nombre: string; tipo: TipoPrecipitacion
  region: string | null; precipitacion_anual_mm: number
  precipitacion_dic_mar_mm: number | null
  precipitacion_jun_ago_mm: number | null
  indice_fen: number; nivel_riesgo_inundacion: 1|2|3|4|5
  descripcion_fen: string; color_riesgo: string; fuente: string
}

export interface ZonaPrecipitacionCercana extends Omit<ZonaPrecipitacionProps, 'descripcion_fen'|'color_riesgo'> {
  distancia_km: number
}

export type TipoFEN       = 'el_nino'|'la_nina'|'neutro'
export type IntensidadFEN = 'debil'|'moderado'|'fuerte'|'extraordinario'

export interface EventoFENData {
  id: number; año_inicio: number; mes_inicio: number
  año_fin: number; mes_fin: number; tipo: TipoFEN
  intensidad: IntensidadFEN | null; oni_peak: number | null
  impacto_peru: string | null; fuente: string; duracion_meses: number | null
}

export interface FenDistribucion {
  tipo: TipoFEN; intensidad: IntensidadFEN | null
  cantidad: number; oni_prom: number | null; oni_max: number | null
  duracion_prom_meses: number | null
}

export interface FenDecadal {
  decada: number; el_nino: number; la_nina: number; intensos: number
}

export interface FenEstadisticas {
  distribucion_tipo_intensidad: FenDistribucion[]
  frecuencia_decadal: FenDecadal[]
  eventos_mas_intensos: EventoFENData[]
  nota_metodologica: string
}

export interface RiesgoLluviaInundacion {
  nombre: string; nivel_riesgo: number
  tipo_inundacion: string; periodo_retorno: number | null
}

export interface RiesgoLluvia {
  punto: { lon: number; lat: number }
  zona_climatica: ZonaPrecipitacionCercana | null
  inundaciones: RiesgoLluviaInundacion[]
  deslizamientos_20km: number
  fen_reciente: {
    año: number; tipo: TipoFEN; intensidad: IntensidadFEN
    oni_peak: number; impacto: string
  } | null
  indice_pluvial: number; nivel_riesgo: string
  metodologia: { formula: string; escala: string; nota: string }
}

export interface EstadisticaAnual {
  anio: number; cantidad: number; magnitud_max: number
  magnitud_prom: number; superficiales: number; intermedios: number
  profundos: number; m5_plus: number; m6_plus: number; m7_plus: number
}

export interface ZonaSismicaInfo {
  zona: number; factor_z: number; descripcion: string
  departamentos: string[]; sismos_historicos: number
  magnitud_max: number | null; sismicidad_nivel: string
}

export interface RiesgoConstruccionPunto {
  lon: number; lat: number; zona_sismica: number | null
  factor_z: number | null; indice: number; nivel_txt: string
  peligros: {
    sismico: number; inundacion: number; deslizamiento: number
    tsunami: number; fallas: number
  }
  recomendaciones: string[]; norma: string
}

export interface RiesgoConstruccionRanking {
  distrito: string; departamento: string
  zona_sismica: number | null; factor_z: number | null
  poblacion: number | null
  indice_riesgo_construccion: number; nivel_txt: string
  peligro_sismico: number; sismos_m4_50km: number
  // 🆕 v9
  indice_riesgo_v9?: number | null
  irc_v9_p10?: number | null; irc_v9_p90?: number | null
  peligro_volcan?: number | null; peligro_sequia?: number | null
  factor_cascada?: number | null
}

export interface CoberturaTipo {
  tipo: string; total: number; oficial: number; osm: number; pct_oficial: number
}

export interface PoblacionZona {
  zona_sismica: number; factor_z: number; descripcion: string
  departamentos: string[]; poblacion: number; pct_poblacion: number
}

export interface RiesgoInfo {
  lon: number; lat: number; region: string | null; distrito: string | null
  nivel_riesgo: number; score_sismico: number; score_fallas: number
  score_inundacion: number; score_deslizamiento: number
  sismos_cercanos_5km: number; sismos_cercanos_20km: number
  mag_maxima_cercana: number | null; falla_mas_cercana: string | null
  dist_falla_km: number | null; infraestructura_cercana: number
  fuente: string; riesgo_construccion?: RiesgoConstruccionPunto | null
}

export interface DiagnosticoLayer {
  tabla: string; total: number; con_region: number
  sin_region: number; pct_cobertura: number; via_knn: number
}

// ── 🆕 v9.0: Volcanes ────────────────────────────────────

export interface VolcanProps {
  id: number; nombre: string
  estado: 'activo_critico'|'activo'|'potencialmente_activo'|'inactivo'
  altitud_m: number; region: string | null; tipo_erupcion: string
  ultima_erupcion: number | null; fuente: string
  radio_peligro_km: Record<string, number>
  color: string
}

// ── 🆕 v9.0: EWS Alertas ─────────────────────────────────

export interface AlertaRT {
  id: number
  nivel_alerta: 'watch'|'warning'|'emergency'
  magnitud: number; profundidad_km: number; lugar: string
  lon: number; lat: number
  infraestructura_afectada: Array<{tipo:string; nombre:string; distancia_km:number}>
  poblacion_expuesta: number
  dispara_tsunami: boolean; dispara_deslizamiento: boolean
  cap_identifier: string | null; cap_xml?: string | null
  pilares_ew4all: { p1:boolean; p2:boolean; p3:boolean; p4:boolean }
  canales_enviados: string[]
  created_at: string
}

export interface EWSStatus {
  sse_clients: number; ws_clients: number
  sismos_vistos: number; running: number
}

// ── 🆕 v9.0: Susceptibilidad ML ──────────────────────────

export type AmenazaML = 'deslizamiento'|'inundacion'|'sequia'
export type NivelML   = 'MUY_BAJO'|'BAJO'|'MEDIO'|'ALTO'|'MUY_ALTO'

export interface SusceptibilidadPunto {
  lon: number; lat: number; amenaza: AmenazaML
  score: number; score_p10: number; score_p90: number
  nivel: NivelML; ic_descripcion: string
  features_usados: Record<string, number>
  shap_values: Record<string, number>
  modelo_info: {
    algoritmo: string; auc_pr: number | null; auc_roc: number | null
    entrenado_en: string | null; version: string; tecnica_balance: string | null
  }
}

export interface ModeloMetadata {
  amenaza: AmenazaML; algoritmo: string
  auc_roc: number | null; auc_pr: number | null
  f1_score: number | null; precision_score: number | null
  recall_score: number | null; n_samples: number | null
  n_positivos: number | null; n_negativos: number | null
  ratio_imbalance: number | null
  features_usadas: string[] | null; features_elim_vif: string[] | null
  features_elim_rfe: string[] | null
  importancias_shap: Record<string,number> | null
  hiperparametros: Record<string,unknown> | null
  tecnica_balance: string | null; entrenado_en: string | null; version: string | null
}

// ── 🆕 v9.0: Exposición / IVS ────────────────────────────

export interface ExposicionDistrito {
  ubigeo: string; poblacion_total: number; n_viviendas: number
  pct_adobe: number; pct_pobreza: number; pct_sin_agua: number
  pct_analfabetismo: number; pct_sin_desague: number; pct_adulto_mayor: number
  gem_tax_predominante: string
  pct_ladrillo_conf: number; pct_concreto: number; pct_quincha: number
  ivs: number; nivel_ivs: string; indice_riesgo_total: number | null
  nivel_riesgo_total: string | null
  indice_riesgo_v9: number | null; irc_v9_p10: number | null
  irc_v9_p90: number | null; factor_cascada: number | null
  distrito_nombre: string; departamento: string; provincia: string
  comparacion_nacional: { percentil_ivs: number | null; percentil_riesgo_total: number | null }
  metodologia: string; fuente: string
}

// ── 🆕 v9.0: Escenario sísmico ───────────────────────────

export interface DanoPorTipo {
  gem_taxonomy: string; fuente_fragilidad: string
  n_viviendas: number; fraccion: number
  p_ds1: number; p_ds2: number; p_ds3: number; p_ds4: number
  viv_ds1: number; viv_ds2: number; viv_ds3: number; viv_ds4: number
  perdida_usd: number; estado_predominante: string
}

export interface RiesgoEscenario {
  pga_g: number; distancia_km: number; tipo_subduccion: string
  magnitud: number; profundidad_km: number; n_viviendas: number
  hora_del_dia: string
  por_tipo: Record<string, DanoPorTipo>
  totales: {
    viv_ds1: number; viv_ds2: number; viv_ds3: number; viv_ds4: number
    heridos_estimados: number; fallecidos_estimados: number
    perdida_total_usd: number; perdida_total_pib_pct: number
  }
  incertidumbre: string; advertencia: string
  metodologia: string; fuentes: string[]
}

// ── 🆕 v9.0: Sendai Framework ────────────────────────────

export interface SendaiReport {
  año: number
  target_a: Record<string, unknown>
  target_b: Record<string, unknown>
  target_c: Record<string, unknown>
  target_d: Record<string, unknown>
  target_e: Record<string, unknown>
  target_f: Record<string, unknown>
  target_g: Record<string, unknown>
  metodologia: string; creado_en: string
  advertencia: string; marco_referencia: string
}

// ── 🆕 v9.0: Raster ──────────────────────────────────────

export interface RasterPrecipitacion {
  lon: number; lat: number
  precipitacion_anual_mm: number | null
  fuente: string; metodologia: string; nota: string
}

// ── Filtros y estado ──────────────────────────────────────

export interface FiltrosSismos {
  mag_min: number; mag_max: number
  year_start: number; year_end: number
  profundidad?: 'superficial'|'intermedio'|'profundo'|undefined
  region?: string | undefined
}

export interface FiltrosPrecipitacion {
  tipo?: TipoPrecipitacion | undefined
  riesgo_inund_min: number
  fen_min?: number | undefined
}

export type FuenteTipo = 'todos'|'oficial'|'osm'

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
  precipitaciones:     boolean
  // 🆕 v9.0
  volcanes:            boolean
  susceptibilidad:     boolean
  alertas_ews:         boolean
  extrusion_3d:        boolean
}

export type TipoVista = '2d'|'3d'

export interface TooltipInfo {
  x: number; y: number
  object: GeoJSON.Feature | null; layer: string | null
}

export type AmenazaActiva = AmenazaML | null