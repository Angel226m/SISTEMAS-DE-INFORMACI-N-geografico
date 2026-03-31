// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — API Service v9.0
// 🆕 getVolcanes()             — INGEMMET/OVI-IGP 2021
// 🆕 getSusceptibilidadPunto() — XGBoost + SHAP + IC
// 🆕 getSusceptibilidadMapa()  — grilla 0.05° por dpto
// 🆕 getModeloInfo()           — metadata ML entrenado
// 🆕 entrenarModelo()          — POST background task
// 🆕 getAlertasRecientes()     — EWS últimas N horas
// 🆕 getExposicion()           — GEM 2023 + INEI + MIDIS
// 🆕 getSismosTendencia()      — TimescaleDB CAG
// 🆕 getRiesgoEscenario()      — 4DS + Youngs 1997
// 🆕 getSendaiReport()         — 7 targets Sendai
// 🆕 getRasterPrecipitacion()  — COG MinIO window read
// 🆕 getRasterCatalogo()       — STAC metadata
// 🆕 createSSEConnection()     — EventSource EWS
// 🆕 createWSConnection()      — WebSocket EWS
// ✅ Todos los endpoints v8.0 mantenidos
// ══════════════════════════════════════════════════════════

import type {
  EstadisticaAnual, FiltrosSismos, FiltrosPrecipitacion,
  RiesgoInfo, DiagnosticoLayer, ZonaSismicaInfo,
  RiesgoConstruccionPunto, RiesgoConstruccionRanking,
  CoberturaTipo, PoblacionZona,
  EventoFENData, FenEstadisticas, RiesgoLluvia, ZonaPrecipitacionCercana,
  // v9
  AlertaRT, ExposicionDistrito, SusceptibilidadPunto,
  ModeloMetadata, RiesgoEscenario, SendaiReport,
  RasterPrecipitacion, AmenazaML,
} from '../types'

const BASE = (import.meta.env.VITE_API_URL ?? '') as string
const API  = `${BASE}/api/v1`

// ── Cache en memoria ──────────────────────────────────────
interface CacheEntry<T> { data: T; ts: number; etag?: string }
const CACHE    = new Map<string, CacheEntry<unknown>>()
const INFLIGHT = new Map<string, Promise<unknown>>()

const TTL: Record<string, number> = {
  departamentos:                20 * 60_000,
  distritos:                    10 * 60_000,
  fallas:                       15 * 60_000,
  inundaciones:                 15 * 60_000,
  tsunamis:                     15 * 60_000,
  deslizamientos:               15 * 60_000,
  infraestructura:              10 * 60_000,
  estaciones:                   20 * 60_000,
  estadisticas:                  5 * 60_000,
  sismos:                        5 * 60_000,
  heatmap:                       2 * 60_000,
  riesgo:                           30_000,
  diagnostico:                   5 * 60_000,
  'zonas-sismicas':             30 * 60_000,
  'riesgo-construccion':             60_000,
  'riesgo-construccion-ranking':  5 * 60_000,
  'riesgo-construccion-mapa':     5 * 60_000,
  'infra-cobertura':              5 * 60_000,
  poblacion:                    30 * 60_000,
  precipitaciones:              30 * 60_000,
  'precip-cercanas':                 60_000,
  'fen-eventos':                15 * 60_000,
  'fen-estadisticas':           30 * 60_000,
  'riesgo-lluvia':                   60_000,
  // v9
  volcanes:                     86_400_000,  // 24h
  'susceptibilidad-punto':      43_200_000,  // 12h
  'susceptibilidad-mapa':       43_200_000,
  'modelo-info':                 3_600_000,
  'alertas-recientes':              30_000,   // 30s
  exposicion:                    3_600_000,
  tendencia:                     3_600_000,
  escenario:                       300_000,   // 5min
  'sendai-report':              86_400_000,
  'raster-catalogo':            86_400_000,
  'raster-precip':                  60_000,
}

function isFresh(key: string): boolean {
  const entry = CACHE.get(key)
  if (!entry) return false
  const ttl = Object.entries(TTL).find(([k]) => key.startsWith(k))?.[1] ?? 60_000
  return Date.now() - entry.ts < ttl
}

async function apiFetch<T>(
  path: string,
  params?: Record<string, string | number | boolean | undefined>,
  opts: { cacheKey?: string; retries?: number; timeout?: number } = {},
): Promise<T> {
  const { cacheKey, retries = 2, timeout = 25_000 } = opts

  if (cacheKey && isFresh(cacheKey))
    return (CACHE.get(cacheKey) as CacheEntry<T>).data
  if (cacheKey && INFLIGHT.has(cacheKey))
    return INFLIGHT.get(cacheKey) as Promise<T>

  const url = new URL(`${API}${path}`, window.location.origin)
  if (params)
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== '') url.searchParams.set(k, String(v))
    })

  const headers: Record<string, string> = { Accept: 'application/geo+json, application/json' }
  const cached = cacheKey ? CACHE.get(cacheKey) : undefined
  if (cached?.etag) headers['If-None-Match'] = cached.etag

  const fetchPromise = (async (): Promise<T> => {
    let lastErr: unknown
    for (let attempt = 0; attempt <= retries; attempt++) {
      const ctrl  = new AbortController()
      const timer = setTimeout(() => ctrl.abort(), timeout)
      try {
        const res = await fetch(url.toString(), { signal: ctrl.signal, headers })
        if (res.status === 304 && cached) {
          CACHE.set(cacheKey!, { ...cached, ts: Date.now() })
          return (cached as CacheEntry<T>).data
        }
        if (!res.ok) {
          // Try to parse API error detail for better user messages
          let detail = ''
          try {
            const body = await res.json()
            detail = body?.detail?.mensaje ?? body?.detail ?? body?.error ?? ''
          } catch { /* ignore parse errors */ }
          throw new Error(detail || `HTTP ${res.status} — ${path}`)
        }
        const data = await res.json() as T
        if (cacheKey)
          CACHE.set(cacheKey, { data, ts: Date.now(), etag: res.headers.get('ETag') ?? undefined })
        return data
      } catch (err) {
        lastErr = err
        if (err instanceof Error && err.name === 'AbortError') throw new Error(`Timeout en ${path}`)
        if (attempt < retries)
          await new Promise(r => setTimeout(r, Math.min(800 * 2 ** attempt, 5_000)))
      } finally {
        clearTimeout(timer)
      }
    }
    throw lastErr
  })()

  if (cacheKey) {
    INFLIGHT.set(cacheKey, fetchPromise)
    fetchPromise.finally(() => INFLIGHT.delete(cacheKey))
  }
  return fetchPromise
}

// ══════════════════════════════════════════════════════════
//  Endpoints v8.0 (mantenidos)
// ══════════════════════════════════════════════════════════

export const getSismos = (f: Partial<FiltrosSismos> = {}) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos', {
    mag_min: f.mag_min ?? 2.5, mag_max: f.mag_max ?? 9.9,
    year_start: f.year_start ?? 1900, year_end: f.year_end ?? 2030,
    prof_tipo: f.profundidad, region: f.region, limit: 10_000,
  }, { cacheKey: `sismos:${f.profundidad ?? ''}:${f.region ?? ''}`, retries: 3 })

export const getSismosRecientes = (dias = 30, magMin = 2.5) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos/recientes', { dias, mag_min: magMin },
    { cacheKey: `recientes:${dias}:${magMin}` })

export const getHeatmap = (resolucion = 0.1, magMin = 3.0) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos/heatmap', { resolucion, mag_min: magMin },
    { cacheKey: `heatmap:${resolucion}:${magMin}` })

export const getDepartamentos = (zoom = 7) =>
  apiFetch<GeoJSON.FeatureCollection>('/departamentos', { riesgo_min: 1, zoom },
    { cacheKey: `departamentos:${zoom}` })

export const getDistritos = (zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/distritos', { riesgo_min: 1, zoom },
    { cacheKey: `distritos:${zoom}` })

export const getFallas = (activasOnly = false) =>
  apiFetch<GeoJSON.FeatureCollection>('/fallas', { activas_only: activasOnly },
    { cacheKey: `fallas:${activasOnly}` })

export const getInundaciones = (riesgoMin = 1, zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/inundaciones', { riesgo_min: riesgoMin, zoom },
    { cacheKey: `inundaciones:${riesgoMin}` })

export const getTsunamis = (zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/tsunamis', { riesgo_min: 1, zoom },
    { cacheKey: 'tsunamis' })

export const getDeslizamientos = (riesgoMin = 1, zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/deslizamientos', { riesgo_min: riesgoMin, zoom },
    { cacheKey: `deslizamientos:${riesgoMin}` })

export const getInfraestructura = (tipo?: string, criticidadMin = 3, fuenteTipo?: string) =>
  apiFetch<GeoJSON.FeatureCollection>('/infraestructura',
    { tipo, criticidad_min: criticidadMin, fuente_tipo: fuenteTipo, limit: 1000 },
    { cacheKey: `infra:${tipo}:${criticidadMin}:${fuenteTipo ?? ''}` })

export const getEstaciones = () =>
  apiFetch<GeoJSON.FeatureCollection>('/estaciones', { activas: true }, { cacheKey: 'estaciones' })

export const getEstadisticas = (yearStart = 1900, yearEnd = 2030, magMin = 2.5) =>
  apiFetch<EstadisticaAnual[]>('/sismos/estadisticas',
    { year_start: yearStart, year_end: yearEnd, mag_min: magMin },
    { cacheKey: `stats:${yearStart}:${yearEnd}:${magMin}` })

export const getRiesgo = (lon: number, lat: number) =>
  apiFetch<RiesgoInfo>('/riesgo', { lon, lat },
    { cacheKey: `riesgo:${lon.toFixed(4)}:${lat.toFixed(4)}`, retries: 1, timeout: 10_000 })

export const getDiagnosticoRegiones = () =>
  apiFetch<DiagnosticoLayer[]>('/diagnostico/regiones', undefined,
    { cacheKey: 'diagnostico:regiones', retries: 1 })

export const getSismosCercanos = (lon: number, lat: number, radioKm = 50, magMin = 3.0) =>
  apiFetch<unknown[]>('/sismos/cercanos', { lon, lat, radio_km: radioKm, mag_min: magMin })

export const getResumen = () =>
  apiFetch<Record<string, unknown>>('/resumen', undefined, { cacheKey: 'resumen' })

export const getHealth = () =>
  apiFetch<{ status: string; redis: string; ews: Record<string,number> }>(
    '/health'.replace('/v1', ''), undefined, { retries: 0, timeout: 5_000 })

export const getZonasSismicas = async (): Promise<ZonaSismicaInfo[]> => {
  const res = await apiFetch<{ departamentos?: ZonaSismicaInfo[] } | ZonaSismicaInfo[]>(
    '/zonas-sismicas', undefined, { cacheKey: 'zonas-sismicas', retries: 1 })
  return Array.isArray(res) ? res : ((res as { departamentos?: ZonaSismicaInfo[] }).departamentos ?? [])
}

export const getRiesgoConstruccionPunto = (lon: number, lat: number) =>
  apiFetch<RiesgoConstruccionPunto>('/riesgo/construccion',
    { lon, lat },
    { cacheKey: `riesgo-construccion:${lon.toFixed(4)}:${lat.toFixed(4)}`, retries: 1, timeout: 8_000 })

export const getRiesgoConstruccionRanking = async (
  limit = 20, departamento?: string, order: 'v9'|'v8' = 'v9', incluirIvs = false
): Promise<RiesgoConstruccionRanking[]> => {
  const res = await apiFetch<{ ranking?: RiesgoConstruccionRanking[] } | RiesgoConstruccionRanking[]>(
    '/riesgo/construccion/ranking',
    { limit, departamento, order, incluir_ivs: incluirIvs },
    { cacheKey: `riesgo-construccion-ranking:${limit}:${departamento ?? ''}:${order}` })
  return Array.isArray(res) ? res : ((res as { ranking?: RiesgoConstruccionRanking[] }).ranking ?? [])
}

export const getRiesgoConstruccionMapa = () =>
  apiFetch<GeoJSON.FeatureCollection>('/riesgo/construccion/mapa',
    undefined, { cacheKey: 'riesgo-construccion-mapa', retries: 2 })

export const getCoberturaTipos = async (): Promise<CoberturaTipo[]> => {
  interface RawRow { tipo: string; fuente_tipo: 'oficial'|'osm'; total: number }
  const res = await apiFetch<{ por_tipo: RawRow[] }>(
    '/infraestructura/cobertura', undefined, { cacheKey: 'infra-cobertura', retries: 1 })
  const rows: RawRow[] = Array.isArray(res) ? res : (res?.por_tipo ?? [])
  const pivot = new Map<string, CoberturaTipo>()
  for (const row of rows) {
    const entry = pivot.get(row.tipo) ?? { tipo: row.tipo, total: 0, oficial: 0, osm: 0, pct_oficial: 0 }
    if (row.fuente_tipo === 'oficial') entry.oficial += row.total
    else entry.osm += row.total
    entry.total = entry.oficial + entry.osm
    entry.pct_oficial = entry.total > 0 ? Math.round(entry.oficial / entry.total * 100) : 0
    pivot.set(row.tipo, entry)
  }
  return Array.from(pivot.values()).sort((a, b) => b.total - a.total)
}

export const getPoblacionExposicion = () =>
  apiFetch<PoblacionZona[]>('/poblacion', undefined, { cacheKey: 'poblacion', retries: 1 })

export const getPrecipitaciones = (filtros: Partial<FiltrosPrecipitacion> = {}) =>
  apiFetch<GeoJSON.FeatureCollection>('/precipitaciones', {
    riesgo_inund_min: filtros.riesgo_inund_min ?? 1,
    tipo: filtros.tipo, fen_min: filtros.fen_min,
  }, {
    cacheKey: `precipitaciones:${filtros.tipo ?? ''}:${filtros.riesgo_inund_min ?? 1}:${filtros.fen_min ?? ''}`,
  })

export const getPrecipitacionesCercanas = (lon: number, lat: number, radioKm = 100) =>
  apiFetch<{ punto: object; radio_km: number; zonas: ZonaPrecipitacionCercana[]; total: number }>(
    '/precipitaciones/cercanas', { lon, lat, radio_km: radioKm },
    { cacheKey: `precip-cercanas:${lon.toFixed(3)}:${lat.toFixed(3)}:${radioKm}`, timeout: 8_000 })

export const getEventosFEN = async (opts: {
  tipo?: string; intensidad?: string
  añoDesde?: number; añoHasta?: number; oniMin?: number
} = {}): Promise<EventoFENData[]> => {
  const res = await apiFetch<{ eventos?: EventoFENData[] } | EventoFENData[]>(
    '/fen', {
      tipo: opts.tipo, intensidad: opts.intensidad,
      año_desde: opts.añoDesde ?? 1957, año_hasta: opts.añoHasta ?? 2030,
      oni_min: opts.oniMin,
    },
    { cacheKey: `fen-eventos:${opts.tipo ?? ''}:${opts.intensidad ?? ''}:${opts.añoDesde ?? 1957}` })
  return Array.isArray(res) ? res : ((res as { eventos?: EventoFENData[] }).eventos ?? [])
}

export const getFenEstadisticas = () =>
  apiFetch<FenEstadisticas>('/fen/estadisticas', undefined, { cacheKey: 'fen-estadisticas' })

export const getRiesgoLluvia = (lon: number, lat: number) =>
  apiFetch<RiesgoLluvia>('/riesgo/lluvia', { lon, lat },
    { cacheKey: `riesgo-lluvia:${lon.toFixed(4)}:${lat.toFixed(4)}`, retries: 1, timeout: 10_000 })

// ══════════════════════════════════════════════════════════
//  🆕 Endpoints v9.0
// ══════════════════════════════════════════════════════════

/** 20 volcanes INGEMMET/OVI-IGP 2021 con radios de peligro */
export const getVolcanes = (estado?: string, region?: string) =>
  apiFetch<GeoJSON.FeatureCollection>('/volcanes',
    { estado, region },
    { cacheKey: `volcanes:${estado ?? ''}:${region ?? ''}` })

/** Score de susceptibilidad ML para un punto */
export const getSusceptibilidadPunto = (lon: number, lat: number, amenaza: AmenazaML) =>
  apiFetch<SusceptibilidadPunto>(`/susceptibilidad/${amenaza}`,
    { lon, lat },
    {
      cacheKey: `susceptibilidad-punto:${amenaza}:${lon.toFixed(3)}:${lat.toFixed(3)}`,
      timeout: 15_000,
    })

/** Grilla de susceptibilidad 0.05° para un departamento */
export const getSusceptibilidadMapa = (amenaza: AmenazaML, departamento: string) =>
  apiFetch<GeoJSON.FeatureCollection>(`/susceptibilidad/${amenaza}/mapa`,
    { departamento, zoom: 6 },
    {
      cacheKey: `susceptibilidad-mapa:${amenaza}:${departamento}`,
      timeout: 60_000,
    })

/** Metadata de modelos ML entrenados */
export const getModeloInfo = async (amenaza?: AmenazaML): Promise<Record<string, ModeloMetadata>> => {
  const res = await apiFetch<Record<string, ModeloMetadata> | ModeloMetadata>(
    '/susceptibilidad/modelo/info',
    amenaza ? { amenaza } : undefined,
    { cacheKey: `modelo-info:${amenaza ?? 'all'}` })
  if (amenaza && typeof res === 'object' && 'amenaza' in res)
    return { [amenaza]: res as ModeloMetadata }
  return res as Record<string, ModeloMetadata>
}

/** Inicia entrenamiento ML en background */
export const entrenarModelo = async (amenaza: AmenazaML): Promise<{
  status: string; amenaza: string; estimado_segundos: number; timestamp_inicio: number
}> => {
  const url = new URL(`${API}/susceptibilidad/modelo/entrenar`, window.location.origin)
  url.searchParams.set('amenaza', amenaza)
  const res = await fetch(url.toString(), { method: 'POST' })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

/** Alertas EWS recientes */
export const getAlertasRecientes = async (opts: {
  horas?: number; nivel?: string; incluirCap?: boolean; limit?: number
} = {}): Promise<{ alertas: AlertaRT[]; total: number }> => {
  const res = await apiFetch<{ alertas: AlertaRT[]; total: number }>(
    '/alertas/recientes', {
      horas: opts.horas ?? 24, nivel: opts.nivel,
      incluir_cap: opts.incluirCap ?? false, limit: opts.limit ?? 50,
    },
    { cacheKey: `alertas-recientes:${opts.horas ?? 24}:${opts.nivel ?? ''}`, timeout: 5_000 })
  return res
}

/** Exposición + IVS por ubigeo */
export const getExposicion = (ubigeo: string) =>
  apiFetch<ExposicionDistrito>(`/exposicion/${ubigeo}`,
    undefined,
    { cacheKey: `exposicion:${ubigeo}`, timeout: 8_000 })

/** Tendencia sísmica mensual (TimescaleDB CAG + fallback) */
export const getSismosTendencia = (opts: {
  añoInicio?: number; añoFin?: number; region?: string; magMin?: number
} = {}) =>
  apiFetch<{ tendencia: unknown[]; fuente_query: string }>(
    '/sismos/tendencia', {
      año_inicio: opts.añoInicio ?? 1980, año_fin: opts.añoFin ?? 2030,
      region: opts.region, mag_min: opts.magMin ?? 2.5,
    },
    { cacheKey: `tendencia:${opts.añoInicio ?? 1980}:${opts.añoFin ?? 2030}:${opts.region ?? ''}` })

/** Modelo de pérdidas sísmicas 4DS */
export const getRiesgoEscenario = (
  lon: number, lat: number, magnitud = 7.0,
  profundidad_km = 30, n_viviendas = 1000, hora_del_dia: 'dia'|'noche' = 'dia'
) =>
  apiFetch<RiesgoEscenario>('/riesgo/escenario', {
    lon, lat, magnitud, profundidad_km, n_viviendas, hora_del_dia,
  }, {
    cacheKey: `escenario:${lon.toFixed(2)}:${lat.toFixed(2)}:${magnitud}:${profundidad_km}`,
    timeout: 15_000,
  })

/** Reporte Sendai Framework 7 targets */
export const getSendaiReport = (año = 2024) =>
  apiFetch<SendaiReport>('/sendai/report', { año },
    { cacheKey: `sendai-report:${año}` })

/** Precipitación en punto desde COG MinIO */
export const getRasterPrecipitacion = (lon: number, lat: number) =>
  apiFetch<RasterPrecipitacion>('/raster/precipitacion', { lon, lat },
    { cacheKey: `raster-precip:${lon.toFixed(3)}:${lat.toFixed(3)}`, timeout: 15_000 })

/** Metadata STAC del catálogo de rasters */
export const getRasterCatalogo = () =>
  apiFetch<Record<string, unknown>>('/raster/catalogo',
    undefined, { cacheKey: 'raster-catalogo' })

// ── 🆕 v9.0: SSE + WebSocket EWS ─────────────────────────

/** Crea conexión SSE para alertas en tiempo real */
export function createSSEConnection(
  onAlerta: (a: AlertaRT) => void,
  onPing:   (ts: string) => void,
  onError?: (e: Event) => void,
): EventSource {
  const url = `${BASE}/api/v1/alertas/stream`
  const es  = new EventSource(url)
  es.addEventListener('alerta', (e: MessageEvent) => {
    try { onAlerta(JSON.parse(e.data) as AlertaRT) } catch {}
  })
  es.addEventListener('ping', (e: MessageEvent) => {
    try { onPing((JSON.parse(e.data) as { ts: string }).ts) } catch {}
  })
  if (onError) es.onerror = onError
  return es
}

/** Crea conexión WebSocket para alertas en tiempo real */
export function createWSConnection(
  onAlerta:   (a: AlertaRT) => void,
  onBackfill: (alertas: AlertaRT[]) => void,
  onPing?:    (ts: string) => void,
  onClose?:   () => void,
): WebSocket {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  const host  = BASE ? new URL(BASE).host : window.location.host
  const ws    = new WebSocket(`${proto}//${host}/ws/sismos`)

  ws.onmessage = (e: MessageEvent) => {
    try {
      const msg = JSON.parse(e.data) as { type: string; data?: AlertaRT | AlertaRT[]; ts?: string; total?: number }
      if (msg.type === 'alerta' && msg.data)  onAlerta(msg.data as AlertaRT)
      if (msg.type === 'backfill' && msg.data) onBackfill(msg.data as AlertaRT[])
      if (msg.type === 'ping' && msg.ts && onPing) onPing(msg.ts)
    } catch {}
  }
  if (onClose) ws.onclose = onClose
  return ws
}

// ── Limpieza de caché ─────────────────────────────────────
export const clearCache = (prefix?: string) => {
  if (!prefix) { CACHE.clear(); INFLIGHT.clear(); return }
  for (const k of CACHE.keys())    if (k.startsWith(prefix)) CACHE.delete(k)
  for (const k of INFLIGHT.keys()) if (k.startsWith(prefix)) INFLIGHT.delete(k)
}