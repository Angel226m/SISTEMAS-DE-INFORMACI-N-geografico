// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — API Service v7.0
// Nuevos endpoints: zonas-sismicas, riesgo/construccion,
// infraestructura/cobertura, poblacion, IRC mapa/ranking
// ══════════════════════════════════════════════════════════

import type {
  EstadisticaAnual, FiltrosSismos, RiesgoInfo, DiagnosticoLayer,
  ZonaSismicaInfo, RiesgoConstruccionPunto, RiesgoConstruccionRanking,
  CoberturaTipo, PoblacionZona,
} from '../types'

const BASE = (import.meta.env.VITE_API_URL ?? '') as string
const API  = `${BASE}/api/v1`

// ── Cache en memoria ──────────────────────────────────────
interface CacheEntry<T> { data: T; ts: number; etag?: string }
const CACHE    = new Map<string, CacheEntry<unknown>>()
const INFLIGHT = new Map<string, Promise<unknown>>()

const TTL: Record<string, number> = {
  departamentos:         20 * 60_000,
  distritos:             10 * 60_000,
  fallas:                15 * 60_000,
  inundaciones:          15 * 60_000,
  tsunamis:              15 * 60_000,
  deslizamientos:        15 * 60_000,
  infraestructura:       10 * 60_000,
  estaciones:            20 * 60_000,
  estadisticas:           5 * 60_000,
  sismos:                 5 * 60_000,
  heatmap:                2 * 60_000,
  riesgo:                    30_000,
  diagnostico:            5 * 60_000,
  'zonas-sismicas':      30 * 60_000,
  'riesgo-construccion':      60_000,
  'riesgo-construccion-ranking': 5 * 60_000,
  'riesgo-construccion-mapa':    5 * 60_000,
  'infra-cobertura':      5 * 60_000,
  poblacion:             30 * 60_000,
}

function isFresh(key: string): boolean {
  const entry = CACHE.get(key)
  if (!entry) return false
  const ttl = Object.entries(TTL).find(([k]) => key.startsWith(k))?.[1] ?? 60_000
  return Date.now() - entry.ts < ttl
}

async function apiFetch<T>(
  path:    string,
  params?: Record<string, string | number | boolean | undefined>,
  opts:    { cacheKey?: string; retries?: number; timeout?: number } = {},
): Promise<T> {
  const { cacheKey, retries = 2, timeout = 25_000 } = opts

  if (cacheKey && isFresh(cacheKey)) {
    return (CACHE.get(cacheKey) as CacheEntry<T>).data
  }

  if (cacheKey && INFLIGHT.has(cacheKey)) {
    return INFLIGHT.get(cacheKey) as Promise<T>
  }

  const url = new URL(`${API}${path}`, window.location.origin)
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== '') {
        url.searchParams.set(k, String(v))
      }
    })
  }

  const headers: Record<string, string> = {
    Accept: 'application/geo+json, application/json',
  }
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
        if (!res.ok) throw new Error(`HTTP ${res.status} — ${path}`)

        const data = await res.json() as T
        if (cacheKey) {
          CACHE.set(cacheKey, {
            data, ts: Date.now(),
            etag: res.headers.get('ETag') ?? undefined,
          })
        }
        return data
      } catch (err) {
        lastErr = err
        if (err instanceof Error && err.name === 'AbortError') {
          throw new Error(`Timeout en ${path}`)
        }
        if (attempt < retries) {
          await new Promise(r => setTimeout(r, Math.min(800 * 2 ** attempt, 5_000)))
        }
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
//  Endpoints existentes
// ══════════════════════════════════════════════════════════

export const getSismos = (f: Partial<FiltrosSismos> = {}) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos', {
    mag_min:    f.mag_min    ?? 2.5,
    mag_max:    f.mag_max    ?? 9.9,
    year_start: f.year_start ?? 1900,
    year_end:   f.year_end   ?? 2030,
    prof_tipo:  f.profundidad,
    region:     f.region,
    limit:      10_000,
  }, {
    cacheKey: `sismos:${f.profundidad ?? ''}:${f.region ?? ''}`,
    retries: 3,
  })

export const getSismosRecientes = (dias = 30, magMin = 2.5) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos/recientes',
    { dias, mag_min: magMin },
    { cacheKey: `recientes:${dias}:${magMin}` })

export const getHeatmap = (resolucion = 0.1, magMin = 3.0) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos/heatmap',
    { resolucion, mag_min: magMin },
    { cacheKey: `heatmap:${resolucion}:${magMin}` })

export const getDepartamentos = (zoom = 7) =>
  apiFetch<GeoJSON.FeatureCollection>('/departamentos',
    { riesgo_min: 1, zoom },
    { cacheKey: `departamentos:${zoom}` })

export const getDistritos = (zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/distritos',
    { riesgo_min: 1, zoom },
    { cacheKey: `distritos:${zoom}` })

export const getFallas = (activasOnly = false) =>
  apiFetch<GeoJSON.FeatureCollection>('/fallas',
    { activas_only: activasOnly },
    { cacheKey: `fallas:${activasOnly}` })

export const getInundaciones = (riesgoMin = 1, zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/inundaciones',
    { riesgo_min: riesgoMin, zoom },
    { cacheKey: `inundaciones:${riesgoMin}` })

export const getTsunamis = (zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/tsunamis',
    { riesgo_min: 1, zoom },
    { cacheKey: 'tsunamis' })

export const getDeslizamientos = (riesgoMin = 1, zoom = 9) =>
  apiFetch<GeoJSON.FeatureCollection>('/deslizamientos',
    { riesgo_min: riesgoMin, zoom },
    { cacheKey: `deslizamientos:${riesgoMin}` })

export const getInfraestructura = (tipo?: string, criticidadMin = 3, fuenteTipo?: string) =>
  apiFetch<GeoJSON.FeatureCollection>('/infraestructura',
    { tipo, criticidad_min: criticidadMin, fuente_tipo: fuenteTipo, limit: 1000 },
    { cacheKey: `infra:${tipo}:${criticidadMin}:${fuenteTipo ?? ''}` })

export const getEstaciones = () =>
  apiFetch<GeoJSON.FeatureCollection>('/estaciones',
    { activas: true },
    { cacheKey: 'estaciones' })

export const getEstadisticas = (yearStart = 1900, yearEnd = 2030, magMin = 2.5) =>
  apiFetch<EstadisticaAnual[]>('/sismos/estadisticas',
    { year_start: yearStart, year_end: yearEnd, mag_min: magMin },
    { cacheKey: `stats:${yearStart}:${yearEnd}:${magMin}` })

export const getRiesgo = (lon: number, lat: number) =>
  apiFetch<RiesgoInfo>('/riesgo',
    { lon, lat },
    { cacheKey: `riesgo:${lon.toFixed(4)}:${lat.toFixed(4)}`, retries: 1, timeout: 10_000 })

export const getDiagnosticoRegiones = () =>
  apiFetch<DiagnosticoLayer[]>('/diagnostico/regiones',
    undefined,
    { cacheKey: 'diagnostico:regiones', retries: 1 })

export const getSismosCercanos = (lon: number, lat: number, radioKm = 50, magMin = 3.0) =>
  apiFetch<unknown[]>('/sismos/cercanos',
    { lon, lat, radio_km: radioKm, mag_min: magMin })

export const getResumen = () =>
  apiFetch<Record<string, unknown>>('/resumen', undefined, { cacheKey: 'resumen' })

export const getHealth = () =>
  apiFetch<{ status: string }>('/health'.replace('/v1', ''),
    undefined, { retries: 0, timeout: 5_000 })

// ══════════════════════════════════════════════════════════
//  Nuevos endpoints v7.0
// ══════════════════════════════════════════════════════════

/** Zonas sísmicas NTE E.030-2018 por departamento con sismicidad histórica */
export const getZonasSismicas = async (): Promise<ZonaSismicaInfo[]> => {
  const res = await apiFetch<{ departamentos?: ZonaSismicaInfo[] } | ZonaSismicaInfo[]>(
    '/zonas-sismicas', undefined, { cacheKey: 'zonas-sismicas', retries: 1 }
  )
  // API devuelve { norma, referencia, descripcion_z, departamentos: [...] }
  return Array.isArray(res) ? res : ((res as { departamentos?: ZonaSismicaInfo[] }).departamentos ?? [])
}

/** IRC CENEPRED para un punto geográfico */
export const getRiesgoConstruccionPunto = (lon: number, lat: number) =>
  apiFetch<RiesgoConstruccionPunto>('/riesgo/construccion',
    { lon, lat },
    {
      cacheKey: `riesgo-construccion:${lon.toFixed(4)}:${lat.toFixed(4)}`,
      retries: 1, timeout: 8_000,
    })

/** Ranking de distritos por IRC desde mv_riesgo_construccion */
export const getRiesgoConstruccionRanking = async (limit = 20, departamento?: string): Promise<RiesgoConstruccionRanking[]> => {
  const res = await apiFetch<{ ranking?: RiesgoConstruccionRanking[] } | RiesgoConstruccionRanking[]>(
    '/riesgo/construccion/ranking', { limit, departamento },
    { cacheKey: `riesgo-construccion-ranking:${limit}:${departamento ?? ''}` }
  )
  // API devuelve { metodologia, ponderacion, escala, total_resultados, ranking: [...] }
  return Array.isArray(res) ? res : ((res as { ranking?: RiesgoConstruccionRanking[] }).ranking ?? [])
}

/** GeoJSON distritos coloreados por IRC — para capa de mapa */
export const getRiesgoConstruccionMapa = () =>
  apiFetch<GeoJSON.FeatureCollection>('/riesgo/construccion/mapa',
    undefined,
    { cacheKey: 'riesgo-construccion-mapa', retries: 2 })

/** Cobertura oficial vs OSM por tipo de infraestructura */
export const getCoberturaTipos = async (): Promise<CoberturaTipo[]> => {
  interface RawRow {
    tipo: string; fuente_tipo: 'oficial' | 'osm'; total: number
    criticidad_prom?: number
  }
  const res = await apiFetch<{ resumen: unknown; por_tipo: RawRow[] }>(
    '/infraestructura/cobertura', undefined,
    { cacheKey: 'infra-cobertura', retries: 1 }
  )
  // API devuelve { resumen, por_tipo: rows por (tipo, fuente_tipo) }
  // Pivotar a { tipo, oficial, osm, total, pct_oficial }
  const rows: RawRow[] = Array.isArray(res) ? res : (res?.por_tipo ?? [])
  const pivot = new Map<string, CoberturaTipo>()
  for (const row of rows) {
    const entry = pivot.get(row.tipo) ?? { tipo: row.tipo, total: 0, oficial: 0, osm: 0, pct_oficial: 0 }
    if (row.fuente_tipo === 'oficial') entry.oficial += row.total
    else                               entry.osm     += row.total
    entry.total = entry.oficial + entry.osm
    entry.pct_oficial = entry.total > 0 ? Math.round(entry.oficial / entry.total * 100) : 0
    pivot.set(row.tipo, entry)
  }
  return Array.from(pivot.values()).sort((a, b) => b.total - a.total)
}

/** Población expuesta por zona sísmica (INEI 2017) */
export const getPoblacionExposicion = () =>
  apiFetch<PoblacionZona[]>('/poblacion',
    undefined,
    { cacheKey: 'poblacion', retries: 1 })

// ── Limpieza de caché ─────────────────────────────────────
export const clearCache = (prefix?: string) => {
  if (!prefix) { CACHE.clear(); INFLIGHT.clear(); return }
  for (const k of CACHE.keys())    if (k.startsWith(prefix)) CACHE.delete(k)
  for (const k of INFLIGHT.keys()) if (k.startsWith(prefix)) INFLIGHT.delete(k)
}