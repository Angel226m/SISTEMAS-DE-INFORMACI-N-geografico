// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — API Service v4.0
// Cache en memoria + ETag + reintentos exponenciales
// ══════════════════════════════════════════════════════════

import type {
  EstadisticaAnual, FiltrosSismos, HeatmapCell,
} from '../types'

const BASE = (import.meta.env.VITE_API_URL ?? '') as string
const API  = `${BASE}/api/v1`

// ── Cache en memoria (TTL por capa) ───────────────────────
interface CacheEntry<T> {
  data:    T
  ts:      number
  etag?:   string
}
const CACHE = new Map<string, CacheEntry<unknown>>()

const TTL: Record<string, number> = {
  distritos:      10 * 60_000,  // 10 min
  fallas:         15 * 60_000,  // 15 min
  inundaciones:   15 * 60_000,
  tsunamis:       15 * 60_000,
  infraestructura:10 * 60_000,
  estaciones:     20 * 60_000,
  estadisticas:    5 * 60_000,
  sismos:         90_000,        // 90 s
  heatmap:        60_000,
}

function isFresh(key: string): boolean {
  const entry = CACHE.get(key)
  if (!entry) return false
  const ttl = Object.entries(TTL).find(([k]) => key.startsWith(k))?.[1] ?? 60_000
  return Date.now() - entry.ts < ttl
}

// ── Fetch con reintentos y cache ──────────────────────────
async function apiFetch<T>(
  path:    string,
  params?: Record<string, string | number | boolean | undefined>,
  opts:    { cacheKey?: string; retries?: number; timeout?: number } = {},
): Promise<T> {
  const { cacheKey, retries = 2, timeout = 25_000 } = opts

  // Hit cache
  if (cacheKey && isFresh(cacheKey)) {
    return (CACHE.get(cacheKey) as CacheEntry<T>).data
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
  // ETag condicional
  const cached = cacheKey ? CACHE.get(cacheKey) : undefined
  if (cached?.etag) headers['If-None-Match'] = cached.etag

  let lastErr: unknown
  for (let attempt = 0; attempt <= retries; attempt++) {
    const ctrl  = new AbortController()
    const timer = setTimeout(() => ctrl.abort(), timeout)
    try {
      const res = await fetch(url.toString(), { signal: ctrl.signal, headers })

      if (res.status === 304 && cached) {
        // ETag hit — refrescar timestamp
        CACHE.set(cacheKey!, { ...cached, ts: Date.now() })
        return (cached as CacheEntry<T>).data
      }

      if (!res.ok) throw new Error(`HTTP ${res.status} — ${path}`)

      const data = await res.json() as T
      if (cacheKey) {
        CACHE.set(cacheKey, {
          data,
          ts:   Date.now(),
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
        await new Promise(r => setTimeout(r, Math.min(800 * 2 ** attempt, 5000)))
      }
    } finally {
      clearTimeout(timer)
    }
  }
  throw lastErr
}

// ══════════════════════════════════════════════════════════
//  Endpoints
// ══════════════════════════════════════════════════════════

export const getSismos = (f: Partial<FiltrosSismos> = {}) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos', {
    mag_min:    f.mag_min,
    mag_max:    f.mag_max,
    year_start: f.year_start,
    year_end:   f.year_end,
    prof_tipo:  f.profundidad,
    region:     f.region,
    limit:      8000,
  }, { cacheKey: `sismos:${JSON.stringify(f)}`, retries: 3 })

export const getSismosRecientes = (dias = 30, magMin = 2.5) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos/recientes', { dias, mag_min: magMin },
    { cacheKey: `recientes:${dias}:${magMin}` })

export const getHeatmap = (resolucion = 0.1, magMin = 3.0) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos/heatmap',
    { resolucion, mag_min: magMin },
    { cacheKey: `heatmap:${resolucion}:${magMin}` })

export const getDistritos = (simplify = 0.002) =>
  apiFetch<GeoJSON.FeatureCollection>('/distritos',
    { riesgo_min: 1, simplify },
    { cacheKey: 'distritos' })

export const getFallas = (activasOnly = false) =>
  apiFetch<GeoJSON.FeatureCollection>('/fallas',
    { activas_only: activasOnly },
    { cacheKey: `fallas:${activasOnly}` })

export const getInundaciones = (riesgoMin = 1) =>
  apiFetch<GeoJSON.FeatureCollection>('/inundaciones',
    { riesgo_min: riesgoMin },
    { cacheKey: `inundaciones:${riesgoMin}` })

export const getTsunamis = () =>
  apiFetch<GeoJSON.FeatureCollection>('/tsunamis',
    { riesgo_min: 1 },
    { cacheKey: 'tsunamis' })

export const getInfraestructura = (tipo?: string, criticidadMin = 3) =>
  apiFetch<GeoJSON.FeatureCollection>('/infraestructura',
    { tipo, criticidad_min: criticidadMin, limit: 1000 },
    { cacheKey: `infra:${tipo}:${criticidadMin}` })

export const getEstaciones = () =>
  apiFetch<GeoJSON.FeatureCollection>('/estaciones',
    { activas: true },
    { cacheKey: 'estaciones' })

export const getEstadisticas = (yearStart = 1900, yearEnd = 2030, magMin = 2.5) =>
  apiFetch<EstadisticaAnual[]>('/sismos/estadisticas',
    { year_start: yearStart, year_end: yearEnd, mag_min: magMin },
    { cacheKey: `stats:${yearStart}:${yearEnd}:${magMin}` })

export const getSismosCercanos = (lon: number, lat: number, radioKm = 50, magMin = 3.0) =>
  apiFetch<unknown[]>('/sismos/cercanos',
    { lon, lat, radio_km: radioKm, mag_min: magMin })

export const getResumen = () =>
  apiFetch<Record<string, unknown>>('/resumen',
    undefined, { cacheKey: 'resumen' })

export const getHealth = () =>
  apiFetch<{ status: string }>('/health'.replace('/v1', ''),
    undefined, { retries: 0, timeout: 5000 })

// Limpiar cache manualmente
export const clearCache = (prefix?: string) => {
  if (!prefix) { CACHE.clear(); return }
  for (const k of CACHE.keys()) if (k.startsWith(prefix)) CACHE.delete(k)
}