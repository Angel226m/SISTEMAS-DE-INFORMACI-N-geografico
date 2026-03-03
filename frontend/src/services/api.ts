// ── GeoRiesgo Ica — Servicio API v3 ──────────────────────
import type { EstadisticaAnual, AnalisisPorDistrito, FiltrosSismos } from '../types'

const BASE = (import.meta.env.VITE_API_URL ?? '/api') as string

// ── Utilidad fetch con timeout y reintentos ────────────────
async function apiFetch<T>(
  path: string,
  params?: Record<string, string | number | undefined>,
  opts: { retries?: number; timeout?: number } = {},
): Promise<T> {
  const { retries = 2, timeout = 20_000 } = opts

  const url = new URL(`${BASE}${path}`, window.location.href)
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined) url.searchParams.set(k, String(v))
    })
  }

  let lastErr: unknown
  for (let attempt = 0; attempt <= retries; attempt++) {
    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), timeout)
    try {
      const res = await fetch(url.toString(), { signal: controller.signal })
      if (!res.ok) throw new Error(`HTTP ${res.status} — ${res.statusText}`)
      return (await res.json()) as T
    } catch (err) {
      lastErr = err
      if (attempt < retries) await sleep(600 * (attempt + 1))
    } finally {
      clearTimeout(timer)
    }
  }
  throw lastErr
}

const sleep = (ms: number) => new Promise(r => setTimeout(r, ms))

// ── Endpoints ─────────────────────────────────────────────

export const getSismos = (f: Partial<FiltrosSismos> = {}) =>
  apiFetch<GeoJSON.FeatureCollection>('/sismos', {
    mag_min:    f.mag_min,
    mag_max:    f.mag_max,
    year_start: f.year_start,
    year_end:   f.year_end,
    profundidad: f.profundidad,
    distrito:   f.distrito,
  })

export const getDistritos = () =>
  apiFetch<GeoJSON.FeatureCollection>('/distritos')

export const getFallas = (activasOnly = true) =>
  apiFetch<GeoJSON.FeatureCollection>('/fallas', { activas_only: activasOnly ? 'true' : 'false' })

export const getInundaciones = () =>
  apiFetch<GeoJSON.FeatureCollection>('/inundaciones')

export const getInfraestructura = (tipo?: string) =>
  apiFetch<GeoJSON.FeatureCollection>('/infraestructura', { tipo })

export const getEstadisticas = (yearStart = 1900, yearEnd = 2030) =>
  apiFetch<EstadisticaAnual[]>('/estadisticas', { year_start: yearStart, year_end: yearEnd })

export const getAnalisisPorDistrito = () =>
  apiFetch<AnalisisPorDistrito[]>('/analisis/por-distrito')

export const getSismosNearFallas = (distanciaKm = 20, magMin = 4.0) =>
  apiFetch<GeoJSON.FeatureCollection>('/analisis/cercania-fallas', {
    distancia_km: distanciaKm,
    mag_min:      magMin,
  })

export const getHealth = () =>
  apiFetch<{ status: string; db: string }>('/health', undefined, { retries: 0, timeout: 5000 })