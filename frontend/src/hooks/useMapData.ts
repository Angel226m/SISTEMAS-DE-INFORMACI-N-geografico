// ── Hook principal de datos del mapa ─────────────────────
import { useState, useEffect, useCallback, useRef } from 'react'
import {
  getSismos, getDistritos, getFallas,
  getInundaciones, getInfraestructura, getEstadisticas,
} from '../services/api'
import type { EstadisticaAnual, FiltrosSismos } from '../types'

// ── Tipos internos ─────────────────────────────────────────
interface MapData {
  sismos:         GeoJSON.FeatureCollection | null
  distritos:      GeoJSON.FeatureCollection | null
  fallas:         GeoJSON.FeatureCollection | null
  inundaciones:   GeoJSON.FeatureCollection | null
  infraestructura:GeoJSON.FeatureCollection | null
  estadisticas:   EstadisticaAnual[]
}

type DataKey = keyof MapData

type LoadingState  = Record<DataKey, boolean>
type ErrorState    = Record<DataKey, string | null>

const DATA_INIT: MapData = {
  sismos: null, distritos: null, fallas: null,
  inundaciones: null, infraestructura: null, estadisticas: [],
}
const LOAD_INIT: LoadingState = {
  sismos: true, distritos: true, fallas: true,
  inundaciones: true, infraestructura: true, estadisticas: true,
}
const ERR_INIT: ErrorState = {
  sismos: null, distritos: null, fallas: null,
  inundaciones: null, infraestructura: null, estadisticas: null,
}

export interface UseMapDataReturn {
  data:           MapData
  loading:        LoadingState
  errors:         ErrorState
  recargarSismos: (filtros: Partial<FiltrosSismos>) => void
}

export function useMapData(): UseMapDataReturn {
  const [data,    setData]    = useState<MapData>(DATA_INIT)
  const [loading, setLoading] = useState<LoadingState>(LOAD_INIT)
  const [errors,  setErrors]  = useState<ErrorState>(ERR_INIT)

  // Evitar updates en componentes desmontados
  const mounted = useRef(true)
  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])

  const cargar = useCallback(async <K extends DataKey>(
    key: K,
    fetcher: () => Promise<MapData[K]>,
  ) => {
    if (!mounted.current) return
    setLoading(p => ({ ...p, [key]: true }))
    setErrors(p  => ({ ...p, [key]: null }))
    try {
      const result = await fetcher()
      if (mounted.current) setData(p => ({ ...p, [key]: result }))
    } catch (err) {
      if (!mounted.current) return
      const msg = err instanceof Error ? err.message : 'Error desconocido'
      setErrors(p => ({ ...p, [key]: msg }))
    } finally {
      if (mounted.current) setLoading(p => ({ ...p, [key]: false }))
    }
  }, [])

  // Carga inicial: estáticos en paralelo, sismos con filtros por defecto
  useEffect(() => {
    void cargar('distritos',      getDistritos)
    void cargar('fallas',         getFallas)
    void cargar('inundaciones',   getInundaciones)
    void cargar('infraestructura',getInfraestructura)
    void cargar('estadisticas',   () => getEstadisticas(1900, 2030))
    void cargar('sismos', () => getSismos({ mag_min: 3.0, year_start: 1960, year_end: 2030 }))
  }, [cargar])

  const recargarSismos = useCallback((filtros: Partial<FiltrosSismos>) => {
    void cargar('sismos', () => getSismos(filtros))
  }, [cargar])

  return { data, loading, errors, recargarSismos }
}