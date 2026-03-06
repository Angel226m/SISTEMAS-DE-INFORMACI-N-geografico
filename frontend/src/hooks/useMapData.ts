// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — useMapData v4.0
// Carga paralela, caché, retry, abortable
// ══════════════════════════════════════════════════════════

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  getSismos, getDistritos, getFallas,
  getInundaciones, getTsunamis, getInfraestructura,
  getEstaciones, getEstadisticas,
} from '../services/api'
import type { EstadisticaAnual, FiltrosSismos } from '../types'

interface MapData {
  sismos:          GeoJSON.FeatureCollection | null
  distritos:       GeoJSON.FeatureCollection | null
  fallas:          GeoJSON.FeatureCollection | null
  inundaciones:    GeoJSON.FeatureCollection | null
  tsunamis:        GeoJSON.FeatureCollection | null
  infraestructura: GeoJSON.FeatureCollection | null
  estaciones:      GeoJSON.FeatureCollection | null
  estadisticas:    EstadisticaAnual[]
}

type K = keyof MapData

type Loading = Record<K, boolean>
type Errors  = Record<K, string | null>

const DATA0: MapData = {
  sismos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, infraestructura: null,
  estaciones: null, estadisticas: [],
}
const LOAD0: Loading = {
  sismos: true, distritos: true, fallas: true,
  inundaciones: true, tsunamis: true, infraestructura: true,
  estaciones: true, estadisticas: true,
}
const ERR0: Errors = {
  sismos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, infraestructura: null,
  estaciones: null, estadisticas: null,
}

export interface UseMapDataReturn {
  data:            MapData
  loading:         Loading
  errors:          Errors
  recargarSismos:  (filtros: Partial<FiltrosSismos>) => void
  recargarTodo:    () => void
}

export function useMapData(): UseMapDataReturn {
  const [data,    setData]    = useState<MapData>(DATA0)
  const [loading, setLoading] = useState<Loading>(LOAD0)
  const [errors,  setErrors]  = useState<Errors>(ERR0)
  const mounted = useRef(true)

  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])

  const set = useCallback(<T extends K>(
    key: T, value: MapData[T], err: string | null = null,
  ) => {
    if (!mounted.current) return
    setData(p    => ({ ...p, [key]: value }))
    setErrors(p  => ({ ...p, [key]: err }))
    setLoading(p => ({ ...p, [key]: false }))
  }, [])

  const setErr = useCallback((key: K, msg: string) => {
    if (!mounted.current) return
    setErrors(p  => ({ ...p, [key]: msg }))
    setLoading(p => ({ ...p, [key]: false }))
  }, [])

  const cargar = useCallback(async <T extends K>(
    key: T,
    fetcher: () => Promise<MapData[T]>,
  ) => {
    if (!mounted.current) return
    setLoading(p => ({ ...p, [key]: true }))
    setErrors(p  => ({ ...p, [key]: null }))
    try {
      const result = await fetcher()
      set(key, result)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Error desconocido'
      setErr(key, msg)
    }
  }, [set, setErr])

  const cargarEstaticos = useCallback(() => {
    // Cargar capas estáticas en paralelo (no bloquean entre sí)
    void cargar('distritos',       getDistritos)
    void cargar('fallas',          getFallas)
    void cargar('inundaciones',    getInundaciones)
    void cargar('tsunamis',        getTsunamis)
    void cargar('infraestructura', getInfraestructura)
    void cargar('estaciones',      getEstaciones)
    void cargar('estadisticas',    () => getEstadisticas(1900, 2030))
  }, [cargar])

  const cargarSismos = useCallback((f: Partial<FiltrosSismos> = {}) => {
    void cargar('sismos', () => getSismos({ mag_min: 3.0, year_start: 1960, year_end: 2030, ...f }))
  }, [cargar])

  useEffect(() => {
    cargarEstaticos()
    cargarSismos()
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const recargarSismos = useCallback((f: Partial<FiltrosSismos>) => {
    void cargar('sismos', () => getSismos(f))
  }, [cargar])

  const recargarTodo = useCallback(() => {
    if (!mounted.current) return
    setLoading(LOAD0)
    cargarEstaticos()
    cargarSismos()
  }, [cargarEstaticos, cargarSismos])

  return { data, loading, errors, recargarSismos, recargarTodo }
}