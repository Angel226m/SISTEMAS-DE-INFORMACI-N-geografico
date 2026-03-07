// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — useMapData v6.0
// Carga sismos con rango amplio para DataFilterExtension (GPU)
// Sólo recarga sismos desde API si cambia región o profundidad
// ══════════════════════════════════════════════════════════

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  getSismos, getDepartamentos, getDistritos, getFallas,
  getInundaciones, getTsunamis, getDeslizamientos,
  getInfraestructura, getEstaciones, getEstadisticas,
  getRiesgo, getDiagnosticoRegiones,
} from '../services/api'
import type {
  EstadisticaAnual, FiltrosSismos, RiesgoInfo, DiagnosticoLayer,
} from '../types'

interface MapData {
  sismos:          GeoJSON.FeatureCollection | null
  departamentos:   GeoJSON.FeatureCollection | null
  distritos:       GeoJSON.FeatureCollection | null
  fallas:          GeoJSON.FeatureCollection | null
  inundaciones:    GeoJSON.FeatureCollection | null
  tsunamis:        GeoJSON.FeatureCollection | null
  deslizamientos:  GeoJSON.FeatureCollection | null
  infraestructura: GeoJSON.FeatureCollection | null
  estaciones:      GeoJSON.FeatureCollection | null
  estadisticas:    EstadisticaAnual[]
}

type K = keyof MapData
type Loading = Record<K, boolean>
type Errors  = Record<K, string | null>

const DATA0: MapData = {
  sismos: null, departamentos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, deslizamientos: null,
  infraestructura: null, estaciones: null, estadisticas: [],
}
const LOAD0: Loading = {
  sismos: true, departamentos: true, distritos: true, fallas: true,
  inundaciones: true, tsunamis: true, deslizamientos: true,
  infraestructura: true, estaciones: true, estadisticas: true,
}
const ERR0: Errors = {
  sismos: null, departamentos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, deslizamientos: null,
  infraestructura: null, estaciones: null, estadisticas: null,
}

export interface UseMapDataReturn {
  data:           MapData
  loading:        Loading
  errors:         Errors
  riesgo:         RiesgoInfo | null
  riesgoLoading:  boolean
  diagnostico:    DiagnosticoLayer[]
  /**
   * Recarga sismos desde API sólo cuando cambian filtros server-side
   * (region, profundidad). Los filtros mag/year se aplican vía DataFilterExtension.
   */
  recargarSismos: (filtros: Partial<FiltrosSismos>) => void
  buscarRiesgo:   (lon: number, lat: number) => void
  recargarTodo:   () => void
}

export function useMapData(): UseMapDataReturn {
  const [data,          setData]          = useState<MapData>(DATA0)
  const [loading,       setLoading]       = useState<Loading>(LOAD0)
  const [errors,        setErrors]        = useState<Errors>(ERR0)
  const [riesgo,        setRiesgo]        = useState<RiesgoInfo | null>(null)
  const [riesgoLoading, setRiesgoLoading] = useState(false)
  const [diagnostico,   setDiagnostico]   = useState<DiagnosticoLayer[]>([])
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
    void cargar('departamentos',   () => getDepartamentos(7))
    void cargar('distritos',       () => getDistritos(9))
    void cargar('fallas',          getFallas)
    void cargar('inundaciones',    () => getInundaciones(1, 9))
    void cargar('tsunamis',        () => getTsunamis(9))
    void cargar('deslizamientos',  () => getDeslizamientos(1, 9))
    void cargar('infraestructura', getInfraestructura)
    void cargar('estaciones',      getEstaciones)
    void cargar('estadisticas',    () => getEstadisticas(1900, 2030))
  }, [cargar])

  /**
   * Carga amplia: mag >= 2.5, todos los años.
   * El filtrado fino se hace en GPU con DataFilterExtension.
   * Sólo region/profundidad hacen server-side filtering.
   */
  const cargarSismos = useCallback((f: Partial<FiltrosSismos> = {}) => {
    void cargar('sismos', () => getSismos({
      mag_min:     2.5,
      mag_max:     9.9,
      year_start:  1900,
      year_end:    2030,
      // Filtros server-side: sólo pasamos región y profundidad si están presentes
      profundidad: f.profundidad,
      region:      f.region,
    }))
  }, [cargar])

  useEffect(() => {
    cargarEstaticos()
    cargarSismos()
    // Diagnóstico de regiones (para diagnóstico interno)
    getDiagnosticoRegiones()
      .then(d => { if (mounted.current) setDiagnostico(d) })
      .catch(() => {/* silencioso */})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  /**
   * Recargar sismos sólo si cambian filtros server-side (region/profundidad).
   * Si sólo cambian mag/year, NO llames esta función — DataFilterExtension los filtra.
   */
  const recargarSismos = useCallback((filtros: Partial<FiltrosSismos>) => {
    void cargar('sismos', () => getSismos({
      mag_min:     2.5,
      mag_max:     9.9,
      year_start:  1900,
      year_end:    2030,
      profundidad: filtros.profundidad,
      region:      filtros.region,
    }))
  }, [cargar])

  const buscarRiesgo = useCallback((lon: number, lat: number) => {
    if (!mounted.current) return
    setRiesgoLoading(true)
    getRiesgo(lon, lat)
      .then(r => { if (mounted.current) { setRiesgo(r); setRiesgoLoading(false) } })
      .catch(() => { if (mounted.current) setRiesgoLoading(false) })
  }, [])

  const recargarTodo = useCallback(() => {
    if (!mounted.current) return
    setLoading(LOAD0)
    cargarEstaticos()
    cargarSismos()
  }, [cargarEstaticos, cargarSismos])

  return {
    data, loading, errors,
    riesgo, riesgoLoading, diagnostico,
    recargarSismos, buscarRiesgo, recargarTodo,
  }
}