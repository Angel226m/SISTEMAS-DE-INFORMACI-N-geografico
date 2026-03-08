// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — useMapData v7.0
// Nuevos: zonasSismicas, riesgoConstruccionMapa,
//         riesgoConstruccionPunto, coberturaTipos
// ══════════════════════════════════════════════════════════

import { useState, useEffect, useCallback, useRef } from 'react'
import {
  getSismos, getDepartamentos, getDistritos, getFallas,
  getInundaciones, getTsunamis, getDeslizamientos,
  getInfraestructura, getEstaciones, getEstadisticas,
  getRiesgo, getDiagnosticoRegiones,
  getZonasSismicas, getRiesgoConstruccionPunto,
  getRiesgoConstruccionRanking, getRiesgoConstruccionMapa,
  getCoberturaTipos,
} from '../services/api'
import type {
  EstadisticaAnual, FiltrosSismos, RiesgoInfo, DiagnosticoLayer,
  ZonaSismicaInfo, RiesgoConstruccionPunto, RiesgoConstruccionRanking,
  CoberturaTipo,
} from '../types'

interface MapData {
  sismos:               GeoJSON.FeatureCollection | null
  departamentos:        GeoJSON.FeatureCollection | null
  distritos:            GeoJSON.FeatureCollection | null
  fallas:               GeoJSON.FeatureCollection | null
  inundaciones:         GeoJSON.FeatureCollection | null
  tsunamis:             GeoJSON.FeatureCollection | null
  deslizamientos:       GeoJSON.FeatureCollection | null
  infraestructura:      GeoJSON.FeatureCollection | null
  estaciones:           GeoJSON.FeatureCollection | null
  estadisticas:         EstadisticaAnual[]
  riesgoConstruccionMapa: GeoJSON.FeatureCollection | null  // v7.0
}

type K = keyof MapData
type Loading = Record<K, boolean>
type Errors  = Record<K, string | null>

const DATA0: MapData = {
  sismos: null, departamentos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, deslizamientos: null,
  infraestructura: null, estaciones: null, estadisticas: [],
  riesgoConstruccionMapa: null,
}
const LOAD0: Loading = {
  sismos: true, departamentos: true, distritos: true, fallas: true,
  inundaciones: true, tsunamis: true, deslizamientos: true,
  infraestructura: true, estaciones: true, estadisticas: true,
  riesgoConstruccionMapa: false,
}
const ERR0: Errors = {
  sismos: null, departamentos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, deslizamientos: null,
  infraestructura: null, estaciones: null, estadisticas: null,
  riesgoConstruccionMapa: null,
}

export interface UseMapDataReturn {
  data:                     MapData
  loading:                  Loading
  errors:                   Errors
  // Riesgo de punto (f_riesgo_punto)
  riesgo:                   RiesgoInfo | null
  riesgoLoading:            boolean
  // Diagnóstico de cobertura espacial
  diagnostico:              DiagnosticoLayer[]
  // v7.0: Zonas sísmicas NTE E.030
  zonasSismicas:            ZonaSismicaInfo[]
  zonasSismicasLoading:     boolean
  // v7.0: IRC de construcción para un punto
  riesgoConstruccionPunto:  RiesgoConstruccionPunto | null
  riesgoConstruccionLoading:boolean
  // v7.0: Ranking de distritos por IRC
  iRCRanking:               RiesgoConstruccionRanking[]
  // v7.0: Cobertura fuente_tipo
  coberturaTipos:           CoberturaTipo[]
  // Acciones
  recargarSismos:   (filtros: Partial<FiltrosSismos>) => void
  buscarRiesgo:     (lon: number, lat: number) => void
  buscarIRC:        (lon: number, lat: number) => void
  recargarTodo:     () => void
  cargarIRCMapa:    () => void
}

export function useMapData(): UseMapDataReturn {
  const [data,          setData]          = useState<MapData>(DATA0)
  const [loading,       setLoading]       = useState<Loading>(LOAD0)
  const [errors,        setErrors]        = useState<Errors>(ERR0)
  const [riesgo,        setRiesgo]        = useState<RiesgoInfo | null>(null)
  const [riesgoLoading, setRiesgoLoading] = useState(false)
  const [diagnostico,   setDiagnostico]   = useState<DiagnosticoLayer[]>([])
  // v7.0
  const [zonasSismicas,             setZonasSismicas]             = useState<ZonaSismicaInfo[]>([])
  const [zonasSismicasLoading,      setZonasSismicasLoading]      = useState(false)
  const [riesgoConstruccionPunto,   setRiesgoConstruccionPunto]   = useState<RiesgoConstruccionPunto | null>(null)
  const [riesgoConstruccionLoading, setRiesgoConstruccionLoading] = useState(false)
  const [iRCRanking,                setIRCRanking]                = useState<RiesgoConstruccionRanking[]>([])
  const [coberturaTipos,            setCoberturaTipos]            = useState<CoberturaTipo[]>([])

  const mounted = useRef(true)
  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])

  const set = useCallback(<T extends K>(key: T, value: MapData[T], err: string | null = null) => {
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

  const cargar = useCallback(async <T extends K>(key: T, fetcher: () => Promise<MapData[T]>) => {
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

  const cargarSismos = useCallback((f: Partial<FiltrosSismos> = {}) => {
    void cargar('sismos', () => getSismos({
      mag_min:     2.5, mag_max: 9.9,
      year_start:  1900, year_end: 2030,
      profundidad: f.profundidad, region: f.region,
    }))
  }, [cargar])

  // v7.0: cargar IRC mapa bajo demanda (es pesado)
  const cargarIRCMapa = useCallback(() => {
    void cargar('riesgoConstruccionMapa', getRiesgoConstruccionMapa)
  }, [cargar])

  useEffect(() => {
    cargarEstaticos()
    cargarSismos()

    // Zonas sísmicas — carga inicial (ligera)
    setZonasSismicasLoading(true)
    getZonasSismicas()
      .then(d => { if (mounted.current) { setZonasSismicas(d); setZonasSismicasLoading(false) } })
      .catch(() => { if (mounted.current) setZonasSismicasLoading(false) })

    // IRC ranking — carga inicial
    getRiesgoConstruccionRanking(20)
      .then(d => { if (mounted.current) setIRCRanking(d) })
      .catch(() => {})

    // Cobertura fuente_tipo
    getCoberturaTipos()
      .then(d => { if (mounted.current) setCoberturaTipos(d) })
      .catch(() => {})

    // Diagnóstico de regiones
    getDiagnosticoRegiones()
      .then(d => { if (mounted.current) setDiagnostico(d) })
      .catch(() => {})
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const recargarSismos = useCallback((filtros: Partial<FiltrosSismos>) => {
    void cargar('sismos', () => getSismos({
      mag_min: 2.5, mag_max: 9.9,
      year_start: 1900, year_end: 2030,
      profundidad: filtros.profundidad, region: filtros.region,
    }))
  }, [cargar])

  const buscarRiesgo = useCallback((lon: number, lat: number) => {
    if (!mounted.current) return
    setRiesgoLoading(true)
    getRiesgo(lon, lat)
      .then(r => { if (mounted.current) { setRiesgo(r); setRiesgoLoading(false) } })
      .catch(() => { if (mounted.current) setRiesgoLoading(false) })
  }, [])

  // v7.0: IRC de punto
  const buscarIRC = useCallback((lon: number, lat: number) => {
    if (!mounted.current) return
    setRiesgoConstruccionLoading(true)
    getRiesgoConstruccionPunto(lon, lat)
      .then(r => { if (mounted.current) { setRiesgoConstruccionPunto(r); setRiesgoConstruccionLoading(false) } })
      .catch(() => { if (mounted.current) setRiesgoConstruccionLoading(false) })
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
    zonasSismicas, zonasSismicasLoading,
    riesgoConstruccionPunto, riesgoConstruccionLoading,
    iRCRanking, coberturaTipos,
    recargarSismos, buscarRiesgo, buscarIRC,
    recargarTodo, cargarIRCMapa,
  }
}