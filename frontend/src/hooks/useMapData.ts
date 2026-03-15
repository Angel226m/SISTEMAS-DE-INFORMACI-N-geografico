// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — useMapData v8.0
// 🆕 precipitaciones: GeoJSON.FeatureCollection | null
// 🆕 eventosFen: EventoFENData[]
// 🆕 fenEstadisticas: FenEstadisticas | null
// 🆕 riesgoLluvia: RiesgoLluvia | null
// 🆕 cargarPrecipitaciones() — lazy load bajo demanda
// 🆕 buscarRiesgoLluvia(lon, lat)
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
  // 🆕 v8.0
  getPrecipitaciones, getEventosFEN, getFenEstadisticas, getRiesgoLluvia,
} from '../services/api'
import type {
  EstadisticaAnual, FiltrosSismos, RiesgoInfo, DiagnosticoLayer,
  ZonaSismicaInfo, RiesgoConstruccionPunto, RiesgoConstruccionRanking,
  CoberturaTipo, FiltrosPrecipitacion,
  EventoFENData, FenEstadisticas, RiesgoLluvia,
} from '../types'

interface MapData {
  sismos:                 GeoJSON.FeatureCollection | null
  departamentos:          GeoJSON.FeatureCollection | null
  distritos:              GeoJSON.FeatureCollection | null
  fallas:                 GeoJSON.FeatureCollection | null
  inundaciones:           GeoJSON.FeatureCollection | null
  tsunamis:               GeoJSON.FeatureCollection | null
  deslizamientos:         GeoJSON.FeatureCollection | null
  infraestructura:        GeoJSON.FeatureCollection | null
  estaciones:             GeoJSON.FeatureCollection | null
  estadisticas:           EstadisticaAnual[]
  riesgoConstruccionMapa: GeoJSON.FeatureCollection | null
  /** 🆕 v8.0 */
  precipitaciones:        GeoJSON.FeatureCollection | null
}

type K       = keyof MapData
type Loading = Record<K, boolean>
type Errors  = Record<K, string | null>

const DATA0: MapData = {
  sismos: null, departamentos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, deslizamientos: null,
  infraestructura: null, estaciones: null, estadisticas: [],
  riesgoConstruccionMapa: null,
  precipitaciones: null,
}
const LOAD0: Loading = {
  sismos: true, departamentos: true, distritos: true, fallas: true,
  inundaciones: true, tsunamis: true, deslizamientos: true,
  infraestructura: true, estaciones: true, estadisticas: true,
  riesgoConstruccionMapa: false,
  precipitaciones: false,
}
const ERR0: Errors = {
  sismos: null, departamentos: null, distritos: null, fallas: null,
  inundaciones: null, tsunamis: null, deslizamientos: null,
  infraestructura: null, estaciones: null, estadisticas: null,
  riesgoConstruccionMapa: null,
  precipitaciones: null,
}

export interface UseMapDataReturn {
  data:    MapData
  loading: Loading
  errors:  Errors
  // Riesgo punto (f_riesgo_punto)
  riesgo:       RiesgoInfo | null
  riesgoLoading:boolean
  // Diagnóstico cobertura espacial
  diagnostico:  DiagnosticoLayer[]
  // Zonas sísmicas NTE E.030
  zonasSismicas:           ZonaSismicaInfo[]
  zonasSismicasLoading:    boolean
  // IRC construcción punto
  riesgoConstruccionPunto:   RiesgoConstruccionPunto | null
  riesgoConstruccionLoading: boolean
  // IRC ranking
  iRCRanking:    RiesgoConstruccionRanking[]
  // Cobertura fuente_tipo
  coberturaTipos:CoberturaTipo[]
  // 🆕 v8.0: eventos FEN y estadísticas
  eventosFen:      EventoFENData[]
  fenEstadisticas: FenEstadisticas | null
  fenLoading:      boolean
  // 🆕 v8.0: riesgo lluvia
  riesgoLluvia:       RiesgoLluvia | null
  riesgoLluviaLoading:boolean
  // Acciones
  recargarSismos:      (filtros: Partial<FiltrosSismos>) => void
  buscarRiesgo:        (lon: number, lat: number) => void
  buscarIRC:           (lon: number, lat: number) => void
  buscarRiesgoLluvia:  (lon: number, lat: number) => void  // 🆕
  recargarTodo:        () => void
  cargarIRCMapa:       () => void
  cargarPrecipitaciones:(filtros?: Partial<FiltrosPrecipitacion>) => void  // 🆕
}

export function useMapData(): UseMapDataReturn {
  const [data,          setData]          = useState<MapData>(DATA0)
  const [loading,       setLoading]       = useState<Loading>(LOAD0)
  const [errors,        setErrors]        = useState<Errors>(ERR0)
  const [riesgo,        setRiesgo]        = useState<RiesgoInfo | null>(null)
  const [riesgoLoading, setRiesgoLoading] = useState(false)
  const [diagnostico,   setDiagnostico]   = useState<DiagnosticoLayer[]>([])
  const [zonasSismicas,              setZonasSismicas]              = useState<ZonaSismicaInfo[]>([])
  const [zonasSismicasLoading,       setZonasSismicasLoading]       = useState(false)
  const [riesgoConstruccionPunto,    setRiesgoConstruccionPunto]    = useState<RiesgoConstruccionPunto | null>(null)
  const [riesgoConstruccionLoading,  setRiesgoConstruccionLoading]  = useState(false)
  const [iRCRanking,                 setIRCRanking]                 = useState<RiesgoConstruccionRanking[]>([])
  const [coberturaTipos,             setCoberturaTipos]             = useState<CoberturaTipo[]>([])
  // 🆕 v8.0
  const [eventosFen,       setEventosFen]       = useState<EventoFENData[]>([])
  const [fenEstadisticas,  setFenEstadisticas]  = useState<FenEstadisticas | null>(null)
  const [fenLoading,       setFenLoading]        = useState(false)
  const [riesgoLluvia,     setRiesgoLluvia]      = useState<RiesgoLluvia | null>(null)
  const [riesgoLluviaLoading, setRiesgoLluviaLoading] = useState(false)

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
      mag_min: 2.5, mag_max: 9.9, year_start: 1900, year_end: 2030,
      profundidad: f.profundidad, region: f.region,
    }))
  }, [cargar])

  const cargarIRCMapa = useCallback(() => {
    void cargar('riesgoConstruccionMapa', getRiesgoConstruccionMapa)
  }, [cargar])

  // 🆕 v8.0: cargar zonas precipitación (bajo demanda)
  const cargarPrecipitaciones = useCallback((filtros: Partial<FiltrosPrecipitacion> = {}) => {
    void cargar('precipitaciones', () => getPrecipitaciones(filtros))
  }, [cargar])

  // 🆕 v8.0: riesgo lluvia para un punto
  const buscarRiesgoLluvia = useCallback((lon: number, lat: number) => {
    if (!mounted.current) return
    setRiesgoLluviaLoading(true)
    getRiesgoLluvia(lon, lat)
      .then(r => { if (mounted.current) { setRiesgoLluvia(r); setRiesgoLluviaLoading(false) } })
      .catch(() => { if (mounted.current) setRiesgoLluviaLoading(false) })
  }, [])

  useEffect(() => {
    cargarEstaticos()
    cargarSismos()

    // Zonas sísmicas (ligera)
    setZonasSismicasLoading(true)
    getZonasSismicas()
      .then(d => { if (mounted.current) { setZonasSismicas(d); setZonasSismicasLoading(false) } })
      .catch(() => { if (mounted.current) setZonasSismicasLoading(false) })

    // IRC ranking
    getRiesgoConstruccionRanking(20)
      .then(d => { if (mounted.current) setIRCRanking(d) })
      .catch(() => {})

    // Cobertura fuente_tipo
    getCoberturaTipos()
      .then(d => { if (mounted.current) setCoberturaTipos(d) })
      .catch(() => {})

    // Diagnóstico regiones
    getDiagnosticoRegiones()
      .then(d => { if (mounted.current) setDiagnostico(d) })
      .catch(() => {})

    // 🆕 v8.0: eventos FEN y estadísticas
    setFenLoading(true)
    Promise.all([getEventosFEN(), getFenEstadisticas()])
      .then(([eventos, stats]) => {
        if (!mounted.current) return
        setEventosFen(eventos)
        setFenEstadisticas(stats)
        setFenLoading(false)
      })
      .catch(() => { if (mounted.current) setFenLoading(false) })

    // 🆕 v8.0: precipitaciones — carga inicial completa (sin filtros)
    void cargar('precipitaciones', () => getPrecipitaciones())
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  const recargarSismos = useCallback((filtros: Partial<FiltrosSismos>) => {
    void cargar('sismos', () => getSismos({
      mag_min: 2.5, mag_max: 9.9, year_start: 1900, year_end: 2030,
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
    eventosFen, fenEstadisticas, fenLoading,
    riesgoLluvia, riesgoLluviaLoading,
    recargarSismos, buscarRiesgo, buscarIRC,
    buscarRiesgoLluvia,
    recargarTodo, cargarIRCMapa,
    cargarPrecipitaciones,
  }
}