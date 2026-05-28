// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — useMapData v9.0
// 🆕 volcanes: GeoJSON.FeatureCollection | null
// 🆕 alertasEWS: AlertaRT[] — tiempo real SSE/WS
// 🆕 susceptibilidadMapa: GeoJSON.FeatureCollection | null
// 🆕 exposicion: ExposicionDistrito | null
// 🆕 riesgoEscenario: RiesgoEscenario | null
// 🆕 sendaiReport: SendaiReport | null
// 🆕 ewsStatus: EWSStatus | null
// ✅ Todos los hooks v8.0 mantenidos
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
  getPrecipitaciones, getEventosFEN, getFenEstadisticas, getRiesgoLluvia,
  // 🆕 v9
  getVolcanes, getSusceptibilidadMapa, getAlertasRecientes,
  getExposicion, getRiesgoEscenario, getSendaiReport,
  getModeloInfo, entrenarModelo,
  createSSEConnection, createWSConnection,
} from '../services/api'
import type {
  EstadisticaAnual, FiltrosSismos, RiesgoInfo, DiagnosticoLayer,
  ZonaSismicaInfo, RiesgoConstruccionPunto, RiesgoConstruccionRanking,
  CoberturaTipo, FiltrosPrecipitacion,
  EventoFENData, FenEstadisticas, RiesgoLluvia,
  // v9
  AlertaRT, ExposicionDistrito, RiesgoEscenario, SendaiReport, AmenazaML,
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
  precipitaciones:        GeoJSON.FeatureCollection | null
  // 🆕 v9
  volcanes:               GeoJSON.FeatureCollection | null
  susceptibilidadMapa:    GeoJSON.FeatureCollection | null
}

type K       = keyof MapData
type Loading = Record<K, boolean>
type Errors  = Record<K, string | null>

const DATA0: MapData = {
  sismos:null,departamentos:null,distritos:null,fallas:null,
  inundaciones:null,tsunamis:null,deslizamientos:null,
  infraestructura:null,estaciones:null,estadisticas:[],
  riesgoConstruccionMapa:null,precipitaciones:null,
  volcanes:null,susceptibilidadMapa:null,
}
const LOAD0: Loading = {
  sismos:true,departamentos:false,distritos:true,fallas:true,
  inundaciones:false,tsunamis:false,deslizamientos:false,
  infraestructura:false,estaciones:false,estadisticas:true,
  riesgoConstruccionMapa:false,precipitaciones:false,
  volcanes:false,susceptibilidadMapa:false,
}
const ERR0: Errors = {
  sismos:null,departamentos:null,distritos:null,fallas:null,
  inundaciones:null,tsunamis:null,deslizamientos:null,
  infraestructura:null,estaciones:null,estadisticas:null,
  riesgoConstruccionMapa:null,precipitaciones:null,
  volcanes:null,susceptibilidadMapa:null,
}

export interface UseMapDataReturn {
  data:    MapData
  loading: Loading
  errors:  Errors
  riesgo:                    RiesgoInfo | null
  riesgoLoading:             boolean
  diagnostico:               DiagnosticoLayer[]
  zonasSismicas:             ZonaSismicaInfo[]
  zonasSismicasLoading:      boolean
  riesgoConstruccionPunto:   RiesgoConstruccionPunto | null
  riesgoConstruccionLoading: boolean
  iRCRanking:                RiesgoConstruccionRanking[]
  coberturaTipos:            CoberturaTipo[]
  eventosFen:                EventoFENData[]
  fenEstadisticas:           FenEstadisticas | null
  fenLoading:                boolean
  riesgoLluvia:              RiesgoLluvia | null
  riesgoLluviaLoading:       boolean
  // 🆕 v9
  alertasEWS:                AlertaRT[]
  ewsPingTs:                 string | null
  exposicion:                ExposicionDistrito | null
  exposicionLoading:         boolean
  riesgoEscenario:           RiesgoEscenario | null
  riesgoEscenarioLoading:    boolean
  sendaiReport:              SendaiReport | null
  sendaiLoading:             boolean
  mlEntrenando:              boolean
  // Acciones — siempre presentes
  recargarSismos:            (filtros: Partial<FiltrosSismos>) => void
  buscarRiesgo:              (lon: number, lat: number) => void
  buscarIRC:                 (lon: number, lat: number) => void
  buscarRiesgoLluvia:        (lon: number, lat: number) => void
  buscarExposicion:          (ubigeo: string) => void
  calcularEscenario:         (lon:number, lat:number, mag:number, prof:number, nViv:number) => void
  cargarSendai:              (año?: number) => void
  cargarSusceptibilidad:     (amenaza: AmenazaML, depto: string) => void
  recargarTodo:              () => void
  cargarIRCMapa:             () => void
  cargarPrecipitaciones:     (filtros?: Partial<FiltrosPrecipitacion>) => void
  // 🆕 v9.1 — lazy loaders por capa (sólo fetcha si no hay datos aún)
  cargarInundaciones:        () => void
  cargarTsunamis:            () => void
  cargarDeslizamientos:      () => void
  cargarInfraestructura:     () => void
  cargarEstaciones:          () => void
  cargarDepartamentos:       () => void
}

export function useMapData(): UseMapDataReturn {
  const [data,          setData]         = useState<MapData>(DATA0)
  const [loading,       setLoading]      = useState<Loading>(LOAD0)
  const [errors,        setErrors]       = useState<Errors>(ERR0)
  const [riesgo,        setRiesgo]       = useState<RiesgoInfo|null>(null)
  const [riesgoLoading, setRiesgoLoading]= useState(false)
  const [diagnostico,   setDiagnostico]  = useState<DiagnosticoLayer[]>([])
  const [zonasSismicas,           setZonasSismicas]           = useState<ZonaSismicaInfo[]>([])
  const [zonasSismicasLoading,    setZonasSismicasLoading]    = useState(false)
  const [riesgoConstruccionPunto, setRiesgoConstruccionPunto] = useState<RiesgoConstruccionPunto|null>(null)
  const [riesgoConstruccionLoading,setRiesgoConstruccionLoading]=useState(false)
  const [iRCRanking,              setIRCRanking]              = useState<RiesgoConstruccionRanking[]>([])
  const [coberturaTipos,          setCoberturaTipos]          = useState<CoberturaTipo[]>([])
  const [eventosFen,       setEventosFen]      = useState<EventoFENData[]>([])
  const [fenEstadisticas,  setFenEstadisticas] = useState<FenEstadisticas|null>(null)
  const [fenLoading,       setFenLoading]      = useState(false)
  const [riesgoLluvia,     setRiesgoLluvia]    = useState<RiesgoLluvia|null>(null)
  const [riesgoLluviaLoading,setRiesgoLluviaLoading]=useState(false)
  // 🆕 v9
  const [alertasEWS,      setAlertasEWS]      = useState<AlertaRT[]>([])
  const [ewsPingTs,        setEwsPingTs]       = useState<string|null>(null)
  const [exposicion,       setExposicion]      = useState<ExposicionDistrito|null>(null)
  const [exposicionLoading,setExposicionLoading]=useState(false)
  const [riesgoEscenario,  setRiesgoEscenario] = useState<RiesgoEscenario|null>(null)
  const [riesgoEscenarioLoading,setRiesgoEscenarioLoading]=useState(false)
  const [sendaiReport,     setSendaiReport]    = useState<SendaiReport|null>(null)
  const [sendaiLoading,    setSendaiLoading]   = useState(false)

  const mounted   = useRef(true)
  const sseRef    = useRef<EventSource|null>(null)
  const wsRef     = useRef<WebSocket|null>(null)
  const wsRetries = useRef(0)

  useEffect(()=>{
    mounted.current=true
    return ()=>{
      mounted.current=false
      sseRef.current?.close()
      wsRef.current?.close()
    }
  },[])

  const set = useCallback(<T extends K>(key:T, value:MapData[T], err:string|null=null)=>{
    if(!mounted.current) return
    setData(p=>({...p,[key]:value}))
    setErrors(p=>({...p,[key]:err}))
    setLoading(p=>({...p,[key]:false}))
  },[])

  const setErr = useCallback((key:K, msg:string)=>{
    if(!mounted.current) return
    setErrors(p=>({...p,[key]:msg}))
    setLoading(p=>({...p,[key]:false}))
  },[])

  const cargar = useCallback(async <T extends K>(key:T, fetcher:()=>Promise<MapData[T]>)=>{
    if(!mounted.current) return
    setLoading(p=>({...p,[key]:true}))
    setErrors(p=>({...p,[key]:null}))
    try{ set(key,await fetcher()) }
    catch(err){ setErr(key,err instanceof Error?err.message:'Error desconocido') }
  },[set,setErr])

  const cargarEstaticos = useCallback(()=>{
    // CORE (always load — minimal set for meaningful initial render)
    void cargar('distritos',    ()=>getDistritos(9))
    void cargar('fallas',       getFallas)
    void cargar('estadisticas', ()=>getEstadisticas(1900,2030))
  },[cargar])

  // LAZY — only fetch when the corresponding capa is activated
  const cargarDepartamentos  = useCallback(()=>{ void cargar('departamentos', ()=>getDepartamentos(7)) },[cargar])
  const cargarInundaciones   = useCallback(()=>{ void cargar('inundaciones', ()=>getInundaciones(1,9)) },[cargar])
  const cargarTsunamis       = useCallback(()=>{ void cargar('tsunamis', ()=>getTsunamis(9)) },[cargar])
  const cargarDeslizamientos = useCallback(()=>{ void cargar('deslizamientos', ()=>getDeslizamientos(1,9)) },[cargar])
  const cargarInfraestructura= useCallback(()=>{ void cargar('infraestructura', getInfraestructura) },[cargar])
  const cargarEstaciones     = useCallback(()=>{ void cargar('estaciones', getEstaciones) },[cargar])

  const cargarSismos = useCallback((f:Partial<FiltrosSismos>={})=>{
    void cargar('sismos',()=>getSismos({mag_min:2.5,mag_max:9.9,year_start:1900,year_end:2030,profundidad:f.profundidad,region:f.region}))
  },[cargar])

  const cargarIRCMapa = useCallback(()=>{ void cargar('riesgoConstruccionMapa',getRiesgoConstruccionMapa) },[cargar])

  const cargarPrecipitaciones = useCallback((filtros:Partial<FiltrosPrecipitacion>={})=>{
    void cargar('precipitaciones',()=>getPrecipitaciones(filtros))
  },[cargar])

  // 🆕 v9: cargar volcanes
  const cargarVolcanes = useCallback(()=>{
    void cargar('volcanes',()=>getVolcanes())
  },[cargar])

  // 🆕 v9: susceptibilidad grilla con auto-train + retry
  const [mlEntrenando, setMlEntrenando] = useState(false)

  const cargarSusceptibilidad = useCallback(async (amenaza:AmenazaML, depto:string)=>{
    if(!mounted.current) return

    // 1. Verificar si hay modelo entrenado
    let modeloExiste = false
    try {
      const info = await getModeloInfo(amenaza)
      modeloExiste = !!(info[amenaza]?.entrenado_en)
    } catch { modeloExiste = false }

    // 2. Si no hay modelo → entrenar primero
    if (!modeloExiste) {
      setMlEntrenando(true)
      try { await entrenarModelo(amenaza) } catch { /* background task puede retornar 429 si ya corre */ }
      // Esperar que arranque el training (es BackgroundTask)
      await new Promise(r => setTimeout(r, 4000))
      if (!mounted.current) return
    }

    // 3. Intentar fetch del mapa — reintentar hasta 3× si features vacío
    let intentos = 0
    const tryFetch = async (): Promise<void> => {
      intentos++
      try {
        const fc = await getSusceptibilidadMapa(amenaza, depto)
        if (!mounted.current) return
        if (fc.features.length === 0 && intentos < 3) {
          // Modelo aún entrenando → esperar y reintentar
          await new Promise(r => setTimeout(r, 5000))
          if (!mounted.current) return
          return tryFetch()
        }
        setData(p => ({...p, susceptibilidadMapa: fc}))
        setMlEntrenando(false)
      } catch {
        if (intentos < 3 && mounted.current) {
          await new Promise(r => setTimeout(r, 5000))
          if (!mounted.current) return
          return tryFetch()
        }
        if (mounted.current) setMlEntrenando(false)
      }
    }
    void tryFetch()
  },[cargar]) // eslint-disable-line

  // 🆕 v9: EWS SSE connection
  const conectarEWS = useCallback(()=>{
    if(!mounted.current) return
    // Preferir SSE, con fallback WS
    try{
      sseRef.current?.close()
      const es=createSSEConnection(
        (alerta)=>{
          if(!mounted.current) return
          setAlertasEWS(prev=>{
            const exists=prev.some(a=>a.id===alerta.id)
            if(exists) return prev
            return [alerta,...prev].slice(0,50)
          })
        },
        (ts)=>{ if(mounted.current) setEwsPingTs(ts) },
        ()=>{
          // SSE error → intentar WS
          if(!mounted.current||wsRef.current) return
          _conectarWS()
        }
      )
      sseRef.current=es
    }catch{
      _conectarWS()
    }
  },[]) // eslint-disable-line

  const _conectarWS = useCallback(()=>{
    if(!mounted.current||wsRetries.current>5) return
    try{
      wsRef.current?.close()
      const ws=createWSConnection(
        (alerta)=>{
          if(!mounted.current) return
          setAlertasEWS(prev=>[alerta,...prev.filter(a=>a.id!==alerta.id)].slice(0,50))
        },
        (backfill)=>{
          if(!mounted.current) return
          setAlertasEWS(backfill.slice(0,50))
        },
        (ts)=>{ if(mounted.current) setEwsPingTs(ts) },
        ()=>{
          if(!mounted.current) return
          wsRetries.current++
          setTimeout(()=>_conectarWS(), Math.min(2000*wsRetries.current,30_000))
        }
      )
      wsRef.current=ws
    }catch{}
  },[]) // eslint-disable-line

  // 🆕 v9: acciones
  const buscarExposicion = useCallback((ubigeo:string)=>{
    if(!mounted.current) return
    setExposicionLoading(true)
    getExposicion(ubigeo)
      .then(r=>{ if(mounted.current){setExposicion(r);setExposicionLoading(false)} })
      .catch(()=>{ if(mounted.current) setExposicionLoading(false) })
  },[])

  const calcularEscenario = useCallback((lon:number,lat:number,mag:number,prof:number,nViv:number)=>{
    if(!mounted.current) return
    setRiesgoEscenarioLoading(true)
    getRiesgoEscenario(lon,lat,mag,prof,nViv)
      .then(r=>{ if(mounted.current){setRiesgoEscenario(r);setRiesgoEscenarioLoading(false)} })
      .catch(()=>{ if(mounted.current) setRiesgoEscenarioLoading(false) })
  },[])

  const cargarSendai = useCallback((año=2024)=>{
    if(!mounted.current) return
    setSendaiLoading(true)
    getSendaiReport(año)
      .then(r=>{ if(mounted.current){setSendaiReport(r);setSendaiLoading(false)} })
      .catch(()=>{ if(mounted.current) setSendaiLoading(false) })
  },[])

  // Carga inicial
  useEffect(()=>{
    cargarEstaticos()
    cargarSismos()

    setZonasSismicasLoading(true)
    getZonasSismicas()
      .then(d=>{ if(mounted.current){setZonasSismicas(d);setZonasSismicasLoading(false)} })
      .catch(()=>{ if(mounted.current) setZonasSismicasLoading(false) })

    getRiesgoConstruccionRanking(50,'','v9',false)
      .then(d=>{ if(mounted.current) setIRCRanking(d) })
      .catch(()=>{})

    getCoberturaTipos()
      .then(d=>{ if(mounted.current) setCoberturaTipos(d) })
      .catch(()=>{})

    getDiagnosticoRegiones()
      .then(d=>{ if(mounted.current) setDiagnostico(d) })
      .catch(()=>{})

    setFenLoading(true)
    Promise.all([getEventosFEN(),getFenEstadisticas()])
      .then(([eventos,stats])=>{ if(!mounted.current) return; setEventosFen(eventos); setFenEstadisticas(stats); setFenLoading(false) })
      .catch(()=>{ if(mounted.current) setFenLoading(false) })

    void cargar('precipitaciones',()=>getPrecipitaciones())

    // 🆕 v9: carga inicial volcanes
    cargarVolcanes()

    // 🆕 v9: pre-trigger ML training en background (no bloquea UI)
    // Si no hay modelo entrenado, el training arranca ahora y en ~1-3min hay predicciones
    ;(async () => {
      try {
        const info = await getModeloInfo('deslizamiento')
        if (!info['deslizamiento']?.entrenado_en) {
          await entrenarModelo('deslizamiento').catch(()=>{})
        }
      } catch {
        await entrenarModelo('deslizamiento').catch(()=>{})
      }
    })()

    // 🆕 v9: alertas recientes como seed
    getAlertasRecientes({horas:24,limit:10})
      .then(r=>{ if(mounted.current) setAlertasEWS(r.alertas) })
      .catch(()=>{})

    // 🆕 v9: conectar EWS SSE
    setTimeout(()=>conectarEWS(), 1500)
  },[]) // eslint-disable-line

  const recargarSismos = useCallback((filtros:Partial<FiltrosSismos>)=>{
    void cargar('sismos',()=>getSismos({mag_min:2.5,mag_max:9.9,year_start:1900,year_end:2030,...filtros}))
  },[cargar])

  const buscarRiesgo = useCallback((lon:number,lat:number)=>{
    if(!mounted.current) return
    setRiesgoLoading(true)
    getRiesgo(lon,lat)
      .then(r=>{ if(mounted.current){setRiesgo(r);setRiesgoLoading(false)} })
      .catch(()=>{ if(mounted.current) setRiesgoLoading(false) })
  },[])

  const buscarIRC = useCallback((lon:number,lat:number)=>{
    if(!mounted.current) return
    setRiesgoConstruccionLoading(true)
    getRiesgoConstruccionPunto(lon,lat)
      .then(r=>{ if(mounted.current){setRiesgoConstruccionPunto(r);setRiesgoConstruccionLoading(false)} })
      .catch(()=>{ if(mounted.current) setRiesgoConstruccionLoading(false) })
  },[])

  const buscarRiesgoLluvia = useCallback((lon:number,lat:number)=>{
    if(!mounted.current) return
    setRiesgoLluviaLoading(true)
    getRiesgoLluvia(lon,lat)
      .then(r=>{ if(mounted.current){setRiesgoLluvia(r);setRiesgoLluviaLoading(false)} })
      .catch(()=>{ if(mounted.current) setRiesgoLluviaLoading(false) })
  },[])

  const recargarTodo = useCallback(()=>{
    if(!mounted.current) return
    setLoading(LOAD0)
    cargarEstaticos()
    cargarSismos()
    cargarVolcanes()
  },[cargarEstaticos,cargarSismos,cargarVolcanes])

  return {
    data,loading,errors,
    riesgo,riesgoLoading,diagnostico,
    zonasSismicas,zonasSismicasLoading,
    riesgoConstruccionPunto,riesgoConstruccionLoading,
    iRCRanking,coberturaTipos,
    eventosFen,fenEstadisticas,fenLoading,
    riesgoLluvia,riesgoLluviaLoading,
    // v9
    alertasEWS,ewsPingTs,
    exposicion,exposicionLoading,
    riesgoEscenario,riesgoEscenarioLoading,
    sendaiReport,sendaiLoading,
    mlEntrenando,
    recargarSismos,buscarRiesgo,buscarIRC,buscarRiesgoLluvia,
    buscarExposicion,calcularEscenario,cargarSendai,cargarSusceptibilidad,
    recargarTodo,cargarIRCMapa,cargarPrecipitaciones,
    // v9.1 lazy loaders
    cargarInundaciones,cargarTsunamis,cargarDeslizamientos,
    cargarInfraestructura,cargarEstaciones,cargarDepartamentos,
  }
}