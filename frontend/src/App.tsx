// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — App.tsx v9.0  ENTERPRISE
// ══════════════════════════════════════════════════════════

import { useState, useCallback, useEffect, useRef } from 'react'
import MapView        from './components/MapView'
import LayerPanel     from './components/LayerPanel'
import FilterPanel    from './components/FilterPanel'
import type { FiltrosV9 } from './components/FilterPanel'
import StatsChart     from './components/StatsChart'
import LandingPage    from './components/Landingpage'
import { useMapData } from './hooks/useMapData'
import type {
  CapasActivas, FiltrosSismos, TipoVista, TooltipInfo,
  FiltrosPrecipitacion,
} from './types'
import type { MapStyle } from './components/MapView'
import { C }              from './components/ui/constants'
import { Icons }          from './components/ui/Icons'
import { Btn }            from './components/ui/Btn'
import { Loader }         from './components/Loader'
import { ToastList }      from './components/ToastList'
import type { Toast }     from './components/ToastList'
import { HoverTooltip }   from './components/HoverTooltip'
import { InfoPopup }      from './components/InfoPopup'
import { RiesgoPanel }    from './components/RiesgoPanel'
import { ErrorBoundary }  from './components/ErrorBoundary'
import type { FuenteTipo } from './types'

const CAPAS_INIT: CapasActivas = {
  sismos:true,heatmap:false,departamentos:false,fallas:true,inundaciones:false,
  tsunamis:false,deslizamientos:false,riesgo_distritos:true,infraestructura:false,
  estaciones:false,riesgo_construccion:false,precipitaciones:false,
  volcanes:true,susceptibilidad:false,alertas_ews:true,extrusion_3d:false,
}
const FILTROS_INIT: FiltrosSismos    = {mag_min:3.0,mag_max:9.5,year_start:1960,year_end:2030}
const PRECIP_INIT:  FiltrosPrecipitacion = {riesgo_inund_min:1}
const V9_INIT:      FiltrosV9 = {mlScoreMin:0.5, amenazaML:'deslizamiento'}

export default function App(){
  const[showLanding,setShowLanding]=useState(true)
  const[capas,setCapas]=useState<CapasActivas>(CAPAS_INIT)
  const[filtros,setFiltros]=useState<FiltrosSismos>(FILTROS_INIT)
  const[filtrosPrecip,setFiltrosPrecip]=useState<FiltrosPrecipitacion>(PRECIP_INIT)
  const[filtrosV9,setFiltrosV9]=useState<FiltrosV9>(V9_INIT)
  const[fuenteTipo,setFuenteTipo]=useState<FuenteTipo>('todos')
  const[vista,setVista]=useState<TipoVista>('2d')
  const[mapStyle,setMapStyle]=useState<MapStyle>('light')
  const[popup,setPopup]=useState<{props:Record<string,unknown>;layer:string}|null>(null)
  const[tooltip,setTooltip]=useState<TooltipInfo|null>(null)
  const[tab,setTab]=useState<'capas'|'filtros'>('capas')
  const[sidebar,setSidebar]=useState(true)
  const[chart,setChart]=useState(true)
  const[toasts,setToasts]=useState<Toast[]>([])
  const[showRiesgo,setShowRiesgo]=useState(false)

  const{data,loading,errors,riesgo,riesgoLoading,riesgoConstruccionPunto,riesgoConstruccionLoading,
    iRCRanking,coberturaTipos,eventosFen,fenEstadisticas:_fen,fenLoading:_fl,
    riesgoLluvia,riesgoLluviaLoading,alertasEWS,ewsPingTs:_ewsPing,
    riesgoEscenario,sendaiReport,mlEntrenando,
    recargarSismos,buscarRiesgo,buscarIRC,buscarRiesgoLluvia,
    calcularEscenario,cargarSendai,cargarSusceptibilidad,
    recargarTodo,cargarIRCMapa,cargarPrecipitaciones,
    cargarInundaciones,cargarTsunamis,cargarDeslizamientos,
    cargarInfraestructura,cargarEstaciones,cargarDepartamentos,
  }=useMapData()

  const filtrosRef=useRef(filtros)
  filtrosRef.current=filtros
  const totalKeys=Object.keys(loading).length
  const doneKeys=Object.values(loading).filter(v=>!v).length
  const loadPct=(doneKeys/totalKeys)*100
  const isInitial=doneKeys<3
  const totalErr=Object.values(errors).filter(Boolean).length

  const shownErr=useRef(new Set<string>())
  useEffect(()=>{
    Object.entries(errors).forEach(([k,v])=>{
      if(!v) return
      const uid=`${k}:${v}`;if(shownErr.current.has(uid)) return
      shownErr.current.add(uid);addToast('error',`${k}: ${v}`)
    })
  },[errors])
  function addToast(type:Toast['type'],msg:string){const id=Date.now()+Math.random();setToasts(p=>[...p,{id,type,msg}]);setTimeout(()=>setToasts(p=>p.filter(t=>t.id!==id)),5000)}

  useEffect(()=>{if(capas.riesgo_construccion&&!data.riesgoConstruccionMapa) cargarIRCMapa()},[capas.riesgo_construccion]) // eslint-disable-line
  useEffect(()=>{if(capas.precipitaciones) cargarPrecipitaciones(filtrosPrecip)},[filtrosPrecip,capas.precipitaciones]) // eslint-disable-line
  useEffect(()=>{if(capas.susceptibilidad&&filtrosV9.amenazaML) cargarSusceptibilidad(filtrosV9.amenazaML,'Ica')},[filtrosV9.amenazaML,capas.susceptibilidad]) // eslint-disable-line
  // 🆕 v9.1 — lazy loaders for secondary layers
  useEffect(()=>{if(capas.departamentos&&!data.departamentos) cargarDepartamentos()},[capas.departamentos]) // eslint-disable-line
  useEffect(()=>{if(capas.inundaciones&&!data.inundaciones) cargarInundaciones()},[capas.inundaciones]) // eslint-disable-line
  useEffect(()=>{if(capas.tsunamis&&!data.tsunamis) cargarTsunamis()},[capas.tsunamis]) // eslint-disable-line
  useEffect(()=>{if(capas.deslizamientos&&!data.deslizamientos) cargarDeslizamientos()},[capas.deslizamientos]) // eslint-disable-line
  useEffect(()=>{if(capas.infraestructura&&!data.infraestructura) cargarInfraestructura()},[capas.infraestructura]) // eslint-disable-line
  useEffect(()=>{if(capas.estaciones&&!data.estaciones) cargarEstaciones()},[capas.estaciones]) // eslint-disable-line

  useEffect(()=>{
    const h=(e:KeyboardEvent)=>{
      if(e.target instanceof HTMLInputElement) return
      if(e.key==='l'||e.key==='L') setSidebar(p=>!p)
      if(e.key==='f'||e.key==='F'){setSidebar(true);setTab('filtros')}
      if(e.key==='g'||e.key==='G') setChart(p=>!p)
      if(e.key==='Escape'){setPopup(null);setTooltip(null);setShowRiesgo(false)}
      if(e.key==='r'||e.key==='R') window.dispatchEvent(new CustomEvent('geo:center-ica'))
    }
    window.addEventListener('keydown',h);return()=>window.removeEventListener('keydown',h)
  },[])

  const handleFiltros=useCallback((f:FiltrosSismos)=>{
    setFiltros(f);const prev=filtrosRef.current
    if(f.region!==prev.region||f.profundidad!==prev.profundidad) recargarSismos(f)
  },[recargarSismos])
  const handleClick=useCallback((props:Record<string,unknown>,layer:string)=>{setTooltip(null);setPopup({props,layer});setShowRiesgo(false)},[])

  const safeCoberturas=Array.isArray(coberturaTipos)?coberturaTipos:[]
  const totalOficial=safeCoberturas.reduce((s,c)=>s+c.oficial,0)
  const totalOSM=safeCoberturas.reduce((s,c)=>s+c.osm,0)
  const totalInfra=totalOficial+totalOSM
  const alertasActivas=alertasEWS.filter(a=>a.nivel_alerta!=='watch').length
  const sidebarW=sidebar?268:0

  return(
    <div style={{width:'100vw',height:'100vh',overflow:'hidden',background:C.bg,display:'flex',flexDirection:'column'}}>
      {showLanding&&<LandingPage onEnter={()=>setShowLanding(false)}/>}
      {!showLanding&&(<>
        {isInitial&&<Loader pct={loadPct}/>}
        <ToastList toasts={toasts} remove={id=>setToasts(p=>p.filter(t=>t.id!==id))}/>
        <header role="banner" aria-label="GeoRiesgo Perú — panel de control" style={{flexShrink:0,height:52,display:'flex',alignItems:'center',justifyContent:'space-between',padding:'0 14px',background:C.bg,borderBottom:`1px solid ${C.border}`,zIndex:20}}>
          <div style={{display:'flex',alignItems:'center',gap:10}}>
            <Btn active={sidebar} onClick={()=>setSidebar(p=>!p)} title="Sidebar [L]"><Icons.Menu/></Btn>
            <div style={{display:'flex',alignItems:'center',gap:9}}>
              <div style={{width:32,height:32,borderRadius:9,background:`linear-gradient(135deg,${C.primary},${C.secondary})`,display:'flex',alignItems:'center',justifyContent:'center',fontSize:16,color:'white',fontWeight:800}}>G</div>
              <div>
                <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:800,color:C.text,letterSpacing:'-0.02em',lineHeight:1.1}}>GeoRiesgo <span style={{color:C.primary}}>Perú</span></div>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,letterSpacing:'0.1em',textTransform:'uppercase'}}>Multi-hazard · ML · EWS · v9.1</div>
              </div>
            </div>
          </div>
          <div style={{display:'flex',alignItems:'center',gap:8}}>
            <div style={{display:'flex',alignItems:'center',gap:8,padding:'5px 13px',background:C.bgSoft,border:`1px solid ${C.border}`,borderRadius:99}}>
              <div style={{width:8,height:8,borderRadius:'50%',background:C.primary,animation:'pring 1.8s ease-out infinite'}}/>
              {loading.sismos
                ?<span style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:C.textMuted}}>Cargando...</span>
                :<span style={{fontFamily:"'DM Mono',monospace",fontSize:11,fontWeight:700,color:C.text}}>{(data.sismos?.features.length??0).toLocaleString('es-PE')} sismos</span>
              }
            </div>
            {alertasEWS.length>0&&(
              <div style={{display:'flex',alignItems:'center',gap:6,padding:'5px 10px',background:alertasActivas>0?'#fef2f2':'#fff7ed',border:`1px solid ${alertasActivas>0?'#fecaca':'#fed7aa'}`,borderRadius:99}}>
                <div style={{width:7,height:7,borderRadius:'50%',background:alertasActivas>0?C.danger:C.amber,animation:alertasActivas>0?'pring 1s ease-out infinite':'none'}}/>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:alertasActivas>0?C.danger:C.amber}}>{alertasEWS.length} EWS{alertasActivas>0?` · ${alertasActivas} act.`:''}</span>
              </div>
            )}
          </div>
          <div style={{display:'flex',alignItems:'center',gap:5}}>
            <div style={{display:'flex',background:C.bgMuted,border:`1px solid ${C.border}`,borderRadius:8,padding:2,marginRight:4}}>
              {(['light','dark'] as MapStyle[]).map(k=>(
                <button key={k} onClick={()=>setMapStyle(k)} style={{padding:'3px 9px',borderRadius:6,border:'none',cursor:'pointer',fontFamily:"'DM Mono',monospace",fontSize:9,fontWeight:700,background:mapStyle===k?C.bg:'transparent',color:mapStyle===k?C.text:C.textMuted,transition:'all 0.18s'}}>{k==='light'?'Claro':'Oscuro'}</button>
              ))}
            </div>
            <div style={{display:'flex',background:C.bgMuted,border:`1px solid ${C.border}`,borderRadius:8,padding:2,marginRight:4}}>
              {(['2d','3d'] as TipoVista[]).map(v=>(
                <button key={v} onClick={()=>setVista(v)} style={{padding:'3px 9px',borderRadius:6,border:'none',cursor:'pointer',fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,textTransform:'uppercase',background:vista===v?C.bg:'transparent',color:vista===v?C.text:C.textMuted,transition:'all 0.18s'}}>{v}</button>
              ))}
            </div>
            <Btn active={chart} onClick={()=>setChart(p=>!p)} title="Gráfica [G]"><Icons.Chart/></Btn>
            <Btn active={tab==='capas'&&sidebar} onClick={()=>{setSidebar(true);setTab('capas')}} title="Capas [L]"><Icons.Layers/></Btn>
            <Btn active={tab==='filtros'&&sidebar} onClick={()=>{setSidebar(true);setTab('filtros')}} title="Filtros [F]"><Icons.Filter/></Btn>
            <Btn active={false} onClick={()=>{recargarTodo();addToast('success','Recargando...')}} title="Recargar"><Icons.Refresh/></Btn>
          </div>
        </header>
        <div style={{flex:1,display:'flex',overflow:'hidden',position:'relative'}}>
          <aside role="complementary" aria-label="Panel de capas y filtros" style={{flexShrink:0,width:sidebarW,overflow:'hidden',transition:'width 0.26s cubic-bezier(0.4,0,0.2,1)',background:C.bg,borderRight:`1px solid ${C.border}`,display:'flex',flexDirection:'column',zIndex:10}}>
            <div style={{width:268,height:'100%',display:'flex',flexDirection:'column',padding:'12px 12px 0'}}>
              <div style={{display:'flex',gap:3,background:C.bgMuted,borderRadius:10,padding:3,marginBottom:16,flexShrink:0}}>
                {(['capas','filtros'] as const).map(t=>(
                  <button key={t} onClick={()=>setTab(t)} style={{flex:1,padding:'6px 0',borderRadius:8,border:'none',cursor:'pointer',fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,textTransform:'uppercase',letterSpacing:'0.05em',background:tab===t?C.bg:'transparent',color:tab===t?C.text:C.textMuted,boxShadow:tab===t?'0 1px 3px rgba(0,0,0,0.06)':'none',transition:'all 0.18s',display:'flex',alignItems:'center',justifyContent:'center',gap:5}}>
                    {t==='capas'?<><Icons.Layers/> Capas</>:<><Icons.Filter/> Filtros</>}
                  </button>
                ))}
              </div>
              <div style={{flex:1,overflowY:'auto',paddingBottom:12}}>
                {tab==='capas'
                  ?<LayerPanel capas={capas} onChange={setCapas} mlEntrenando={mlEntrenando}/>
                  :<FilterPanel filtros={filtros} onChange={handleFiltros} fuenteTipo={fuenteTipo} onFuenteTipoChange={setFuenteTipo} filtrosPrecip={filtrosPrecip} onFiltrosPrecipChange={setFiltrosPrecip} filtrosV9={filtrosV9} onFiltrosV9Change={setFiltrosV9}/>
                }
              </div>
              <div style={{paddingTop:10,paddingBottom:12,borderTop:`1px solid ${C.border}`,flexShrink:0}}>
                {data.volcanes&&(
                  <div style={{display:'flex',gap:8,marginBottom:8,padding:'5px 8px',background:'#fef2f2',borderRadius:7}}>
                    <div><div style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:'#dc2626'}}>{data.volcanes.features.filter(f=>String((f.properties as Record<string,unknown>)?.estado??'').includes('activo')).length}</div><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>activos</div></div>
                    <div style={{width:1,background:C.border}}/>
                    <div><div style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:'#f97316'}}>{data.volcanes.features.length}</div><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>total</div></div>
                    <div style={{flex:1,display:'flex',alignItems:'center',justifyContent:'flex-end'}}><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>INGEMMET 2021</div></div>
                  </div>
                )}
                {eventosFen.length>0&&(
                  <div style={{display:'flex',gap:8,marginBottom:8,padding:'5px 8px',background:'#fff7ed',borderRadius:7}}>
                    <div><div style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:C.orange}}>{eventosFen.filter(e=>e.tipo==='el_nino').length}</div><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>El Niño</div></div>
                    <div style={{width:1,background:C.border}}/>
                    <div><div style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:C.teal}}>{eventosFen.filter(e=>e.tipo==='la_nina').length}</div><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>La Niña</div></div>
                    <div style={{flex:1,display:'flex',alignItems:'center',justifyContent:'flex-end'}}><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>NOAA-CPC</div></div>
                  </div>
                )}
                {totalInfra>0&&(
                  <div style={{display:'flex',gap:8,marginBottom:8,padding:'6px 8px',background:C.primaryBg,borderRadius:7}}>
                    <div><div style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:C.primary}}>{totalOficial.toLocaleString()}</div><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>oficial</div></div>
                    <div style={{width:1,background:C.border}}/>
                    <div><div style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:'#6366f1'}}>{totalOSM.toLocaleString()}</div><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>OSM</div></div>
                    <div style={{flex:1,display:'flex',alignItems:'center',justifyContent:'flex-end'}}><div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>infra validada</div></div>
                  </div>
                )}
                <p style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,lineHeight:2.0}}>USGS · IGP · INGEMMET · SENAMHI<br/>NOAA-CPC · GEM · CENEPRED · INDECI</p>
                <p style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.textMuted,marginTop:2}}>[L] panel  [F] filtros  [G] gráfica  [R] centrar</p>
              </div>
            </div>
          </aside>
          <div style={{flex:1,display:'flex',flexDirection:'column',overflow:'hidden',position:'relative'}}>
            <div style={{flex:1,position:'relative'}}>
              <ErrorBoundary>
                <MapView
                  sismos={data.sismos} departamentos={data.departamentos} distritos={data.distritos}
                  fallas={data.fallas} inundaciones={data.inundaciones} tsunamis={data.tsunamis}
                  deslizamientos={data.deslizamientos} infraestructura={data.infraestructura}
                  estaciones={data.estaciones} riesgoConstruccionMapa={data.riesgoConstruccionMapa}
                  precipitaciones={data.precipitaciones} volcanes={data.volcanes}
                  susceptibilidadMapa={data.susceptibilidadMapa}
                  alertasEWS={capas.alertas_ews?alertasEWS:[]}
                  capas={capas} vista={vista} mapStyle={mapStyle} filtros={filtros}
                  onClickFeature={handleClick} onHoverFeature={setTooltip}
                />
              </ErrorBoundary>
              {tooltip&&!popup&&<HoverTooltip info={tooltip}/>}
              {popup&&<InfoPopup props={popup.props} layer={popup.layer} onClose={()=>setPopup(null)}/>}
              {showRiesgo&&<RiesgoPanel riesgo={riesgo} loading={riesgoLoading} irc={riesgoConstruccionPunto} ircLoading={riesgoConstruccionLoading} lluvia={riesgoLluvia} lluviaLoading={riesgoLluviaLoading} onClose={()=>setShowRiesgo(false)}/>}

              {/* Badge ML entrenando */}
              {mlEntrenando&&(
                <div style={{position:'absolute',bottom:50,left:'50%',transform:'translateX(-50%)',zIndex:20,
                  background:'rgba(124,58,237,0.95)',backdropFilter:'blur(12px)',
                  border:'1px solid #a78bfa',borderRadius:10,padding:'7px 16px',
                  display:'flex',alignItems:'center',gap:8}}>
                  <div style={{width:10,height:10,borderRadius:'50%',border:'2px solid rgba(255,255,255,0.4)',borderTopColor:'white',animation:'spin 0.7s linear infinite'}}/>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:'white'}}>
                    Entrenando XGBoost + Optuna · puede tardar ~5 min
                  </span>
                </div>
              )}
              <div style={{position:'absolute',top:14,left:14,zIndex:10,background:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${C.border}`,borderRadius:12,padding:'7px 12px',boxShadow:'0 2px 8px rgba(0,0,0,0.06)'}}>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,textTransform:'uppercase',letterSpacing:'0.1em',marginBottom:2}}>{vista==='3d'?'Vista 3D':'Vista 2D'}</div>
                <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:13,fontWeight:700,color:C.text}}>Ica, Perú</div>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>14.07°S  75.73°O</div>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:'#dc2626',marginTop:2,fontWeight:700}}>Z4 · 0.45g · FEN×1.8 · IRC v9</div>
              </div>
              {alertasEWS.length>0&&alertasEWS[0].nivel_alerta!=='watch'&&(
                <div style={{position:'absolute',top:14,left:'50%',transform:'translateX(-50%)',zIndex:15,background:'rgba(220,38,38,0.95)',backdropFilter:'blur(12px)',border:'1px solid #fca5a5',borderRadius:10,padding:'6px 14px',display:'flex',alignItems:'center',gap:8}}>
                  <div style={{width:8,height:8,borderRadius:'50%',background:'white',animation:'pring 0.8s ease-out infinite'}}/>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:'white'}}>⚡ {alertasEWS[0].nivel_alerta.toUpperCase()} · M{alertasEWS[0].magnitud.toFixed(1)}{alertasEWS[0].dispara_tsunami?' · TSUNAMI':''}</span>
                </div>
              )}
              <div style={{position:'absolute',top:14,right:56,zIndex:10,display:'flex',flexDirection:'column',gap:5}}>
                {[{title:'Centrar Ica [R]',icon:<Icons.Locate/>,ev:'geo:center-ica'},{title:'Vista Perú',icon:<Icons.Globe/>,ev:'geo:center-peru'}].map(({title,icon,ev})=>(
                  <button key={ev} title={title} onClick={()=>window.dispatchEvent(new CustomEvent(ev))} style={{width:34,height:34,borderRadius:9,background:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${C.border}`,color:C.textSec,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 6px rgba(0,0,0,0.06)'}}>{icon}</button>
                ))}
                <button title="Riesgo completo" onClick={()=>{setShowRiesgo(true);setPopup(null);buscarRiesgo(-75.73,-14.07);buscarIRC(-75.73,-14.07);buscarRiesgoLluvia(-75.73,-14.07)}} style={{width:34,height:34,borderRadius:9,background:showRiesgo?`${C.primary}15`:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${showRiesgo?C.primary:C.border}`,color:showRiesgo?C.primary:C.textSec,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 6px rgba(0,0,0,0.06)'}}><Icons.Build/></button>
                <button title="Riesgo pluvial + FEN" onClick={()=>{setShowRiesgo(true);setPopup(null);buscarRiesgoLluvia(-75.73,-14.07)}} style={{width:34,height:34,borderRadius:9,background:riesgoLluvia?`${C.teal}15`:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${riesgoLluvia?C.teal:C.border}`,color:riesgoLluvia?C.teal:C.textSec,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 6px rgba(0,0,0,0.06)'}}><Icons.Rain/></button>
                <button title="Alertas EWS" onClick={()=>setCapas(p=>({...p,alertas_ews:!p.alertas_ews}))} style={{width:34,height:34,borderRadius:9,background:capas.alertas_ews?`${C.amber}15`:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${capas.alertas_ews?C.amber:C.border}`,color:capas.alertas_ews?C.amber:C.textSec,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 6px rgba(0,0,0,0.06)',position:'relative'}}>
                  <Icons.Bell/>
                  {alertasActivas>0&&<div style={{position:'absolute',top:-3,right:-3,width:12,height:12,borderRadius:'50%',background:C.danger,border:'2px solid white',display:'flex',alignItems:'center',justifyContent:'center'}}><span style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:'white',fontWeight:700}}>{alertasActivas}</span></div>}
                </button>
                <button title="Escenario 4DS M7.0" onClick={()=>calcularEscenario(-75.73,-14.07,7.0,30,5000)} style={{width:34,height:34,borderRadius:9,background:riesgoEscenario?`${C.orange}15`:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${riesgoEscenario?C.orange:C.border}`,color:riesgoEscenario?C.orange:C.textSec,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 6px rgba(0,0,0,0.06)'}}><span style={{fontSize:11,fontWeight:700}}>4DS</span></button>
                <button title="Sendai 2024" onClick={()=>cargarSendai(2024)} style={{width:34,height:34,borderRadius:9,background:sendaiReport?`${C.violet}15`:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${sendaiReport?C.violet:C.border}`,color:sendaiReport?C.violet:C.textSec,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center',boxShadow:'0 2px 6px rgba(0,0,0,0.06)'}}><span style={{fontSize:10,fontWeight:700}}>SDG</span></button>
              </div>
              {Object.entries(loading).some(([,v])=>v)&&!isInitial&&(
                <div style={{position:'absolute',top:alertasEWS.length>0&&alertasEWS[0]?.nivel_alerta!=='watch'?58:14,left:'50%',transform:'translateX(-50%)',zIndex:10,background:'rgba(255,255,255,0.93)',backdropFilter:'blur(12px)',border:`1px solid ${C.border}`,borderRadius:20,padding:'5px 14px',display:'flex',alignItems:'center',gap:8}}>
                  <div style={{width:10,height:10,borderRadius:'50%',border:`2px solid ${C.primary}30`,borderTopColor:C.primary,animation:'spin 0.6s linear infinite'}}/>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.primary}}>Actualizando</span>
                </div>
              )}
              {_ewsPing&&(
                <div style={{position:'absolute',bottom:28,right:14,zIndex:10,display:'flex',alignItems:'center',gap:5,padding:'3px 8px',background:'rgba(255,255,255,0.85)',backdropFilter:'blur(8px)',border:`1px solid ${C.border}`,borderRadius:99}}>
                  <div style={{width:5,height:5,borderRadius:'50%',background:C.primary}}/>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:7.5,color:C.textMuted}}>EWS live · {new Date(_ewsPing).toLocaleTimeString('es-PE')}</span>
                </div>
              )}
            </div>
            <div style={{flexShrink:0,height:chart?154:0,overflow:'hidden',transition:'height 0.26s cubic-bezier(0.4,0,0.2,1)',background:C.bg,borderTop:`1px solid ${C.border}`}}>
              <div style={{height:154,padding:'10px 18px'}}>
                <ErrorBoundary>
                  <StatsChart estadisticas={data.estadisticas} loading={loading.estadisticas} ircRanking={iRCRanking} eventosFen={eventosFen} escenario={riesgoEscenario} sendai={sendaiReport}/>
                </ErrorBoundary>
              </div>
            </div>
            <div style={{position:'absolute',bottom:0,left:0,right:0,height:22,background:C.bgSoft,borderTop:`1px solid ${C.border}`,display:'flex',alignItems:'center',padding:'0 12px',gap:10,zIndex:40,fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>
              <span>{Object.values(capas).filter(Boolean).length} capas</span>
              <span style={{width:1,height:10,background:C.border}}/>
              <span style={{color:C.primary}}>GPU · {filtros.mag_min.toFixed(1)}–{filtros.mag_max.toFixed(1)} Mw</span>
              {data.volcanes&&<><span style={{width:1,height:10,background:C.border}}/><span style={{color:'#dc2626'}}>🌋 {data.volcanes.features.length}</span></>}
              {alertasEWS.length>0&&<><span style={{width:1,height:10,background:C.border}}/><span style={{color:alertasActivas>0?C.danger:C.amber}}>⚡ {alertasEWS.length} EWS</span></>}
              {data.precipitaciones&&<><span style={{width:1,height:10,background:C.border}}/><span style={{color:C.teal}}>{data.precipitaciones.features.length} precip · {eventosFen.length} FEN</span></>}
              {totalInfra>0&&<><span style={{width:1,height:10,background:C.border}}/><span><span style={{color:C.primary}}>{totalOficial.toLocaleString()}</span> of · <span style={{color:'#6366f1'}}>{totalOSM.toLocaleString()}</span> osm</span></>}
              {totalErr>0&&<><span style={{width:1,height:10,background:C.border}}/><span style={{color:C.danger}}>{totalErr} error{totalErr>1?'es':''}</span></>}
              <span style={{marginLeft:'auto'}}>USGS · INGEMMET · GEM · INDECI · UNDRR · v9.0</span>
            </div>
          </div>
        </div>
        <style>{`
          @keyframes spin    { to { transform: rotate(360deg) } }
          @keyframes pring   { 0%{transform:scale(1);opacity:.7} 80%,100%{transform:scale(2);opacity:0} }
          @keyframes slideUp { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:translateY(0)} }
        `}</style>
      </>)}
    </div>
  )
}