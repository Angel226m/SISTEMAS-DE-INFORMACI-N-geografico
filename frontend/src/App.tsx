// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — App.tsx v9.0  ENTERPRISE
// 🆕 EWS badge: alertas activas en header + ping timestamp
// 🆕 Panel Volcanes: popup estado + radios peligro
// 🆕 Panel ML: score + IC 80% al click
// 🆕 Escenario 4DS + botón flotante
// 🆕 Sendai button + modo StatsChart
// 🆕 FilterPanel v9: volcanes, amenazaML, ewsNivel
// ✅ Todos los paneles v8 mantenidos
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
  RiesgoInfo, RiesgoConstruccionPunto, FuenteTipo,
  FiltrosPrecipitacion, RiesgoLluvia,
} from './types'
import type { MapStyle } from './components/MapView'

const C = {
  primary:'#059669', primaryBg:'#ecfdf5', primaryLt:'#10b981',
  secondary:'#0ea5e9', accent:'#6366f1',
  danger:'#dc2626', dangerBg:'#fef2f2',
  warning:'#f59e0b', warningBg:'#fffbeb',
  amber:'#f59e0b', orange:'#f97316', teal:'#0891b2', violet:'#7c3aed',
  bg:'#ffffff', bgSoft:'#f8fafc', bgMuted:'#f1f5f9', border:'#e2e8f0',
  text:'#0f172a', textSec:'#475569', textMuted:'#94a3b8',
}

const Icons = {
  Menu:   ()=><svg width="16" height="12" viewBox="0 0 16 12" fill="currentColor"><rect y="0" width="16" height="2" rx="1"/><rect y="5" width="11" height="2" rx="1"/><rect y="10" width="16" height="2" rx="1"/></svg>,
  Chart:  ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><rect x="1" y="7" width="2.5" height="6"/><rect x="5.75" y="4" width="2.5" height="9"/><rect x="10.5" y="1" width="2.5" height="12"/></svg>,
  Layers: ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><polygon points="7,1 13,4.5 7,8 1,4.5"/><polyline points="1,8.5 7,12 13,8.5"/></svg>,
  Filter: ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><path d="M1 2h12M3 7h8M5 12h4"/></svg>,
  Refresh:()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12.5 2.5A6 6 0 1 1 8.5 1.5"/><polyline points="8.5,1.5 12.5,1.5 12.5,5.5"/></svg>,
  X:      ()=><svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="1" y1="1" x2="9" y2="9"/><line x1="9" y1="1" x2="1" y2="9"/></svg>,
  Locate: ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><circle cx="7" cy="7" r="3"/><line x1="7" y1="1" x2="7" y2="4"/><line x1="7" y1="10" x2="7" y2="13"/><line x1="1" y1="7" x2="4" y2="7"/><line x1="10" y1="7" x2="13" y2="7"/></svg>,
  Globe:  ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><circle cx="7" cy="7" r="6"/><path d="M1 7h12M7 1c-2 2-3 4-3 6s1 4 3 6M7 1c2 2 3 4 3 6s-1 4-3 6"/></svg>,
  Build:  ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><rect x="1" y="4" width="12" height="9" rx="1"/><path d="M4 4V3a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v1"/></svg>,
  Rain:   ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><path d="M3 9a4 4 0 1 1 8 0"/><line x1="4" y1="11" x2="4" y2="13"/><line x1="7" y1="11" x2="7" y2="13"/><line x1="10" y1="11" x2="10" y2="13"/></svg>,
  Bell:   ()=><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><path d="M7 1.5A4.5 4.5 0 0 0 2.5 6v3l-1 2h11l-1-2V6A4.5 4.5 0 0 0 7 1.5z"/><line x1="7" y1="12.5" x2="7" y2="13.5"/></svg>,
}

const CAPAS_INIT: CapasActivas = {
  sismos:true,heatmap:false,departamentos:false,fallas:true,inundaciones:false,
  tsunamis:false,deslizamientos:false,riesgo_distritos:true,infraestructura:false,
  estaciones:false,riesgo_construccion:false,precipitaciones:false,
  volcanes:true,susceptibilidad:false,alertas_ews:true,extrusion_3d:false,
}
const FILTROS_INIT: FiltrosSismos    = {mag_min:3.0,mag_max:9.5,year_start:1960,year_end:2030}
const PRECIP_INIT:  FiltrosPrecipitacion = {riesgo_inund_min:1}
const V9_INIT:      FiltrosV9 = {mlScoreMin:0.5, amenazaML:'deslizamiento'}

interface Toast{id:number;type:'error'|'success'|'warn';msg:string}
const TOAST_C={error:{fg:C.danger,bg:C.dangerBg},success:{fg:C.primary,bg:C.primaryBg},warn:{fg:C.warning,bg:C.warningBg}}
const RISK_LABELS=['Muy bajo','Bajo','Moderado','Alto','Muy alto']
const RISK_COLORS=[C.primary,'#10b981',C.warning,'#f97316',C.danger]
const ZONA_COLORS:Record<number,string>={1:'#059669',2:'#f59e0b',3:'#f97316',4:'#dc2626'}

function ZonaBadge({zona,factor}:{zona?:number|null;factor?:number|null}){
  if(!zona) return null
  const color=ZONA_COLORS[zona]??C.textMuted
  return(
    <div style={{display:'inline-flex',alignItems:'center',gap:5,padding:'3px 9px',
      background:color+'12',border:`1px solid ${color}30`,borderRadius:99,marginBottom:6}}>
      <div style={{width:6,height:6,borderRadius:'50%',background:color}}/>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color}}>Z{zona}{factor?` · ${factor}g`:''} · NTE E.030</span>
    </div>
  )
}
function FENBadge({indice,desc}:{indice:number;desc?:string}){
  const color=indice>=3.5?C.danger:indice>=2.0?C.orange:indice>=1.3?C.warning:indice<0.9?C.primary:C.textMuted
  return(
    <div style={{display:'inline-flex',alignItems:'center',gap:5,padding:'3px 9px',
      background:color+'10',border:`1px solid ${color}25`,borderRadius:99,marginBottom:6}}>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color}}>FEN ×{indice.toFixed(1)}{desc?` · ${desc}`:''}</span>
    </div>
  )
}
function ToastList({toasts,remove}:{toasts:Toast[];remove:(id:number)=>void}){
  if(!toasts.length) return null
  return(
    <div style={{position:'fixed',bottom:52,right:14,zIndex:300,display:'flex',flexDirection:'column',gap:7}}>
      {toasts.map(t=>(
        <div key={t.id} style={{display:'flex',alignItems:'flex-start',gap:10,padding:'9px 12px',
          background:TOAST_C[t.type].bg,border:`1px solid ${TOAST_C[t.type].fg}30`,
          borderLeft:`3px solid ${TOAST_C[t.type].fg}`,
          borderRadius:10,boxShadow:'0 4px 14px rgba(0,0,0,0.07)',maxWidth:300}}>
          <span style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.text,flex:1,lineHeight:1.4}}>{t.msg}</span>
          <button onClick={()=>remove(t.id)} style={{background:'none',border:'none',cursor:'pointer',color:C.textMuted,padding:0}}><Icons.X/></button>
        </div>
      ))}
    </div>
  )
}
function Loader({pct}:{pct:number}){
  return(
    <div style={{position:'fixed',inset:0,zIndex:999,background:C.bg,display:'flex',alignItems:'center',justifyContent:'center',flexDirection:'column',gap:24}}>
      <div style={{position:'relative',width:72,height:72}}>
        {[0,1,2].map(i=>(
          <div key={i} style={{position:'absolute',inset:i*10,borderRadius:'50%',border:'2px solid',
            borderColor:[C.primary+'80',C.primaryLt+'50',C.secondary+'35'][i],
            animation:`pring ${1.5+i*0.5}s ease-out infinite`,animationDelay:`${i*0.2}s`}}/>
        ))}
        <div style={{position:'absolute',inset:26,borderRadius:'50%',background:`linear-gradient(135deg,${C.primary},${C.secondary})`}}/>
      </div>
      <div style={{textAlign:'center'}}>
        <p style={{fontFamily:"'DM Sans',sans-serif",color:C.text,fontSize:22,fontWeight:800,letterSpacing:'-0.02em',margin:0}}>
          GeoRiesgo <span style={{color:C.primary}}>Perú</span>
        </p>
        <p style={{fontFamily:"'DM Mono',monospace",color:C.textMuted,fontSize:10,letterSpacing:'0.1em',textTransform:'uppercase',marginTop:4}}>
          v9.0 · Volcanes · ML · EWS · IRC v9 · Sendai
        </p>
      </div>
      <div style={{width:220,height:3,background:C.bgMuted,borderRadius:2}}>
        <div style={{height:'100%',width:`${pct}%`,background:`linear-gradient(90deg,${C.primary},${C.secondary})`,borderRadius:2,transition:'width 0.5s ease'}}/>
      </div>
    </div>
  )
}
function HoverTooltip({info}:{info:TooltipInfo}){
  if(!info.object?.properties) return null
  const p=info.object.properties as Record<string,unknown>
  const mag=Number(p.magnitud??0)
  const mc=mag>=7?C.danger:mag>=5?C.warning:C.primary
  const isSismo='magnitud' in p
  const isPrecip='indice_fen' in p&&'precipitacion_anual_mm' in p
  const isVolcan='estado' in p&&'altitud_m' in p
  const isEWS='nivel_alerta' in p&&'dispara_tsunami' in p
  const isML='score' in p&&'nivel' in p
  return(
    <div style={{position:'absolute',left:Math.min(info.x+12,window.innerWidth-260),top:Math.max(info.y-60,10),
      zIndex:60,pointerEvents:'none',background:'rgba(255,255,255,0.96)',backdropFilter:'blur(8px)',
      border:`1px solid ${C.border}`,borderRadius:10,padding:'8px 12px',
      boxShadow:'0 4px 20px rgba(0,0,0,0.1)',minWidth:160,maxWidth:260}}>
      {isSismo&&!isEWS?(
        <>
          <div style={{display:'flex',alignItems:'baseline',gap:5,marginBottom:4}}>
            <span style={{fontFamily:"'DM Mono',monospace",fontSize:22,fontWeight:700,color:mc,lineHeight:1}}>{mag.toFixed(1)}</span>
            <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted}}>Mw</span>
          </div>
          <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:11,color:C.textSec,lineHeight:1.4}}>{String(p.fecha??'')} · {String(p.tipo_profundidad??'')}</div>
        </>
      ):isVolcan?(
        <>
          <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.text,fontWeight:700,marginBottom:4}}>{String(p.nombre??'')}</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:'#dc2626'}}>🌋 {String(p.estado??'').replace('_',' ')}</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>{Number(p.altitud_m??0).toLocaleString()} m.s.n.m.</div>
        </>
      ):isEWS?(
        <>
          <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,fontWeight:700,color:C.danger,marginBottom:4}}>⚡ EWS {String(p.nivel_alerta??'').toUpperCase()}</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:C.text}}>M{Number(p.magnitud??0).toFixed(1)}</div>
          {p.dispara_tsunami&&<div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#06b6d4',marginTop:2}}>⚠ cascada tsunami</div>}
        </>
      ):isML?(
        <>
          <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.text,fontWeight:700,marginBottom:4}}>ML Score</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:14,color:C.violet,fontWeight:700}}>{(Number(p.score??0)*100).toFixed(1)}%</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.textMuted}}>IC 80%: {(Number(p.score_p10??0)*100).toFixed(0)}–{(Number(p.score_p90??0)*100).toFixed(0)}%</div>
        </>
      ):isPrecip?(
        <>
          <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.text,fontWeight:700,marginBottom:4}}>{String(p.nombre??'')}</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.teal}}>FEN ×{Number(p.indice_fen??1).toFixed(1)}</div>
          <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>{Number(p.precipitacion_anual_mm??0).toFixed(0)} mm/año</div>
        </>
      ):(
        <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.text,fontWeight:600}}>{String(p.nombre??p.tipo??'Feature')}</div>
      )}
    </div>
  )
}
function Row({label,value,color}:{label:string;value:string|number;color?:string}){
  return(
    <div style={{display:'flex',justifyContent:'space-between',gap:8,marginBottom:5}}>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>{label}</span>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:600,color:color??C.text,textAlign:'right'}}>{String(value)}</span>
    </div>
  )
}
function InfoPopup({props:p,layer:_l,onClose}:{props:Record<string,unknown>;layer:string;onClose:()=>void}){
  const isSismo='magnitud' in p
  const isVolcan='estado' in p&&'altitud_m' in p&&!isSismo
  const isEWS='nivel_alerta' in p&&'dispara_tsunami' in p
  const isML='score' in p&&'nivel' in p&&'score_p10' in p
  const isFalla='activa' in p&&'longitud_km' in p
  const isDept='capital' in p&&'area_km2' in p&&!isSismo
  const isPrecip='indice_fen' in p&&'precipitacion_anual_mm' in p
  const isIRC='indice_riesgo_construccion' in p&&!isSismo
  const isDistrito='nivel_riesgo' in p&&'provincia' in p&&!isFalla&&!isSismo&&!isPrecip&&!isIRC
  const isInfra='criticidad' in p&&!isSismo&&!isPrecip&&!isVolcan&&!isEWS&&!isML
  const isTsunami='altura_ola_m' in p
  const isDesliz='activo' in p&&'tipo' in p&&!isFalla
  const isEstacion='codigo' in p
  const accent=isSismo?C.danger:isVolcan?'#dc2626':isEWS?C.orange:isML?C.violet:isFalla?C.warning:isPrecip?C.teal:isDept?'#7c3aed':isIRC?C.amber:C.textMuted
  const kind=isSismo?'Sismo':isVolcan?'Volcán':isEWS?'Alerta EWS':isML?'ML Score':isFalla?'Falla Geológica':isPrecip?'Zona Climática':isDept?'Departamento':isIRC?'IRC Distrito':isDistrito?'Distrito':isInfra?'Infraestructura':isTsunami?'Tsunami':isDesliz?'Deslizamiento':isEstacion?'Estación':'Elemento'
  return(
    <div style={{position:'absolute',bottom:48,left:14,zIndex:50,width:308,animation:'slideUp 0.2s ease-out forwards'}}>
      <div style={{background:C.bg,border:`1px solid ${C.border}`,borderTop:`3px solid ${accent}`,borderRadius:14,overflow:'hidden',boxShadow:'0 8px 32px rgba(0,0,0,0.1)'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'9px 12px',background:C.bgSoft,borderBottom:`1px solid ${C.border}`}}>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:accent,letterSpacing:'0.08em'}}>{kind}</span>
          <button onClick={onClose} style={{width:22,height:22,borderRadius:6,background:C.bgMuted,border:`1px solid ${C.border}`,color:C.textMuted,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center'}}><Icons.X/></button>
        </div>
        <div style={{padding:'12px 14px'}}>
          {isSismo&&!isEWS&&(()=>{
            const mag=Number(p.magnitud??0),prof=Number(p.profundidad_km??0)
            const mc=mag>=7?C.danger:mag>=6?'#f97316':mag>=5?C.warning:C.primary
            return(<>
              <div style={{display:'flex',alignItems:'baseline',gap:6,marginBottom:12,paddingBottom:10,borderBottom:`1px solid ${C.border}`}}>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:40,fontWeight:800,color:mc,lineHeight:1}}>{mag.toFixed(1)}</span>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:12,color:C.textMuted}}>Mw</span>
              </div>
              <Row label="Fecha" value={String(p.fecha??'')}/>
              <Row label="Profundidad" value={`${prof} km`} color={prof<30?C.danger:prof<70?'#f97316':'#0ea5e9'}/>
              {p.region&&<Row label="Región" value={String(p.region)}/>}
              <div style={{marginTop:7,padding:'7px 10px',background:C.bgSoft,borderRadius:8,fontFamily:"'DM Sans',sans-serif",fontSize:11,color:C.textSec,lineHeight:1.4}}>{String(p.lugar??'')}</div>
            </>)
          })()}
          {isVolcan&&(()=>{
            const estado=String(p.estado??'')
            const sc={'activo_critico':'#dc2626','activo':'#f97316','potencialmente_activo':'#f59e0b','inactivo':'#9ca3af'}[estado]??'#9ca3af'
            return(<>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre??'')}</p>
              <div style={{display:'inline-flex',alignItems:'center',gap:6,padding:'4px 10px',background:sc+'12',border:`1px solid ${sc}30`,borderRadius:99,marginBottom:10}}>
                <div style={{width:6,height:6,borderRadius:'50%',background:sc}}/>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:sc,fontWeight:700}}>{estado.replace(/_/g,' ')}</span>
              </div>
              <Row label="Altitud" value={`${Number(p.altitud_m??0).toLocaleString('es-PE')} m.s.n.m.`}/>
              {p.tipo_erupcion&&<Row label="Tipo erupción" value={String(p.tipo_erupcion)}/>}
              {p.ultima_erupcion&&<Row label="Última erupción" value={String(p.ultima_erupcion)}/>}
              {p.region&&<Row label="Región" value={String(p.region)}/>}
              {typeof p.radio_peligro_km==='object'&&p.radio_peligro_km&&Object.keys(p.radio_peligro_km as object).length>0&&(
                <div style={{marginTop:8,padding:'6px 8px',background:'#fef2f2',border:'1px solid #fecaca',borderRadius:7}}>
                  <div style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:'#991b1b',marginBottom:3}}>Radios de peligro (km)</div>
                  {Object.entries(p.radio_peligro_km as Record<string,number>).map(([nivel,km])=>(
                    <Row key={nivel} label={nivel} value={`${km} km`} color='#dc2626'/>
                  ))}
                </div>
              )}
            </>)
          })()}
          {isEWS&&(()=>{
            const nivel=String(p.nivel_alerta??'')
            const nc={'emergency':C.danger,'warning':C.orange,'watch':C.amber}[nivel]??C.textMuted
            return(<>
              <div style={{display:'inline-flex',alignItems:'center',gap:6,padding:'4px 10px',background:nc+'12',border:`1px solid ${nc}30`,borderRadius:99,marginBottom:10}}>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:12,color:nc,fontWeight:800}}>⚡ {nivel.toUpperCase()}</span>
              </div>
              <Row label="Magnitud" value={`M${Number(p.magnitud??0).toFixed(1)} Mw`} color={nc}/>
              {p.lugar&&<Row label="Lugar" value={String(p.lugar).substring(0,40)}/>}
              {p.created_at&&<Row label="Detectado" value={new Date(String(p.created_at)).toLocaleTimeString('es-PE')}/>}
              {p.dispara_tsunami&&<div style={{marginTop:6,padding:'5px 8px',background:'#ecfeff',border:'1px solid #a5f3fc',borderRadius:6,fontFamily:"'DM Mono',monospace",fontSize:8,color:'#0e7490'}}>⚠ Posible cascada tsunami (Gill & Malamud 2014)</div>}
              {p.dispara_deslizamiento&&<div style={{marginTop:4,padding:'5px 8px',background:'#fef3c7',border:'1px solid #fde68a',borderRadius:6,fontFamily:"'DM Mono',monospace",fontSize:8,color:'#92400e'}}>⚠ Posible cascada deslizamiento</div>}
              <div style={{marginTop:8,padding:'5px 8px',background:C.bgSoft,border:`1px solid ${C.border}`,borderRadius:6,fontFamily:"'DM Mono',monospace",fontSize:7.5,color:C.textMuted}}>CAP v1.2 · INDECI 2020 · EW4All UNDRR 2022</div>
            </>)
          })()}
          {isML&&(()=>{
            const score=Number(p.score??0)
            const nivel=String(p.nivel??'')
            const nc={'MUY_ALTO':C.danger,'ALTO':C.orange,'MEDIO':C.warning,'BAJO':'#10b981','MUY_BAJO':C.primary}[nivel]??C.textMuted
            return(<>
              <div style={{textAlign:'center',marginBottom:10}}>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:32,fontWeight:800,color:nc,lineHeight:1}}>{(score*100).toFixed(1)}%</div>
                <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:11,color:nc,marginTop:2}}>{nivel.replace('_',' ')}</div>
              </div>
              <Row label="IC 80% inf." value={`${(Number(p.score_p10??0)*100).toFixed(0)}%`}/>
              <Row label="IC 80% sup." value={`${(Number(p.score_p90??0)*100).toFixed(0)}%`}/>
              <div style={{marginTop:8,padding:'6px 8px',background:'#faf5ff',border:'1px solid #e9d5ff',borderRadius:6,fontFamily:"'DM Mono',monospace",fontSize:7.5,color:C.violet}}>Kumar et al. 2023 · bootstrapping 100 iter.</div>
            </>)
          })()}
          {isPrecip&&(()=>{
            const fen=Number(p.indice_fen??1)
            return(<>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre??'')}</p>
              <FENBadge indice={fen} desc={String(p.descripcion_fen??'').substring(0,24)}/>
              <Row label="Precipitación anual" value={`${Number(p.precipitacion_anual_mm??0).toFixed(0)} mm/año`}/>
              <Row label="Tipo" value={String(p.tipo??'')}/>
              {p.nivel_riesgo_inundacion&&<Row label="Riesgo inund." value={`${p.nivel_riesgo_inundacion}/5`} color={Number(p.nivel_riesgo_inundacion)>=4?C.danger:C.warning}/>}
            </>)
          })()}
          {isDept&&!isPrecip&&(()=>{
            const nivel=Math.max(1,Math.min(5,Number(p.nivel_riesgo??1)))
            return(<>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:16,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre??'')}</p>
              <ZonaBadge zona={p.zona_sismica as number} factor={p.factor_z as number}/>
              {p.capital&&<Row label="Capital" value={String(p.capital)}/>}
              <div style={{display:'flex',gap:3,marginTop:6}}>
                {RISK_LABELS.map((_,i)=><div key={i} style={{flex:1,height:5,borderRadius:3,background:i<nivel?RISK_COLORS[i]:C.bgMuted}}/>)}
              </div>
            </>)
          })()}
          {(isDistrito||isIRC)&&!isDept&&!isPrecip&&(()=>{
            const nivel=Math.max(1,Math.min(5,Number(p.nivel_riesgo??1)))
            const irc=p.indice_riesgo_construccion?Number(p.indice_riesgo_construccion):null
            const ircV9=p.indice_riesgo_v9?Number(p.indice_riesgo_v9):null
            const factorCascada=p.factor_cascada?Number(p.factor_cascada as unknown as number):null
            return(<>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:16,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre??'')}</p>
              <ZonaBadge zona={p.zona_sismica as number} factor={p.factor_z as number}/>
              {p.provincia&&<Row label="Provincia" value={String(p.provincia)}/>}
              {p.departamento&&<Row label="Departamento" value={String(p.departamento)}/>}
              {irc!==null&&(
                <div style={{marginTop:8,padding:'8px 10px',background:'#fffbeb',border:'1px solid #fde68a',borderRadius:8}}>
                  <div style={{display:'flex',justifyContent:'space-between',marginBottom:ircV9?4:0}}>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#92400e'}}>IRC v8</span>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:15,fontWeight:800,color:RISK_COLORS[Math.max(0,Math.min(4,Math.round(irc)-1))]}}>{irc.toFixed(2)}</span>
                  </div>
                  {ircV9&&(
                    <div style={{display:'flex',justifyContent:'space-between'}}>
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#dc2626'}}>IRC v9</span>
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:15,fontWeight:800,color:C.danger}}>{ircV9.toFixed(2)}</span>
                    </div>
                  )}
                  {factorCascada&&factorCascada>1.01 && (
                    <div style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.teal,marginTop:4}}>
                      Cascada ×{factorCascada.toFixed(3)}
                    </div>
                  )}
                </div>
              )}
              <div style={{display:'flex',gap:3,marginTop:8}}>
                {RISK_LABELS.map((_,i)=><div key={i} style={{flex:1,height:5,borderRadius:3,background:i<nivel?RISK_COLORS[i]:C.bgMuted}}/>)}
              </div>
            </>)
          })()}
          {isFalla&&(()=>{
            const activa=Boolean(p.activa)
            return(<>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre??'')}</p>
              <div style={{display:'inline-flex',alignItems:'center',gap:6,padding:'4px 10px',background:activa?C.dangerBg:C.bgMuted,borderRadius:99,marginBottom:8}}>
                <div style={{width:6,height:6,borderRadius:'50%',background:activa?C.danger:C.textMuted}}/>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:activa?C.danger:C.textMuted}}>{activa?'Falla Activa':'Falla Inactiva'}</span>
              </div>
              {p.tipo&&<Row label="Tipo" value={String(p.tipo)}/>}
              {p.longitud_km&&<Row label="Longitud" value={`${Number(p.longitud_km).toFixed(1)} km`}/>}
              {p.magnitud_max&&<Row label="Mag. máx." value={`${p.magnitud_max} Mw`} color={C.danger}/>}
            </>)
          })()}
          {isInfra&&!isTsunami&&!isEstacion&&!isDesliz&&(
            <>
              {p.nombre&&<p style={{fontFamily:"'DM Sans',sans-serif",fontSize:14,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre)}</p>}
              <ZonaBadge zona={p.zona_sismica as number}/>
              {p.tipo&&<Row label="Tipo" value={String(p.tipo).replace(/_/g,' ')}/>}
              {p.criticidad&&<Row label="Criticidad" value={`${p.criticidad}/5`} color={Number(p.criticidad)>=4?C.danger:C.warning}/>}
            </>
          )}
        </div>
      </div>
    </div>
  )
}
function RiesgoPanel({riesgo,loading,irc,ircLoading,lluvia,lluviaLoading,onClose}:{
  riesgo:RiesgoInfo|null;loading:boolean;irc:RiesgoConstruccionPunto|null;ircLoading:boolean;
  lluvia:RiesgoLluvia|null;lluviaLoading:boolean;onClose:()=>void
}){
  if(!loading&&!riesgo&&!ircLoading&&!irc&&!lluviaLoading&&!lluvia) return null
  const nivel=riesgo?Math.max(1,Math.min(5,riesgo.nivel_riesgo)):0
  return(
    <div style={{position:'absolute',bottom:48,right:14,zIndex:50,width:264,animation:'slideUp 0.2s ease-out forwards'}}>
      <div style={{background:C.bg,border:`1px solid ${C.border}`,borderTop:`3px solid ${C.primary}`,borderRadius:14,overflow:'hidden',boxShadow:'0 8px 32px rgba(0,0,0,0.1)'}}>
        <div style={{display:'flex',alignItems:'center',justifyContent:'space-between',padding:'9px 12px',background:C.bgSoft,borderBottom:`1px solid ${C.border}`}}>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color:C.primary}}>Riesgo del punto</span>
          <button onClick={onClose} style={{width:22,height:22,borderRadius:6,background:C.bgMuted,border:`1px solid ${C.border}`,color:C.textMuted,cursor:'pointer',display:'flex',alignItems:'center',justifyContent:'center'}}><Icons.X/></button>
        </div>
        <div style={{padding:'12px 14px'}}>
          {loading
            ?<div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted,textAlign:'center',padding:'8px 0'}}>Calculando...</div>
            :riesgo&&(<>
                <div style={{textAlign:'center',marginBottom:8}}>
                  <div style={{fontFamily:"'DM Mono',monospace",fontSize:32,fontWeight:800,color:RISK_COLORS[nivel-1],lineHeight:1}}>{riesgo.nivel_riesgo.toFixed(1)}</div>
                  <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:11,color:RISK_COLORS[nivel-1],marginTop:2}}>{RISK_LABELS[nivel-1]}</div>
                </div>
                <div style={{display:'flex',gap:3,marginBottom:10}}>
                  {RISK_LABELS.map((_,i)=><div key={i} style={{flex:1,height:5,borderRadius:3,background:i<nivel?RISK_COLORS[i]:C.bgMuted}}/>)}
                </div>
                {riesgo.region&&<Row label="Región" value={riesgo.region}/>}
                {riesgo.distrito&&<Row label="Distrito" value={riesgo.distrito}/>}
                <Row label="Sismos 5km" value={riesgo.sismos_cercanos_5km}/>
                {riesgo.falla_mas_cercana&&<Row label="Falla" value={`${riesgo.falla_mas_cercana} (${riesgo.dist_falla_km?.toFixed(1)}km)`} color={C.warning}/>}
              </>)
          }
          {(lluviaLoading||lluvia)&&(
            <div style={{marginTop:10,paddingTop:10,borderTop:`1px solid ${C.border}`}}>
              <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.teal,textTransform:'uppercase',letterSpacing:'0.1em',marginBottom:6}}>Riesgo Pluvial</div>
              {lluviaLoading
                ?<div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted}}>Calculando...</div>
                :lluvia&&(()=>{
                    const n2=Math.max(1,Math.min(5,['MUY BAJO','BAJO','MEDIO','ALTO','MUY ALTO'].indexOf(lluvia.nivel_riesgo)+1))
                    return(<>
                      {lluvia.zona_climatica&&<FENBadge indice={lluvia.zona_climatica.indice_fen}/>}
                      <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:4}}>
                        <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>Índice</span>
                        <span style={{fontFamily:"'DM Mono',monospace",fontSize:20,fontWeight:800,color:RISK_COLORS[Math.max(0,n2-1)]}}>{lluvia.indice_pluvial.toFixed(2)}</span>
                      </div>
                    </>)
                  })()
              }
            </div>
          )}
          {(ircLoading||irc)&&(
            <div style={{marginTop:10,paddingTop:10,borderTop:`1px solid ${C.border}`}}>
              <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,textTransform:'uppercase',letterSpacing:'0.1em',marginBottom:6}}>IRC</div>
              {ircLoading
                ?<div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted}}>Calculando...</div>
                :irc&&(<>
                    <ZonaBadge zona={irc.zona_sismica} factor={irc.factor_z}/>
                    <div style={{display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>Índice</span>
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:20,fontWeight:800,color:RISK_COLORS[Math.max(0,Math.min(4,Math.round(irc.indice)-1))]}}>{irc.indice.toFixed(2)}</span>
                    </div>
                  </>)
              }
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
function Btn({active,onClick,title,children}:{active:boolean;onClick:()=>void;title?:string;children:React.ReactNode}){
  return(
    <button title={title} onClick={onClick} style={{width:32,height:32,borderRadius:8,cursor:'pointer',
      background:active?`${C.primary}15`:C.bgMuted,border:`1px solid ${active?`${C.primary}40`:C.border}`,
      color:active?C.primary:C.textMuted,display:'flex',alignItems:'center',justifyContent:'center',transition:'all 0.16s ease'}}>
      {children}
    </button>
  )
}

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
    riesgoLluvia,riesgoLluviaLoading,alertasEWS,ewsPingTs,
    riesgoEscenario,sendaiReport,mlEntrenando,
    recargarSismos,buscarRiesgo,buscarIRC,buscarRiesgoLluvia,
    calcularEscenario,cargarSendai,cargarSusceptibilidad,
    recargarTodo,cargarIRCMapa,cargarPrecipitaciones,
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
        <header style={{flexShrink:0,height:52,display:'flex',alignItems:'center',justifyContent:'space-between',padding:'0 14px',background:C.bg,borderBottom:`1px solid ${C.border}`,zIndex:20}}>
          <div style={{display:'flex',alignItems:'center',gap:10}}>
            <Btn active={sidebar} onClick={()=>setSidebar(p=>!p)} title="Sidebar [L]"><Icons.Menu/></Btn>
            <div style={{display:'flex',alignItems:'center',gap:9}}>
              <div style={{width:32,height:32,borderRadius:9,background:`linear-gradient(135deg,${C.primary},${C.secondary})`,display:'flex',alignItems:'center',justifyContent:'center',fontSize:16,color:'white',fontWeight:800}}>G</div>
              <div>
                <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:800,color:C.text,letterSpacing:'-0.02em',lineHeight:1.1}}>GeoRiesgo <span style={{color:C.primary}}>Perú</span></div>
                <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,letterSpacing:'0.1em',textTransform:'uppercase'}}>Multi-hazard · ML · EWS · v9.0</div>
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
          <aside style={{flexShrink:0,width:sidebarW,overflow:'hidden',transition:'width 0.26s cubic-bezier(0.4,0,0.2,1)',background:C.bg,borderRight:`1px solid ${C.border}`,display:'flex',flexDirection:'column',zIndex:10}}>
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
              {ewsPingTs&&(
                <div style={{position:'absolute',bottom:28,right:14,zIndex:10,display:'flex',alignItems:'center',gap:5,padding:'3px 8px',background:'rgba(255,255,255,0.85)',backdropFilter:'blur(8px)',border:`1px solid ${C.border}`,borderRadius:99}}>
                  <div style={{width:5,height:5,borderRadius:'50%',background:C.primary}}/>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:7.5,color:C.textMuted}}>EWS live · {new Date(ewsPingTs).toLocaleTimeString('es-PE')}</span>
                </div>
              )}
            </div>
            <div style={{flexShrink:0,height:chart?154:0,overflow:'hidden',transition:'height 0.26s cubic-bezier(0.4,0,0.2,1)',background:C.bg,borderTop:`1px solid ${C.border}`}}>
              <div style={{height:154,padding:'10px 18px'}}>
                <StatsChart estadisticas={data.estadisticas} loading={loading.estadisticas} ircRanking={iRCRanking} eventosFen={eventosFen} escenario={riesgoEscenario} sendai={sendaiReport}/>
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