import { C } from './ui/constants'
import type { TooltipInfo } from '../types'

export function HoverTooltip({info}:{info:TooltipInfo}){
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
