import { C, RISK_LABELS, RISK_COLORS } from './ui/constants'
import { Icons } from './ui/Icons'
import { Row } from './ui/Row'
import { ZonaBadge, FENBadge } from './ui/Badges'
import type { RiesgoInfo, RiesgoConstruccionPunto, RiesgoLluvia } from '../types'

export function RiesgoPanel({riesgo,loading,irc,ircLoading,lluvia,lluviaLoading,onClose}:{
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
