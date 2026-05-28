import { C, RISK_LABELS, RISK_COLORS } from './ui/constants'
import { Icons } from './ui/Icons'
import { Row } from './ui/Row'
import { ZonaBadge, FENBadge } from './ui/Badges'

export function InfoPopup({props:p,layer:_l,onClose}:{props:Record<string,unknown>;layer:string;onClose:()=>void}){
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
            const mmi=p.mmi_epicentro ? Number(p.mmi_epicentro) : null
            const energia=p.energia_j ? Number(p.energia_j) : null
            const radio=p.radio_sentido_km ? Number(p.radio_sentido_km) : null
            const intensidad=p.intensidad_desc ? String(p.intensidad_desc) : null
            const mmiColor = mmi ? (mmi >= 8 ? C.danger : mmi >= 6 ? C.orange : mmi >= 4 ? C.warning : C.primary) : C.textMuted
            return(<>
              <div style={{display:'flex',alignItems:'baseline',gap:6,marginBottom:12,paddingBottom:10,borderBottom:`1px solid ${C.border}`}}>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:40,fontWeight:800,color:mc,lineHeight:1}}>{mag.toFixed(1)}</span>
                <span style={{fontFamily:"'DM Mono',monospace",fontSize:12,color:C.textMuted}}>Mw</span>
                {intensidad&&<span style={{fontFamily:"'DM Sans',sans-serif",fontSize:11,color:mc,marginLeft:'auto',fontWeight:600}}>{intensidad}</span>}
              </div>
              <Row label="Fecha" value={String(p.fecha??'')}/>
              <Row label="Profundidad" value={`${prof} km`} color={prof<30?C.danger:prof<70?'#f97316':'#0ea5e9'}/>
              {p.tipo_profundidad&&<Row label="Tipo" value={String(p.tipo_profundidad)}/>}
              {p.region&&<Row label="Región" value={String(p.region)}/>}
              {(mmi!==null||energia!==null||radio!==null)&&(
                <div style={{marginTop:8,padding:'7px 10px',background:C.bgSoft,border:`1px solid ${C.border}`,borderRadius:8}}>
                  {mmi!==null&&<div style={{display:'flex',justifyContent:'space-between',marginBottom:3}}>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.textMuted}}>MMI epicentro</span>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:11,fontWeight:800,color:mmiColor}}>{mmi.toFixed(1)}/12</span>
                  </div>}
                  {energia!==null&&<div style={{display:'flex',justifyContent:'space-between',marginBottom:3}}>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.textMuted}}>Energía</span>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.text}}>{energia.toExponential(2)} J</span>
                  </div>}
                  {radio!==null&&<div style={{display:'flex',justifyContent:'space-between'}}>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.textMuted}}>Radio percepción</span>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.text}}>~{radio.toLocaleString('es-PE')} km</span>
                  </div>}
                  <div style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted,marginTop:4}}>Wald et al. 1999 · Toppozada 1975 · G-R</div>
                </div>
              )}
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
            const suelo=p.clasificacion_suelo ? String(p.clasificacion_suelo) : null
            const factorS=p.factor_suelo_s ? Number(p.factor_suelo_s) : null
            const mmi=p.mmi_estimada ? Number(p.mmi_estimada) : null
            const mmiColor = mmi ? (mmi >= 8 ? C.danger : mmi >= 6 ? C.orange : mmi >= 4 ? C.warning : C.primary) : C.textMuted
            const SUELO_LABELS: Record<string,string> = {
              S0:'Roca dura (VS30>1500 m/s)',S1:'Roca blanda (760-1500 m/s)',
              S2:'Suelo rígido (360-760 m/s)',S3:'Suelo blando (180-360 m/s)',
              S4:'Suelo muy blando (<180 m/s)',
            }
            return(<>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:16,fontWeight:700,color:C.text,marginBottom:8}}>{String(p.nombre??'')}</p>
              <ZonaBadge zona={p.zona_sismica as number} factor={p.factor_z as number}/>
              {p.provincia&&<Row label="Provincia" value={String(p.provincia)}/>}
              {p.departamento&&<Row label="Departamento" value={String(p.departamento)}/>}
              {(irc!==null||ircV9!==null)&&(
                <div style={{marginTop:8,padding:'8px 10px',background:'#fffbeb',border:'1px solid #fde68a',borderRadius:8}}>
                  <div style={{display:'flex',justifyContent:'space-between',marginBottom:ircV9?4:0}}>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#92400e'}}>IRC v8</span>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:15,fontWeight:800,color:RISK_COLORS[Math.max(0,Math.min(4,Math.round((irc??1))-1))]}}>{(irc??0).toFixed(2)}</span>
                  </div>
                  {ircV9&&(
                    <div style={{display:'flex',justifyContent:'space-between'}}>
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#dc2626'}}>IRC v9</span>
                      <span style={{fontFamily:"'DM Mono',monospace",fontSize:15,fontWeight:800,color:C.danger}}>{ircV9.toFixed(2)}</span>
                    </div>
                  )}
                  {factorCascada&&factorCascada>1.01 && (
                    <div style={{fontFamily:"'DM Mono',monospace",fontSize:8,color:C.teal,marginTop:4}}>
                      Cascada ×{factorCascada.toFixed(3)} · Gill &amp; Malamud 2014
                    </div>
                  )}
                </div>
              )}
              {suelo&&(
                <div style={{marginTop:6,padding:'7px 10px',background:'#f0fdf4',border:'1px solid #bbf7d0',borderRadius:8}}>
                  <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:3}}>
                    <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#15803d',fontWeight:700}}>Suelo {suelo} — NTE E.031-2020</span>
                    {factorS&&<span style={{fontFamily:"'DM Mono',monospace",fontSize:11,fontWeight:800,color:'#15803d'}}>Fs={factorS.toFixed(2)}</span>}
                  </div>
                  <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:10,color:'#166534',lineHeight:1.3}}>{SUELO_LABELS[suelo]??''}</div>
                </div>
              )}
              {mmi!==null&&mmi>1&&(
                <div style={{marginTop:6,padding:'6px 10px',background:'#fff7ed',border:`1px solid ${C.orange}30`,borderRadius:8,display:'flex',justifyContent:'space-between',alignItems:'center'}}>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#7c3aed'}}>MMI estimada (Wald 1999)</span>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:13,fontWeight:800,color:mmiColor}}>{mmi.toFixed(1)}</span>
                </div>
              )}
              <div style={{display:'flex',gap:3,marginTop:8}}>
                {RISK_LABELS.map((_,i)=><div key={i} style={{flex:1,height:5,borderRadius:3,background:i<Math.max(1,Math.min(5,Number(p.nivel_riesgo??1)))?RISK_COLORS[i]:C.bgMuted}}/>)}
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
