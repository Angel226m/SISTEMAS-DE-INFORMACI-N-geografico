// ══════════════════════════════════════════════════════════
// StatsChart.tsx v9.0
// 🆕 Modo 'irc_v9': barras horizontales IRC v9 con IC 80%
// 🆕 Modo 'escenario': 4 estados DS apilados por tipo construcción
// 🆕 Modo 'sendai': radar proxy 7 targets Sendai
// ✅ Modos cantidad, magnitud, profundidad, irc, fen mantenidos
// ══════════════════════════════════════════════════════════

import { useState, memo } from 'react'
import {
  BarChart, Bar, AreaChart, Area, XAxis, YAxis,
  Tooltip, ResponsiveContainer, Cell, ReferenceLine,
  CartesianGrid, Legend, ErrorBar,
} from 'recharts'
import type {
  EstadisticaAnual, RiesgoConstruccionRanking, EventoFENData,
  RiesgoEscenario, SendaiReport,
} from '../types'

const C = {
  primary:'#059669', danger:'#dc2626', warning:'#f59e0b',
  text:'#0f172a', textMuted:'#94a3b8', border:'#e2e8f0', bgMuted:'#f1f5f9',
  orange:'#f97316', indigo:'#6366f1', amber:'#f59e0b',
  teal:'#0891b2', violet:'#7c3aed',
}

const RISK_COLORS  = [C.primary,'#10b981',C.warning,C.orange,C.danger]
const FEN_COLORS: Record<string,string> = {
  debil:'#10b981', moderado:'#f59e0b', fuerte:'#f97316', extraordinario:'#dc2626', la_nina:'#0ea5e9',
}
const DS_COLORS = ['#10b981','#f59e0b','#f97316','#dc2626']
const DS_LABELS = ['DS1 Leve','DS2 Moderado','DS3 Extenso','DS4 Colapso']

type Modo = 'cantidad'|'magnitud'|'profundidad'|'irc'|'irc_v9'|'fen'|'escenario'|'sendai'

interface Props {
  estadisticas:  EstadisticaAnual[]
  loading:       boolean
  ircRanking?:   RiesgoConstruccionRanking[]
  eventosFen?:   EventoFENData[]
  escenario?:    RiesgoEscenario|null    // 🆕 v9
  sendai?:       SendaiReport|null       // 🆕 v9
}

function CustomTooltip({ active, payload, label }: { active?:boolean; payload?:Array<{value:number;payload:EstadisticaAnual}>; label?:string }) {
  if(!active||!payload?.length) return null
  const d=payload[0]?.payload; if(!d) return null
  return (
    <div style={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:10,padding:'10px 14px',boxShadow:'0 4px 16px rgba(0,0,0,0.08)',minWidth:170}}>
      <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:13,fontWeight:700,color:C.text,marginBottom:8}}>{label}</p>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:11,color:C.primary,marginBottom:3}}>{d.cantidad.toLocaleString('es-PE')} sismos</p>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.warning,marginBottom:2}}>Máx: {d.magnitud_max} Mw</p>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted,marginBottom:6}}>Prom: {d.magnitud_prom?.toFixed(2)} Mw</p>
      {(d.m6_plus>0||d.m7_plus>0)&&<p style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.danger,marginBottom:4}}>M6+: {d.m6_plus}  M7+: {d.m7_plus}</p>}
      <div style={{height:1,background:C.border,margin:'6px 0'}} />
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#dc2626'}}>Superf: {d.superficiales}</p>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#f97316'}}>Inter:  {d.intermedios}</p>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:'#0ea5e9'}}>Prof:   {d.profundos}</p>
    </div>
  )
}

function IRCTooltip({ active, payload }: { active?:boolean; payload?:Array<{payload:RiesgoConstruccionRanking}> }) {
  if(!active||!payload?.length) return null
  const d=payload[0]?.payload; if(!d) return null
  const nivel=Math.max(1,Math.min(5,Math.round(d.indice_riesgo_construccion)))
  return (
    <div style={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:10,padding:'10px 14px',boxShadow:'0 4px 16px rgba(0,0,0,0.08)',minWidth:220}}>
      <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,fontWeight:700,color:C.text,marginBottom:4}}>{d.distrito}</p>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted,marginBottom:6}}>{d.departamento}</p>
      <div style={{display:'flex',justifyContent:'space-between',marginBottom:4}}>
        <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted}}>IRC v8</span>
        <span style={{fontFamily:"'DM Mono',monospace",fontSize:13,fontWeight:800,color:RISK_COLORS[nivel-1]}}>{d.indice_riesgo_construccion.toFixed(2)}</span>
      </div>
      {d.indice_riesgo_v9&&(
        <div style={{display:'flex',justifyContent:'space-between',marginBottom:4}}>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted}}>IRC v9</span>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:13,fontWeight:800,color:'#dc2626'}}>{d.indice_riesgo_v9.toFixed(2)}</span>
        </div>
      )}
      {d.factor_cascada&&d.factor_cascada>1.01&&(
        <p style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.teal}}>Cascada ×{d.factor_cascada.toFixed(3)}</p>
      )}
    </div>
  )
}

function FENTooltip({ active, payload }: { active?:boolean; payload?:Array<{payload:EventoFENData&{label:string;oni_abs:number}}> }) {
  if(!active||!payload?.length) return null
  const d=payload[0]?.payload; if(!d) return null
  const isElNino=d.tipo==='el_nino'
  const color=isElNino?(FEN_COLORS[d.intensidad??'debil']??C.orange):C.teal
  return (
    <div style={{background:'#fff',border:`1px solid ${C.border}`,borderRadius:10,padding:'10px 14px',boxShadow:'0 4px 16px rgba(0,0,0,0.08)',maxWidth:240}}>
      <p style={{fontFamily:"'DM Mono',monospace",fontSize:11,fontWeight:800,color,marginBottom:4}}>
        {d.tipo==='el_nino'?'🌡 El Niño':'❄ La Niña'} {d.año_inicio}/{(d.año_fin%100).toString().padStart(2,'0')}
      </p>
      {d.intensidad&&<p style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted,marginBottom:4}}>{d.intensidad} · ONI {d.oni_peak?.toFixed(2)??'-'}°C</p>}
      {d.duracion_meses&&<p style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,marginBottom:6}}>Duración: {d.duracion_meses} meses</p>}
      {d.impacto_peru&&<p style={{fontFamily:"'DM Sans',sans-serif",fontSize:10,color:C.text,lineHeight:1.4,borderTop:`1px solid ${C.border}`,paddingTop:6}}>{d.impacto_peru.substring(0,120)}{d.impacto_peru.length>120?'…':''}</p>}
    </div>
  )
}

function Skeleton() {
  return (
    <div style={{height:'100%',display:'flex',alignItems:'flex-end',gap:2,paddingBottom:4}}>
      {Array.from({length:30},(_,i)=>(
        <div key={i} style={{flex:1,borderRadius:'2px 2px 0 0',background:C.bgMuted,
          height:`${18+Math.sin(i*0.6)*14+(i%3)*5}px`,opacity:0.6}} />
      ))}
    </div>
  )
}

const StatsChart = memo(function StatsChart({ estadisticas, loading, ircRanking=[], eventosFen=[], escenario, sendai }: Props) {
  const [modo, setModo] = useState<Modo>('cantidad')

  if(loading) return <Skeleton />

  const max   = estadisticas.length?Math.max(...estadisticas.map(e=>e.cantidad)):0
  const avg   = estadisticas.length?estadisticas.reduce((s,e)=>s+e.cantidad,0)/estadisticas.length:0
  const total = estadisticas.length?estadisticas.reduce((s,e)=>s+e.cantidad,0):0
  const barC  = (n:number)=>n>=max*0.8?C.danger:n>=max*0.5?C.warning:C.primary

  const MODOS: {key:Modo;label:string;color:string}[] = [
    {key:'cantidad',   label:'Cantidad',  color:C.primary },
    {key:'magnitud',   label:'Magnitud',  color:C.warning },
    {key:'profundidad',label:'Profund.',  color:C.indigo  },
    {key:'irc',        label:'IRC v8',    color:C.amber   },
    {key:'irc_v9',     label:'IRC v9',    color:'#dc2626' },  // 🆕
    {key:'fen',        label:'🌡 FEN',    color:C.teal    },
    ...(escenario?[{key:'escenario' as Modo,label:'4DS',color:C.orange}]:[]),
    ...(sendai?   [{key:'sendai'   as Modo,label:'Sendai',color:C.violet}]:[]),
  ]

  const ircTop = [...ircRanking].sort((a,b)=>b.indice_riesgo_construccion-a.indice_riesgo_construccion).slice(0,15)
  const ircV9Top = [...ircRanking].filter(r=>r.indice_riesgo_v9).sort((a,b)=>(b.indice_riesgo_v9??0)-(a.indice_riesgo_v9??0)).slice(0,15)
  const fenData = eventosFen.filter(e=>e.tipo!=='neutro'&&e.oni_peak!==null).sort((a,b)=>a.año_inicio-b.año_inicio).map(e=>({...e,label:`${e.año_inicio}/${(e.año_fin%100).toString().padStart(2,'0')}`,oni_abs:Math.abs(e.oni_peak??0)}))

  // 🆕 v9: datos escenario 4DS
  const escenarioData = escenario ? Object.entries(escenario.por_tipo).map(([tipo, d]) => ({
    name: tipo.replace('_',' ').substring(0,10),
    ds1: Math.round(d.viv_ds1), ds2: Math.round(d.viv_ds2),
    ds3: Math.round(d.viv_ds3), ds4: Math.round(d.viv_ds4),
    perdida_k: Math.round(d.perdida_usd/1000),
  })) : []

  // 🆕 v9: datos Sendai 7 targets
  const sendaiData = sendai ? [
    {name:'A',label:'Mortalidad',  valor:typeof sendai.target_a==='object'?Object.values(sendai.target_a)[0] as number:0,color:'#dc2626'},
    {name:'B',label:'Afectados',   valor:typeof sendai.target_b==='object'?Object.values(sendai.target_b)[0] as number:0,color:'#f97316'},
    {name:'C',label:'Pérd. econ.', valor:typeof sendai.target_c==='object'?Object.values(sendai.target_c)[0] as number:0,color:'#f59e0b'},
    {name:'D',label:'Infraestr.',  valor:typeof sendai.target_d==='object'?Object.values(sendai.target_d)[0] as number:0,color:'#6366f1'},
    {name:'E',label:'Estrategias', valor:typeof sendai.target_e==='object'?Object.values(sendai.target_e)[0] as number:0,color:'#0891b2'},
    {name:'F',label:'Cooperación', valor:typeof sendai.target_f==='object'?Object.values(sendai.target_f)[0] as number:0,color:'#059669'},
    {name:'G',label:'MHEWS',       valor:typeof sendai.target_g==='object'?Object.values(sendai.target_g)[0] as number:0,color:'#7c3aed'},
  ] : []

  return (
    <div style={{height:'100%',display:'flex',flexDirection:'column'}}>
      {/* Header tabs */}
      <div style={{display:'flex',justifyContent:'space-between',alignItems:'center',marginBottom:5,flexShrink:0}}>
        <div style={{display:'flex',gap:3,flexWrap:'wrap'}}>
          {MODOS.map(({key,label,color})=>(
            <button key={key} onClick={()=>setModo(key)} style={{
              padding:'2px 8px',borderRadius:5,border:'none',cursor:'pointer',
              background:modo===key?`${color}15`:'transparent',
              color:modo===key?color:C.textMuted,
              fontFamily:"'DM Mono',monospace",fontSize:9,fontWeight:modo===key?700:400,
              transition:'all 0.15s',
            }}>{label}</button>
          ))}
        </div>
        <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>
          {modo==='irc'      ? <span style={{color:C.amber,fontWeight:700}}>{ircTop.length} distritos</span>
          :modo==='irc_v9'   ? <span style={{color:'#dc2626',fontWeight:700}}>{ircV9Top.length} dist IRC v9</span>
          :modo==='fen'      ? <span style={{color:C.teal,fontWeight:700}}>{fenData.length} eventos ENSO</span>
          :modo==='escenario'? <span style={{color:C.orange,fontWeight:700}}>PGA {escenario?.pga_g.toFixed(3)}g · M{escenario?.magnitud}</span>
          :modo==='sendai'   ? <span style={{color:C.violet,fontWeight:700}}>Sendai {sendai?.año}</span>
          :<><span style={{color:C.text,fontWeight:700}}>{total.toLocaleString('es-PE')}</span> total</>}
        </span>
      </div>

      {/* Gráfica */}
      <div style={{flex:1,minHeight:0}}>
        <ResponsiveContainer width="100%" height="100%">
          {modo==='cantidad' ? (
            <BarChart data={estadisticas} margin={{top:2,right:0,left:-30,bottom:0}}>
              <XAxis dataKey="anio" tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} cursor={{fill:'rgba(5,150,105,0.04)'}} />
              <ReferenceLine y={avg} stroke={C.border} strokeDasharray="3 3" />
              <Bar dataKey="cantidad" radius={[2,2,0,0]} maxBarSize={12}>
                {estadisticas.map((e,i)=><Cell key={i} fill={barC(e.cantidad)} />)}
              </Bar>
            </BarChart>
          ) : modo==='magnitud' ? (
            <AreaChart data={estadisticas} margin={{top:2,right:0,left:-30,bottom:0}}>
              <defs>
                <linearGradient id="gMag" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={C.warning} stopOpacity={0.3} />
                  <stop offset="95%" stopColor={C.warning} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
              <XAxis dataKey="anio" tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} domain={[4,'auto']} />
              <Tooltip content={<CustomTooltip />} />
              <Area dataKey="magnitud_max"  stroke={C.warning} strokeWidth={1.5} fill="url(#gMag)" dot={false} />
              <Area dataKey="magnitud_prom" stroke={C.primary} strokeWidth={1} fill="none" strokeDasharray="3 3" dot={false} />
            </AreaChart>
          ) : modo==='irc' ? (
            <BarChart data={ircTop} layout="vertical" margin={{top:0,right:10,left:8,bottom:0}}>
              <XAxis type="number" domain={[0,5]} tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <YAxis type="category" dataKey="distrito" tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} width={70} />
              <Tooltip content={<IRCTooltip />} cursor={{fill:'rgba(245,158,11,0.05)'}} />
              <Bar dataKey="indice_riesgo_construccion" radius={[0,3,3,0]} maxBarSize={10}>
                {ircTop.map((e,i)=>{const n=Math.max(1,Math.min(5,Math.round(e.indice_riesgo_construccion)));return<Cell key={i} fill={RISK_COLORS[n-1]} />})}
              </Bar>
            </BarChart>
          ) : modo==='irc_v9' ? (
            // 🆕 v9: IRC v9 con intervalos de confianza (ErrorBar)
            <BarChart data={ircV9Top} layout="vertical" margin={{top:0,right:16,left:8,bottom:0}}>
              <XAxis type="number" domain={[0,6]} tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <YAxis type="category" dataKey="distrito" tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} width={70} />
              <Tooltip content={<IRCTooltip />} cursor={{fill:'rgba(220,38,38,0.05)'}} />
              <Bar dataKey="indice_riesgo_v9" radius={[0,3,3,0]} maxBarSize={10}>
                <ErrorBar dataKey="irc_v9_p90" width={4} strokeWidth={1.5} stroke="#dc262640" direction="x" />
                {ircV9Top.map((e,i)=>{const n=Math.max(1,Math.min(5,Math.round(e.indice_riesgo_v9??3)));return<Cell key={i} fill={RISK_COLORS[n-1]} />})}
              </Bar>
            </BarChart>
          ) : modo==='fen' ? (
            <BarChart data={fenData} margin={{top:2,right:0,left:-30,bottom:0}}>
              <XAxis dataKey="label" tick={{fill:C.textMuted,fontSize:7,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} domain={[0,2.6]}
                label={{value:'|ONI| °C',angle:-90,position:'insideLeft',fill:C.textMuted,fontSize:7}} />
              <Tooltip content={<FENTooltip />} cursor={{fill:'rgba(8,145,178,0.05)'}} />
              <ReferenceLine y={0.5} stroke={C.teal} strokeDasharray="2 2" />
              <ReferenceLine y={1.5} stroke={C.orange} strokeDasharray="2 2" />
              <ReferenceLine y={2.0} stroke={C.danger} strokeDasharray="2 2" />
              <Bar dataKey="oni_abs" radius={[2,2,0,0]} maxBarSize={14}>
                {fenData.map((e,i)=>{const color=e.tipo==='la_nina'?C.teal:(FEN_COLORS[e.intensidad??'debil']??C.orange);return<Cell key={i} fill={color} />})}
              </Bar>
            </BarChart>
          ) : modo==='escenario' && escenarioData.length ? (
            // 🆕 v9: 4 estados DS apilados por tipo construcción (Tarque et al. 2012)
            <BarChart data={escenarioData} margin={{top:2,right:4,left:-24,bottom:0}}>
              <XAxis dataKey="name" tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <YAxis tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <Tooltip formatter={(v:number|undefined,n:string|undefined)=>[(v??0).toLocaleString('es-PE'),n??'']} cursor={{fill:'rgba(0,0,0,0.03)'}} />
              <Legend iconSize={8} wrapperStyle={{fontSize:8,fontFamily:'DM Mono'}} />
              {DS_LABELS.map((label,i)=>(
                <Bar key={label} dataKey={`ds${i+1}`} name={label} stackId="ds"
                  fill={DS_COLORS[i]} maxBarSize={20} />
              ))}
            </BarChart>
          ) : modo==='sendai' && sendaiData.length ? (
            // 🆕 v9: Sendai 7 targets como barras con advertencia
            <BarChart data={sendaiData} margin={{top:2,right:4,left:-24,bottom:0}}>
              <XAxis dataKey="name" tick={{fill:C.textMuted,fontSize:9,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <YAxis tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <Tooltip formatter={(v:number|undefined,_:string|undefined,p?:{payload?:{label:string}})=>[(v??0),p?.payload?.label??'']} cursor={{fill:'rgba(0,0,0,0.03)'}} />
              <Bar dataKey="valor" radius={[3,3,0,0]} maxBarSize={30}>
                {sendaiData.map((d,i)=><Cell key={i} fill={d.color} />)}
              </Bar>
            </BarChart>
          ) : (
            // Profundidad
            <AreaChart data={estadisticas} margin={{top:2,right:0,left:-30,bottom:0}}>
              <defs>
                {[['gS','#dc2626'],['gI','#f97316'],['gP','#0ea5e9']].map(([id,c])=>(
                  <linearGradient key={id} id={id} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%"  stopColor={c} stopOpacity={0.3} />
                    <stop offset="95%" stopColor={c} stopOpacity={0.02} />
                  </linearGradient>
                ))}
              </defs>
              <XAxis dataKey="anio" tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{fill:C.textMuted,fontSize:8,fontFamily:'DM Mono'}} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Area type="monotone" dataKey="superficiales" stroke="#dc2626" fill="url(#gS)" strokeWidth={1.2} dot={false} stackId="a" />
              <Area type="monotone" dataKey="intermedios"   stroke="#f97316" fill="url(#gI)" strokeWidth={1.2} dot={false} stackId="a" />
              <Area type="monotone" dataKey="profundos"     stroke="#0ea5e9" fill="url(#gP)" strokeWidth={1.2} dot={false} stackId="a" />
            </AreaChart>
          )}
        </ResponsiveContainer>
      </div>

      {/* Mini leyendas contextuales */}
      {modo==='fen' && (
        <div style={{display:'flex',gap:8,marginTop:4,flexShrink:0,flexWrap:'wrap'}}>
          {[{color:C.teal,label:'La Niña'},{color:'#10b981',label:'Débil'},{color:C.warning,label:'Moderado'},{color:C.orange,label:'Fuerte'},{color:C.danger,label:'Extraordinario'}].map(({color,label})=>(
            <div key={label} style={{display:'flex',alignItems:'center',gap:3}}>
              <div style={{width:8,height:8,borderRadius:2,background:color}} />
              <span style={{fontFamily:"'DM Mono',monospace",fontSize:7.5,color:C.textMuted}}>{label}</span>
            </div>
          ))}
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted,marginLeft:'auto'}}>líneas: 0.5|1.5|2.0°C ONI</span>
        </div>
      )}
      {modo==='irc_v9' && (
        <div style={{display:'flex',gap:6,marginTop:4,flexShrink:0}}>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:7.5,color:'#dc2626'}}>IRC v9 = 35%S+20%I+18%D+10%T+8%V+5%Q+4%F × cascada</span>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted,marginLeft:'auto'}}>barras de error: IC 80%</span>
        </div>
      )}
      {modo==='escenario' && escenario && (
        <div style={{display:'flex',gap:10,marginTop:4,flexShrink:0,flexWrap:'wrap'}}>
          {DS_LABELS.map((l,i)=>(
            <div key={l} style={{display:'flex',alignItems:'center',gap:3}}>
              <div style={{width:8,height:8,borderRadius:2,background:DS_COLORS[i]}} />
              <span style={{fontFamily:"'DM Mono',monospace",fontSize:7.5,color:C.textMuted}}>{l}</span>
            </div>
          ))}
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted,marginLeft:'auto'}}>Youngs 1997 · GEM 2023</span>
        </div>
      )}
      {modo==='sendai' && sendai && (
        <div style={{marginTop:4,flexShrink:0}}>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:7,color:C.textMuted}}>
            ⚠ Métricas proxy — NO sustituyen reporte oficial INDECI/CENEPRED al UNDRR
          </span>
        </div>
      )}
    </div>
  )
})

export default StatsChart