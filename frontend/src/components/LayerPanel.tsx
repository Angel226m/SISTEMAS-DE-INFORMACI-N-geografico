// ══════════════════════════════════════════════════════════
// LayerPanel.tsx v9.0
// 🆕 Capa: volcanes (INGEMMET/OVI-IGP 2021)
// 🆕 Leyenda volcanes: escala de estado + radios peligro
// 🆕 Capa: susceptibilidad ML (XGBoost + SHAP)
// 🆕 Leyenda ML: escala de score + niveles
// 🆕 Capa: alertas_ews (EWS multi-hazard tiempo real)
// 🆕 Leyenda IRC v9: 7 amenazas + factor_cascada
// ✅ Todas las leyendas v8.0 mantenidas
// ══════════════════════════════════════════════════════════

import type { CapasActivas } from '../types'

interface Props { capas: CapasActivas; onChange: (c: CapasActivas) => void; mlEntrenando?: boolean }

const C = {
  text:'#0f172a', textSec:'#475569', textMuted:'#94a3b8',
  border:'#e2e8f0', bg:'#ffffff', bgSoft:'#f8fafc', bgMuted:'#f1f5f9',
  primary:'#059669', primaryBg:'#ecfdf5',
  amber:'#f59e0b', orange:'#f97316', danger:'#dc2626',
  teal:'#0891b2', cyan:'#06b6d4', violet:'#7c3aed',
}

interface LayerDef {
  key: keyof CapasActivas; label: string; sub: string
  icon: string; color: string; bg: string; badge?: string
}

const LAYERS: LayerDef[] = [
  { key:'sismos',             label:'Sismos',             sub:'USGS 1900–hoy · GPU filter',       icon:'●', color:'#dc2626', bg:'#fef2f2', badge:'GPU'  },
  { key:'heatmap',            label:'Densidad sísmica',   sub:'Mapa de calor ponderado',           icon:'◉', color:'#f97316', bg:'#fff7ed'               },
  { key:'departamentos',      label:'Departamentos',      sub:'Zona sísmica NTE E.030-2018',       icon:'▦', color:'#7c3aed', bg:'#f5f3ff', badge:'v7.5' },
  { key:'riesgo_distritos',   label:'Riesgo distritos',   sub:'IRC v9 · 7 amenazas',               icon:'▧', color:'#059669', bg:'#ecfdf5', badge:'v9'   },
  { key:'riesgo_construccion',label:'IRC — Construcción', sub:'CENEPRED · NTE E.030/E.031',        icon:'⬡', color:'#f59e0b', bg:'#fffbeb', badge:'v9'   },
  { key:'precipitaciones',    label:'Precipitaciones',    sub:'SENAMHI/CHIRPS · índice FEN',       icon:'◈', color:'#0891b2', bg:'#ecfeff', badge:'v8'   },
  // 🆕 v9
  { key:'volcanes',           label:'Volcanes',           sub:'INGEMMET/OVI-IGP 2021 · 20',        icon:'🌋', color:'#dc2626', bg:'#fef2f2', badge:'v9'  },
  { key:'susceptibilidad',    label:'Susceptibilidad ML', sub:'XGBoost + SHAP · AUC >0.95',        icon:'◐', color:'#7c3aed', bg:'#faf5ff', badge:'v9'   },
  { key:'alertas_ews',        label:'Alertas EWS',        sub:'INDECI 2020 · tiempo real',          icon:'⚡', color:'#f97316', bg:'#fff7ed', badge:'v9'  },
  { key:'fallas',             label:'Fallas geológicas',  sub:'Audin et al. 2008 + IGP 2021',      icon:'⌗', color:'#f59e0b', bg:'#fffbeb'               },
  { key:'inundaciones',       label:'Inundaciones',       sub:'ANA / CENEPRED',                    icon:'≈', color:'#0ea5e9', bg:'#f0f9ff'               },
  { key:'tsunamis',           label:'Tsunamis',           sub:'PREDES / IGP / INDECI',             icon:'≋', color:'#06b6d4', bg:'#ecfeff'               },
  { key:'deslizamientos',     label:'Deslizamientos',     sub:'CENEPRED / INGEMMET',               icon:'◤', color:'#92400e', bg:'#fef3c7'               },
  { key:'infraestructura',    label:'Infraestructura',    sub:'Oficial + OSM · 60k puntos',        icon:'⊕', color:'#6366f1', bg:'#eef2ff', badge:'v7.5' },
  { key:'estaciones',         label:'Estaciones',         sub:'IGP / SENAMHI / ANA / DHN',         icon:'◎', color:'#10b981', bg:'#f0fdf4'               },
  { key:'extrusion_3d',       label:'Extrusión 3D',       sub:'Modo 3D requerido',                 icon:'⬡', color:'#ec4899', bg:'#fdf2f8'               },
]

const ZONA_SISMICA = [
  { zona:4, factor:'0.45g', label:'Zona 4 — Muy alto', color:'#dc2626',
    deptos:'Tumbes, Piura, Lambayeque, La Libertad, Ancash, Lima, Callao, Ica, Arequipa, Moquegua, Tacna' },
  { zona:3, factor:'0.35g', label:'Zona 3 — Alto',     color:'#f97316',
    deptos:'Cajamarca, San Martín, Huancavelica, Junín, Pasco, Cusco' },
  { zona:2, factor:'0.25g', label:'Zona 2 — Moderado', color:'#f59e0b',
    deptos:'Amazonas, Huánuco, Ayacucho, Apurímac, Puno, Ucayali' },
  { zona:1, factor:'0.10g', label:'Zona 1 — Bajo',     color:'#059669',
    deptos:'Loreto, Madre de Dios' },
]

const RISK_SCALE = [
  { level:1, label:'Muy bajo', color:'#059669' },
  { level:2, label:'Bajo',     color:'#10b981' },
  { level:3, label:'Moderado', color:'#f59e0b' },
  { level:4, label:'Alto',     color:'#f97316' },
  { level:5, label:'Muy alto', color:'#dc2626' },
]

const DEPTH_ITEMS = [
  { color:'#dc2626', label:'Superficial', sub:'< 30 km',  desc:'Mayor daño en superficie' },
  { color:'#f97316', label:'Intermedio',  sub:'30–70 km', desc:'Daño moderado' },
  { color:'#0ea5e9', label:'Profundo',    sub:'> 70 km',  desc:'Menor intensidad' },
]

const TSUNAMI_SCALE = [
  { color:'#06b6d4', label:'< 1 m',  desc:'Bajo' },
  { color:'#0891b2', label:'1–3 m',  desc:'Moderado' },
  { color:'#0e7490', label:'3–10 m', desc:'Alto' },
  { color:'#164e63', label:'> 10 m', desc:'Catastrófico' },
]

const FEN_SCALE = [
  { range:'< 0.9',   label:'Sequía en FEN',           color:'#4ade80', desc:'Altiplano / Puna sur'         },
  { range:'0.9–1.3', label:'Sin cambio',               color:'#94a3b8', desc:'Amazonia / Sierra media'      },
  { range:'1.3–2.0', label:'Amplificación moderada',   color:'#f59e0b', desc:'Ceja de selva / Sierra norte' },
  { range:'2.0–3.5', label:'Amplificación alta',       color:'#f97316', desc:'Costa central / Lambayeque'   },
  { range:'> 3.5',   label:'Catastrófico en FEN',      color:'#dc2626', desc:'Costa norte: Piura / Tumbes'  },
]

// 🆕 v9.0
const VOLCAN_ESTADOS = [
  { estado:'activo_critico',         label:'Activo crítico',        color:'#dc2626', radios:'30/60/100 km' },
  { estado:'activo',                 label:'Activo',                color:'#f97316', radios:'30/60 km' },
  { estado:'potencialmente_activo',  label:'Potencialmente activo', color:'#f59e0b', radios:'30 km' },
  { estado:'inactivo',               label:'Inactivo',              color:'#9ca3af', radios:'—' },
]

const ML_NIVELES = [
  { nivel:'MUY_BAJO', label:'Muy bajo', range:'0–20%',  color:'#059669' },
  { nivel:'BAJO',     label:'Bajo',     range:'20–40%', color:'#10b981' },
  { nivel:'MEDIO',    label:'Medio',    range:'40–60%', color:'#f59e0b' },
  { nivel:'ALTO',     label:'Alto',     range:'60–80%', color:'#f97316' },
  { nivel:'MUY_ALTO', label:'Muy alto', range:'80–100%',color:'#dc2626' },
]

const EWS_NIVELES = [
  { nivel:'watch',     label:'Watch',     desc:'M≥5.5 costa/M≥6.0 sierra',  color:'#f59e0b' },
  { nivel:'warning',   label:'Warning',   desc:'M≥6.5 + tsunamigénico',      color:'#f97316' },
  { nivel:'emergency', label:'Emergency', desc:'M≥7.5 + colapso inmediato',  color:'#dc2626' },
]

export default function LayerPanel({ capas, onChange, mlEntrenando=false }: Props) {
  const active = Object.values(capas).filter(Boolean).length

  return (
    <div>
      <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', marginBottom:14 }}>
        <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.15em' }}>Capas del mapa</span>
        <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, fontWeight:600, color:C.primary, background:C.primaryBg, padding:'2px 8px', borderRadius:99 }}>
          {active} activas
        </span>
      </div>

      <div style={{ display:'flex', flexDirection:'column', gap:3 }}>
        {LAYERS.map(({ key, label, sub, icon, color, bg, badge }) => {
          const on = capas[key]
          const isML = key === 'susceptibilidad'
          const training = isML && mlEntrenando
          return (
            <button key={key} onClick={() => onChange({ ...capas, [key]: !on })}
              style={{ width:'100%', display:'flex', alignItems:'center', gap:9, padding:'8px 10px',
                background:on?bg:'transparent', border:`1px solid ${on?color+'28':'transparent'}`,
                borderRadius:10, cursor:'pointer', transition:'all 0.18s ease', textAlign:'left' }}>
              <div style={{ width:28, height:28, borderRadius:7, flexShrink:0,
                background:on?bg:C.bgMuted, border:`1px solid ${on?color+'38':C.border}`,
                display:'flex', alignItems:'center', justifyContent:'center',
                fontSize:13, color:on?color:C.textMuted }}>
                {training
                  ? <div style={{width:10,height:10,borderRadius:'50%',border:'2px solid rgba(124,58,237,0.3)',borderTopColor:'#7c3aed',animation:'spin 0.7s linear infinite'}}/>
                  : icon}
              </div>
              <div style={{ flex:1, minWidth:0 }}>
                <div style={{ display:'flex', alignItems:'center', gap:5 }}>
                  <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:12, fontWeight:600,
                    color:on?C.text:C.textMuted, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
                    {label}
                  </span>
                  {badge && (() => {
                    const bc = badge==='v9'?'#dc2626':badge==='v8'?C.teal:badge==='v7.5'?C.amber:badge==='GPU'?'#6366f1':badge==='XGBoost'?C.violet:C.textMuted
                    const bb = badge==='v9'?'#fef2f2':badge==='v8'?'#ecfeff':badge==='v7.5'?'#fffbeb':badge==='GPU'?'#eef2ff':C.bgMuted
                    const bbd = badge==='v9'?'#fecaca':badge==='v8'?'#a5f3fc':badge==='v7.5'?'#fde68a':badge==='GPU'?'#c7d2fe':C.border
                    return <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:700, color:bc,
                      background:bb, border:`1px solid ${bbd}`, padding:'1px 4px', borderRadius:3, flexShrink:0 }}>{badge}</span>
                  })()}
                  {training&&<span style={{fontFamily:"'DM Mono',monospace",fontSize:7,fontWeight:700,color:'#7c3aed',background:'#faf5ff',border:'1px solid #ddd6fe',padding:'1px 4px',borderRadius:3,flexShrink:0}}>entrenando...</span>}
                </div>
                <div style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, marginTop:1 }}>
                  {training ? 'XGBoost + Optuna · VIF → RFE → SMOTE · ~3-5 min' : sub}
                </div>
              </div>
              <div style={{ width:32, height:17, borderRadius:9, flexShrink:0,
                background:on?color:C.bgMuted, border:`1px solid ${on?color:C.border}`, position:'relative' }}>
                <div style={{ position:'absolute', top:2, left:on?15:2, width:11, height:11, borderRadius:'50%',
                  background:'white', boxShadow:'0 1px 3px rgba(0,0,0,0.2)', transition:'left 0.22s cubic-bezier(0.4,0,0.2,1)' }} />
              </div>
            </button>
          )
        })}
      </div>

      {/* 🆕 v9.0: Leyenda Volcanes */}
      <div style={{ marginTop:18, paddingTop:14, borderTop:`1px solid ${C.border}` }}>
        <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:8 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em' }}>Volcanes</span>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:700, color:'#dc2626', background:'#fef2f2', border:'1px solid #fecaca', padding:'1px 4px', borderRadius:3 }}>INGEMMET 2021</span>
        </div>
        {VOLCAN_ESTADOS.map(({ estado, label, color, radios }) => (
          <div key={estado} style={{ display:'flex', gap:8, marginBottom:6, alignItems:'flex-start' }}>
            <div style={{ width:28, height:16, borderRadius:4, flexShrink:0, marginTop:1,
              background:color+'25', border:`2px solid ${color}80` }} />
            <div style={{ flex:1 }}>
              <div style={{ display:'flex', justifyContent:'space-between' }}>
                <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:10, fontWeight:600, color:C.textSec }}>{label}</span>
                <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, color }}>{radios}</span>
              </div>
            </div>
          </div>
        ))}
        <div style={{ marginTop:6, padding:'5px 8px', background:'#fef2f2', border:'1px solid #fecaca', borderRadius:6 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7.5, color:'#991b1b', lineHeight:1.5 }}>
            Radio peligro niveles 3-5<br />según INDECI Protocolo Volcánico 2020
          </span>
        </div>
      </div>

      {/* 🆕 v9.0: Leyenda Susceptibilidad ML */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:8 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em' }}>Susceptibilidad ML</span>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:700, color:C.violet, background:'#faf5ff', border:'1px solid #e9d5ff', padding:'1px 4px', borderRadius:3 }}>XGBoost</span>
        </div>
        <div style={{ display:'flex', gap:2, marginBottom:5 }}>
          {ML_NIVELES.map(({ color }) => (
            <div key={color} style={{ flex:1, height:7, background:color, borderRadius:2 }} />
          ))}
        </div>
        <div style={{ display:'flex', justifyContent:'space-between', marginBottom:4 }}>
          {ML_NIVELES.map(({ nivel, color }) => (
            <div key={nivel} style={{ flex:1, textAlign:'center' }}>
              <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, color }}>{nivel.replace('_', ' ')}</span>
            </div>
          ))}
        </div>
        {ML_NIVELES.map(({ label, range, color }) => (
          <div key={label} style={{ display:'flex', alignItems:'center', gap:6, marginBottom:3 }}>
            <div style={{ width:8, height:8, borderRadius:2, background:color, flexShrink:0 }} />
            <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:10, color:C.textSec }}>{label}</span>
            <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, color:C.textMuted, marginLeft:'auto' }}>{range}</span>
          </div>
        ))}
        <div style={{ marginTop:6, padding:'5px 8px', background:'#faf5ff', border:'1px solid #e9d5ff', borderRadius:6 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7.5, color:C.violet, lineHeight:1.5 }}>
            IC 80% via bootstrapping 100 iter.<br />SHAP top-5 features explicativas
          </span>
        </div>
      </div>

      {/* 🆕 v9.0: Leyenda EWS Alertas */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:8 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em' }}>EWS Alertas</span>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:700, color:'#f97316', background:'#fff7ed', border:'1px solid #fed7aa', padding:'1px 4px', borderRadius:3 }}>tiempo real</span>
        </div>
        {EWS_NIVELES.map(({ nivel, label, desc, color }) => (
          <div key={nivel} style={{ display:'flex', gap:8, marginBottom:7, alignItems:'flex-start' }}>
            <div style={{ width:28, height:16, borderRadius:4, flexShrink:0, marginTop:1,
              background:color+'20', border:`2px solid ${color}80`,
              display:'flex', alignItems:'center', justifyContent:'center' }}>
              <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:800, color }}>{nivel.charAt(0).toUpperCase()}</span>
            </div>
            <div style={{ flex:1 }}>
              <div style={{ display:'flex', justifyContent:'space-between' }}>
                <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:10.5, fontWeight:600, color:C.textSec }}>{label}</span>
              </div>
              <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:9, color:C.textMuted }}>{desc}</span>
            </div>
          </div>
        ))}
        <div style={{ marginTop:4, padding:'5px 8px', background:'#fff7ed', border:'1px solid #fed7aa', borderRadius:6 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7.5, color:'#92400e', lineHeight:1.5 }}>
            CAP v1.2 · ITU-T X.1303bis<br />Cascada tsunami: M≥6.5+d{'<'}50km<br />Cascada desliz: M≥5.0+peligro≥3
          </span>
        </div>
      </div>

      {/* Leyenda FEN/Precipitaciones (v8) */}
      <div style={{ marginTop:14, paddingTop:14, borderTop:`1px solid ${C.border}` }}>
        <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:8 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em' }}>Precipitaciones · FEN</span>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:700, color:C.teal, background:'#ecfeff', border:'1px solid #a5f3fc', padding:'1px 4px', borderRadius:3 }}>SENAMHI</span>
        </div>
        {FEN_SCALE.map(({ range, label, color, desc }) => (
          <div key={range} style={{ display:'flex', gap:8, marginBottom:6, alignItems:'flex-start' }}>
            <div style={{ width:28, height:16, borderRadius:4, flexShrink:0, marginTop:1,
              background:color+'25', border:`2px solid ${color}80` }} />
            <div style={{ flex:1 }}>
              <div style={{ display:'flex', justifyContent:'space-between' }}>
                <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:10, fontWeight:600, color:C.textSec }}>{label}</span>
                <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, color }}>{range}</span>
              </div>
              <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:9, color:C.textMuted }}>{desc}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Leyenda Zona Sísmica */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:8 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em' }}>Zona Sísmica · NTE E.030</span>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7, fontWeight:700, color:C.amber, background:'#fffbeb', border:'1px solid #fde68a', padding:'1px 4px', borderRadius:3 }}>DS N°003-2016</span>
        </div>
        {ZONA_SISMICA.map(({ zona, factor, label, color, deptos }) => (
          <div key={zona} style={{ display:'flex', gap:8, marginBottom:7, alignItems:'flex-start' }}>
            <div style={{ width:26, height:17, borderRadius:4, flexShrink:0, marginTop:1,
              background:color+'20', border:`2px solid ${color}80`,
              display:'flex', alignItems:'center', justifyContent:'center' }}>
              <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, fontWeight:800, color }}>{zona}</span>
            </div>
            <div style={{ flex:1, minWidth:0 }}>
              <div style={{ display:'flex', justifyContent:'space-between' }}>
                <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:10.5, fontWeight:600, color:C.textSec }}>{label}</span>
                <span style={{ fontFamily:"'DM Mono',monospace", fontSize:9, fontWeight:700, color }}>{factor}</span>
              </div>
              <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:9, color:C.textMuted, lineHeight:1.3, display:'block' }}>{deptos}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Leyenda IRC v9 */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em', marginBottom:6 }}>
          IRC v9 — 7 amenazas
        </div>
        <div style={{ background:'#fffbeb', border:'1px solid #fde68a', borderRadius:7, padding:'6px 8px', marginBottom:8 }}>
          <div style={{ fontFamily:"'DM Mono',monospace", fontSize:8, color:'#92400e', lineHeight:1.6 }}>
            35%S + 20%I + 18%D + 10%T<br />
            + 8%<span style={{ color:'#dc2626' }}>V</span> + 5%<span style={{ color:'#f59e0b' }}>Q</span> + 4%F × <span style={{ color:C.teal }}>cascada</span>
          </div>
        </div>
        <div style={{ display:'flex', gap:2, marginBottom:5 }}>
          {RISK_SCALE.map(({ color }) => (
            <div key={color} style={{ flex:1, height:7, background:color, borderRadius:2 }} />
          ))}
        </div>
        <div style={{ display:'flex', justifyContent:'space-between', marginBottom:4 }}>
          {RISK_SCALE.map(({ level, label }) => (
            <div key={level} style={{ flex:1, textAlign:'center' }}>
              <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, color:C.textMuted }}>{label}</span>
            </div>
          ))}
        </div>
        <div style={{ padding:'5px 8px', background:'#f0fdf4', border:'1px solid #bbf7d0', borderRadius:6 }}>
          <span style={{ fontFamily:"'DM Mono',monospace", fontSize:7.5, color:'#166534', lineHeight:1.5 }}>
            IC 80%: irc_v9_p10 / irc_v9_p90<br />Li et al. 2023 · bootstrapping 500 iter.
          </span>
        </div>
      </div>

      {/* Leyenda Profundidad */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em', marginBottom:8 }}>Profundidad sísmica</div>
        {DEPTH_ITEMS.map(({ color, label, sub, desc }) => (
          <div key={label} style={{ display:'flex', alignItems:'center', gap:7, marginBottom:6 }}>
            <div style={{ width:9, height:9, borderRadius:'50%', background:color, flexShrink:0, boxShadow:`0 0 0 2px ${color}30` }} />
            <div style={{ flex:1 }}>
              <div style={{ display:'flex', alignItems:'center', gap:5 }}>
                <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:11, color:C.textSec }}>{label}</span>
                <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, color:C.textMuted }}>{sub}</span>
              </div>
              <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:9, color:C.textMuted }}>{desc}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Leyenda Fallas */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em', marginBottom:8 }}>Fallas geológicas</div>
        {[{ color:'#dc2626', label:'Activa (neotectónica)' }, { color:'#9ca3af', label:'Inactiva' }].map(({ color, label }) => (
          <div key={label} style={{ display:'flex', alignItems:'center', gap:7, marginBottom:5 }}>
            <div style={{ width:18, height:2.5, background:color, borderRadius:2, flexShrink:0 }} />
            <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:11, color:C.textSec }}>{label}</span>
          </div>
        ))}
      </div>

      {/* Leyenda Infraestructura */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em', marginBottom:8 }}>Infraestructura crítica</div>
        <div style={{ display:'flex', gap:6, marginBottom:10 }}>
          {[{ label:'Oficial', color:'#059669', bg:'#ecfdf5', border:'#a7f3d0', size:9 }, { label:'OSM', color:'#6366f1', bg:'#f5f3ff', border:'#ddd6fe', size:7 }].map(({ label, color, bg, border, size }) => (
            <div key={label} style={{ display:'flex', alignItems:'center', gap:4, padding:'3px 8px', background:bg, border:`1px solid ${border}`, borderRadius:6 }}>
              <div style={{ width:size, height:size, borderRadius:'50%', background:color, border:'2px solid white', boxShadow:`0 0 0 1.5px ${color}` }} />
              <span style={{ fontFamily:"'DM Mono',monospace", fontSize:8, fontWeight:700, color }}>{label}</span>
            </div>
          ))}
        </div>
        {[
          { color:'#ef4444', label:'Hospital / Clínica' }, { color:'#6366f1', label:'Escuela / Universidad' },
          { color:'#06b6d4', label:'Aeropuerto' }, { color:'#14b8a6', label:'Puerto' },
          { color:'#eab308', label:'Bomberos' }, { color:'#3b82f6', label:'Policía' },
        ].map(({ color, label }) => (
          <div key={label} style={{ display:'flex', alignItems:'center', gap:7, marginBottom:4 }}>
            <div style={{ width:8, height:8, borderRadius:'50%', background:color, flexShrink:0 }} />
            <span style={{ fontFamily:"'DM Sans',sans-serif", fontSize:10, color:C.textSec }}>{label}</span>
          </div>
        ))}
      </div>

      {/* Leyenda Tsunamis */}
      <div style={{ marginTop:14, paddingTop:12, borderTop:`1px solid ${C.border}` }}>
        <div style={{ fontFamily:"'DM Mono',monospace", fontSize:9, color:C.textMuted, textTransform:'uppercase', letterSpacing:'0.12em', marginBottom:8 }}>Altura de ola tsunami</div>
        <div style={{ display:'flex', gap:2, marginBottom:4 }}>
          {TSUNAMI_SCALE.map(({ color }) => <div key={color} style={{ flex:1, height:5, background:color, borderRadius:2 }} />)}
        </div>
        <div style={{ display:'flex', justifyContent:'space-between' }}>
          {TSUNAMI_SCALE.map(({ label }) => (
            <span key={label} style={{ fontFamily:"'DM Mono',monospace", fontSize:7, color:C.textMuted, flex:1, textAlign:'center' }}>{label}</span>
          ))}
        </div>
      </div>
    </div>
  )
}