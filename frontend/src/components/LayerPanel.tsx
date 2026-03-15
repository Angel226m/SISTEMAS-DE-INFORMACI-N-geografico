// ══════════════════════════════════════════════════════════
// LayerPanel.tsx v8.0  ENTERPRISE
// 🆕 Capa: precipitaciones (zonas climáticas · FEN index)
// 🆕 Leyenda FEN: escala de multiplicador por zona
// 🆕 Badge v8.0 en precipitaciones y IRC
// ✅ Todas las leyendas v7.5 mantenidas
// ══════════════════════════════════════════════════════════

import type { CapasActivas } from '../types'

interface Props {
  capas:    CapasActivas
  onChange: (c: CapasActivas) => void
}

const C = {
  text:    '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border:  '#e2e8f0', bg: '#ffffff', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  primary: '#059669', primaryBg: '#ecfdf5',
  amber:   '#f59e0b', orange: '#f97316', danger: '#dc2626',
  teal:    '#0891b2', cyan: '#06b6d4',
}

interface LayerDef {
  key:    keyof CapasActivas
  label:  string
  sub:    string
  icon:   string
  color:  string
  bg:     string
  badge?: string
}

const LAYERS: LayerDef[] = [
  { key: 'sismos',             label: 'Sismos',             sub: 'USGS 1900–hoy · GPU filter',    icon: '●', color: '#dc2626', bg: '#fef2f2', badge: 'GPU' },
  { key: 'heatmap',            label: 'Densidad sísmica',   sub: 'Mapa de calor ponderado',        icon: '◉', color: '#f97316', bg: '#fff7ed' },
  { key: 'departamentos',      label: 'Departamentos',      sub: 'Zona sísmica NTE E.030-2018',    icon: '▦', color: '#7c3aed', bg: '#f5f3ff', badge: 'v7.5' },
  { key: 'riesgo_distritos',   label: 'Riesgo distritos',   sub: 'Índice multi-variable',          icon: '▧', color: '#059669', bg: '#ecfdf5' },
  { key: 'riesgo_construccion',label: 'IRC — Construcción', sub: 'CENEPRED · NTE E.030/E.031',    icon: '⬡', color: '#f59e0b', bg: '#fffbeb', badge: 'v8.0' },
  { key: 'precipitaciones',    label: 'Precipitaciones',    sub: 'SENAMHI/CHIRPS · índice FEN',   icon: '◈', color: '#0891b2', bg: '#ecfeff', badge: 'v8.0' },
  { key: 'fallas',             label: 'Fallas geológicas',  sub: 'Audin et al. 2008 + IGP 2021',  icon: '⌗', color: '#f59e0b', bg: '#fffbeb' },
  { key: 'inundaciones',       label: 'Inundaciones',       sub: 'ANA / CENEPRED',                icon: '≈', color: '#0ea5e9', bg: '#f0f9ff' },
  { key: 'tsunamis',           label: 'Tsunamis',           sub: 'PREDES / IGP / INDECI',         icon: '≋', color: '#06b6d4', bg: '#ecfeff' },
  { key: 'deslizamientos',     label: 'Deslizamientos',     sub: 'CENEPRED / INGEMMET',           icon: '◤', color: '#92400e', bg: '#fef3c7' },
  { key: 'infraestructura',    label: 'Infraestructura',    sub: 'Oficial + OSM · 60k puntos',    icon: '⊕', color: '#6366f1', bg: '#eef2ff', badge: 'v7.5' },
  { key: 'estaciones',         label: 'Estaciones',         sub: 'IGP / SENAMHI / ANA / DHN',     icon: '◎', color: '#10b981', bg: '#f0fdf4' },
  { key: 'extrusion_3d',       label: 'Extrusión 3D',       sub: 'Modo 3D requerido',              icon: '⬡', color: '#ec4899', bg: '#fdf2f8' },
]

const ZONA_SISMICA = [
  { zona: 4, factor: '0.45g', label: 'Zona 4 — Muy alto', color: '#dc2626',
    deptos: 'Tumbes, Piura, Lambayeque, La Libertad, Ancash, Lima, Callao, Ica, Arequipa, Moquegua, Tacna' },
  { zona: 3, factor: '0.35g', label: 'Zona 3 — Alto',     color: '#f97316',
    deptos: 'Cajamarca, San Martín, Huancavelica, Junín, Pasco, Cusco' },
  { zona: 2, factor: '0.25g', label: 'Zona 2 — Moderado', color: '#f59e0b',
    deptos: 'Amazonas, Huánuco, Ayacucho, Apurímac, Puno, Ucayali' },
  { zona: 1, factor: '0.10g', label: 'Zona 1 — Bajo',     color: '#059669',
    deptos: 'Loreto, Madre de Dios' },
]

const RISK_SCALE = [
  { level: 1, label: 'Muy bajo', color: '#059669' },
  { level: 2, label: 'Bajo',     color: '#10b981' },
  { level: 3, label: 'Moderado', color: '#f59e0b' },
  { level: 4, label: 'Alto',     color: '#f97316' },
  { level: 5, label: 'Muy alto', color: '#dc2626' },
]

const DEPTH_ITEMS = [
  { color: '#dc2626', label: 'Superficial', sub: '< 30 km',  desc: 'Mayor daño en superficie' },
  { color: '#f97316', label: 'Intermedio',  sub: '30–70 km', desc: 'Daño moderado' },
  { color: '#0ea5e9', label: 'Profundo',    sub: '> 70 km',  desc: 'Menor intensidad' },
]

const TSUNAMI_SCALE = [
  { color: '#06b6d4', label: '< 1 m',  desc: 'Bajo' },
  { color: '#0891b2', label: '1–3 m',  desc: 'Moderado' },
  { color: '#0e7490', label: '3–10 m', desc: 'Alto' },
  { color: '#164e63', label: '> 10 m', desc: 'Catastrófico' },
]

/** Escala de índice FEN para coloreado de precipitaciones */
const FEN_SCALE = [
  { range: '< 0.9', label: 'Sequía en FEN',           color: '#4ade80', desc: 'Altiplano / Puna sur' },
  { range: '0.9–1.3', label: 'Sin cambio',             color: '#94a3b8', desc: 'Amazonia / Sierra media' },
  { range: '1.3–2.0', label: 'Amplificación moderada', color: '#f59e0b', desc: 'Ceja de selva / Sierra norte' },
  { range: '2.0–3.5', label: 'Amplificación alta',     color: '#f97316', desc: 'Costa central / Lambayeque' },
  { range: '> 3.5',   label: 'Catastrófico en FEN',    color: '#dc2626', desc: 'Costa norte: Piura / Tumbes' },
]

const TIPO_PRECIP_COLORS: Record<string, string> = {
  muy_alta: '#0ea5e9',
  alta:     '#38bdf8',
  moderada: '#7dd3fc',
  baja:     '#f59e0b',
  muy_baja: '#f97316',
}

export default function LayerPanel({ capas, onChange }: Props) {
  const active = Object.values(capas).filter(Boolean).length

  return (
    <div>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 14 }}>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.15em' }}>
          Capas del mapa
        </span>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: 600, color: C.primary, background: C.primaryBg, padding: '2px 8px', borderRadius: 99 }}>
          {active} activas
        </span>
      </div>

      {/* Lista de capas */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
        {LAYERS.map(({ key, label, sub, icon, color, bg, badge }) => {
          const on = capas[key]
          return (
            <button key={key}
              onClick={() => onChange({ ...capas, [key]: !on })}
              style={{
                width: '100%', display: 'flex', alignItems: 'center', gap: 9,
                padding: '8px 10px',
                background: on ? bg : 'transparent',
                border: `1px solid ${on ? color + '28' : 'transparent'}`,
                borderRadius: 10, cursor: 'pointer',
                transition: 'all 0.18s ease', textAlign: 'left',
              }}
            >
              <div style={{
                width: 28, height: 28, borderRadius: 7, flexShrink: 0,
                background: on ? bg : C.bgMuted,
                border: `1px solid ${on ? color + '38' : C.border}`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 13, color: on ? color : C.textMuted, transition: 'all 0.18s',
              }}>{icon}</div>

              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                  <span style={{
                    fontFamily: "'DM Sans',sans-serif", fontSize: 12, fontWeight: 600,
                    color: on ? C.text : C.textMuted,
                    overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                  }}>{label}</span>
                  {badge && (
                    <span style={{
                      fontFamily: "'DM Mono',monospace", fontSize: 7, fontWeight: 700,
                      color:   badge === 'v8.0' ? C.teal   : badge === 'v7.5' ? C.amber : badge === 'GPU' ? '#6366f1' : C.textMuted,
                      background: badge === 'v8.0' ? '#ecfeff' : badge === 'v7.5' ? '#fffbeb' : badge === 'GPU' ? '#eef2ff' : C.bgMuted,
                      border: `1px solid ${badge === 'v8.0' ? '#a5f3fc' : badge === 'v7.5' ? '#fde68a' : badge === 'GPU' ? '#c7d2fe' : C.border}`,
                      padding: '1px 4px', borderRadius: 3, letterSpacing: '0.04em', flexShrink: 0,
                    }}>{badge}</span>
                  )}
                </div>
                <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, marginTop: 1 }}>
                  {sub}
                </div>
              </div>

              <div style={{
                width: 32, height: 17, borderRadius: 9, flexShrink: 0,
                background: on ? color : C.bgMuted,
                border: `1px solid ${on ? color : C.border}`,
                position: 'relative', transition: 'all 0.22s ease',
              }}>
                <div style={{
                  position: 'absolute', top: 2,
                  left: on ? 15 : 2,
                  width: 11, height: 11, borderRadius: '50%',
                  background: 'white', boxShadow: '0 1px 3px rgba(0,0,0,0.2)',
                  transition: 'left 0.22s cubic-bezier(0.4,0,0.2,1)',
                }} />
              </div>
            </button>
          )
        })}
      </div>

      {/* ── 🆕 v8.0: Leyenda Precipitaciones + FEN ───────── */}
      <div style={{ marginTop: 18, paddingTop: 14, borderTop: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em' }}>
            Precipitaciones · Índice FEN
          </span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, fontWeight: 700, color: C.teal, background: '#ecfeff', border: '1px solid #a5f3fc', padding: '1px 4px', borderRadius: 3 }}>
            SENAMHI/CHIRPS
          </span>
        </div>
        {/* Escala indice_fen */}
        {FEN_SCALE.map(({ range, label, color, desc }) => (
          <div key={range} style={{ display: 'flex', gap: 8, marginBottom: 6, alignItems: 'flex-start' }}>
            <div style={{
              width: 28, height: 16, borderRadius: 4, flexShrink: 0, marginTop: 1,
              background: color + '25', border: `2px solid ${color}80`,
            }} />
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, fontWeight: 600, color: C.textSec }}>{label}</span>
                <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color }}>{range}</span>
              </div>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 9, color: C.textMuted }}>{desc}</span>
            </div>
          </div>
        ))}

        {/* Tipo climático chips */}
        <div style={{ marginTop: 8, fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.textMuted, marginBottom: 4 }}>
          Tipo climático
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
          {Object.entries(TIPO_PRECIP_COLORS).map(([tipo, color]) => (
            <div key={tipo} style={{ display: 'flex', alignItems: 'center', gap: 3, padding: '2px 7px', background: color + '15', border: `1px solid ${color}40`, borderRadius: 99 }}>
              <div style={{ width: 5, height: 5, borderRadius: '50%', background: color }} />
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7.5, color: C.textSec }}>
                {tipo.replace('_', ' ')}
              </span>
            </div>
          ))}
        </div>

        {/* Nota FEN */}
        <div style={{ marginTop: 8, padding: '5px 8px', background: '#fef3c7', border: '1px solid #fde68a', borderRadius: 6 }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7.5, color: '#92400e', lineHeight: 1.5 }}>
            FEN 1997/98 (ONI=2.4): Piura recibió<br />
            3 500 mm en 3 meses (×4.5 la media)
          </span>
        </div>
      </div>

      {/* ── Leyenda: Zona Sísmica NTE E.030-2018 ────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em' }}>
            Zona Sísmica · NTE E.030
          </span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, fontWeight: 700, color: C.amber, background: '#fffbeb', border: '1px solid #fde68a', padding: '1px 4px', borderRadius: 3 }}>
            DS N°003-2016
          </span>
        </div>
        {ZONA_SISMICA.map(({ zona, factor, label, color, deptos }) => (
          <div key={zona} style={{ display: 'flex', gap: 8, marginBottom: 7, alignItems: 'flex-start' }}>
            <div style={{
              width: 26, height: 17, borderRadius: 4, flexShrink: 0, marginTop: 1,
              background: color + '20', border: `2px solid ${color}80`,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, fontWeight: 800, color }}>{zona}</span>
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10.5, fontWeight: 600, color: C.textSec }}>{label}</span>
                <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: 700, color }}>{factor}</span>
              </div>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 9, color: C.textMuted, lineHeight: 1.3, display: 'block' }}>{deptos}</span>
            </div>
          </div>
        ))}
      </div>

      {/* ── Leyenda: IRC — Riesgo de Construcción v8.0 ────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 6 }}>
          Índice Riesgo Construcción
        </div>
        <div style={{ background: '#fffbeb', border: '1px solid #fde68a', borderRadius: 7, padding: '6px 8px', marginBottom: 8 }}>
          <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: '#92400e', lineHeight: 1.5 }}>
            0.40×Sísmico + 0.25×Inundación×<span style={{ color: C.teal }}>FEN</span><br />
            + 0.20×Desliz. + 0.10×Tsunami + 0.05×Fallas
          </div>
        </div>
        <div style={{ display: 'flex', gap: 2, marginBottom: 5 }}>
          {RISK_SCALE.map(({ level, color }) => (
            <div key={level} style={{ flex: 1, height: 7, background: color, borderRadius: 2 }} />
          ))}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          {RISK_SCALE.map(({ level, color }) => (
            <div key={level} style={{ flex: 1, textAlign: 'center' }}>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, color, display: 'block' }}>{level}</span>
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 2 }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.textMuted }}>Muy bajo</span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.textMuted }}>Muy alto</span>
        </div>
      </div>

      {/* ── Leyenda: Profundidad sísmica ─────────────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Profundidad sísmica
        </div>
        {DEPTH_ITEMS.map(({ color, label, sub, desc }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 6 }}>
            <div style={{ width: 9, height: 9, borderRadius: '50%', background: color, flexShrink: 0, boxShadow: `0 0 0 2px ${color}30` }} />
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
                <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textSec }}>{label}</span>
                <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.textMuted }}>{sub}</span>
              </div>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 9, color: C.textMuted }}>{desc}</span>
            </div>
          </div>
        ))}
      </div>

      {/* ── Leyenda: Fallas geológicas ───────────────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Fallas geológicas
        </div>
        {[
          { color: '#dc2626', label: 'Activa (neotectónica)' },
          { color: '#9ca3af', label: 'Inactiva' },
        ].map(({ color, label }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 5 }}>
            <div style={{ width: 18, height: 2.5, background: color, borderRadius: 2, flexShrink: 0 }} />
            <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textSec }}>{label}</span>
          </div>
        ))}
      </div>

      {/* ── Leyenda: Infraestructura + fuente_tipo ───────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Infraestructura crítica
        </div>
        <div style={{ display: 'flex', gap: 6, marginBottom: 10 }}>
          {[
            { label: 'Oficial', color: '#059669', bg: '#ecfdf5', border: '#a7f3d0', size: 9 },
            { label: 'OSM',     color: '#6366f1', bg: '#f5f3ff', border: '#ddd6fe', size: 7 },
          ].map(({ label, color, bg, border, size }) => (
            <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 4, padding: '3px 8px', background: bg, border: `1px solid ${border}`, borderRadius: 6 }}>
              <div style={{ width: size, height: size, borderRadius: '50%', background: color, border: '2px solid white', boxShadow: `0 0 0 1.5px ${color}` }} />
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, fontWeight: 700, color }}>{label}</span>
            </div>
          ))}
        </div>
        {[
          { color: '#ef4444', label: 'Hospital / Clínica'    },
          { color: '#6366f1', label: 'Escuela / Universidad' },
          { color: '#06b6d4', label: 'Aeropuerto'            },
          { color: '#14b8a6', label: 'Puerto'                },
          { color: '#eab308', label: 'Bomberos'              },
          { color: '#3b82f6', label: 'Policía'               },
          { color: '#a78bfa', label: 'Albergue CENEPRED'     },
        ].map(({ color, label }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0 }} />
            <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textSec }}>{label}</span>
          </div>
        ))}
      </div>

      {/* ── Leyenda: Tsunamis ────────────────────────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Altura de ola tsunami
        </div>
        <div style={{ display: 'flex', gap: 2, marginBottom: 4 }}>
          {TSUNAMI_SCALE.map(({ color }) => (
            <div key={color} style={{ flex: 1, height: 5, background: color, borderRadius: 2 }} />
          ))}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          {TSUNAMI_SCALE.map(({ label }) => (
            <span key={label} style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, color: C.textMuted, flex: 1, textAlign: 'center' }}>{label}</span>
          ))}
        </div>
      </div>

      {/* ── Leyenda: Suelo NTE E.031-2020 ───────────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8 }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em' }}>
            Clasificación Suelo
          </span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, fontWeight: 700, color: '#0ea5e9', background: '#f0f9ff', border: '1px solid #bae6fd', padding: '1px 4px', borderRadius: 3 }}>
            NTE E.031
          </span>
        </div>
        {[
          { code: 'S1', label: 'Roca / Suelo rígido',   sub: 'Vs30 > 500 m/s',    color: '#059669' },
          { code: 'S2', label: 'Suelo intermedio',       sub: 'Vs30 180–500 m/s',  color: '#f59e0b' },
          { code: 'S3', label: 'Suelo blando',           sub: 'Vs30 < 180 m/s',    color: '#f97316' },
          { code: 'S4', label: 'Condiciones especiales', sub: 'Tsunami/licuefacción',color: '#dc2626' },
        ].map(({ code, label, sub, color }) => (
          <div key={code} style={{ display: 'flex', gap: 8, marginBottom: 7, alignItems: 'flex-start' }}>
            <div style={{
              width: 24, height: 24, borderRadius: 6, flexShrink: 0,
              background: color + '18', border: `2px solid ${color}70`,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: 800, color }}>{code}</span>
            </div>
            <div>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10.5, fontWeight: 600, color: C.textSec, display: 'block' }}>{label}</span>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.textMuted }}>{sub}</span>
            </div>
          </div>
        ))}
        <div style={{ marginTop: 6, padding: '5px 8px', background: '#f0f9ff', borderRadius: 6, border: '1px solid #bae6fd' }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7.5, color: '#0369a1', lineHeight: 1.5 }}>
            Proxy geográfico — análisis definitivo<br />requiere ensayo SPT/CPT o Vs30 (CISMID)
          </span>
        </div>
      </div>
    </div>
  )
}