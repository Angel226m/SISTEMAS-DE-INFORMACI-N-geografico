// ══════════════════════════════════════════════════════════
// LayerPanel.tsx v6.0 — Leyendas accesibles (sin rainbow/jet)
// Mejoras según investigación de visualización de riesgo sísmico:
// - Escala secuencial perceptualmente uniforme (verde→rojo)
// - Leyenda de fallas activas vs inactivas
// - Leyenda de tsunamis con altura de ola
// - Leyenda mejorada de profundidad sísmica
// ══════════════════════════════════════════════════════════

import type { CapasActivas } from '../types'

interface Props {
  capas:    CapasActivas
  onChange: (c: CapasActivas) => void
}

const C = {
  text:     '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border:   '#e2e8f0', bg: '#ffffff', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  primary:  '#059669', primaryBg: '#ecfdf5',
}

interface LayerDef {
  key:   keyof CapasActivas
  label: string
  sub:   string
  icon:  string
  color: string
  bg:    string
}

const LAYERS: LayerDef[] = [
  { key: 'sismos',           label: 'Sismos',             sub: 'USGS 1900–hoy · GPU filter',  icon: '●', color: '#dc2626', bg: '#fef2f2' },
  { key: 'heatmap',          label: 'Densidad sísmica',   sub: 'Mapa de calor',                icon: '◉', color: '#f97316', bg: '#fff7ed' },
  { key: 'departamentos',    label: 'Departamentos',      sub: 'GADM/INEI · 25 regiones',      icon: '▦', color: '#7c3aed', bg: '#f5f3ff' },
  { key: 'riesgo_distritos', label: 'Índice de riesgo',   sub: 'Por distrito',                 icon: '◧', color: '#059669', bg: '#ecfdf5' },
  { key: 'fallas',           label: 'Fallas geológicas',  sub: 'INGEMMET/IGP',                 icon: '⌗', color: '#f59e0b', bg: '#fffbeb' },
  { key: 'inundaciones',     label: 'Inundaciones',       sub: 'ANA/CENEPRED',                 icon: '◈', color: '#0ea5e9', bg: '#f0f9ff' },
  { key: 'tsunamis',         label: 'Tsunamis',           sub: 'PREDES/IGP',                   icon: '≋', color: '#06b6d4', bg: '#ecfeff' },
  { key: 'deslizamientos',   label: 'Deslizamientos',     sub: 'CENEPRED/INGEMMET',            icon: '◤', color: '#92400e', bg: '#fef3c7' },
  { key: 'infraestructura',  label: 'Infraestructura',    sub: 'Hospitales, escuelas',          icon: '⊕', color: '#6366f1', bg: '#eef2ff' },
  { key: 'estaciones',       label: 'Estaciones',         sub: 'IGP/SENAMHI/ANA',              icon: '◎', color: '#10b981', bg: '#f0fdf4' },
  { key: 'extrusion_3d',     label: 'Extrusión 3D',       sub: 'Modo 3D requerido',            icon: '⬡', color: '#ec4899', bg: '#fdf2f8' },
]

// Escala de riesgo secuencial — perceptualmente uniforme (verde→rojo)
// Conforme a investigación: Crameri et al. 2020, Dallo et al. 2024
const RISK_SCALE = [
  { level: 1, label: 'Muy bajo', color: '#059669' },
  { level: 2, label: 'Bajo',     color: '#10b981' },
  { level: 3, label: 'Moderado', color: '#f59e0b' },
  { level: 4, label: 'Alto',     color: '#f97316' },
  { level: 5, label: 'Muy alto', color: '#dc2626' },
]

const DEPTH_ITEMS = [
  { color: '#dc2626', label: 'Superficial', sub: '< 30 km',   desc: 'Mayor daño superficial' },
  { color: '#f97316', label: 'Intermedio',  sub: '30–70 km',  desc: 'Daño moderado' },
  { color: '#0ea5e9', label: 'Profundo',    sub: '> 70 km',   desc: 'Menor intensidad' },
]

const TSUNAMI_SCALE = [
  { color: '#06b6d4', label: '< 1 m',  desc: 'Bajo' },
  { color: '#0891b2', label: '1–3 m',  desc: 'Moderado' },
  { color: '#0e7490', label: '3–10 m', desc: 'Alto' },
  { color: '#164e63', label: '> 10 m', desc: 'Catastrófico' },
]

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

      {/* Lista */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 3 }}>
        {LAYERS.map(({ key, label, sub, icon, color, bg }) => {
          const on = capas[key]
          return (
            <button
              key={key}
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
                fontSize: 13, color: on ? color : C.textMuted,
                transition: 'all 0.18s',
              }}>{icon}</div>

              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{
                  fontFamily: "'DM Sans',sans-serif", fontSize: 12, fontWeight: 600,
                  color: on ? C.text : C.textMuted,
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                }}>{label}</div>
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

      {/* ── Leyenda: Nivel de riesgo ─────────────────────── */}
      <div style={{ marginTop: 18, paddingTop: 14, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Nivel de riesgo
        </div>
        {/* Barra de color — escala secuencial uniforme (sin rainbow) */}
        <div style={{ display: 'flex', gap: 2, marginBottom: 6 }}>
          {RISK_SCALE.map(({ level, color }) => (
            <div key={level} style={{ flex: 1, textAlign: 'center' }}>
              <div style={{ height: 7, background: color, borderRadius: 2 }} />
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
          {RISK_SCALE.map(({ level, label, color }) => (
            <div key={level} style={{ flex: 1, textAlign: 'center' }}>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, color, display: 'block' }}>{level}</span>
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
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
          { color: '#dc2626', label: 'Falla activa',    desc: 'Neotectónica verificada' },
          { color: '#9ca3af', label: 'Falla inactiva',  desc: 'Sin actividad reciente'  },
        ].map(({ color, label, desc }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 5 }}>
            <div style={{ width: 18, height: 2.5, background: color, borderRadius: 2, flexShrink: 0 }} />
            <div>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textSec, display: 'block' }}>{label}</span>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 9, color: C.textMuted }}>{desc}</span>
            </div>
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

      {/* ── Leyenda: Infraestructura crítica ─────────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Infraestructura crítica
        </div>
        {[
          { color: '#ef4444', label: 'Hospital/Clínica'    },
          { color: '#6366f1', label: 'Escuela/Universidad' },
          { color: '#06b6d4', label: 'Aeropuerto'          },
          { color: '#14b8a6', label: 'Puerto'              },
          { color: '#eab308', label: 'Bomberos'            },
        ].map(({ color, label }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0 }} />
            <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textSec }}>{label}</span>
          </div>
        ))}
      </div>

      {/* ── Leyenda: Deslizamientos ──────────────────────── */}
      <div style={{ marginTop: 14, paddingTop: 12, borderTop: `1px solid ${C.border}` }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8 }}>
          Deslizamientos
        </div>
        {[
          { color: '#92400e', label: 'Deslizamiento' },
          { color: '#b45309', label: 'Huayco'        },
          { color: '#d97706', label: 'Derrumbe'      },
          { color: '#f59e0b', label: 'Flujo detrítico'},
        ].map(({ color, label }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 7, marginBottom: 4 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: color, flexShrink: 0 }} />
            <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textSec }}>{label}</span>
          </div>
        ))}
      </div>
    </div>
  )
}