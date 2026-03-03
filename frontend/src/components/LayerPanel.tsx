// == LayerPanel.tsx -- Capas con tema claro profesional ==
import type { CapasActivas } from '../types'

interface Props { capas: CapasActivas; onChange: (c: CapasActivas) => void }

const C = {
  primary: '#059669', primaryLt: '#10b981', primaryBg: '#ecfdf5',
  secondary: '#0ea5e9', secondaryBg: '#f0f9ff',
  accent: '#6366f1',
  text: '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border: '#e2e8f0', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
}

const CAPAS = [
  { key: 'sismos'           as const, label: 'Sismos historicos', icon: '\u25CF', sub: '1960 - hoy',          color: '#dc2626', bg: '#fef2f2' },
  { key: 'heatmap'          as const, label: 'Densidad sismica',  icon: '\u25C9', sub: 'Heatmap intensidad',  color: '#f97316', bg: '#fff7ed' },
  { key: 'fallas'           as const, label: 'Fallas geologicas', icon: '\u2307', sub: 'INGEMMET',            color: '#f59e0b', bg: '#fffbeb' },
  { key: 'inundaciones'     as const, label: 'Zonas inundables',  icon: '\u25C8', sub: 'ANA Peru',            color: '#0ea5e9', bg: '#f0f9ff' },
  { key: 'riesgo_distritos' as const, label: 'Indice de riesgo',  icon: '\u25A6', sub: 'Por distrito',        color: '#059669', bg: '#ecfdf5' },
  { key: 'infraestructura'  as const, label: 'Infraestructura',   icon: '\u2295', sub: 'Hospitales, escuelas', color: '#6366f1', bg: '#eef2ff' },
  { key: 'extrusion_3d'     as const, label: 'Extrusion 3D',      icon: '\u2B21', sub: 'Solo en modo 3D',    color: '#ec4899', bg: '#fdf2f8' },
]

export default function LayerPanel({ capas, onChange }: Props) {
  const active = Object.values(capas).filter(Boolean).length

  return (
    <div>
      {/* Header con contador */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <span style={{
          fontFamily: "'Inter', sans-serif",
          fontSize: 10, fontWeight: 600, color: C.textMuted,
          textTransform: 'uppercase', letterSpacing: '0.12em',
        }}>Capas del mapa</span>
        <span style={{
          fontFamily: "'JetBrains Mono', monospace",
          fontSize: 9, fontWeight: 600, color: C.primary,
          background: C.primaryBg,
          padding: '2px 8px', borderRadius: 99,
        }}>{active} activas</span>
      </div>

      {/* Lista de capas */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
        {CAPAS.map(({ key, label, icon, sub, color, bg }) => {
          const on = capas[key]
          return (
            <button
              key={key}
              onClick={() => onChange({ ...capas, [key]: !on })}
              style={{
                width: '100%', display: 'flex', alignItems: 'center', gap: 10,
                padding: '9px 10px',
                background: on ? bg : 'transparent',
                border: `1px solid ${on ? color + '30' : 'transparent'}`,
                borderRadius: 10, cursor: 'pointer',
                transition: 'all 0.2s ease',
                textAlign: 'left',
              }}
            >
              {/* Icono coloreado */}
              <div style={{
                width: 28, height: 28, borderRadius: 7,
                background: on ? bg : C.bgMuted,
                border: `1px solid ${on ? color + '40' : C.border}`,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                fontSize: 13, color: on ? color : C.textMuted,
                flexShrink: 0, transition: 'all 0.2s',
              }}>{icon}</div>

              {/* Texto */}
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{
                  fontFamily: "'Inter', sans-serif",
                  fontSize: 12, fontWeight: 600,
                  color: on ? C.text : C.textMuted,
                  overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                  transition: 'color 0.2s',
                }}>{label}</div>
                <div style={{
                  fontFamily: "'JetBrains Mono', monospace",
                  fontSize: 9, color: C.textMuted,
                  marginTop: 1,
                }}>{sub}</div>
              </div>

              {/* Toggle */}
              <div style={{
                width: 32, height: 18, borderRadius: 9, flexShrink: 0,
                background: on ? color : C.bgMuted,
                border: `1px solid ${on ? color : C.border}`,
                position: 'relative', transition: 'all 0.25s ease',
              }}>
                <div style={{
                  position: 'absolute', top: 2,
                  left: on ? 16 : 2,
                  width: 12, height: 12, borderRadius: '50%',
                  background: 'white',
                  boxShadow: '0 1px 3px rgba(0,0,0,0.2)',
                  transition: 'left 0.25s cubic-bezier(0.4,0,0.2,1)',
                }} />
              </div>
            </button>
          )
        })}
      </div>

      {/* Leyenda de riesgo */}
      <div style={{ marginTop: 20, paddingTop: 16, borderTop: `1px solid ${C.border}` }}>
        <div style={{
          fontFamily: "'Inter', sans-serif",
          fontSize: 10, fontWeight: 600, color: C.textMuted,
          textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 10,
        }}>Nivel de riesgo</div>
        <div style={{ display: 'flex', gap: 3 }}>
          {['#059669','#10b981','#f59e0b','#f97316','#dc2626'].map((c, i) => (
            <div key={i} style={{ flex: 1, textAlign: 'center' }}>
              <div style={{ height: 6, background: c, borderRadius: 3, marginBottom: 4 }} />
              <span style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: 8, color: C.textMuted, fontWeight: 600,
              }}>{i + 1}</span>
            </div>
          ))}
        </div>
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 2 }}>
          <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 8, color: C.textMuted }}>Bajo</span>
          <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 8, color: C.textMuted }}>Muy alto</span>
        </div>
      </div>

      {/* Leyenda profundidad */}
      <div style={{ marginTop: 16, paddingTop: 14, borderTop: `1px solid ${C.border}` }}>
        <div style={{
          fontFamily: "'Inter', sans-serif",
          fontSize: 10, fontWeight: 600, color: C.textMuted,
          textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 8,
        }}>Profundidad sismica</div>
        {[
          { color: '#dc2626', label: 'Superficial', sub: '< 30 km' },
          { color: '#f97316', label: 'Intermedio',  sub: '30-70 km' },
          { color: '#0ea5e9', label: 'Profundo',    sub: '> 70 km' },
        ].map(({ color, label, sub }) => (
          <div key={label} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 5 }}>
            <div style={{ width: 8, height: 8, borderRadius: '50%', background: color, flexShrink: 0 }} />
            <span style={{ fontFamily: "'Inter', sans-serif", fontSize: 11, color: C.textSec }}>{label}</span>
            <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 9, color: C.textMuted, marginLeft: 'auto' }}>{sub}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
