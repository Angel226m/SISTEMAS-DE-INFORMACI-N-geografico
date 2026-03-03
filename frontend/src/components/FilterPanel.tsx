// == FilterPanel.tsx =======================================
import type { FiltrosSismos } from '../types'

const C = {
  text: '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border: '#e2e8f0', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  primary: '#059669', danger: '#dc2626', warning: '#f59e0b',
}

interface Props { filtros: FiltrosSismos; onChange: (f: FiltrosSismos) => void }

// ── Slider genérico ────────────────────────────────────────
function Slider({
  label, value, min, max, step, color, unit, format, onChange,
}: {
  label: string; value: number; min: number; max: number; step: number
  color: string; unit?: string; format?: (v: number) => string
  onChange: (v: number) => void
}) {
  const pct = ((value - min) / (max - min)) * 100
  const display = format ? format(value) : `${value}${unit ?? ''}`
  return (
    <div style={{ marginBottom: 18 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
        <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted }}>
          {label}
        </span>
        <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 12, fontWeight: 700, color }}>
          {display}
        </span>
      </div>
      <input
        type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(Number(e.target.value))}
        style={{
          width: '100%', height: 5, borderRadius: 3,
          appearance: 'none', outline: 'none', cursor: 'pointer',
          background: `linear-gradient(to right, ${color} ${pct}%, rgba(0,0,0,0.08) ${pct}%)`,
        }}
      />
      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 3 }}>
        <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 8, color: 'rgba(0,0,0,0.2)' }}>
          {min}{unit ?? ''}
        </span>
        <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 8, color: 'rgba(0,0,0,0.2)' }}>
          {max}{unit ?? ''}
        </span>
      </div>
    </div>
  )
}

// ── Chip de selección ──────────────────────────────────────
function Chip({
  label, active, color, onClick,
}: { label: string; active: boolean; color: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: '4px 10px', borderRadius: 99, border: `1px solid ${active ? color : C.border}`,
        background: active ? `${color}15` : 'transparent',
        color: active ? color : C.textMuted,
        fontFamily: "'JetBrains Mono',monospace", fontSize: 9, fontWeight: 700,
        cursor: 'pointer', transition: 'all 0.18s ease',
        letterSpacing: '0.05em',
      }}
    >
      {label}
    </button>
  )
}

// ── Escala Richter ─────────────────────────────────────────
const RICHTER = [
  { range: '2–3', desc: 'Micro, raramente sentido',    c: '#6ee7b7' },
  { range: '3–4', desc: 'Menor, sentido localmente',   c: '#22c55e' },
  { range: '4–5', desc: 'Ligero, daños muy menores',   c: '#eab308' },
  { range: '5–6', desc: 'Moderado, daños apreciables', c: '#f97316' },
  { range: '6–7', desc: 'Fuerte, daños destructivos',  c: '#ef4444' },
  { range: '7+',  desc: 'Severo / Catastrófico',       c: '#7f1d1d' },
]

const PROF_OPTS: Array<{ value: FiltrosSismos['profundidad']; label: string; color: string }> = [
  { value: undefined,       label: 'Todos',       color: C.textMuted },
  { value: 'superficial',   label: 'Superficial', color: '#dc2626'   },
  { value: 'intermedio',    label: 'Intermedio',  color: '#f97316'   },
  { value: 'profundo',      label: 'Profundo',    color: '#0ea5e9'   },
]

// ── Componente principal ───────────────────────────────────
export default function FilterPanel({ filtros, onChange }: Props) {
  const años     = filtros.year_end - filtros.year_start
  const totalPos = 2030 - 1900
  const leftPct  = ((filtros.year_start - 1900) / totalPos) * 100
  const widthPct = (años / totalPos) * 100

  const divider = <div style={{ height: 1, background: C.border, margin: '6px 0 18px' }} />

  return (
    <div>
      <div style={{
        fontFamily: "'JetBrains Mono',monospace", fontSize: 9,
        color: C.textMuted, textTransform: 'uppercase',
        letterSpacing: '0.2em', marginBottom: 20,
      }}>
        Filtros de búsqueda
      </div>

      {/* Magnitud */}
      <Slider
        label="Magnitud mínima"
        value={filtros.mag_min} min={2.5} max={8.5} step={0.1}
        color={C.danger} unit=" Mw"
        onChange={v => onChange({ ...filtros, mag_min: v })}
      />

      {divider}

      {/* Año inicio */}
      <Slider
        label="Año inicio"
        value={filtros.year_start} min={1900} max={2029} step={1}
        color="#f97316"
        onChange={v => onChange({ ...filtros, year_start: Math.min(v, filtros.year_end - 1) })}
      />

      {/* Año fin */}
      <Slider
        label="Año fin"
        value={filtros.year_end} min={1901} max={2030} step={1}
        color={C.warning}
        onChange={v => onChange({ ...filtros, year_end: Math.max(v, filtros.year_start + 1) })}
      />

      {divider}

      {/* Profundidad */}
      <div style={{ marginBottom: 18 }}>
        <div style={{
          fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted, marginBottom: 8,
        }}>
          Tipo de profundidad
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
          {PROF_OPTS.map(({ value, label, color }) => (
            <Chip
              key={label}
              label={label}
              active={filtros.profundidad === value}
              color={color}
              onClick={() => onChange({ ...filtros, profundidad: value })}
            />
          ))}
        </div>
      </div>

      {/* Resumen visual */}
      <div style={{
        background: C.bgSoft, border: `1px solid ${C.border}`,
        borderRadius: 12, padding: 14, marginTop: 4,
      }}>
        <div style={{
          fontFamily: "'JetBrains Mono',monospace", fontSize: 9,
          color: C.textMuted, textTransform: 'uppercase',
          letterSpacing: '0.15em', marginBottom: 12,
        }}>
          Rango activo
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
          <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 13, fontWeight: 700, color: '#f97316' }}>
            {filtros.year_start}
          </span>
          <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, color: C.textMuted }}>→</span>
          <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 13, fontWeight: 700, color: C.warning }}>
            {filtros.year_end}
          </span>
        </div>

        {/* Barra de rango */}
        <div style={{ height: 5, background: 'rgba(0,0,0,0.07)', borderRadius: 3, overflow: 'hidden', marginBottom: 8 }}>
          <div style={{
            height: '100%', marginLeft: `${leftPct}%`, width: `${widthPct}%`,
            background: 'linear-gradient(90deg, #f97316, #eab308)', borderRadius: 3,
          }} />
        </div>

        <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted }}>
          {años} años · Mag ≥ {filtros.mag_min.toFixed(1)} Mw
          {filtros.profundidad && ` · ${filtros.profundidad}`}
        </div>

        {/* Escala Richter */}
        <div style={{ marginTop: 12, paddingTop: 10, borderTop: `1px solid ${C.border}` }}>
          {RICHTER.map(({ range, desc, c }) => {
            const threshold = parseFloat(range)
            const active = filtros.mag_min <= threshold
            return (
              <div
                key={range}
                style={{
                  display: 'flex', gap: 8, alignItems: 'center', padding: '2px 0',
                  opacity: active ? 1 : 0.22, transition: 'opacity 0.2s',
                }}
              >
                <span style={{
                  fontFamily: "'JetBrains Mono',monospace", fontSize: 9,
                  color: c, fontWeight: 700, width: 30, flexShrink: 0,
                }}>
                  {range}
                </span>
                <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textSec }}>
                  {desc}
                </span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}