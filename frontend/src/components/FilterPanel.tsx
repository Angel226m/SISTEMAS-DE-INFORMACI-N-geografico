// ══════════════════════════════════════════════════════════
// FilterPanel.tsx v6.0
// Mejoras v6.0:
// - Filtro fuente_tipo (Oficial / OSM) para infraestructura
// - Badge zona sísmica calculada desde region
// - Resumen activo mejorado con IRC context
// - GPU filter badge mantiene latencia cero para mag/año
// ══════════════════════════════════════════════════════════

import { useState, useEffect } from 'react'
import type { FiltrosSismos, FuenteTipo } from '../types'

const C = {
  text: '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border: '#e2e8f0', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  primary: '#059669', danger: '#dc2626', warning: '#f59e0b',
  amber: '#f59e0b', orange: '#f97316',
}

function Slider({ label, value, min, max, step, color, unit, format, onChange, onCommit }: {
  label: string; value: number; min: number; max: number; step: number
  color: string; unit?: string; format?: (v: number) => string
  onChange: (v: number) => void
  onCommit?: (v: number) => void
}) {
  const pct     = ((value - min) / (max - min)) * 100
  const display = format ? format(value) : `${value}${unit ?? ''}`
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 7 }}>
        <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted }}>{label}</span>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 12, fontWeight: 700, color }}>{display}</span>
      </div>
      <input
        type="range" min={min} max={max} step={step} value={value}
        onChange={e => onChange(Number(e.target.value))}
        onMouseUp={e => onCommit?.(Number((e.target as HTMLInputElement).value))}
        onTouchEnd={e => onCommit?.(Number((e.target as HTMLInputElement).value))}
        style={{
          width: '100%', height: 5, borderRadius: 3,
          appearance: 'none', outline: 'none', cursor: 'pointer',
          background: `linear-gradient(to right, ${color} ${pct}%, rgba(0,0,0,0.08) ${pct}%)`,
        }}
      />
      <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 3 }}>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: 'rgba(0,0,0,0.2)' }}>{min}{unit ?? ''}</span>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: 'rgba(0,0,0,0.2)' }}>{max}{unit ?? ''}</span>
      </div>
    </div>
  )
}

function Chip({ label, active, color, onClick }: {
  label: string; active: boolean; color: string; onClick: () => void
}) {
  return (
    <button onClick={onClick} style={{
      padding: '4px 10px', borderRadius: 99,
      border: `1px solid ${active ? color : C.border}`,
      background: active ? `${color}14` : 'transparent',
      color: active ? color : C.textMuted,
      fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: 700,
      cursor: 'pointer', transition: 'all 0.16s ease', letterSpacing: '0.04em',
    }}>
      {label}
    </button>
  )
}

const RICHTER = [
  { range: '2–3', desc: 'Micro, raramente sentido',    c: '#6ee7b7' },
  { range: '3–4', desc: 'Menor, sentido localmente',   c: '#22c55e' },
  { range: '4–5', desc: 'Ligero, daños menores',       c: '#eab308' },
  { range: '5–6', desc: 'Moderado, daños apreciables', c: '#f97316' },
  { range: '6–7', desc: 'Fuerte, destructivos',        c: '#ef4444' },
  { range: '7+',  desc: 'Severo / Catastrófico',       c: '#7f1d1d' },
]

const PROF_OPTS = [
  { value: undefined              as FiltrosSismos['profundidad'], label: 'Todos',       color: C.textMuted },
  { value: 'superficial' as const,                                  label: 'Superficial', color: '#dc2626'   },
  { value: 'intermedio'  as const,                                  label: 'Intermedio',  color: '#f97316'   },
  { value: 'profundo'    as const,                                  label: 'Profundo',    color: '#0ea5e9'   },
]

const REGIONES = [
  'Ica', 'Lima', 'Arequipa', 'Cusco', 'Ancash', 'Piura',
  'Tacna', 'Puno', 'Moquegua', 'San Martin', 'Junin',
]

// Zona sísmica por región (NTE E.030-2018)
const ZONA_REGION: Record<string, { zona: number; factor: string; color: string }> = {
  'Ica':       { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Lima':      { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Arequipa':  { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Ancash':    { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Tacna':     { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Moquegua':  { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Piura':     { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Cusco':     { zona: 3, factor: '0.35g', color: '#f97316' },
  'San Martin':{ zona: 3, factor: '0.35g', color: '#f97316' },
  'Junin':     { zona: 3, factor: '0.35g', color: '#f97316' },
  'Puno':      { zona: 2, factor: '0.25g', color: '#f59e0b' },
}

interface Props {
  filtros: FiltrosSismos
  onChange: (f: FiltrosSismos) => void
  fuenteTipo?: FuenteTipo
  onFuenteTipoChange?: (ft: FuenteTipo) => void
}

export default function FilterPanel({ filtros, onChange, fuenteTipo = 'todos', onFuenteTipoChange }: Props) {
  const [local, setLocal] = useState(filtros)
  useEffect(() => { setLocal(filtros) }, [filtros])

  const apply = (f: FiltrosSismos) => {
    setLocal(f)
    onChange(f)
  }

  const divider = <div style={{ height: 1, background: C.border, margin: '6px 0 16px' }} />
  const años    = local.year_end - local.year_start
  const leftPct = ((local.year_start - 1900) / (2030 - 1900)) * 100
  const wPct    = (años / (2030 - 1900)) * 100

  const zonaInfo = local.region ? ZONA_REGION[local.region] : null

  return (
    <div>
      <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.18em', marginBottom: 4 }}>
        Filtros de búsqueda
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 5, marginBottom: 16 }}>
        <div style={{ width: 5, height: 5, borderRadius: '50%', background: C.primary, flexShrink: 0 }} />
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.primary }}>
          Mag + año filtran en GPU · sin latencia
        </span>
      </div>

      {/* Magnitud */}
      <Slider label="Magnitud mínima" value={local.mag_min} min={2.5} max={8.5} step={0.1}
        color={C.danger} unit=" Mw"
        onChange={v => apply({ ...local, mag_min: v })} />
      <Slider label="Magnitud máxima" value={local.mag_max} min={2.5} max={9.9} step={0.1}
        color={C.orange} unit=" Mw"
        onChange={v => apply({ ...local, mag_max: Math.max(v, local.mag_min + 0.5) })} />

      {divider}

      {/* Años */}
      <Slider label="Año inicio" value={local.year_start} min={1900} max={2024} step={1}
        color={C.orange}
        onChange={v => apply({ ...local, year_start: Math.min(v, local.year_end - 1) })} />
      <Slider label="Año fin" value={local.year_end} min={1901} max={2030} step={1}
        color={C.warning}
        onChange={v => apply({ ...local, year_end: Math.max(v, local.year_start + 1) })} />

      {divider}

      {/* Profundidad — server-side */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted, marginBottom: 6 }}>
          Tipo de profundidad
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 6 }}>
          <div style={{ width: 5, height: 5, borderRadius: '50%', background: C.warning, flexShrink: 0 }} />
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.warning }}>
            Requiere consulta al servidor
          </span>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
          {PROF_OPTS.map(({ value, label, color }) => (
            <Chip key={label} label={label} active={local.profundidad === value}
              color={color} onClick={() => apply({ ...local, profundidad: value })} />
          ))}
        </div>
      </div>

      {divider}

      {/* Región — server-side */}
      <div style={{ marginBottom: 16 }}>
        <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted, marginBottom: 6 }}>
          Región / Departamento
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginBottom: 6 }}>
          <div style={{ width: 5, height: 5, borderRadius: '50%', background: C.warning, flexShrink: 0 }} />
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.warning }}>
            Requiere consulta al servidor
          </span>
        </div>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
          <Chip key="todas" label="Todas" active={!local.region}
            color={C.primary} onClick={() => apply({ ...local, region: undefined })} />
          {REGIONES.map(r => (
            <Chip key={r} label={r} active={local.region === r}
              color="#6366f1"
              onClick={() => apply({ ...local, region: local.region === r ? undefined : r })} />
          ))}
        </div>

        {/* Zona sísmica de la región seleccionada */}
        {zonaInfo && (
          <div style={{
            marginTop: 10, padding: '8px 10px',
            background: zonaInfo.color + '10',
            border: `1px solid ${zonaInfo.color}30`,
            borderLeft: `3px solid ${zonaInfo.color}`,
            borderRadius: 8,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textSec }}>
                {local.region} — Zona Sísmica
              </span>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 13, fontWeight: 800, color: zonaInfo.color }}>
                Z{zonaInfo.zona}
              </span>
            </div>
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: zonaInfo.color, marginTop: 2 }}>
              Factor Z = {zonaInfo.factor} · NTE E.030-2018
            </div>
          </div>
        )}
      </div>

      {divider}

      {/* Fuente infraestructura — v7.0 */}
      {onFuenteTipoChange && (
        <>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted, marginBottom: 6 }}>
              Fuente infraestructura
            </div>
            <div style={{ display: 'flex', gap: 4 }}>
              {([
                { value: 'todos', label: 'Todos', color: C.textMuted },
                { value: 'oficial', label: 'Oficial', color: C.primary },
                { value: 'osm', label: 'OSM', color: '#6366f1' },
              ] as { value: FuenteTipo; label: string; color: string }[]).map(({ value, label, color }) => (
                <Chip key={value} label={label} active={fuenteTipo === value}
                  color={color} onClick={() => onFuenteTipoChange(value)} />
              ))}
            </div>
          </div>
          {divider}
        </>
      )}

      {/* Resumen visual */}
      <div style={{ background: C.bgSoft, border: `1px solid ${C.border}`, borderRadius: 12, padding: 14 }}>
        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.14em', marginBottom: 10 }}>
          Rango activo
        </div>

        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 7 }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 13, fontWeight: 700, color: C.orange }}>{local.year_start}</span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 11, color: C.textMuted }}>→</span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 13, fontWeight: 700, color: C.warning }}>{local.year_end}</span>
        </div>

        <div style={{ height: 5, background: 'rgba(0,0,0,0.07)', borderRadius: 3, overflow: 'hidden', marginBottom: 7 }}>
          <div style={{
            height: '100%', marginLeft: `${leftPct}%`, width: `${wPct}%`,
            background: 'linear-gradient(90deg,#f97316,#eab308)', borderRadius: 3,
          }} />
        </div>

        <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted }}>
          {años} años · Mag {local.mag_min.toFixed(1)}–{local.mag_max.toFixed(1)} Mw
          {local.profundidad && ` · ${local.profundidad}`}
          {local.region && ` · ${local.region}`}
        </div>

        {/* Escala Richter */}
        <div style={{ marginTop: 12, paddingTop: 10, borderTop: `1px solid ${C.border}` }}>
          {RICHTER.map(({ range, desc, c }) => {
            const thr    = parseFloat(range)
            const active = local.mag_min <= thr
            return (
              <div key={range} style={{ display: 'flex', gap: 8, alignItems: 'center', padding: '2px 0', opacity: active ? 1 : 0.2, transition: 'opacity 0.2s' }}>
                <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: c, fontWeight: 700, width: 28, flexShrink: 0 }}>{range}</span>
                <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textSec }}>{desc}</span>
              </div>
            )
          })}
        </div>

        {/* Reset */}
        <button
          onClick={() => {
            const reset: FiltrosSismos = { mag_min: 3.0, mag_max: 9.5, year_start: 1960, year_end: 2030 }
            setLocal(reset)
            onChange(reset)
          }}
          style={{
            marginTop: 12, width: '100%', padding: '6px 0',
            borderRadius: 7, border: `1px solid ${C.border}`,
            background: 'transparent', color: C.textMuted,
            fontFamily: "'DM Mono',monospace", fontSize: 9,
            cursor: 'pointer', transition: 'all 0.16s',
          }}
          onMouseEnter={e => { (e.currentTarget.style.background = C.bgMuted) }}
          onMouseLeave={e => { (e.currentTarget.style.background = 'transparent') }}
        >
          Restablecer filtros
        </button>
      </div>
    </div>
  )
}