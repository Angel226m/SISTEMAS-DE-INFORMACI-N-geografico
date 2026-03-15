// ══════════════════════════════════════════════════════════
// FilterPanel.tsx v8.0
// 🆕 Sección FEN/Precipitación: tipo climático + indice_fen
// 🆕 Badge FEN contextual al seleccionar región
// ✅ Todos los filtros v7.x mantenidos
// ══════════════════════════════════════════════════════════

import { useState, useEffect } from 'react'
import type { FiltrosSismos, FuenteTipo, FiltrosPrecipitacion, TipoPrecipitacion } from '../types'

const C = {
  text: '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border: '#e2e8f0', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  primary: '#059669', danger: '#dc2626', warning: '#f59e0b',
  amber: '#f59e0b', orange: '#f97316', teal: '#0891b2', cyan: '#06b6d4',
}

function Slider({ label, value, min, max, step, color, unit, format, onChange }: {
  label: string; value: number; min: number; max: number; step: number
  color: string; unit?: string; format?: (v: number) => string
  onChange: (v: number) => void
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
  { value: undefined as FiltrosSismos['profundidad'], label: 'Todos',       color: C.textMuted },
  { value: 'superficial' as const,                    label: 'Superficial', color: '#dc2626'   },
  { value: 'intermedio'  as const,                    label: 'Intermedio',  color: '#f97316'   },
  { value: 'profundo'    as const,                    label: 'Profundo',    color: '#0ea5e9'   },
]

const REGIONES = [
  'Tumbes','Piura','Lambayeque','La Libertad','Ancash',
  'Lima','Callao','Ica','Arequipa','Moquegua','Tacna',
  'Cajamarca','San Martin','Huancavelica','Junin','Pasco','Cusco',
  'Amazonas','Huanuco','Ayacucho','Apurimac','Puno','Ucayali',
  'Loreto','Madre de Dios',
]

const ZONA_REGION: Record<string, { zona: number; factor: string; color: string }> = {
  'Tumbes':        { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Piura':         { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Lambayeque':    { zona: 4, factor: '0.45g', color: '#dc2626' },
  'La Libertad':   { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Ancash':        { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Lima':          { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Callao':        { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Ica':           { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Arequipa':      { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Moquegua':      { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Tacna':         { zona: 4, factor: '0.45g', color: '#dc2626' },
  'Cajamarca':     { zona: 3, factor: '0.35g', color: '#f97316' },
  'San Martin':    { zona: 3, factor: '0.35g', color: '#f97316' },
  'Huancavelica':  { zona: 3, factor: '0.35g', color: '#f97316' },
  'Junin':         { zona: 3, factor: '0.35g', color: '#f97316' },
  'Pasco':         { zona: 3, factor: '0.35g', color: '#f97316' },
  'Cusco':         { zona: 3, factor: '0.35g', color: '#f97316' },
  'Amazonas':      { zona: 2, factor: '0.25g', color: '#f59e0b' },
  'Huanuco':       { zona: 2, factor: '0.25g', color: '#f59e0b' },
  'Ayacucho':      { zona: 2, factor: '0.25g', color: '#f59e0b' },
  'Apurimac':      { zona: 2, factor: '0.25g', color: '#f59e0b' },
  'Puno':          { zona: 2, factor: '0.25g', color: '#f59e0b' },
  'Ucayali':       { zona: 2, factor: '0.25g', color: '#f59e0b' },
  'Loreto':        { zona: 1, factor: '0.10g', color: '#059669' },
  'Madre de Dios': { zona: 1, factor: '0.10g', color: '#059669' },
}

// 🆕 v8.0: índice FEN contextual por región (aproximación)
const FEN_REGION: Record<string, { indice: number; desc: string; color: string }> = {
  'Piura':        { indice: 4.5, desc: 'Catastrófico en FEN', color: '#dc2626' },
  'Tumbes':       { indice: 4.2, desc: 'Catastrófico en FEN', color: '#dc2626' },
  'Lambayeque':   { indice: 3.2, desc: 'Amplificación alta',  color: '#f97316' },
  'La Libertad':  { indice: 2.8, desc: 'Amplificación alta',  color: '#f97316' },
  'Lima':         { indice: 2.0, desc: 'Amplificación mod.',  color: '#f59e0b' },
  'Ancash':       { indice: 1.4, desc: 'Amplificación mod.',  color: '#f59e0b' },
  'Ica':          { indice: 1.8, desc: 'Amplificación mod.',  color: '#f59e0b' },
  'Arequipa':     { indice: 1.6, desc: 'Amplificación mod.',  color: '#f59e0b' },
  'Puno':         { indice: 0.7, desc: 'Sequía en FEN',       color: '#059669' },
  'Cusco':        { indice: 0.8, desc: 'Reducción en FEN',    color: '#059669' },
  'Loreto':       { indice: 1.0, desc: 'Sin cambio',          color: '#94a3b8' },
  'Madre de Dios':{ indice: 0.95,desc: 'Sin cambio',          color: '#94a3b8' },
}

const TIPO_PRECIP_OPTS: { value: TipoPrecipitacion | undefined; label: string; color: string }[] = [
  { value: undefined,     label: 'Todos',    color: C.textMuted },
  { value: 'muy_alta',    label: 'Muy alta', color: '#0ea5e9'  },
  { value: 'alta',        label: 'Alta',     color: '#38bdf8'  },
  { value: 'moderada',    label: 'Moderada', color: '#7dd3fc'  },
  { value: 'baja',        label: 'Baja',     color: '#f59e0b'  },
  { value: 'muy_baja',    label: 'Muy baja', color: '#f97316'  },
]

interface Props {
  filtros:                FiltrosSismos
  onChange:               (f: FiltrosSismos) => void
  fuenteTipo?:            FuenteTipo
  onFuenteTipoChange?:    (ft: FuenteTipo) => void
  filtrosPrecip?:         FiltrosPrecipitacion
  onFiltrosPrecipChange?: (f: FiltrosPrecipitacion) => void
}

export default function FilterPanel({
  filtros, onChange,
  fuenteTipo = 'todos', onFuenteTipoChange,
  filtrosPrecip, onFiltrosPrecipChange,
}: Props) {
  const [local, setLocal] = useState(filtros)
  const [localPrecip, setLocalPrecip] = useState<FiltrosPrecipitacion>(
    filtrosPrecip ?? { riesgo_inund_min: 1 }
  )
  useEffect(() => { setLocal(filtros) }, [filtros])

  const apply = (f: FiltrosSismos) => { setLocal(f); onChange(f) }
  const applyPrecip = (f: FiltrosPrecipitacion) => {
    setLocalPrecip(f)
    onFiltrosPrecipChange?.(f)
  }

  const divider = <div style={{ height: 1, background: C.border, margin: '6px 0 16px' }} />
  const años    = local.year_end - local.year_start
  const leftPct = ((local.year_start - 1900) / (2030 - 1900)) * 100
  const wPct    = (años / (2030 - 1900)) * 100

  const zonaInfo = local.region ? ZONA_REGION[local.region] : null
  const fenInfo  = local.region ? FEN_REGION[local.region]  : null

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

      {/* Profundidad */}
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

      {/* Región */}
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

        {/* Badge zona sísmica */}
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

        {/* 🆕 v8.0: Badge FEN contextual */}
        {fenInfo && (
          <div style={{
            marginTop: 6, padding: '7px 10px',
            background: fenInfo.color + '08',
            border: `1px solid ${fenInfo.color}25`,
            borderLeft: `3px solid ${fenInfo.color}`,
            borderRadius: 8,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10.5, color: C.textSec }}>
                Índice FEN (lluvia en El Niño)
              </span>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 12, fontWeight: 800, color: fenInfo.color }}>
                ×{fenInfo.indice.toFixed(1)}
              </span>
            </div>
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: fenInfo.color, marginTop: 2 }}>
              {fenInfo.desc} · SENAMHI/CHIRPS 2024
            </div>
          </div>
        )}
      </div>

      {divider}

      {/* 🆕 v8.0: Filtros Precipitación */}
      {onFiltrosPrecipChange && (
        <>
          <div style={{ marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 6 }}>
              <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted }}>
                Precipitaciones
              </span>
              <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 7, fontWeight: 700, color: C.teal, background: '#ecfeff', border: '1px solid #a5f3fc', padding: '1px 4px', borderRadius: 3 }}>
                v8.0
              </span>
            </div>

            {/* Tipo climático */}
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, marginBottom: 5 }}>
              Tipo climático
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4, marginBottom: 10 }}>
              {TIPO_PRECIP_OPTS.map(({ value, label, color }) => (
                <Chip key={label} label={label} active={localPrecip.tipo === value}
                  color={color}
                  onClick={() => applyPrecip({ ...localPrecip, tipo: value })} />
              ))}
            </div>

            {/* Riesgo inundación mínimo */}
            <Slider
              label="Riesgo inundación mín."
              value={localPrecip.riesgo_inund_min}
              min={1} max={5} step={1}
              color={C.teal}
              format={v => ['Muy bajo','Bajo','Moderado','Alto','Muy alto'][v-1] ?? String(v)}
              onChange={v => applyPrecip({ ...localPrecip, riesgo_inund_min: v })}
            />

            {/* Índice FEN mínimo */}
            <Slider
              label="Índice FEN mínimo"
              value={localPrecip.fen_min ?? 0}
              min={0} max={4.5} step={0.5}
              color={C.orange}
              format={v => v === 0 ? 'Sin filtro' : `×${v.toFixed(1)}`}
              onChange={v => applyPrecip({ ...localPrecip, fen_min: v === 0 ? undefined : v })}
            />
          </div>
          {divider}
        </>
      )}

      {/* Fuente infraestructura */}
      {onFuenteTipoChange && (
        <>
          <div style={{ marginBottom: 16 }}>
            <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted, marginBottom: 6 }}>
              Fuente infraestructura
            </div>
            <div style={{ display: 'flex', gap: 4 }}>
              {([
                { value: 'todos',   label: 'Todos',   color: C.textMuted },
                { value: 'oficial', label: 'Oficial', color: C.primary   },
                { value: 'osm',     label: 'OSM',     color: '#6366f1'   },
              ] as { value: FuenteTipo; label: string; color: string }[]).map(({ value, label, color }) => (
                <Chip key={value} label={label} active={fuenteTipo === value}
                  color={color} onClick={() => onFuenteTipoChange(value)} />
              ))}
            </div>
          </div>
          {divider}
        </>
      )}

      {/* Resumen activo */}
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
            setLocal(reset); onChange(reset)
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