// ══════════════════════════════════════════════════════════
// StatsChart.tsx v6.0
// Nuevos modos: IRC ranking distritos + población por zona
// React.memo evita re-renders innecesarios
// ══════════════════════════════════════════════════════════

import { useState, memo } from 'react'
import {
  BarChart, Bar, AreaChart, Area, XAxis, YAxis,
  Tooltip, ResponsiveContainer, Cell, ReferenceLine,
  CartesianGrid, RadialBarChart, RadialBar, PieChart, Pie,
} from 'recharts'
import type { EstadisticaAnual, RiesgoConstruccionRanking } from '../types'

const C = {
  primary: '#059669', danger: '#dc2626', warning: '#f59e0b',
  text: '#0f172a', textMuted: '#94a3b8', border: '#e2e8f0', bgMuted: '#f1f5f9',
  orange: '#f97316', indigo: '#6366f1', amber: '#f59e0b',
}

const RISK_COLORS = [C.primary, '#10b981', C.warning, C.orange, C.danger]

type Modo = 'cantidad' | 'magnitud' | 'profundidad' | 'irc'

interface Props {
  estadisticas: EstadisticaAnual[]
  loading: boolean
  ircRanking?: RiesgoConstruccionRanking[]
}

function CustomTooltip({ active, payload, label }: {
  active?: boolean; payload?: Array<{ value: number; payload: EstadisticaAnual }>; label?: string
}) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  return (
    <div style={{
      background: '#fff', border: `1px solid ${C.border}`, borderRadius: 10,
      padding: '10px 14px', boxShadow: '0 4px 16px rgba(0,0,0,0.08)', minWidth: 170,
    }}>
      <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 13, fontWeight: 700, color: C.text, marginBottom: 8 }}>
        {label}
      </p>
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 11, color: C.primary, marginBottom: 3 }}>
        {d.cantidad.toLocaleString('es-PE')} sismos
      </p>
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.warning, marginBottom: 2 }}>
        Máx: {d.magnitud_max} Mw
      </p>
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted, marginBottom: 6 }}>
        Prom: {d.magnitud_prom?.toFixed(2)} Mw
      </p>
      {(d.m6_plus > 0 || d.m7_plus > 0) && (
        <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.danger, marginBottom: 4 }}>
          M6+: {d.m6_plus}  M7+: {d.m7_plus}
        </p>
      )}
      <div style={{ height: 1, background: C.border, margin: '6px 0' }} />
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: '#dc2626' }}>Superf: {d.superficiales}</p>
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: '#f97316' }}>Inter:  {d.intermedios}</p>
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: '#0ea5e9' }}>Prof:   {d.profundos}</p>
    </div>
  )
}

function IRCTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: RiesgoConstruccionRanking }> }) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload
  if (!d) return null
  const nivel = Math.max(1, Math.min(5, Math.round(d.indice_riesgo_construccion)))
  return (
    <div style={{
      background: '#fff', border: `1px solid ${C.border}`, borderRadius: 10,
      padding: '10px 14px', boxShadow: '0 4px 16px rgba(0,0,0,0.08)', minWidth: 200,
    }}>
      <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, fontWeight: 700, color: C.text, marginBottom: 4 }}>
        {d.distrito}
      </p>
      <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted, marginBottom: 6 }}>
        {d.departamento}
      </p>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted }}>IRC</span>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 13, fontWeight: 800, color: RISK_COLORS[nivel - 1] }}>
          {d.indice_riesgo_construccion.toFixed(2)}
        </span>
      </div>
      {d.zona_sismica && (
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted }}>Zona sísmica</span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, fontWeight: 700, color: C.danger }}>Z{d.zona_sismica} ({d.factor_z}g)</span>
        </div>
      )}
    </div>
  )
}

function Skeleton() {
  return (
    <div style={{ height: '100%', display: 'flex', alignItems: 'flex-end', gap: 2, paddingBottom: 4 }}>
      {Array.from({ length: 30 }, (_, i) => (
        <div key={i} style={{
          flex: 1, borderRadius: '2px 2px 0 0', background: C.bgMuted,
          height: `${18 + Math.sin(i * 0.6) * 14 + (i % 3) * 5}px`, opacity: 0.6,
        }} />
      ))}
    </div>
  )
}

const StatsChart = memo(function StatsChart({ estadisticas, loading, ircRanking = [] }: Props) {
  const [modo, setModo] = useState<Modo>('cantidad')

  if (loading) return <Skeleton />
  if (!estadisticas.length && modo !== 'irc') {
    return (
      <div style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontFamily: "'DM Mono',monospace", fontSize: 11, color: C.textMuted }}>
        Sin datos — API en línea?
      </div>
    )
  }

  const max   = estadisticas.length ? Math.max(...estadisticas.map(e => e.cantidad)) : 0
  const avg   = estadisticas.length ? estadisticas.reduce((s, e) => s + e.cantidad, 0) / estadisticas.length : 0
  const total = estadisticas.length ? estadisticas.reduce((s, e) => s + e.cantidad, 0) : 0
  const barC  = (n: number) => n >= max * 0.8 ? C.danger : n >= max * 0.5 ? C.warning : C.primary

  const MODOS: { key: Modo; label: string; color: string }[] = [
    { key: 'cantidad',    label: 'Cantidad',  color: C.primary },
    { key: 'magnitud',    label: 'Magnitud',  color: C.warning },
    { key: 'profundidad', label: 'Profund.',  color: C.indigo  },
    { key: 'irc',         label: 'IRC ▸Top',  color: C.amber   },
  ]

  // IRC ranking — top 15 distritos
  const ircTop = [...ircRanking]
    .sort((a, b) => b.indice_riesgo_construccion - a.indice_riesgo_construccion)
    .slice(0, 15)

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 5, flexShrink: 0 }}>
        <div style={{ display: 'flex', gap: 4 }}>
          {MODOS.map(({ key, label, color }) => (
            <button key={key} onClick={() => setModo(key)} style={{
              padding: '2px 8px', borderRadius: 5, border: 'none', cursor: 'pointer',
              background: modo === key ? `${color}15` : 'transparent',
              color: modo === key ? color : C.textMuted,
              fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: modo === key ? 700 : 400,
              transition: 'all 0.15s',
            }}>{label}</button>
          ))}
        </div>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted }}>
          {modo === 'irc' ? (
            <span style={{ color: C.amber, fontWeight: 700 }}>{ircTop.length} distritos · IRC CENEPRED</span>
          ) : (
            <>
              <span style={{ color: C.text, fontWeight: 700 }}>{total.toLocaleString('es-PE')}</span>
              {' total'}
              {estadisticas.length > 0 && (
                <span style={{ color: C.textMuted }}>
                  {' · '}{estadisticas[0].anio}–{estadisticas[estadisticas.length - 1].anio}
                </span>
              )}
            </>
          )}
        </span>
      </div>

      {/* Gráfica */}
      <div style={{ flex: 1, minHeight: 0 }}>
        <ResponsiveContainer width="100%" height="100%">
          {modo === 'cantidad' ? (
            <BarChart data={estadisticas} margin={{ top: 2, right: 0, left: -30, bottom: 0 }}>
              <XAxis dataKey="anio" tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(5,150,105,0.04)' }} />
              <ReferenceLine y={avg} stroke={C.border} strokeDasharray="3 3" />
              <Bar dataKey="cantidad" radius={[2, 2, 0, 0]} maxBarSize={12}>
                {estadisticas.map((e, i) => <Cell key={i} fill={barC(e.cantidad)} />)}
              </Bar>
            </BarChart>
          ) : modo === 'magnitud' ? (
            <AreaChart data={estadisticas} margin={{ top: 2, right: 0, left: -30, bottom: 0 }}>
              <defs>
                <linearGradient id="gMag" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor={C.warning} stopOpacity={0.3} />
                  <stop offset="95%" stopColor={C.warning} stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke={C.border} vertical={false} />
              <XAxis dataKey="anio" tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} domain={[4, 'auto']} />
              <Tooltip content={<CustomTooltip />} />
              <Area dataKey="magnitud_max"  stroke={C.warning} strokeWidth={1.5} fill="url(#gMag)" dot={false} />
              <Area dataKey="magnitud_prom" stroke={C.primary} strokeWidth={1}   fill="none" strokeDasharray="3 3" dot={false} />
            </AreaChart>
          ) : modo === 'irc' ? (
            <BarChart
              data={ircTop}
              layout="vertical"
              margin={{ top: 0, right: 10, left: 8, bottom: 0 }}
            >
              <XAxis type="number" domain={[0, 5]} tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} />
              <YAxis
                type="category" dataKey="distrito"
                tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }}
                tickLine={false} axisLine={false} width={70}
              />
              <Tooltip content={<IRCTooltip />} cursor={{ fill: 'rgba(245,158,11,0.05)' }} />
              <Bar dataKey="indice_riesgo_construccion" radius={[0, 3, 3, 0]} maxBarSize={10}>
                {ircTop.map((e, i) => {
                  const nivel = Math.max(1, Math.min(5, Math.round(e.indice_riesgo_construccion)))
                  return <Cell key={i} fill={RISK_COLORS[nivel - 1]} />
                })}
              </Bar>
            </BarChart>
          ) : (
            <AreaChart data={estadisticas} margin={{ top: 2, right: 0, left: -30, bottom: 0 }}>
              <defs>
                <linearGradient id="gS" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor="#dc2626" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#dc2626" stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="gI" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor="#f97316" stopOpacity={0.25} />
                  <stop offset="95%" stopColor="#f97316" stopOpacity={0.02} />
                </linearGradient>
                <linearGradient id="gP" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%"  stopColor="#0ea5e9" stopOpacity={0.2} />
                  <stop offset="95%" stopColor="#0ea5e9" stopOpacity={0.02} />
                </linearGradient>
              </defs>
              <XAxis dataKey="anio" tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} interval="preserveStartEnd" />
              <YAxis tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'DM Mono' }} tickLine={false} axisLine={false} />
              <Tooltip content={<CustomTooltip />} />
              <Area type="monotone" dataKey="superficiales" stroke="#dc2626" fill="url(#gS)" strokeWidth={1.2} dot={false} stackId="a" />
              <Area type="monotone" dataKey="intermedios"   stroke="#f97316" fill="url(#gI)" strokeWidth={1.2} dot={false} stackId="a" />
              <Area type="monotone" dataKey="profundos"     stroke="#0ea5e9" fill="url(#gP)" strokeWidth={1.2} dot={false} stackId="a" />
            </AreaChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  )
})

export default StatsChart