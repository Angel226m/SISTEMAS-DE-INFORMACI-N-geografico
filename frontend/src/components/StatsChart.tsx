// == StatsChart.tsx ========================================
import {
  BarChart, Bar, XAxis, YAxis, Tooltip,
  ResponsiveContainer, Cell, ReferenceLine,
} from 'recharts'
import type { EstadisticaAnual } from '../types'

const C = {
  primary: '#059669', secondary: '#0ea5e9', danger: '#dc2626',
  warning: '#f59e0b', accent: '#6366f1',
  text: '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
  border: '#e2e8f0', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
}

interface Props { estadisticas: EstadisticaAnual[]; loading: boolean }

// ── Tooltip personalizado ──────────────────────────────────
function CustomTooltip({
  active, payload, label,
}: {
  active?: boolean
  payload?: Array<{ value: number; payload: EstadisticaAnual }>
  label?: string
}) {
  if (!active || !payload?.length) return null
  const d = payload[0]?.payload as EstadisticaAnual | undefined
  return (
    <div style={{
      background: '#fff', border: `1px solid ${C.border}`,
      borderRadius: 10, padding: '10px 14px',
      boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
      minWidth: 160,
    }}>
      <p style={{
        fontFamily: "'Inter',sans-serif", fontSize: 13, fontWeight: 700,
        color: C.text, marginBottom: 8,
      }}>
        {label}
      </p>
      <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, color: C.primary, marginBottom: 4 }}>
        {d?.cantidad ?? 0} sismos totales
      </p>
      {d && (
        <>
          <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 10, color: C.warning, marginBottom: 2 }}>
            Máx: {d.magnitud_max} Mw
          </p>
          <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 10, color: C.textMuted, marginBottom: 6 }}>
            Prom: {d.magnitud_promedio} Mw
          </p>
          <div style={{ height: 1, background: C.border, marginBottom: 6 }} />
          <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: '#dc2626' }}>
            Superficial: {d.superficiales}
          </p>
          <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: '#f97316' }}>
            Intermedio:  {d.intermedios}
          </p>
          <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: '#0ea5e9' }}>
            Profundo:    {d.profundos}
          </p>
        </>
      )}
    </div>
  )
}

// ── Esqueleto de carga ─────────────────────────────────────
function LoadingSkeleton() {
  return (
    <div style={{ height: '100%', display: 'flex', alignItems: 'flex-end', gap: 2, paddingBottom: 4 }}>
      {Array.from({ length: 32 }, (_, i) => (
        <div
          key={i}
          style={{
            flex: 1, borderRadius: '3px 3px 0 0',
            background: C.bgMuted,
            height: `${16 + Math.sin(i * 0.55) * 14 + (i % 4) * 6}px`,
            opacity: 0.6,
          }}
        />
      ))}
    </div>
  )
}

// ── Componente principal ───────────────────────────────────
export default function StatsChart({ estadisticas, loading }: Props) {
  if (loading) return <LoadingSkeleton />

  if (!estadisticas.length) {
    return (
      <div style={{
        height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontFamily: "'JetBrains Mono',monospace", fontSize: 11, color: C.textMuted,
      }}>
        Sin datos — backend en línea?
      </div>
    )
  }

  const max   = Math.max(...estadisticas.map(e => e.cantidad))
  const avg   = estadisticas.reduce((s, e) => s + e.cantidad, 0) / estadisticas.length
  const total = estadisticas.reduce((s, e) => s + e.cantidad, 0)

  const barColor = (n: number) =>
    n >= max * 0.8 ? C.danger :
    n >= max * 0.5 ? C.warning :
    C.primary

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: 6, flexShrink: 0,
      }}>
        <span style={{
          fontFamily: "'Inter',sans-serif", fontSize: 10, fontWeight: 600,
          color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em',
        }}>
          Sismos por año
        </span>
        <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted }}>
          Total:&nbsp;
          <span style={{ color: C.text, fontWeight: 700 }}>{total.toLocaleString('es-PE')}</span>
        </span>
      </div>

      {/* Gráfica */}
      <div style={{ flex: 1, minHeight: 0 }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={estadisticas} margin={{ top: 2, right: 0, left: -32, bottom: 0 }}>
            <XAxis
              dataKey="year"
              tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'JetBrains Mono' }}
              tickLine={false} axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fill: C.textMuted, fontSize: 8, fontFamily: 'JetBrains Mono' }}
              tickLine={false} axisLine={false}
            />
            <Tooltip content={<CustomTooltip />} cursor={{ fill: 'rgba(5,150,105,0.05)' }} />
            <ReferenceLine y={avg} stroke={C.border} strokeDasharray="3 3" />
            <Bar dataKey="cantidad" radius={[3, 3, 0, 0]} maxBarSize={14}>
              {estadisticas.map((entry, i) => (
                <Cell key={i} fill={barColor(entry.cantidad)} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}