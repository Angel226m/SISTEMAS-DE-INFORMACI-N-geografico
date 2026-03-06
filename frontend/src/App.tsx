// ══════════════════════════════════════════════════════════
// GeoRiesgo Perú — App.tsx v4.0
// ══════════════════════════════════════════════════════════

import { useState, useCallback, useEffect, useRef } from 'react'
import MapView        from './components/MapView'
import LayerPanel     from './components/LayerPanel'
import FilterPanel    from './components/FilterPanel'
import StatsChart     from './components/StatsChart'
import LandingPage    from './components/Landingpage'
import { useMapData } from './hooks/useMapData'
import type { CapasActivas, FiltrosSismos, TipoVista, TooltipInfo } from './types'
import type { MapStyle } from './components/MapView'

const C = {
  primary:   '#059669', primaryBg: '#ecfdf5', primaryLt: '#10b981',
  secondary: '#0ea5e9',
  accent:    '#6366f1',  danger: '#dc2626', dangerBg: '#fef2f2',
  warning:   '#f59e0b',  warningBg: '#fffbeb',
  bg:        '#ffffff',  bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  border:    '#e2e8f0',
  text:      '#0f172a',  textSec: '#475569', textMuted: '#94a3b8',
}

// ── Íconos SVG ─────────────────────────────────────────────
const Icons = {
  Menu:    () => <svg width="16" height="12" viewBox="0 0 16 12" fill="currentColor"><rect y="0" width="16" height="2" rx="1"/><rect y="5" width="11" height="2" rx="1"/><rect y="10" width="16" height="2" rx="1"/></svg>,
  Chart:   () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><rect x="1" y="7" width="2.5" height="6"/><rect x="5.75" y="4" width="2.5" height="9"/><rect x="10.5" y="1" width="2.5" height="12"/></svg>,
  Layers:  () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><polygon points="7,1 13,4.5 7,8 1,4.5"/><polyline points="1,8.5 7,12 13,8.5"/></svg>,
  Filter:  () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><path d="M1 2h12M3 7h8M5 12h4"/></svg>,
  Refresh: () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12.5 2.5A6 6 0 1 1 8.5 1.5"/><polyline points="8.5,1.5 12.5,1.5 12.5,5.5"/></svg>,
  X:       () => <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="1" y1="1" x2="9" y2="9"/><line x1="9" y1="1" x2="1" y2="9"/></svg>,
  Locate:  () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><circle cx="7" cy="7" r="3"/><line x1="7" y1="1" x2="7" y2="4"/><line x1="7" y1="10" x2="7" y2="13"/><line x1="1" y1="7" x2="4" y2="7"/><line x1="10" y1="7" x2="13" y2="7"/></svg>,
  Map:     () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><polygon points="5,1 9,1 13,4 13,13 9,13 5,10 1,13 1,4"/><line x1="5" y1="1" x2="5" y2="10"/><line x1="9" y1="1" x2="9" y2="13"/></svg>,
  Globe:   () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><circle cx="7" cy="7" r="6"/><path d="M1 7h12M7 1c-2 2-3 4-3 6s1 4 3 6M7 1c2 2 3 4 3 6s-1 4-3 6"/></svg>,
}

// ── Estado inicial ─────────────────────────────────────────
const CAPAS_INIT: CapasActivas = {
  sismos: true, heatmap: false, fallas: true,
  inundaciones: false, tsunamis: false, riesgo_distritos: true,
  infraestructura: false, estaciones: false, extrusion_3d: false,
}
const FILTROS_INIT: FiltrosSismos = { mag_min: 3.0, mag_max: 9.5, year_start: 1960, year_end: 2030 }

// ── Toast ──────────────────────────────────────────────────
interface Toast { id: number; type: 'error' | 'success' | 'warn'; msg: string }
const TOAST_C = {
  error:   { fg: C.danger,  bg: C.dangerBg  },
  success: { fg: C.primary, bg: C.primaryBg },
  warn:    { fg: C.warning, bg: C.warningBg },
}

function ToastList({ toasts, remove }: { toasts: Toast[]; remove: (id: number) => void }) {
  if (!toasts.length) return null
  return (
    <div style={{ position: 'fixed', bottom: 52, right: 14, zIndex: 300, display: 'flex', flexDirection: 'column', gap: 7 }}>
      {toasts.map(t => (
        <div key={t.id} style={{
          display: 'flex', alignItems: 'flex-start', gap: 10, padding: '9px 12px',
          background: TOAST_C[t.type].bg,
          border: `1px solid ${TOAST_C[t.type].fg}30`,
          borderLeft: `3px solid ${TOAST_C[t.type].fg}`,
          borderRadius: 10, boxShadow: '0 4px 14px rgba(0,0,0,0.07)',
          animation: 'slideInR 0.2s ease', maxWidth: 300,
        }}>
          <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, color: C.text, flex: 1, lineHeight: 1.4 }}>{t.msg}</span>
          <button onClick={() => remove(t.id)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.textMuted, padding: 0 }}>
            <Icons.X />
          </button>
        </div>
      ))}
    </div>
  )
}

// ── Loader ─────────────────────────────────────────────────
function Loader({ pct }: { pct: number }) {
  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 999, background: C.bg, display: 'flex', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: 24 }}>
      <div style={{ position: 'relative', width: 72, height: 72 }}>
        {[0, 1, 2].map(i => (
          <div key={i} style={{ position: 'absolute', inset: i * 10, borderRadius: '50%', border: '2px solid', borderColor: [C.primary + '80', C.primaryLt + '50', C.secondary + '35'][i], animation: `pring ${1.5 + i * 0.5}s ease-out infinite`, animationDelay: `${i * 0.2}s` }} />
        ))}
        <div style={{ position: 'absolute', inset: 26, borderRadius: '50%', background: `linear-gradient(135deg,${C.primary},${C.secondary})` }} />
      </div>
      <div style={{ textAlign: 'center' }}>
        <p style={{ fontFamily: "'DM Sans',sans-serif", color: C.text, fontSize: 22, fontWeight: 800, letterSpacing: '-0.02em', margin: 0 }}>
          GeoRiesgo <span style={{ color: C.primary }}>Perú</span>
        </p>
        <p style={{ fontFamily: "'DM Mono',monospace", color: C.textMuted, fontSize: 10, letterSpacing: '0.1em', textTransform: 'uppercase', marginTop: 4 }}>
          Cargando datos geoespaciales...
        </p>
      </div>
      <div style={{ width: 220, height: 3, background: C.bgMuted, borderRadius: 2 }}>
        <div style={{ height: '100%', width: `${pct}%`, background: `linear-gradient(90deg,${C.primary},${C.secondary})`, borderRadius: 2, transition: 'width 0.5s ease' }} />
      </div>
    </div>
  )
}

// ── Tooltip hover sobre features ───────────────────────────
function HoverTooltip({ info }: { info: TooltipInfo }) {
  if (!info.object?.properties) return null
  const p   = info.object.properties as Record<string, unknown>
  const mag = Number(p.magnitud ?? 0)
  const isSismo = 'magnitud' in p
  const mc  = mag >= 7 ? C.danger : mag >= 5 ? C.warning : C.primary

  return (
    <div style={{
      position: 'absolute',
      left:  Math.min(info.x + 12, window.innerWidth  - 220),
      top:   Math.max(info.y - 60, 10),
      zIndex: 60, pointerEvents: 'none',
      background: 'rgba(255,255,255,0.96)',
      backdropFilter: 'blur(8px)',
      border: `1px solid ${C.border}`,
      borderRadius: 10, padding: '8px 12px',
      boxShadow: '0 4px 20px rgba(0,0,0,0.1)',
      minWidth: 160, maxWidth: 220,
    }}>
      {isSismo ? (
        <>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 5, marginBottom: 4 }}>
            <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 22, fontWeight: 700, color: mc, lineHeight: 1 }}>{mag.toFixed(1)}</span>
            <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted }}>Mw</span>
          </div>
          <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textSec, lineHeight: 1.4 }}>
            {String(p.fecha ?? '')} · {String(p.tipo_profundidad ?? '')}
          </div>
          <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textMuted, marginTop: 2 }}>
            Prof: {Number(p.profundidad_km ?? 0)} km
          </div>
        </>
      ) : (
        <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, color: C.text, fontWeight: 600 }}>
          {String(p.nombre ?? p.tipo ?? 'Feature')}
        </div>
      )}
    </div>
  )
}

// ── Popup de detalle al hacer clic ─────────────────────────
const RISK_LABELS = ['Muy bajo', 'Bajo', 'Moderado', 'Alto', 'Muy alto']
const RISK_COLORS = [C.primary, '#10b981', C.warning, '#f97316', C.danger]

function Row({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 5 }}>
      <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted }}>{label}</span>
      <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, fontWeight: 600, color: color ?? C.text, textAlign: 'right' }}>{String(value)}</span>
    </div>
  )
}

function InfoPopup({ props, layer, onClose }: {
  props: Record<string, unknown>; layer: string; onClose: () => void
}) {
  const p = props
  const isSismo  = 'magnitud' in p
  const isFalla  = 'activa' in p
  const isDistrito = 'nivel_riesgo' in p && 'nombre' in p && !isFalla && !isSismo
  const isInfra  = 'criticidad' in p
  const isTsunami = 'altura_ola_m' in p
  const isEstacion = 'codigo' in p

  const accentColor =
    isSismo   ? C.danger  :
    isFalla   ? C.warning :
    isDistrito? C.primary :
    isInfra   ? C.accent  :
    isTsunami ? '#06b6d4' :
    C.textMuted

  const kindLabel =
    isSismo   ? 'Sismo'           :
    isFalla   ? 'Falla Geológica' :
    isDistrito? 'Distrito'        :
    isInfra   ? 'Infraestructura' :
    isTsunami ? 'Zona Tsunami'    :
    isEstacion? 'Estación'        :
    'Elemento'

  return (
    <div style={{
      position: 'absolute', bottom: 48, left: 14, zIndex: 50, width: 280,
      animation: 'slideUp 0.2s ease-out forwards',
    }}>
      <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderTop: `3px solid ${accentColor}`, borderRadius: 14, overflow: 'hidden', boxShadow: '0 8px 32px rgba(0,0,0,0.1)' }}>
        {/* Header */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '9px 12px', background: C.bgSoft, borderBottom: `1px solid ${C.border}` }}>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, fontWeight: 700, color: accentColor, letterSpacing: '0.08em' }}>{kindLabel}</span>
          <button onClick={onClose} style={{ width: 22, height: 22, borderRadius: 6, background: C.bgMuted, border: `1px solid ${C.border}`, color: C.textMuted, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Icons.X />
          </button>
        </div>

        {/* Cuerpo */}
        <div style={{ padding: '12px 14px' }}>
          {isSismo && (() => {
            const mag  = Number(p.magnitud ?? 0)
            const prof = Number(p.profundidad_km ?? 0)
            const mc   = mag >= 7 ? C.danger : mag >= 6 ? '#f97316' : mag >= 5 ? C.warning : C.primary
            return (
              <>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 12, paddingBottom: 10, borderBottom: `1px solid ${C.border}` }}>
                  <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 40, fontWeight: 800, color: mc, lineHeight: 1 }}>{mag.toFixed(1)}</span>
                  <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 12, color: C.textMuted }}>Mw</span>
                </div>
                <Row label="Fecha"         value={String(p.fecha ?? '')} />
                <Row label="Profundidad"   value={`${prof} km`} color={prof < 30 ? C.danger : prof < 70 ? '#f97316' : '#0ea5e9'} />
                <Row label="Tipo"          value={String(p.tipo_profundidad ?? '')} />
                <Row label="ID USGS"       value={String(p.usgs_id ?? '-')} />
                {p.region && <Row label="Región" value={String(p.region)} />}
                <div style={{ marginTop: 7, padding: '7px 10px', background: C.bgSoft, borderRadius: 8, fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textSec, lineHeight: 1.4 }}>
                  {String(p.lugar ?? '')}
                </div>
              </>
            )
          })()}

          {isDistrito && (() => {
            const nivel = Math.max(1, Math.min(5, Number(p.nivel_riesgo ?? 1)))
            return (
              <>
                <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 16, fontWeight: 700, color: C.text, marginBottom: 10 }}>
                  {String(p.nombre ?? '')}
                </p>
                {p.provincia    && <Row label="Provincia"    value={String(p.provincia)} />}
                {p.departamento && <Row label="Departamento" value={String(p.departamento)} />}
                <div style={{ marginBottom: 6 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5 }}>
                    <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted }}>Nivel de riesgo</span>
                    <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 11, fontWeight: 700, color: RISK_COLORS[nivel - 1] }}>
                      {RISK_LABELS[nivel - 1]}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: 3 }}>
                    {RISK_LABELS.map((_, i) => (
                      <div key={i} style={{ flex: 1, height: 5, borderRadius: 3, background: i < nivel ? RISK_COLORS[i] : C.bgMuted }} />
                    ))}
                  </div>
                </div>
                {p.fuente && <Row label="Fuente" value={String(p.fuente)} />}
              </>
            )
          })()}

          {isFalla && (() => {
            const activa = Boolean(p.activa)
            return (
              <>
                <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 700, color: C.text, marginBottom: 8 }}>
                  {String(p.nombre ?? '')}
                </p>
                {p.nombre_alt && <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, color: C.textMuted, marginBottom: 8 }}>{String(p.nombre_alt)}</p>}
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '4px 10px', background: activa ? C.dangerBg : C.bgMuted, borderRadius: 99, marginBottom: 8 }}>
                  <div style={{ width: 6, height: 6, borderRadius: '50%', background: activa ? C.danger : C.textMuted }} />
                  <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: activa ? C.danger : C.textMuted }}>
                    {activa ? 'Falla Activa' : 'Falla Inactiva'}
                  </span>
                </div>
                {p.tipo        && <Row label="Tipo"        value={String(p.tipo)} />}
                {p.mecanismo   && <Row label="Mecanismo"   value={String(p.mecanismo)} />}
                {p.longitud_km && <Row label="Longitud"    value={`${Number(p.longitud_km).toFixed(1)} km`} />}
                {p.magnitud_max && <Row label="Mag. máx."  value={`${p.magnitud_max} Mw`} color={C.danger} />}
                {p.region      && <Row label="Región"      value={String(p.region)} />}
                {p.fuente      && <Row label="Fuente"      value={String(p.fuente)} />}
                {p.referencia  && (
                  <div style={{ marginTop: 6, padding: '6px 9px', background: C.bgSoft, borderRadius: 7, fontFamily: "'DM Sans',sans-serif", fontSize: 10, color: C.textMuted, lineHeight: 1.4 }}>
                    {String(p.referencia)}
                  </div>
                )}
              </>
            )
          })()}

          {isTsunami && (() => (
            <>
              <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 700, color: C.text, marginBottom: 10 }}>
                {String(p.nombre ?? '')}
              </p>
              {p.altura_ola_m     && <Row label="Altura ola"     value={`${p.altura_ola_m} m`} color="#06b6d4" />}
              {p.tiempo_arribo_min && <Row label="Tiempo arribo"  value={`${p.tiempo_arribo_min} min`} />}
              {p.periodo_retorno  && <Row label="Período retorno" value={`${p.periodo_retorno} años`} />}
              {p.nivel_riesgo     && <Row label="Nivel riesgo"    value={String(p.nivel_riesgo)} color={C.danger} />}
              {p.region           && <Row label="Región"          value={String(p.region)} />}
              {p.fuente           && <Row label="Fuente"          value={String(p.fuente)} />}
            </>
          ))()}

          {isEstacion && (() => (
            <>
              <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 700, color: C.text, marginBottom: 10 }}>
                {String(p.nombre ?? '')}
              </p>
              {p.codigo      && <Row label="Código"      value={String(p.codigo)} />}
              {p.tipo        && <Row label="Tipo"        value={String(p.tipo)} />}
              {p.institucion && <Row label="Institución" value={String(p.institucion)} />}
              {p.altitud_m   && <Row label="Altitud"     value={`${p.altitud_m} m.s.n.m.`} />}
              {p.region      && <Row label="Región"      value={String(p.region)} />}
            </>
          ))()}

          {isInfra && !isTsunami && !isEstacion && (
            <>
              {p.nombre && <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 8 }}>{String(p.nombre)}</p>}
              {p.tipo        && <Row label="Tipo"       value={String(p.tipo).replace('_', ' ')} />}
              {p.criticidad  && <Row label="Criticidad" value={`${p.criticidad}/5`} color={Number(p.criticidad) >= 4 ? C.danger : C.warning} />}
              {p.estado      && <Row label="Estado"     value={String(p.estado)} />}
              {p.region      && <Row label="Región"     value={String(p.region)} />}
            </>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Btn ────────────────────────────────────────────────────
function Btn({ active, onClick, title, children }: {
  active: boolean; onClick: () => void; title?: string; children: React.ReactNode
}) {
  return (
    <button title={title} onClick={onClick} style={{
      width: 32, height: 32, borderRadius: 8, cursor: 'pointer',
      background: active ? `${C.primary}15` : C.bgMuted,
      border: `1px solid ${active ? `${C.primary}40` : C.border}`,
      color:  active ? C.primary : C.textMuted,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      transition: 'all 0.16s ease',
    }}>
      {children}
    </button>
  )
}

// ══════════════════════════════════════════════════════════
//  App
// ══════════════════════════════════════════════════════════
export default function App() {
  const [showLanding, setShowLanding] = useState(true)
  const [capas,     setCapas]     = useState<CapasActivas>(CAPAS_INIT)
  const [filtros,   setFiltros]   = useState<FiltrosSismos>(FILTROS_INIT)
  const [vista,     setVista]     = useState<TipoVista>('2d')
  const [mapStyle,  setMapStyle]  = useState<MapStyle>('light')
  const [popup,     setPopup]     = useState<{ props: Record<string, unknown>; layer: string } | null>(null)
  const [tooltip,   setTooltip]   = useState<TooltipInfo | null>(null)
  const [tab,       setTab]       = useState<'capas' | 'filtros'>('capas')
  const [sidebar,   setSidebar]   = useState(true)
  const [chart,     setChart]     = useState(true)
  const [toasts,    setToasts]    = useState<Toast[]>([])

  const { data, loading, errors, recargarSismos, recargarTodo } = useMapData()

  // % de carga total para el loader
  const totalKeys = 8
  const doneKeys  = Object.values(loading).filter(v => !v).length
  const loadPct   = (doneKeys / totalKeys) * 100
  const isInitial = doneKeys < 3  // Mostrar loader hasta que 3 capas carguen

  // Mostrar errores como toasts (una vez por error único)
  const shownErr = useRef(new Set<string>())
  useEffect(() => {
    Object.entries(errors).forEach(([k, v]) => {
      if (!v) return
      const uid = `${k}:${v}`
      if (shownErr.current.has(uid)) return
      shownErr.current.add(uid)
      addToast('error', `${k}: ${v}`)
    })
  }, [errors])

  function addToast(type: Toast['type'], msg: string) {
    const id = Date.now() + Math.random()
    setToasts(p => [...p, { id, type, msg }])
    setTimeout(() => setToasts(p => p.filter(t => t.id !== id)), 5000)
  }

  // Atajos de teclado
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return
      if (e.key === 'l' || e.key === 'L') setSidebar(p => !p)
      if (e.key === 'f' || e.key === 'F') { setSidebar(true); setTab('filtros') }
      if (e.key === 'g' || e.key === 'G') setChart(p => !p)
      if (e.key === 'Escape') { setPopup(null); setTooltip(null) }
      if (e.key === 'r' || e.key === 'R') window.dispatchEvent(new CustomEvent('geo:center-ica'))
    }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [])

  const handleFiltros = useCallback((f: FiltrosSismos) => {
    setFiltros(f)
    recargarSismos(f)
  }, [recargarSismos])

  const handleClick = useCallback((props: Record<string, unknown>, layer: string) => {
    setTooltip(null)
    setPopup({ props, layer })
  }, [])

  const sidebarW = sidebar ? 268 : 0
  const totalErr = Object.values(errors).filter(Boolean).length

  const MAP_STYLES: { key: MapStyle; label: string }[] = [
    { key: 'light', label: 'Claro' },
    { key: 'dark',  label: 'Oscuro' },
  ]

  return (
    <div style={{ width: '100vw', height: '100vh', overflow: 'hidden', background: C.bg, display: 'flex', flexDirection: 'column' }}>
      {showLanding && <LandingPage onEnter={() => setShowLanding(false)} />}
      {!showLanding && (<>
      {isInitial && <Loader pct={loadPct} />}
      <ToastList toasts={toasts} remove={id => setToasts(p => p.filter(t => t.id !== id))} />

      {/* ── HEADER ────────────────────────────────────────── */}
      <header style={{ flexShrink: 0, height: 52, display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 14px', background: C.bg, borderBottom: `1px solid ${C.border}`, zIndex: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <Btn active={sidebar} onClick={() => setSidebar(p => !p)} title="Sidebar [L]"><Icons.Menu /></Btn>
          <div style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
            <div style={{ width: 32, height: 32, borderRadius: 9, background: `linear-gradient(135deg,${C.primary},${C.secondary})`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 16, color: 'white', fontWeight: 800, boxShadow: `0 2px 8px ${C.primary}40` }}>G</div>
            <div>
              <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 800, color: C.text, letterSpacing: '-0.02em', lineHeight: 1.1 }}>
                GeoRiesgo <span style={{ color: C.primary }}>Perú</span>
              </div>
              <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, letterSpacing: '0.1em', textTransform: 'uppercase' }}>
                Riesgo Sísmico · v4.0
              </div>
            </div>
          </div>
        </div>

        {/* Badge sismos */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 13px', background: C.bgSoft, border: `1px solid ${C.border}`, borderRadius: 99 }}>
          <div style={{ width: 8, height: 8, borderRadius: '50%', background: C.primary, animation: 'pring 1.8s ease-out infinite' }} />
          {loading.sismos
            ? <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 11, color: C.textMuted }}>Cargando...</span>
            : <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 11, fontWeight: 700, color: C.text }}>
                {(data.sismos?.features.length ?? 0).toLocaleString('es-PE')} sismos
              </span>
          }
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
          {/* Estilo de mapa */}
          <div style={{ display: 'flex', background: C.bgMuted, border: `1px solid ${C.border}`, borderRadius: 8, padding: 2, marginRight: 4 }}>
            {MAP_STYLES.map(({ key, label }) => (
              <button key={key} onClick={() => setMapStyle(key)} style={{
                padding: '3px 9px', borderRadius: 6, border: 'none', cursor: 'pointer',
                fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: 700,
                background: mapStyle === key ? C.bg : 'transparent',
                color:      mapStyle === key ? C.text : C.textMuted,
                boxShadow:  mapStyle === key ? '0 1px 2px rgba(0,0,0,0.06)' : 'none',
                transition: 'all 0.18s',
              }}>{label}</button>
            ))}
          </div>

          {/* Vista 2D/3D */}
          <div style={{ display: 'flex', background: C.bgMuted, border: `1px solid ${C.border}`, borderRadius: 8, padding: 2, marginRight: 4 }}>
            {(['2d', '3d'] as TipoVista[]).map(v => (
              <button key={v} onClick={() => setVista(v)} style={{
                padding: '3px 9px', borderRadius: 6, border: 'none', cursor: 'pointer',
                fontFamily: "'DM Mono',monospace", fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
                background: vista === v ? C.bg : 'transparent',
                color:      vista === v ? C.text : C.textMuted,
                transition: 'all 0.18s',
              }}>{v}</button>
            ))}
          </div>

          <Btn active={chart}                onClick={() => setChart(p => !p)}                              title="Gráfica [G]"><Icons.Chart /></Btn>
          <Btn active={tab==='capas'&&sidebar}   onClick={() => { setSidebar(true); setTab('capas') }}        title="Capas [L]"><Icons.Layers /></Btn>
          <Btn active={tab==='filtros'&&sidebar} onClick={() => { setSidebar(true); setTab('filtros') }}      title="Filtros [F]"><Icons.Filter /></Btn>
          <Btn active={false}                onClick={() => { recargarTodo(); addToast('success', 'Recargando datos...') }} title="Recargar [R]"><Icons.Refresh /></Btn>
        </div>
      </header>

      {/* ── CUERPO ────────────────────────────────────────── */}
      <div style={{ flex: 1, display: 'flex', overflow: 'hidden', position: 'relative' }}>

        {/* Sidebar */}
        <aside style={{ flexShrink: 0, width: sidebarW, overflow: 'hidden', transition: 'width 0.26s cubic-bezier(0.4,0,0.2,1)', background: C.bg, borderRight: `1px solid ${C.border}`, display: 'flex', flexDirection: 'column', zIndex: 10 }}>
          <div style={{ width: 268, height: '100%', display: 'flex', flexDirection: 'column', padding: '12px 12px 0' }}>
            {/* Tabs */}
            <div style={{ display: 'flex', gap: 3, background: C.bgMuted, borderRadius: 10, padding: 3, marginBottom: 16, flexShrink: 0 }}>
              {(['capas', 'filtros'] as const).map(t => (
                <button key={t} onClick={() => setTab(t)} style={{
                  flex: 1, padding: '6px 0', borderRadius: 8, border: 'none', cursor: 'pointer',
                  fontFamily: "'DM Mono',monospace", fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em',
                  background: tab === t ? C.bg : 'transparent',
                  color: tab === t ? C.text : C.textMuted,
                  boxShadow: tab === t ? '0 1px 3px rgba(0,0,0,0.06)' : 'none',
                  transition: 'all 0.18s', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 5,
                }}>
                  {t === 'capas' ? <><Icons.Layers /> Capas</> : <><Icons.Filter /> Filtros</>}
                </button>
              ))}
            </div>

            <div style={{ flex: 1, overflowY: 'auto', paddingBottom: 12 }}>
              {tab === 'capas'
                ? <LayerPanel capas={capas} onChange={setCapas} />
                : <FilterPanel filtros={filtros} onChange={handleFiltros} />
              }
            </div>

            {/* Footer sidebar */}
            <div style={{ paddingTop: 10, paddingBottom: 12, borderTop: `1px solid ${C.border}`, flexShrink: 0 }}>
              <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, lineHeight: 2.0 }}>
                USGS · INEI/GADM · IGP · INGEMMET · ANA<br />
                CENEPRED · PREDES · SENAMHI · OSM
              </p>
              <p style={{ fontFamily: "'DM Mono',monospace", fontSize: 8, color: C.textMuted, marginTop: 2 }}>
                [L] panel  [F] filtros  [G] gráfica  [R] recargar
              </p>
            </div>
          </div>
        </aside>

        {/* Área mapa */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', position: 'relative' }}>
          <div style={{ flex: 1, position: 'relative' }}>
            <MapView
              sismos={data.sismos}
              distritos={data.distritos}
              fallas={data.fallas}
              inundaciones={data.inundaciones}
              tsunamis={data.tsunamis}
              infraestructura={data.infraestructura}
              estaciones={data.estaciones}
              capas={capas}
              vista={vista}
              mapStyle={mapStyle}
              onClickFeature={handleClick}
              onHoverFeature={setTooltip}
            />

            {/* Tooltip hover */}
            {tooltip && !popup && <HoverTooltip info={tooltip} />}

            {/* Popup detalle */}
            {popup && <InfoPopup props={popup.props} layer={popup.layer} onClose={() => setPopup(null)} />}

            {/* Badge ubicación */}
            <div style={{ position: 'absolute', top: 14, left: 14, zIndex: 10, background: 'rgba(255,255,255,0.93)', backdropFilter: 'blur(12px)', border: `1px solid ${C.border}`, borderRadius: 12, padding: '7px 12px', boxShadow: '0 2px 8px rgba(0,0,0,0.06)' }}>
              <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 2 }}>
                {vista === '3d' ? 'Vista 3D' : 'Vista 2D'}
              </div>
              <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 13, fontWeight: 700, color: C.text }}>Ica, Perú</div>
              <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted }}>14.07°S  75.73°O</div>
            </div>

            {/* Controles recentrar */}
            <div style={{ position: 'absolute', top: 14, right: 56, zIndex: 10, display: 'flex', flexDirection: 'column', gap: 5 }}>
              <button title="Centrar en Ica [R]"
                onClick={() => window.dispatchEvent(new CustomEvent('geo:center-ica'))}
                style={{ width: 34, height: 34, borderRadius: 9, background: 'rgba(255,255,255,0.93)', backdropFilter: 'blur(12px)', border: `1px solid ${C.border}`, color: C.textSec, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', boxShadow: '0 2px 6px rgba(0,0,0,0.06)' }}>
                <Icons.Locate />
              </button>
              <button title="Vista Perú"
                onClick={() => window.dispatchEvent(new CustomEvent('geo:center-peru'))}
                style={{ width: 34, height: 34, borderRadius: 9, background: 'rgba(255,255,255,0.93)', backdropFilter: 'blur(12px)', border: `1px solid ${C.border}`, color: C.textSec, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', boxShadow: '0 2px 6px rgba(0,0,0,0.06)' }}>
                <Icons.Globe />
              </button>
            </div>

            {/* Indicadores de carga por capa */}
            {Object.entries(loading).some(([, v]) => v) && !isInitial && (
              <div style={{ position: 'absolute', top: 14, left: '50%', transform: 'translateX(-50%)', zIndex: 10, background: 'rgba(255,255,255,0.93)', backdropFilter: 'blur(12px)', border: `1px solid ${C.border}`, borderRadius: 20, padding: '5px 14px', display: 'flex', alignItems: 'center', gap: 8 }}>
                <div style={{ width: 10, height: 10, borderRadius: '50%', border: `2px solid ${C.primary}30`, borderTopColor: C.primary, animation: 'spin 0.6s linear infinite' }} />
                <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.primary }}>Actualizando datos</span>
              </div>
            )}
          </div>

          {/* Gráfica estadísticas */}
          <div style={{ flexShrink: 0, height: chart ? 148 : 0, overflow: 'hidden', transition: 'height 0.26s cubic-bezier(0.4,0,0.2,1)', background: C.bg, borderTop: `1px solid ${C.border}` }}>
            <div style={{ height: 148, padding: '10px 18px' }}>
              <StatsChart estadisticas={data.estadisticas} loading={loading.estadisticas} />
            </div>
          </div>

          {/* Barra de estado */}
          <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: 22, background: C.bgSoft, borderTop: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', padding: '0 12px', gap: 10, zIndex: 40, fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted }}>
            <span>{Object.values(capas).filter(Boolean).length} capas</span>
            <span style={{ width: 1, height: 10, background: C.border }} />
            <span style={{ color: vista === '3d' ? C.accent : C.textMuted }}>{vista.toUpperCase()}</span>
            <span style={{ width: 1, height: 10, background: C.border }} />
            <span style={{ color: mapStyle === 'dark' ? C.secondary : C.textMuted }}>{mapStyle === 'dark' ? 'Mapa oscuro' : 'Mapa claro'}</span>
            {totalErr > 0 && <>
              <span style={{ width: 1, height: 10, background: C.border }} />
              <span style={{ color: C.danger }}>{totalErr} error{totalErr > 1 ? 'es' : ''}</span>
            </>}
            <span style={{ marginLeft: 'auto' }}>
              USGS · IGP · ANA · CENEPRED · OSM
            </span>
          </div>
        </div>
      </div>

      <style>{`
        @keyframes spin     { to { transform: rotate(360deg) } }
        @keyframes pring    { 0%{transform:scale(1);opacity:.7} 80%,100%{transform:scale(2);opacity:0} }
        @keyframes slideUp  { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:translateY(0)} }
        @keyframes slideInR { from{opacity:0;transform:translateX(16px)} to{opacity:1;transform:translateX(0)} }
      `}</style>
    </>)}
    </div>
  )
}