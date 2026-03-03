// GeoRiesgo Ica — App.tsx v3.0
import { useState, useCallback, useEffect, useRef } from 'react'
import MapView     from './components/MapView'
import LayerPanel  from './components/LayerPanel'
import FilterPanel from './components/FilterPanel'
import StatsChart  from './components/StatsChart'
import { useMapData } from './hooks/useMapData'
import type { CapasActivas, FiltrosSismos, TipoVista } from './types'

const C = {
  primary: '#059669', primaryLt: '#10b981', primaryBg: '#ecfdf5',
  secondary: '#0ea5e9',
  accent: '#6366f1', danger: '#dc2626', dangerBg: '#fef2f2',
  warning: '#f59e0b', warningBg: '#fffbeb',
  bg: '#ffffff', bgSoft: '#f8fafc', bgMuted: '#f1f5f9',
  border: '#e2e8f0',
  text: '#0f172a', textSec: '#475569', textMuted: '#94a3b8',
}

const IconMenu    = () => <svg width="16" height="12" viewBox="0 0 16 12" fill="currentColor"><rect y="0" width="16" height="2" rx="1"/><rect y="5" width="11" height="2" rx="1"/><rect y="10" width="16" height="2" rx="1"/></svg>
const IconChart   = () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><rect x="1" y="7" width="2.5" height="6"/><rect x="5.75" y="4" width="2.5" height="9"/><rect x="10.5" y="1" width="2.5" height="12"/></svg>
const IconLayers  = () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><polygon points="7,1 13,4.5 7,8 1,4.5"/><polyline points="1,8.5 7,12 13,8.5"/></svg>
const IconFilter  = () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><path d="M1 2h12M3 7h8M5 12h4"/></svg>
const IconRefresh = () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"><path d="M12.5 2.5A6 6 0 1 1 8.5 1.5"/><polyline points="8.5,1.5 12.5,1.5 12.5,5.5"/></svg>
const IconX       = () => <svg width="10" height="10" viewBox="0 0 10 10" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round"><line x1="1" y1="1" x2="9" y2="9"/><line x1="9" y1="1" x2="1" y2="9"/></svg>
const IconLocate  = () => <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"><circle cx="7" cy="7" r="3"/><line x1="7" y1="1" x2="7" y2="4"/><line x1="7" y1="10" x2="7" y2="13"/><line x1="1" y1="7" x2="4" y2="7"/><line x1="10" y1="7" x2="13" y2="7"/></svg>

const CAPAS_INIT: CapasActivas = {
  sismos: true, heatmap: false, fallas: true,
  inundaciones: false, riesgo_distritos: true,
  infraestructura: false, extrusion_3d: false,
}

const FILTROS_INIT: FiltrosSismos = {
  mag_min: 3.0, mag_max: 9.5,
  year_start: 1960, year_end: 2030,
}

// ── Toast ──────────────────────────────────────────────────
interface Toast { id: number; type: 'error' | 'success' | 'warn'; message: string }
const TOAST_COLORS: Record<Toast['type'], { fg: string; bg: string }> = {
  error:   { fg: C.danger,  bg: C.dangerBg  },
  success: { fg: C.primary, bg: C.primaryBg },
  warn:    { fg: C.warning, bg: C.warningBg },
}

function ToastList({ toasts, onRemove }: { toasts: Toast[]; onRemove: (id: number) => void }) {
  if (!toasts.length) return null
  return (
    <div style={{ position: 'fixed', bottom: 52, right: 14, zIndex: 300, display: 'flex', flexDirection: 'column', gap: 8 }}>
      {toasts.map(t => (
        <div key={t.id} style={{
          display: 'flex', alignItems: 'flex-start', gap: 10, padding: '10px 14px',
          background: TOAST_COLORS[t.type].bg,
          border: `1px solid ${TOAST_COLORS[t.type].fg}30`,
          borderLeft: `3px solid ${TOAST_COLORS[t.type].fg}`,
          borderRadius: 10, boxShadow: '0 4px 16px rgba(0,0,0,0.08)',
          animation: 'slideInRight 0.22s ease-out', maxWidth: 320,
        }}>
          <span style={{ fontFamily: "'Inter',sans-serif", fontSize: 12, color: C.text, flexGrow: 1, lineHeight: 1.5 }}>{t.message}</span>
          <button onClick={() => onRemove(t.id)} style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.textMuted, padding: 0, marginTop: 1 }}>
            <IconX />
          </button>
        </div>
      ))}
    </div>
  )
}

// ── Loader ─────────────────────────────────────────────────
function Loader({ step }: { step: number }) {
  const STEPS = ['Iniciando sistema', 'Conectando al backend', 'Cargando datos PostGIS', 'Renderizando mapa']
  const pct = ((step + 1) / STEPS.length) * 100
  return (
    <div style={{ position: 'fixed', inset: 0, zIndex: 999, background: C.bg, display: 'flex', alignItems: 'center', justifyContent: 'center', flexDirection: 'column', gap: 28 }}>
      <div style={{ position: 'relative', width: 80, height: 80 }}>
        {[0, 1, 2].map(i => (
          <div key={i} style={{ position: 'absolute', inset: i * 10, borderRadius: '50%', border: '2px solid', borderColor: [C.primary + '90', C.primaryLt + '60', C.secondary + '40'][i], animation: `pulseRing ${1.4 + i * 0.5}s ease-out infinite`, animationDelay: `${i * 0.25}s` }} />
        ))}
      </div>
      <div style={{ textAlign: 'center' }}>
        <p style={{ fontFamily: "'Inter',sans-serif", color: C.text, fontSize: 22, fontWeight: 800, letterSpacing: '-0.02em', marginBottom: 6 }}>
          GeoRiesgo <span style={{ color: C.primary }}>Ica</span>
        </p>
        <p style={{ fontFamily: "'JetBrains Mono',monospace", color: C.textMuted, fontSize: 11, letterSpacing: '0.1em', textTransform: 'uppercase' }}>
          {STEPS[Math.min(step, STEPS.length - 1)]}
        </p>
      </div>
      <div style={{ width: 240, height: 4, background: C.bgMuted, borderRadius: 2 }}>
        <div style={{ height: '100%', width: `${pct}%`, background: `linear-gradient(90deg,${C.primary},${C.secondary})`, borderRadius: 2, transition: 'width 0.4s ease' }} />
      </div>
      <div style={{ display: 'flex', gap: 8 }}>
        {STEPS.map((_, i) => (
          <div key={i} style={{ width: 6, height: 6, borderRadius: '50%', background: i <= step ? C.primary : C.bgMuted, transition: 'background 0.3s' }} />
        ))}
      </div>
    </div>
  )
}

// ── Badge de conteo ────────────────────────────────────────
function SismoBadge({ total, loading }: { total: number; loading: boolean }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 14px', background: C.bgSoft, border: `1px solid ${C.border}`, borderRadius: 999 }}>
      <div style={{ position: 'relative', width: 8, height: 8 }}>
        <div style={{ position: 'absolute', inset: 0, borderRadius: '50%', background: C.primary, animation: 'pulseRing 1.8s ease-out infinite' }} />
        <div style={{ width: 8, height: 8, borderRadius: '50%', background: C.primary }} />
      </div>
      {loading
        ? <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, color: C.textMuted }}>Cargando...</span>
        : <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, fontWeight: 700, color: C.text }}>
            {total.toLocaleString('es-PE')} sismos
          </span>
      }
    </div>
  )
}

// ── Barra de estado ────────────────────────────────────────
function StatusBar({ activeCount, totalErrors, vista }: { activeCount: number; totalErrors: number; vista: TipoVista }) {
  const sep = <span style={{ width: 1, height: 10, background: C.border, display: 'inline-block', margin: '0 4px' }} />
  return (
    <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, height: 24, background: C.bgSoft, borderTop: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', padding: '0 14px', gap: 10, zIndex: 40, fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted }}>
      <span>{activeCount} capas activas</span>
      {sep}
      <span style={{ color: vista === '3d' ? C.accent : C.textMuted }}>{vista.toUpperCase()}</span>
      {totalErrors > 0 && <>{sep}<span style={{ color: C.danger }}>{totalErrors} error{totalErrors > 1 ? 'es' : ''}</span></>}
      <span style={{ marginLeft: 'auto' }}>
        Fuentes: USGS · INEI · GADM · INGEMMET &nbsp;|&nbsp; PostGIS
      </span>
    </div>
  )
}

// ── Popup de feature ───────────────────────────────────────
type FeatKind = 'sismo' | 'distrito' | 'falla' | 'infraestructura' | 'otro'

const RISK_LABELS = ['Muy bajo', 'Bajo', 'Moderado', 'Alto', 'Muy alto']
const RISK_COLORS = [C.primary, '#10b981', C.warning, '#f97316', C.danger]

function detectKind(p: Record<string, unknown>): FeatKind {
  if ('magnitud' in p)    return 'sismo'
  if ('nivel_riesgo' in p) return 'distrito'
  if ('activa' in p)      return 'falla'
  if ('criticidad' in p)  return 'infraestructura'
  return 'otro'
}

function Row({ label, value, color }: { label: string; value: string | number; color?: string }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', gap: 8, marginBottom: 5 }}>
      <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted }}>{label}</span>
      <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 10, fontWeight: 600, color: color ?? C.text, textAlign: 'right' }}>{String(value)}</span>
    </div>
  )
}

function InfoPopup({ info, onClose }: { info: Record<string, unknown>; onClose: () => void }) {
  const raw  = ((info?.object as { properties?: Record<string, unknown> } | undefined)?.properties ?? {}) as Record<string, unknown>
  const kind = detectKind(raw)
  const kindColors: Record<FeatKind, string> = { sismo: C.danger, distrito: C.primary, falla: C.warning, infraestructura: C.accent, otro: C.textMuted }
  const kindLabels: Record<FeatKind, string> = { sismo: 'Sismo', distrito: 'Distrito', falla: 'Falla Geológica', infraestructura: 'Infraestructura', otro: 'Elemento' }
  const ac = kindColors[kind]

  return (
    <div style={{ position: 'absolute', bottom: 48, left: 14, zIndex: 50, width: 270, animation: 'slideUp 0.2s ease-out forwards' }}>
      <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderTop: `3px solid ${ac}`, borderRadius: 14, overflow: 'hidden', boxShadow: '0 8px 32px rgba(0,0,0,0.1)' }}>
        {/* Header popup */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '9px 12px', background: C.bgSoft, borderBottom: `1px solid ${C.border}` }}>
          <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 10, fontWeight: 700, color: ac, letterSpacing: '0.1em' }}>
            {kindLabels[kind]}
          </span>
          <button onClick={onClose} style={{ width: 22, height: 22, borderRadius: 6, background: C.bgMuted, border: `1px solid ${C.border}`, color: C.textMuted, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <IconX />
          </button>
        </div>

        {/* Contenido */}
        <div style={{ padding: '12px 14px' }}>
          {kind === 'sismo' && (() => {
            const mag  = Number(raw.magnitud ?? 0)
            const prof = Number(raw.profundidad_km ?? 0)
            const mc   = mag >= 7 ? C.danger : mag >= 6 ? '#f97316' : mag >= 5 ? C.warning : C.primary
            return (
              <>
                <div style={{ display: 'flex', alignItems: 'baseline', gap: 6, marginBottom: 12, paddingBottom: 10, borderBottom: `1px solid ${C.border}` }}>
                  <span style={{ fontFamily: "'Inter',sans-serif", fontSize: 38, fontWeight: 800, color: mc, lineHeight: 1 }}>{mag.toFixed(1)}</span>
                  <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 12, color: C.textMuted }}>Mw</span>
                </div>
                <Row label="Fecha"         value={String(raw.fecha ?? '')} />
                <Row label="Profundidad"   value={`${prof} km`} color={prof < 30 ? C.danger : prof < 70 ? '#f97316' : C.secondary} />
                <Row label="Tipo"          value={String(raw.tipo_profundidad ?? '')} />
                <Row label="Tipo mag."     value={String(raw.tipo_magnitud ?? '-')} />
                <div style={{ marginTop: 6, padding: '6px 10px', background: C.bgSoft, borderRadius: 8, fontFamily: "'Inter',sans-serif", fontSize: 11, color: C.textSec, lineHeight: 1.4 }}>
                  {String(raw.lugar ?? '')}
                </div>
              </>
            )
          })()}

          {kind === 'distrito' && (() => {
            const nivel = Math.max(1, Math.min(5, Number(raw.nivel_riesgo ?? 1)))
            return (
              <>
                <p style={{ fontFamily: "'Inter',sans-serif", fontSize: 16, fontWeight: 700, color: C.text, marginBottom: 10 }}>
                  {String(raw.nombre ?? '')}
                </p>
                {raw.provincia && <Row label="Provincia" value={String(raw.provincia)} />}
                <div style={{ marginBottom: 6 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5 }}>
                    <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted }}>Nivel de riesgo</span>
                    <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 11, fontWeight: 700, color: RISK_COLORS[nivel - 1] }}>
                      {RISK_LABELS[nivel - 1]}
                    </span>
                  </div>
                  <div style={{ display: 'flex', gap: 3 }}>
                    {RISK_LABELS.map((_, i) => (
                      <div key={i} style={{ flex: 1, height: 6, borderRadius: 3, background: i < nivel ? RISK_COLORS[i] : C.bgMuted }} />
                    ))}
                  </div>
                </div>
                {raw.fuente && <Row label="Fuente" value={String(raw.fuente)} />}
              </>
            )
          })()}

          {kind === 'falla' && (() => {
            const activa = Boolean(raw.activa)
            return (
              <>
                <p style={{ fontFamily: "'Inter',sans-serif", fontSize: 15, fontWeight: 700, color: C.text, marginBottom: 10 }}>
                  {String(raw.nombre ?? '')}
                </p>
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: 6, padding: '4px 10px', background: activa ? C.dangerBg : C.bgMuted, border: `1px solid ${activa ? C.danger + '30' : C.border}`, borderRadius: 999, marginBottom: 8 }}>
                  <div style={{ width: 6, height: 6, borderRadius: '50%', background: activa ? C.danger : C.textMuted }} />
                  <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 10, color: activa ? C.danger : C.textMuted }}>
                    {activa ? 'Falla Activa' : 'Falla Inactiva'}
                  </span>
                </div>
                {raw.tipo       && <Row label="Tipo"       value={String(raw.tipo)} />}
                {raw.longitud_km && <Row label="Longitud" value={`${Number(raw.longitud_km).toFixed(1)} km`} />}
                {raw.fuente     && <Row label="Fuente"     value={String(raw.fuente)} />}
              </>
            )
          })()}

          {(kind === 'infraestructura' || kind === 'otro') && (
            <>
              {raw.nombre && <p style={{ fontFamily: "'Inter',sans-serif", fontSize: 14, fontWeight: 700, color: C.text, marginBottom: 8 }}>{String(raw.nombre)}</p>}
              {Object.entries(raw).filter(([k]) => k !== 'nombre').slice(0, 8).map(([k, v]) => (
                <Row key={k} label={k} value={String(v)} />
              ))}
            </>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Botón header ───────────────────────────────────────────
function Btn({ active, onClick, title, children }: { active: boolean; onClick: () => void; title?: string; children: React.ReactNode }) {
  return (
    <button title={title} onClick={onClick} style={{
      width: 32, height: 32, borderRadius: 8, cursor: 'pointer',
      background:   active ? `${C.primary}15` : C.bgMuted,
      border: `1px solid ${active ? `${C.primary}40` : C.border}`,
      color:  active ? C.primary : C.textMuted,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      transition: 'all 0.18s ease',
    }}>
      {children}
    </button>
  )
}

// ══════════════════════════════════════════════════════════
//  App principal
// ══════════════════════════════════════════════════════════
export default function App() {
  const [capas,      setCapas]      = useState<CapasActivas>(CAPAS_INIT)
  const [filtros,    setFiltros]    = useState<FiltrosSismos>(FILTROS_INIT)
  const [vista,      setVista]      = useState<TipoVista>('2d')
  const [popup,      setPopup]      = useState<Record<string, unknown> | null>(null)
  const [tab,        setTab]        = useState<'capas' | 'filtros'>('capas')
  const [sidebar,    setSidebar]    = useState(true)
  const [chart,      setChart]      = useState(true)
  const [toasts,     setToasts]     = useState<Toast[]>([])
  const [loaderStep, setLoaderStep] = useState(0)

  const { data, loading, errors, recargarSismos } = useMapData()
  const isInitial = loading.sismos && loading.distritos && loading.fallas

  // Avanzar steps del loader
  useEffect(() => {
    if      (!loading.distritos && !loading.fallas) setLoaderStep(3)
    else if (!loading.fallas)                        setLoaderStep(2)
    else if (!loading.distritos)                     setLoaderStep(1)
  }, [loading.distritos, loading.fallas])

  // Mostrar errores como toasts (una sola vez por error)
  const shownErrors = useRef(new Set<string>())
  useEffect(() => {
    Object.entries(errors).forEach(([k, v]) => {
      if (!v) return
      const uid = `${k}:${v}`
      if (shownErrors.current.has(uid)) return
      shownErrors.current.add(uid)
      addToast('error', `"${k}": ${v}`)
    })
  }, [errors])

  function addToast(type: Toast['type'], message: string) {
    const id = Date.now() + Math.random()
    setToasts(p => [...p, { id, type, message }])
    setTimeout(() => setToasts(p => p.filter(t => t.id !== id)), 5500)
  }

  // Atajos de teclado
  useEffect(() => {
    const h = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement) return
      if (e.key === 'l' || e.key === 'L') setSidebar(p => !p)
      if (e.key === 'f' || e.key === 'F') { setSidebar(true); setTab('filtros') }
      if (e.key === 'g' || e.key === 'G') setChart(p => !p)
      if (e.key === 'Escape') setPopup(null)
    }
    window.addEventListener('keydown', h)
    return () => window.removeEventListener('keydown', h)
  }, [])

  const handleFiltros = useCallback((f: FiltrosSismos) => {
    setFiltros(f)
    recargarSismos(f)
  }, [recargarSismos])

  const totalErrors  = Object.values(errors).filter(Boolean).length
  const activeCount  = Object.values(capas).filter(Boolean).length
  const sidebarWidth = sidebar ? 268 : 0

  return (
    <div style={{ width: '100vw', height: '100vh', overflow: 'hidden', background: C.bg, display: 'flex', flexDirection: 'column' }}>
      {isInitial && <Loader step={loaderStep} />}
      <ToastList toasts={toasts} onRemove={id => setToasts(p => p.filter(t => t.id !== id))} />

      {/* ── HEADER ───────────────────────────────────────── */}
      <header style={{ flexShrink: 0, height: 52, display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 14px', background: C.bg, borderBottom: `1px solid ${C.border}`, boxShadow: '0 1px 3px rgba(0,0,0,0.04)', zIndex: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <Btn active={sidebar} onClick={() => setSidebar(p => !p)} title="Sidebar [L]"><IconMenu /></Btn>
          <div style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
            <div style={{ width: 32, height: 32, borderRadius: 9, background: `linear-gradient(135deg,${C.primary},${C.secondary})`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 16, color: 'white', fontWeight: 800, boxShadow: `0 2px 8px ${C.primary}40` }}>G</div>
            <div>
              <div style={{ fontFamily: "'Inter',sans-serif", fontSize: 15, fontWeight: 800, color: C.text, letterSpacing: '-0.02em', lineHeight: 1.1 }}>
                GeoRiesgo <span style={{ color: C.primary }}>Ica</span>
              </div>
              <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted, letterSpacing: '0.12em', textTransform: 'uppercase' }}>
                Riesgo Sísmico · Perú
              </div>
            </div>
          </div>
        </div>

        <SismoBadge total={data.sismos?.features.length ?? 0} loading={loading.sismos} />

        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {loading.sismos && !isInitial && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '4px 10px', background: C.primaryBg, border: `1px solid ${C.primary}30`, borderRadius: 8 }}>
              <div style={{ width: 10, height: 10, borderRadius: '50%', border: `2px solid ${C.primary}30`, borderTopColor: C.primary, animation: 'spin 0.7s linear infinite' }} />
              <span style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 10, color: C.primary }}>Actualizando</span>
            </div>
          )}
          {/* Toggle 2D / 3D */}
          <div style={{ display: 'flex', background: C.bgMuted, border: `1px solid ${C.border}`, borderRadius: 8, padding: 2 }}>
            {(['2d', '3d'] as TipoVista[]).map(v => (
              <button key={v} onClick={() => setVista(v)} style={{
                padding: '3px 10px', borderRadius: 6, border: 'none', cursor: 'pointer',
                fontFamily: "'JetBrains Mono',monospace", fontSize: 10, fontWeight: 700, textTransform: 'uppercase',
                background: vista === v ? C.bg : 'transparent',
                color:      vista === v ? C.text : C.textMuted,
                boxShadow:  vista === v ? '0 1px 2px rgba(0,0,0,0.06)' : 'none',
                transition: 'all 0.2s',
              }}>{v}</button>
            ))}
          </div>
          <Btn active={chart}              onClick={() => setChart(p => !p)}                            title="Gráfica [G]"><IconChart /></Btn>
          <Btn active={tab==='capas'&&sidebar} onClick={() => { setSidebar(true); setTab('capas') }}   title="Capas [L]"><IconLayers /></Btn>
          <Btn active={tab==='filtros'&&sidebar} onClick={() => { setSidebar(true); setTab('filtros') }} title="Filtros [F]"><IconFilter /></Btn>
          <Btn active={false} onClick={() => { recargarSismos(filtros); addToast('success', 'Recargando sismos...') }} title="Recargar"><IconRefresh /></Btn>
        </div>
      </header>

      {/* ── CUERPO ───────────────────────────────────────── */}
      <div style={{ flex: 1, display: 'flex', overflow: 'hidden', position: 'relative' }}>

        {/* Sidebar */}
        <aside style={{ flexShrink: 0, width: sidebarWidth, overflow: 'hidden', transition: 'width 0.28s cubic-bezier(0.4,0,0.2,1)', background: C.bg, borderRight: `1px solid ${C.border}`, display: 'flex', flexDirection: 'column', zIndex: 10 }}>
          <div style={{ width: 268, height: '100%', display: 'flex', flexDirection: 'column', padding: '14px 14px 0', overflowY: 'auto' }}>
            {/* Tabs */}
            <div style={{ display: 'flex', gap: 3, background: C.bgMuted, borderRadius: 10, padding: 3, marginBottom: 18, flexShrink: 0 }}>
              {(['capas', 'filtros'] as const).map(t => (
                <button key={t} onClick={() => setTab(t)} style={{
                  flex: 1, padding: '6px 0', borderRadius: 8, border: 'none', cursor: 'pointer',
                  fontFamily: "'JetBrains Mono',monospace", fontSize: 10, fontWeight: 700,
                  textTransform: 'uppercase', letterSpacing: '0.07em',
                  background: tab === t ? C.bg : 'transparent',
                  color: tab === t ? C.text : C.textMuted,
                  boxShadow: tab === t ? '0 1px 3px rgba(0,0,0,0.06)' : 'none',
                  transition: 'all 0.2s', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 5,
                }}>
                  {t === 'capas' ? <><IconLayers /> Capas</> : <><IconFilter /> Filtros</>}
                </button>
              ))}
            </div>

            <div style={{ flex: 1, overflowY: 'auto', paddingBottom: 14 }}>
              {tab === 'capas'
                ? <LayerPanel capas={capas} onChange={setCapas} />
                : <FilterPanel filtros={filtros} onChange={handleFiltros} />
              }
            </div>

            {/* Footer sidebar */}
            <div style={{ paddingTop: 10, paddingBottom: 14, borderTop: `1px solid ${C.border}`, flexShrink: 0 }}>
              <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted, lineHeight: 2.2, letterSpacing: '0.04em' }}>
                <a href="https://earthquake.usgs.gov" target="_blank" rel="noreferrer" style={{ color: C.primary, textDecoration: 'none' }}>USGS</a>
                {'  ·  '}
                <a href="https://www.inei.gob.pe" target="_blank" rel="noreferrer" style={{ color: C.secondary, textDecoration: 'none' }}>INEI</a>
                {'  ·  '}
                <a href="https://geocatmin.ingemmet.gob.pe" target="_blank" rel="noreferrer" style={{ color: C.accent, textDecoration: 'none' }}>INGEMMET</a>
                <br />
                <span style={{ color: C.textMuted }}>PostGIS · deck.gl · MapLibre GL</span>
              </p>
              <p style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 8, color: C.textMuted, marginTop: 4 }}>
                [L] sidebar  [F] filtros  [G] gráfica  [Esc] cerrar
              </p>
            </div>
          </div>
        </aside>

        {/* Área del mapa */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden', position: 'relative' }}>
          {/* Mapa */}
          <div style={{ flex: 1, position: 'relative' }}>
            <MapView
              sismos={data.sismos}
              distritos={data.distritos}
              fallas={data.fallas}
              inundaciones={data.inundaciones}
              infraestructura={data.infraestructura}
              capas={capas}
              vista={vista}
              onClickFeature={info => setPopup(info)}
            />

            {popup && <InfoPopup info={popup} onClose={() => setPopup(null)} />}

            {/* Badge ubicación */}
            <div style={{ position: 'absolute', top: 14, left: 14, zIndex: 10, background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(12px)', border: `1px solid ${C.border}`, borderRadius: 12, padding: '7px 12px', boxShadow: '0 2px 8px rgba(0,0,0,0.06)' }}>
              <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.12em', marginBottom: 3 }}>
                {vista === '3d' ? 'Perspectiva 3D' : 'Vista Aérea 2D'}
              </div>
              <div style={{ fontFamily: "'Inter',sans-serif", fontSize: 13, fontWeight: 700, color: C.text }}>Ica, Perú</div>
              <div style={{ fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textMuted }}>14.07°S  75.73°O</div>
            </div>

            {/* Botón recentrar */}
            <button
              title="Recentrar en Ica"
              onClick={() => window.dispatchEvent(new CustomEvent('georiesgo:recenter'))}
              style={{ position: 'absolute', top: 14, right: 56, zIndex: 10, width: 34, height: 34, borderRadius: 9, background: 'rgba(255,255,255,0.92)', backdropFilter: 'blur(12px)', border: `1px solid ${C.border}`, color: C.textSec, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', boxShadow: '0 2px 8px rgba(0,0,0,0.06)' }}
            >
              <IconLocate />
            </button>

            {/* Chips capas activas */}
            <div style={{ position: 'absolute', bottom: 36, right: 12, zIndex: 10, display: 'flex', flexDirection: 'column', gap: 3, alignItems: 'flex-end' }}>
              {Object.entries(capas).filter(([, v]) => v).map(([k]) => (
                <div key={k} style={{ background: 'rgba(255,255,255,0.9)', backdropFilter: 'blur(8px)', border: `1px solid ${C.border}`, borderRadius: 6, padding: '2px 8px', fontFamily: "'JetBrains Mono',monospace", fontSize: 9, color: C.textSec }}>
                  {k.replace(/_/g, ' ')}
                </div>
              ))}
            </div>
          </div>

          {/* Gráfica */}
          <div style={{ flexShrink: 0, height: chart ? 148 : 0, overflow: 'hidden', transition: 'height 0.28s cubic-bezier(0.4,0,0.2,1)', background: C.bg, borderTop: `1px solid ${C.border}` }}>
            <div style={{ height: 148, padding: '10px 20px' }}>
              <StatsChart estadisticas={data.estadisticas} loading={loading.estadisticas} />
            </div>
          </div>

          {/* Barra de estado (dentro del área del mapa para no solapar sidebar) */}
          <StatusBar activeCount={activeCount} totalErrors={totalErrors} vista={vista} />
        </div>
      </div>

      <style>{`
        @keyframes spin         { to { transform: rotate(360deg) } }
        @keyframes pulseRing    { 0%{transform:scale(1);opacity:.7} 80%,100%{transform:scale(1.8);opacity:0} }
        @keyframes slideUp      { from{opacity:0;transform:translateY(12px)} to{opacity:1;transform:translateY(0)} }
        @keyframes slideInRight { from{opacity:0;transform:translateX(20px)} to{opacity:1;transform:translateX(0)} }
      `}</style>
    </div>
  )
}