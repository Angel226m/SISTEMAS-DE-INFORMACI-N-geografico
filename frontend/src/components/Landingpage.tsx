// ══════════════════════════════════════════════════════════
// LandingPage.tsx — GeoRiesgo Perú v4.0
// ══════════════════════════════════════════════════════════
import { useEffect, useRef, useState, useCallback } from 'react'

const C = {
  bg:        '#ffffff', bgSoft: '#f8fafc',
  primary:   '#059669', primaryLt: '#10b981', primaryGlow: 'rgba(5,150,105,0.10)',
  secondary: '#0ea5e9', accent: '#0891b2',
  amber:     '#f59e0b', violet: '#7c3aed',
  text:      '#0f172a', textSec: '#334155', textMuted: '#64748b',
  border:    '#e2e8f0',
}

const STATS = [
  { value: '+2.5M', label: 'sismos catalogados',       sub: 'desde 1900',     color: C.primary   },
  { value: '9.5',   label: 'magnitud máx registrada',  sub: 'escala USGS',    color: C.secondary },
  { value: '32',    label: 'fallas geológicas',        sub: 'INGEMMET/IGP',   color: C.amber     },
  { value: '25',    label: 'departamentos cubiertos',  sub: 'cobertura Perú', color: C.accent    },
]

const FEATURES = [
  { icon: '◉', title: 'Mapa Sísmico Nacional',     color: C.primary,   border: '#a7f3d0', bg: 'linear-gradient(135deg,#f0fdf4,#ecfdf5)', desc: 'Catálogo USGS con +2.5M eventos desde 1900. Filtros por magnitud, profundidad, región y año. Heatmap de densidad en tiempo real.' },
  { icon: '≋', title: 'Zonas Tsunami & Inundación', color: '#06b6d4',   border: '#a5f3fc', bg: 'linear-gradient(135deg,#ecfeff,#f0fdfa)', desc: 'Zonas de inundación ANA/CENEPRED y mapas tsunami PREDES/IGP. Altura de ola estimada, tiempo de arribo y períodos de retorno.' },
  { icon: '⌗', title: 'Fallas Geológicas',          color: C.amber,     border: '#fde68a', bg: 'linear-gradient(135deg,#fffbeb,#fef3c7)', desc: '32 fallas neotectónicas verificadas: Subducción Nazca, Sistema Lima, Cordillera Blanca, Cusco-Vilcanota. Coordenadas Audin et al. 2008.' },
  { icon: '◈', title: 'Índice de Riesgo Distrital', color: C.accent,    border: '#a5f3fc', bg: 'linear-gradient(135deg,#f0f9ff,#e0f2fe)', desc: 'Score compuesto por distrito: actividad sísmica histórica, proximidad a fallas activas, zonas inundables y vulnerabilidad INEI.' },
  { icon: '⊕', title: 'Infraestructura Crítica',    color: '#6366f1',   border: '#c7d2fe', bg: 'linear-gradient(135deg,#eef2ff,#ede9fe)', desc: 'Hospitales, escuelas, aeropuertos, puertos, estaciones de poder y plantas de agua. Cobertura OSM + 23 instalaciones verificadas.' },
  { icon: '◎', title: 'Estaciones de Monitoreo',    color: '#10b981',   border: '#a7f3d0', bg: 'linear-gradient(135deg,#f0fdf4,#ecfeff)', desc: 'Red sísmica IGP nacional, estaciones SENAMHI y puntos hidrométricos ANA. Altitud, institución y estado operativo.' },
]

const ENDPOINTS = [
  { method: 'GET',  color: C.primary,   path: '/api/v1/sismos',            desc: 'Catálogo sísmico con filtros: mag, año, región, profundidad + paginación' },
  { method: 'GET',  color: C.secondary, path: '/api/v1/sismos/heatmap',     desc: 'Grid de densidad pre-calculado — vista materializada PostGIS < 40ms' },
  { method: 'GET',  color: C.accent,    path: '/api/v1/fallas',             desc: '32 fallas con tipo, mecanismo, magnitud máx y referencia científica' },
  { method: 'GET',  color: '#0284c7',   path: '/api/v1/inundaciones',       desc: 'Zonas ANA/CENEPRED con profundidad máx, cuenca y período retorno' },
  { method: 'GET',  color: '#0e7490',   path: '/api/v1/tsunamis',           desc: 'Zonas costeras con altura ola, tiempo arribo y nivel de riesgo' },
  { method: 'GET',  color: C.amber,     path: '/api/v1/sismos/estadisticas',desc: 'Estadísticas anuales con M5+, M6+, M7+ desde vistas materializadas' },
  { method: 'GET',  color: '#6366f1',   path: '/api/v1/infraestructura',    desc: 'Infraestructura crítica con filtros por tipo, criticidad y región' },
  { method: 'GET',  color: '#10b981',   path: '/api/v1/estaciones',         desc: 'Estaciones sísmicas, meteorológicas e hidrométricas activas' },
]

const FUENTES = [
  { name: 'USGS',     desc: 'Catálogo sísmico global',   color: C.primary   },
  { name: 'IGP',      desc: 'Red sísmica nacional',      color: C.secondary },
  { name: 'INGEMMET', desc: 'Fallas neotectónicas',      color: C.amber     },
  { name: 'INEI',     desc: 'Límites distritales',       color: C.accent    },
  { name: 'ANA',      desc: 'Zonas inundables',          color: '#0284c7'   },
  { name: 'CENEPRED', desc: 'Riesgo de desastres',       color: '#0e7490'   },
  { name: 'PREDES',   desc: 'Tsunamis costeros',         color: '#7c3aed'   },
  { name: 'SENAMHI',  desc: 'Estaciones meteorológicas', color: '#f97316'   },
  { name: 'GADM 4.1', desc: 'Fronteras administrativas', color: '#6366f1'   },
  { name: 'OSM',      desc: 'Infraestructura crítica',   color: '#10b981'   },
]

// ── Reveal on scroll ──────────────────────────────────────
function useVisible(threshold = 0.1) {
  const ref = useRef<HTMLDivElement>(null)
  const [vis, setVis] = useState(false)
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const obs = new IntersectionObserver(([e]) => {
      if (e.isIntersecting) { setVis(true); obs.disconnect() }
    }, { threshold })
    obs.observe(el)
    return () => obs.disconnect()
  }, [threshold])
  return { ref, vis }
}

function Reveal({ children, delay = 0 }: { children: React.ReactNode; delay?: number }) {
  const { ref, vis } = useVisible()
  return (
    <div ref={ref} style={{
      opacity: vis ? 1 : 0,
      transform: vis ? 'translateY(0)' : 'translateY(22px)',
      transition: `opacity 0.6s ease ${delay}ms, transform 0.65s cubic-bezier(.22,.68,0,1.2) ${delay}ms`,
      willChange: 'opacity, transform',
    }}>
      {children}
    </div>
  )
}

function Wave({ color = C.primary, opacity = 0.15, delay = '0s', dur = '4s' }) {
  return (
    <svg viewBox="0 0 600 50" preserveAspectRatio="none" style={{ width: '100%', height: 44, display: 'block' }}>
      <polyline
        points="0,25 40,25 58,8 76,42 94,12 112,38 130,20 148,32 166,14 184,36 202,22 220,28 245,28 263,10 281,44 299,14 317,38 335,18 353,34 371,22 389,28 414,28 432,12 450,40 468,16 486,32 504,20 522,30 540,18 558,28 576,25 600,25"
        fill="none" stroke={color} strokeWidth="1.6" strokeOpacity={opacity}
        strokeDasharray="900"
        style={{ animation: `swave ${dur} linear infinite`, animationDelay: delay }}
      />
    </svg>
  )
}

function Navbar({ onEnter, scrolled }: { onEnter: () => void; scrolled: boolean }) {
  return (
    <nav style={{
      position: 'sticky', top: 0, zIndex: 100,
      padding: '0 28px', height: 58,
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      background: scrolled ? 'rgba(255,255,255,0.94)' : 'transparent',
      backdropFilter: scrolled ? 'blur(16px)' : 'none',
      borderBottom: scrolled ? `1px solid ${C.border}` : '1px solid transparent',
      transition: 'all 0.3s ease',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
        <div style={{ width: 30, height: 30, borderRadius: 9, background: `linear-gradient(135deg,${C.primary},${C.secondary})`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 14, color: 'white', fontWeight: 900, boxShadow: '0 2px 8px rgba(5,150,105,0.28)' }}>G</div>
        <div style={{ lineHeight: 1 }}>
          <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 14, fontWeight: 800, color: C.text, letterSpacing: '-0.02em' }}>GeoRiesgo</span>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.primary, marginLeft: 5, letterSpacing: '0.05em' }}>PERÚ</span>
        </div>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: 22 }}>
        {[['funciones','Funciones'],['api','API'],['datos','Datos']].map(([id, label]) => (
          <a key={id} href={`#${id}`} style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 13, fontWeight: 500, color: C.textSec, textDecoration: 'none', transition: 'color 0.15s' }}
            onMouseEnter={e => (e.currentTarget.style.color = C.primary)}
            onMouseLeave={e => (e.currentTarget.style.color = C.textSec)}
          >{label}</a>
        ))}
        <button onClick={onEnter} style={{ background: `linear-gradient(135deg,${C.primary},${C.primaryLt})`, color: 'white', border: 'none', padding: '8px 20px', borderRadius: 10, fontFamily: "'DM Sans',sans-serif", fontSize: 13, fontWeight: 700, cursor: 'pointer', boxShadow: '0 2px 10px rgba(5,150,105,0.28)', transition: 'all 0.2s ease' }}
          onMouseEnter={e => { (e.currentTarget.style.transform = 'translateY(-1px)'); (e.currentTarget.style.boxShadow = '0 5px 18px rgba(5,150,105,0.4)') }}
          onMouseLeave={e => { (e.currentTarget.style.transform = ''); (e.currentTarget.style.boxShadow = '0 2px 10px rgba(5,150,105,0.28)') }}
        >Abrir Mapa →</button>
      </div>
    </nav>
  )
}

interface Props { onEnter: () => void }

export default function LandingPage({ onEnter }: Props) {
  const wrapRef           = useRef<HTMLDivElement>(null)
  const [scrollY, setSY]  = useState(0)
  const [mounted, setM]   = useState(false)
  const onScroll = useCallback(() => setSY(wrapRef.current?.scrollTop ?? 0), [])

  useEffect(() => {
    setM(true)
    const el = wrapRef.current
    el?.addEventListener('scroll', onScroll, { passive: true })
    return () => el?.removeEventListener('scroll', onScroll)
  }, [onScroll])

  const heroParallax = scrollY * 0.22
  const heroOpacity  = Math.max(0, 1 - scrollY / 420)
  const scrolled     = scrollY > 28

  return (
    <div ref={wrapRef} style={{ position: 'absolute', inset: 0, overflowY: 'scroll', overflowX: 'hidden', fontFamily: "'DM Sans','Inter',sans-serif", background: C.bg, color: C.text, scrollBehavior: 'smooth' }}>
      <style>{`
        @keyframes swave  { from { stroke-dashoffset:900 } to { stroke-dashoffset:0 } }
        @keyframes floatY { 0%,100% { transform:translateY(0) } 50% { transform:translateY(-10px) } }
        @keyframes fadeUp { from { opacity:0; transform:translateY(16px) } to { opacity:1; transform:translateY(0) } }
        @keyframes blink  { 0%,100% { opacity:1 } 50% { opacity:.3 } }
        .feat-card:hover  { transform:translateY(-5px) !important; box-shadow:0 18px 48px rgba(0,0,0,0.09) !important; }
        .ep-row:hover     { background: #f8fafc !important; }
        .src-chip:hover   { transform:translateY(-2px); box-shadow:0 4px 14px rgba(0,0,0,0.06) !important; }
        #funciones,#api,#datos { scroll-margin-top:64px }
      `}</style>

      <Navbar onEnter={onEnter} scrolled={scrolled} />

      {/* ═══ HERO ════════════════════════════════════════ */}
      <section style={{ minHeight: 'calc(100vh - 58px)', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', position: 'relative', overflow: 'hidden', paddingBottom: 90 }}>
        {/* Fondo */}
        <div style={{ position: 'absolute', inset: 0, pointerEvents: 'none' }}>
          <div style={{ position: 'absolute', top: '-8%', left: '3%', width: 500, height: 500, background: `radial-gradient(circle,${C.primaryGlow} 0%,transparent 68%)`, transform: `translateY(${heroParallax * 0.3}px)` }} />
          <div style={{ position: 'absolute', bottom: '-5%', right: '5%', width: 420, height: 420, background: 'radial-gradient(circle,rgba(14,165,233,0.08) 0%,transparent 68%)', transform: `translateY(${-heroParallax * 0.18}px)` }} />
          <svg style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', opacity: 0.028 }}>
            <defs><pattern id="g0" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M40 0L0 0 0 40" fill="none" stroke={C.text} strokeWidth="1"/></pattern></defs>
            <rect width="100%" height="100%" fill="url(#g0)"/>
          </svg>
          {[
            { s: 72,  t: '14%', l: '6%',  bg: C.primaryGlow,               d: '6s',  dl: '0s'   },
            { s: 44,  t: '70%', l: '4%',  bg: 'rgba(14,165,233,0.07)',      d: '7.5s', dl: '1.5s' },
            { s: 88,  t: '16%', r: '5%',  bg: 'rgba(8,145,178,0.06)',       d: '8s',  dl: '0.8s' },
            { s: 52,  t: '76%', r: '8%',  bg: C.primaryGlow,               d: '5.5s', dl: '2.2s' },
          ].map((b, i) => (
            <div key={i} style={{ position: 'absolute', width: b.s, height: b.s, borderRadius: '50%', background: b.bg, top: b.t, left: (b as any).l, right: (b as any).r, animation: `floatY ${b.d} ease-in-out infinite`, animationDelay: b.dl }} />
          ))}
          <div style={{ position: 'absolute', bottom: 52, left: 0, right: 0, transform: `translateY(${heroParallax * 0.4}px)` }}>
            <Wave color={C.primary}   opacity={0.14} delay="0s"   dur="4.2s" />
            <Wave color={C.secondary} opacity={0.09} delay="1.4s" dur="5.8s" />
            <Wave color={C.accent}    opacity={0.06} delay="0.7s" dur="7s"   />
          </div>
        </div>

        {/* Contenido */}
        <div style={{ position: 'relative', zIndex: 2, textAlign: 'center', padding: '0 24px', maxWidth: 820, opacity: mounted ? heroOpacity : 0, transform: `translateY(${mounted ? -heroParallax * 0.1 : 14}px)`, transition: mounted ? 'opacity 0.08s linear' : 'opacity 0.5s ease,transform 0.5s ease' }}>
          <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: C.bgSoft, border: `1px solid ${C.border}`, borderRadius: 40, padding: '6px 16px', marginBottom: 26, animation: 'fadeUp 0.6s ease both' }}>
            <span style={{ width: 7, height: 7, borderRadius: '50%', background: C.primary, display: 'inline-block', animation: 'blink 1.8s ease-in-out infinite' }} />
            <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted, letterSpacing: '0.14em', textTransform: 'uppercase' }}>Monitoreo sísmico nacional · v4.0</span>
          </div>

          <h1 style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 'clamp(36px,6vw,76px)', fontWeight: 800, margin: '0 0 18px', lineHeight: 1.04, letterSpacing: '-0.035em', animation: 'fadeUp 0.6s ease 0.1s both' }}>
            Riesgo Sísmico{' '}
            <span style={{ background: `linear-gradient(135deg,${C.primary},${C.secondary})`, WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent', backgroundClip: 'text' }}>Perú</span>
          </h1>

          <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 'clamp(15px,2vw,18px)', color: C.textMuted, lineHeight: 1.68, maxWidth: 580, margin: '0 auto 36px', animation: 'fadeUp 0.6s ease 0.18s both' }}>
            Plataforma geoespacial nacional: +2.5M sismos, 32 fallas activas, zonas tsunami, inundaciones,
            infraestructura crítica y red de monitoreo sobre PostGIS.
          </p>

          <div style={{ display: 'flex', gap: 12, justifyContent: 'center', flexWrap: 'wrap', animation: 'fadeUp 0.6s ease 0.28s both' }}>
            <button onClick={onEnter} style={{ background: `linear-gradient(135deg,${C.primary},${C.primaryLt})`, color: 'white', border: 'none', padding: '14px 38px', borderRadius: 14, fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 700, cursor: 'pointer', boxShadow: '0 4px 24px rgba(5,150,105,0.3)', transition: 'all 0.22s ease' }}
              onMouseEnter={e => { (e.currentTarget.style.transform = 'translateY(-2px)'); (e.currentTarget.style.boxShadow = '0 8px 34px rgba(5,150,105,0.42)') }}
              onMouseLeave={e => { (e.currentTarget.style.transform = ''); (e.currentTarget.style.boxShadow = '0 4px 24px rgba(5,150,105,0.3)') }}
            >Explorar el Mapa</button>
            <a href="#funciones" style={{ background: 'transparent', color: C.primary, border: `1.5px solid ${C.primary}45`, padding: '13px 28px', borderRadius: 14, fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 600, cursor: 'pointer', textDecoration: 'none', display: 'inline-flex', alignItems: 'center', transition: 'all 0.22s ease' }}
              onMouseEnter={e => { (e.currentTarget.style.borderColor = C.primary); (e.currentTarget.style.background = C.primaryGlow) }}
              onMouseLeave={e => { (e.currentTarget.style.borderColor = C.primary + '45'); (e.currentTarget.style.background = 'transparent') }}
            >Ver funciones ↓</a>
          </div>
        </div>

        {/* Stats strip */}
        <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, background: 'rgba(255,255,255,0.9)', backdropFilter: 'blur(14px)', borderTop: `1px solid ${C.border}`, display: 'grid', gridTemplateColumns: 'repeat(4,1fr)', animation: 'fadeUp 0.7s ease 0.4s both' }}>
          {STATS.map((s, i) => (
            <div key={i} style={{ padding: '16px 22px', borderRight: i < 3 ? `1px solid ${C.border}` : 'none' }}>
              <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 24, fontWeight: 600, color: s.color, lineHeight: 1, marginBottom: 4 }}>{s.value}</div>
              <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, color: C.textSec, fontWeight: 500, marginBottom: 1 }}>{s.label}</div>
              <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, color: C.textMuted, letterSpacing: '0.09em', textTransform: 'uppercase' }}>{s.sub}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ═══ FUNCIONES ═══════════════════════════════════ */}
      <section id="funciones" style={{ padding: '96px 24px', maxWidth: 1160, margin: '0 auto' }}>
        <Reveal>
          <div style={{ textAlign: 'center', marginBottom: 52 }}>
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.primary, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: 12 }}>Capacidades del sistema</div>
            <h2 style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 'clamp(26px,3.5vw,44px)', fontWeight: 800, color: C.text, margin: 0, letterSpacing: '-0.025em', lineHeight: 1.12 }}>
              Análisis geoespacial de <span style={{ color: C.primary }}>riesgo sísmico</span>
            </h2>
          </div>
        </Reveal>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(310px,1fr))', gap: 18 }}>
          {FEATURES.map((f, i) => (
            <Reveal key={i} delay={i * 50}>
              <div className="feat-card" style={{ background: f.bg, border: `1px solid ${f.border}`, borderRadius: 20, padding: '26px 26px 24px', transition: 'all 0.25s cubic-bezier(.22,.68,0,1.2)', cursor: 'default', boxShadow: '0 2px 10px rgba(0,0,0,0.04)' }}>
                <div style={{ width: 40, height: 40, borderRadius: 12, background: `${f.color}14`, border: `1px solid ${f.color}28`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18, color: f.color, marginBottom: 14 }}>{f.icon}</div>
                <h3 style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 14.5, fontWeight: 700, color: C.text, margin: '0 0 9px', lineHeight: 1.3 }}>{f.title}</h3>
                <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 13, color: C.textMuted, margin: 0, lineHeight: 1.65 }}>{f.desc}</p>
              </div>
            </Reveal>
          ))}
        </div>
      </section>

      {/* ═══ API ═════════════════════════════════════════ */}
      <section id="api" style={{ padding: '80px 24px 96px', background: C.bgSoft, borderTop: `1px solid ${C.border}`, borderBottom: `1px solid ${C.border}` }}>
        <div style={{ maxWidth: 960, margin: '0 auto' }}>
          <Reveal>
            <div style={{ textAlign: 'center', marginBottom: 48 }}>
              <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.secondary, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: 12 }}>Backend FastAPI v4.0</div>
              <h2 style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 'clamp(24px,3vw,38px)', fontWeight: 800, color: C.text, margin: '0 0 12px', letterSpacing: '-0.02em' }}>Endpoints espaciales PostGIS</h2>
              <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 14, color: C.textMuted, maxWidth: 480, margin: '0 auto' }}>
                ETags para caché HTTP · vistas materializadas · orjson · GZip · paginación
              </p>
            </div>
          </Reveal>
          <Reveal delay={60}>
            <div style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 16, overflow: 'hidden', boxShadow: '0 2px 8px rgba(0,0,0,0.04)' }}>
              {ENDPOINTS.map((ep, i) => (
                <div key={i} className="ep-row" style={{ display: 'flex', alignItems: 'flex-start', gap: 12, padding: '14px 18px', borderBottom: i < ENDPOINTS.length - 1 ? `1px solid ${C.border}` : 'none', transition: 'background 0.15s', cursor: 'default', background: 'transparent' }}>
                  <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9, fontWeight: 700, background: `${ep.color}16`, color: ep.color, border: `1px solid ${ep.color}30`, padding: '3px 7px', borderRadius: 5, flexShrink: 0, marginTop: 1, letterSpacing: '0.05em' }}>{ep.method}</span>
                  <div style={{ minWidth: 0 }}>
                    <code style={{ fontFamily: "'DM Mono',monospace", fontSize: 11.5, color: C.text, fontWeight: 600, display: 'block', marginBottom: 4, wordBreak: 'break-all' }}>{ep.path}</code>
                    <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, color: C.textMuted, margin: 0, lineHeight: 1.5 }}>{ep.desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </Reveal>
        </div>
      </section>

      {/* ═══ DATOS ═══════════════════════════════════════ */}
      <section id="datos" style={{ padding: '96px 24px', maxWidth: 1100, margin: '0 auto' }}>
        <Reveal>
          <div style={{ textAlign: 'center', marginBottom: 48 }}>
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.accent, letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: 12 }}>Fuentes oficiales verificadas</div>
            <h2 style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 'clamp(24px,3.5vw,40px)', fontWeight: 800, color: C.text, margin: 0, letterSpacing: '-0.02em' }}>Datos científicos de calidad</h2>
          </div>
        </Reveal>
        <Reveal delay={60}>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, justifyContent: 'center', marginBottom: 56 }}>
            {FUENTES.map((f, i) => (
              <div key={i} className="src-chip" style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 12, padding: '9px 16px', display: 'flex', alignItems: 'center', gap: 9, cursor: 'default', transition: 'all 0.2s ease', boxShadow: '0 1px 3px rgba(0,0,0,0.04)' }}>
                <div style={{ width: 8, height: 8, borderRadius: '50%', background: f.color }} />
                <div>
                  <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 11.5, fontWeight: 700, color: C.text }}>{f.name}</div>
                  <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 11, color: C.textMuted }}>{f.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </Reveal>

        <Reveal delay={120}>
          <div style={{ background: 'linear-gradient(135deg,#f0fdf4,#f0f9ff)', border: `1px solid ${C.border}`, borderRadius: 22, padding: '32px 38px' }}>
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: C.textMuted, letterSpacing: '0.16em', textTransform: 'uppercase', marginBottom: 22 }}>Stack tecnológico</div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(185px,1fr))', gap: 22 }}>
              {[
                { cat: 'Frontend',      color: C.primary,   items: ['React 18 + TypeScript', 'MapLibre GL + deck.gl', 'Recharts · DM Sans/Mono', 'Cache ETag en memoria'] },
                { cat: 'Backend',       color: C.secondary, items: ['FastAPI + asyncpg', 'orjson · GZip middleware', 'ETag / Cache-Control', 'Uvicorn workers dinámicos'] },
                { cat: 'Base de datos', color: C.accent,    items: ['PostgreSQL 16 + PostGIS 3.4', 'Vistas materializadas', 'Índices GiST + GIN trigram', 'Funciones espaciales'] },
                { cat: 'ETL & Docker',  color: '#0284c7',   items: ['Python 3.12-slim (~180MB)', 'Shapely 2.0 (sin GDAL)', 'Tenacity retry exp.', 'Multi-source incremental'] },
              ].map((s, i) => (
                <div key={i}>
                  <div style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12, fontWeight: 700, color: s.color, marginBottom: 11 }}>{s.cat}</div>
                  {s.items.map((item, j) => (
                    <div key={j} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 7 }}>
                      <div style={{ width: 4, height: 4, borderRadius: '50%', background: s.color, opacity: 0.4, flexShrink: 0 }} />
                      <span style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 12.5, color: C.textSec }}>{item}</span>
                    </div>
                  ))}
                </div>
              ))}
            </div>
          </div>
        </Reveal>
      </section>

      {/* ═══ CTA ═════════════════════════════════════════ */}
      <section style={{ padding: '78px 24px 92px', background: `linear-gradient(155deg,${C.primary} 0%,#0a7a55 38%,${C.secondary} 100%)`, textAlign: 'center', position: 'relative', overflow: 'hidden' }}>
        <svg style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', opacity: 0.06, pointerEvents: 'none' }}>
          <defs><pattern id="ctag" width="48" height="48" patternUnits="userSpaceOnUse"><path d="M48 0L0 0 0 48" fill="none" stroke="white" strokeWidth="1"/></pattern></defs>
          <rect width="100%" height="100%" fill="url(#ctag)"/>
        </svg>
        <div style={{ position: 'absolute', bottom: 0, left: 0, right: 0, opacity: 0.18, pointerEvents: 'none' }}>
          <Wave color="white" opacity={1} delay="0s"   dur="4s" />
          <Wave color="white" opacity={1} delay="1.6s" dur="6s" />
        </div>
        <Reveal>
          <div style={{ position: 'relative', zIndex: 2 }}>
            <div style={{ fontFamily: "'DM Mono',monospace", fontSize: 10, color: 'rgba(255,255,255,0.55)', letterSpacing: '0.2em', textTransform: 'uppercase', marginBottom: 16 }}>Listo para usar</div>
            <h2 style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 'clamp(28px,4.5vw,54px)', fontWeight: 800, color: 'white', margin: '0 0 16px', letterSpacing: '-0.025em', lineHeight: 1.08 }}>Explora el mapa ahora</h2>
            <p style={{ fontFamily: "'DM Sans',sans-serif", fontSize: 16, color: 'rgba(255,255,255,0.72)', maxWidth: 450, margin: '0 auto 34px', lineHeight: 1.65 }}>
              Visualiza +2.5M sismos, 32 fallas activas, zonas tsunami e inundación,
              infraestructura crítica y estaciones de monitoreo de todo el Perú.
            </p>
            <button onClick={onEnter} style={{ background: 'white', color: C.primary, border: 'none', padding: '15px 46px', borderRadius: 14, fontFamily: "'DM Sans',sans-serif", fontSize: 15, fontWeight: 800, cursor: 'pointer', boxShadow: '0 8px 32px rgba(0,0,0,0.17)', transition: 'all 0.22s ease' }}
              onMouseEnter={e => { (e.currentTarget.style.transform = 'translateY(-2px)'); (e.currentTarget.style.boxShadow = '0 14px 40px rgba(0,0,0,0.24)') }}
              onMouseLeave={e => { (e.currentTarget.style.transform = ''); (e.currentTarget.style.boxShadow = '0 8px 32px rgba(0,0,0,0.17)') }}
            >Abrir GeoRiesgo Perú →</button>
          </div>
        </Reveal>
      </section>

      {/* ═══ FOOTER ══════════════════════════════════════ */}
      <footer style={{ padding: '18px 28px', background: C.bgSoft, borderTop: `1px solid ${C.border}`, display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: 10 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
          <div style={{ width: 22, height: 22, borderRadius: 7, background: `linear-gradient(135deg,${C.primary},${C.secondary})`, display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, color: 'white', fontWeight: 900 }}>G</div>
          <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 10.5, color: C.textMuted }}>GeoRiesgo Perú v4.0 · PostGIS · deck.gl · MapLibre GL</span>
        </div>
        <span style={{ fontFamily: "'DM Mono',monospace", fontSize: 9.5, color: C.textMuted, letterSpacing: '0.04em' }}>USGS · IGP · INGEMMET · INEI · ANA · CENEPRED · PREDES · SENAMHI · OSM</span>
      </footer>
    </div>
  )
}