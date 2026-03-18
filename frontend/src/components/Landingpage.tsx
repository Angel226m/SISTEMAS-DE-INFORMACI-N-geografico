// ══════════════════════════════════════════════════════════
// LandingPage.tsx — GeoRiesgo Perú v9.0
// 🆕 Features: Volcanes · ML XGBoost · EWS SSE/WS · Sendai · STAC/COG
// 🆕 Endpoints: volcanes, susceptibilidad, alertas, escenario, sendai, raster
// 🆕 Tech: IRC v9 (7 amenazas), factor_cascada, IVS GEM 2023
// ✅ Todos los features v8 mantenidos
// ══════════════════════════════════════════════════════════
import { useEffect, useRef, useState, useCallback } from 'react'

const C = {
  bg:'#ffffff',bgSoft:'#f8fafc',
  primary:'#059669',primaryLt:'#10b981',primaryGlow:'rgba(5,150,105,0.10)',
  secondary:'#0ea5e9',accent:'#0891b2',
  amber:'#f59e0b',violet:'#7c3aed',brown:'#92400e',teal:'#0891b2',
  text:'#0f172a',textSec:'#334155',textMuted:'#64748b',border:'#e2e8f0',
}

const STATS = [
  { value:'+2.5M', label:'sismos catalogados',      sub:'desde 1900',         color:C.primary   },
  { value:'20',    label:'volcanes inventariados',   sub:'INGEMMET 2021',      color:'#dc2626'   },
  { value:'22',    label:'zonas climáticas',         sub:'SENAMHI/CHIRPS',    color:C.teal      },
  { value:'7',     label:'amenazas en IRC v9',       sub:'CENEPRED 2014',     color:C.accent    },
]

const FEATURES = [
  { icon:'◉', title:'Mapa Sísmico Nacional',        color:C.primary,  border:'#a7f3d0', bg:'linear-gradient(135deg,#f0fdf4,#ecfdf5)', desc:'Catálogo USGS con +2.5M eventos desde 1900. Filtros GPU en tiempo real vía DataFilterExtension (deck.gl). Heatmap de densidad y ScatterplotLayer con profundidad.' },
  { icon:'🌋', title:'Volcanes — INGEMMET 2021',    color:'#dc2626',  border:'#fecaca', bg:'linear-gradient(135deg,#fef2f2,#fff1f2)', desc:'20 volcanes activos y potencialmente activos. Radios de peligro por nivel (30–200 km). Estado activo_critico → peligro inminente. OVI-IGP monitoreo continuo.' },
  { icon:'◐', title:'Susceptibilidad ML',           color:C.violet,   border:'#e9d5ff', bg:'linear-gradient(135deg,#faf5ff,#f5f3ff)', desc:'XGBoost + SMOTE-Tomek + Optuna 50 trials. AUC-PR > 0.95 (Kumar et al. 2023). IC 80% via bootstrapping 100 iter. SHAP top-5 features explicativas.' },
  { icon:'⚡', title:'EWS Alertas Tiempo Real',     color:C.amber,    border:'#fde68a', bg:'linear-gradient(135deg,#fffbeb,#fef3c7)', desc:'SSE + WebSocket multi-hazard. CAP v1.2 ITU-T X.1303bis. Cascada: tsunami (M≥6.5+d<50km) y deslizamiento (M≥5.0+peligro≥3). 4 pilares EW4All UNDRR 2022.' },
  { icon:'◈', title:'Precipitaciones & FEN',        color:C.teal,     border:'#a5f3fc', bg:'linear-gradient(135deg,#ecfeff,#f0fdfa)', desc:'22 zonas climáticas SENAMHI/CHIRPS 1981-2020. Índice FEN multiplicador durante El Niño. Piura/Tumbes: ×4.5 en FEN extraordinario. 23 eventos ENSO 1957-2024 NOAA-CPC.' },
  { icon:'⬡', title:'IRC v9 — 7 Amenazas',         color:C.amber,    border:'#fde68a', bg:'linear-gradient(135deg,#fffbeb,#fef3c7)', desc:'35%S+20%I+18%D+10%T+8%V+5%Q+4%F × factor_cascada. IC 80% bootstrapping 500 iter. (Li et al. 2023). IVS GEM 2023 + MIDIS SISFOH 2022.' },
  { icon:'◉', title:'Escenario Sísmico 4DS',        color:C.amber,   border:'#fed7aa', bg:'linear-gradient(135deg,#fff7ed,#fef3c7)', desc:'4 estados de daño lognormales (DS1–DS4). Atenuación Youngs et al. 1997 subducción Nazca-SA. Fragilidad adobe Tarque et al. 2012 PUCP. MDR + heridos + fallecidos estimados.' },
  { icon:'🌐', title:'Sendai Framework',            color:C.violet,   border:'#c4b5fd', bg:'linear-gradient(135deg,#faf5ff,#f5f3ff)', desc:'7 targets Marco Sendai 2015-2030 (UNDRR). Métricas proxy automáticas desde GeoRiesgo v9. Advertencia obligatoria: NO sustituye reporte oficial INDECI/CENEPRED.' },
  { icon:'◈', title:'Raster STAC + COG MinIO',     color:'#0284c7',  border:'#bae6fd', bg:'linear-gradient(135deg,#f0f9ff,#e0f2fe)', desc:'CHIRPS climatología 1981-2020 en Cloud Optimized GeoTIFF. Window read vía /vsicurl/. Catálogo STAC 1.0. MinIO object storage. COG deflate + predictor=2.' },
]

const ENDPOINTS = [
  {method:'GET',color:C.primary,  path:'/api/v1/sismos',                          desc:'Catálogo sísmico GPU DataFilterExtension'},
  {method:'GET',color:'#dc2626',  path:'/api/v1/volcanes',                        desc:'20 volcanes INGEMMET 2021 + radios peligro'},
  {method:'GET',color:C.violet,   path:'/api/v1/susceptibilidad/{amenaza}',       desc:'Score ML punto + IC 80% + SHAP — XGBoost'},
  {method:'POST',color:C.violet,  path:'/api/v1/susceptibilidad/modelo/entrenar', desc:'Train background 90s · Optuna 50 trials'},
  {method:'GET',color:C.amber,    path:'/api/v1/alertas/stream',                  desc:'SSE EWS multi-hazard tiempo real — CAP v1.2'},
  {method:'WS', color:C.amber,   path:'/ws/sismos',                              desc:'WebSocket alertas EWS + backfill 3 alertas'},
  {method:'GET',color:C.teal,     path:'/api/v1/precipitaciones',                 desc:'22 zonas climáticas — coloreadas por indice_fen'},
  {method:'GET',color:'#f97316',  path:'/api/v1/fen',                             desc:'Catálogo ENSO 1957-2024 NOAA-CPC ONI'},
  {method:'GET',color:C.amber,    path:'/api/v1/riesgo/escenario',                desc:'4DS + Youngs 1997 + GEM 2023 · pérdidas'},
  {method:'GET',color:'#7c3aed',  path:'/api/v1/sendai/report',                   desc:'7 targets Sendai proxy + advertencia obligatoria'},
  {method:'GET',color:'#0284c7',  path:'/api/v1/raster/precipitacion',            desc:'Window read COG MinIO via /vsicurl/'},
  {method:'GET',color:C.primary,  path:'/api/v1/exposicion/{ubigeo}',             desc:'GEM 2023 + INEI 2017 + MIDIS SISFOH 2022'},
  {method:'GET',color:C.secondary,path:'/api/v1/riesgo/construccion/ranking',     desc:'IRC v9/v8 · order=v9|v8 · incluir_ivs'},
  {method:'GET',color:C.accent,   path:'/api/v1/sismos/tendencia',                desc:'Series temporales TimescaleDB CAG + fallback'},
]

const FUENTES = [
  {name:'USGS',           desc:'Catálogo sísmico global',    color:C.primary   },
  {name:'IGP',            desc:'Red sísmica nacional',       color:C.secondary },
  {name:'INGEMMET',       desc:'Volcanes + fallas 2021',     color:'#dc2626'   },
  {name:'SENAMHI',        desc:'Atlas climático 22 zonas',   color:C.teal      },
  {name:'CHIRPS v2',      desc:'Climatología 1981-2020',     color:'#38bdf8'   },
  {name:'NOAA-CPC',       desc:'ONI ENSO 1957-2024',         color:'#f97316'   },
  {name:'GEM 2023',       desc:'Vulnerability Model global', color:C.violet    },
  {name:'MIDIS SISFOH',   desc:'IVS vulnerabilidad social',  color:'#ec4899'   },
  {name:'INDECI',         desc:'Protocolo alertas 2020',     color:C.amber     },
  {name:'CENEPRED',       desc:'Riesgo de desastres',        color:'#0e7490'   },
  {name:'INEI/GADM',      desc:'Límites distritales',        color:C.accent    },
  {name:'UNDRR/Sendai',   desc:'Marco 2015-2030',            color:C.violet    },
]

function useVisible(threshold=0.1){
  const ref=useRef<HTMLDivElement>(null)
  const[vis,setVis]=useState(false)
  useEffect(()=>{
    const el=ref.current;if(!el)return
    const obs=new IntersectionObserver(([e])=>{if(e.isIntersecting){setVis(true);obs.disconnect()}},{threshold})
    obs.observe(el);return()=>obs.disconnect()
  },[threshold])
  return{ref,vis}
}

function Reveal({children,delay=0}:{children:React.ReactNode;delay?:number}){
  const{ref,vis}=useVisible()
  return(
    <div ref={ref} style={{opacity:vis?1:0,transform:vis?'translateY(0)':'translateY(22px)',
      transition:`opacity 0.6s ease ${delay}ms, transform 0.65s cubic-bezier(.22,.68,0,1.2) ${delay}ms`}}>
      {children}
    </div>
  )
}

function Wave({color=C.primary,opacity=0.15,delay='0s',dur='4s'}){
  return(
    <svg viewBox="0 0 600 50" preserveAspectRatio="none" style={{width:'100%',height:44,display:'block'}}>
      <polyline points="0,25 40,25 58,8 76,42 94,12 112,38 130,20 148,32 166,14 184,36 202,22 220,28 245,28 263,10 281,44 299,14 317,38 335,18 353,34 371,22 389,28 414,28 432,12 450,40 468,16 486,32 504,20 522,30 540,18 558,28 576,25 600,25"
        fill="none" stroke={color} strokeWidth="1.6" strokeOpacity={opacity} strokeDasharray="900"
        style={{animation:`swave ${dur} linear infinite`,animationDelay:delay}} />
    </svg>
  )
}

function Navbar({onEnter,scrolled}:{onEnter:()=>void;scrolled:boolean}){
  return(
    <nav style={{position:'sticky',top:0,zIndex:100,padding:'0 28px',height:58,
      display:'flex',alignItems:'center',justifyContent:'space-between',
      background:scrolled?'rgba(255,255,255,0.94)':'transparent',
      backdropFilter:scrolled?'blur(16px)':'none',
      borderBottom:scrolled?`1px solid ${C.border}`:'1px solid transparent',
      transition:'all 0.3s ease'}}>
      <div style={{display:'flex',alignItems:'center',gap:10}}>
        <div style={{width:30,height:30,borderRadius:9,background:`linear-gradient(135deg,${C.primary},${C.secondary})`,
          display:'flex',alignItems:'center',justifyContent:'center',fontSize:14,color:'white',fontWeight:900}}>G</div>
        <div style={{lineHeight:1}}>
          <span style={{fontFamily:"'DM Sans',sans-serif",fontSize:14,fontWeight:800,color:C.text,letterSpacing:'-0.02em'}}>GeoRiesgo</span>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.primary,marginLeft:5}}>PERÚ</span>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,marginLeft:4}}>v9.0</span>
        </div>
      </div>
      <div style={{display:'flex',alignItems:'center',gap:22}}>
        {[['funciones','Funciones'],['api','API'],['datos','Datos']].map(([id,label])=>(
          <a key={id} href={`#${id}`} style={{fontFamily:"'DM Sans',sans-serif",fontSize:13,fontWeight:500,color:C.textSec,textDecoration:'none'}}
            onMouseEnter={e=>(e.currentTarget.style.color=C.primary)}
            onMouseLeave={e=>(e.currentTarget.style.color=C.textSec)}>{label}</a>
        ))}
        <button onClick={onEnter}
          style={{background:`linear-gradient(135deg,${C.primary},${C.primaryLt})`,color:'white',border:'none',
            padding:'8px 20px',borderRadius:10,fontFamily:"'DM Sans',sans-serif",fontSize:13,fontWeight:700,cursor:'pointer',
            boxShadow:'0 2px 10px rgba(5,150,105,0.28)',transition:'all 0.2s ease'}}
          onMouseEnter={e=>{e.currentTarget.style.transform='translateY(-1px)'}}
          onMouseLeave={e=>{e.currentTarget.style.transform=''}}>
          Abrir Mapa →
        </button>
      </div>
    </nav>
  )
}

interface Props{onEnter:()=>void}

export default function LandingPage({onEnter}:Props){
  const wrapRef=useRef<HTMLDivElement>(null)
  const[scrollY,setSY]=useState(0)
  const[mounted,setM]=useState(false)
  const onScroll=useCallback(()=>setSY(wrapRef.current?.scrollTop??0),[])

  useEffect(()=>{
    setM(true)
    const el=wrapRef.current
    el?.addEventListener('scroll',onScroll,{passive:true})
    return()=>el?.removeEventListener('scroll',onScroll)
  },[onScroll])

  const heroParallax=scrollY*0.22
  const heroOpacity=Math.max(0,1-scrollY/420)
  const scrolled=scrollY>28

  return(
    <div ref={wrapRef} style={{position:'absolute',inset:0,overflowY:'scroll',overflowX:'hidden',
      fontFamily:"'DM Sans','Inter',sans-serif",background:C.bg,color:C.text,scrollBehavior:'smooth'}}>
      <style>{`
        @keyframes swave{from{stroke-dashoffset:900}to{stroke-dashoffset:0}}
        @keyframes fadeUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:translateY(0)}}
        @keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
        .feat-card:hover{transform:translateY(-5px)!important;box-shadow:0 18px 48px rgba(0,0,0,0.09)!important;}
        .ep-row:hover{background:#f8fafc!important;}
        .src-chip:hover{transform:translateY(-2px);box-shadow:0 4px 14px rgba(0,0,0,0.06)!important;}
        #funciones,#api,#datos{scroll-margin-top:64px}
      `}</style>

      <Navbar onEnter={onEnter} scrolled={scrolled} />

      {/* HERO */}
      <section style={{minHeight:'calc(100vh - 58px)',display:'flex',flexDirection:'column',alignItems:'center',justifyContent:'center',position:'relative',overflow:'hidden',paddingBottom:90}}>
        <div style={{position:'absolute',inset:0,pointerEvents:'none'}}>
          <div style={{position:'absolute',top:'-8%',left:'3%',width:500,height:500,background:`radial-gradient(circle,${C.primaryGlow} 0%,transparent 68%)`,transform:`translateY(${heroParallax*0.3}px)`}} />
          <div style={{position:'absolute',bottom:'-5%',right:'5%',width:420,height:420,background:'radial-gradient(circle,rgba(8,145,178,0.08) 0%,transparent 68%)',transform:`translateY(${-heroParallax*0.18}px)`}} />
          <svg style={{position:'absolute',inset:0,width:'100%',height:'100%',opacity:0.028}}>
            <defs><pattern id="g0" width="40" height="40" patternUnits="userSpaceOnUse"><path d="M40 0L0 0 0 40" fill="none" stroke={C.text} strokeWidth="1"/></pattern></defs>
            <rect width="100%" height="100%" fill="url(#g0)"/>
          </svg>
          <div style={{position:'absolute',bottom:52,left:0,right:0,transform:`translateY(${heroParallax*0.4}px)`}}>
            <Wave color={C.primary} opacity={0.14} delay="0s" dur="4.2s" />
            <Wave color={C.secondary} opacity={0.09} delay="1.4s" dur="5.8s" />
          </div>
        </div>

        <div style={{position:'relative',zIndex:2,textAlign:'center',padding:'0 24px',maxWidth:940,
          opacity:mounted?heroOpacity:0,transform:`translateY(${mounted?-heroParallax*0.1:14}px)`,
          transition:mounted?'opacity 0.08s linear':'opacity 0.5s ease,transform 0.5s ease'}}>
          <div style={{display:'inline-flex',alignItems:'center',gap:8,background:C.bgSoft,border:`1px solid ${C.border}`,borderRadius:40,padding:'6px 16px',marginBottom:26,animation:'fadeUp 0.6s ease both'}}>
            <span style={{width:7,height:7,borderRadius:'50%',background:C.primary,display:'inline-block',animation:'blink 1.8s ease-in-out infinite'}} />
            <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted,letterSpacing:'0.14em',textTransform:'uppercase'}}>
              v9.0 · Volcanes · ML · EWS · IRC 7 amenazas · Sendai · STAC
            </span>
          </div>

          <h1 style={{fontFamily:"'DM Sans',sans-serif",fontSize:'clamp(36px,6vw,76px)',fontWeight:800,margin:'0 0 18px',lineHeight:1.04,letterSpacing:'-0.035em',animation:'fadeUp 0.6s ease 0.1s both'}}>
            GeoRiesgo{' '}
            <span style={{background:`linear-gradient(135deg,${C.primary},${C.secondary})`,WebkitBackgroundClip:'text',WebkitTextFillColor:'transparent',backgroundClip:'text'}}>Perú</span>
          </h1>

          <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:'clamp(15px,2vw,18px)',color:C.textMuted,lineHeight:1.68,maxWidth:700,margin:'0 auto 36px',animation:'fadeUp 0.6s ease 0.18s both'}}>
            Plataforma geoespacial nacional v9.0: +2.5M sismos GPU, 20 volcanes INGEMMET, susceptibilidad XGBoost+SHAP,
            alertas EWS SSE/WebSocket CAP v1.2, IRC 7 amenazas con factor_cascada, Sendai 7 targets y raster STAC/COG.
          </p>

          <div style={{display:'flex',gap:7,justifyContent:'center',flexWrap:'wrap',marginBottom:24,animation:'fadeUp 0.6s ease 0.22s both'}}>
            {[
              {label:'Volcanes v9',      color:'#dc2626',  bg:'#fef2f2'  },
              {label:'XGBoost ML',       color:C.violet,   bg:'#faf5ff'  },
              {label:'EWS SSE/WS',       color:C.amber,    bg:'#fffbeb'  },
              {label:'IRC 7 amenazas',   color:C.primary,  bg:'#ecfdf5'  },
              {label:'STAC + COG',       color:'#0284c7',  bg:'#f0f9ff'  },
            ].map(({label,color,bg})=>(
              <span key={label} style={{fontFamily:"'DM Mono',monospace",fontSize:9,fontWeight:700,color,background:bg,
                border:`1px solid ${color}30`,padding:'3px 10px',borderRadius:99,letterSpacing:'0.08em',textTransform:'uppercase'}}>
                ✦ {label}
              </span>
            ))}
          </div>

          <div style={{display:'flex',gap:12,justifyContent:'center',flexWrap:'wrap',animation:'fadeUp 0.6s ease 0.28s both'}}>
            <button onClick={onEnter}
              style={{background:`linear-gradient(135deg,${C.primary},${C.primaryLt})`,color:'white',border:'none',
                padding:'14px 38px',borderRadius:14,fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:700,cursor:'pointer',
                boxShadow:'0 4px 24px rgba(5,150,105,0.3)',transition:'all 0.22s ease'}}
              onMouseEnter={e=>{e.currentTarget.style.transform='translateY(-2px)'}}
              onMouseLeave={e=>{e.currentTarget.style.transform=''}}>
              Explorar el Mapa
            </button>
            <a href="#funciones" style={{background:'transparent',color:C.primary,border:`1.5px solid ${C.primary}45`,
              padding:'13px 28px',borderRadius:14,fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:600,
              cursor:'pointer',textDecoration:'none',display:'inline-flex',alignItems:'center',transition:'all 0.22s ease'}}
              onMouseEnter={e=>{e.currentTarget.style.borderColor=C.primary;e.currentTarget.style.background=C.primaryGlow}}
              onMouseLeave={e=>{e.currentTarget.style.borderColor=C.primary+'45';e.currentTarget.style.background='transparent'}}>
              Ver funciones ↓
            </a>
          </div>
        </div>

        <div style={{position:'absolute',bottom:0,left:0,right:0,background:'rgba(255,255,255,0.9)',backdropFilter:'blur(14px)',
          borderTop:`1px solid ${C.border}`,display:'grid',gridTemplateColumns:'repeat(4,1fr)',animation:'fadeUp 0.7s ease 0.4s both'}}>
          {STATS.map((s,i)=>(
            <div key={i} style={{padding:'16px 22px',borderRight:i<3?`1px solid ${C.border}`:'none'}}>
              <div style={{fontFamily:"'DM Mono',monospace",fontSize:24,fontWeight:600,color:s.color,lineHeight:1,marginBottom:4}}>{s.value}</div>
              <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.textSec,fontWeight:500,marginBottom:1}}>{s.label}</div>
              <div style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted,letterSpacing:'0.09em',textTransform:'uppercase'}}>{s.sub}</div>
            </div>
          ))}
        </div>
      </section>

      {/* FUNCIONES */}
      <section id="funciones" style={{padding:'96px 24px',maxWidth:1200,margin:'0 auto'}}>
        <Reveal>
          <div style={{textAlign:'center',marginBottom:52}}>
            <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.primary,letterSpacing:'0.2em',textTransform:'uppercase',marginBottom:12}}>Capacidades del sistema</div>
            <h2 style={{fontFamily:"'DM Sans',sans-serif",fontSize:'clamp(26px,3.5vw,44px)',fontWeight:800,color:C.text,margin:0,letterSpacing:'-0.025em',lineHeight:1.12}}>
              Multi-hazard geoespacial <span style={{color:C.primary}}>v9.0 Enterprise</span>
            </h2>
          </div>
        </Reveal>
        <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(310px,1fr))',gap:18}}>
          {FEATURES.map((f,i)=>(
            <Reveal key={i} delay={i*50}>
              <div className="feat-card" style={{background:f.bg,border:`1px solid ${f.border}`,borderRadius:20,padding:'26px 26px 24px',transition:'all 0.25s cubic-bezier(.22,.68,0,1.2)',cursor:'default',boxShadow:'0 2px 10px rgba(0,0,0,0.04)'}}>
                <div style={{width:40,height:40,borderRadius:12,background:`${f.color}14`,border:`1px solid ${f.color}28`,display:'flex',alignItems:'center',justifyContent:'center',fontSize:18,color:f.color,marginBottom:14}}>{f.icon}</div>
                <h3 style={{fontFamily:"'DM Sans',sans-serif",fontSize:14.5,fontWeight:700,color:C.text,margin:'0 0 9px',lineHeight:1.3}}>{f.title}</h3>
                <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:13,color:C.textMuted,margin:0,lineHeight:1.65}}>{f.desc}</p>
              </div>
            </Reveal>
          ))}
        </div>
      </section>

      {/* API */}
      <section id="api" style={{padding:'80px 24px 96px',background:C.bgSoft,borderTop:`1px solid ${C.border}`,borderBottom:`1px solid ${C.border}`}}>
        <div style={{maxWidth:980,margin:'0 auto'}}>
          <Reveal>
            <div style={{textAlign:'center',marginBottom:48}}>
              <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.secondary,letterSpacing:'0.2em',textTransform:'uppercase',marginBottom:12}}>Backend FastAPI v9.0</div>
              <h2 style={{fontFamily:"'DM Sans',sans-serif",fontSize:'clamp(24px,3vw,38px)',fontWeight:800,color:C.text,margin:'0 0 12px',letterSpacing:'-0.02em'}}>Endpoints geoespaciales + ML + EWS</h2>
              <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:14,color:C.textMuted,maxWidth:560,margin:'0 auto'}}>
                PostGIS 3.4 · TimescaleDB · Redis · MinIO · asyncpg · orjson · SlowAPI rate limiting
              </p>
            </div>
          </Reveal>
          <Reveal delay={60}>
            <div style={{background:C.bg,border:`1px solid ${C.border}`,borderRadius:16,overflow:'hidden',boxShadow:'0 2px 8px rgba(0,0,0,0.04)'}}>
              {ENDPOINTS.map((ep,i)=>(
                <div key={i} className="ep-row" style={{display:'flex',alignItems:'flex-start',gap:12,padding:'14px 18px',
                  borderBottom:i<ENDPOINTS.length-1?`1px solid ${C.border}`:'none',transition:'background 0.15s',cursor:'default',background:'transparent'}}>
                  <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,fontWeight:700,background:`${ep.color}16`,
                    color:ep.color,border:`1px solid ${ep.color}30`,padding:'3px 7px',borderRadius:5,flexShrink:0,marginTop:1,letterSpacing:'0.05em'}}>{ep.method}</span>
                  <div style={{minWidth:0}}>
                    <code style={{fontFamily:"'DM Mono',monospace",fontSize:11.5,color:C.text,fontWeight:600,display:'block',marginBottom:4,wordBreak:'break-all'}}>{ep.path}</code>
                    <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.textMuted,margin:0,lineHeight:1.5}}>{ep.desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </Reveal>
        </div>
      </section>

      {/* DATOS */}
      <section id="datos" style={{padding:'96px 24px',maxWidth:1100,margin:'0 auto'}}>
        <Reveal>
          <div style={{textAlign:'center',marginBottom:48}}>
            <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.accent,letterSpacing:'0.2em',textTransform:'uppercase',marginBottom:12}}>Fuentes oficiales verificadas</div>
            <h2 style={{fontFamily:"'DM Sans',sans-serif",fontSize:'clamp(24px,3.5vw,40px)',fontWeight:800,color:C.text,margin:0,letterSpacing:'-0.02em'}}>Datos científicos de calidad</h2>
          </div>
        </Reveal>
        <Reveal delay={60}>
          <div style={{display:'flex',flexWrap:'wrap',gap:10,justifyContent:'center',marginBottom:56}}>
            {FUENTES.map((f,i)=>(
              <div key={i} className="src-chip" style={{background:C.bg,border:`1px solid ${C.border}`,borderRadius:12,padding:'9px 16px',
                display:'flex',alignItems:'center',gap:9,cursor:'default',transition:'all 0.2s ease',boxShadow:'0 1px 3px rgba(0,0,0,0.04)'}}>
                <div style={{width:8,height:8,borderRadius:'50%',background:f.color}} />
                <div>
                  <div style={{fontFamily:"'DM Mono',monospace",fontSize:11.5,fontWeight:700,color:C.text}}>{f.name}</div>
                  <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:11,color:C.textMuted}}>{f.desc}</div>
                </div>
              </div>
            ))}
          </div>
        </Reveal>

        <Reveal delay={120}>
          <div style={{background:'linear-gradient(135deg,#f0fdf4,#f0f9ff)',border:`1px solid ${C.border}`,borderRadius:22,padding:'32px 38px'}}>
            <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:C.textMuted,letterSpacing:'0.16em',textTransform:'uppercase',marginBottom:22}}>Stack tecnológico v9.0</div>
            <div style={{display:'grid',gridTemplateColumns:'repeat(auto-fit,minmax(200px,1fr))',gap:22}}>
              {[
                {cat:'Frontend v9',   color:C.primary,   items:['React 18 + TypeScript','MapLibre GL + deck.gl','SSE + WebSocket EWS','StatsChart: IRC v9 + 4DS + Sendai']},
                {cat:'Backend v9',    color:C.secondary, items:['FastAPI + asyncpg · SlowAPI','Redis cache + EWSWorker','XGBoost + Optuna + SHAP','CAP v1.2 · orjson · GZip']},
                {cat:'Base de datos', color:C.accent,    items:['PostgreSQL 16 + PostGIS 3.4','TimescaleDB · CAG sismos_mensual','17 tablas · 90+ índices','SP-GIST puntos · BRIN temporal']},
                {cat:'Datos v9',      color:C.teal,      items:['INGEMMET volcanes 2021','GEM Exposure 2023 · IVS MIDIS','STAC 1.0 · COG MinIO','Sendai proxy UNDRR 2022']},
              ].map((s,i)=>(
                <div key={i}>
                  <div style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,fontWeight:700,color:s.color,marginBottom:11}}>{s.cat}</div>
                  {s.items.map((item,j)=>(
                    <div key={j} style={{display:'flex',alignItems:'center',gap:8,marginBottom:7}}>
                      <div style={{width:4,height:4,borderRadius:'50%',background:s.color,opacity:0.4,flexShrink:0}} />
                      <span style={{fontFamily:"'DM Sans',sans-serif",fontSize:12.5,color:C.textSec}}>{item}</span>
                    </div>
                  ))}
                </div>
              ))}
            </div>
          </div>
        </Reveal>
      </section>

      {/* CTA */}
      <section style={{padding:'78px 24px 92px',background:`linear-gradient(155deg,${C.primary} 0%,#0a7a55 38%,${C.secondary} 100%)`,textAlign:'center',position:'relative',overflow:'hidden'}}>
        <svg style={{position:'absolute',inset:0,width:'100%',height:'100%',opacity:0.06,pointerEvents:'none'}}>
          <defs><pattern id="ctag" width="48" height="48" patternUnits="userSpaceOnUse"><path d="M48 0L0 0 0 48" fill="none" stroke="white" strokeWidth="1"/></pattern></defs>
          <rect width="100%" height="100%" fill="url(#ctag)"/>
        </svg>
        <div style={{position:'absolute',bottom:0,left:0,right:0,opacity:0.18,pointerEvents:'none'}}>
          <Wave color="white" opacity={1} delay="0s" dur="4s" />
          <Wave color="white" opacity={1} delay="1.6s" dur="6s" />
        </div>
        <Reveal>
          <div style={{position:'relative',zIndex:2}}>
            <div style={{fontFamily:"'DM Mono',monospace",fontSize:10,color:'rgba(255,255,255,0.55)',letterSpacing:'0.2em',textTransform:'uppercase',marginBottom:16}}>
              v9.0 · Volcanes · XGBoost · EWS · IRC v9 · Sendai · STAC/COG
            </div>
            <h2 style={{fontFamily:"'DM Sans',sans-serif",fontSize:'clamp(28px,4.5vw,54px)',fontWeight:800,color:'white',margin:'0 0 16px',letterSpacing:'-0.025em',lineHeight:1.08}}>Explora el mapa ahora</h2>
            <p style={{fontFamily:"'DM Sans',sans-serif",fontSize:16,color:'rgba(255,255,255,0.72)',maxWidth:580,margin:'0 auto 34px',lineHeight:1.65}}>
              +2.5M sismos GPU · 20 volcanes · XGBoost ML · alertas EWS tiempo real ·
              IRC 7 amenazas · escenario 4DS Youngs 1997 · Sendai 7 targets · STAC/COG MinIO
            </p>
            <button onClick={onEnter}
              style={{background:'white',color:C.primary,border:'none',padding:'15px 46px',borderRadius:14,
                fontFamily:"'DM Sans',sans-serif",fontSize:15,fontWeight:800,cursor:'pointer',
                boxShadow:'0 8px 32px rgba(0,0,0,0.17)',transition:'all 0.22s ease'}}
              onMouseEnter={e=>{e.currentTarget.style.transform='translateY(-2px)'}}
              onMouseLeave={e=>{e.currentTarget.style.transform=''}}>
              Abrir GeoRiesgo Perú →
            </button>
          </div>
        </Reveal>
      </section>

      <footer style={{padding:'18px 28px',background:C.bgSoft,borderTop:`1px solid ${C.border}`,display:'flex',justifyContent:'space-between',alignItems:'center',flexWrap:'wrap',gap:10}}>
        <div style={{display:'flex',alignItems:'center',gap:9}}>
          <div style={{width:22,height:22,borderRadius:7,background:`linear-gradient(135deg,${C.primary},${C.secondary})`,display:'flex',alignItems:'center',justifyContent:'center',fontSize:11,color:'white',fontWeight:900}}>G</div>
          <span style={{fontFamily:"'DM Mono',monospace",fontSize:10.5,color:C.textMuted}}>GeoRiesgo Perú v9.0 · PostGIS · deck.gl · XGBoost · EWS · STAC</span>
        </div>
        <span style={{fontFamily:"'DM Mono',monospace",fontSize:9.5,color:C.textMuted,letterSpacing:'0.04em'}}>USGS · IGP · INGEMMET · INEI · SENAMHI · NOAA-CPC · GEM · INDECI · CENEPRED · UNDRR</span>
      </footer>
    </div>
  )
}