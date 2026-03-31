import { C } from './ui/constants'

export function Loader({pct}:{pct:number}){
  return(
    <div style={{position:'fixed',inset:0,zIndex:999,background:C.bg,display:'flex',alignItems:'center',justifyContent:'center',flexDirection:'column',gap:24}}>
      <div style={{position:'relative',width:72,height:72}}>
        {[0,1,2].map(i=>(
          <div key={i} style={{position:'absolute',inset:i*10,borderRadius:'50%',border:'2px solid',
            borderColor:[C.primary+'80',C.primaryLt+'50',C.secondary+'35'][i],
            animation:`pring ${1.5+i*0.5}s ease-out infinite`,animationDelay:`${i*0.2}s`}}/>
        ))}
        <div style={{position:'absolute',inset:26,borderRadius:'50%',background:`linear-gradient(135deg,${C.primary},${C.secondary})`}}/>
      </div>
      <div style={{textAlign:'center'}}>
        <p style={{fontFamily:"'DM Sans',sans-serif",color:C.text,fontSize:22,fontWeight:800,letterSpacing:'-0.02em',margin:0}}>
          GeoRiesgo <span style={{color:C.primary}}>Perú</span>
        </p>
        <p style={{fontFamily:"'DM Mono',monospace",color:C.textMuted,fontSize:10,letterSpacing:'0.1em',textTransform:'uppercase',marginTop:4}}>
          v9.0 · Volcanes · ML · EWS · IRC v9 · Sendai
        </p>
      </div>
      <div style={{width:220,height:3,background:C.bgMuted,borderRadius:2}}>
        <div style={{height:'100%',width:`${pct}%`,background:`linear-gradient(90deg,${C.primary},${C.secondary})`,borderRadius:2,transition:'width 0.5s ease'}}/>
      </div>
    </div>
  )
}
