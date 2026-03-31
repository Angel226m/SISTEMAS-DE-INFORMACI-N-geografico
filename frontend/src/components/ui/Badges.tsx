import { C, ZONA_COLORS } from './constants'

export function ZonaBadge({zona,factor}:{zona?:number|null;factor?:number|null}){
  if(!zona) return null
  const color=ZONA_COLORS[zona]??C.textMuted
  return(
    <div style={{display:'inline-flex',alignItems:'center',gap:5,padding:'3px 9px',
      background:color+'12',border:`1px solid ${color}30`,borderRadius:99,marginBottom:6}}>
      <div style={{width:6,height:6,borderRadius:'50%',background:color}}/>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color}}>Z{zona}{factor?` · ${factor}g`:''} · NTE E.030</span>
    </div>
  )
}

export function FENBadge({indice,desc}:{indice:number;desc?:string}){
  const color=indice>=3.5?C.danger:indice>=2.0?C.orange:indice>=1.3?C.warning:indice<0.9?C.primary:C.textMuted
  return(
    <div style={{display:'inline-flex',alignItems:'center',gap:5,padding:'3px 9px',
      background:color+'10',border:`1px solid ${color}25`,borderRadius:99,marginBottom:6}}>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:700,color}}>FEN ×{indice.toFixed(1)}{desc?` · ${desc}`:''}</span>
    </div>
  )
}
