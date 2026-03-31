import { C } from './constants'

export function Row({label,value,color}:{label:string;value:string|number;color?:string}){
  return(
    <div style={{display:'flex',justifyContent:'space-between',gap:8,marginBottom:5}}>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:9,color:C.textMuted}}>{label}</span>
      <span style={{fontFamily:"'DM Mono',monospace",fontSize:10,fontWeight:600,color:color??C.text,textAlign:'right'}}>{String(value)}</span>
    </div>
  )
}
