import { C } from './constants'

export function Btn({active,onClick,title,children}:{active:boolean;onClick:()=>void;title?:string;children:React.ReactNode}){
  return(
    <button title={title} onClick={onClick} style={{width:32,height:32,borderRadius:8,cursor:'pointer',
      background:active?`${C.primary}15`:C.bgMuted,border:`1px solid ${active?`${C.primary}40`:C.border}`,
      color:active?C.primary:C.textMuted,display:'flex',alignItems:'center',justifyContent:'center',transition:'all 0.16s ease'}}>
      {children}
    </button>
  )
}
