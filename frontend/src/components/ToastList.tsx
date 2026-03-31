import { C } from './ui/constants'
import { Icons } from './ui/Icons'

interface Toast{id:number;type:'error'|'success'|'warn';msg:string}
export type { Toast }

const TOAST_C={error:{fg:C.danger,bg:C.dangerBg},success:{fg:C.primary,bg:C.primaryBg},warn:{fg:C.warning,bg:C.warningBg}}

export function ToastList({toasts,remove}:{toasts:Toast[];remove:(id:number)=>void}){
  if(!toasts.length) return null
  return(
    <div style={{position:'fixed',bottom:52,right:14,zIndex:300,display:'flex',flexDirection:'column',gap:7}}>
      {toasts.map(t=>(
        <div key={t.id} style={{display:'flex',alignItems:'flex-start',gap:10,padding:'9px 12px',
          background:TOAST_C[t.type].bg,border:`1px solid ${TOAST_C[t.type].fg}30`,
          borderLeft:`3px solid ${TOAST_C[t.type].fg}`,
          borderRadius:10,boxShadow:'0 4px 14px rgba(0,0,0,0.07)',maxWidth:300}}>
          <span style={{fontFamily:"'DM Sans',sans-serif",fontSize:12,color:C.text,flex:1,lineHeight:1.4}}>{t.msg}</span>
          <button onClick={()=>remove(t.id)} style={{background:'none',border:'none',cursor:'pointer',color:C.textMuted,padding:0}}><Icons.X/></button>
        </div>
      ))}
    </div>
  )
}
