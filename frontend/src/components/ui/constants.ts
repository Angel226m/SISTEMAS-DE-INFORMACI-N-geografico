export const C = {
  primary:'#059669', primaryBg:'#ecfdf5', primaryLt:'#10b981',
  secondary:'#0ea5e9', accent:'#6366f1',
  danger:'#dc2626', dangerBg:'#fef2f2',
  warning:'#f59e0b', warningBg:'#fffbeb',
  amber:'#f59e0b', orange:'#f97316', teal:'#0891b2', violet:'#7c3aed',
  bg:'#ffffff', bgSoft:'#f8fafc', bgMuted:'#f1f5f9', border:'#e2e8f0',
  text:'#0f172a', textSec:'#475569', textMuted:'#94a3b8',
} as const

export const RISK_LABELS = ['Muy bajo','Bajo','Moderado','Alto','Muy alto']
export const RISK_COLORS = [C.primary,'#10b981',C.warning,'#f97316',C.danger]
export const ZONA_COLORS: Record<number,string> = {1:'#059669',2:'#f59e0b',3:'#f97316',4:'#dc2626'}
