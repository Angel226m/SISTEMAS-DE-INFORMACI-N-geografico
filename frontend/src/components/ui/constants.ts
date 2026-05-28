export const C = {
  // Brand
  primary:'#059669', primaryBg:'#ecfdf5', primaryLt:'#10b981',
  secondary:'#0ea5e9', accent:'#6366f1',
  // State
  danger:'#dc2626', dangerBg:'#fef2f2',
  warning:'#f59e0b', warningBg:'#fffbeb',
  amber:'#f59e0b', orange:'#f97316', teal:'#0891b2', violet:'#7c3aed',
  // Neutral
  bg:'#ffffff', bgSoft:'#f8fafc', bgMuted:'#f1f5f9', bgDark:'#0f172a',
  border:'#e2e8f0', borderMd:'#cbd5e1',
  text:'#0f172a', textSec:'#475569', textMuted:'#94a3b8', textInv:'#f8fafc',
  // Elevation shadows
  shadow:'0 1px 3px rgba(0,0,0,0.06)',
  shadowMd:'0 4px 12px rgba(0,0,0,0.08)',
  shadowLg:'0 8px 32px rgba(0,0,0,0.10)',
  // Semantic surface backgrounds
  surfaceInfo:'#eff6ff', surfaceSuccess:'#f0fdf4',
  surfaceWarning:'#fffbeb', surfaceDanger:'#fef2f2',
} as const

// Soil type colors — NTE E.031-2020
export const SUELO_COLORS: Record<string,string> = {
  S0:'#1d4ed8', S1:'#2563eb', S2:'#0891b2', S3:'#f59e0b', S4:'#dc2626',
}

// MMI intensity scale colors (1-12)
export function mmiColor(mmi: number): string {
  if (mmi >= 10) return '#7c3aed'
  if (mmi >= 8)  return '#dc2626'
  if (mmi >= 6)  return '#f97316'
  if (mmi >= 4)  return '#f59e0b'
  if (mmi >= 2)  return '#0891b2'
  return '#94a3b8'
}

export const RISK_LABELS = ['Muy bajo','Bajo','Moderado','Alto','Muy alto']
export const RISK_COLORS = [C.primary,'#10b981',C.warning,'#f97316',C.danger]
export const ZONA_COLORS: Record<number,string> = {1:'#059669',2:'#f59e0b',3:'#f97316',4:'#dc2626'}

// Typography scale (rem)
export const T = {
  xs: '0.625rem', sm: '0.6875rem', base: '0.75rem',
  md: '0.875rem', lg: '1rem', xl: '1.125rem', '2xl': '1.25rem',
} as const

// Spacing scale (px)
export const S = {
  '1': 4, '2': 8, '3': 12, '4': 16, '5': 20, '6': 24,
} as const
