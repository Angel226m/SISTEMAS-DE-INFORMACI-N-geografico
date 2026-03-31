import { Component, type ErrorInfo, type ReactNode } from 'react'

interface Props {
  children: ReactNode
  fallback?: ReactNode
}

interface State {
  hasError: boolean
  error: Error | null
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false, error: null }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[ErrorBoundary]', error, info.componentStack)
  }

  handleReset = () => {
    this.setState({ hasError: false, error: null })
  }

  render() {
    if (this.state.hasError) {
      if (this.props.fallback) return this.props.fallback
      return (
        <div
          role="alert"
          style={{
            padding: 24,
            margin: 16,
            background: '#fef2f2',
            border: '1px solid #fecaca',
            borderRadius: 12,
            fontFamily: "'DM Sans', sans-serif",
          }}
        >
          <h2 style={{ fontSize: 16, fontWeight: 700, color: '#991b1b', marginBottom: 8 }}>
            Error inesperado
          </h2>
          <p style={{ fontSize: 13, color: '#dc2626', marginBottom: 12 }}>
            {this.state.error?.message ?? 'Algo salió mal al renderizar este componente.'}
          </p>
          <button
            onClick={this.handleReset}
            style={{
              padding: '6px 16px',
              borderRadius: 8,
              border: '1px solid #dc2626',
              background: '#fff',
              color: '#dc2626',
              fontWeight: 600,
              cursor: 'pointer',
              fontSize: 12,
            }}
          >
            Reintentar
          </button>
        </div>
      )
    }
    return this.props.children
  }
}
