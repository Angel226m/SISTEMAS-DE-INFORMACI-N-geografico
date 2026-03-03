import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],

  // Proxy para desarrollo local: /api/* → backend FastAPI en puerto 8000
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: path => path,        // conserva el prefijo /api
      },
    },
  },

  // Optimizar dependencias pesadas
  build: {
    rollupOptions: {
      output: {
        manualChunks: {
          'deck':     ['@deck.gl/core', '@deck.gl/layers', '@deck.gl/aggregation-layers'],
          'maplibre': ['maplibre-gl'],
          'recharts': ['recharts'],
        },
      },
    },
  },
})
