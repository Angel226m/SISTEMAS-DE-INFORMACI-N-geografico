// == MapView.tsx — Mapa estable ante resize / zoom =========
import { useEffect, useRef, useCallback } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { Deck } from '@deck.gl/core'
import { ScatterplotLayer, GeoJsonLayer } from '@deck.gl/layers'
// @ts-ignore — aggregation-layers no tiene types perfectos
import { HeatmapLayer } from '@deck.gl/aggregation-layers'
import type { CapasActivas, TipoVista } from '../types'

// ── Constantes ─────────────────────────────────────────────
const ICA: [number, number] = [-75.73, -14.07]
const ICA_ZOOM = 8.5
const MAP_STYLE = 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json'

type FC   = GeoJSON.FeatureCollection
type Feat = GeoJSON.Feature
type FPt  = GeoJSON.Feature<GeoJSON.Point>

// ── Helpers de color ───────────────────────────────────────
const profColor = (km: number): [number, number, number, number] =>
  km < 30  ? [220, 38,  38,  210] :
  km < 70  ? [249, 115, 22,  200] :
             [14,  165, 233, 190]

const riskColor = (n: number): [number, number, number, number] => {
  const MAP: Record<number, [number, number, number, number]> = {
    1: [5,   150, 105, 110],
    2: [16,  185, 129, 130],
    3: [245, 158, 11,  150],
    4: [249, 115, 22,  170],
    5: [220, 38,  38,  190],
  }
  return MAP[Math.max(1, Math.min(5, n))] ?? [148, 163, 184, 100]
}

const get = <T,>(f: Feat, k: string): T | undefined =>
  (f.properties as Record<string, unknown> | null)?.[k] as T | undefined

// ── Props ──────────────────────────────────────────────────
interface Props {
  sismos:          FC | null
  distritos:       FC | null
  fallas:          FC | null
  inundaciones:    FC | null
  infraestructura: FC | null
  capas:           CapasActivas
  vista:           TipoVista
  onClickFeature:  (info: Record<string, unknown>) => void
}

export default function MapView({
  sismos, distritos, fallas, inundaciones, infraestructura,
  capas, vista, onClickFeature,
}: Props) {
  const wrapRef      = useRef<HTMLDivElement>(null)   // div contenedor
  const mapContainer = useRef<HTMLDivElement>(null)   // div para MapLibre
  const canvasRef    = useRef<HTMLCanvasElement>(null) // canvas para Deck.gl
  const mapRef       = useRef<maplibregl.Map | null>(null)
  const deckRef      = useRef<Deck | null>(null)
  const clickRef     = useRef(onClickFeature)
  clickRef.current   = onClickFeature

  // ── Inicializar mapa y Deck una sola vez ──────────────────
  useEffect(() => {
    if (!mapContainer.current || !canvasRef.current || mapRef.current) return

    const map = new maplibregl.Map({
      container: mapContainer.current,
      style:     MAP_STYLE,
      center:    ICA,
      zoom:      ICA_ZOOM,
      pitch:     0,
      bearing:   0,
      attributionControl: false,
    })

    map.addControl(new maplibregl.NavigationControl({ showCompass: true }), 'top-right')
    map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-right')
    mapRef.current = map

    const deck = new Deck({
      canvas:  canvasRef.current,
      width:   '100%',
      height:  '100%',
      initialViewState: { longitude: ICA[0], latitude: ICA[1], zoom: ICA_ZOOM, pitch: 0 },
      controller: false,   // MapLibre controla la cámara
      layers:  [],
      onClick: (info) => {
        if ((info as { object?: unknown }).object) {
          clickRef.current(info as Record<string, unknown>)
        }
      },
    })
    deckRef.current = deck

    // Sincronizar cámara MapLibre → Deck.gl al moverse el mapa
    const syncCamera = () => {
      const c = map.getCenter()
      deck.setProps({
        viewState: {
          longitude: c.lng,
          latitude:  c.lat,
          zoom:      map.getZoom(),
          bearing:   map.getBearing(),
          pitch:     map.getPitch(),
        },
      })
    }
    map.on('move', syncCamera)
    map.on('load', syncCamera)

    return () => {
      deck.finalize()
      deckRef.current = null
      map.remove()
      mapRef.current = null
    }
  }, []) // Solo al montar

  // ── ResizeObserver: mapa se ajusta al contenedor siempre ──
  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      mapRef.current?.resize()
      // El canvas de Deck.gl usa 100%/100% CSS → se ajusta solo
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // ── Vista 3D / 2D ──────────────────────────────────────────
  useEffect(() => {
    mapRef.current?.easeTo({ pitch: vista === '3d' ? 52 : 0, duration: 900 })
  }, [vista])

  // ── Recentrar desde evento global ─────────────────────────
  useEffect(() => {
    const handler = () =>
      mapRef.current?.flyTo({ center: ICA, zoom: ICA_ZOOM, pitch: vista === '3d' ? 52 : 0, duration: 1100 })
    window.addEventListener('georiesgo:recenter', handler)
    return () => window.removeEventListener('georiesgo:recenter', handler)
  }, [vista])

  // ── Capas Deck.gl ──────────────────────────────────────────
  const buildLayers = useCallback(() => {
    const layers = []

    // Distritos (índice de riesgo)
    if (capas.riesgo_distritos && distritos) {
      layers.push(new GeoJsonLayer({
        id: 'distritos',
        data: distritos,
        getFillColor: (f: Feat) => riskColor(get<number>(f, 'nivel_riesgo') ?? 3),
        getLineColor: [100, 116, 139, 80] as [number,number,number,number],
        lineWidthMinPixels: 1,
        pickable: true,
        updateTriggers: { getFillColor: [distritos] },
      }))
    }

    // Zonas inundables
    if (capas.inundaciones && inundaciones) {
      layers.push(new GeoJsonLayer({
        id: 'inundaciones',
        data: inundaciones,
        getFillColor: [14, 165, 233, 55] as [number,number,number,number],
        getLineColor: [14, 165, 233, 160] as [number,number,number,number],
        lineWidthMinPixels: 1.5,
        pickable: true,
      }))
    }

    // Heatmap de densidad sísmica
    if (capas.heatmap && sismos?.features.length) {
      layers.push(new HeatmapLayer({
        id: 'heatmap',
        data: sismos.features,
        getPosition: (f: FPt) => f.geometry.coordinates as [number, number],
        getWeight:   (f: Feat) => get<number>(f, 'magnitud') ?? 3,
        radiusPixels: 60,
        intensity:    2,
        threshold:    0.03,
        colorRange: [
          [5,150,105,0],[5,150,105,80],[245,158,11,140],
          [249,115,22,180],[220,38,38,220],[127,29,29,255],
        ],
      }))
    }

    // Puntos sísmicos
    // ↓ Usa profundidad_km (nombre correcto del backend PostGIS)
    if (capas.sismos && sismos?.features.length) {
      layers.push(new ScatterplotLayer({
        id: 'sismos',
        data: sismos.features,
        getPosition: (f: FPt) => f.geometry.coordinates as [number, number, number],
        getRadius:   (f: Feat) => (get<number>(f, 'magnitud') ?? 3) * 2400,
        getFillColor:(f: Feat) => profColor(get<number>(f, 'profundidad_km') ?? 30),
        radiusMinPixels: 3,
        radiusMaxPixels: 28,
        pickable: true,
        stroked: true,
        getLineColor: [255, 255, 255, 160] as [number,number,number,number],
        lineWidthMinPixels: 0.5,
        updateTriggers: { getPosition: [sismos], getFillColor: [sismos] },
      }))
    }

    // Fallas geológicas
    if (capas.fallas && fallas) {
      layers.push(new GeoJsonLayer({
        id: 'fallas',
        data: fallas,
        getLineColor: [220, 38, 38, 210] as [number,number,number,number],
        lineWidthMinPixels: 2.5,
        lineWidthMaxPixels: 6,
        pickable: true,
      }))
    }

    // Infraestructura crítica
    if (capas.infraestructura && infraestructura) {
      layers.push(new ScatterplotLayer({
        id: 'infraestructura',
        data: infraestructura.features,
        getPosition: (f: FPt) => f.geometry.coordinates as [number, number],
        getRadius: 400,
        getFillColor: [99, 102, 241, 220] as [number,number,number,number],
        getLineColor: [255, 255, 255, 200] as [number,number,number,number],
        radiusMinPixels: 5,
        radiusMaxPixels: 14,
        stroked: true,
        lineWidthMinPixels: 1.5,
        pickable: true,
      }))
    }

    return layers
  }, [capas, sismos, distritos, fallas, inundaciones, infraestructura])

  useEffect(() => {
    deckRef.current?.setProps({ layers: buildLayers() })
  }, [buildLayers])

  // ── Render ─────────────────────────────────────────────────
  return (
    <div
      ref={wrapRef}
      style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}
    >
      {/* Contenedor MapLibre */}
      <div ref={mapContainer} style={{ position: 'absolute', inset: 0 }} />

      {/* Canvas Deck.gl superpuesto, pointer-events: none para pasar eventos al mapa */}
      <canvas
        ref={canvasRef}
        style={{
          position: 'absolute', inset: 0,
          width: '100%', height: '100%',
          pointerEvents: 'none',   // ← el mapa recibe scroll/drag
        }}
      />

      {/* Capa interactiva: captura clics sobre features Deck.gl */}
      <div
        style={{ position: 'absolute', inset: 0 }}
        onMouseMove={e => {
          if (!deckRef.current) return
          const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect()
          deckRef.current.setProps({
            _onMetrics: undefined, // reset metrics
          })
          // Pasar la posición del mouse a Deck para pickeo
          void deckRef.current.pickObject({ x: e.clientX - rect.left, y: e.clientY - rect.top })
        }}
        onClick={e => {
          if (!deckRef.current) return
          const rect = (e.currentTarget as HTMLDivElement).getBoundingClientRect()
          const info = deckRef.current.pickObject({ x: e.clientX - rect.left, y: e.clientY - rect.top })
          if (info?.object) clickRef.current(info as unknown as Record<string, unknown>)
        }}
      />
    </div>
  )
}