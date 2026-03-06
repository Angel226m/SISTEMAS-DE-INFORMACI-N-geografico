// ══════════════════════════════════════════════════════════
// MapView.tsx v4.1 — Correcciones:
//   - Eliminado 'antialias' de MapOptions (no existe en MapLibre)
//   - Zoom con rueda del ratón centrado en el cursor (scrollZoom)
//   - Sincronización MapLibre ↔ Deck.gl estable sin deriva
//   - canvas de Deck.gl con pointer-events correctos
// ══════════════════════════════════════════════════════════

import { useEffect, useRef, useCallback } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { Deck } from '@deck.gl/core'
import { ScatterplotLayer, GeoJsonLayer } from '@deck.gl/layers'
import { HeatmapLayer } from '@deck.gl/aggregation-layers'
import type { CapasActivas, TipoVista, TooltipInfo } from '../types'

// ── Constantes ───────────────────────────────────────────
const PERU_CENTER: [number, number] = [-75.0, -10.5]
const PERU_ZOOM   = 5.2
const ICA_CENTER: [number, number]  = [-75.73, -14.07]
const ICA_ZOOM    = 8.5

const MAP_STYLES = {
  light: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  dark:  'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  topo:  'https://api.maptiler.com/maps/topo/style.json?key=get_your_own_OpIi9ZULNHzrESv6T2vL',
} as const
type MapStyle = keyof typeof MAP_STYLES

type FC   = GeoJSON.FeatureCollection
type Feat = GeoJSON.Feature
type FPt  = GeoJSON.Feature<GeoJSON.Point>

// ── Colores ──────────────────────────────────────────────
const profColor = (km: number): [number, number, number, number] =>
  km < 30  ? [220, 38,  38,  220] :
  km < 70  ? [249, 115, 22,  200] :
             [14,  165, 233, 185]

const riskColor = (n: number): [number, number, number, number] => {
  const COLORS: [number,number,number,number][] = [
    [5,   150, 105, 90],
    [16,  185, 129, 110],
    [245, 158, 11,  130],
    [249, 115, 22,  155],
    [220, 38,  38,  175],
  ]
  return COLORS[Math.max(0, Math.min(4, n - 1))] ?? [148, 163, 184, 80]
}

const infraColor = (tipo: string): [number, number, number, number] => {
  const MAP: Record<string, [number,number,number,number]> = {
    hospital:          [239, 68,  68,  230],
    clinica:           [248, 113, 113, 210],
    escuela:           [99,  102, 241, 220],
    aeropuerto:        [6,   182, 212, 230],
    puerto:            [20,  184, 166, 230],
    bomberos:          [234, 179, 8,   230],
    policia:           [59,  130, 246, 220],
    central_electrica: [250, 204, 21,  230],
    planta_agua:       [56,  189, 248, 220],
    puente:            [156, 163, 175, 210],
  }
  return MAP[tipo] ?? [148, 163, 184, 200]
}

const get = <T,>(f: Feat, k: string): T | undefined =>
  (f.properties as Record<string, unknown> | null)?.[k] as T | undefined

// ── Props ─────────────────────────────────────────────────
interface Props {
  sismos:          FC | null
  distritos:       FC | null
  fallas:          FC | null
  inundaciones:    FC | null
  tsunamis:        FC | null
  infraestructura: FC | null
  estaciones:      FC | null
  capas:           CapasActivas
  vista:           TipoVista
  mapStyle?:       MapStyle
  onClickFeature:  (props: Record<string, unknown>, layer: string) => void
  onHoverFeature?: (info: TooltipInfo | null) => void
}

export default function MapView({
  sismos, distritos, fallas, inundaciones, tsunamis,
  infraestructura, estaciones,
  capas, vista, mapStyle = 'light',
  onClickFeature, onHoverFeature,
}: Props) {
  const wrapRef   = useRef<HTMLDivElement>(null)
  const mapDiv    = useRef<HTMLDivElement>(null)
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const mapRef    = useRef<maplibregl.Map | null>(null)
  const deckRef   = useRef<Deck | null>(null)
  const clickRef  = useRef(onClickFeature)
  const hoverRef  = useRef(onHoverFeature)
  clickRef.current = onClickFeature
  hoverRef.current = onHoverFeature

  // ── Inicialización única ───────────────────────────────
  useEffect(() => {
    if (!mapDiv.current || !canvasRef.current || mapRef.current) return

    // CORRECCIÓN: MapLibre MapOptions — sin 'antialias' (no existe en MapOptions)
    // El antialias se controla a nivel de contexto WebGL interno de MapLibre
    const map = new maplibregl.Map({
      container:         mapDiv.current,
      style:             MAP_STYLES[mapStyle],
      center:            ICA_CENTER,
      zoom:              ICA_ZOOM,
      pitch:             0,
      bearing:           0,
      maxPitch:          70,
      attributionControl: false,
      // ZOOM CENTRADO EN CURSOR: configuración del scrollZoom
      // MapLibre por defecto ya hace zoom en el cursor, pero lo explicitamos:
      scrollZoom:        true,
    })

    // Aseguramos que el zoom con rueda esté habilitado y centrado en el cursor
    // (comportamiento por defecto de MapLibre, pero lo reforzamos)
    map.scrollZoom.setWheelZoomRate(1 / 450)  // sensibilidad estándar
    // El zoom centrado en el cursor es el comportamiento nativo de MapLibre GL

    map.addControl(new maplibregl.NavigationControl({ showCompass: true, visualizePitch: true }), 'top-right')
    map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-right')
    mapRef.current = map

    // ── Deck.gl ────────────────────────────────────────
    const deck = new Deck({
      canvas:     canvasRef.current,
      width:      '100%',
      height:     '100%',
      initialViewState: {
        longitude: ICA_CENTER[0],
        latitude:  ICA_CENTER[1],
        zoom:      ICA_ZOOM,
        pitch:     0,
        bearing:   0,
      },
      controller: false, // MapLibre controla TODA la cámara
      layers:     [],
      parameters: { clearColor: [0, 0, 0, 0] },

      onClick: (info) => {
        if (!info.object) return
        const props = (info.object as Feat).properties ?? {}
        clickRef.current(props as Record<string, unknown>, info.layer?.id ?? 'unknown')
      },

      onHover: (info) => {
        if (!hoverRef.current) return
        if (!info.object) { hoverRef.current(null); return }
        hoverRef.current({
          x:      info.x,
          y:      info.y,
          object: info.object as Feat,
          layer:  info.layer?.id ?? null,
        })
      },

      getTooltip: () => null,
    })
    deckRef.current = deck

    // ── Sync cámara MapLibre → Deck.gl ─────────────────
    // Se llama en cada frame para evitar cualquier deriva
    const syncViewState = () => {
      if (!deckRef.current) return
      const center = map.getCenter()
      deckRef.current.setProps({
        viewState: {
          longitude:          center.lng,
          latitude:           center.lat,
          zoom:               map.getZoom(),
          bearing:            map.getBearing(),
          pitch:              map.getPitch(),
          transitionDuration: 0,
        },
      })
    }

    map.on('move',    syncViewState)
    map.on('zoom',    syncViewState)
    map.on('rotate',  syncViewState)
    map.on('pitch',   syncViewState)
    map.on('moveend', syncViewState)
    map.on('load',    syncViewState)
    map.on('render',  syncViewState)

    return () => {
      deck.finalize()
      deckRef.current = null
      map.remove()
      mapRef.current = null
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── ResizeObserver ────────────────────────────────────
  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      mapRef.current?.resize()
      if (mapRef.current && deckRef.current) {
        const c = mapRef.current.getCenter()
        deckRef.current.setProps({
          viewState: {
            longitude:          c.lng,
            latitude:           c.lat,
            zoom:               mapRef.current.getZoom(),
            bearing:            mapRef.current.getBearing(),
            pitch:              mapRef.current.getPitch(),
            transitionDuration: 0,
          },
        })
      }
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // ── Cambio de estilo ──────────────────────────────────
  useEffect(() => {
    mapRef.current?.setStyle(MAP_STYLES[mapStyle])
  }, [mapStyle])

  // ── Vista 2D / 3D ─────────────────────────────────────
  useEffect(() => {
    mapRef.current?.easeTo({ pitch: vista === '3d' ? 55 : 0, duration: 800 })
  }, [vista])

  // ── Eventos de vuelo ──────────────────────────────────
  useEffect(() => {
    const flyToIca = () => mapRef.current?.flyTo({
      center: ICA_CENTER, zoom: ICA_ZOOM,
      pitch: vista === '3d' ? 55 : 0, duration: 1200,
    })
    const flyToPeru = () => mapRef.current?.flyTo({
      center: PERU_CENTER, zoom: PERU_ZOOM, pitch: 0, duration: 1400,
    })
    window.addEventListener('geo:center-ica',  flyToIca)
    window.addEventListener('geo:center-peru', flyToPeru)
    return () => {
      window.removeEventListener('geo:center-ica',  flyToIca)
      window.removeEventListener('geo:center-peru', flyToPeru)
    }
  }, [vista])

  // ── Construir capas Deck.gl ────────────────────────────
  const buildLayers = useCallback(() => {
    const layers = []

    // 1. Distritos
    if (capas.riesgo_distritos && distritos) {
      layers.push(new GeoJsonLayer({
        id:             'distritos',
        data:           distritos,
        getFillColor:   (f: Feat) => riskColor(get<number>(f, 'nivel_riesgo') ?? 3),
        getLineColor:   [100, 116, 139, 60] as [number,number,number,number],
        lineWidthMinPixels: 0.5,
        lineWidthMaxPixels: 2,
        pickable:       true,
        autoHighlight:  true,
        highlightColor: [255, 255, 255, 40],
      }))
    }

    // 2. Zonas inundables
    if (capas.inundaciones && inundaciones) {
      layers.push(new GeoJsonLayer({
        id:             'inundaciones',
        data:           inundaciones,
        getFillColor:   (f: Feat) => {
          const nivel = get<number>(f, 'nivel_riesgo') ?? 3
          return [14, 165, 233, 40 + nivel * 15] as [number,number,number,number]
        },
        getLineColor:   [14, 165, 233, 180] as [number,number,number,number],
        lineWidthMinPixels: 1.5,
        lineWidthMaxPixels: 4,
        pickable:       true,
        autoHighlight:  true,
        highlightColor: [14, 165, 233, 60],
      }))
    }

    // 3. Tsunamis
    if (capas.tsunamis && tsunamis) {
      layers.push(new GeoJsonLayer({
        id:             'tsunamis',
        data:           tsunamis,
        getFillColor:   [6, 182, 212, 55] as [number,number,number,number],
        getLineColor:   [6, 182, 212, 200] as [number,number,number,number],
        lineWidthMinPixels: 2,
        lineWidthMaxPixels: 5,
        pickable:       true,
        autoHighlight:  true,
        highlightColor: [6, 182, 212, 70],
      }))
    }

    // 4. Heatmap
    if (capas.heatmap && sismos?.features.length) {
      layers.push(new HeatmapLayer({
        id:           'heatmap',
        data:         sismos.features,
        getPosition:  (f: FPt) => f.geometry.coordinates as [number, number],
        getWeight:    (f: Feat) => Math.pow(10, (get<number>(f, 'magnitud') ?? 3) - 2),
        radiusPixels: 55,
        intensity:    1.8,
        threshold:    0.025,
        colorRange: [
          [5,   150, 105, 0  ],
          [5,   150, 105, 80 ],
          [245, 158, 11,  150],
          [249, 115, 22,  190],
          [220, 38,  38,  220],
          [127, 29,  29,  255],
        ] as [number, number, number, number][],
      }))
    }

    // 5. Sismos
    if (capas.sismos && sismos?.features.length) {
      layers.push(new ScatterplotLayer({
        id:              'sismos',
        data:            sismos.features,
        getPosition:     (f: FPt) => f.geometry.coordinates as [number, number, number],
        getRadius:       (f: Feat) => {
          const mag = get<number>(f, 'magnitud') ?? 3
          return Math.pow(1.8, mag) * 800
        },
        getFillColor:    (f: Feat) => profColor(get<number>(f, 'profundidad_km') ?? 30),
        getLineColor:    [255, 255, 255, 120] as [number,number,number,number],
        radiusMinPixels: 2,
        radiusMaxPixels: 32,
        radiusUnits:     'meters',
        pickable:        true,
        stroked:         true,
        lineWidthMinPixels: 0.5,
        autoHighlight:   true,
        highlightColor:  [255, 255, 255, 80],
      }))
    }

    // 6. Fallas
    if (capas.fallas && fallas) {
      layers.push(new GeoJsonLayer({
        id:             'fallas',
        data:           fallas,
        getLineColor:   (f: Feat) => {
          const activa = get<boolean>(f, 'activa')
          return activa
            ? [220, 38,  38,  220] as [number,number,number,number]
            : [156, 163, 175, 140] as [number,number,number,number]
        },
        lineWidthMinPixels: 1.5,
        lineWidthMaxPixels: 5,
        pickable:       true,
        autoHighlight:  true,
        highlightColor: [255, 200, 0, 60],
      }))
    }

    // 7. Infraestructura
    if (capas.infraestructura && infraestructura) {
      layers.push(new ScatterplotLayer({
        id:              'infraestructura',
        data:            infraestructura.features,
        getPosition:     (f: FPt) => f.geometry.coordinates as [number, number],
        getRadius:       700,
        radiusUnits:     'meters',
        getFillColor:    (f: Feat) => infraColor(get<string>(f, 'tipo') ?? ''),
        getLineColor:    [255, 255, 255, 200] as [number,number,number,number],
        radiusMinPixels: 4,
        radiusMaxPixels: 18,
        stroked:         true,
        lineWidthMinPixels: 1.5,
        pickable:        true,
        autoHighlight:   true,
        highlightColor:  [255, 255, 255, 100],
      }))
    }

    // 8. Estaciones
    if (capas.estaciones && estaciones) {
      layers.push(new ScatterplotLayer({
        id:              'estaciones',
        data:            estaciones.features,
        getPosition:     (f: FPt) => f.geometry.coordinates as [number, number],
        getRadius:       500,
        radiusUnits:     'meters',
        getFillColor:    (f: Feat) => {
          const tipo = get<string>(f, 'tipo') ?? ''
          return tipo === 'sismica'
            ? [16, 185, 129, 230] as [number,number,number,number]
            : [56, 189, 248, 230] as [number,number,number,number]
        },
        getLineColor:    [255, 255, 255, 180] as [number,number,number,number],
        radiusMinPixels: 4,
        radiusMaxPixels: 12,
        stroked:         true,
        lineWidthMinPixels: 1.5,
        pickable:        true,
        autoHighlight:   true,
        highlightColor:  [255, 255, 255, 80],
      }))
    }

    return layers
  }, [capas, sismos, distritos, fallas, inundaciones, tsunamis, infraestructura, estaciones])

  useEffect(() => {
    deckRef.current?.setProps({ layers: buildLayers() })
  }, [buildLayers])

  return (
    <div
      ref={wrapRef}
      style={{ position: 'absolute', inset: 0, overflow: 'hidden' }}
    >
      {/* Base: MapLibre GL — recibe TODOS los eventos de navegación */}
      <div ref={mapDiv} style={{ position: 'absolute', inset: 0 }} />

      {/* Deck.gl canvas: pointer-events:none para que MapLibre reciba scroll/drag */}
      <canvas
        ref={canvasRef}
        style={{
          position:      'absolute',
          inset:         0,
          width:         '100%',
          height:        '100%',
          pointerEvents: 'none',  // ← CRUCIAL: MapLibre maneja zoom/pan nativamente
        }}
      />

      {/*
        Capa de interacción para CLICKS y HOVER sobre features Deck.gl.
        IMPORTANTE: pointer-events:none en el canvas, pero esta capa
        captura clics sobre features de forma precisa.
        El scroll/zoom pasa directamente al div del mapa (z-index más bajo).
      */}
      <div
        style={{ position: 'absolute', inset: 0, cursor: 'crosshair' }}
        onWheelCapture={(e) => {
          // Reenviar el evento de scroll al div del mapa para que MapLibre
          // haga zoom centrado en el cursor correctamente
          if (mapDiv.current) {
            mapDiv.current.dispatchEvent(new WheelEvent('wheel', {
              deltaY:   e.deltaY,
              deltaX:   e.deltaX,
              deltaMode: e.deltaMode,
              clientX:  e.clientX,
              clientY:  e.clientY,
              ctrlKey:  e.ctrlKey,
              bubbles:  true,
            }))
          }
          e.preventDefault()
        }}
        onClick={e => {
          if (!deckRef.current) return
          const rect = (e.currentTarget as HTMLElement).getBoundingClientRect()
          const info = deckRef.current.pickObject({
            x: e.clientX - rect.left,
            y: e.clientY - rect.top,
          })
          if (info?.object) {
            const props = (info.object as Feat).properties ?? {}
            clickRef.current(props as Record<string, unknown>, info.layer?.id ?? '')
          }
        }}
        onMouseMove={e => {
          if (!deckRef.current || !hoverRef.current) return
          const rect = (e.currentTarget as HTMLElement).getBoundingClientRect()
          const info = deckRef.current.pickObject({
            x: e.clientX - rect.left,
            y: e.clientY - rect.top,
          })
          if (info?.object) {
            hoverRef.current({
              x:      e.clientX - rect.left,
              y:      e.clientY - rect.top,
              object: info.object as Feat,
              layer:  info.layer?.id ?? null,
            })
          } else {
            hoverRef.current(null)
          }
        }}
        onMouseLeave={() => hoverRef.current?.(null)}
      />
    </div>
  )
}

export type { MapStyle }