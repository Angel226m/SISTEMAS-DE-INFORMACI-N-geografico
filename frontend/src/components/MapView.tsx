// ══════════════════════════════════════════════════════════
// MapView.tsx v7.0  — DataFilterExtension + updateTriggers
//
// MEJORAS v7.0 (basadas en investigación / docs deck.gl):
//
// 1. DataFilterExtension en ScatterplotLayer de sismos:
//    Filtra por [magnitud, año, profundidad_code] 100% en GPU.
//    → Sin re-fetch a la API al mover sliders de mag/año.
//    → Filtrando 1M puntos a 60fps sin latencia.
//    Ref: https://deck.gl/docs/api-reference/extensions/data-filter-extension
//
// 2. updateTriggers en todas las capas:
//    Indica a deck.gl exactamente qué props cambiaron,
//    evitando recalculación de buffers GPU innecesaria.
//    Ref: deck.gl performance guide
//
// 3. useMemo para sismosFeatures (heatmap):
//    HeatmapLayer no soporta DataFilterExtension, por lo que
//    el array filtrado se memoiza en CPU para evitar
//    regeneración de buffers GPU en cada render de React.
//
// 4. Visible layers = SÓLO capas activas en el array:
//    layers.push sólo cuando capa activa Y datos listos.
//    visible:false provoca checks en cada frame → peor rendimiento.
//    Ref: github.com/visgl/deck.gl/discussions/7021
//
// 5. MapboxOverlay (overlaid) sigue siendo la integración
//    oficial para deck.gl + MapLibre (sin sync manual de cámara).
// ══════════════════════════════════════════════════════════

import { useEffect, useRef, useCallback, useMemo } from 'react'
import maplibregl from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'
import { MapboxOverlay } from '@deck.gl/mapbox'
import { ScatterplotLayer, GeoJsonLayer } from '@deck.gl/layers'
import { HeatmapLayer } from '@deck.gl/aggregation-layers'
import { DataFilterExtension } from '@deck.gl/extensions'
import type { CapasActivas, TipoVista, TooltipInfo, FiltrosSismos } from '../types'

// ── Constantes ─────────────────────────────────────────────
const PERU_CENTER: [number, number] = [-75.0, -10.5]
const PERU_ZOOM = 5.2
const ICA_CENTER: [number, number] = [-75.73, -14.07]
const ICA_ZOOM = 8.5

const MAP_STYLES = {
  light: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
  dark:  'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
  topo:  'https://api.maptiler.com/maps/topo/style.json?key=get_your_own_OpIi9ZULNHzrESv6T2vL',
} as const
export type MapStyle = keyof typeof MAP_STYLES

type FC   = GeoJSON.FeatureCollection
type Feat = GeoJSON.Feature
type FPt  = GeoJSON.Feature<GeoJSON.Point>

// ── DataFilterExtension: codificación de profundidad ──────
// Codificamos el tipo de profundidad como número para GPU
const profCode = (tipo: string | null): number =>
  tipo === 'superficial' ? 1 : tipo === 'intermedio' ? 2 : tipo === 'profundo' ? 3 : 99

// ── Helpers de color ───────────────────────────────────────
const profColor = (km: number): [number,number,number,number] =>
  km < 30  ? [220,38,38,220]  :
  km < 70  ? [249,115,22,200] :
             [14,165,233,185]

// Escala secuencial perceptualmente uniforme para riesgo
// (investigación: evitar rainbow/jet — usar escalas secuenciales)
const riskColor = (n: number): [number,number,number,number] => {
  const C: [number,number,number,number][] = [
    [5,150,105,90],    // 1 - Muy bajo  (verde esmeralda)
    [16,185,129,110],  // 2 - Bajo      (verde claro)
    [245,158,11,130],  // 3 - Moderado  (ámbar)
    [249,115,22,155],  // 4 - Alto      (naranja)
    [220,38,38,175],   // 5 - Muy alto  (rojo)
  ]
  return C[Math.max(0, Math.min(4, n - 1))] ?? [148,163,184,80]
}

const deslizColor = (tipo: string | null): [number,number,number,number] => {
  const M: Record<string,[number,number,number,number]> = {
    deslizamiento:   [146,64,14,160],
    huayco:          [180,83,9,170],
    derrumbe:        [217,119,6,160],
    flujo_detritico: [245,158,11,150],
    reptacion:       [161,98,7,140],
  }
  return M[tipo ?? ''] ?? [146,64,14,140]
}

const infraColor = (tipo: string): [number,number,number,number] => {
  const M: Record<string,[number,number,number,number]> = {
    hospital:          [239,68,68,230],
    clinica:           [248,113,113,210],
    escuela:           [99,102,241,220],
    aeropuerto:        [6,182,212,230],
    puerto:            [20,184,166,230],
    bomberos:          [234,179,8,230],
    policia:           [59,130,246,220],
    central_electrica: [250,204,21,230],
    planta_agua:       [56,189,248,220],
    puente:            [156,163,175,210],
  }
  return M[tipo] ?? [148,163,184,200]
}

const get = <T,>(f: Feat, k: string): T | undefined =>
  (f.properties as Record<string,unknown> | null)?.[k] as T | undefined

// ── Props ──────────────────────────────────────────────────
interface Props {
  sismos:          FC | null
  departamentos:   FC | null
  distritos:       FC | null
  fallas:          FC | null
  inundaciones:    FC | null
  tsunamis:        FC | null
  deslizamientos:  FC | null
  infraestructura: FC | null
  estaciones:      FC | null
  capas:           CapasActivas
  vista:           TipoVista
  mapStyle?:       MapStyle
  filtros:         FiltrosSismos   // ← NUEVO v7.0: para DataFilterExtension
  onClickFeature:  (props: Record<string,unknown>, layer: string) => void
  onHoverFeature?: (info: TooltipInfo | null) => void
}

export default function MapView({
  sismos, departamentos, distritos, fallas, inundaciones, tsunamis,
  deslizamientos, infraestructura, estaciones,
  capas, vista, mapStyle = 'light',
  filtros,
  onClickFeature, onHoverFeature,
}: Props) {
  const mapDiv     = useRef<HTMLDivElement>(null)
  const mapRef     = useRef<maplibregl.Map | null>(null)
  const overlayRef = useRef<MapboxOverlay | null>(null)
  const clickRef   = useRef(onClickFeature)
  const hoverRef   = useRef(onHoverFeature)
  clickRef.current = onClickFeature
  hoverRef.current = onHoverFeature

  // ── useMemo: heatmap filtrado en CPU (HeatmapLayer no soporta DataFilterExtension)
  const heatmapData = useMemo(() => {
    if (!sismos?.features) return []
    return sismos.features.filter(f => {
      const mag  = get<number>(f, 'magnitud') ?? 0
      const year = parseInt((get<string>(f, 'fecha') ?? '1960-01-01').substring(0, 4))
      const tipo = get<string>(f, 'tipo_profundidad') ?? null
      if (mag  < filtros.mag_min   || mag  > filtros.mag_max)   return false
      if (year < filtros.year_start || year > filtros.year_end)  return false
      if (filtros.profundidad && tipo !== filtros.profundidad)   return false
      return true
    }) as FPt[]
  }, [sismos, filtros.mag_min, filtros.mag_max, filtros.year_start, filtros.year_end, filtros.profundidad])

  // ── Init: MapLibre + MapboxOverlay ─────────────────────
  useEffect(() => {
    if (!mapDiv.current || mapRef.current) return

    const map = new maplibregl.Map({
      container:          mapDiv.current,
      style:              MAP_STYLES[mapStyle],
      center:             ICA_CENTER,
      zoom:               ICA_ZOOM,
      pitch:              0,
      bearing:            0,
      maxPitch:           70,
      attributionControl: false,
      scrollZoom:         true,
    })

    map.scrollZoom.setWheelZoomRate(1 / 450)
    map.addControl(
      new maplibregl.NavigationControl({ showCompass: true, visualizePitch: true }),
      'top-right'
    )
    map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-right')
    mapRef.current = map

    const overlay = new MapboxOverlay({
      interleaved: false,
      layers: [],
      onClick: (info) => {
        if (!info.object) return
        const props = (info.object as Feat).properties ?? {}
        clickRef.current(props as Record<string,unknown>, info.layer?.id ?? '')
      },
      onHover: (info) => {
        if (!hoverRef.current) return
        if (!info.object) { hoverRef.current(null); return }
        hoverRef.current({
          x: info.x, y: info.y,
          object: info.object as Feat,
          layer:  info.layer?.id ?? null,
        })
      },
      getTooltip: () => null,
    })

    map.addControl(overlay as unknown as maplibregl.IControl)
    overlayRef.current = overlay

    return () => {
      overlay.finalize()
      overlayRef.current = null
      map.remove()
      mapRef.current = null
    }
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // ── Cambio de estilo ───────────────────────────────────
  useEffect(() => {
    mapRef.current?.setStyle(MAP_STYLES[mapStyle])
  }, [mapStyle])

  // ── Vista 2D / 3D ──────────────────────────────────────
  useEffect(() => {
    mapRef.current?.easeTo({ pitch: vista === '3d' ? 55 : 0, duration: 800 })
  }, [vista])

  // ── Vuelos programáticos ───────────────────────────────
  useEffect(() => {
    const toIca  = () => mapRef.current?.flyTo({ center: ICA_CENTER,  zoom: ICA_ZOOM,  pitch: vista === '3d' ? 55 : 0, duration: 1200 })
    const toPeru = () => mapRef.current?.flyTo({ center: PERU_CENTER, zoom: PERU_ZOOM, pitch: 0, duration: 1400 })
    window.addEventListener('geo:center-ica',  toIca)
    window.addEventListener('geo:center-peru', toPeru)
    return () => {
      window.removeEventListener('geo:center-ica',  toIca)
      window.removeEventListener('geo:center-peru', toPeru)
    }
  }, [vista])

  // ── Construcción de capas deck.gl ──────────────────────
  const buildLayers = useCallback(() => {
    const layers = []

    // — Departamentos —
    if (capas.departamentos && departamentos)
      layers.push(new GeoJsonLayer({
        id: 'departamentos', data: departamentos,
        getFillColor: (f: Feat) => {
          const b = riskColor(get<number>(f, 'nivel_riesgo') ?? 3)
          return [b[0], b[1], b[2], 18] as [number,number,number,number]
        },
        getLineColor: [124,58,237,200] as [number,number,number,number],
        lineWidthMinPixels: 1, lineWidthMaxPixels: 3,
        pickable: true, autoHighlight: true,
        highlightColor: [124,58,237,40],
        updateTriggers: { getFillColor: [] },
      }))

    // — Distritos —
    if (capas.riesgo_distritos && distritos)
      layers.push(new GeoJsonLayer({
        id: 'distritos', data: distritos,
        getFillColor: (f: Feat) => riskColor(get<number>(f, 'nivel_riesgo') ?? 3),
        getLineColor: [100,116,139,60] as [number,number,number,number],
        lineWidthMinPixels: 0.5, lineWidthMaxPixels: 2,
        pickable: true, autoHighlight: true,
        highlightColor: [255,255,255,40],
        updateTriggers: { getFillColor: [] },
      }))

    // — Inundaciones —
    if (capas.inundaciones && inundaciones)
      layers.push(new GeoJsonLayer({
        id: 'inundaciones', data: inundaciones,
        getFillColor: (f: Feat) => {
          const n = get<number>(f, 'nivel_riesgo') ?? 3
          return [14,165,233,40 + n * 15] as [number,number,number,number]
        },
        getLineColor: [14,165,233,180] as [number,number,number,number],
        lineWidthMinPixels: 1.5, lineWidthMaxPixels: 4,
        pickable: true, autoHighlight: true,
        highlightColor: [14,165,233,60],
        updateTriggers: { getFillColor: [] },
      }))

    // — Tsunamis —
    if (capas.tsunamis && tsunamis)
      layers.push(new GeoJsonLayer({
        id: 'tsunamis', data: tsunamis,
        getFillColor: [6,182,212,55] as [number,number,number,number],
        getLineColor: [6,182,212,200] as [number,number,number,number],
        lineWidthMinPixels: 2, lineWidthMaxPixels: 5,
        pickable: true, autoHighlight: true,
        highlightColor: [6,182,212,70],
      }))

    // — Deslizamientos —
    if (capas.deslizamientos && deslizamientos)
      layers.push(new GeoJsonLayer({
        id: 'deslizamientos', data: deslizamientos,
        getFillColor: (f: Feat) => deslizColor(get<string>(f, 'tipo') ?? null),
        getLineColor: [120,53,15,200] as [number,number,number,number],
        lineWidthMinPixels: 1, lineWidthMaxPixels: 3,
        pickable: true, autoHighlight: true,
        highlightColor: [234,179,8,60],
        updateTriggers: { getFillColor: [] },
      }))

    // — Heatmap (datos filtrados en CPU con useMemo) —
    if (capas.heatmap && heatmapData.length)
      layers.push(new HeatmapLayer({
        id: 'heatmap',
        data: heatmapData,
        getPosition: (f: FPt) => f.geometry.coordinates as [number,number],
        getWeight: (f: Feat) => Math.pow(10, (get<number>(f, 'magnitud') ?? 3) - 2),
        radiusPixels: 55, intensity: 1.8, threshold: 0.025,
        colorRange: [
          [5,150,105,0],[5,150,105,80],[245,158,11,150],
          [249,115,22,190],[220,38,38,220],[127,29,29,255],
        ] as [number,number,number,number][],
      }))

    // — Sismos — DataFilterExtension: filtra mag + año + profundidad en GPU
    if (capas.sismos && sismos?.features.length)
      layers.push(new ScatterplotLayer({
        id: 'sismos',
        data: sismos.features,
        getPosition:  (f: FPt)  => f.geometry.coordinates as [number,number,number],
        getRadius:    (f: Feat) => Math.pow(1.8, get<number>(f, 'magnitud') ?? 3) * 800,
        getFillColor: (f: Feat) => profColor(get<number>(f, 'profundidad_km') ?? 30),
        getLineColor: [255,255,255,120] as [number,number,number,number],
        radiusMinPixels: 4, radiusMaxPixels: 32,
        radiusUnits: 'meters',
        pickable: true, stroked: true, lineWidthMinPixels: 0.8,
        autoHighlight: true,
        highlightColor: [255,255,255,100],
        // ── DataFilterExtension v7.0 ────────────────────
        // filterSize: 3 → [magnitud, año, profundidad_code]
        // Actualizar filterRange cambia un uniform en el shader GPU
        // sin reconstruir buffers → latencia ~0ms al mover sliders
        getFilterValue: (f: Feat) => [
          get<number>(f, 'magnitud') ?? 0,
          parseInt((get<string>(f, 'fecha') ?? '1960-01-01').substring(0, 4)),
          profCode(get<string>(f, 'tipo_profundidad') ?? null),
        ] as [number, number, number],
        filterRange: [
          [filtros.mag_min, filtros.mag_max],
          [filtros.year_start, filtros.year_end],
          filtros.profundidad
            ? [profCode(filtros.profundidad), profCode(filtros.profundidad)]
            : [0, 99],
        ] as [[number,number],[number,number],[number,number]],
        extensions: [new DataFilterExtension({ filterSize: 3 })],
        updateTriggers: {
          getFillColor:   [],       // color no cambia con filtros
          getFilterValue: [],       // valor calculado desde props estáticas del feature
          // filterRange se actualiza automáticamente como prop directa (no necesita updateTrigger)
        },
      }))

    // — Fallas geológicas —
    if (capas.fallas && fallas)
      layers.push(new GeoJsonLayer({
        id: 'fallas', data: fallas,
        getLineColor: (f: Feat) =>
          get<boolean>(f, 'activa')
            ? [220,38,38,220] as [number,number,number,number]
            : [156,163,175,140] as [number,number,number,number],
        lineWidthMinPixels: 1.5, lineWidthMaxPixels: 5,
        pickable: true, autoHighlight: true,
        highlightColor: [255,200,0,60],
        updateTriggers: { getLineColor: [] },
      }))

    // — Infraestructura crítica —
    if (capas.infraestructura && infraestructura)
      layers.push(new ScatterplotLayer({
        id: 'infraestructura',
        data: infraestructura.features,
        getPosition:  (f: FPt)  => f.geometry.coordinates as [number,number],
        getRadius:    700, radiusUnits: 'meters',
        getFillColor: (f: Feat) => infraColor(get<string>(f, 'tipo') ?? ''),
        getLineColor: [255,255,255,200] as [number,number,number,number],
        radiusMinPixels: 5, radiusMaxPixels: 20,
        stroked: true, lineWidthMinPixels: 1.5,
        pickable: true, autoHighlight: true,
        highlightColor: [255,255,255,100],
        updateTriggers: { getFillColor: [] },
      }))

    // — Estaciones de monitoreo —
    if (capas.estaciones && estaciones)
      layers.push(new ScatterplotLayer({
        id: 'estaciones',
        data: estaciones.features,
        getPosition:  (f: FPt)  => f.geometry.coordinates as [number,number],
        getRadius:    500, radiusUnits: 'meters',
        getFillColor: (f: Feat) =>
          get<string>(f, 'tipo') === 'sismica'
            ? [16,185,129,230] as [number,number,number,number]
            : [56,189,248,230] as [number,number,number,number],
        getLineColor: [255,255,255,180] as [number,number,number,number],
        radiusMinPixels: 4, radiusMaxPixels: 14,
        stroked: true, lineWidthMinPixels: 1.5,
        pickable: true, autoHighlight: true,
        highlightColor: [255,255,255,80],
        updateTriggers: { getFillColor: [] },
      }))

    return layers
  }, [
    capas, sismos, heatmapData, departamentos, distritos, fallas,
    inundaciones, tsunamis, deslizamientos, infraestructura, estaciones,
    filtros.mag_min, filtros.mag_max, filtros.year_start, filtros.year_end, filtros.profundidad,
  ])

  // ── Actualizar capas en el overlay ─────────────────────
  useEffect(() => {
    overlayRef.current?.setProps({ layers: buildLayers() })
  }, [buildLayers])

  return (
    <div ref={mapDiv} style={{ position: 'absolute', inset: 0 }} />
  )
}