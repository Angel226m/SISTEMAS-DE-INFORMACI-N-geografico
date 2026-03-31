"""
damage_model.py  —  GeoRiesgo Perú v9.0
Modelo de pérdidas sísmicas por escenario.

Fuentes científicas:
  · Youngs et al. 1997 BSSA 87(4):702 — atenuación subducción (interface + intraslab)
  · Tarque et al. 2012 PUCP — fragilidad adobe; tasa mortalidad 0.55
  · GEM Vulnerability / Exposure Model 2023 — taxonomía edificios
  · CENEPRED 2014 — pesos IRC peligro sísmico

Exporta:
  scenario_losses(lat, lon, magnitude, depth_km, exposure, ...) -> dict
"""

from __future__ import annotations

import math
import logging
from typing import Any

log = logging.getLogger(__name__)

# ─── Constantes globales ────────────────────────────────────────────────────

# 4 estados de daño (DS0 = sin daño implícito)
DS = ["DS1_leve", "DS2_moderado", "DS3_extenso", "DS4_colapso"]

# Fracción de pérdida por estado de daño (GEM 2023)
LOSS_RATIO: dict[str, float] = {
    "DS1_leve":     0.05,
    "DS2_moderado": 0.20,
    "DS3_extenso":  0.55,
    "DS4_colapso":  1.00,
}

# Mortalidad condicional por estado de daño (Tarque 2012, adobe / mampostería)
MORTALITY_DS: dict[str, float] = {
    "DS1_leve":     0.00,
    "DS2_moderado": 0.01,
    "DS3_extenso":  0.10,
    "DS4_colapso":  0.55,
}

# Parámetros de fragilidad lognormal (μ_ln_PGA [g], β) por taxonomía
# DS1..DS4 cada fila = (median_PGA_g, log_std)
FRAGILITY: dict[str, list[tuple[float, float]]] = {
    # Adobe / tapial  (Tarque 2012)
    "ADOBE":       [(0.05, 0.60), (0.10, 0.55), (0.20, 0.55), (0.35, 0.60)],
    # Mampostería sin refuerzo  (GEM 2023)
    "URM":         [(0.08, 0.60), (0.15, 0.55), (0.30, 0.55), (0.50, 0.60)],
    # Mampostería confinada  (GEM 2023)
    "CM":          [(0.12, 0.60), (0.25, 0.55), (0.50, 0.55), (0.80, 0.60)],
    # Concreto reforzado  (GEM 2023)
    "RC":          [(0.20, 0.60), (0.40, 0.55), (0.70, 0.55), (1.10, 0.60)],
    # Madera  (GEM 2023)
    "WOOD":        [(0.10, 0.65), (0.20, 0.60), (0.40, 0.60), (0.70, 0.65)],
    # Infraestructura crítica  (GEM 2023)
    "INFRA":       [(0.15, 0.55), (0.30, 0.50), (0.60, 0.50), (1.00, 0.55)],
    # Por defecto (mix)
    "DEFAULT":     [(0.08, 0.60), (0.18, 0.55), (0.38, 0.55), (0.65, 0.60)],
}

# Zona sísmica → factor amplificación suelo (NTE E.030 Peru)
ZONE_FACTOR: dict[int, float] = {1: 0.10, 2: 0.25, 3: 0.35, 4: 0.45}


# ─── Funciones de atenuación ────────────────────────────────────────────────

def _youngs1997_interface(Mw: float, R_rup: float, h: float) -> float:
    """
    PGA mediano en roca (g) para subducción de interfaz.
    Youngs et al. 1997 BSSA 87(4):702, Tabla 2 columna INTERFACE.
    """
    if R_rup < 1.0:
        R_rup = 1.0
    ln_Y = (
        -2.991
        + 1.414 * Mw
        + -1.00 * math.log(R_rup + 1.814 * math.exp(0.697 * Mw))
        + 0.012 * h
        + 0.0
    )
    return math.exp(ln_Y)


def _youngs1997_intraslab(Mw: float, R_rup: float, h: float) -> float:
    """
    PGA mediano en roca (g) para subducción intraplaca.
    Youngs et al. 1997, Tabla 2 columna INTRASLAB.
    """
    if R_rup < 1.0:
        R_rup = 1.0
    ln_Y = (
        -0.458
        + 1.414 * Mw
        + -1.00 * math.log(R_rup + 1.353 * math.exp(0.690 * Mw))
        + 0.0082 * h
        + 0.9
    )
    return math.exp(ln_Y)


def pga_youngs97(
    Mw: float,
    lat_site: float,
    lon_site: float,
    lat_eq: float,
    lon_eq: float,
    depth_km: float,
    mechanism: str = "interface",
    site_class: str = "rock",
) -> float:
    """
    Calcula PGA (g) usando Youngs et al. 1997.

    Parameters
    ----------
    Mw          : magnitud momento
    lat_site / lon_site  : coordenadas del sitio
    lat_eq / lon_eq      : epicentro del sismo
    depth_km    : profundidad hipocentral
    mechanism   : 'interface' | 'intraslab'
    site_class  : 'rock' | 'soil' (amplificación 1.4x para suelo)
    """
    # Distancia epicentral → hipocentral (Rrup simple)
    d_lat = math.radians(lat_site - lat_eq)
    d_lon = math.radians(lon_site - lon_eq)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat_eq))
         * math.cos(math.radians(lat_site))
         * math.sin(d_lon / 2) ** 2)
    epi_km = 6371.0 * 2 * math.asin(math.sqrt(max(a, 0)))
    R_rup = math.sqrt(epi_km ** 2 + depth_km ** 2)

    if mechanism == "intraslab":
        pga_rock = _youngs1997_intraslab(Mw, R_rup, depth_km)
    else:
        pga_rock = _youngs1997_interface(Mw, R_rup, depth_km)

    # Amplificación por suelo NEHRP
    site_amp = 1.0
    if site_class in ("soil", "soft_soil"):
        site_amp = 1.4 if site_class == "soil" else 2.0

    return pga_rock * site_amp


# ─── Curvas de fragilidad lognormal ─────────────────────────────────────────

def _lognormal_cdf(x: float, median: float, beta: float) -> float:
    """P(DS ≥ ds | PGA = x)  distribución lognormal."""
    if x <= 0 or median <= 0 or beta <= 0:
        return 0.0
    z = math.log(x / median) / beta
    # Aproximación erfcc para la CDF normal estándar
    return 0.5 * (1.0 + _erf(z / math.sqrt(2)))


def _erf(x: float) -> float:
    """Implementación de erf sin scipy."""
    # Abramowitz & Stegun 7.1.26
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + 0.3275911 * x)
    poly = t * (0.254829592
                + t * (-0.284496736
                       + t * (1.421413741
                              + t * (-1.453152027
                                     + t * 1.061405429))))
    return sign * (1.0 - poly * math.exp(-x * x))


def fragility_exceedance(pga_g: float, taxonomy: str = "DEFAULT") -> dict[str, float]:
    """
    Devuelve P(DS ≥ dsi | PGA) para DS1..DS4.
    """
    params = FRAGILITY.get(taxonomy.upper(), FRAGILITY["DEFAULT"])
    return {
        DS[i]: _lognormal_cdf(pga_g, params[i][0], params[i][1])
        for i in range(4)
    }


def fragility_probability(pga_g: float, taxonomy: str = "DEFAULT") -> dict[str, float]:
    """
    Devuelve P(DS = dsi | PGA)  (probabilidades marginales).
    """
    exc = fragility_exceedance(pga_g, taxonomy)
    p_exc = [exc[ds] for ds in DS]  # P(DS≥1), P(DS≥2), P(DS≥3), P(DS≥4)
    probs = {}
    probs["DS0_ninguno"] = max(0.0, 1.0 - p_exc[0])
    for i, ds in enumerate(DS):
        upper = p_exc[i]
        lower = p_exc[i + 1] if i < 3 else 0.0
        probs[ds] = max(0.0, upper - lower)
    return probs


# ─── Función principal ───────────────────────────────────────────────────────

def scenario_losses(
    *,
    # Parámetros del sismo
    lat_eq: float,
    lon_eq: float,
    magnitude: float,
    depth_km: float = 30.0,
    mechanism: str = "interface",
    # Sitio de evaluación
    lat_site: float | None = None,
    lon_site: float | None = None,
    site_class: str = "soil",
    zona_sismica: int = 3,
    # Exposición
    exposure: dict[str, Any] | None = None,
    # Parámetros de escenario
    n_pop: int = 1000,
    valor_usd: float = 1_000_000.0,
    taxonomy: str = "DEFAULT",
    pga_override: float | None = None,
    # v9.0: nuevos parámetros para escenario multi-taxonomía
    n_viviendas: int | None = None,
    mix_construccion: dict[str, float] | None = None,
    hora_del_dia: str = "dia",
) -> dict[str, Any]:
    """
    Calcula pérdidas sísmicas para un escenario dado.

    Parameters
    ----------
    lat_eq, lon_eq   : epicentro
    magnitude        : Mw
    depth_km         : profundidad hipocentral
    mechanism        : 'interface' | 'intraslab'
    lat_site, lon_site : coordenadas del sitio (si None → usa epicentro)
    site_class       : 'rock' | 'soil' | 'soft_soil'
    zona_sismica     : 1..4 (NTE E.030 Perú)
    exposure         : dict con campos opcionales:
                         n_buildings, pct_adobe, pct_urm, pct_cm,
                         pct_rc, valor_usd, n_pop
    n_pop            : población expuesta (si no viene en exposure)
    valor_usd        : valor del parque edificatorio USD
    taxonomy         : taxonomía predominante  (ADOBE|URM|CM|RC|WOOD|DEFAULT)
    pga_override     : si se provee, sobreescribe el cálculo de atenuación
    n_viviendas      : número total de viviendas (alternativa a exposure.n_buildings)
    mix_construccion : dict con porcentajes por tipo {"adobe":0.3, "ladrillo_conf":0.4, ...}
    hora_del_dia     : 'dia' | 'noche' — ajusta mortalidad (noche ×1.4 Coburn et al. 1992)

    Returns
    -------
    dict con:
      pga_g, intensity_label, damage_states (probs), expected_loss_usd,
      expected_loss_ratio, fatalities_estimate, ds_counts (si n_buildings dado),
      zona_sismica, mechanism, por_tipo (si mix_construccion dado)
    """

    # ── Validación de entradas ─────────────────────────────────────────────
    if not (-90 <= lat_eq <= 90) or not (-180 <= lon_eq <= 180):
        raise ValueError(f"Coordenadas del epicentro fuera de rango: lat={lat_eq}, lon={lon_eq}")
    if not (0.0 < magnitude <= 10.0):
        raise ValueError(f"Magnitud fuera de rango razonable: {magnitude}")
    if depth_km < 0:
        raise ValueError(f"Profundidad negativa: {depth_km}")
    if mechanism not in ("interface", "intraslab"):
        raise ValueError(f"Mecanismo no válido: {mechanism!r}")
    if hora_del_dia not in ("dia", "noche"):
        raise ValueError(f"hora_del_dia no válida: {hora_del_dia!r}")

    # ── Defaults de exposición ──────────────────────────────────────────────
    if exposure is None:
        exposure = {}

    n_buildings: int = n_viviendas or exposure.get("n_buildings", 100)
    pct_adobe: float = exposure.get("pct_adobe", 0.30)
    pop: int = int(exposure.get("n_pop", n_pop))
    val_usd: float = float(exposure.get("valor_usd", valor_usd))

    # Mortalidad nocturna: factor ×1.4 (Coburn et al. 1992 — más personas en edificios)
    mortality_factor = 1.4 if hora_del_dia == "noche" else 1.0

    # Determinar mecanismo automáticamente si no se especificó explícitamente
    if mechanism == "interface" and depth_km > 70:
        mechanism = "intraslab"

    # Taxonomía basada en porcentaje adobe si no se especifica
    if taxonomy == "DEFAULT" and pct_adobe > 0.5:
        taxonomy = "ADOBE"
    elif taxonomy == "DEFAULT" and pct_adobe > 0.2:
        taxonomy = "URM"

    # ── Calcular PGA ────────────────────────────────────────────────────────
    if pga_override is not None:
        pga_g = float(pga_override)
    else:
        if lat_site is None:
            lat_site = lat_eq
        if lon_site is None:
            lon_site = lon_eq

        try:
            pga_g = pga_youngs97(
                Mw=magnitude,
                lat_site=lat_site, lon_site=lon_site,
                lat_eq=lat_eq, lon_eq=lon_eq,
                depth_km=depth_km,
                mechanism=mechanism,
                site_class=site_class,
            )
        except Exception as exc:
            log.warning("pga_youngs97 falló: %s — usando fallback z-factor", exc)
            Z = ZONE_FACTOR.get(int(zona_sismica), 0.35)
            pga_g = Z * (magnitude / 7.0) ** 2  # estimación simplificada

    # Clamp PGA a rango razonable [0.001g, 3.0g]
    pga_g = max(0.001, min(pga_g, 3.0))

    # ── Etiqueta de intensidad (MMI aproximada) ──────────────────────────────
    if pga_g < 0.017:
        intensity_label = "I-III (imperceptible)"
    elif pga_g < 0.040:
        intensity_label = "IV (leve)"
    elif pga_g < 0.092:
        intensity_label = "V (moderado)"
    elif pga_g < 0.180:
        intensity_label = "VI (fuerte)"
    elif pga_g < 0.340:
        intensity_label = "VII (muy fuerte)"
    elif pga_g < 0.650:
        intensity_label = "VIII (severo)"
    elif pga_g < 1.240:
        intensity_label = "IX (violento)"
    else:
        intensity_label = "X+ (extremo)"

    # ── Probabilidades de daño ──────────────────────────────────────────────
    probs = fragility_probability(pga_g, taxonomy)

    # ── Pérdida esperada ─────────────────────────────────────────────────────
    expected_loss_ratio = sum(
        probs.get(ds, 0.0) * LOSS_RATIO[ds]
        for ds in DS
    )
    expected_loss_usd = val_usd * expected_loss_ratio

    # ── Estimación de fatalidades (con factor nocturno) ─────────────────────
    fatalities = sum(
        probs.get(ds, 0.0) * MORTALITY_DS[ds] * pop
        for ds in DS
    ) * mortality_factor
    fatalities_estimate = int(round(fatalities))

    # ── Conteo de edificios por estado de daño ────────────────────────────────
    ds_counts: dict[str, int] = {
        "DS0_ninguno": max(0, int(round(probs.get("DS0_ninguno", 0.0) * n_buildings))),
    }
    for ds in DS:
        ds_counts[ds] = max(0, int(round(probs.get(ds, 0.0) * n_buildings)))

    # ── Desglose multi-taxonomía (v9.0) ──────────────────────────────────────
    # mix_construccion: {"adobe": 0.30, "ladrillo_conf": 0.40, "concreto_armado": 0.25, ...}
    _MIX_TO_TAXONOMY = {
        "adobe": "ADOBE", "tapial": "ADOBE",
        "ladrillo": "URM", "ladrillo_conf": "CM", "albanileria": "CM",
        "concreto": "RC", "concreto_armado": "RC",
        "madera": "WOOD", "quincha": "WOOD",
    }
    por_tipo: dict[str, Any] | None = None
    if mix_construccion:
        por_tipo = {}
        total_loss = 0.0
        total_fatalities = 0.0
        for tipo_key, pct in mix_construccion.items():
            tax = _MIX_TO_TAXONOMY.get(tipo_key.lower(), "DEFAULT")
            tp = fragility_probability(pga_g, tax)
            lr = sum(tp.get(d, 0.0) * LOSS_RATIO[d] for d in DS)
            ft = sum(tp.get(d, 0.0) * MORTALITY_DS[d] * pop * pct for d in DS) * mortality_factor
            loss_usd = val_usd * pct * lr
            total_loss += loss_usd
            total_fatalities += ft
            por_tipo[tipo_key] = {
                "taxonomy": tax,
                "pct": round(pct, 4),
                "loss_ratio": round(lr, 4),
                "loss_usd": round(loss_usd, 2),
                "fatalities": int(round(ft)),
                "damage_probabilities": {k: round(v, 4) for k, v in tp.items()},
            }
        # Sobre-escribir totales con el desglose ponderado
        expected_loss_usd = round(total_loss, 2)
        expected_loss_ratio = round(total_loss / val_usd, 4) if val_usd > 0 else 0.0
        fatalities_estimate = int(round(total_fatalities))

    # ── Resultado ─────────────────────────────────────────────────────────────
    result: dict[str, Any] = {
        "pga_g": round(pga_g, 4),
        "intensity_label": intensity_label,
        "taxonomy": taxonomy,
        "mechanism": mechanism,
        "zona_sismica": zona_sismica,
        "damage_probabilities": {k: round(v, 4) for k, v in probs.items()},
        "expected_loss_ratio": round(expected_loss_ratio, 4),
        "expected_loss_usd": round(expected_loss_usd, 2),
        "fatalities_estimate": fatalities_estimate,
        "ds_counts": ds_counts,
        "n_buildings": n_buildings,
        "n_pop": pop,
        "exposure_value_usd": val_usd,
        "hora_del_dia": hora_del_dia,
        "inputs": {
            "lat_eq": lat_eq, "lon_eq": lon_eq,
            "magnitude": magnitude, "depth_km": depth_km,
        },
    }
    if por_tipo is not None:
        result["por_tipo"] = por_tipo
    return result


# ─── Función auxiliar: escenario rápido desde ID de sismo ───────────────────

def scenario_from_sismo(sismo: dict[str, Any], distrito: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Wrapper conveniente: recibe fila de BD (sismos) + opcional fila distritos.
    """
    lat_eq = float(sismo.get("latitud", -12.0))
    lon_eq = float(sismo.get("longitud", -76.0))
    Mw = float(sismo.get("magnitud", 6.0))
    depth = float(sismo.get("profundidad", 30.0))
    mec = "intraslab" if depth > 70 else "interface"

    exposure: dict[str, Any] = {}
    lat_s, lon_s = lat_eq, lon_eq
    zona = 3

    if distrito:
        lat_s = float(distrito.get("lat_centroid", lat_eq))
        lon_s = float(distrito.get("lon_centroid", lon_eq))
        zona = int(distrito.get("zona_sismica", 3))
        exposure = {
            "n_buildings": int(distrito.get("n_viviendas", 1000)),
            "pct_adobe":   float(distrito.get("pct_adobe", 0.30)),
            "n_pop":       int(distrito.get("poblacion", 5000)),
            "valor_usd":   float(distrito.get("valor_expuesto_usd", 1_000_000)),
        }

    return scenario_losses(
        lat_eq=lat_eq, lon_eq=lon_eq,
        magnitude=Mw, depth_km=depth,
        mechanism=mec,
        lat_site=lat_s, lon_site=lon_s,
        zona_sismica=zona,
        exposure=exposure,
    )


# ─── Utilidades para batch ──────────────────────────────────────────────────

def batch_losses(
    sismos: list[dict[str, Any]],
    distritos: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    Calcula pérdidas para una lista de sismos.
    Cada resultado incluye sismo_id y distrito_ubigeo si disponibles.
    """
    results = []
    for sismo in sismos:
        try:
            dist = None
            if distritos:
                ubigeo = sismo.get("ubigeo_cercano")
                dist = next((d for d in distritos if d.get("ubigeo") == ubigeo), None)
            res = scenario_from_sismo(sismo, dist)
            res["sismo_id"] = sismo.get("id")
            if dist:
                res["ubigeo"] = dist.get("ubigeo")
                res["distrito"] = dist.get("nombre")
            results.append(res)
        except Exception as exc:
            log.error("batch_losses sismo=%s error=%s", sismo.get("id"), exc)
    return results


# ─── Self-test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json

    # Sismo de Lima 8.0 Mw, interfaz, suelo blando
    r = scenario_losses(
        lat_eq=-11.9,
        lon_eq=-77.1,
        magnitude=8.0,
        depth_km=25.0,
        mechanism="interface",
        lat_site=-12.05,
        lon_site=-77.05,
        site_class="soft_soil",
        zona_sismica=4,
        exposure={
            "n_buildings": 500_000,
            "pct_adobe": 0.35,
            "n_pop": 2_500_000,
            "valor_usd": 50_000_000_000,
        },
        taxonomy="ADOBE",
    )
    print(json.dumps(r, indent=2, ensure_ascii=False))