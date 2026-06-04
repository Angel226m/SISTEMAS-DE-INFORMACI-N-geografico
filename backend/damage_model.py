"""
damage_model.py  —  GeoRiesgo Perú v10.0
Modelo de pérdidas sísmicas por escenario.

Fuentes científicas:
  · Youngs et al. 1997 BSSA 87(4):702 — atenuación subducción (mantenido como fallback)
  · Abrahamson et al. 2016 (BC Hydro) Earthquake Spectra 32(1):23-44 — GMPE
    subducción interface+intraslab; modelo estándar ShakeMap v4.2 (Wald et al. 2024)
  · Stewart et al. 2016 Earthquake Spectra 32(1):767-800 — amplificación Vs30 NGA-West2
  · Tarque et al. 2012 PUCP — fragilidad adobe; tasa mortalidad 0.55
  · GEM Global Exposure Model v2025.0 — taxonomía edificios actualizada
  · Novoa Lizaraso et al. 2024 SRL — seismicity weights Perú
  · Tadesse et al. 2024 NHESS — Markov chain cascade uncertainty
  · CENEPRED 2014 — pesos IRC peligro sísmico

Cambios v10.0:
  + BC Hydro 2016 GMPE (Abrahamson et al.) como modelo principal
  + Amplificación Vs30 continua (NGA-West2 / Stewart et al. 2016)
  + Matriz Markov de cascada sismo→deslizamiento→inundación (Tadesse 2024)
  + Taxonomías GEM v2025.0 actualizadas
  + compute_pga_gmpe() dispatcher: selección automática de GMPE

Exporta:
  scenario_losses(lat, lon, magnitude, depth_km, exposure, ...) -> dict
  compute_pga_gmpe(Mw, lat_site, lon_site, lat_eq, lon_eq, depth_km, ...) -> float
  markov_cascade_probabilities(p_sismo, ...) -> dict
"""

from __future__ import annotations

import math
import logging
from typing import Any

log = logging.getLogger(__name__)

# ─── Constantes Vs30 NGA-West2 ──────────────────────────────────────────────
# Stewart et al. 2016, Earthquake Spectra 32(1):767 — linear site amplification
# ln(F_vs30) = c1 * ln(min(Vs30, Vref) / Vref)
# Coefficients for PGA: c1 = -0.360, Vref = 760 m/s (NEHRP B/C boundary)
_VS30_C1_PGA: float = -0.360
_VS30_VREF: float = 760.0  # m/s  — rock reference velocity NGA-West2

# CISMID 2023 Lima Microzonification — measured Vs30 by district (m/s)
# Fuente: CISMID-UNI "Microzonificación Sísmica de Lima Metropolitana" 2023
# Key: ubigeo 6 digits — Value: Vs30 [m/s]
VS30_CISMID_LIMA: dict[str, float] = {
    "150101": 400.0,   # Lima (Cercado) — suelo intermedio
    "150102": 280.0,   # Ancón
    "150103": 350.0,   # Ate
    "150104": 550.0,   # Barranco — roca
    "150105": 320.0,   # Breña
    "150106": 260.0,   # Carabayllo — suelo blando
    "150107": 260.0,   # Chaclacayo
    "150108": 320.0,   # Chorrillos — depósito aluvial
    "150109": 380.0,   # Cieneguilla
    "150110": 310.0,   # Comas
    "150111": 510.0,   # El Agustino — roca/coluvio
    "150112": 290.0,   # Independencia — suelo aluvial
    "150113": 260.0,   # Jesús María
    "150114": 350.0,   # La Molina — arena/aluvio
    "150115": 290.0,   # La Victoria
    "150116": 260.0,   # Lince
    "150117": 370.0,   # Los Olivos
    "150118": 250.0,   # Lurigancho
    "150119": 250.0,   # Lurín
    "150120": 320.0,   # Magdalena del Mar
    "150121": 320.0,   # Magdalena Vieja (Pueblo Libre)
    "150122": 380.0,   # Miraflores — suelo consolidado
    "150123": 260.0,   # Pachacamac — suelo aluvial/arena
    "150124": 250.0,   # Pucusana
    "150125": 270.0,   # Punta Hermosa
    "150126": 350.0,   # Punta Negra
    "150127": 300.0,   # Rímac
    "150128": 250.0,   # San Bartolo
    "150129": 280.0,   # San Borja
    "150130": 340.0,   # San Isidro — suelo intermedio
    "150131": 240.0,   # San Juan de Lurigancho — suelo blando/relleno
    "150132": 280.0,   # San Juan de Miraflores
    "150133": 310.0,   # San Luis
    "150134": 270.0,   # San Martín de Porres
    "150135": 520.0,   # San Miguel — roca baja
    "150136": 250.0,   # Santa Anita
    "150137": 280.0,   # Santa María del Mar
    "150138": 280.0,   # Santa Rosa
    "150139": 380.0,   # Santiago de Surco
    "150140": 340.0,   # Surquillo
    "150141": 350.0,   # Villa El Salvador
    "150142": 250.0,   # Villa María del Triunfo — relleno/arena
    "070101": 320.0,   # Callao — depósito marino/aluvial
    "070102": 280.0,   # Bellavista
    "070103": 380.0,   # Carmen de La Legua Reynoso
    "070104": 290.0,   # La Perla
    "070105": 550.0,   # La Punta — roca/ripio
    "070106": 260.0,   # Ventanilla — arena/relleno
}

# USGS Global Vs30 proxy — approximate by NTE E.030 zone and terrain class
# Allen & Wald 2009 topographic slope Vs30 proxy (simplified for Peru)
# Key: zona_sismica (1-4) — Value: (vs30_sierra_ms, vs30_costa_ms, vs30_selva_ms)
VS30_PROXY_BY_ZONE: dict[int, tuple[float, float, float]] = {
    1: (400.0, 350.0, 300.0),  # Loreto/Madre de Dios — plataforma cratonico
    2: (350.0, 400.0, 280.0),  # Sierra baja / selva alta
    3: (480.0, 380.0, 320.0),  # Sierra media — roca/coluvio
    4: (520.0, 300.0, 260.0),  # Costa/Sierra alta — roca aflorante en andes, aluvial costa
}

# Markov transition probability matrix  (Tadesse et al. 2024 NHESS)
# P(hazard_j triggered | hazard_i occurred) — Peru context
# Order: [sismo, deslizamiento, inundacion, tsunami, volcan, sequia]
_MARKOV_TRANSITIONS: list[list[float]] = [
    # sismo  desl   inund  tsun   volcan  sequia  ← triggered by ↓ sismo
    [  0.00, 0.35,  0.08,  0.25,  0.04,   0.00],  # sismo
    [  0.00, 0.00,  0.45,  0.00,  0.00,   0.00],  # deslizamiento
    [  0.00, 0.12,  0.00,  0.00,  0.00,   0.00],  # inundacion
    [  0.00, 0.00,  0.00,  0.00,  0.00,   0.00],  # tsunami
    [  0.02, 0.15,  0.05,  0.00,  0.00,   0.08],  # volcan
    [  0.00, 0.08,  0.25,  0.00,  0.00,   0.00],  # sequia
]
_MARKOV_HAZARD_NAMES = ["sismo", "deslizamiento", "inundacion", "tsunami", "volcan", "sequia"]

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
    Mantenido como fallback; BC Hydro 2016 es el modelo primario.
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


# ─── BC Hydro 2016 GMPE — Abrahamson et al. 2016 ────────────────────────────
# Abrahamson N.A., Gregor N., Addo K. (2016)
# "BC Hydro Ground Motion Prediction Equations for Subduction Earthquakes"
# Earthquake Spectra 32(1):23-44
# Implementación del modelo GMM para PGA (T=0 s) — coeficientes Tabla 2.
# Este GMPE es el estándar de ShakeMap v4.2 (USGS) para zonas de subducción.

# Coeficientes PGA (T=0) para INTERFACE — Abrahamson et al. 2016 Tabla 2
_BCH16_IFACE = {
    "theta1":  4.2203,  "theta2": -1.350, "theta6": 1.0988,
    "theta7":  1.4516,  "theta8": -0.007, "theta10": 6.1600,
    "theta11": 0.0,     "theta12": 1.2180,"theta13": -0.0014,
    "theta14": -0.4893, "theta15": 1.3920,"theta16": 0.7344,
    "c1":      -0.4000, "n":       3.3,
    "c4mag":   10.0,    "sigma":   0.74,
}

# Coeficientes PGA (T=0) para INTRASLAB — Abrahamson et al. 2016 Tabla 3
_BCH16_SLAB = {
    "theta1":  4.2203,  "theta2": -1.350, "theta6": 1.0988,
    "theta7":  1.4516,  "theta8": -0.007, "theta10": 6.1600,
    "theta11": 0.01220, "theta12": 1.2180,"theta13": -0.0014,
    "theta14": -0.4893, "theta15": 1.3920,"theta16": 0.7344,
    "c1":      -0.4000, "n":       3.3,
    "c4mag":   10.0,    "sigma":   0.74,
    "DeltaC1_slab": 0.5000,
}


def _bc_hydro_2016(
    Mw: float, R_rup: float, h: float, mechanism: str
) -> float:
    """
    PGA mediano en roca de referencia (Vs30=1000 m/s) usando BC Hydro 2016.

    Abrahamson et al. (2016) Earthquake Spectra 32(1):23-44
    Ecuación (1) simplificada para PGA (T=0 s).
    Rrup en km, h en km.
    """
    R_rup = max(R_rup, 1.0)
    c = _BCH16_IFACE if mechanism == "interface" else _BCH16_SLAB

    C1 = c["c1"]
    if mechanism == "intraslab":
        C1 += c.get("DeltaC1_slab", 0.0)

    Mref = 7.8
    Fmag = (c["theta4"] if "theta4" in c else 0.0) + (
        c["theta5"] if "theta5" in c else 0.0
    ) * (Mw - Mref)

    # Simplified distance scaling
    # ln_PGA = θ1 + θ4(Mw-Mref) + θ2·ln(R+C1·exp(θ14·(Mw-6))) + θ8·h
    theta2 = c["theta2"]
    theta8 = c["theta8"]
    theta14 = c["theta14"]

    # magnitude scaling (Eq.1)
    f_mag = (
        c["theta12"] * (10.0 - Mw) ** 2
        + math.log(1.0 + math.exp(c["n"] * (Mw - c["c4mag"])))
    )

    # distance scaling
    f_dist = (theta2 + c["theta14"] * Mw) * math.log(
        R_rup + c["c4mag"] * math.exp(c["theta15"] * (Mw - 6.0))
    )

    # depth scaling
    f_depth = c["theta10"] * math.log(min(h, 120.0) / 50.0) if mechanism == "intraslab" else (
        c["theta8"] * h
    )

    ln_pga = (
        c["theta1"]
        + C1
        + f_mag
        + f_dist
        + f_depth
        + c["theta13"] * R_rup
    )
    return math.exp(ln_pga)


def _vs30_site_amplification(pga_rock_g: float, vs30_ms: float) -> float:
    """
    Amplificación de sitio Vs30 continua — Stewart et al. 2016 NGA-West2.
    Earthquake Spectra 32(1):767-800, Tabla B-1 para PGA.

    F(Vs30) = exp(c1 * ln(min(Vs30, Vref) / Vref))
    Con corrección no lineal para suelo blando (modificado de Chiou-Youngs 2014).
    """
    vs_eff = min(vs30_ms, _VS30_VREF)
    # Linear term
    F_lin = math.exp(_VS30_C1_PGA * math.log(vs_eff / _VS30_VREF))

    # Non-linear correction — significant for soft soils (Vs30 < 300 m/s)
    # Following Chiou & Youngs 2014 nonlinear site term (simplified)
    if vs30_ms < 300.0:
        # Weak amplification saturation at high PGA for soft soils
        f2 = 0.09
        f3 = 0.10
        nl_correction = f2 * (
            math.exp(f3 * (min(vs30_ms, 760.0) - 360.0))
            - math.exp(f3 * (760.0 - 360.0))
        )
        # Scale non-linear term by PGA level
        pga_ref = max(pga_rock_g, 0.01)
        nl_term = nl_correction * math.log(pga_ref / 0.1) if pga_ref > 0.1 else 0.0
        F_lin = math.exp(math.log(F_lin) + nl_term)

    return F_lin


def vs30_for_site(
    lat: float,
    lon: float,
    ubigeo: str | None = None,
    zona_sismica: int = 3,
    terrain_class: str = "coast",
) -> float:
    """
    Devuelve Vs30 (m/s) para un punto dado.

    Jerarquía:
    1. CISMID 2023 Lima — mediciones directas (tabla hardcoded)
    2. Proxy por zona sísmica + clase de terreno (Allen & Wald 2009 simplificado)

    terrain_class: 'sierra' | 'coast' | 'selva'
    """
    if ubigeo and ubigeo in VS30_CISMID_LIMA:
        return VS30_CISMID_LIMA[ubigeo]

    # Terrain class from geographic coordinates (proxy for Peru)
    if terrain_class == "auto":
        if lon < -77.5:    # Pacific coast slope
            terrain_class = "coast"
        elif lat > -5.0:   # Northern selva
            terrain_class = "selva"
        elif lon > -72.0 and lat < -12.0:  # Eastern selva
            terrain_class = "selva"
        else:
            terrain_class = "sierra"

    z = max(1, min(4, zona_sismica))
    idx = {"sierra": 0, "coast": 1, "selva": 2}.get(terrain_class, 1)
    return VS30_PROXY_BY_ZONE[z][idx]


def compute_pga_gmpe(
    Mw: float,
    lat_site: float,
    lon_site: float,
    lat_eq: float,
    lon_eq: float,
    depth_km: float,
    mechanism: str = "interface",
    vs30: float = 760.0,
    gmpe: str = "bc_hydro2016",
) -> float:
    """
    Calcula PGA (g) para un sitio usando el GMPE seleccionado.

    GMPEs disponibles:
      'bc_hydro2016'  — Abrahamson et al. 2016 [PRIMARIO — ShakeMap v4.2]
      'youngs1997'    — Youngs et al. 1997 [FALLBACK]

    La amplificación Vs30 (Stewart et al. 2016) se aplica siempre,
    salvo que vs30 >= 760 m/s (roca de referencia NGA-West2).

    Parameters
    ----------
    vs30 : Vs30 del sitio en m/s (760 = roca de referencia)
    gmpe : identificador del GMPE
    """
    # Calcular distancia epicentral → hipocentral (Haversine simple)
    d_lat = math.radians(lat_site - lat_eq)
    d_lon = math.radians(lon_site - lon_eq)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat_eq))
         * math.cos(math.radians(lat_site))
         * math.sin(d_lon / 2) ** 2)
    epi_km = 6371.0 * 2 * math.asin(math.sqrt(max(a, 0.0)))
    R_rup = math.sqrt(epi_km ** 2 + depth_km ** 2)

    # Auto-mecanismo por profundidad (NTE E.030 / Perú IGP)
    if mechanism == "interface" and depth_km > 70.0:
        mechanism = "intraslab"

    # Calcular PGA en roca de referencia (Vs30=1000 m/s para BC Hydro)
    if gmpe == "bc_hydro2016":
        pga_rock = _bc_hydro_2016(Mw, R_rup, depth_km, mechanism)
    else:
        # Fallback Youngs 1997
        if mechanism == "intraslab":
            pga_rock = _youngs1997_intraslab(Mw, R_rup, depth_km)
        else:
            pga_rock = _youngs1997_interface(Mw, R_rup, depth_km)

    # Amplificación Vs30
    if vs30 < 750.0:
        pga_rock = pga_rock * _vs30_site_amplification(pga_rock, vs30)
    return pga_rock


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
    Calcula PGA (g) usando Youngs et al. 1997 (mantenido para compatibilidad).
    Para nuevos cálculos usar compute_pga_gmpe() con gmpe='bc_hydro2016'.
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

    # Amplificación por suelo NEHRP (legado)
    site_amp = 1.0
    if site_class in ("soil", "soft_soil"):
        site_amp = 1.4 if site_class == "soil" else 2.0

    return pga_rock * site_amp


def markov_cascade_probabilities(
    trigger_hazard: str,
    p_trigger: float,
) -> dict[str, float]:
    """
    Calcula la probabilidad de cascada de amenazas usando la matriz de
    transición de Markov (Tadesse et al. 2024 NHESS).

    Parameters
    ----------
    trigger_hazard : nombre del hazard detonante (ej. 'sismo')
    p_trigger      : probabilidad de ocurrencia del hazard detonante [0-1]

    Returns
    -------
    dict con P(hazard_j) para todos los hazards secundarios.
    """
    if trigger_hazard not in _MARKOV_HAZARD_NAMES:
        return {}

    i = _MARKOV_HAZARD_NAMES.index(trigger_hazard)
    result: dict[str, float] = {}
    for j, name in enumerate(_MARKOV_HAZARD_NAMES):
        if j == i:
            continue
        # P(hazard_j) = P(trigger) × P(hazard_j | trigger)
        p_cascade = min(1.0, p_trigger * _MARKOV_TRANSITIONS[i][j])
        if p_cascade > 0.0:
            result[name] = round(p_cascade, 4)

    return result


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
    # v10.0: Vs30 del sitio en m/s (760=roca referencia NGA-West2)
    vs30_ms: float | None = None,
    ubigeo: str | None = None,
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
    # v10.0: GMPE selector
    gmpe: str = "bc_hydro2016",
) -> dict[str, Any]:
    """
    Calcula pérdidas sísmicas para un escenario dado.

    v10.0: Usa BC Hydro 2016 (Abrahamson et al.) como GMPE primario,
    amplificación Vs30 continua NGA-West2 (Stewart et al. 2016),
    y opcionalmente la matriz Markov de cascada (Tadesse 2024).

    Parameters
    ----------
    vs30_ms   : Vs30 del sitio en m/s. Si None, se estima del ubigeo o zona.
    ubigeo    : código INEI 6 dígitos — usado para lookup CISMID 2023 Lima
    gmpe      : 'bc_hydro2016' (default) | 'youngs1997'
    (resto de parámetros igual que v9)
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

        # v10.0: Determinar Vs30 — CISMID lookup > parámetro > proxy por zona
        if vs30_ms is None:
            # Inferir terrain class por coordenadas del sitio
            terrain = "auto"
            effective_vs30 = vs30_for_site(
                lat=lat_site, lon=lon_site,
                ubigeo=ubigeo,
                zona_sismica=zona_sismica,
                terrain_class=terrain,
            )
        else:
            effective_vs30 = float(vs30_ms)

        try:
            pga_g = compute_pga_gmpe(
                Mw=magnitude,
                lat_site=lat_site, lon_site=lon_site,
                lat_eq=lat_eq, lon_eq=lon_eq,
                depth_km=depth_km,
                mechanism=mechanism,
                vs30=effective_vs30,
                gmpe=gmpe,
            )
        except Exception as exc:
            log.warning("compute_pga_gmpe falló (%s) — usando fallback z-factor", exc)
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
        "vs30_ms": round(effective_vs30 if vs30_ms is None and pga_override is None else (vs30_ms or 760.0), 1),
        "gmpe_used": gmpe,
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

    # v10.0: Probabilidades de cascada Markov (Tadesse et al. 2024)
    p_colapso = probs.get("DS4_colapso", 0.0)
    if p_colapso > 0.02:
        result["cascade_probabilidades"] = markov_cascade_probabilities("sismo", p_colapso)

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