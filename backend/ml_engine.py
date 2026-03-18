#!/usr/bin/env python3
# ML Engine module - minimal implementation for v9.0
import logging
import os
import pickle
from pathlib import Path
from typing import Optional, Dict, Any

import numpy as np

logger = logging.getLogger(__name__)

AMENAZAS_VALIDAS = ["deslizamiento", "inundacion", "tsunami"]
MODELO_DIR = Path("/app/models") if os.path.exists("/app/models") else Path("./models")
MODELO_DIR.mkdir(exist_ok=True)

class SusceptibilityModel:
    def __init__(self, model_dir: Path = MODELO_DIR):
        self.model_dir = model_dir
        self.models = {}
        self.metadata = {
            "deslizamiento": {"features": ["pendiente", "precipitacion", "litologia", "cobertura"], "version": "XGBoost v1.0", "trained": False, "accuracy": 0.0},
            "inundacion": {"features": ["elevacion", "distancia_rio", "cuenca", "precipitacion"], "version": "RF v1.0", "trained": False, "accuracy": 0.0},
            "tsunami": {"features": ["distancia_costa", "batimetria", "sismicidad", "peligro_sismico"], "version": "LR v1.0", "trained": False, "accuracy": 0.0},
        }
    
    def is_trained(self, amenaza: str) -> bool:
        if amenaza not in AMENAZAS_VALIDAS: return False
        return (self.model_dir / f"{amenaza}_model.pkl").exists()
    
    def load_model(self, amenaza: str) -> bool:
        if amenaza not in AMENAZAS_VALIDAS: return False
        model_path = self.model_dir / f"{amenaza}_model.pkl"
        if not model_path.exists(): return False
        try:
            self.models[amenaza] = pickle.load(open(model_path, "rb"))
            self.metadata[amenaza]["trained"] = True
            logger.info("ML modelo '%s' cargado", amenaza)
            return True
        except Exception as e:
            logger.error("Error cargando '%s': %s", amenaza, e)
            return False
    
    def predict_punto(self, amenaza: str, lon: float, lat: float, features: Dict) -> Dict:
        if amenaza not in AMENAZAS_VALIDAS:
            return {"error": "amenaza_invalida", "validas": AMENAZAS_VALIDAS}
        return {"susceptibilidad": 0.5, "nivel": "medio", "confianza": 0.0, "features_importance": {}, "intervalo_confianza": [0.3, 0.7]}
    
    def get_model_info(self, amenaza: Optional[str] = None) -> Dict:
        if amenaza is None:
            return {"version": "ML Engine v9.0", "amenazas": AMENAZAS_VALIDAS, "modelos": {am: {"entrenado": self.metadata[am]["trained"], "accuracy": self.metadata[am]["accuracy"]} for am in AMENAZAS_VALIDAS}}
        return {"amenaza": amenaza, "entrenado": self.metadata[amenaza]["trained"] if amenaza in AMENAZAS_VALIDAS else False}

susceptibility_model = SusceptibilityModel(model_dir=MODELO_DIR)
logger.info("ML Engine inicializado: %s", ", ".join(AMENAZAS_VALIDAS))
