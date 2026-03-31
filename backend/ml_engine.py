#!/usr/bin/env python3
# ══════════════════════════════════════════════════════════════════
# GeoRiesgo Perú — ML Engine v9.0
#
# XGBoost susceptibility models for multi-hazard assessment.
# Pipeline: feature extraction → VIF → SMOTE-Tomek → Optuna → XGBoost → SHAP
#
# Fuentes:
#   Kumar et al. 2023 Remote Sensing 15(5):1376 — landslide susceptibility
#   Gill & Malamud 2014 Rev. Geophys. — cascade hazard amplification
# ══════════════════════════════════════════════════════════════════

from __future__ import annotations

import json
import logging
import os
import pickle
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)

AMENAZAS_VALIDAS = ["deslizamiento", "inundacion", "sequia"]
MODELO_DIR = Path("/app/models") if os.path.exists("/app/models") else Path("./models")
MODELO_DIR.mkdir(exist_ok=True)

# Feature definitions per hazard type
FEATURES_POR_AMENAZA: dict[str, list[str]] = {
    "deslizamiento": [
        "pendiente_grados", "elevacion_m", "precipitacion_anual_mm",
        "distancia_falla_km", "peligro_sismico", "cobertura_vegetal",
        "tipo_suelo", "densidad_drenaje",
    ],
    "inundacion": [
        "elevacion_m", "distancia_rio_km", "precipitacion_anual_mm",
        "pendiente_grados", "area_cuenca_km2", "indice_fen",
        "tipo_suelo", "cobertura_vegetal",
    ],
    "sequia": [
        "precipitacion_anual_mm", "temperatura_media_c", "elevacion_m",
        "cobertura_vegetal", "indice_fen", "tipo_suelo",
        "evapotranspiracion", "humedad_suelo",
    ],
}

# Niveles de susceptibilidad
NIVELES = ["MUY_BAJO", "BAJO", "MEDIO", "ALTO", "MUY_ALTO"]


def _score_to_nivel(score: float) -> str:
    if score < 0.2:
        return "MUY_BAJO"
    if score < 0.4:
        return "BAJO"
    if score < 0.6:
        return "MEDIO"
    if score < 0.8:
        return "ALTO"
    return "MUY_ALTO"


class SusceptibilityModel:
    """
    Multi-hazard susceptibility model using XGBoost.

    Supports training from DB features and prediction for arbitrary points.
    Falls back to heuristic scoring when no trained model is available.
    """

    def __init__(self, model_dir: Path = MODELO_DIR):
        self.model_dir = model_dir
        self.models: dict[str, Any] = {}
        self._metadata: dict[str, dict[str, Any]] = {}
        self._training_locks: dict[str, bool] = {}

    @staticmethod
    def _build_feature_vector(
        amenaza: str, lat: float, lon: float,
        zona_sismica: int, area_km2: float,
        peligro_sismico: float, peligro_inundacion: float,
        peligro_deslizamiento: float, peligro_sequia: float,
        fallas_activas_50km: float,
    ) -> list[float]:
        """
        Build the feature vector for a given amenaza.

        Uses geographic proxy values where real raster data is unavailable:
        - pendiente ≈ |lat| * 2.5: Andes steepness correlates with latitude in Peru
          (southern Andes steeper, per Bookhagen & Strecker 2012).
        - elevación ≈ |lat| * 300 + offset: Andean uplift gradient.
        - precipitación ≈ f(lon): western Peru (coast) → drier; eastern (selva) → wetter.
        - cobertura_vegetal ≈ threshold(lat): selva vs. sierra/costa biome split.
        These proxies are adequate for district-level susceptibility ranking but
        should be replaced with real DEM/raster data for site-level assessment.
        """
        if amenaza == "deslizamiento":
            return [
                abs(lat) * 2.5,                              # proxy pendiente
                abs(lat) * 300 + 1000,                       # proxy elevación
                max(200, 2500 - abs(lon + 75) * 200),        # proxy precipitación
                max(1, fallas_activas_50km),                  # distancia falla proxy
                peligro_sismico,                              # from distrito record
                0.6 if abs(lat) > 10 else 0.3,               # proxy cobertura vegetal
                zona_sismica,                                 # proxy tipo suelo
                area_km2 / 100,                               # proxy densidad drenaje
            ]
        if amenaza == "inundacion":
            return [
                abs(lat) * 300 + 500,                        # proxy elevación
                max(0.5, abs(lon + 75) * 5),                 # proxy distancia río
                max(200, 2500 - abs(lon + 75) * 200),        # proxy precipitación
                abs(lat) * 2.5,                              # proxy pendiente
                area_km2,                                    # proxy area cuenca
                1.5 if abs(lat) < 8 else 1.0,                # proxy índice FEN
                zona_sismica,                                # proxy tipo suelo
                0.5,                                         # proxy cobertura vegetal
            ]
        # sequia
        return [
            max(200, 2500 - abs(lon + 75) * 200),            # proxy precipitación
            20 + abs(lat) * 0.5,                             # proxy temperatura
            abs(lat) * 300 + 1000,                           # proxy elevación
            0.6 if abs(lat) > 10 else 0.3,                   # proxy cobertura
            1.0,                                             # proxy FEN
            zona_sismica,                                    # proxy tipo suelo
            4.5,                                             # proxy evapotranspiración
            0.3,                                             # proxy humedad suelo
        ]

    def is_trained(self, amenaza: str) -> bool:
        if amenaza not in AMENAZAS_VALIDAS:
            return False
        if amenaza in self.models:
            return True
        return (self.model_dir / f"{amenaza}_model.pkl").exists()

    def load_model(self, amenaza: str) -> bool:
        if amenaza not in AMENAZAS_VALIDAS:
            return False
        model_path = self.model_dir / f"{amenaza}_model.pkl"
        if not model_path.exists():
            return False
        try:
            with open(model_path, "rb") as f:
                bundle = pickle.load(f)  # noqa: S301
            self.models[amenaza] = bundle["model"]
            self._metadata[amenaza] = bundle.get("metadata", {})
            logger.info("ML modelo '%s' cargado desde %s", amenaza, model_path)
            return True
        except Exception as e:
            logger.error("Error cargando modelo '%s': %s", amenaza, e)
            return False

    async def train(self, amenaza: str, conn: Any) -> dict[str, Any]:
        """
        Train an XGBoost model for the given hazard type using DB features.

        Pipeline real v9.0:
        1. Extract features from DB (distritos + amenaza-specific data)
        2. VIF — elimina variables con VIF > 10 (multicolinealidad)
        3. Train/test split estratificado 80/20
        4. SMOTE-Tomek resampling en train set
        5. Optuna bayesiano (20 trials, AUC-PR)
        6. Train final XGBoost con mejores hiperparámetros
        7. SHAP TreeExplainer feature importances
        8. Persist model + metadata
        """
        if self._training_locks.get(amenaza):
            return {"status": "already_training", "amenaza": amenaza}

        self._training_locks[amenaza] = True
        t0 = time.perf_counter()

        try:
            # Lazy imports for training dependencies
            from sklearn.metrics import (
                average_precision_score,
                f1_score,
                precision_score,
                recall_score,
                roc_auc_score,
            )
            from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split

            try:
                import xgboost as xgb
            except ImportError:
                logger.error("xgboost no disponible, usando fallback")
                return await self._train_fallback(amenaza, conn)

            # 1. Extract training data from database
            X, y, feature_names = await self._extract_features(amenaza, conn)

            if len(X) < 10:
                logger.warning(
                    "ML train '%s': solo %d muestras, insuficiente", amenaza, len(X)
                )
                return await self._train_fallback(amenaza, conn)

            # ── 2. VIF — Variance Inflation Factor ───────────────────────────
            features_elim_vif: list[str] = []
            try:
                from numpy.linalg import LinAlgError

                keep_mask = np.ones(X.shape[1], dtype=bool)
                for _ in range(X.shape[1]):
                    X_sub = X[:, keep_mask]
                    if X_sub.shape[1] < 2:
                        break
                    # VIF_j = 1 / (1 - R²_j)
                    corr = np.corrcoef(X_sub, rowvar=False)
                    # Regularise to avoid singular matrix
                    corr += np.eye(corr.shape[0]) * 1e-8
                    try:
                        inv_corr = np.linalg.inv(corr)
                    except LinAlgError:
                        break
                    vifs = np.diag(inv_corr)
                    max_vif_idx = int(np.argmax(vifs))
                    if vifs[max_vif_idx] <= 10.0:
                        break
                    # Map back to original index
                    orig_indices = np.where(keep_mask)[0]
                    drop_idx = orig_indices[max_vif_idx]
                    features_elim_vif.append(feature_names[drop_idx])
                    keep_mask[drop_idx] = False

                if features_elim_vif:
                    X = X[:, keep_mask]
                    feature_names = [f for f, k in zip(feature_names, keep_mask) if k]
                    logger.info("VIF eliminó %d features: %s", len(features_elim_vif), features_elim_vif)
            except Exception as exc:
                logger.warning("VIF falló (continuando sin filtro): %s", exc)

            # ── 3. Train/test split ──────────────────────────────────────────
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42,
                stratify=y if len(np.unique(y)) > 1 else None,
            )

            # ── 4. SMOTE-Tomek resampling ────────────────────────────────────
            tecnica_balance = "scale_pos_weight"
            n_pos = int(np.sum(y_train == 1))
            n_neg = int(np.sum(y_train == 0))
            scale_pos = max(1.0, n_neg / max(n_pos, 1))

            if n_pos >= 5 and n_neg >= 5:
                try:
                    from imblearn.combine import SMOTETomek
                    smt = SMOTETomek(random_state=42)
                    X_train, y_train = smt.fit_resample(X_train, y_train)
                    scale_pos = 1.0  # balanced after resampling
                    tecnica_balance = "SMOTE-Tomek"
                    logger.info(
                        "SMOTE-Tomek: %d→%d muestras (pos=%d, neg=%d)",
                        n_pos + n_neg, len(y_train),
                        int(np.sum(y_train == 1)), int(np.sum(y_train == 0)),
                    )
                except ImportError:
                    logger.warning("imbalanced-learn no disponible, usando scale_pos_weight")
                except Exception as exc:
                    logger.warning("SMOTE-Tomek falló: %s — usando scale_pos_weight", exc)

            # ── 5. Optuna hyperparameter optimization ────────────────────────
            best_params: dict[str, Any] = {
                "objective": "binary:logistic",
                "eval_metric": "aucpr",
                "max_depth": 6,
                "learning_rate": 0.1,
                "n_estimators": 100,
                "scale_pos_weight": scale_pos,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "random_state": 42,
                "verbosity": 0,
            }
            n_optuna_trials = 0

            if len(X_train) >= 30:
                try:
                    import optuna
                    optuna.logging.set_verbosity(optuna.logging.WARNING)

                    def _objective(trial: "optuna.Trial") -> float:
                        p = {
                            "objective": "binary:logistic",
                            "eval_metric": "aucpr",
                            "max_depth": trial.suggest_int("max_depth", 3, 8),
                            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                            "n_estimators": trial.suggest_int("n_estimators", 50, 300),
                            "scale_pos_weight": scale_pos,
                            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
                            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
                            "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                            "gamma": trial.suggest_float("gamma", 0.0, 5.0),
                            "random_state": 42,
                            "verbosity": 0,
                        }
                        clf = xgb.XGBClassifier(**p)
                        skf = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
                        scores = cross_val_score(
                            clf, X_train, y_train, cv=skf, scoring="average_precision",
                        )
                        return float(np.mean(scores))

                    study = optuna.create_study(direction="maximize")
                    study.optimize(_objective, n_trials=20, timeout=120)
                    n_optuna_trials = len(study.trials)

                    bp = study.best_params
                    best_params.update(bp)
                    logger.info(
                        "Optuna %d trials: best AUC-PR=%.3f params=%s",
                        n_optuna_trials, study.best_value, bp,
                    )
                except ImportError:
                    logger.warning("optuna no disponible, usando hiperparámetros default")
                except Exception as exc:
                    logger.warning("Optuna falló: %s — usando defaults", exc)

            # ── 6. Entrenar modelo final ─────────────────────────────────────
            model = xgb.XGBClassifier(**best_params)
            model.fit(
                X_train, y_train,
                eval_set=[(X_test, y_test)],
                verbose=False,
            )

            # ── 7. Evaluar (holdout + cross-validation) ────────────────────
            y_pred = model.predict(X_test)
            y_proba = model.predict_proba(X_test)[:, 1]

            auc_roc = float(roc_auc_score(y_test, y_proba)) if len(np.unique(y_test)) > 1 else None
            auc_pr = float(average_precision_score(y_test, y_proba)) if len(np.unique(y_test)) > 1 else None
            f1 = float(f1_score(y_test, y_pred, zero_division=0))
            precision = float(precision_score(y_test, y_pred, zero_division=0))
            recall = float(recall_score(y_test, y_pred, zero_division=0))

            # Cross-validation metrics for publication-grade reporting
            cv_auc_roc = None
            cv_auc_pr = None
            if len(X) >= 30 and len(np.unique(y)) > 1:
                try:
                    skf_eval = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
                    cv_model = xgb.XGBClassifier(**best_params)
                    cv_roc = cross_val_score(cv_model, X, y, cv=skf_eval, scoring="roc_auc")
                    cv_pr = cross_val_score(cv_model, X, y, cv=skf_eval, scoring="average_precision")
                    cv_auc_roc = round(float(np.mean(cv_roc)), 4)
                    cv_auc_pr = round(float(np.mean(cv_pr)), 4)
                    logger.info(
                        "CV 5-fold '%s': AUC-ROC=%.3f±%.3f, AUC-PR=%.3f±%.3f",
                        amenaza, np.mean(cv_roc), np.std(cv_roc),
                        np.mean(cv_pr), np.std(cv_pr),
                    )
                except Exception as exc:
                    logger.warning("Cross-validation falló: %s", exc)

            # ── 8. SHAP feature importances ──────────────────────────────────
            importances: dict[str, float]
            try:
                import shap
                explainer = shap.TreeExplainer(model)
                shap_values = explainer.shap_values(X_test)
                # Mean absolute SHAP per feature
                mean_abs_shap = np.abs(shap_values).mean(axis=0)
                importances = dict(
                    zip(feature_names, [round(float(v), 6) for v in mean_abs_shap])
                )
                logger.info("SHAP importances calculadas para '%s'", amenaza)
            except ImportError:
                logger.warning("shap no disponible, usando feature_importances_ de XGBoost")
                importances = dict(
                    zip(feature_names, [float(v) for v in model.feature_importances_])
                )
            except Exception as exc:
                logger.warning("SHAP falló: %s — usando feature_importances_", exc)
                importances = dict(
                    zip(feature_names, [float(v) for v in model.feature_importances_])
                )

            # ── 9. Guardar modelo ────────────────────────────────────────────
            metadata = {
                "amenaza": amenaza,
                "algoritmo": "XGBoost + Optuna" if n_optuna_trials > 0 else "XGBoost",
                "auc_roc": auc_roc,
                "auc_pr": auc_pr,
                "cv_auc_roc": cv_auc_roc,
                "cv_auc_pr": cv_auc_pr,
                "f1_score": f1,
                "precision_score": precision,
                "recall_score": recall,
                "n_samples": len(X),
                "n_positivos": int(np.sum(y == 1)),
                "n_negativos": int(np.sum(y == 0)),
                "ratio_imbalance": round(scale_pos, 2),
                "features_usadas": feature_names,
                "features_elim_vif": features_elim_vif or None,
                "features_elim_rfe": None,
                "importancias_shap": importances,
                "hiperparametros": best_params,
                "tecnica_balance": tecnica_balance,
                "optuna_trials": n_optuna_trials,
                "entrenado_en": datetime.now(timezone.utc).isoformat(),
                "version": "XGBoost+SMOTE-Tomek+Optuna+SHAP v9.0",
            }

            bundle = {
                "model": model,
                "metadata": metadata,
                "feature_names": feature_names,
            }
            model_path = self.model_dir / f"{amenaza}_model.pkl"
            with open(model_path, "wb") as f:
                pickle.dump(bundle, f)

            self.models[amenaza] = model
            self._metadata[amenaza] = metadata

            # 10. Save metadata to DB
            await self._save_metadata_db(amenaza, metadata, conn)

            elapsed = time.perf_counter() - t0
            logger.info(
                "ML train '%s': %d muestras, AUC-ROC=%.3f, AUC-PR=%.3f, F1=%.3f en %.1fs [%s]",
                amenaza, len(X), auc_roc or 0, auc_pr or 0, f1, elapsed, tecnica_balance,
            )
            return metadata

        except Exception as exc:
            logger.error("ML train '%s' falló: %s", amenaza, exc)
            return await self._train_fallback(amenaza, conn)
        finally:
            self._training_locks[amenaza] = False

    async def _train_fallback(self, amenaza: str, conn: Any) -> dict[str, Any]:
        """Create a heuristic model when training data is insufficient."""
        metadata = {
            "amenaza": amenaza,
            "algoritmo": "Heuristic",
            "auc_roc": None,
            "auc_pr": None,
            "f1_score": None,
            "precision_score": None,
            "recall_score": None,
            "n_samples": 0,
            "n_positivos": 0,
            "n_negativos": 0,
            "ratio_imbalance": None,
            "features_usadas": FEATURES_POR_AMENAZA.get(amenaza, []),
            "features_elim_vif": None,
            "features_elim_rfe": None,
            "importancias_shap": None,
            "hiperparametros": None,
            "tecnica_balance": None,
            "entrenado_en": datetime.now(timezone.utc).isoformat(),
            "version": "Heuristic v9.0",
        }

        bundle = {"model": None, "metadata": metadata, "feature_names": FEATURES_POR_AMENAZA.get(amenaza, [])}
        model_path = self.model_dir / f"{amenaza}_model.pkl"
        with open(model_path, "wb") as f:
            pickle.dump(bundle, f)

        self.models[amenaza] = None
        self._metadata[amenaza] = metadata

        await self._save_metadata_db(amenaza, metadata, conn)
        logger.info("ML fallback heurístico para '%s' guardado", amenaza)
        return metadata

    async def _extract_features(
        self, amenaza: str, conn: Any
    ) -> tuple[np.ndarray, np.ndarray, list[str]]:
        """
        Extract features from the database for training.
        Uses distrito-level data as training points.
        """
        feature_names = FEATURES_POR_AMENAZA.get(amenaza, [])

        # Query distrito-level features
        rows = await conn.fetch("""
            SELECT
                d.id, d.nombre,
                d.nivel_riesgo,
                d.zona_sismica,
                COALESCE(d.peligro_sismico, 3) AS peligro_sismico,
                COALESCE(d.peligro_inundacion, 1) AS peligro_inundacion,
                COALESCE(d.peligro_deslizamiento, 1) AS peligro_deslizamiento,
                COALESCE(d.peligro_tsunami, 1) AS peligro_tsunami,
                COALESCE(d.peligro_volcan, 1) AS peligro_volcan,
                COALESCE(d.peligro_sequia, 1) AS peligro_sequia,
                COALESCE(d.factor_cascada, 1.0) AS factor_cascada,
                COALESCE(d.poblacion, 0) AS poblacion,
                COALESCE(d.area_km2, 0) AS area_km2,
                COALESCE(d.fallas_activas_50km, 0) AS fallas_activas_50km,
                ST_Y(ST_Centroid(d.geom)) AS lat,
                ST_X(ST_Centroid(d.geom)) AS lon
            FROM distritos d
            WHERE d.geom IS NOT NULL
        """)

        if not rows:
            return np.array([]), np.array([]), feature_names

        # Build feature matrix based on amenaza type
        X_list: list[list[float]] = []
        y_list: list[int] = []

        _target_field = {
            "deslizamiento": "peligro_deslizamiento",
            "inundacion": "peligro_inundacion",
            "sequia": "peligro_sequia",
        }[amenaza]

        for row in rows:
            lat = float(row["lat"]) if row["lat"] else -12.0
            lon = float(row["lon"]) if row["lon"] else -76.0

            features = self._build_feature_vector(
                amenaza=amenaza, lat=lat, lon=lon,
                zona_sismica=int(row["zona_sismica"] or 3),
                area_km2=float(row["area_km2"] or 100),
                peligro_sismico=float(row["peligro_sismico"]),
                peligro_inundacion=float(row["peligro_inundacion"]),
                peligro_deslizamiento=float(row["peligro_deslizamiento"]),
                peligro_sequia=float(row["peligro_sequia"]),
                fallas_activas_50km=float(row["fallas_activas_50km"]),
            )
            target = 1 if int(row[_target_field]) >= 3 else 0

            X_list.append(features)
            y_list.append(target)

        return np.array(X_list, dtype=np.float32), np.array(y_list, dtype=np.int32), feature_names

    async def predict_point(
        self, lon: float, lat: float, amenaza: str, conn: Any
    ) -> dict[str, Any]:
        """
        Predict susceptibility score for a single point.

        Returns dict with: score, score_p10, score_p90, nivel, features_usados,
        shap_values, modelo_info
        """
        if amenaza not in AMENAZAS_VALIDAS:
            raise ValueError(f"Amenaza inválida: {amenaza}")

        # Validate coordinates (Peru bounding box with margin)
        if not (-20.0 <= lat <= 1.0 and -82.0 <= lon <= -68.0):
            raise ValueError(
                f"Coordenadas fuera del rango de Perú: lat={lat}, lon={lon}"
            )

        # Try loading model if not in memory
        if amenaza not in self.models:
            self.load_model(amenaza)

        model = self.models.get(amenaza)
        feature_names = FEATURES_POR_AMENAZA.get(amenaza, [])

        # Get nearest distrito features for context
        distrito_row = await conn.fetchrow("""
            SELECT
                d.zona_sismica,
                COALESCE(d.peligro_sismico, 3) AS peligro_sismico,
                COALESCE(d.peligro_inundacion, 1) AS peligro_inundacion,
                COALESCE(d.peligro_deslizamiento, 1) AS peligro_deslizamiento,
                COALESCE(d.peligro_tsunami, 1) AS peligro_tsunami,
                COALESCE(d.peligro_volcan, 1) AS peligro_volcan,
                COALESCE(d.peligro_sequia, 1) AS peligro_sequia,
                COALESCE(d.factor_cascada, 1.0) AS factor_cascada,
                COALESCE(d.area_km2, 100) AS area_km2,
                COALESCE(d.fallas_activas_50km, 0) AS fallas_activas_50km,
                d.nombre
            FROM distritos d
            WHERE d.geom IS NOT NULL
            ORDER BY d.geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
            LIMIT 1
        """, lon, lat)

        zona = int(distrito_row["zona_sismica"] or 3) if distrito_row else 3
        area = float(distrito_row["area_km2"] or 100) if distrito_row else 100

        # Build feature vector using shared method (single source of truth)
        features = self._build_feature_vector(
            amenaza=amenaza, lat=lat, lon=lon,
            zona_sismica=zona, area_km2=area,
            peligro_sismico=float(distrito_row["peligro_sismico"]) if distrito_row else 3.0,
            peligro_inundacion=float(distrito_row["peligro_inundacion"]) if distrito_row else 1.0,
            peligro_deslizamiento=float(distrito_row["peligro_deslizamiento"]) if distrito_row else 1.0,
            peligro_sequia=float(distrito_row["peligro_sequia"]) if distrito_row else 1.0,
            fallas_activas_50km=float(distrito_row["fallas_activas_50km"]) if distrito_row else 0.0,
        )

        X = np.array([features], dtype=np.float32)

        # Predict with trained model or fall back to heuristic
        if model is not None and hasattr(model, "predict_proba"):
            try:
                proba = model.predict_proba(X)
                score = float(proba[0][1])

                # Bootstrap confidence interval with feature-scaled noise
                n_bootstrap = 50
                scores_boot: list[float] = []
                rng = np.random.default_rng()  # non-deterministic for real uncertainty
                # Scale noise per-feature (5% of feature magnitude) for meaningful CI
                feature_scales = np.abs(X[0]) * 0.05 + 1e-6
                for _ in range(n_bootstrap):
                    noise = rng.normal(0, 1, X.shape).astype(np.float32) * feature_scales
                    X_noisy = X + noise
                    p = model.predict_proba(X_noisy)
                    scores_boot.append(float(p[0][1]))

                score_p10 = float(np.percentile(scores_boot, 10))
                score_p90 = float(np.percentile(scores_boot, 90))

                # SHAP local explanations for this single prediction
                try:
                    import shap
                    explainer = shap.TreeExplainer(model)
                    shap_vals = explainer.shap_values(X)
                    importances = dict(
                        zip(feature_names, [round(float(v), 6) for v in shap_vals[0]])
                    )
                except Exception:
                    importances = dict(
                        zip(feature_names, [float(v) for v in model.feature_importances_])
                    )
            except Exception as exc:
                logger.warning("ML predict failed for '%s': %s — using heuristic", amenaza, exc)
                score, score_p10, score_p90, importances = self._heuristic_score(
                    amenaza, lon, lat, distrito_row, feature_names
                )
        else:
            score, score_p10, score_p90, importances = self._heuristic_score(
                amenaza, lon, lat, distrito_row, feature_names
            )

        nivel = _score_to_nivel(score)

        meta = self._metadata.get(amenaza, {})
        return {
            "lon": round(lon, 6),
            "lat": round(lat, 6),
            "amenaza": amenaza,
            "score": round(score, 4),
            "score_p10": round(score_p10, 4),
            "score_p90": round(score_p90, 4),
            "nivel": nivel,
            "ic_descripcion": f"IC 80%: [{score_p10:.2f}, {score_p90:.2f}]",
            "features_usados": dict(zip(feature_names, [round(float(v), 4) for v in features])),
            "shap_values": {k: round(v, 4) for k, v in sorted(importances.items(), key=lambda x: -abs(x[1]))[:5]},
            "modelo_info": {
                "algoritmo": meta.get("algoritmo", "Heuristic"),
                "auc_pr": meta.get("auc_pr"),
                "auc_roc": meta.get("auc_roc"),
                "entrenado_en": meta.get("entrenado_en"),
                "version": meta.get("version", "Heuristic v9.0"),
                "tecnica_balance": meta.get("tecnica_balance"),
            },
        }

    def _heuristic_score(
        self, amenaza: str, lon: float, lat: float,
        distrito_row: Any, feature_names: list[str],
    ) -> tuple[float, float, float, dict[str, float]]:
        """
        Compute a heuristic score based on district-level hazard data.
        Used when no trained model is available.
        """
        if distrito_row is None:
            return 0.5, 0.35, 0.65, {f: 0.0 for f in feature_names}

        if amenaza == "deslizamiento":
            base = float(distrito_row.get("peligro_deslizamiento", 1) or 1) / 5.0
            sismo_factor = float(distrito_row.get("peligro_sismico", 3) or 3) / 5.0
            cascada = float(distrito_row.get("factor_cascada", 1.0) or 1.0)
            score = min(1.0, base * 0.5 + sismo_factor * 0.25 + (cascada - 1.0) * 0.25)
        elif amenaza == "inundacion":
            base = float(distrito_row.get("peligro_inundacion", 1) or 1) / 5.0
            score = min(1.0, base * 0.7 + 0.15)
        else:  # sequia
            base = float(distrito_row.get("peligro_sequia", 1) or 1) / 5.0
            score = min(1.0, base * 0.7 + 0.1)

        score = max(0.01, min(0.99, score))
        margin = min(0.15, score * 0.3)
        score_p10 = max(0.0, score - margin)
        score_p90 = min(1.0, score + margin)

        # Distribute importance heuristically
        n = len(feature_names)
        weights = np.linspace(0.3, 0.05, n) if n > 0 else []
        importances = {name: round(float(w), 4) for name, w in zip(feature_names, weights)}

        return round(score, 4), round(score_p10, 4), round(score_p90, 4), importances

    def get_model_info(self, amenaza: Optional[str] = None) -> dict[str, Any]:
        if amenaza is None:
            result = {}
            for am in AMENAZAS_VALIDAS:
                meta = self._metadata.get(am, {})
                result[am] = {
                    "amenaza": am,
                    "algoritmo": meta.get("algoritmo", "no entrenado"),
                    "auc_roc": meta.get("auc_roc"),
                    "entrenado_en": meta.get("entrenado_en"),
                    "version": meta.get("version"),
                    "entrenado": self.is_trained(am),
                }
            return result

        if amenaza not in AMENAZAS_VALIDAS:
            return {"amenaza": amenaza, "entrenado": False}

        meta = self._metadata.get(amenaza, {})
        return {
            "amenaza": amenaza,
            "algoritmo": meta.get("algoritmo", "no entrenado"),
            "auc_roc": meta.get("auc_roc"),
            "auc_pr": meta.get("auc_pr"),
            "f1_score": meta.get("f1_score"),
            "precision_score": meta.get("precision_score"),
            "recall_score": meta.get("recall_score"),
            "n_samples": meta.get("n_samples"),
            "n_positivos": meta.get("n_positivos"),
            "n_negativos": meta.get("n_negativos"),
            "ratio_imbalance": meta.get("ratio_imbalance"),
            "features_usadas": meta.get("features_usadas"),
            "importancias_shap": meta.get("importancias_shap"),
            "hiperparametros": meta.get("hiperparametros"),
            "tecnica_balance": meta.get("tecnica_balance"),
            "entrenado_en": meta.get("entrenado_en"),
            "version": meta.get("version"),
            "entrenado": self.is_trained(amenaza),
        }

    async def _save_metadata_db(
        self, amenaza: str, metadata: dict[str, Any], conn: Any
    ) -> None:
        """Persist model metadata to the modelo_metadata table."""
        try:
            await conn.execute("""
                INSERT INTO modelo_metadata (amenaza, algoritmo, auc_roc, auc_pr,
                    f1_score, precision_score, recall_score,
                    n_samples, n_positivos, n_negativos, ratio_imbalance,
                    features_usadas, importancias_shap, hiperparametros,
                    tecnica_balance, entrenado_en, version)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                        $12::TEXT[], $13::JSONB, $14::JSONB, $15, $16::TIMESTAMPTZ, $17)
                ON CONFLICT (amenaza) DO UPDATE SET
                    algoritmo = EXCLUDED.algoritmo,
                    auc_roc = EXCLUDED.auc_roc,
                    auc_pr = EXCLUDED.auc_pr,
                    f1_score = EXCLUDED.f1_score,
                    precision_score = EXCLUDED.precision_score,
                    recall_score = EXCLUDED.recall_score,
                    n_samples = EXCLUDED.n_samples,
                    n_positivos = EXCLUDED.n_positivos,
                    n_negativos = EXCLUDED.n_negativos,
                    ratio_imbalance = EXCLUDED.ratio_imbalance,
                    features_usadas = EXCLUDED.features_usadas,
                    importancias_shap = EXCLUDED.importancias_shap,
                    hiperparametros = EXCLUDED.hiperparametros,
                    tecnica_balance = EXCLUDED.tecnica_balance,
                    entrenado_en = EXCLUDED.entrenado_en,
                    version = EXCLUDED.version
            """,
                amenaza,
                metadata.get("algoritmo"),
                metadata.get("auc_roc"),
                metadata.get("auc_pr"),
                metadata.get("f1_score"),
                metadata.get("precision_score"),
                metadata.get("recall_score"),
                metadata.get("n_samples"),
                metadata.get("n_positivos"),
                metadata.get("n_negativos"),
                metadata.get("ratio_imbalance"),
                metadata.get("features_usadas"),
                json.dumps(metadata.get("importancias_shap")) if metadata.get("importancias_shap") else None,
                json.dumps(metadata.get("hiperparametros")) if metadata.get("hiperparametros") else None,
                metadata.get("tecnica_balance"),
                metadata.get("entrenado_en"),
                metadata.get("version"),
            )
        except Exception as exc:
            logger.warning("No se pudo guardar metadata ML en BD: %s", exc)


susceptibility_model = SusceptibilityModel(model_dir=MODELO_DIR)
logger.info("ML Engine v9.0 inicializado: %s", ", ".join(AMENAZAS_VALIDAS))
