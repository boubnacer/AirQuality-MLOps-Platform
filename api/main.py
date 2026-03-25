import os
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, HTTPException
from loguru import logger

from api.schemas import PredictRequest, PredictResponse, HealthResponse
from api.model_loader import model_store

app = FastAPI(title="Air Quality Prediction API", version="1.0.0")

CLASSIFIER_FEATURE_COLS = [
    "pm2_5", "pm10", "no2", "o3", "co", "so2",
    "pm2_5_mean_5", "pm2_5_std_5", "pm2_5_delta",
    "pm10_mean_5", "no2_mean_5",
    "hour", "day_of_week", "is_weekend", "zone_enc",
]

FORECASTER_FEATURE_COLS = [
    "pm2_5", "pm10", "no2", "o3", "co", "so2",
    "pm2_5_mean_5", "pm2_5_std_5", "pm2_5_delta",
    "pm2_5_mean_15", "pm2_5_std_15",
    "hour", "day_of_week", "is_weekend", "zone_enc",
    "temperature", "humidity",
]

ZONE_MAP = {"urban_background": 0, "traffic": 1, "industrial": 2, "residential": 3}


@app.on_event("startup")
async def startup():
    import asyncio
    logger.info("Loading models from MLflow (background)...")
    asyncio.create_task(asyncio.to_thread(model_store.load))


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(
        status="ok",
        classifier_name=model_store.classifier_info.get("name", ""),
        classifier_version=str(model_store.classifier_info.get("version", "")),
        classifier_stage=model_store.classifier_info.get("stage", ""),
        forecaster_name=model_store.forecaster_info.get("name", ""),
        forecaster_version=str(model_store.forecaster_info.get("version", "")),
        forecaster_stage=model_store.forecaster_info.get("stage", ""),
    )


@app.post("/predict", response_model=PredictResponse)
def predict(request: PredictRequest):
    m = request.measurements
    ts = request.timestamp

    row = {
        "pm2_5": m.pm2_5 or 0,
        "pm10": m.pm10 or 0,
        "no2": m.no2 or 0,
        "o3": m.o3 or 0,
        "co": m.co or 0,
        "so2": m.so2 or 0,
        "temperature": m.temperature or 20,
        "humidity": m.humidity or 50,
        "pm2_5_mean_5": m.pm2_5 or 0,
        "pm2_5_std_5": 0,
        "pm2_5_delta": 0,
        "pm2_5_mean_15": m.pm2_5 or 0,
        "pm2_5_std_15": 0,
        "pm10_mean_5": m.pm10 or 0,
        "no2_mean_5": m.no2 or 0,
        "hour": ts.hour,
        "day_of_week": ts.weekday(),
        "is_weekend": int(ts.weekday() >= 5),
        "zone_enc": ZONE_MAP.get(request.location.zone, 0),
    }

    df = pd.DataFrame([row])
    X_clf = df[[c for c in CLASSIFIER_FEATURE_COLS if c in df.columns]].fillna(0)
    X_fct = df[[c for c in FORECASTER_FEATURE_COLS if c in df.columns]].fillna(0)

    anomaly_prob = None
    is_anomaly = None
    pm25_forecast = None
    no2_forecast = None
    model_name = "none"
    model_version = "none"

    if model_store.classifier is not None:
        try:
            raw = model_store.classifier.predict(X_clf)
            prob = float(raw[0])
            anomaly_prob = prob if 0 <= prob <= 1 else float(prob > 0.5)
            is_anomaly = anomaly_prob > 0.5
            model_name = model_store.classifier_info.get("name", "")
            model_version = str(model_store.classifier_info.get("version", ""))
        except Exception as e:
            logger.error(f"Classifier prediction failed: {e}")

    if model_store.forecaster is not None:
        try:
            preds = model_store.forecaster.predict(X_fct)
            pm25_forecast = float(preds[0])
            no2_forecast = float(preds[0])
            if not model_name or model_name == "none":
                model_name = model_store.forecaster_info.get("name", "")
                model_version = str(model_store.forecaster_info.get("version", ""))
        except Exception as e:
            logger.error(f"Forecaster prediction failed: {e}")

    return PredictResponse(
        sensor_id=request.sensor_id,
        timestamp=request.timestamp,
        anomaly_probability=anomaly_prob,
        is_anomaly=is_anomaly,
        pm2_5_forecast=pm25_forecast,
        no2_forecast=no2_forecast,
        model_name=model_name,
        model_version=model_version,
    )


@app.post("/reload")
def reload_models():
    model_store.load()
    return {"status": "reloaded"}
