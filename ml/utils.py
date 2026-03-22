import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from loguru import logger

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

ANOMALY_THRESHOLDS = {
    "pm2_5": {"warning": 25, "critical": 75},
    "pm10":  {"warning": 50, "critical": 150},
    "no2":   {"warning": 100, "critical": 200},
    "o3":    {"warning": 120, "critical": 180},
    "co":    {"warning": 4, "critical": 10},
    "so2":   {"warning": 125, "critical": 350},
}


def generate_labels(df: pd.DataFrame) -> pd.Series:
    labels = pd.Series(0, index=df.index)
    for pollutant, thresholds in ANOMALY_THRESHOLDS.items():
        if pollutant in df.columns:
            labels = labels | (df[pollutant] > thresholds["critical"]).astype(int)
    return labels


def log_run(experiment_name: str, model_name: str, params: dict, metrics: dict, model, artifacts: dict = None):
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run(run_name=model_name) as run:
        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "model", registered_model_name=None)
        if artifacts:
            for path in artifacts.values():
                if path and os.path.exists(path):
                    mlflow.log_artifact(path)
        logger.info(f"MLflow run {run.info.run_id} logged under '{experiment_name}'")
        return run.info.run_id


def get_production_metrics(model_registry_name: str) -> dict:
    client = mlflow.tracking.MlflowClient()
    try:
        versions = client.get_latest_versions(model_registry_name, stages=["Production"])
        if not versions:
            return {}
        run_id = versions[0].run_id
        run = client.get_run(run_id)
        return run.data.metrics
    except Exception as e:
        logger.warning(f"Could not fetch production metrics for {model_registry_name}: {e}")
        return {}


def promote_model(model_registry_name: str, run_id: str, stage: str = "Staging"):
    client = mlflow.tracking.MlflowClient()
    try:
        result = mlflow.register_model(f"runs:/{run_id}/model", model_registry_name)
        client.transition_model_version_stage(
            name=model_registry_name,
            version=result.version,
            stage=stage,
        )
        logger.info(f"Model '{model_registry_name}' v{result.version} promoted to {stage}")
        return result.version
    except Exception as e:
        logger.error(f"promote_model failed: {e}")
        raise