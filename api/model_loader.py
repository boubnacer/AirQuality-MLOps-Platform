import os
import mlflow
import mlflow.pyfunc
from loguru import logger

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

CLASSIFIER_NAME = "air-quality-anomaly-classifier"
FORECASTER_NAME = "air-quality-pollution-forecaster"


class ModelStore:
    def __init__(self):
        self.classifier = None
        self.classifier_info = {}
        self.forecaster = None
        self.forecaster_info = {}

    def load(self):
        self.classifier, self.classifier_info = self._load_model(CLASSIFIER_NAME)
        self.forecaster, self.forecaster_info = self._load_model(FORECASTER_NAME)

    def _load_model(self, registry_name: str):
        client = mlflow.tracking.MlflowClient()
        try:
            versions = client.get_latest_versions(registry_name, stages=["Production"])
            if not versions:
                logger.warning(f"No Production model for '{registry_name}', trying Staging...")
                versions = client.get_latest_versions(registry_name, stages=["Staging"])
            if not versions:
                logger.warning(f"No model found for '{registry_name}'")
                return None, {"name": registry_name, "version": "none", "stage": "none"}

            v = versions[0]
            model_uri = f"models:/{registry_name}/{v.version}"
            model = mlflow.pyfunc.load_model(model_uri)
            info = {"name": registry_name, "version": v.version, "stage": v.current_stage}
            logger.info(f"Loaded {registry_name} v{v.version} ({v.current_stage})")
            return model, info
        except Exception as e:
            logger.error(f"Failed to load {registry_name}: {e}")
            return None, {"name": registry_name, "version": "error", "stage": "error"}


model_store = ModelStore()