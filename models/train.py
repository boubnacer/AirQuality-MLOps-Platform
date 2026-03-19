import sys
import os
from loguru import logger

sys.path.insert(0, "/opt/airflow")

from ml.data.feature_engineering import build_feature_matrix
from ml.models.anomaly_classifier import train_best as train_classifier
from ml.models.pollution_forecaster import train_best as train_forecaster
from ml.models.sensor_clustering import train_best as train_clustering


def run(task: str = "all", days: int = 30) -> dict:
    results = {}

    if task in ("all", "classification"):
        logger.info("=== Training anomaly classifier ===")
        df = build_feature_matrix(days=days)
        if len(df) > 100:
            run_id, improved = train_classifier(df)
            results["classification"] = {"run_id": run_id, "improved": improved}
        else:
            logger.warning("Not enough data for classification")
            results["classification"] = {"run_id": None, "improved": False}

    if task in ("all", "forecasting"):
        logger.info("=== Training pollution forecaster ===")
        df = build_feature_matrix(days=days)
        if len(df) > 100:
            run_id, improved = train_forecaster(df)
            results["forecasting"] = {"run_id": run_id, "improved": improved}
        else:
            logger.warning("Not enough data for forecasting")
            results["forecasting"] = {"run_id": None, "improved": False}

    if task in ("all", "clustering"):
        logger.info("=== Training sensor clustering ===")
        run_id, improved = train_clustering(days=max(days, 7))
        results["clustering"] = {"run_id": run_id, "improved": improved}

    logger.info(f"Training results: {results}")
    return results


if __name__ == "__main__":
    task = sys.argv[1] if len(sys.argv) > 1 else "all"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
    run(task=task, days=days)
