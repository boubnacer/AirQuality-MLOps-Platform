import os
import mlflow
from loguru import logger

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

MODEL_CONFIGS = [
    {
        "registry_name": "air-quality-anomaly-classifier",
        "primary_metric": "f1",
        "higher_is_better": True,
    },
    {
        "registry_name": "air-quality-pollution-forecaster",
        "primary_metric": "rmse",
        "higher_is_better": False,
    },
    {
        "registry_name": "air-quality-sensor-clustering",
        "primary_metric": "silhouette_score",
        "higher_is_better": True,
    },
]


def get_metrics_by_stage(registry_name: str, stage: str) -> dict:
    client = mlflow.tracking.MlflowClient()
    try:
        versions = client.get_latest_versions(registry_name, stages=[stage])
        if not versions:
            return {}
        run = client.get_run(versions[0].run_id)
        return {"metrics": run.data.metrics, "version": versions[0].version, "run_id": versions[0].run_id}
    except Exception as e:
        logger.warning(f"get_metrics_by_stage({registry_name}, {stage}): {e}")
        return {}


def compare_and_promote(registry_name: str, primary_metric: str, higher_is_better: bool) -> dict:
    client = mlflow.tracking.MlflowClient()
    staging = get_metrics_by_stage(registry_name, "Staging")
    production = get_metrics_by_stage(registry_name, "Production")

    result = {"registry_name": registry_name, "action": "none"}

    if not staging:
        logger.info(f"{registry_name}: no staging model found")
        return result

    staging_val = staging["metrics"].get(primary_metric)
    if staging_val is None:
        logger.warning(f"{registry_name}: staging model missing metric '{primary_metric}'")
        return result

    if not production:
        logger.info(f"{registry_name}: no production model, promoting staging v{staging['version']}")
        _promote_to_production(client, registry_name, staging["version"])
        result["action"] = "promoted_first"
        return result

    prod_val = production["metrics"].get(primary_metric, None)
    if prod_val is None:
        _promote_to_production(client, registry_name, staging["version"])
        result["action"] = "promoted_first"
        return result

    challenger_wins = staging_val > prod_val if higher_is_better else staging_val < prod_val
    logger.info(
        f"{registry_name}: staging {primary_metric}={staging_val:.4f} vs "
        f"production {primary_metric}={prod_val:.4f} — "
        f"{'challenger wins' if challenger_wins else 'champion retained'}"
    )

    if challenger_wins:
        _archive_production(client, registry_name, production["version"])
        _promote_to_production(client, registry_name, staging["version"])
        result["action"] = "promoted_challenger"
        result["improvement"] = staging_val - prod_val if higher_is_better else prod_val - staging_val
    else:
        client.transition_model_version_stage(registry_name, staging["version"], "Archived")
        result["action"] = "retained_champion"

    return result


def _promote_to_production(client, registry_name: str, version: str):
    client.transition_model_version_stage(registry_name, version, "Production")
    logger.info(f"Promoted {registry_name} v{version} to Production")


def _archive_production(client, registry_name: str, version: str):
    client.transition_model_version_stage(registry_name, version, "Archived")
    logger.info(f"Archived {registry_name} v{version}")


def run_evaluation() -> list:
    results = []
    for config in MODEL_CONFIGS:
        result = compare_and_promote(
            config["registry_name"],
            config["primary_metric"],
            config["higher_is_better"],
        )
        results.append(result)
        logger.info(f"Evaluation result: {result}")
    return results


if __name__ == "__main__":
    run_evaluation()
