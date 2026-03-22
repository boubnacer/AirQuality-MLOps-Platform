import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from loguru import logger

from ml.utils import log_run
from ml.data.feature_engineering import build_cluster_dataset

EXPERIMENT = "sensor-clustering"
REGISTRY_NAME = "air-quality-sensor-clustering"

FEATURE_COLS = ["pm2_5_avg", "pm2_5_max", "pm10_avg", "no2_avg", "o3_avg", "co_avg", "so2_avg", "pm2_5_std"]


def _get_X(df: pd.DataFrame) -> np.ndarray:
    available = [c for c in FEATURE_COLS if c in df.columns]
    scaler = StandardScaler()
    return scaler.fit_transform(df[available].fillna(0)), scaler


def train_kmeans(df: pd.DataFrame, n_clusters: int = 4) -> str:
    X_scaled, scaler = _get_X(df)
    params = {"n_clusters": n_clusters, "random_state": 42, "n_init": 10}
    model = KMeans(**params)
    labels = model.fit_predict(X_scaled)
    sil = silhouette_score(X_scaled, labels) if len(set(labels)) > 1 else 0.0
    inertia = float(model.inertia_)
    metrics = {"silhouette_score": float(sil), "inertia": inertia, "n_clusters": n_clusters}
    logger.info(f"KMeans — silhouette={sil:.3f} inertia={inertia:.1f}")

    cluster_df = df.copy()
    cluster_df["cluster"] = labels
    means = cluster_df.groupby("cluster")[[c for c in FEATURE_COLS if c in df.columns]].mean()
    logger.info(f"Cluster means:\n{means.to_string()}")

    vis_path = _save_cluster_plot(X_scaled, labels, "kmeans_clusters.png")
    return log_run(EXPERIMENT, "KMeans", params, metrics, model, {"vis": vis_path})


def train_dbscan(df: pd.DataFrame, eps: float = 0.8, min_samples: int = 3) -> str:
    X_scaled, scaler = _get_X(df)
    params = {"eps": eps, "min_samples": min_samples}
    model = DBSCAN(**params)
    labels = model.fit_predict(X_scaled)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = int((labels == -1).sum())
    sil = silhouette_score(X_scaled, labels) if n_clusters > 1 else 0.0
    metrics = {
        "silhouette_score": float(sil),
        "n_clusters_found": n_clusters,
        "n_noise_points": n_noise,
    }
    logger.info(f"DBSCAN — clusters={n_clusters} noise={n_noise} silhouette={sil:.3f}")
    vis_path = _save_cluster_plot(X_scaled, labels, "dbscan_clusters.png")
    return log_run(EXPERIMENT, "DBSCAN", params, metrics, model, {"vis": vis_path})


def train_best(days: int = 90) -> str:
    df = build_cluster_dataset(days=days)
    if len(df) < 5:
        logger.warning("Not enough sensor data for clustering")
        return None, False
    run_km = train_kmeans(df)
    run_db = train_dbscan(df)

    import mlflow
    client = mlflow.tracking.MlflowClient()
    best_run_id, best_sil = None, -1
    for run_id in [run_km, run_db]:
        run = client.get_run(run_id)
        sil = run.data.metrics.get("silhouette_score", -1)
        if sil > best_sil:
            best_sil, best_run_id = sil, run_id

    from ml.utils import promote_model, get_production_metrics
    prod_metrics = get_production_metrics(REGISTRY_NAME)
    if best_sil > prod_metrics.get("silhouette_score", -1):
        promote_model(REGISTRY_NAME, best_run_id, "Staging")
        return best_run_id, True
    return best_run_id, False


def _save_cluster_plot(X_scaled: np.ndarray, labels: np.ndarray, filename: str) -> str:
    import tempfile, os
    from sklearn.decomposition import PCA
    pca = PCA(n_components=2)
    coords = pca.fit_transform(X_scaled)
    fig, ax = plt.subplots(figsize=(8, 6))
    scatter = ax.scatter(coords[:, 0], coords[:, 1], c=labels, cmap="tab10", alpha=0.7)
    plt.colorbar(scatter, ax=ax, label="Cluster")
    ax.set_title("Sensor Clusters (PCA projection)")
    ax.set_xlabel("PC1")
    ax.set_ylabel("PC2")
    path = os.path.join(tempfile.gettempdir(), filename)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path
