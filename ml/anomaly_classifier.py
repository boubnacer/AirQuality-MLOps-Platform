import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import accuracy_score, f1_score, classification_report, confusion_matrix
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from loguru import logger

from ml.utils import generate_labels, log_run, get_production_metrics, promote_model

EXPERIMENT = "anomaly-classification"
REGISTRY_NAME = "air-quality-anomaly-classifier"

FEATURE_COLS = [
    "pm2_5", "pm10", "no2", "o3", "co", "so2",
    "pm2_5_mean_5", "pm2_5_std_5", "pm2_5_delta",
    "pm10_mean_5", "no2_mean_5",
    "hour", "day_of_week", "is_weekend", "zone_enc",
]


def _get_xy(df: pd.DataFrame):
    available = [c for c in FEATURE_COLS if c in df.columns]
    X = df[available].fillna(0)
    y = generate_labels(df)
    return X, y


def train_logistic_regression(df: pd.DataFrame) -> str:
    X, y = _get_xy(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    params = {"C": 1.0, "max_iter": 500, "solver": "lbfgs"}
    model = Pipeline([("scaler", StandardScaler()), ("clf", LogisticRegression(**params))])
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, preds),
        "f1": f1_score(y_test, preds, zero_division=0),
        "test_size": len(y_test),
        "anomaly_rate": float(y.mean()),
    }
    logger.info(f"LogReg — accuracy={metrics['accuracy']:.3f} f1={metrics['f1']:.3f}")
    cm_path = _save_confusion_matrix(y_test, preds, "logreg_cm.png")
    return log_run(EXPERIMENT, "LogisticRegression", params, metrics, model, {"cm": cm_path})


def train_random_forest(df: pd.DataFrame) -> str:
    X, y = _get_xy(df)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    params = {"n_estimators": 100, "max_depth": 10, "min_samples_split": 5, "random_state": 42}
    model = RandomForestClassifier(**params)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, preds),
        "f1": f1_score(y_test, preds, zero_division=0),
        "test_size": len(y_test),
        "anomaly_rate": float(y.mean()),
    }
    logger.info(f"RandomForest — accuracy={metrics['accuracy']:.3f} f1={metrics['f1']:.3f}")
    fi_path = _save_feature_importance(model, [c for c in FEATURE_COLS if c in X.columns], "rf_feature_importance.png")
    cm_path = _save_confusion_matrix(y_test, preds, "rf_cm.png")
    return log_run(EXPERIMENT, "RandomForest", params, metrics, model, {"fi": fi_path, "cm": cm_path})


def train_isolation_forest(df: pd.DataFrame) -> str:
    X, _ = _get_xy(df)
    params = {"n_estimators": 100, "contamination": 0.05, "random_state": 42}
    model = IsolationForest(**params)
    model.fit(X)
    raw_preds = model.predict(X)
    preds = (raw_preds == -1).astype(int)
    y = generate_labels(df)
    metrics = {
        "f1": f1_score(y, preds, zero_division=0),
        "anomaly_rate_predicted": float(preds.mean()),
        "anomaly_rate_actual": float(y.mean()),
    }
    logger.info(f"IsolationForest — f1={metrics['f1']:.3f}")
    return log_run(EXPERIMENT, "IsolationForest", params, metrics, model)


def train_best(df: pd.DataFrame) -> str:
    run_lr = train_logistic_regression(df)
    run_rf = train_random_forest(df)
    run_if = train_isolation_forest(df)

    import mlflow
    client = mlflow.tracking.MlflowClient()
    best_run_id, best_f1 = None, -1
    for run_id in [run_lr, run_rf, run_if]:
        run = client.get_run(run_id)
        f1 = run.data.metrics.get("f1", 0)
        if f1 > best_f1:
            best_f1, best_run_id = f1, run_id

    prod_metrics = get_production_metrics(REGISTRY_NAME)
    if best_f1 > prod_metrics.get("f1", 0):
        version = promote_model(REGISTRY_NAME, best_run_id, "Staging")
        logger.info(f"Promoted run {best_run_id} to Staging (f1={best_f1:.3f})")
        return best_run_id, True
    logger.info(f"No improvement over production (prod f1={prod_metrics.get('f1', 0):.3f})")
    return best_run_id, False


def _save_confusion_matrix(y_true, y_pred, filename: str) -> str:
    import tempfile, os
    cm = confusion_matrix(y_true, y_pred)
    fig, ax = plt.subplots()
    im = ax.imshow(cm, cmap="Blues")
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.set_title("Confusion Matrix")
    plt.colorbar(im)
    path = os.path.join(tempfile.gettempdir(), filename)
    fig.savefig(path)
    plt.close(fig)
    return path


def _save_feature_importance(model, feature_names: list, filename: str) -> str:
    import tempfile, os
    importances = model.feature_importances_
    idx = np.argsort(importances)[::-1][:15]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh([feature_names[i] for i in idx][::-1], importances[idx][::-1])
    ax.set_title("Feature Importances")
    ax.set_xlabel("Importance")
    path = os.path.join(tempfile.gettempdir(), filename)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path
