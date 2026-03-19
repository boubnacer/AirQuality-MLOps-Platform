import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import xgboost as xgb
from loguru import logger

from ml.utils import log_run, get_production_metrics, promote_model
from ml.data.feature_engineering import build_forecast_dataset

EXPERIMENT = "pollution-forecasting"
REGISTRY_NAME = "air-quality-pollution-forecaster"

FEATURE_COLS = [
    "pm2_5", "pm10", "no2", "o3", "co", "so2",
    "pm2_5_mean_5", "pm2_5_std_5", "pm2_5_delta",
    "pm2_5_mean_15", "pm2_5_std_15",
    "hour", "day_of_week", "is_weekend", "zone_enc",
    "temperature", "humidity",
]


def _get_xy(df: pd.DataFrame, target: str = "pm2_5"):
    X, y = build_forecast_dataset(df, target=target, horizon=1)
    available = [c for c in FEATURE_COLS if c in X.columns]
    return X[available].fillna(0), y


def train_ridge(df: pd.DataFrame, target: str = "pm2_5") -> str:
    X, y = _get_xy(df, target)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    params = {"alpha": 1.0}
    model = Pipeline([("scaler", StandardScaler()), ("reg", Ridge(**params))])
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    metrics = {
        "rmse": float(np.sqrt(mean_squared_error(y_test, preds))),
        "mae": float(mean_absolute_error(y_test, preds)),
        "r2": float(r2_score(y_test, preds)),
    }
    params["target"] = target
    logger.info(f"Ridge [{target}] — rmse={metrics['rmse']:.3f} mae={metrics['mae']:.3f} r2={metrics['r2']:.3f}")
    res_path = _save_residuals(y_test, preds, f"ridge_residuals_{target}.png")
    return log_run(EXPERIMENT, f"Ridge_{target}", params, metrics, model, {"residuals": res_path})


def train_xgboost(df: pd.DataFrame, target: str = "pm2_5") -> str:
    X, y = _get_xy(df, target)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    params = {
        "n_estimators": 200,
        "learning_rate": 0.05,
        "max_depth": 6,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "random_state": 42,
    }
    model = xgb.XGBRegressor(**params, verbosity=0)
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)
    preds = model.predict(X_test)
    metrics = {
        "rmse": float(np.sqrt(mean_squared_error(y_test, preds))),
        "mae": float(mean_absolute_error(y_test, preds)),
        "r2": float(r2_score(y_test, preds)),
    }
    params["target"] = target
    logger.info(f"XGBoost [{target}] — rmse={metrics['rmse']:.3f} mae={metrics['mae']:.3f} r2={metrics['r2']:.3f}")
    fi_path = _save_xgb_importance(model, X.columns.tolist(), f"xgb_importance_{target}.png")
    res_path = _save_residuals(y_test, preds, f"xgb_residuals_{target}.png")
    return log_run(EXPERIMENT, f"XGBoost_{target}", params, metrics, model, {"fi": fi_path, "residuals": res_path})


def train_best(df: pd.DataFrame) -> str:
    run_ids = []
    for target in ["pm2_5", "no2"]:
        run_ids.append(train_ridge(df, target))
        run_ids.append(train_xgboost(df, target))

    import mlflow
    client = mlflow.tracking.MlflowClient()
    best_run_id, best_rmse = None, float("inf")
    for run_id in run_ids:
        run = client.get_run(run_id)
        rmse = run.data.metrics.get("rmse", float("inf"))
        if rmse < best_rmse:
            best_rmse, best_run_id = rmse, run_id

    prod_metrics = get_production_metrics(REGISTRY_NAME)
    prod_rmse = prod_metrics.get("rmse", float("inf"))
    if best_rmse < prod_rmse:
        promote_model(REGISTRY_NAME, best_run_id, "Staging")
        logger.info(f"Promoted run {best_run_id} to Staging (rmse={best_rmse:.3f})")
        return best_run_id, True
    logger.info(f"No improvement over production (prod rmse={prod_rmse:.3f})")
    return best_run_id, False


def _save_residuals(y_true, y_pred, filename: str) -> str:
    import tempfile, os
    residuals = np.array(y_true) - np.array(y_pred)
    fig, ax = plt.subplots()
    ax.scatter(y_pred, residuals, alpha=0.3, s=5)
    ax.axhline(0, color="red", linestyle="--")
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Residuals")
    ax.set_title("Residual Plot")
    path = os.path.join(tempfile.gettempdir(), filename)
    fig.savefig(path)
    plt.close(fig)
    return path


def _save_xgb_importance(model, feature_names: list, filename: str) -> str:
    import tempfile, os
    importances = model.feature_importances_
    idx = np.argsort(importances)[::-1][:15]
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh([feature_names[i] for i in idx][::-1], importances[idx][::-1])
    ax.set_title("XGBoost Feature Importances")
    path = os.path.join(tempfile.gettempdir(), filename)
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path
