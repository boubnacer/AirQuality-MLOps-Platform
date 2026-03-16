import os
import pandas as pd
import numpy as np
import psycopg2
from loguru import logger

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB", "airquality"),
    "user":     os.getenv("POSTGRES_USER", "airquality"),
    "password": os.getenv("POSTGRES_PASSWORD", "airquality123"),
}

POLLUTANTS = ["pm2_5", "pm10", "no2", "o3", "co", "so2"]


def load_measurements(days: int = 30) -> pd.DataFrame:
    sql = f"""
        SELECT m.sensor_id, m.timestamp, m.pm2_5, m.pm10, m.no2, m.o3, m.co, m.so2,
               m.temperature, m.humidity, s.city, s.zone
        FROM measurements m
        JOIN sensors s ON m.sensor_id = s.sensor_id
        WHERE m.timestamp >= NOW() - INTERVAL '{days} days'
        ORDER BY m.sensor_id, m.timestamp
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(sql, conn, parse_dates=["timestamp"])
    logger.info(f"Loaded {len(df)} measurements from last {days} days")
    return df


def add_rolling_features(df: pd.DataFrame, windows: list = [5, 15, 60]) -> pd.DataFrame:
    df = df.sort_values(["sensor_id", "timestamp"]).copy()
    for pollutant in POLLUTANTS:
        if pollutant not in df.columns:
            continue
        for w in windows:
            grp = df.groupby("sensor_id")[pollutant]
            df[f"{pollutant}_mean_{w}"] = grp.transform(lambda x: x.rolling(w, min_periods=1).mean())
            df[f"{pollutant}_std_{w}"]  = grp.transform(lambda x: x.rolling(w, min_periods=1).std().fillna(0))
    return df


def add_delta_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["sensor_id", "timestamp"]).copy()
    for pollutant in POLLUTANTS:
        if pollutant not in df.columns:
            continue
        df[f"{pollutant}_delta"] = df.groupby("sensor_id")[pollutant].diff().fillna(0)
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df["hour"]        = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"]  = (df["day_of_week"] >= 5).astype(int)
    df["month"]       = df["timestamp"].dt.month
    return df


def add_zone_encoding(df: pd.DataFrame) -> pd.DataFrame:
    zone_map = {"urban_background": 0, "traffic": 1, "industrial": 2, "residential": 3}
    df["zone_enc"] = df["zone"].map(zone_map).fillna(0).astype(int)
    return df


def build_feature_matrix(days: int = 30) -> pd.DataFrame:
    df = load_measurements(days)
    df = add_rolling_features(df)
    df = add_delta_features(df)
    df = add_time_features(df)
    df = add_zone_encoding(df)
    df = df.dropna(subset=POLLUTANTS)
    logger.info(f"Feature matrix shape: {df.shape}")
    return df


def build_forecast_dataset(df: pd.DataFrame, target: str = "pm2_5", horizon: int = 1) -> tuple:
    df = df.sort_values(["sensor_id", "timestamp"]).copy()
    df["target"] = df.groupby("sensor_id")[target].shift(-horizon)
    df = df.dropna(subset=["target"])
    feature_cols = [c for c in df.columns if c not in ["sensor_id", "timestamp", "city", "zone", "target"]]
    return df[feature_cols].fillna(0), df["target"]


def build_cluster_dataset(days: int = 90) -> pd.DataFrame:
    sql = f"""
        SELECT sensor_id,
               AVG(pm2_5) AS pm2_5_avg, MAX(pm2_5) AS pm2_5_max,
               AVG(pm10)  AS pm10_avg,  AVG(no2)   AS no2_avg,
               AVG(o3)    AS o3_avg,    AVG(co)     AS co_avg,
               AVG(so2)   AS so2_avg,
               STDDEV(pm2_5) AS pm2_5_std,
               COUNT(*)      AS record_count
        FROM measurements
        WHERE timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY sensor_id
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(sql, conn)
    df = df.set_index("sensor_id").fillna(0)
    return df