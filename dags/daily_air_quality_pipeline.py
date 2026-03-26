import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, "/opt/airflow")

from dags.sensors.new_data_sensor import NewDataSensor
from dags.sensors.streaming_done_sensor import StreamingDoneSensor
from dags.operators.mlflow_train_operator import MLflowTrainOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
    "email": os.getenv("ALERT_EMAIL", ""),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=3),
}

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")


def _send_slack(context):
    if not SLACK_WEBHOOK:
        return
    import requests
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["run_id"]
    msg = f":red_circle: DAG `{dag_id}` task `{task_id}` failed (run: {run_id})"
    try:
        requests.post(SLACK_WEBHOOK, json={"text": msg}, timeout=5)
    except Exception:
        pass


def extract_sensor_data(**context):
    import psycopg2
    import psycopg2.extras

    run_date = context["ds"]
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "airquality"),
        user=os.getenv("POSTGRES_USER", "airquality"),
        password=os.getenv("POSTGRES_PASSWORD", "airquality123"),
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM measurements WHERE timestamp::date = %s", (run_date,)
        )
        count = cur.fetchone()[0]
    conn.close()
    context["task_instance"].xcom_push(key="record_count", value=count)
    return count


def validate_data_quality(**context):
    import psycopg2

    run_date = context["ds"]
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "airquality"),
        user=os.getenv("POSTGRES_USER", "airquality"),
        password=os.getenv("POSTGRES_PASSWORD", "airquality123"),
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE pm2_5 IS NULL) AS null_pm25,
                COUNT(*) FILTER (WHERE pm2_5 < 0 OR pm10 < 0) AS negative_values
            FROM measurements
            WHERE timestamp::date = %s
            """,
            (run_date,),
        )
        row = cur.fetchone()

        cur.execute(
            """
            INSERT INTO catalog.quality_metrics (table_name, pipeline_date, row_count, null_rate, freshness_minutes)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (table_name, pipeline_date) DO NOTHING
            """,
            (
                "measurements",
                run_date,
                row[0],
                round(row[1] / row[0], 4) if row[0] > 0 else 0,
                0,
            ),
        )
        conn.commit()
    conn.close()

    null_rate = row[1] / row[0] if row[0] > 0 else 0
    if null_rate > 0.3:
        raise ValueError(f"Data quality check failed: null_rate={null_rate:.2%} > 30%")
    return {"total": row[0], "null_rate": null_rate, "negative_values": row[2]}


def compute_daily_summaries(**context):
    import psycopg2

    run_date = context["ds"]
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "airquality"),
        user=os.getenv("POSTGRES_USER", "airquality"),
        password=os.getenv("POSTGRES_PASSWORD", "airquality123"),
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO daily_summaries
                (sensor_id, summary_date, pm2_5_avg, pm2_5_max, pm10_avg, no2_avg,
                 o3_avg, co_avg, so2_avg, anomaly_count, record_count)
            SELECT
                m.sensor_id,
                %s::date,
                AVG(pm2_5), MAX(pm2_5), AVG(pm10), AVG(no2),
                AVG(o3), AVG(co), AVG(so2),
                COALESCE((
                    SELECT COUNT(*) FROM anomalies a
                    WHERE a.sensor_id = m.sensor_id AND a.timestamp::date = %s::date
                ), 0),
                COUNT(*)
            FROM measurements m
            WHERE m.timestamp::date = %s::date
            GROUP BY m.sensor_id
            ON CONFLICT (sensor_id, summary_date) DO UPDATE
                SET pm2_5_avg = EXCLUDED.pm2_5_avg,
                    pm2_5_max = EXCLUDED.pm2_5_max,
                    pm10_avg = EXCLUDED.pm10_avg,
                    no2_avg = EXCLUDED.no2_avg,
                    o3_avg = EXCLUDED.o3_avg,
                    co_avg = EXCLUDED.co_avg,
                    so2_avg = EXCLUDED.so2_avg,
                    anomaly_count = EXCLUDED.anomaly_count,
                    record_count = EXCLUDED.record_count
            """,
            (run_date, run_date, run_date),
        )
        conn.commit()
    conn.close()


def evaluate_models(**context):
    from ml.evaluate import run_evaluation
    results = run_evaluation()
    any_improved = any(r.get("action") in ("promoted_challenger", "promoted_first") for r in results)
    context["task_instance"].xcom_push(key="any_improved", value=any_improved)
    return results


def branch_deploy(**context):
    improved = context["task_instance"].xcom_pull(task_ids="evaluate_model", key="any_improved")
    return "deploy_model" if improved else "skip_deploy"


def deploy_model(**context):
    import mlflow
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    client = mlflow.tracking.MlflowClient()

    deployed = []
    for registry_name in [
        "air-quality-anomaly-classifier",
        "air-quality-pollution-forecaster",
        "air-quality-sensor-clustering",
    ]:
        try:
            staging_versions = client.get_latest_versions(registry_name, stages=["Staging"])
            for v in staging_versions:
                client.transition_model_version_stage(registry_name, v.version, "Production")
                deployed.append(f"{registry_name}:v{v.version}")
        except Exception as e:
            pass

    return {"deployed": deployed}


def update_catalog(**context):
    from catalog.ingest_metadata import run as ingest
    ingest()


with DAG(
    dag_id="daily_air_quality_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily air quality pipeline: extract, validate, train, evaluate, deploy",
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["air-quality", "production"],
    on_failure_callback=_send_slack,
) as dag:

    wait_for_new_data = NewDataSensor(
        task_id="wait_for_new_data",
        min_new_records=500,
        poke_interval=300,
        timeout=3600,
        mode="reschedule",
    )

    wait_for_streaming = StreamingDoneSensor(
        task_id="wait_for_streaming",
        max_age_minutes=15,
        poke_interval=120,
        timeout=1800,
        mode="reschedule",
    )

    extract = PythonOperator(
        task_id="extract_sensor_data",
        python_callable=extract_sensor_data,
    )

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    summarize = PythonOperator(
        task_id="compute_daily_summaries",
        python_callable=compute_daily_summaries,
    )

    train_classifier = MLflowTrainOperator(
        task_id="train_classifier",
        experiment_name="anomaly-classification",
        model_type="classification",
        days=30,
    )

    train_forecaster = MLflowTrainOperator(
        task_id="train_forecaster",
        experiment_name="pollution-forecasting",
        model_type="forecasting",
        days=30,
    )

    train_clustering = MLflowTrainOperator(
        task_id="train_clustering",
        experiment_name="sensor-clustering",
        model_type="clustering",
        days=90,
    )

    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_models,
    )

    branch = BranchPythonOperator(
        task_id="branch_deploy",
        python_callable=branch_deploy,
    )

    deploy = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
    )

    skip_deploy = EmptyOperator(task_id="skip_deploy")

    catalog_update = PythonOperator(
        task_id="update_catalog",
        python_callable=update_catalog,
        trigger_rule="none_failed_min_one_success",
    )

    [wait_for_new_data, wait_for_streaming] >> extract >> validate >> summarize
    summarize >> [train_classifier, train_forecaster, train_clustering]
    [train_classifier, train_forecaster, train_clustering] >> evaluate
    evaluate >> branch >> [deploy, skip_deploy]
    [deploy, skip_deploy] >> catalog_update
