from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default arguments for our pipeline
default_args = {
    'owner': 'dataops_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'daily_air_quality_pipeline',
    default_args=default_args,
    description='Main ETL and MLOps pipeline for Air Quality prediction',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['mlops', 'production'],
) as dag:

    # 1. Sensors (Waiting for data)
    wait_for_new_data = EmptyOperator(task_id='wait_for_new_data')
    wait_for_streaming = EmptyOperator(task_id='wait_for_streaming')

    # 2. Data Preparation
    extract_sensor_data = EmptyOperator(task_id='extract_sensor_data')
    validate_data_quality = EmptyOperator(task_id='validate_data_quality')
    compute_daily_summaries = EmptyOperator(task_id='compute_daily_summaries')

    # 3. Model Training (Parallel Execution)
    train_classifier = EmptyOperator(task_id='train_classifier')
    train_forecaster = EmptyOperator(task_id='train_forecaster')
    train_clustering = EmptyOperator(task_id='train_clustering')

    # 4. Evaluation & Deployment
    evaluate_model = EmptyOperator(task_id='evaluate_model')
    branch_deploy = EmptyOperator(task_id='branch_deploy')
    deploy_model = EmptyOperator(task_id='deploy_model')
    skip_deploy = EmptyOperator(task_id='skip_deploy')
    update_catalog = EmptyOperator(task_id='update_catalog')

    # --- DEFINE THE PIPELINE GRAPH (The visual arrows) ---
    
    [wait_for_new_data, wait_for_streaming] >> extract_sensor_data
    
    extract_sensor_data >> validate_data_quality >> compute_daily_summaries
    
    compute_daily_summaries >> [train_classifier, train_forecaster, train_clustering]
    
    [train_classifier, train_forecaster, train_clustering] >> evaluate_model
    
    evaluate_model >> branch_deploy
    
    branch_deploy >> [deploy_model, skip_deploy]
    
    [deploy_model, skip_deploy] >> update_catalog