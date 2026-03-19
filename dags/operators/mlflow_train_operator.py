import sys
import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

sys.path.insert(0, "/opt/airflow")


class MLflowTrainOperator(BaseOperator):
    template_fields = ("experiment_name", "model_type", "hyperparams")

    @apply_defaults
    def __init__(
        self,
        experiment_name: str,
        model_type: str,
        hyperparams: dict = None,
        days: int = 30,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.experiment_name = experiment_name
        self.model_type = model_type
        self.hyperparams = hyperparams or {}
        self.days = days

    def execute(self, context):
        from ml.train import run as train_run

        self.log.info(f"Starting training: experiment={self.experiment_name} model={self.model_type}")
        results = train_run(task=self.model_type, days=self.days)
        run_id = results.get(self.model_type, {}).get("run_id")
        improved = results.get(self.model_type, {}).get("improved", False)

        self.log.info(f"Training complete: run_id={run_id} improved={improved}")
        context["task_instance"].xcom_push(key="run_id", value=run_id)
        context["task_instance"].xcom_push(key="improved", value=improved)
        return run_id