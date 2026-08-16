

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/mnt/c/Users/shriv/OneDrive/Desktop/nyc-taxi-azure-data-engineering"
PYTHON_EXE = f"{PROJECT_DIR}/.venv-linux/bin/python"


with DAG(
    dag_id="nyc_taxi_end_to_end_pipeline",
    description="NYC Taxi Bronze Silver Gold PySpark Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "pyspark", "azure", "data-engineering"],
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f'cd "{PROJECT_DIR}" && "{PYTHON_EXE}" pyspark/bronze_ingestion.py',
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command=f'cd "{PROJECT_DIR}" && "{PYTHON_EXE}" pyspark/silver_transformation.py',
    )

    gold_transformation = BashOperator(
        task_id="gold_transformation",
        bash_command=f'cd "{PROJECT_DIR}" && "{PYTHON_EXE}" pyspark/gold_transformation.py',
    )

    bronze_ingestion >> silver_transformation >> gold_transformation
