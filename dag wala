import os
import shutil
import subprocess
import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Set Kaggle credentials
os.environ['KAGGLE_USERNAME'] = 'studenticse'
os.environ['KAGGLE_KEY'] = '2d1b6a3a4a990f35a55cd3b6bf6b7ee5'

class Extract:
    def __init__(self, dataset, extract_dir, delete_zip_after_unzip=True, encoding="utf-8", verbose=True):
        self.dataset = dataset
        self.extract_dir = Path(extract_dir)
        self.extract_dir.mkdir(parents=True, exist_ok=True)
        self.delete_zip_after_unzip = delete_zip_after_unzip
        self.encoding = encoding
        self.verbose = verbose

        if shutil.which("kaggle") is None:
            raise EnvironmentError("Kaggle CLI not found. Install it and add to PATH.")

        if not self._has_csv_in_dir():
            self._download_and_unzip()

        csv_path = self._find_accidents_csv()
        if not csv_path:
            raise FileNotFoundError("No yellow_taxi*.csv found.")

        self.csv_path = str(csv_path)
        self.df = self._load_csv()

    def _has_csv_in_dir(self) -> bool:
        return any(self.extract_dir.glob("yellow_taxi*.csv")) or any(self.extract_dir.glob("*.csv"))

    def _find_accidents_csv(self) -> Optional[Path]:
        preferred = sorted(self.extract_dir.glob("yellow_taxi*.csv"))
        return preferred[-1] if preferred else next(iter(sorted(self.extract_dir.glob("*.csv"))), None)

    def _download_and_unzip(self):
        cmd = ["kaggle", "datasets", "download", "-d", self.dataset, "-p", str(self.extract_dir), "--unzip"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Kaggle download failed:\n{result.stderr}")
        if self.delete_zip_after_unzip:
            for z in self.extract_dir.glob("*.zip"):
                z.unlink()

    def _load_csv(self):
        try:
            return pd.read_csv(self.csv_path, encoding=self.encoding, low_memory=False)
        except UnicodeDecodeError:
            return pd.read_csv(self.csv_path, encoding="utf-8-sig", low_memory=False)

# Python callable for Airflow
def fetch_yellow_taxi_data():
    try:
        extract = Extract(
            dataset="elemento/nyc-yellow-taxi-trip-data",
            extract_dir=r"C:\Users\user\spark-4.0.1-bin-hadoop3\Airflow\mysql_data",
            delete_zip_after_unzip=True,
            encoding="utf-8",
            verbose=True,
        )
        logging.info("Yellow taxi data downloaded successfully.")
    except Exception as e:
        logging.error(f"Error in fetch_yellow_taxi_data: {e}")
        raise

def upload_csv_to_airflow():
    try:
        source_path = r"C:\Users\user\spark-4.0.1-bin-hadoop3\Airflow\Output\yellow_taxi.csv"
        airflow_data_dir = "/opt/airflow/data"
        os.makedirs(airflow_data_dir, exist_ok=True)
        shutil.copy(source_path, os.path.join(airflow_data_dir, "yellow_taxi.csv"))
        logging.info(f"CSV uploaded to {airflow_data_dir}")
    except Exception as e:
        logging.error(f"Error in upload_csv_to_airflow: {e}")
        raise

# DAG definition
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="yellow_taxi_pipeline",
    default_args=default_args,
    description="ETL pipeline for yellow taxi",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["yellow_taxi", "etl"],
) as dag:

    download_task = PythonOperator(
        task_id="download_yellow_taxi_data",
        python_callable=fetch_yellow_taxi_data,
    )

    upload_csv_task = PythonOperator(
        task_id="upload_yellow_taxi_csv",
        python_callable=upload_csv_to_airflow,
    )

    etl_task = BashOperator(
        task_id="spark_etl_yellow_taxi",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            "--jars /opt/airflow/spark_jobs/mysql-connector-j-8.0.33.jar "
            "/opt/airflow/spark_jobs/etl_yellow_taxi.py "
            "/opt/airflow/data/yellow_taxi.csv "
            "jdbc:mysql://mysql:3306/airflow_db "
            "airflow airflow"
        ),
    )
