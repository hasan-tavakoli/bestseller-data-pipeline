from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from include.custom_hooks.ucimlrepo_hook import UCIDataFetchHook
from airflow.utils.task_group import TaskGroup
import yaml
import logging


def load_config(config_path):
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logging.info(f"Loaded config file from {config_path}")
    except Exception as e:
        logging.error(f"Error loading config file: {str(e)}")
        raise

    config_files = [
        {
            "dataset_id": file["dataset_id"],
            "file_name": file["file_name"],
        }
        for file in config["files"]
    ]
    return config_files


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_and_save_using_hook(dataset_id: str, destination_path: str, **kwargs):
    try:
        hook = UCIDataFetchHook(dataset_id=dataset_id, output_path=destination_path)
        hook.get_data()
        logging.info(
            f"Successfully fetched and saved dataset: {dataset_id} to {destination_path}"
        )
    except Exception as e:
        logging.error(f"Error fetching dataset {dataset_id}: {str(e)}")
        raise


dag = DAG(
    "fetch_and_save_uci_data",
    default_args=default_args,
    description="Fetch and save datasets from UCI repository",
    schedule_interval=None,
    catchup=False,
    params={"config_path": "/usr/local/airflow/dags/config/uci_data_fetch_config.yaml"},
)

with dag:
    config_path = dag.params["config_path"]
    config_files = load_config(config_path)
    with TaskGroup(group_id="uci_data_data_group") as uci_data_task_group:
        for index, config_file in enumerate(config_files):
            file_name = config_file["file_name"]
            dataset_id = config_file["dataset_id"]
            destination_path = f"/tmp/" + file_name
            extract_data_task = PythonOperator(
                task_id=f"extract_data_{index}_{file_name}",
                python_callable=extract_and_save_using_hook,
                op_kwargs={
                    "dataset_id": dataset_id,
                    "destination_path": destination_path,
                },
            )

logging.info("DAG loaded successfully")
