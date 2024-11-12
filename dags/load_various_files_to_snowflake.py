from airflow import DAG
from datetime import datetime
from datetime import datetime, timedelta
from include.custom_operators.snowflake_upload_operator import SnowflakeUploadOperator
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
import yaml
import logging


def load_config(config_path):
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        config_files = [
            {
                "snowflake_conn_id": file["snowflake_conn_id"],
                "stage_name": file["stage_name"],
                "local_file_name": file["local_file_name"],
                "sql_craete_table": file["sql_craete_table"],
                "snowflake_table": file["snowflake_table"],
                "file_format": file["file_format"],
            }
            for file in config["files"]
        ]
        return config_files
    except Exception as e:
        logging.error(f"Failed to load configuration: {str(e)}")
        raise


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "load_various_files_to_snowflake",
    default_args=default_args,
    description="DAG to upload various files to Snowflake",
    schedule_interval=None,
    catchup=False,
    params={"config_path": "/usr/local/airflow/dags/config/load_various_files.yaml"},
)

with dag:
    config_path = dag.params["config_path"]
    config_files = load_config(config_path)
    with TaskGroup(group_id="upload_file_to_snowflake_group") as uci_data_task_group:
        for index, config_file in enumerate(config_files):
            snowflake_conn_id = config_file["snowflake_conn_id"]
            sql_craete_table = config_file["sql_craete_table"]
            snowflake_table = config_file["snowflake_table"]
            local_file_name = config_file["local_file_name"]
            stage_name = config_file["stage_name"]
            file_format = config_file["file_format"]
            sql_craete_table_path = f"/queries/" + sql_craete_table

            task_id = f"upload_file_{index}_{snowflake_table}"

            upload_task = SnowflakeUploadOperator(
                task_id=task_id,
                snowflake_conn_id=snowflake_conn_id,
                stage_name=stage_name,
                snowflake_table=snowflake_table,
                local_file_name=local_file_name,
                file_format=file_format,
                sql_craete_table=sql_craete_table_path,
                dag=dag,
            )
            logging.info(f"Created task: {task_id}")
    publish_to_dataset = EmptyOperator(
        task_id="publish_to_dataset",
        outlets=[Dataset(f"SEED://publish_output_dataset")],
        dag=dag,
    )
    uci_data_task_group >> publish_to_dataset

logging.info("DAG loaded successfully")
