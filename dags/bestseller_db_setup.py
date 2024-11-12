from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.datasets import Dataset
import os
from datetime import datetime


profile_config = ProfileConfig(
    profile_name="bestseller",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="dbt_to_snowflake",
        profile_args={"database": "BESTSELLER", "schema": "DEV"},
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        f"/usr/local/airflow/dags/dbt/bestseller",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"/usr/local/airflow/dbt_venv/bin/dbt",
    ),
    schedule=[Dataset(f"SEED://publish_output_dataset")],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="bestseller_db_setup",
    default_args={"retries": 2},
)
