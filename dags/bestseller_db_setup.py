from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig


from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.datasets import Dataset
from datetime import datetime

def create_dbt_dag():
    profile_config = ProfileConfig(
        profile_name="bestseller",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="dbt_to_snowflake",
            profile_args={"database": "BESTSELLER", "schema": "DEV"},
        ),
    )

    return DbtDag(
        project_config=ProjectConfig("/usr/local/airflow/dags/dbt/bestseller"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path="/usr/local/airflow/dbt_venv/bin/dbt",
        ),
        operator_args={
        "install_deps": True,  
        "full_refresh": True,  
    },
        schedule=[Dataset("SEED://publish_output_dataset")],
        start_date=datetime(2023, 1, 1),
        catchup=False,
        dag_id="bestseller_db_setup",
        default_args={"retries": 2},
    )

dag = create_dbt_dag()
