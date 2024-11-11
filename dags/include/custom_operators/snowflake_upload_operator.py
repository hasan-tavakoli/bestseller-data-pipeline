from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults
import os
import logging


class SnowflakeUploadOperator(BaseOperator):

    template_fields = ("sql_craete_table",)
    template_ext = (".sql",)

    @apply_defaults
    def __init__(
        self,
        snowflake_conn_id,
        stage_name,
        sql_craete_table,
        snowflake_table,
        local_file_name,
        file_format="csv",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql_craete_table = sql_craete_table
        self.snowflake_table = snowflake_table
        self.stage_name = stage_name
        self.snowflake_conn_id = snowflake_conn_id
        self.local_file_name = local_file_name
        self.local_file_path = f"/tmp/{self.local_file_name}"
        self.file_format = file_format.lower()
        self.logger = logging.getLogger(__name__)

    def _create_table_if_not_exists(self):
        try:
            self.snowflake_hook.run(self.sql_craete_table)
            self.log.info(f"Table {self.snowflake_table} created.")
        except Exception as e:
            self.log.error(f"Failed to create table: {str(e)}")
            raise

    def _upload_file_to_stage(self):
        try:
            self.snowflake_hook.run(
                f"PUT file://{self.local_file_path} @{self.stage_name} auto_compress=false;"
            )
            self.log.info(
                f"File {self.local_file_name} uploaded to stage {self.stage_name}."
            )
        except Exception as e:
            self.log.error(f"Failed to upload file to stage: {str(e)}")
            raise

    def load_data_from_stage_to_table(self):

        try:
            if self.file_format == "csv":
                file_format_sql = """
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ',', TIMESTAMP_FORMAT = 'MM/DD/YYYY HH24:MI')
                """
            elif self.file_format == "json":
                file_format_sql = "FILE_FORMAT = (TYPE = 'JSON')"
            elif self.file_format == "parquet":
                file_format_sql = "FILE_FORMAT = (TYPE = 'PARQUET')"
            elif self.file_format == "xlsx":
                file_format_sql = """
                FILE_FORMAT = (TYPE = 'CSV', SKIP_HEADER = 1, FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = ',', TIMESTAMP_FORMAT = 'MM/DD/YYYY HH24:MI')
                """
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")

            copy_sql = f"""
            COPY INTO {self.snowflake_table}
            FROM @{self.stage_name}/{self.local_file_name}
            {file_format_sql}
            ON_ERROR = 'CONTINUE';
            """
            self.snowflake_hook.run(copy_sql)
            self.log.info(
                f"Data loaded into table {self.snowflake_table} from stage {self.stage_name}."
            )
        except Exception as e:
            self.log.error(f"Failed to load data from stage to table: {str(e)}")
            raise

    def _convert_excel_to_csv(self):
        import pandas as pd

        try:
            self.log.info(f"Converting Excel file {self.local_file_path} to CSV.")
            df = pd.read_excel(self.local_file_path)
            csv_path = self.local_file_path.replace(".xlsx", ".csv")
            df.to_csv(csv_path, index=False)
            self.local_file_path = csv_path
            self.log.info(f"Excel file converted to CSV: {self.local_file_path}")
        except Exception as e:
            self.log.error(f"Failed to convert Excel to CSV: {str(e)}")
            raise

    def execute(self, context):
        try:
            self.snowflake_hook = SnowflakeHook(
                snowflake_conn_id=self.snowflake_conn_id
            )
            self._create_table_if_not_exists()
            self._upload_file_to_stage()
            if self.file_format == "xlsx":
                self._convert_excel_to_csv()
            self.load_data_from_stage_to_table()
            if os.path.exists(self.local_file_path):
                # os.remove(self.local_file_path)
                self.log.info(
                    f"Local file {self.local_file_path} deleted successfully."
                )
        except Exception as e:
            self.log.error(f"Error occurred: {str(e)}")
            raise
