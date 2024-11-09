# BESTSELLER Data Pipeline Project

## Project Overview
This project builds an end-to-end data pipeline to process and analyze e-commerce transaction data. The goal is to extract data from CSV files, transform it, and load it into a data warehouse for analytics.

## Tools & Technologies Used
- **Apache Airflow**: Orchestrates the data pipeline, schedules tasks, and monitors logs.
- **dbt (Data Build Tool)**: Transforms raw data into an analytics-friendly format.
- **Snowflake**: Serves as the data warehouse for storing and querying data.

## Project Structure
- .astro
- dags
- dbt
- include
- plugins
- tests
- .dockerignore
- .env
- .gitignore
- airflow_settings.yaml
- Dockerfile
- packages.txt
- README.md
- requirements.txt

### Airflow with Astro

To streamline local development of Apache Airflow, **Astro** was used. The command `astro dev init` was run to initialize the local development environment, enabling easy setup and management of the project. Astro is a cloud-native tool that simplifies Airflow management and allows for smooth development, testing, and deployment of Airflow workflows. It automatically sets up Docker configurations and dependencies needed for Airflow.

With the integration of Astro, the project now benefits from an easy-to-use local development environment for testing the DAGs before deploying them to production.

## Step 2: Configuration for Execution

After initializing the local development environment with `astro dev init`, the next step is to configure and run the Airflow environment locally. This is done by using the `astro dev start` command. The command initializes the Docker containers and sets up the required environment based on the configurations in the `.astro` folder and other related files.

To start the local development environment, run the following command:

```bash
astro dev start
```
### Airflow Login Credentials

To access the Airflow UI in your local development environment, use the following credentials:

- **Username**: admin
- **Password**: admin

## Step 3: Data Extraction and Storage Strategy

### Custom Hook for Data Extraction

To efficiently extract data from external sources, a custom Airflow hook named **UCIDataFetchHook** was developed. This hook connects to the UCI Machine Learning Repository, downloads datasets, and stores them in a local directory.

#### Why a Custom Hook?
- **Reusability**: The hook is reusable across multiple DAGs, allowing consistent data extraction without rewriting code.
- **Scalability**: It simplifies connecting to external data sources, making the pipeline extensible for future use cases.
- **Optimization**: Using hooks allows for better separation of concerns, improving code maintainability.

### Dedicated DAG for Data Extraction and Storage

A dedicated DAG named `fetch_and_save_uci_data` was created to automate the data extraction and storage process. This DAG utilizes the custom hook to fetch datasets and save them as CSV files.

#### Key Features:
- **Dynamic Task Generation**: The DAG reads dataset configurations from a YAML file (`uci_data_fetch_config.yaml`). This configuration-based approach allows for the easy addition of new datasets by updating the configuration file.
- **Modularity**: Each dataset extraction is handled as a separate task, making the DAG modular and easier to maintain.
- **Scalability**: By separating dataset configurations from the code, the pipeline can be extended to handle new data sources with minimal changes.


## Step 4: Data Transfer from Local to Snowflake

In this step, a new DAG named `load_various_files_to_snowflake` has been created to automate the transfer of data from the local data in environment to Snowflake.

#### Custom Operator for Data Transformation

A custom operator was designed to handle data in different formats beyond just CSV files. This operator processes the input dataand then loads it into the designated Snowflake table. The custom operator makes the pipeline more flexible and scalable by supporting various file formats and enabling the use of different SQL queries.

#### Key Features:
- **Dynamic File Format Support**: The custom operator can handle multiple file formats (such as CSV, JSON, etc.) and loads the data into Snowflake according to the corresponding SQL query.
- **Query and Schema Management**: A new folder, `queries`, has been added to store the SQL queries and table schemas for creating the necessary tables in Snowflake.
- **Configurable Workflow**: The configuration for each file is stored in a YAML file, making it easy to manage and extend the pipeline by simply adding new configurations.

### Each entry in the configuration specifies:

- **snowflake_conn_id**: The connection ID to Snowflake.
- **stage_name**: The Snowflake stage where the data will be loaded.
- **local_file_name**: The name of the file in the local directory.
- **sql_create_table**: The SQL script used to create the table in Snowflake.
- **snowflake_table**: The name of the target table in Snowflake.
- **file_format**: The format of the input file (CSV, JSON, etc.).

### User Input for Each File

For each file, the user provides the necessary information in the configuration file. This allows for easy management of different data sources and ensures the pipeline can handle various types of files seamlessly. The flexibility of the pipeline ensures it can scale as new data sources are added without requiring significant changes to the existing codebase.





