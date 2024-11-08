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

### Step 2: Configuration for Execution

After initializing the local development environment with `astro dev init`, the next step is to configure and run the Airflow environment locally. This is done by using the `astro dev start` command. The command initializes the Docker containers and sets up the required environment based on the configurations in the `.astro` folder and other related files.

To start the local development environment, run the following command:

```bash
astro dev start
```
# Airflow Login Credentials

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