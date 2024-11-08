# BESTSELLER Data Pipeline Project

## Project Overview
This project builds an end-to-end data pipeline to process and analyze e-commerce transaction data. The goal is to extract data from CSV files, transform it, and load it into a data warehouse for analytics.

## Tools & Technologies Used
- **Apache Airflow**: Orchestrates the data pipeline, schedules tasks, and monitors logs.
- **dbt (Data Build Tool)**: Transforms raw data into an analytics-friendly format.
- **Snowflake**: Serves as the data warehouse for storing and querying data.

## Project Structure
 .astro
- dags
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


