# DBT Data Pipeline Project

## Project Overview
This project builds an end-to-end data pipeline to process and analyze e-commerce transaction data. The goal is to extract data from CSV files, transform it, and load it into a data warehouse for analytics.

## Tools & Technologies Used
- **Apache Airflow**: Orchestrates the data pipeline, schedules tasks, and monitors logs.
- **dbt (Data Build Tool)**: Transforms raw data into an analytics-friendly format.
- **Snowflake**: Serves as the data warehouse for storing and querying data.

## Project Structure
```bash
bestseller-data-pipeline
│
├── .astro                    # .astro directory (configuration for Astro)
├── dags                       # dags directory (contains Airflow DAGs)
│   ├── config                 # config directory (configuration files for Airflow dags)
│   ├── dbt                    # dbt directory 
│   │   └── bestseller         # dbt project related to the bestseller data pipeline
│   ├── include                # include directory (for reusable code or resources)
│   ├── queries                # queries directory (contains SQL query files used in DAGs)
│   ├── .airflowignore         #  file (to exclude files from being picked up by Airflow)
│   ├── bestseller_db_setup.py  # Airflow script for running the dbt project and testing
│   ├── fetch_and_save_uci_data.py  # Airflow script for fetching and saving UCI data
│   └── load_various_files_to_snowflake.py # Airflow script for loading local data into Snowflake
│
├── files                      # files directory (including initial setup and necessary instructions)
├── include                    # include directory (for shared code or libraries)
├── plugins                    # plugins directory (for custom Airflow plugins)
├── tests                      # tests directory (for test scripts and validation)
│
├── .dockerignore              # file (specifies which files to ignore during Docker builds)
├── .gitignore                 # .gitignore file (specifies which files to ignore in version control)
├── Dockerfile                 # Dockerfile (configuration for building the Docker image)
├── packages.txt               # packages.txt (list of required packages or dependencies)
├── README.md                  # README file (documentation of the project)
└── requirements.txt           # requirements.txt (Python dependencies for the project)
```

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

### Setup Instructions

Before running Astro, you need to execute a file named `create.sql` located in the `files` folder (as mentioned above). You must run this file in **Snowflake**. Make sure that **all the instructions** in the file are executed, as this is needed for the initial setup of Snowflake.

#### Creating Snowflake Connections in Airflow

After running Astro, once you enter to **Airflow**, you need to create two connections to **Snowflake**:

1. **dbt_to_snowflake**:  
   This connection should point to the **DEV schema**, using the **data warehouse**, **username**, and **password** provided in the file or chosen by you.

2. **snowflake-bs**:  
   This connection should point to the **RAW schema**.

#### Running the DAGs

Once these connections are set up, you can run the first DAG, `fetch_and_save_uci_data.py`, which will fetch data. This DAG can be scheduled to run whenever necessary.

When the second DAG, `load_various_files_to_snowflake.py`, is executed, it will automatically trigger the third DAG, `bestseller_db_setup.py`.

Make sure all instructions are followed properly.

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


## Step 5 - Model Design

In Step 5, the design model is structured as shown below:

![Model Design](https://github.com/hasan-tavakoli/bestseller-data-pipeline/blob/main/image/image.png?raw=true)


This design represents the architecture and flow of the data pipeline, illustrating how each component interacts with Snowflake and Airflow.


## Data Pipeline Explanation

### Step Overview

Let's explain it a bit.

In the previous step, I mentioned that the data is entered into a table named `RAW_ONLINE_RETAIL` in the `RAW` schema. This zone is where raw data is stored. In this area, the data is entered into the table without any changes.

### Data Transformation

In the next step, the data is slightly transformed. For example, names are changed, or fields that should be used as filters are adjusted (meaning those that need to be considered based on the column type).

### Global Zone

In the next phase, I renamed the zone to “Global.” At this stage, the data needs to be transformed further and stored as a table since it might be needed multiple times. I thought that data from other zones might also need to be integrated, and this is where all the data would be accumulated.

### Adding Columns to Prevent Data Removal

However, at this point, several columns are added to the table. I do this to prevent unnecessary data removal. For instance, at some point, we might need to log system failures or other issues.

One such column is `transaction_status`, which indicates whether a transaction is canceled or not.

Another column is `transaction_type`. Based on my research, I found that after cleaning the product ID and transaction ID, it would be better to add this column. In some cases, we might have conditions like:

```sql
WHEN transaction_status = 'normal' AND quantity > 0 AND unit_price = 0 THEN 'free_or_promotion'
WHEN transaction_status = 'normal' AND quantity < 0 AND unit_price = 0 THEN 'return_free'
WHEN transaction_status = 'normal' AND quantity < 0 AND unit_price > 0 THEN 'return_paid'
WHEN transaction_status = 'cancelled' AND quantity < 0 THEN 'cancelled_return'
ELSE 'sale'
```

### Business Assumptions and Other Requirements

Anyway, this is my assumption, and there might be other business cases or requirements.

### Data Cleaning and Ready for Fact and Dimension Tables

After this phase, the data is cleaned and ready for entry into the dimension and fact tables. Upon further analysis, I realized that it would be better to have two dimensions. I assumed that we only deal with these transactions.

### Product ID Variants

Each product ID can have multiple names—this could be related to the same product but with different variants. For example, a product like a carburetor could have different colors, etc.

### Customer Data Segmentation

I also separated customer data so that analysis related to countries for those customers could be retrieved. The first entry date is also useful for tracking purposes.

### Fact Table Considerations

For the fact table, I thought it would be better to have a key because it would make it easier for us to join data when reporting. A fact table also needs to bring in numeric values, and additional columns can be added or removed based on the specific use case.

### Dimensions and Other Tables

The dimensions could potentially have other tables, or they might not be needed here.

### RFM Analysis and Data Mart

In the next step, based on the RFM (Recency, Frequency, Monetary) analysis requirement, I created a consolidated mart, which integrates data from the dimensions obtained via(seed) DBT and the fact table I created.

### Logging and Testing

Logging and testing have also been implemented throughout the pipeline to ensure data integrity and successful execution of the processes.

- **Logging:** 
  Detailed logging has been set up to capture important events during the execution of the DAGs. This includes logging information such as the success or failure of tasks, processing times, and any exceptions that may arise. This allows for better monitoring and troubleshooting of the data pipeline.

- **Testing:** 
  Various tests have been created to validate the functionality of the pipeline. 


![Model Design](https://github.com/hasan-tavakoli/bestseller-data-pipeline/blob/main/image/image2.png?raw=true)
