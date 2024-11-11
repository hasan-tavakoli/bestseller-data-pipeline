-- Use an admin role
USE ROLE ACCOUNTADMIN; 

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;  -- Create the 'transform' role if it does not already exist.
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;  -- Grant the 'transform' role to the ACCOUNTADMIN role.

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;  -- Create the warehouse 'COMPUTE_WH' if it does not already exist.
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;  -- Grant operational permissions on 'COMPUTE_WH' to the 'transform' role.

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt  -- Create the user 'dbt' if it does not already exist.
  PASSWORD='dbtPassword123'  -- Set the password for the 'dbt' user.
  LOGIN_NAME='dbt'  -- Set the login name for the 'dbt' user.
  MUST_CHANGE_PASSWORD=FALSE  -- User does not need to change the password upon first login.
  DEFAULT_WAREHOUSE='COMPUTE_WH'  -- Set the default warehouse to 'COMPUTE_WH'.
  DEFAULT_ROLE='transform'  -- Set the default role to 'transform'.
  DEFAULT_NAMESPACE='BESTSELLER.RAW'  -- Set the default namespace to 'BESTSELLER.RAW'.
  COMMENT='DBT user used for data transformation';  -- Comment describing the purpose of the 'dbt' user.
GRANT ROLE transform to USER dbt;  -- Grant the 'transform' role to the 'dbt' user.

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS BESTSELLER;  -- Create the 'BESTSELLER' database if it does not already exist.
CREATE SCHEMA IF NOT EXISTS BESTSELLER.RAW;  -- Create the 'RAW' schema in the 'BESTSELLER' database if it does not already exist.

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform;  -- Grant full permissions on 'COMPUTE_WH' warehouse to the 'transform' role.
GRANT ALL ON DATABASE BESTSELLER to ROLE transform;  -- Grant full permissions on the 'BESTSELLER' database to the 'transform' role.
GRANT ALL ON ALL SCHEMAS IN DATABASE BESTSELLER to ROLE transform;  -- Grant full permissions on all schemas in the 'BESTSELLER' database to the 'transform' role.
GRANT ALL ON FUTURE SCHEMAS IN DATABASE BESTSELLER to ROLE transform;  -- Grant full permissions on future schemas in the 'BESTSELLER' database to the 'transform' role.
GRANT ALL ON ALL TABLES IN SCHEMA BESTSELLER.RAW to ROLE transform;  -- Grant full permissions on all tables in the 'BESTSELLER.RAW' schema to the 'transform' role.
GRANT ALL ON FUTURE TABLES IN SCHEMA BESTSELLER.RAW to ROLE transform;  -- Grant full permissions on future tables in the 'BESTSELLER.RAW' schema to the 'transform' role.


--Finally, two connections are required:
--These connections should be configured for use in Apache Airflow.
--dbt_to_snowflake with the DEV schema.
--snowflake-bs for the RAW schema.
