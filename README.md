---
# Architecture:

![postgre_scd drawio_58144198](https://github.com/Mouhamed-Jinja/Python-Airflow-Postgres-Docker-DWH/assets/132110499/2561686b-a8d3-4ec8-b2a6-2c5af8518292)

## Airflow ETL Workflow

This repository contains Airflow Directed Acyclic Graphs (DAGs) and associated scripts for orchestrating an Extract, Transform, Load (ETL) workflow. The workflow is designed to extract data from a source, perform transformations, and load it into a data warehouse.

## Overview

The ETL workflow consists of the following components:

- **DAGs**: Airflow DAGs define the workflow's structure and task dependencies.
- **Scripts**: Python scripts used by Airflow tasks for data extraction, transformation, and loading.
- **SQL Scripts**: SQL scripts for database operations, such as creating tables or performing Slowly Changing Dimension (SCD) updates.
## Data Pipeline:

![Architecture](https://github.com/Mouhamed-Jinja/Python-Airflow-Postgres-Docker-DWH/assets/132110499/8c7d80f9-9f77-4dea-ae93-effa87727afd)

## DAGs

### DAG_Build_v1.0

This DAG orchestrates the full ETL workflow, including building dimension tables and loading the data warehouse.

Tasks:
- `Build_Dimantions`: Builds dimension tables using SQL scripts.
- `Extract_v1`: Extracts data from the source system.
- `Transform_v1`: Transforms extracted data using Python scripts.
- `Load_v1`: Loads transformed data into the data warehouse.

### DAG_ETL_v1.0

This DAG focuses on the ETL process, excluding dimension table builds.

Tasks:
- `Extract_v1`: Extracts data from the source system.
- `Transform_v1`: Transforms extracted data using Python scripts.
- `Load_v1`: Loads transformed data into the data warehouse.

## Scripts

### build.py

Python script for building dimension tables in the data warehouse.

### Extract.py

Python script for extracting data from the source system.

### Transform.py

Python script for transforming extracted data.

### Load-DWH.py

Python script for loading transformed data into the data warehouse.

### SQL Scripts

- `dimproduct.sql`: SQL script for Slowly Changing Dimension (SCD) operations on the product dimension.
- `dimcustomer.sql`: SQL script for SCD operations on the customer dimension.
- `fact_sales.sql`: SQL script for loading data into the fact table.

## Usage

1. Install Apache Airflow and configure the Airflow environment.
2. Clone this repository.
3. Place the DAG files in the Airflow DAGs directory (`$AIRFLOW_HOME/dags`).
4. Execute the DAGs using the Airflow UI or CLI.
5. Monitor the DAG runs and task executions in the Airflow UI.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

---
