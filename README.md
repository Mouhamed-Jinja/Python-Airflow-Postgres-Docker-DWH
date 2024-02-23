![python-pipeline](https://github.com/Mouhamed-Jinja/Python-Data-Pipeline-PG-DWH/assets/132110499/9caee518-0fbe-4cb8-a0c3-b41535f6af12)

## Project Description

### Retail Data Warehouse (Retail-DWH)

The Retail Data Warehouse (Retail-DWH) project is an end-to-end data pipeline for processing and analyzing retail data. It encompasses data extraction, transformation, loading (ETL), and storage in a data warehouse. The goal of the project is to provide a robust solution for handling large volumes of retail data, enabling businesses to gain insights and make data-driven decisions.

### Features:

1. **Data Extraction**: Extracts retail data from CSV files using Python scripts.
2. **Data Transformation**: Cleans and transforms the raw retail data to prepare it for analysis.
3. **Data Loading**: Loads the transformed data into a PostgreSQL database, organized in a star schema format.
4. **Dimensional Modeling**: Implements dimensional modeling techniques to organize data into dimensions and fact tables, facilitating efficient querying and analysis.
5. **Dockerization**: Provides Docker Compose configuration for easy deployment of the PostgreSQL database.

### Components:

- **Extract.py**: Python script for extracting retail data from CSV files.
- **Transform.py**: Python script for cleaning and transforming the raw data.
- **Load-DWH.py**: Python script for loading transformed data into the data warehouse.
- **Docker Compose File**: Defines a Docker Compose configuration for running the PostgreSQL database container.

### Technologies Used:

- Python
- Pandas
- SQLAlchemy
- psycopg2
- Docker
- PostgreSQL

### Usage:

1. Clone the repository.
2. Customize configuration parameters (e.g., database connection details) as needed.
3. Run the appropriate Python scripts (`Extract.py`, `Transform.py`, `Load-DWH.py`) to execute the data pipeline.
4. Monitor the pipeline execution and check the data warehouse for the loaded data.
5. Analyze the data using SQL queries or connect to visualization tools for further analysis.
---
