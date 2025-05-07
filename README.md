# Housing Retail Pipeline

A data pipeline project for processing and analyzing housing retail data using Apache Airflow and Azure Data Factory (ADF) with Databricks integration.

## WorkFlow
![image](https://github.com/Jhonatan19991/images/blob/main/Housing-images/housing.drawio.png)

## Project Overview

This project implements an automated data pipeline for housing retail data processing and analysis. It includes data extraction, transformation, and loading (ETL) processes, along with exploratory data analysis capabilities. The solution leverages both Apache Airflow for local development and Azure Data Factory for cloud-based orchestration, with Databricks notebooks for scalable data processing.

## Project Structure

```
housing-retail-pipeline/
├── dag/                    # Airflow DAG definitions
│   ├── dags.py            # Main DAG configuration
│   └── func.py            # DAG helper functions
├── data/                   # Data storage directory
├── src/                    # Source code directory
├── requirements.txt       # Project dependencies
└── README.md             # Project documentation
```

## Features

- Hybrid pipeline architecture:
  - Local development using Apache Airflow
  - Production deployment using Azure Data Factory
- Databricks integration for scalable data processing
- Automated data pipeline for housing retail data
- Data processing and transformation
- Exploratory Data Analysis (EDA) capabilities
- Integration with various data sources
- Data validation and quality checks

## Azure Integration

### Azure Data Factory (ADF)
- Orchestrates the entire data pipeline in production
- Manages data movement and transformation workflows
- Integrates with Azure Databricks for data processing
- Handles scheduling and monitoring of pipeline runs
- Provides data lineage and monitoring capabilities

### Azure Databricks
- Executes data transformation notebooks
- Provides scalable compute resources for data processing
- Enables interactive data exploration and analysis
- Supports both batch and streaming data processing
- Integrates with Azure Data Factory for orchestration

### ADF Pipeline

![image](https://github.com/Jhonatan19991/images/blob/main/Housing-images/ADF-PipeLine.png)

## Prerequisites

- Python 3.x
- Apache Airflow
- Virtual environment (recommended)
- Azure subscription
- Azure Data Factory instance
- Azure Databricks workspace
- Azure Storage account

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd housing-retail-pipeline
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up Airflow:
```bash
# Initialize Airflow database
airflow db init

# Create Airflow user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

5. Azure Setup:
   - Create an Azure Data Factory instance
   - Set up an Azure Databricks workspace
   - Configure Azure Storage account
   - Set up necessary Azure service connections
   - Configure Azure Key Vault for secrets management

## Usage

### Local Development (Airflow)
1. Start the Airflow webserver:
```bash
airflow webserver -p 8080
```

2. Start the Airflow scheduler:
```bash
airflow scheduler
```

3. Access the Airflow web interface at `http://localhost:8080`

4. Task overview
![image](https://github.com/Jhonatan19991/images/blob/main/Housing-images/Airflow-Task.png)



### Azure Data Factory
1. Access your Azure Data Factory instance through the Azure Portal
2. Monitor pipeline runs and execution status
3. Configure triggers and schedules
4. Set up alerts and notifications

### Databricks
1. Access your Databricks workspace
2. Run and monitor notebook executions
3. Configure cluster settings
4. Monitor job performance

## Dependencies

The project uses several key Python packages:
- Apache Airflow for workflow orchestration
- Pandas for data manipulation
- NumPy for numerical operations
- Matplotlib and Seaborn for data visualization
- Jupyter for interactive development
- SQLAlchemy for database operations
- Azure SDK for Python
- Databricks Connect
- Azure Data Factory SDK

For a complete list of dependencies, see `requirements.txt`.


