\# NYC Taxi Azure Data Engineering



\## Project Overview



An end-to-end Azure Data Engineering project using NYC Yellow Taxi Trips 2024 data.



The project demonstrates batch data ingestion, data transformation, data warehousing, orchestration, analytics, and visualization using Azure Data Engineering technologies.



\## Architecture



Kaggle Dataset

&#x20;       ↓

Azure Data Factory

&#x20;       ↓

ADLS Gen2 - Bronze

&#x20;       ↓

Azure Databricks + PySpark

&#x20;       ↓

ADLS Gen2 - Silver

&#x20;       ↓

ADLS Gen2 - Gold

&#x20;       ↓

Azure Synapse Analytics / Azure SQL

&#x20;       ↓

Power BI



Airflow will be used for workflow orchestration.



\## Technologies



\- Azure Data Factory

\- Azure Data Lake Storage Gen2

\- Azure Databricks

\- PySpark

\- Azure Synapse Analytics

\- Azure SQL

\- Apache Airflow

\- SQL

\- Power BI

\- Python

\- Git \& GitHub

\- Docker



\## Architecture Pattern



The project follows the Medallion Architecture:



\### Bronze Layer



Stores raw data with minimal transformation.



\### Silver Layer



Contains cleaned, validated, and transformed data.



\### Gold Layer



Contains business-ready analytical datasets for Synapse/Azure SQL and Power BI.



\## Data Source



NYC Yellow Taxi Trips 2024 Aggregated Dataset.



The dataset is used for educational and portfolio purposes.



\## Key Analytics



The pipeline will support analytics such as:



\- Total taxi trips

\- Total revenue

\- Average fare

\- Average trip distance

\- Trips by hour

\- Trips by day

\- Monthly revenue trends

\- Peak travel periods

\- Payment type analysis

\- Pickup and drop-off analysis

\- Revenue by location

\- Distance versus fare analysis



\## Project Structure



```text

nyc-taxi-azure-data-engineering/

│

├── data/

│   ├── raw/

│   ├── bronze/

│   ├── silver/

│   └── gold/

│

├── notebooks/

├── pyspark/

├── sql/

├── airflow/

├── powerbi/

├── architecture/

├── docs/

├── config/

│

├── README.md

├── requirements.txt

└── .gitignore

