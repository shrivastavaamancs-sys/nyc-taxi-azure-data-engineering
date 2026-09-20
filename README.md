# NYC Taxi Azure Data Engineering Pipeline

## 📌 Project Overview
An end-to-end cloud-native **Data Engineering Pipeline** designed to ingest, transform, and analyze the large-scale **NYC Taxi Trip Dataset**. This project leverages **Azure, Apache Airflow, PySpark, SQL, and Power BI** to deliver automated ETL workflows and business intelligence analytics.

---

## 🏗️ Architecture & Data Flow
1. **Raw Data Storage:** Ingesting trip datasets into the `data/raw` storage layer.
2. **Workflow Orchestration:** Scheduling and running data pipelines using **Apache Airflow**.
3. **Data Transformation:** Executing distributed data transformations, data quality checks, and aggregations using **PySpark**.
4. **Data Warehousing:** Storing processed records in optimized **SQL** databases for analytical queries.
5. **Business Intelligence:** Creating visual dashboards and KPI reporting in **Power BI**.

---

## 🛠️ Tech Stack & Tools
* **Cloud Platform:** Microsoft Azure
* **Orchestration:** Apache Airflow
* **Big Data Processing:** PySpark, Python
* **Database & Querying:** SQL
* **Visualization & BI:** Power BI
* **Version Control:** Git, GitHub

---

## 📂 Repository Structure
```text
nyc-taxi-azure-data-engineering/
├── airflow/        # DAGs and pipeline orchestration workflows
├── architecture/   # System architecture diagrams & design docs
├── config/         # Pipeline configuration and environment files
├── data/
│   └── raw/        # Raw NYC Taxi trip datasets
├── docs/           # Project documentation and specifications
├── notebooks/      # Data exploration and transformation notebooks
├── powerbi/        # Power BI dashboard files (.pbix) and reports
├── pyspark/        # PySpark ETL transformation scripts
├── sql/            # SQL schema definitions and analytical queries
├── .gitignore      # Git ignore rules
├── README.md       # Project documentation
└── requirements.txt# Dependencies and libraries
