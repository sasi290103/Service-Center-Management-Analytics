#  Service Center Management Analytics – DLT Pipeline Project

This project simulates a **real-world automotive service center** and showcases how to ingest and analyze operational data using **Databricks Delta Live Tables (DLT)**, synthetic datasets (generated via Faker), and **interactive dashboards**.

---

## Project Overview

The pipeline focuses on tracking and optimizing business operations such as:
- Customer visits and sales
- Vehicle servicing and parts
- Service convenience scoring by model and branch

Using **DLT pipelines**, we transform raw synthetic data into analytics-ready gold tables.

---

## Datasets Used (Generated using Faker)

| Dataset File Name                     | Description |
|--------------------------------------|-------------|
| `Malik_Customers_Data.csv`           | Customer demographics and contact info |
| `Malik_Motors_Sales.csv`             | Sales transactions including product, price, date |
| `Malik_motors_Service.csv`           | Service records for each vehicle and service type |
| `Malik_Motors_vehicle_data.csv`      | Vehicle details (model, manufacturer, fuel type) |
| `Model_Convenience_Score.csv`        | Convenience score for each vehicle model |
| `branch_convience_score.csv`         | Branch-wise convenience/service score |

>  These datasets are **synthetically generated** using the Python `faker` library and stored in CSV format.

---

##  Architecture: DLT Pipeline

- **Bronze Layer**: Ingest raw CSV files into Delta tables.
- **Silver Layer**: Clean, join, and normalize service and sales records.
- **Gold Layer**: Aggregate KPIs such as:
  - Average service duration by model
  - Sales performance by branch
  - Customer frequency and churn analysis

### Pipeline Flow

A visual representation of the Delta Live Tables pipeline is shown below:

![DLT Pipeline Graph](diagrams/dlt_pipeline_graph.png)

> *Replace with actual image path if different*

---

## 📊 Dashboards

The final gold tables are used to build dashboards that show:
- Daily/weekly service trends
- Model performance analysis
- Branch efficiency comparison
- Customer behavior insights

> Dashboards are created using **Databricks SQL** and **notebooks**.

---

## 🚀 How to Run the Project

1. Upload the synthetic CSV datasets to DBFS or a cloud bucket.
2. Create a Delta Live Table pipeline in Databricks workspace.
3. Use the following execution flow in notebooks:
   - `Bronze Ingestion`
   - `Silver Transformation`
   - `Gold Aggregation`
   - `Dashboard Notebook`
4. Visualize results in Databricks Dashboards.

---

## ⚙️ Tech Stack

- **Databricks**
- **Delta Live Tables (DLT)**
- **Apache Spark (Structured Streaming)**
- **Faker (for data generation)**
- **PySpark**
- **SQL Dashboards**

---

## 🙌 Credits

- **Author**: [Your Name]
- **Synthetic Data**: Generated using Python `faker`
- **Tools**: Databricks, Delta Lake, PySpark
