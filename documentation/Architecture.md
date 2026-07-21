# Solution Architecture

## Overview

The Supermarket Data Engineering Project implements a modern cloud-based data engineering architecture using Microsoft Azure services. The solution is designed to ingest raw supermarket datasets, transform them into high-quality business-ready data, and generate analytical reports that support business decision-making.

The architecture follows the Medallion Architecture (Bronze, Silver, and Gold), which organizes data into multiple processing layers to improve data quality, maintainability, and scalability.

---

## Architecture Diagram

<p align="center">
    <img src="../images/architecture_diagram.png" alt="Solution Architecture" width="1000"/>
</p>

---

## Architecture Components

### GitHub Repository

The source CSV files and metadata configuration file are stored in a GitHub repository. The metadata file controls which datasets are ingested into Azure Data Lake Storage.

---

### Azure Data Factory

Azure Data Factory (ADF) is responsible for orchestrating the data ingestion process.

The ingestion pipeline uses:

- Lookup Activity
- ForEach Activity
- Parameterized Datasets
- Copy Activity

This metadata-driven approach enables scalable and reusable data ingestion without creating separate pipelines for each dataset.

---

### Azure Data Lake Storage Gen2

Azure Data Lake Storage Gen2 acts as the centralized storage layer for the project.

The storage is organized into the following containers:

- Landing
- Bronze
- Silver
- Gold
- Analytics

Each container represents a different stage of the data engineering workflow.

---

### Azure Databricks

Azure Databricks performs distributed data processing using PySpark.

Separate notebooks are used for:

- Bronze Transformation
- Silver Transformation
- Gold Transformation
- Data Visualization
- Sales Reporting
- Inventory Optimization

---

## Medallion Architecture

### Landing Layer

- Stores raw CSV files copied from GitHub.
- Acts as the initial storage location before processing.

---

### Bronze Layer

- Reads raw CSV files from the Landing container.
- Converts the data into Delta format.
- Preserves the original source data.

---

### Silver Layer

The Silver layer performs data quality improvements including:

- Duplicate removal
- Missing value handling
- Data type standardization
- Invalid value correction
- Business rule validation

The resulting datasets are clean, consistent, and ready for business transformations.

---

### Gold Layer

The Gold layer creates business-ready datasets through aggregations and KPI calculations.

Gold tables include:

- Sales Performance
- Monthly Sales Performance
- Product Performance
- Inventory Analysis
- Supplier Performance

These datasets support reporting and business analytics.

---

### Analytics Layer

Dedicated analytics scripts generate:

- Sales Performance Reports
- Inventory Optimization Reports
- Business KPI Reports

The generated reports are exported as CSV files into the Analytics container.

---

## End-to-End Workflow

1. Raw CSV files are stored in GitHub.
2. Azure Data Factory reads the metadata file.
3. The ingestion pipeline copies the datasets into the Landing container.
4. Azure Databricks transforms the raw data into Delta format (Bronze).
5. Data cleansing and validation are performed in the Silver layer.
6. Business aggregations are created in the Gold layer.
7. SQL analysis and Python scripts generate analytical reports.
8. Final reports are stored in the Analytics container.

---

## Benefits of the Architecture

- Metadata-driven ingestion
- Scalable cloud-native design
- Modular processing pipeline
- Improved data quality
- Reliable Delta Lake storage
- Reusable Databricks notebooks
- Business-ready analytical datasets
- Support for future scalability and automation

---