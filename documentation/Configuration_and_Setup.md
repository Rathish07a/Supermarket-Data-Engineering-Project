# Configuration and Setup Guide

# Overview

This document explains how to configure and execute the **Supermarket Data Engineering Project**. The project implements an end-to-end Azure Data Engineering pipeline using Azure services and follows the Medallion Architecture (Landing, Bronze, Silver, and Gold).

The pipeline performs the following tasks:

- Ingest raw CSV files from GitHub
- Store raw data in Azure Data Lake Storage Gen2
- Transform data using Azure Databricks and PySpark
- Build business-ready Gold Layer datasets
- Generate analytical reports using SQL and Python

---

# Technology Stack

| Technology | Purpose |
|------------|----------|
| Azure Data Factory | Data Ingestion & Orchestration |
| Azure Data Lake Storage Gen2 | Data Storage |
| Azure Databricks | Data Transformation |
| PySpark | Data Processing |
| Delta Lake | Storage Format |
| SQL | Business Analysis |
| Python | Report Generation |
| GitHub | Source Data Repository |
| Visual Studio Code | Development Environment |

---

# Prerequisites

Before running the project, ensure the following software and services are available:

## Azure Services

- Azure Subscription
- Azure Resource Group
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks Workspace

## Development Tools

- Visual Studio Code
- Git
- Python 3.x

## Accounts

- GitHub Account
- Microsoft Azure Account

---

# Project Structure

```
Supermarket-Data-Engineering-Project/
│
├── adf/
├── data/
├── database/
├── documentation/
├── images/
├── outputs/
├── scripts/
└── README.md
```

---

# Azure Resource Setup

## Step 1 – Create a Resource Group

Create a Resource Group to hold all Azure resources.

Example:

```
Supermarket-Data-Engineering-RG
```

---

## Step 2 – Create Azure Data Lake Storage Gen2

Create a Storage Account with **Hierarchical Namespace Enabled**.

After creating the storage account, create the following containers:

```
landing
bronze
silver
gold
analytics
```

Each container represents one stage of the Medallion Architecture.

---

## Step 3 – Create Azure Data Factory

Create a new Azure Data Factory instance.

Example:

```
Supermarket-ADF
```

Azure Data Factory is responsible for:

- Reading metadata
- Downloading CSV files
- Loading data into ADLS
- Executing Databricks notebooks

---

## Step 4 – Create Azure Databricks Workspace

Create a new Azure Databricks Workspace.

Inside the workspace:

- Create a Compute Cluster
- Import all notebooks from the **scripts/notebooks** folder

---

# Configure Azure Data Lake

Inside the Storage Account create the following directory structure.

```
landing/

bronze/

silver/

gold/

analytics/
```

The data flows through these layers as shown below.

```
GitHub

↓

Landing

↓

Bronze

↓

Silver

↓

Gold

↓

Reports
```

---

# Configure Linked Services

Azure Data Factory requires Linked Services to connect to external systems.

## HTTP Linked Service

Purpose:

Reads CSV files and metadata from GitHub.

---

## Azure Data Lake Linked Service

Purpose:

Reads and writes data to Azure Data Lake Storage Gen2.

---

## Azure Databricks Linked Service

Purpose:

Executes Databricks notebooks from Azure Data Factory.

---

# Configure Datasets

Create parameterized datasets.

Required datasets include:

- GitHub CSV Dataset
- Metadata JSON Dataset
- Landing Dataset

Parameterization allows multiple files to be processed using the same pipeline.

---

# Metadata Configuration

The project uses metadata-driven ingestion.

Metadata file:

```
data/metadata/files.json
```

Example:

```json
[
  {
    "filename": "products.csv",
    "folder": "raw"
  },
  {
    "filename": "stores.csv",
    "folder": "raw"
  }
]
```

Azure Data Factory reads this metadata to determine which files to ingest.

---

# Azure Data Factory Pipeline

The ingestion pipeline performs the following operations.

```
Lookup Activity

↓

ForEach Activity

↓

Copy Activity

↓

Landing Container
```

Pipeline Steps

1. Read metadata JSON.
2. Iterate through all files.
3. Download CSV files from GitHub.
4. Store them in Landing.

---

# Azure Databricks Configuration

Import the following notebooks.

```
01_Bronze_Transformation

02_Silver_Transformation

03_Gold_Transformation

04_Data_Visualization
```

---

# Notebook Execution Order

Execute notebooks in the following order.

```
Landing

↓

01_Bronze_Transformation

↓

02_Silver_Transformation

↓

03_Gold_Transformation

↓

04_Data_Visualization

↓

05_Sales_Reporter

↓

06_Inventory_Optimizer
```

---

# Bronze Layer

Purpose

- Read CSV files
- Preserve raw data
- Convert CSV into Delta format

Output

```
landing/

↓

bronze/
```

---

# Silver Layer

Purpose

- Remove duplicate records
- Handle missing values
- Validate data
- Standardize formats
- Fix invalid records

Output

```
bronze/

↓

silver/
```

---

# Gold Layer

Purpose

Generate business-ready datasets.

Examples

- Sales Performance
- Product Performance
- Inventory Analysis
- Supplier Performance

Output

```
silver/

↓

gold/
```

---

# SQL Analysis

The **database** folder contains SQL scripts.

```
sales_analysis.sql

inventory_analysis.sql

comprehensive_kpi_queries.sql
```

These queries generate business insights from Gold Layer tables.

---

# Python Report Generation

Python scripts generate business reports.

```
05_Sales_Reporter.py

06_Inventory_Optimizer.py
```

Generated reports include:

- Sales Performance
- Product Performance
- Inventory Optimization
- Supplier Analysis

---

# Expected Outputs

After successful execution the following outputs are generated.

## Landing

Raw CSV files.

---

## Bronze

Raw Delta tables.

---

## Silver

Cleaned Delta tables.

---

## Gold

Business-ready Delta tables.

---

## Reports

CSV reports.

Examples:

```
Sales Summary

Top Products

Store Performance

Inventory Report

Supplier Performance
```

---

## Visualizations

Charts generated from Gold Layer datasets.

---

# Troubleshooting

## Pipeline Failure

Check:

- Linked Services
- Dataset Parameters
- GitHub URLs
- Storage Permissions

---

## Databricks Errors

Verify:

- Cluster is running
- Storage paths are correct
- Containers exist

---

## Delta Errors

Ensure:

- Destination path exists
- Existing files are Delta format
- Correct write mode is used

---

## Missing Files

Verify:

- Metadata file
- Landing container
- GitHub repository paths

---

# Best Practices

- Keep Landing Layer unchanged.
- Perform all cleaning in Silver Layer.
- Store only business-ready data in Gold Layer.
- Use Delta format for all transformed datasets.
- Use parameterized pipelines.
- Use metadata-driven ingestion.
- Keep notebooks modular.
- Maintain project documentation.

---

# Conclusion

The Supermarket Data Engineering Project demonstrates an end-to-end Azure Data Engineering solution using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, SQL, and Python.

The project follows industry-standard Medallion Architecture and metadata-driven ingestion to build a scalable, maintainable, and production-oriented data pipeline. It transforms raw supermarket data into high-quality analytical datasets that support sales analysis, inventory optimization, supplier performance evaluation, and business decision-making.