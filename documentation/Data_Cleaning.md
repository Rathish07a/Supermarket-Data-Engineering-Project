# Data Cleaning and Transformation

## Overview

The Silver Layer is responsible for transforming raw datasets into clean, consistent, and business-ready data. Data quality issues such as duplicate records, missing values, invalid values, inconsistent data types, and formatting inconsistencies are identified and resolved during this stage.

The transformations are implemented using **PySpark** in **Azure Databricks**, and the cleaned datasets are stored in the **Silver Layer** in Delta format. These datasets serve as the foundation for business aggregations and analytics in the Gold Layer.

---

## Data Quality Objectives

The primary objectives of the data cleaning process are:

- Remove duplicate records
- Handle missing values
- Validate business data
- Standardize data types
- Maintain referential integrity
- Improve data consistency
- Prepare datasets for analytical processing

---

# Data Cleaning Process

## 1. Reading Bronze Layer Data

The transformation process begins by reading Delta tables from the Bronze Layer.

### Sample Implementation

```python
df = spark.read.format("delta").load(bronze_path)
```

---

## 2. Duplicate Record Removal

Duplicate records can result in incorrect reporting and inaccurate business metrics. Duplicate rows are removed before performing further transformations.

### Sample Implementation

```python
df = df.dropDuplicates()
```

---

## 3. Missing Value Handling

Missing values were identified in several datasets. Examples include:

- Manager Name
- Contact Person
- Delivery Date (for cancelled purchase orders)

Missing values are handled using appropriate business rules while preserving data integrity.

### Sample Implementation

```python
from pyspark.sql.functions import col, when

df = df.withColumn(
    "manager_name",
    when(col("manager_name").isNull(), "Unknown")
    .otherwise(col("manager_name"))
)
```

---

## 4. Invalid Value Validation

Business validation rules are applied to identify invalid numerical values.

Examples include:

- Negative Unit Prices
- Negative Inventory Quantities
- Invalid Sales Amounts

Records violating business rules are corrected or filtered before loading into the Silver Layer.

### Sample Implementation

```python
from pyspark.sql.functions import col

df = df.filter(col("unit_price") >= 0)
```

---

## 5. Data Type Standardization

To ensure consistency across datasets, columns are converted to their appropriate data types.

Examples include:

- Date → Date
- Integer IDs → Integer
- Monetary values → Decimal

### Sample Implementation

```python
from pyspark.sql.functions import col

df = df.withColumn(
    "transaction_date",
    col("transaction_date").cast("date")
)
```

---

## 6. Date Validation

Date columns are validated to ensure logical consistency.

Validation checks include:

- Valid date formats
- Transaction dates
- Delivery dates occurring after order dates

This ensures accurate reporting and reliable business analysis.

---

## 7. Referential Integrity Validation

Relationships between datasets are validated before loading into the Silver Layer.

Examples include:

- Store ID exists in Stores table
- Product ID exists in Products table
- Supplier ID exists in Suppliers table

These validations help maintain consistency across all business entities.

### Sample Implementation

```python
validated_df = sales_df.join(
    stores_df,
    "store_id",
    "inner"
)
```

---

## 8. Data Standardization

Text columns are standardized to improve consistency.

Common transformations include:

- Removing leading and trailing spaces
- Standardizing text formatting
- Normalizing categorical values

### Sample Implementation

```python
from pyspark.sql.functions import trim

df = df.withColumn(
    "store_name",
    trim(col("store_name"))
)
```

---

## 9. Writing Data to the Silver Layer

After completing all validation and transformation steps, the cleaned dataset is written to the Silver Layer in Delta format.

### Sample Implementation

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)
```

---

# Silver Layer Workflow

The overall transformation process follows these steps:

1. Read Bronze Layer Delta tables.
2. Remove duplicate records.
3. Handle missing values.
4. Validate numerical values.
5. Standardize data types.
6. Validate business relationships.
7. Apply data standardization rules.
8. Write cleaned data to the Silver Layer.

---

# Benefits of Data Cleaning

The Silver Layer provides several benefits:

- Improved data quality
- Reliable business reporting
- Consistent data formats
- Reduced duplicate records
- Better analytical performance
- Accurate KPI calculations
- Enhanced downstream processing

---

# Outcome

After completing the transformation process, the Silver Layer contains clean, validated, and standardized datasets that are ready for business aggregations in the Gold Layer.

These datasets are subsequently used to generate:

- Sales Performance datasets
- Inventory Analysis datasets
- Supplier Performance datasets
- Business KPI reports
- Analytical reports for decision-making

The Silver Layer serves as the trusted source for all downstream business analytics within the project.