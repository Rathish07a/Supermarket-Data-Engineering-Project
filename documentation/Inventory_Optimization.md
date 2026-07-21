# Inventory Optimization

## Overview

The Inventory Optimization module analyzes inventory levels, product movement, and supplier performance to support efficient stock management across supermarket stores.

The analysis uses Gold Layer datasets generated from the Medallion Architecture. These datasets provide business-ready information for identifying low-stock products, monitoring inventory turnover, evaluating supplier performance, and improving procurement planning.

The generated reports help minimize stock shortages, reduce excess inventory, and improve supply chain efficiency.

---

# Objectives

The Inventory Optimization module aims to:

- Monitor inventory levels across stores.
- Identify low-stock products.
- Detect overstocked inventory.
- Analyze inventory turnover.
- Evaluate supplier delivery performance.
- Support inventory replenishment planning.

---

# Gold Layer Datasets

The analysis uses the following Gold Layer tables:

- Gold Inventory Analysis
- Gold Supplier Performance

These datasets are generated after applying business transformations and aggregations to the Silver Layer data.

---

# Key Performance Indicators (KPIs)

The Inventory Optimization module calculates several important KPIs:

- Current Stock Quantity
- Low Stock Products
- Overstocked Products
- Inventory Turnover
- Average Delivery Time
- Supplier Performance Score
- Supplier Reliability
- Inventory Replenishment Status

These KPIs assist inventory managers in maintaining optimal stock levels while improving supplier performance.

---

# Business Questions Addressed

The Inventory Optimization analysis answers the following business questions:

- Which products are running low on stock?
- Which products are overstocked?
- Which products have the highest inventory turnover?
- Which suppliers deliver products on time?
- Which suppliers have the best overall performance?
- Which products require immediate replenishment?

---

# Data Processing Workflow

The Inventory Optimization process follows these steps:

1. Read Gold Layer Delta tables.
2. Analyze inventory quantities.
3. Identify low-stock products.
4. Detect overstocked inventory.
5. Calculate supplier performance metrics.
6. Generate inventory reports.
7. Export reports as CSV files.

---

# Sample SQL Queries

### Low Stock Products

```sql
SELECT
    product_name,
    stock_quantity
FROM gold_inventory_analysis
WHERE stock_quantity < 50
ORDER BY stock_quantity;
```

---

### Supplier Performance

```sql
SELECT
    supplier_name,
    AVG(delivery_days) AS average_delivery_time
FROM gold_supplier_performance
GROUP BY supplier_name
ORDER BY average_delivery_time;
```

---

# Sample PySpark Implementation

Load the Gold Layer inventory data.

```python
inventory_df = spark.read.format("delta").load(gold_inventory_path)
```

Identify products with low inventory.

```python
from pyspark.sql.functions import col

low_stock_df = inventory_df.filter(
    col("stock_quantity") < 50
)
```

Calculate supplier performance metrics.

```python
supplier_summary = supplier_df.groupBy("supplier_name") \
    .avg("delivery_days")
```

---

# Generated Reports

The Inventory Optimization module generates the following reports:

- Low Stock Report
- Inventory Velocity Report
- Overstock Analysis Report
- Inventory Replenishment Report
- Supplier Delivery Performance
- Supplier Performance Scorecard
- Supplier Reliability Analysis

The reports are exported as CSV files and stored in the Analytics container.

---

# Business Benefits

The Inventory Optimization module helps organizations by:

- Preventing stock shortages.
- Reducing excess inventory.
- Improving inventory planning.
- Supporting procurement decisions.
- Monitoring supplier reliability.
- Optimizing warehouse operations.
- Improving customer satisfaction through better product availability.

---

# Outcome

The generated reports provide actionable insights into inventory and supplier performance.

Business users can proactively identify inventory risks, optimize replenishment strategies, evaluate supplier reliability, and maintain efficient stock levels across all stores.

The Inventory Optimization module demonstrates how data engineering enables operational excellence through accurate, timely, and business-ready inventory analytics.

---