# Databricks notebook source
# MAGIC %md
# MAGIC # Inventory Optimizer
# MAGIC
# MAGIC ## Business Objective
# MAGIC Generate inventory and supplier performance reports from the Gold layer to support inventory optimization, stock planning, and supplier performance evaluation.
# MAGIC
# MAGIC ## Reports Generated
# MAGIC - Low Stock Report
# MAGIC - Inventory Velocity Report
# MAGIC - Overstock Analysis Report
# MAGIC - Inventory Replenishment Report
# MAGIC - Supplier Delivery Performance
# MAGIC - Supplier Reliability Analysis
# MAGIC - Supplier Performance Scorecard
# MAGIC
# MAGIC ## Output
# MAGIC Business-ready CSV reports stored in the Analytics layer for inventory management and procurement analysis.

# COMMAND ----------

# ==========================================================
# Import Libraries
# ==========================================================

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

# ==========================================================
# ADLS Gen2 Configuration
# ==========================================================

storage_account_name = "stsupermarketrath004"
# Storage authentication configured in Azure Databricks.
# Credentials removed for security reasons.
storage_account_key = "your_storage_account_key"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
print("Connected to ADLS Gen2 successfully!")

# COMMAND ----------

# ==========================================================
# Storage paths
# ==========================================================

analytics_path = f"abfss://analytics@{storage_account_name}.dfs.core.windows.net/"
gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

# ==========================================================
# Read Gold Tables
# ==========================================================

gold_inventory_df = spark.read.format("delta").load(
    f"{gold_path}gold_inventory_analysis"
)

gold_supplier_df = spark.read.format("delta").load(
    f"{gold_path}gold_supplier_performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 1 - Low Stock Report
# MAGIC
# MAGIC ## Business Objective
# MAGIC Identify products with low inventory levels to support timely replenishment and prevent stock shortages.
# MAGIC
# MAGIC ## Output
# MAGIC low_stock_report.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 1 - Low Stock Report
# ==========================================================

low_stock_report_df = (
    gold_inventory_df
    .filter(col("quantity_on_hand") < 50)
    .select(
        "inventory_id",
        "store_id",
        "product_id",
        "product_name",
        "category",
        "quantity_on_hand",
        "reorder_point",
        "stock_status"
    )
    .orderBy("quantity_on_hand")
)

display(low_stock_report_df)

# COMMAND ----------

# ==========================================================
# Export Low Stock Report
# ==========================================================

low_stock_report_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/low_stock_report")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 2 - Inventory Velocity Report
# MAGIC
# MAGIC ## Business Objective
# MAGIC Analyze inventory movement by identifying fast, medium, and slow-moving products based on sales velocity.
# MAGIC
# MAGIC ## Output
# MAGIC inventory_velocity_report.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 2 - Inventory Velocity Report
# ==========================================================

inventory_velocity_report_df = (
    gold_inventory_df
    .select(
        "inventory_id",
        "store_id",
        "product_id",
        "product_name",
        "category",
        "total_quantity_sold",
        "quantity_on_hand",
        "sales_velocity"
    )
    .orderBy(desc("total_quantity_sold"))
)

display(inventory_velocity_report_df)

# COMMAND ----------

# ==========================================================
# Export Inventory Velocity Report
# ==========================================================

inventory_velocity_report_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/inventory_velocity_report")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 3 - Overstock Analysis Report
# MAGIC
# MAGIC ## Business Objective
# MAGIC Identify products that are overstocked to support inventory optimization and reduce carrying costs.
# MAGIC
# MAGIC ## Output
# MAGIC overstock_analysis_report.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 3 - Overstock Analysis Report
# ==========================================================

overstock_analysis_report_df = (
    gold_inventory_df
    .filter(col("overstock_candidate") == "Yes")
    .select(
        "inventory_id",
        "store_id",
        "product_id",
        "product_name",
        "category",
        "quantity_on_hand",
        "total_quantity_sold",
        "overstock_candidate"
    )
)

display(overstock_analysis_report_df)

# COMMAND ----------

# ==========================================================
# Export Overstock Analysis Report
# ==========================================================

overstock_analysis_report_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/overstock_analysis_report")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 4 - Inventory Replenishment Report
# MAGIC
# MAGIC ## Business Objective
# MAGIC Identify products that require replenishment based on current stock levels and predefined reorder points.
# MAGIC
# MAGIC ## Output
# MAGIC inventory_replenishment_report.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 4 - Inventory Replenishment Report
# ==========================================================

inventory_replenishment_report_df = (
    gold_inventory_df
    .filter(col("quantity_on_hand") <= col("reorder_point"))
    .select(
        "inventory_id",
        "store_id",
        "product_id",
        "product_name",
        "category",
        "quantity_on_hand",
        "reorder_point",
        "stock_status"
    )
    .orderBy("quantity_on_hand")
)

display(inventory_replenishment_report_df)

# COMMAND ----------

# ==========================================================
# Export Inventory Replenishment Report
# ==========================================================

inventory_replenishment_report_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/inventory_replenishment_report")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 5 - Supplier Delivery Performance
# MAGIC
# MAGIC ## Business Objective
# MAGIC Evaluate supplier delivery efficiency based on average delivery time and on-time delivery performance.
# MAGIC
# MAGIC ## Output
# MAGIC supplier_delivery_performance.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 5 - Supplier Delivery Performance
# ==========================================================

supplier_delivery_performance_df = (
    gold_supplier_df
    .select(
        "supplier_id",
        "supplier_name",
        "contact_person",
        "total_orders",
        "average_delivery_days",
        "on_time_deliveries",
        "late_deliveries"
    )
    .orderBy("average_delivery_days")
)

display(supplier_delivery_performance_df)

# COMMAND ----------

# ==========================================================
# Export Supplier Delivery Performance
# ==========================================================

supplier_delivery_performance_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/supplier_delivery_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 6 - Supplier Reliability Analysis
# MAGIC
# MAGIC ## Business Objective
# MAGIC Analyze supplier reliability based on on-time delivery performance and supplier ratings.
# MAGIC
# MAGIC ## Output
# MAGIC supplier_reliability_analysis.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 6 - Supplier Reliability Analysis
# ==========================================================

supplier_reliability_analysis_df = (
    gold_supplier_df
    .select(
        "supplier_id",
        "supplier_name",
        "on_time_delivery_percentage",
        "supplier_rating",
        "supplier_rank"
    )
    .orderBy("supplier_rank")
)

display(supplier_reliability_analysis_df)

# COMMAND ----------

# ==========================================================
# Export Supplier Reliability Analysis
# ==========================================================

supplier_reliability_analysis_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/supplier_reliability_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inventory Report 7 - Supplier Performance Scorecard
# MAGIC
# MAGIC ## Business Objective
# MAGIC Generate an overall supplier performance scorecard by consolidating key supplier KPIs for procurement analysis and performance evaluation.
# MAGIC
# MAGIC ## Output
# MAGIC supplier_performance_scorecard.csv

# COMMAND ----------

# ==========================================================
# Inventory Report 7 - Supplier Performance Scorecard
# ==========================================================

supplier_performance_scorecard_df = (
    gold_supplier_df
    .select(
        "supplier_id",
        "supplier_name",
        "contact_person",
        "total_orders",
        "average_delivery_days",
        "on_time_delivery_percentage",
        "supplier_rating",
        "supplier_rank"
    )
    .orderBy("supplier_rank")
)

display(supplier_performance_scorecard_df)

# COMMAND ----------

# ==========================================================
# Export Supplier Performance Scorecard
# ==========================================================

supplier_performance_scorecard_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}inventory_reports/supplier_performance_scorecard")