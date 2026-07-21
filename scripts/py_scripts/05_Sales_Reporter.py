# Databricks notebook source
# MAGIC %md
# MAGIC # Sales Reporter
# MAGIC
# MAGIC ## Business Objective
# MAGIC Generate business-ready sales reports from the Gold layer to support sales analysis, performance monitoring, and strategic decision-making.
# MAGIC
# MAGIC ## Reports Generated
# MAGIC - Sales Performance Summary
# MAGIC - Top Store Performance
# MAGIC - Monthly Sales Trend
# MAGIC - Product Sales Summary
# MAGIC - Top Revenue Products
# MAGIC - Market Share Analysis
# MAGIC - Product Diversity Report
# MAGIC
# MAGIC ## Output
# MAGIC Business-ready CSV reports stored in the Analytics layer for management reporting and business analysis.

# COMMAND ----------

# ==========================================================
# Import Required Libraries
# ==========================================================

from pyspark.sql.functions import *
from pyspark.sql.types import *

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

gold_sales_df = spark.read.format("delta").load(
    f"{gold_path}gold_sales_performance"
)

gold_monthly_sales_df = spark.read.format("delta").load(
    f"{gold_path}gold_monthly_sales_performance"
)

gold_product_df = spark.read.format("delta").load(
    f"{gold_path}gold_product_performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 1 - Sales Performance Summary
# MAGIC
# MAGIC ## Business Objective
# MAGIC Generate a consolidated sales performance report for each store, including revenue, transactions, quantity sold, average transaction value, and sales ranking.
# MAGIC
# MAGIC ## Output
# MAGIC sales_performance_summary.csv

# COMMAND ----------

# ==========================================================
# Sales Report 1 - Sales Performance Summary
# ==========================================================

sales_performance_summary_df = (
    gold_sales_df.select(
        "store_id",
        "store_name",
        "location",
        "manager_name",
        "total_transactions",
        "total_quantity_sold",
        "total_revenue",
        "average_transaction_value",
        "store_rank"
    )
    .orderBy("store_rank")
)

display(sales_performance_summary_df)

# COMMAND ----------

# ==========================================================
# Export Sales Performance Summary
# ==========================================================

sales_performance_summary_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{analytics_path}sales_reports/sales_performance_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 2 - Top Store Performance
# MAGIC
# MAGIC ## Business Objective
# MAGIC Identify the top-performing stores based on total revenue to support business performance evaluation and strategic decision-making.
# MAGIC
# MAGIC ## Output
# MAGIC top_store_performance.csv

# COMMAND ----------

# ==========================================================
# Sales Report 2 - Top Store Performance
# ==========================================================

top_store_performance_df = (
    gold_sales_df
    .filter(col("store_rank") <= 5)
    .select(
        "store_id",
        "store_name",
        "location",
        "manager_name",
        "total_transactions",
        "total_quantity_sold",
        "total_revenue",
        "average_transaction_value",
        "store_rank"
    )
    .orderBy("store_rank")
)

display(top_store_performance_df)

# COMMAND ----------

# ==========================================================
# Export Top Store Performance Report
# ==========================================================

top_store_performance_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{analytics_path}sales_reports/top_store_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 3 - Monthly Sales Trend
# MAGIC
# MAGIC ## Business Objective
# MAGIC Analyze month-on-month sales performance across all stores to identify revenue trends and support business planning.
# MAGIC
# MAGIC ## Output
# MAGIC monthly_sales_trend.csv

# COMMAND ----------

# ==========================================================
# Sales Report 3 - Monthly Sales Trend
# ==========================================================

monthly_sales_trend_df = (
    gold_monthly_sales_df
    .groupBy(
        "sales_year",
        "sales_month"
    )
    .agg(
        sum("total_transactions").alias("total_transactions"),
        sum("total_quantity_sold").alias("total_quantity_sold"),
        round(sum("total_revenue"), 2).alias("total_revenue"),
        round(avg("average_transaction_value"), 2).alias("average_transaction_value")
    )
    .orderBy("sales_year", "sales_month")
)

display(monthly_sales_trend_df)

# COMMAND ----------

# ==========================================================
# Export Monthly Sales Trend Report
# ==========================================================

monthly_sales_trend_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(f"{analytics_path}sales_reports/monthly_sales_trend")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 4 - Product Sales Summary
# MAGIC
# MAGIC ## Business Objective
# MAGIC Generate a comprehensive product-wise sales report to evaluate product performance based on revenue and quantity sold.
# MAGIC
# MAGIC ## Output
# MAGIC product_sales_summary.csv

# COMMAND ----------

# ==========================================================
# Sales Report 4 - Product Sales Summary
# ==========================================================

product_sales_summary_df = (
    gold_product_df
    .select(
        "product_id",
        "product_name",
        "category",
        "total_quantity_sold",
        "total_revenue",
        "average_unit_price"
    )
    .orderBy(desc("total_revenue"))
)

display(product_sales_summary_df)

# COMMAND ----------

# ==========================================================
# Export Product Sales Summary
# ==========================================================

product_sales_summary_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}sales_reports/product_sales_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 5 - Top Revenue Products
# MAGIC
# MAGIC ## Business Objective
# MAGIC Identify the highest revenue-generating products to support product performance analysis.
# MAGIC
# MAGIC ## Output
# MAGIC top_revenue_products.csv

# COMMAND ----------

# ==========================================================
# Sales Report 5 - Top Revenue Products
# ==========================================================

top_revenue_products_df = (
    gold_product_df
    .orderBy(desc("total_revenue"))
    .limit(10)
)

display(top_revenue_products_df)

# COMMAND ----------

# ==========================================================
# Export Top Revenue Products
# ==========================================================

top_revenue_products_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}sales_reports/top_revenue_products")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 6 - Market Share Analysis
# MAGIC
# MAGIC ## Business Objective
# MAGIC Analyze the percentage contribution of each store to the organization's overall revenue.
# MAGIC
# MAGIC ## Output
# MAGIC market_share_analysis.csv

# COMMAND ----------

# ==========================================================
# Sales Report 6 - Market Share Analysis
# ==========================================================
from pyspark.sql.window import Window
market_share_analysis_df = (
    gold_sales_df
    .withColumn(
        "market_share_percentage",
        round(
            col("total_revenue") /
            sum("total_revenue").over(Window.partitionBy()) * 100,
            2
        )
    )
    .select(
        "store_id",
        "store_name",
        "total_revenue",
        "market_share_percentage"
    )
    .orderBy(desc("market_share_percentage"))
)

display(market_share_analysis_df)

# COMMAND ----------

# ==========================================================
# Export Market Share Analysis
# ==========================================================

market_share_analysis_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}sales_reports/market_share_analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Report 7 - Product Diversity Report
# MAGIC
# MAGIC ## Business Objective
# MAGIC Analyze the diversity of products sold across stores to understand product variety and customer demand.
# MAGIC
# MAGIC ## Output
# MAGIC product_diversity_report.csv

# COMMAND ----------

# ==========================================================
# Sales Report 7 - Product Diversity Report
# ==========================================================

product_diversity_report_df = (
    gold_sales_df
    .select(
        "store_id",
        "store_name",
        "distinct_products_sold"
    )
    .orderBy(desc("distinct_products_sold"))
)

display(product_diversity_report_df)

# COMMAND ----------

# ==========================================================
# Export Product Diversity Report
# ==========================================================

product_diversity_report_df.coalesce(1) \
.write \
.mode("overwrite") \
.option("header","true") \
.csv(f"{analytics_path}sales_reports/product_diversity_report")