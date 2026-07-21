# Databricks notebook source
# MAGIC %md
# MAGIC # Data Visualization
# MAGIC
# MAGIC ## Business Objective
# MAGIC
# MAGIC Visualize business insights from the Gold layer using Pandas and Matplotlib.
# MAGIC
# MAGIC ### Objectives
# MAGIC
# MAGIC - Analyze sales performance
# MAGIC - Identify top-performing products
# MAGIC - Monitor inventory status
# MAGIC - Evaluate supplier performance
# MAGIC
# MAGIC ### Data Source
# MAGIC
# MAGIC - Gold Layer (Delta Tables)
# MAGIC
# MAGIC ### Tools
# MAGIC
# MAGIC - Pandas
# MAGIC - Matplotlib

# COMMAND ----------

# ==========================================================
# Import Required Libraries
# ==========================================================

import pandas as pd
import matplotlib.pyplot as plt

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
# Configure Storage Paths
# ==========================================================

storage_account = "stsupermarketrath004"
container_name = "gold"

gold_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"

# COMMAND ----------

# ==========================================================
# Read Gold Delta Tables
# ==========================================================

gold_sales_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_sales_performance")
)

gold_monthly_sales_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_monthly_sales_performance")
)

gold_products_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_product_performance")
)

gold_inventory_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_inventory_analysis")
)

gold_supplier_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_supplier_performance")
)

# COMMAND ----------

# ==========================================================
# Convert Spark DataFrames to Pandas
# ==========================================================

sales_pd = gold_sales_df.toPandas()
monthly_sales_pd = gold_monthly_sales_df.toPandas()
products_pd = gold_products_df.toPandas()
inventory_pd = gold_inventory_df.toPandas()
supplier_pd = gold_supplier_df.toPandas()

# COMMAND ----------

# ==========================================================
# Verify Data
# ==========================================================

print("Sales Shape:", sales_pd.shape)
print("Monthly_Sales Shape:", monthly_sales_pd.shape)
print("Products Shape:", products_pd.shape)
print("Inventory Shape:", inventory_pd.shape)
print("Supplier Shape:", supplier_pd.shape)

# COMMAND ----------

sales_pd.head()

# COMMAND ----------

monthly_sales_pd.head()

# COMMAND ----------

products_pd.head()

# COMMAND ----------

inventory_pd.head()

# COMMAND ----------

supplier_pd.head()

# COMMAND ----------

print(sales_pd.columns.tolist())

# COMMAND ----------

print(monthly_sales_pd.columns.tolist())

# COMMAND ----------

print(products_pd.columns.tolist())

# COMMAND ----------

print(inventory_pd.columns.tolist())


# COMMAND ----------

print(supplier_pd.columns.tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revenue by Store
# MAGIC
# MAGIC ### Business Question
# MAGIC Which store generated the highest revenue?
# MAGIC
# MAGIC PySpark/Pandas Transformation
# MAGIC Matplotlib Visualization
# MAGIC
# MAGIC Business Insight

# COMMAND ----------

# Sort stores by revenue (highest first)
revenue_by_store = sales_pd.sort_values(
    by="total_revenue",
    ascending=False
)

revenue_by_store

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))

plt.bar(
    revenue_by_store["store_name"],
    revenue_by_store["total_revenue"]
)

plt.title("Total Revenue by Store")
plt.xlabel("Store")
plt.ylabel("Revenue")
plt.xticks(rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Store Ranking by Revenue and Transaction Count
# MAGIC
# MAGIC ### Business Question
# MAGIC How does each store rank by revenue and transaction count?
# MAGIC
# MAGIC ### Objective
# MAGIC Compare the performance of each store based on revenue generated and the number of customer transactions.

# COMMAND ----------

store_ranking = sales_pd.sort_values(
    by="total_revenue",
    ascending=False
).reset_index(drop=True)

store_ranking

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

x = np.arange(len(store_ranking))
width = 0.35

plt.figure(figsize=(12,6))

plt.bar(
    x - width/2,
    store_ranking["total_revenue"],
    width,
    label="Revenue"
)

plt.bar(
    x + width/2,
    store_ranking["total_transactions"],
    width,
    label="Transactions"
)

plt.xticks(x, store_ranking["store_name"], rotation=45)

plt.title("Store Performance by Revenue and Transactions")
plt.xlabel("Store")
plt.ylabel("Value")
plt.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - This chart compares store performance using two key metrics:
# MAGIC   - Total Revenue
# MAGIC   - Total Transactions
# MAGIC - Stores with high revenue but fewer transactions may indicate a higher average transaction value.
# MAGIC - Stores with many transactions but lower revenue may benefit from strategies that increase basket size or promote higher-value products.

# COMMAND ----------

# MAGIC %md
# MAGIC # Month-on-Month Sales Trend
# MAGIC
# MAGIC ### Business Question
# MAGIC What is the month-on-month sales trend for each store?
# MAGIC
# MAGIC ### Objective
# MAGIC Analyze monthly revenue trends to identify seasonal patterns and sales performance over time.

# COMMAND ----------

import pandas as pd

monthly_sales_pd["month"] = pd.to_datetime(
    monthly_sales_pd["sales_year"].astype(str) + "-" +
    monthly_sales_pd["sales_month"].astype(str)
)

monthly_sales_pd["month"] = monthly_sales_pd["month"].dt.strftime("%Y-%m")

# COMMAND ----------

monthly_sales = (
    monthly_sales_pd
    .groupby("month", as_index=False)["total_revenue"]
    .sum()
)

monthly_sales

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))

plt.plot(
    monthly_sales["month"],
    monthly_sales["total_revenue"],
    marker="o"
)

plt.title("Month-on-Month Sales Trend")
plt.xlabel("Month")
plt.ylabel("Total Revenue")

plt.xticks(rotation=45)

plt.grid(True)

plt.tight_layout()

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - The line chart illustrates the monthly revenue trend across the analysis period.
# MAGIC - Peaks indicate months with stronger sales performance, while dips may represent seasonal slowdowns.
# MAGIC - This trend helps management forecast future demand, plan promotions, and optimize inventory.

# COMMAND ----------

# MAGIC %md
# MAGIC # Product Performance Analysis
# MAGIC
# MAGIC ## Top 5 Products by Quantity Sold and Revenue
# MAGIC
# MAGIC ### Business Questions
# MAGIC - What are the top 5 best-selling products by quantity?
# MAGIC - Which products generate the highest revenue?
# MAGIC
# MAGIC ### Objective
# MAGIC Identify the best-selling and highest-revenue products to support inventory planning and sales strategy.

# COMMAND ----------

top_quantity = (
    products_pd
    .sort_values(by="total_quantity_sold", ascending=False)
    .head(5)
)

# COMMAND ----------

top_revenue = (
    products_pd
    .sort_values(by="total_revenue", ascending=False)
    .head(5)
)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))

plt.barh(
    top_quantity["product_name"],
    top_quantity["total_quantity_sold"]
)

plt.title("Top 5 Products by Quantity Sold")
plt.xlabel("Quantity Sold")
plt.ylabel("Product")

plt.tight_layout()
plt.show()

# COMMAND ----------

from pyspark.sql.functions import sum, col

top_revenue = (
    spark.createDataFrame(products_pd)
    .groupBy("product_name")
    .agg(
        sum("total_revenue").alias("total_revenue")
    )
    .orderBy(col("total_revenue").desc())
    .limit(5)
    .toPandas()
)

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))

plt.barh(
    top_revenue["product_name"],
    top_revenue["total_revenue"]
)

plt.title("Top 5 Products by Revenue")
plt.xlabel("Revenue")
plt.ylabel("Product")

# Highest revenue at the top
plt.gca().invert_yaxis()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - The first chart highlights the products with the highest sales volume.
# MAGIC - The second chart identifies the products contributing the most revenue.
# MAGIC - A product may sell in high quantities but generate less revenue if its unit price is low.
# MAGIC - Comparing both charts helps management prioritize inventory, pricing, and promotional strategies.

# COMMAND ----------

# MAGIC %md
# MAGIC # Product Diversity by Store
# MAGIC
# MAGIC ### Business Question
# MAGIC Which store has the most diverse product sales?
# MAGIC
# MAGIC ### Objective
# MAGIC Determine how many unique products each store sold during the analysis period.

# COMMAND ----------

# Count unique products sold per store
product_diversity = (
    silver_sales_pd
    .groupby("store_id")["product_id"]
    .nunique()
    .reset_index(name="unique_products")
)

# Add store names
product_diversity = product_diversity.merge(
    sales_pd[["store_id", "store_name"]],
    on="store_id",
    how="left"
)

# Sort by highest diversity
product_diversity = product_diversity.sort_values(
    by="unique_products",
    ascending=False
)

product_diversity

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))

plt.bar(
    product_diversity["store_name"],
    product_diversity["unique_products"]
)

plt.title("Unique Products Sold by Store")
plt.xlabel("Store")
plt.ylabel("Number of Unique Products")

plt.xticks(rotation=45)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - This chart compares the number of unique products sold by each store.
# MAGIC - Stores with higher product diversity typically cater to a broader range of customer preferences.
# MAGIC - Management can use this information to optimize product assortment and identify opportunities to expand product offerings in lower-diversity stores.

# COMMAND ----------

# MAGIC %md
# MAGIC # Stock Status Distribution
# MAGIC
# MAGIC ### Business Question
# MAGIC Which products are currently Out of Stock, Low Stock, or In Stock?
# MAGIC
# MAGIC ### Objective
# MAGIC Visualize the current inventory status across all products to support inventory monitoring and replenishment decisions.

# COMMAND ----------

stock_status = (
    inventory_pd["stock_status"]
    .value_counts()
    .reset_index()
)

stock_status.columns = ["stock_status", "count"]

stock_status

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(7,7))

plt.pie(
    stock_status["count"],
    labels=stock_status["stock_status"],
    autopct="%1.1f%%",
    startangle=90
)

plt.title("Stock Status Distribution")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - The majority of products are currently **In Stock**, indicating healthy inventory availability.
# MAGIC - Products classified as **Low Stock** should be monitored and replenished soon.
# MAGIC - **Out of Stock** products require immediate procurement to prevent lost sales and maintain customer satisfaction.

# COMMAND ----------

# MAGIC %md
# MAGIC # Sales Velocity Distribution
# MAGIC
# MAGIC ### Business Question
# MAGIC Which products have the highest sales velocity?
# MAGIC
# MAGIC ### Objective
# MAGIC Visualize the distribution of products based on their sales velocity classification to identify fast-moving and slow-moving inventory.

# COMMAND ----------

sales_velocity = (
    inventory_pd["sales_velocity"]
    .value_counts()
    .reset_index()
)

sales_velocity.columns = ["sales_velocity", "count"]

sales_velocity

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(8,5))

plt.bar(
    sales_velocity["sales_velocity"],
    sales_velocity["count"]
)

plt.title("Sales Velocity Distribution")
plt.xlabel("Sales Velocity")
plt.ylabel("Number of Products")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - Fast-moving products experience high customer demand and require frequent replenishment.
# MAGIC - Medium-moving products have steady demand and should be monitored regularly.
# MAGIC - Slow-moving products may require promotional campaigns or inventory optimization to reduce holding costs.

# COMMAND ----------

# MAGIC %md
# MAGIC # Overstock Candidate Analysis
# MAGIC
# MAGIC ### Business Question
# MAGIC Which products are slow-moving and overstock candidates?
# MAGIC
# MAGIC ### Objective
# MAGIC Identify products with high inventory levels but low sales performance to support inventory optimization and reduce holding costs.

# COMMAND ----------

overstock_products = inventory_pd[
    inventory_pd["overstock_candidate"] == "Yes"
]

if overstock_products.empty:
    print("No overstock candidates identified in the current dataset.")
else:
    top_overstock = overstock_products.sort_values(
        by="quantity_on_hand",
        ascending=False
    ).head(10)

    plt.figure(figsize=(12,6))
    plt.barh(
        top_overstock["product_name"],
        top_overstock["quantity_on_hand"]
    )
    plt.title("Top 10 Overstock Candidate Products")
    plt.xlabel("Quantity on Hand")
    plt.ylabel("Product")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

inventory_pd["overstock_candidate"].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Reorder Point Analysis
# MAGIC
# MAGIC ### Business Question
# MAGIC Which products should be prioritized for replenishment?
# MAGIC
# MAGIC ### Objective
# MAGIC Compare current inventory with the reorder point to identify products that require immediate restocking.

# COMMAND ----------

reorder_products = (
    inventory_pd[
        inventory_pd["quantity_on_hand"] <= inventory_pd["reorder_point"]
    ]
    .sort_values(by="quantity_on_hand")
)

reorder_products

# COMMAND ----------

top_reorder = reorder_products.head(10)

import matplotlib.pyplot as plt

plt.figure(figsize=(12,6))

plt.barh(
    top_reorder["product_name"],
    top_reorder["quantity_on_hand"],
    label="Current Stock"
)

plt.barh(
    top_reorder["product_name"],
    top_reorder["reorder_point"],
    alpha=0.5,
    label="Reorder Point"
)

plt.title("Products Requiring Replenishment")
plt.xlabel("Quantity")
plt.ylabel("Product")
plt.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - Products with current stock below their reorder point should be replenished immediately.
# MAGIC - Maintaining inventory above the reorder point helps prevent stockouts and ensures uninterrupted product availability.
# MAGIC - Procurement teams can use this analysis to prioritize purchase orders and improve inventory planning.

# COMMAND ----------

# MAGIC %md
# MAGIC # Average Delivery Days by Supplier
# MAGIC
# MAGIC ### Business Question
# MAGIC Which suppliers have the shortest and longest average delivery times?
# MAGIC
# MAGIC ### Objective
# MAGIC Evaluate supplier efficiency by comparing their average delivery times to support procurement and supplier management decisions.

# COMMAND ----------

delivery_days = (
    supplier_pd
    .sort_values(
        by="average_delivery_days",
        ascending=True
    )
)

delivery_days

# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(10,6))

plt.barh(
    delivery_days["supplier_name"],
    delivery_days["average_delivery_days"]
)

plt.title("Average Delivery Days by Supplier")
plt.xlabel("Average Delivery Days")
plt.ylabel("Supplier")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - Suppliers with fewer average delivery days provide faster replenishment.
# MAGIC - Suppliers with higher delivery times may require closer monitoring or alternative sourcing strategies.
# MAGIC - Procurement teams can use this information to improve supplier selection and reduce inventory delays.

# COMMAND ----------

# MAGIC %md
# MAGIC # Supplier Reliability Analysis
# MAGIC
# MAGIC ### Business Question
# MAGIC Which suppliers consistently deliver orders on time?
# MAGIC
# MAGIC ### Objective
# MAGIC Compare supplier reliability using on-time deliveries, late deliveries, and on-time delivery percentage to support supplier evaluation and procurement decisions.

# COMMAND ----------

supplier_reliability = (
    supplier_pd
    .sort_values(
        by="on_time_delivery_percentage",
        ascending=False
    )
)

supplier_reliability

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

x = np.arange(len(supplier_reliability))
width = 0.35

plt.figure(figsize=(12,6))

plt.bar(
    x - width/2,
    supplier_reliability["on_time_deliveries"],
    width,
    label="On-Time Deliveries"
)

plt.bar(
    x + width/2,
    supplier_reliability["late_deliveries"],
    width,
    label="Late Deliveries"
)

plt.xticks(
    x,
    supplier_reliability["supplier_name"],
    rotation=45
)

plt.title("Supplier Reliability")
plt.xlabel("Supplier")
plt.ylabel("Number of Deliveries")

plt.legend()

plt.tight_layout()
plt.show()

# COMMAND ----------

plt.figure(figsize=(10,6))

plt.barh(
    supplier_reliability["supplier_name"],
    supplier_reliability["on_time_delivery_percentage"]
)

plt.title("On-Time Delivery Percentage by Supplier")
plt.xlabel("On-Time Delivery Percentage")
plt.ylabel("Supplier")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insight
# MAGIC
# MAGIC - Suppliers with a higher on-time delivery percentage are more reliable and help maintain a stable supply chain.
# MAGIC - Suppliers with frequent late deliveries may increase the risk of inventory shortages and operational delays.
# MAGIC - Procurement teams can use this analysis to monitor supplier performance, negotiate service-level agreements, and make informed sourcing decisions.

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC This dashboard analyzed supermarket sales, inventory, and supplier performance using the Medallion Architecture (Bronze, Silver, and Gold layers).
# MAGIC
# MAGIC Key insights include:
# MAGIC - Identified top-performing stores and products based on revenue and sales volume.
# MAGIC - Analyzed monthly sales trends using transaction-level data.
# MAGIC - Evaluated product diversity across stores.
# MAGIC - Monitored inventory health through stock status, sales velocity, overstock analysis, and reorder point evaluation.
# MAGIC - Assessed supplier performance using delivery times and reliability metrics.
# MAGIC
# MAGIC These insights support data-driven decision-making in sales, inventory management, and supplier evaluation while demonstrating an end-to-end Azure Data Engineering pipeline using Azure Data Factory, Azure Databricks, Delta Lake, and PySpark.