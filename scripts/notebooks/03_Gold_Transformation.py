# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Transformation
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook transforms validated Silver Delta tables into business-ready Gold Delta tables.
# MAGIC
# MAGIC ### Gold Deliverables
# MAGIC
# MAGIC - Sales Performance
# MAGIC - Product Performance
# MAGIC - Inventory Analysis
# MAGIC - Supplier Performance
# MAGIC - Store Dashboard
# MAGIC - Executive Dashboard
# MAGIC
# MAGIC All Gold tables are optimized for business reporting and analytical workloads.

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
# Storage Paths
# ==========================================================

gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

display(dbutils.fs.ls(silver_path))

# COMMAND ----------

silver_sales_df = spark.read.format("delta").load(f"{silver_path}salestransactions")

silver_products_df = spark.read.format("delta").load(f"{silver_path}products")

silver_stores_df = spark.read.format("delta").load(f"{silver_path}stores")

silver_inventory_df = spark.read.format("delta").load(f"{silver_path}inventory")

silver_suppliers_df = spark.read.format("delta").load(f"{silver_path}suppliers")

silver_PurchaseOrders_df = spark.read.format("delta").load(f"{silver_path}purchaseorders")

# COMMAND ----------

silver_sales_df.createOrReplaceTempView("sales")
silver_products_df.createOrReplaceTempView("products")
silver_stores_df.createOrReplaceTempView("stores")
silver_inventory_df.createOrReplaceTempView("inventory")
silver_suppliers_df.createOrReplaceTempView("suppliers")
silver_PurchaseOrders_df.createOrReplaceTempView("purchase_orders")

# COMMAND ----------

display(silver_sales_df)

# COMMAND ----------

silver_sales_df.printSchema()

# COMMAND ----------

display(silver_sales_df.limit(10))

# COMMAND ----------


display(silver_products_df)

display(silver_stores_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 1 - Sales Performance
# MAGIC
# MAGIC ## Business Objective
# MAGIC
# MAGIC Generate store-level sales KPIs for business reporting.
# MAGIC
# MAGIC ### Business Questions Covered
# MAGIC
# MAGIC - Which store has the highest revenue?
# MAGIC - How does each store rank by revenue?
# MAGIC - What is the average transaction value?
# MAGIC - How many transactions were completed per store?
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC Business-ready Gold Delta table for dashboards and reporting.

# COMMAND ----------



# COMMAND ----------

# ==========================================================
# Sales Performance
# ==========================================================

gold_sales_performance_df = (
    silver_sales_df
    .groupBy("store_id")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("quantity_sold").alias("total_quantity_sold"),
        sum("total_amount").alias("total_revenue"),
        round(avg("total_amount"), 2).alias("average_order_value")
    )
)

display(gold_sales_performance_df)

# COMMAND ----------

# ==========================================================
# Gold Sales Performance (Store Summary)
# ==========================================================
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

gold_sales_performance_df = (
    silver_sales_df.alias("sales")
    .join(
        silver_stores_df.alias("stores"),
        col("sales.store_id") == col("stores.store_id"),
        "inner"
    )
    .groupBy(
        col("sales.store_id"),
        col("stores.store_name"),
        col("stores.location"),
        col("stores.manager_name")
    )
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("quantity_sold").alias("total_quantity_sold"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("average_transaction_value"),
        countDistinct("product_id").alias("distinct_products_sold")
    )
)

# COMMAND ----------

# ==========================================================
# Add Store Revenue Ranking
# ==========================================================

store_rank_window = Window.orderBy(desc("total_revenue"))

gold_sales_performance_df = (
    gold_sales_performance_df
    .withColumn(
        "store_rank",
        dense_rank().over(store_rank_window)
    )
)
display(gold_sales_performance_df)

# COMMAND ----------

# ==========================================================
# Write Gold Sales Performance
# ==========================================================

gold_sales_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}gold_sales_performance")

# COMMAND ----------

# ==========================================================
# Validate Gold Sales Performance
# ==========================================================

gold_sales_performance_validation_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_sales_performance")
)

print(f"Total Stores : {gold_sales_performance_validation_df.count()}")

display(gold_sales_performance_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 2 - Monthly Sales Performance
# MAGIC
# MAGIC ## Business Objective
# MAGIC Generate month-wise store sales KPIs to analyze sales trends and monitor business performance over time.
# MAGIC
# MAGIC ## Business Questions Covered
# MAGIC - How does each store's revenue change month over month?
# MAGIC - What is the monthly sales trend for each store?
# MAGIC - How many transactions were completed each month per store?
# MAGIC - What is the monthly average transaction value for each store?
# MAGIC
# MAGIC ## Output
# MAGIC Business-ready Gold Delta table for monthly sales trend analysis, executive reporting, and time-series dashboards.

# COMMAND ----------

# ==========================================================
# Gold Monthly Sales Performance Transformation
# ==========================================================

from pyspark.sql.functions import *

gold_monthly_sales_performance_df = (
    silver_sales_df.alias("sales")
    .join(
        silver_stores_df.alias("stores"),
        col("sales.store_id") == col("stores.store_id"),
        "inner"
    )
    .groupBy(
        col("sales.store_id"),
        col("stores.store_name"),
        col("stores.location"),
        col("stores.manager_name"),
        year(col("sales.sale_date")).alias("sales_year"),
        month(col("sales.sale_date")).alias("sales_month")
    )
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("quantity_sold").alias("total_quantity_sold"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("average_transaction_value")
    )
)

display(gold_monthly_sales_performance_df)

# COMMAND ----------

# ==========================================================
# Write Gold Monthly Sales Performance
# ==========================================================

gold_monthly_sales_performance_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}gold_monthly_sales_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 3 - Product Performance
# MAGIC
# MAGIC ## Business Objective
# MAGIC
# MAGIC Generate product-level sales insights to identify high-performing products and support merchandising decisions.
# MAGIC
# MAGIC ### Business Questions Covered
# MAGIC
# MAGIC - What are the top 5 best-selling products by quantity?
# MAGIC - Which products generate the highest revenue?
# MAGIC - Which products are sold across the most stores?
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC Business-ready Gold Delta table for product performance reporting and analytics.

# COMMAND ----------

sales = silver_sales_df.alias("sales")
products = silver_products_df.alias("products")
stores = silver_stores_df.alias("stores")
inventory = silver_inventory_df.alias("inventory")
suppliers = silver_suppliers_df.alias("suppliers")
purchase_orders = silver_PurchaseOrders_df.alias("purchase_orders")

# COMMAND ----------

# ==========================================================
# Gold Product Performance Transformation
# ==========================================================

gold_product_performance_df = (
    sales
    .join(
        products,
        col("sales.product_id") == col("products.product_id"),
        "inner"
    )
    .join(
        stores,
        col("sales.store_id") == col("stores.store_id"),
        "inner"
    )
    .groupBy(
        col("stores.store_id"),
        col("stores.store_name"),
        col("stores.location"),
        col("products.product_id"),
        col("products.product_name"),
        col("products.category")
    )
    .agg(
        count(col("sales.transaction_id")).alias("total_transactions"),
        sum(col("sales.quantity_sold")).alias("total_quantity_sold"),
        round(sum(col("sales.total_amount")), 2).alias("total_revenue"),
        round(avg(col("products.unit_price")), 2).alias("average_unit_price"),
        countDistinct(col("sales.transaction_id")).alias("transaction_count")
    )
)

# COMMAND ----------

# ==========================================================
# Business Metrics Enrichment
# ==========================================================

quantity_rank_window = Window.orderBy(
    desc("total_quantity_sold")
)
revenue_rank_window = Window.orderBy(
    desc("total_revenue")
)
gold_product_performance_df = (
    gold_product_performance_df
    .withColumn(
        "quantity_rank",
        dense_rank().over(quantity_rank_window)
    )
    .withColumn(
        "revenue_rank",
        dense_rank().over(revenue_rank_window)
    )
)

# COMMAND ----------

gold_product_performance_df = (
    gold_product_performance_df
    .withColumn(
        "top_5_best_seller",
        when(
            col("quantity_rank") <= 5,
            lit("Yes")
        ).otherwise(
            lit("No")
        )
    )
)

# COMMAND ----------

display(
    gold_product_performance_df
    .orderBy("quantity_rank")
)

# COMMAND ----------

# ==========================================================
# Write Gold Product Performance
# ==========================================================
(
    gold_product_performance_df.write\
                        .format("delta")\
                        .mode("overwrite")\
                        .save(f"{gold_path}gold_product_performance")
)

# COMMAND ----------

# ==========================================================
# Validate Gold Product Performance
# ==========================================================
gold_product_validation_df = (
    spark.read\
    .format("delta")\
    .load(f"{gold_path}gold_product_performance")
)

print(f"Total Products : {gold_product_validation_df.count()}")

display(
    gold_product_validation_df
    .orderBy("quantity_rank")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 3 - Inventory Analysis
# MAGIC
# MAGIC ## Business Objective
# MAGIC
# MAGIC Generate inventory insights to optimize stock levels and support procurement decisions.
# MAGIC
# MAGIC ### Business Questions Covered
# MAGIC
# MAGIC - Which products are low on stock?
# MAGIC - Which products are fast-moving?
# MAGIC - Which products are slow-moving?
# MAGIC - What is the recommended reorder point?
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC Business-ready Gold Delta table for inventory monitoring and replenishment planning.

# COMMAND ----------

# ==========================================================
# Gold Inventory Analysis Transformation
# ==========================================================

gold_inventory_analysis_df = (
    inventory
    .join(
        products,
        col("inventory.product_id") == col("products.product_id"),
        "inner"
    )
    .join(
        sales,
        [
            inventory.store_id == sales.store_id,
            inventory.product_id == sales.product_id
        ],
        "left"
    )
    .groupBy(
        col("inventory.inventory_id"),
        col("inventory.store_id"),
        col("inventory.product_id"),
        col("products.product_name"),
        col("products.category"),
        col("products.unit_price"),
        col("inventory.quantity_on_hand")
    )
    .agg(
        coalesce(sum(col("sales.quantity_sold")), lit(0)).alias("total_quantity_sold")
    )
    .withColumn(
        "inventory_value",
        round(col("quantity_on_hand") * col("unit_price"), 2)
    )
)

# COMMAND ----------

# ==========================================================
# Business Metrics Enrichment
# ==========================================================

gold_inventory_analysis_df = (
    gold_inventory_analysis_df

    .withColumn(
        "stock_status",
        when(col("quantity_on_hand") == 0, "Out of Stock")
        .when(col("quantity_on_hand") < 50, "Low Stock")
        .otherwise("In Stock")
    )

    .withColumn(
        "sales_velocity",
        when(col("total_quantity_sold") >= col("quantity_on_hand") * 2, "Fast Moving")
        .when(col("total_quantity_sold") >= col("quantity_on_hand"), "Medium Moving")
        .otherwise("Slow Moving")
    )

    .withColumn(
        "reorder_point",
        ceil(col("total_quantity_sold") * 0.30)
    )

    .withColumn(
        "overstock_candidate",
        when(
            (col("quantity_on_hand") > col("total_quantity_sold") * 2) &
            (col("sales_velocity") == "Slow Moving"),
            "Yes"
        ).otherwise("No")
    )
)
gold_inventory_analysis_df.printSchema()
display(gold_inventory_analysis_df)

# COMMAND ----------

# ==========================================================
# Write Gold Inventory Analysis
# ==========================================================

(
    gold_inventory_analysis_df.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}gold_inventory_analysis")
)

# COMMAND ----------

# ==========================================================
# Validate Gold Inventory Analysis
# ==========================================================

gold_inventory_validation_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_inventory_analysis")
)

print(f"Total Inventory Records: {gold_inventory_validation_df.count()}")

display(gold_inventory_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Table 4 - Supplier Performance
# MAGIC
# MAGIC ## Business Objective
# MAGIC
# MAGIC Generate supplier performance metrics to evaluate delivery efficiency and supplier reliability.
# MAGIC
# MAGIC ### Business Questions Covered
# MAGIC
# MAGIC - Which suppliers have the shortest average delivery time?
# MAGIC - Which suppliers are the most reliable?
# MAGIC
# MAGIC ### Output
# MAGIC
# MAGIC Business-ready Gold Delta table for supplier performance analysis.

# COMMAND ----------

# ==========================================================
# Gold Supplier Performance Transformation
# ==========================================================

gold_supplier_performance_df = (
    purchase_orders
    .join(
        suppliers,
        col("purchase_orders.supplier_id") == col("suppliers.supplier_id"),
        "inner"
    )
    .withColumn(
        "delivery_days",
        datediff(
            col("purchase_orders.delivery_date"),
            col("purchase_orders.order_date")
        )
    )
    .groupBy(
        col("suppliers.supplier_id"),
        col("suppliers.supplier_name"),
        col("suppliers.contact_person")
    )
    .agg(
        count(col("purchase_orders.po_id")).alias("total_orders"),
        round(avg("delivery_days"), 2).alias("average_delivery_days"),
        sum(
            when(col("delivery_days") <= 7, 1).otherwise(0)
        ).alias("on_time_deliveries"),
        sum(
            when(col("delivery_days") > 7, 1).otherwise(0)
        ).alias("late_deliveries")
    )
)

# COMMAND ----------

# ==========================================================
# Business Metrics Enrichment
# ==========================================================

supplier_rank_window = Window.orderBy(col("average_delivery_days").asc())

gold_supplier_performance_df = (
    gold_supplier_performance_df
    .withColumn("supplier_rank", dense_rank().over(supplier_rank_window))
    .withColumn(
        "on_time_delivery_percentage",
        round((col("on_time_deliveries") / col("total_orders")) * 100, 2)
    )
    .withColumn(
        "supplier_rating",
        when(col("on_time_delivery_percentage") >= 95, "Excellent")
        .when(col("on_time_delivery_percentage") >= 85, "Good")
        .when(col("on_time_delivery_percentage") >= 70, "Average")
        .otherwise("Needs Improvement")
    )
)

display(gold_supplier_performance_df.orderBy("supplier_rank"))

# COMMAND ----------

# ==========================================================
# Write Gold Supplier Performance
# ==========================================================

(
    gold_supplier_performance_df.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}gold_supplier_performance")
)

# COMMAND ----------

# ==========================================================
# Validate Gold Supplier Performance
# ==========================================================

gold_supplier_validation_df = (
    spark.read
    .format("delta")
    .load(f"{gold_path}gold_supplier_performance")
)

print(f"Total Suppliers: {gold_supplier_validation_df.count()}")

display(gold_supplier_validation_df.orderBy("supplier_rank"))

# COMMAND ----------

gold_supplier_performance_df.printSchema()

# COMMAND ----------

