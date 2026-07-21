# Databricks notebook source
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
storage_account_key = "your_storage_account_keys"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)
print("Connected to ADLS Gen2 successfully!")

# COMMAND ----------

# ==========================================================
# Storage Paths
# ==========================================================

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"

silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

bronze_products_df = spark.read.format("delta").load(bronze_path + "products")
display(bronze_products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Transformation for product table
# MAGIC

# COMMAND ----------

silver_products_df = bronze_products_df.filter(col("unit_price")>0)
display(silver_products_df)

# COMMAND ----------

silver_products_df.write\
    .mode("overwrite")\
    .format("delta")\
    .save(silver_path +"products")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stores Table transformation

# COMMAND ----------

bronze_stores_df = spark.read\
    .format("delta")\
    .load(bronze_path + "stores")
display(bronze_stores_df)

# COMMAND ----------

silver_stores_df = bronze_stores_df.fillna("Unknown","manager_name")
display(silver_stores_df)

# COMMAND ----------

silver_stores_df.write\
    .mode("overwrite")\
    .format("delta")\
    .save(silver_path + "stores")


# COMMAND ----------

display(dbutils.fs.ls(silver_path))
display(dbutils.fs.ls(silver_path + "products"))

# COMMAND ----------

bronze_suppliers_df = spark.read\
    .format("delta")\
    .load(bronze_path + "suppliers")
display(bronze_suppliers_df)


# COMMAND ----------


silver_suppliers_df = (
    bronze_suppliers_df
    .dropDuplicates()
    .withColumn("phone", regexp_replace("phone", "[^0-9]", ""))
    .withColumn(
        "phone",
        when(length("phone") >= 10,
             expr("right(phone,10)"))
        .otherwise(None)
    )
)

# COMMAND ----------

display(silver_suppliers_df)

# COMMAND ----------

silver_suppliers_df.write\
    .mode("overwrite")\
    .format("delta")\
    .save(silver_path + "suppliers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inventory Table transformations

# COMMAND ----------

bronze_inventory_df = spark.read\
    .format("delta")\
    .load(bronze_path + "inventory")
display(bronze_inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC silver_inventory_df = bronze_inventory_df.filter(col("quantity_on_hand") > 0)
# MAGIC display(silver_inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC silver_inventory_df.write\
# MAGIC     .mode("overwrite")\
# MAGIC     .format("delta")\
# MAGIC     .save(silver_path + "inventory")

# COMMAND ----------

bronze_inventory_df.filter(
    col("quantity_on_hand") < 0
).show()
silver_inventory_df = bronze_inventory_df


# COMMAND ----------

silver_inventory_df.write\
    .mode("overwrite")\
    .format("delta")\
    .save(silver_path + "inventory")

# COMMAND ----------

display(silver_inventory_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Purchase order table Transformation**

# COMMAND ----------

bronze_PurchaseOrders_df = spark.read\
    .format("delta").load(bronze_path + "purchaseorders")
display(bronze_PurchaseOrders_df)


# COMMAND ----------

silver_PurchaseOrders_df = (bronze_PurchaseOrders_df.dropDuplicates()\
                            .withColumn("delivery_status",
                                        when(col("delivery_date").isNull(), "pending")\
                                        .otherwise("shipped")))
display(silver_PurchaseOrders_df)

# COMMAND ----------

silver_PurchaseOrders_df.write\
    .mode("overwrite")\
    .format("delta")\
    .save(silver_path + "purchaseorders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales_Transaction Table Transformation

# COMMAND ----------

bronze_SalesTransaction_df = spark.read\
    .format("delta").load(bronze_path + "salestransactions")
display(bronze_SalesTransaction_df)

# COMMAND ----------



silver_SalesTransaction_df = (
    bronze_SalesTransaction_df
    .dropDuplicates()
    .filter(col("quantity_sold") > 0)
    .filter(col("unit_price") >= 0)
    .withColumn("sale_date", to_date(col("sale_date")))
    .withColumn(
        "total_amount",
        col("quantity_sold") * col("unit_price")
    )
)

# COMMAND ----------

display(silver_SalesTransaction_df)

# COMMAND ----------

silver_SalesTransaction_df.write\
    .mode("overwrite")\
    .format("delta")\
    .save(silver_path + "salestransactions")

# COMMAND ----------

silver_products_df.show(10)