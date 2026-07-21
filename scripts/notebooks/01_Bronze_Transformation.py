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

landing_path = f"abfss://landing@{storage_account_name}.dfs.core.windows.net/"

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/"

# COMMAND ----------

display(dbutils.fs.ls(landing_path))

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading the Data from the Sourse**

# COMMAND ----------

# ==========================================================
# Notebook Parameters
# ==========================================================

dbutils.widgets.text("folder_name","")
dbutils.widgets.text("file_name","")
folder_name = dbutils.widgets.get("folder_name")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# ==========================================================
# Read Landing File
# ==========================================================
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"abfss://landing@stsupermarketrath004.dfs.core.windows.net/{folder_name}/{file_name}")

print(f"Reading: {folder_name}/{file_name}")


# COMMAND ----------

df.write\
        .format("delta")\
        .mode("overwrite")\
        .save(f"abfss://bronze@stsupermarketrath004.dfs.core.windows.net/{folder_name.lower()}")

print(f"Successfully loaded {folder_name} into Bronze layer")

# COMMAND ----------

print(file_name)
print(folder_name)