# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC PySpark supports Arrays, Structs, and Maps in DataFrames.

# COMMAND ----------

#arrays
#Used when one column contains a list of items (like multiple phone numbers).
from pyspark.sql.functions import split

data = [("Amit", "9876543210,9123456789")]
df = spark.createDataFrame(data, ["name", "contacts"])

# Convert comma-separated string to Array
df=df.withColumn("contact_list",split("contacts",","))
df.display()


# COMMAND ----------

#Structs
#Used when you need to keep grouped data in one column.
from pyspark.sql.functions import struct

df = df.withColumn("contact_struct", struct("name", "contacts"))
df.select("contact_struct").show(truncate=False)





# COMMAND ----------

#Maps
#Useful for key-value data like {"type": "prepaid", "status": "active"}
from pyspark.sql.functions import create_map, lit

df = df.withColumn("user_map", create_map(lit("type"), lit("prepaid"), lit("status"), lit("active")))
df.select("user_map").show(truncate=False)


# COMMAND ----------

