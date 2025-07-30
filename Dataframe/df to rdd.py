# Databricks notebook source
data = [("Raju", 25), ("Prabhas", 35), ("Nagaraju", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# Convert to RDD
rdd = df.rdd

# View RDD content
for row in rdd.collect():
    print(row)


# COMMAND ----------

from pyspark.sql import Row

rdd = spark.sparkContext.parallelize([
    Row(name="Raju", age=25),
    Row(name="Prabhas", age=35),
    Row(name="Nagaraju", age=28)
])

df = spark.createDataFrame(rdd)
df.show()


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

rdd = spark.sparkContext.parallelize([
    ("Raju", 25), 
    ("Prabhas", 35), 
    ("Nagaraju", 28)
])

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.createDataFrame(rdd, schema)
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC summary 
# MAGIC | Task                   | Code                                 |
# MAGIC | ---------------------- | ------------------------------------ |
# MAGIC | DataFrame → RDD        | `df.rdd`                             |
# MAGIC | RDD → DataFrame        | `spark.createDataFrame(rdd)`         |
# MAGIC | RDD → DF (with schema) | `spark.createDataFrame(rdd, schema)` |
# MAGIC

# COMMAND ----------

